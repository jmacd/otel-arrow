// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context.

use bytes::Bytes;
use otap_df_config::transport_headers_policy::{
    CaptureStats, CompiledHeaderCapturePolicy, HeaderPropagationPolicy, NameStrategy,
    PropagationAction, ValueKindConfig,
};

const MAGIC: u32 = 0x4354_5832; // CTX2
const VERSION: u16 = 2;
const HEADER_LEN: usize = 24;
const ENTRY_LEN: usize = 16;
const ITEM_LEN: usize = 32;
const NO_ENTRY: u16 = u16::MAX;

const MAGIC_AT: usize = 0;
const VERSION_AT: usize = 4;
const ENTRY_COUNT_AT: usize = 6;
const ITEM_COUNT_AT: usize = 8;
const PRESENCE_WORDS_AT: usize = 10;
const MEMBER_COUNT_AT: usize = 12;
const BLOB_OFFSET_AT: usize = 16;

/// Header value kind preserved in the item descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderValueKind {
    /// A transport text value, which may contain arbitrary bytes.
    Text = 0,
    /// A transport binary value.
    Binary = 1,
}

/// One borrowed header supplied by a receiver or projector.
#[derive(Clone, Copy, Debug)]
pub struct HeaderInput<'a> {
    /// Input name preserved
    pub wire_name: &'a str,
    /// Stored name used by selectors and overrides.
    pub stored_name: &'a str,
    /// Raw header bytes.
    pub value: &'a [u8],
    /// Text or binary transport semantics.
    pub kind: HeaderValueKind,
    /// Compiled capture-rule identifier.
    pub rule_id: u16,
    /// Optional compiled `store_as` entry slot.
    pub entry: Option<u16>,
}

/// Failure while constructing or validating a context envelope.
#[derive(Debug, thiserror::Error)]
pub enum ContextBytesError {
    /// A context exceeded an indexed table bound.
    #[error("context envelope has too many {what}")]
    TooMany {
        /// Bounded item category.
        what: &'static str,
    },
    /// A byte length or offset exceeded the packed format.
    #[error("context envelope is too large")]
    TooLarge,
    /// The source bytes are not a valid context envelope.
    #[error("invalid context envelope")]
    InvalidEnvelope,
}

/// Immutable encoded pdata context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PdataContextBytes {
    bytes: Bytes,
}

/// Borrowed view of one packed context item.
#[derive(Clone, Copy, Debug)]
pub struct ContextItem<'a> {
    context: &'a PdataContextBytes,
    descriptor_at: usize,
    blob_at: usize,
}

impl<'a> ContextItem<'a> {
    /// Original transport wire name.
    #[must_use]
    pub fn wire_name(&self) -> Option<&'a str> {
        std::str::from_utf8(
            self.context
                .range_bytes(self.descriptor_at + 8, self.blob_at)?,
        )
        .ok()
    }

    /// Stored name used by propagation selectors and overrides.
    #[must_use]
    pub fn stored_name(&self) -> Option<&'a str> {
        std::str::from_utf8(
            self.context
                .range_bytes(self.descriptor_at + 16, self.blob_at)?,
        )
        .ok()
    }

    /// Stored name used by transport-header consumers.
    #[must_use]
    pub fn name(&self) -> Option<&'a str> {
        self.stored_name()
    }

    /// Typed raw value.
    #[must_use]
    pub fn value(&self) -> Option<(HeaderValueKind, &'a [u8])> {
        let kind = decode_kind(*self.context.bytes.get(self.descriptor_at + 4)?)?;
        Some((
            kind,
            self.context
                .range_bytes(self.descriptor_at + 24, self.blob_at)?,
        ))
    }

    /// Returns a text value as UTF-8.
    #[must_use]
    pub fn value_as_str(&self) -> Option<&'a str> {
        let (kind, value) = self.value()?;
        if kind != HeaderValueKind::Text {
            return None;
        }
        std::str::from_utf8(value).ok()
    }

    /// Compiled capture-rule identifier.
    #[must_use]
    pub fn rule_id(&self) -> Option<u16> {
        read_u16(&self.context.bytes, self.descriptor_at)
    }

    /// Optional context-entry slot.
    #[must_use]
    pub fn entry_slot(&self) -> Option<u16> {
        let slot = read_u16(&self.context.bytes, self.descriptor_at + 2)?;
        (slot != NO_ENTRY).then_some(slot)
    }
}

/// Iterator over bag items in arrival order.
pub struct ContextItems<'a> {
    context: &'a PdataContextBytes,
    next: usize,
    count: usize,
    item_at: usize,
    blob_at: usize,
}

impl<'a> Iterator for ContextItems<'a> {
    type Item = ContextItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.count {
            return None;
        }
        let item = ContextItem {
            context: self.context,
            descriptor_at: self.item_at + self.next * ITEM_LEN,
            blob_at: self.blob_at,
        };
        self.next += 1;
        Some(item)
    }
}

/// One packed header selected for propagation.
pub struct PropagatedContextItem<'a> {
    /// Egress wire name selected by the policy.
    pub header_name: &'a str,
    /// Text or binary transport semantics.
    pub value_kind: HeaderValueKind,
    /// Raw header bytes.
    pub value: &'a [u8],
}

/// Zero-allocation propagation iterator over packed context items.
pub struct ContextPropagation<'a> {
    items: ContextItems<'a>,
    policy: &'a HeaderPropagationPolicy,
}

impl<'a> Iterator for ContextPropagation<'a> {
    type Item = PropagatedContextItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.items.by_ref() {
            let stored_name = item.stored_name()?;
            let (action, name_strategy) = self.policy.resolve_stored_name(stored_name);
            if action == PropagationAction::Drop {
                continue;
            }
            let header_name = match name_strategy {
                NameStrategy::Preserve => item.wire_name()?,
                NameStrategy::StoredName => stored_name,
            };
            let (value_kind, value) = item.value()?;
            return Some(PropagatedContextItem {
                header_name,
                value_kind,
                value,
            });
        }
        None
    }
}

/// Borrowed view of one compiled context entry.
pub struct ContextEntry<'a> {
    context: &'a PdataContextBytes,
    first_member: usize,
    member_count: usize,
    member_at: usize,
    item_at: usize,
    blob_at: usize,
    hash: u64,
}

impl ContextEntry<'_> {
    /// Returns the typed hash. Callers must compare values on a hash hit.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.hash
    }

    /// Iterates the entry's typed values in arrival order.
    pub fn values(&self) -> impl Iterator<Item = (HeaderValueKind, &[u8])> {
        (0..self.member_count).filter_map(move |index| {
            let member = read_u16(
                &self.context.bytes,
                self.member_at + (self.first_member + index) * 2,
            )? as usize;
            self.context
                .value_at(self.item_at + member * ITEM_LEN, self.blob_at)
        })
    }
}

/// Projection accumulator for deriving one immutable output context.
pub struct ContextProjectionAccumulator<'a> {
    input: &'a PdataContextBytes,
}

impl ContextProjectionAccumulator<'_> {
    /// Appends one bag-only header in one new allocation.
    ///
    /// Existing descriptors and blob bytes are copied without decoding.
    /// Blob-relative offsets remain unchanged.
    pub fn append_bag_header(
        self,
        header: HeaderInput<'_>,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        if header.entry.is_some() {
            return Err(ContextBytesError::TooMany {
                what: "entry-producing projections",
            });
        }

        let old = Layout::parse(&self.input.bytes)?;
        let new_item_count = old
            .item_count
            .checked_add(1)
            .ok_or(ContextBytesError::TooMany { what: "items" })?;
        if new_item_count > u16::MAX as usize {
            return Err(ContextBytesError::TooMany { what: "items" });
        }
        let new = Layout::new(
            old.entry_count,
            new_item_count,
            old.member_count,
            old.presence_words,
            old.blob_len + input_blob_len(&header),
        )?;
        let mut output = vec![0u8; new.total_len];
        write_header(&mut output, &new);

        output[HEADER_LEN..old.item_at].copy_from_slice(&self.input.bytes[HEADER_LEN..old.item_at]);
        output[new.item_at..new.item_at + old.item_count * ITEM_LEN]
            .copy_from_slice(&self.input.bytes[old.item_at..old.member_at]);
        output[new.member_at..new.member_at + old.member_count * 2]
            .copy_from_slice(&self.input.bytes[old.member_at..old.blob_at]);
        output[new.blob_at..new.blob_at + old.blob_len]
            .copy_from_slice(&self.input.bytes[old.blob_at..]);

        let mut blob_cursor = old.blob_len;
        write_item(&mut output, &new, old.item_count, &header, &mut blob_cursor)?;
        Ok(PdataContextBytes::from_vec(output))
    }
}

impl PdataContextBytes {
    /// Captures headers with a compiled policy.
    pub fn capture<'a>(
        policy: &CompiledHeaderCapturePolicy,
        pairs: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    ) -> Result<(Option<Self>, Option<CaptureStats>), ContextBytesError> {
        let defaults = policy.defaults();
        let mut headers = smallvec::SmallVec::<[HeaderInput<'_>; 32]>::new();
        let mut skipped_max_entries = 0;
        let mut skipped_name_too_long = 0;
        let mut skipped_value_too_long = 0;

        for (wire_name, value) in pairs {
            let Some(matched) = policy.match_header(wire_name) else {
                continue;
            };
            if headers.len() >= defaults.max_entries {
                skipped_max_entries += 1;
                continue;
            }
            if wire_name.len() > defaults.max_name_bytes {
                skipped_name_too_long += 1;
                continue;
            }
            if value.len() > defaults.max_value_bytes {
                skipped_value_too_long += 1;
                continue;
            }
            let kind = match matched.value_kind {
                Some(ValueKindConfig::Binary) => HeaderValueKind::Binary,
                Some(ValueKindConfig::Text) => HeaderValueKind::Text,
                None if wire_name.ends_with("-bin") => HeaderValueKind::Binary,
                None => HeaderValueKind::Text,
            };
            headers.push(HeaderInput {
                wire_name,
                stored_name: matched.stored_name,
                value,
                kind,
                rule_id: matched.rule_id,
                entry: matched.entry,
            });
        }

        let stats =
            (skipped_max_entries > 0 || skipped_name_too_long > 0 || skipped_value_too_long > 0)
                .then_some(CaptureStats {
                    skipped_max_entries,
                    skipped_name_too_long,
                    skipped_value_too_long,
                });
        let context = (!headers.is_empty())
            .then(|| Self::build(policy.entry_count(), headers))
            .transpose()?;
        Ok((context, stats))
    }

    /// Builds a packed context in one retained allocation.
    pub fn build<'a>(
        entry_count: usize,
        headers: impl IntoIterator<Item = HeaderInput<'a>>,
    ) -> Result<Self, ContextBytesError> {
        let headers: smallvec::SmallVec<[HeaderInput<'_>; 32]> = headers.into_iter().collect();
        if entry_count > u16::MAX as usize {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }
        if headers.len() > u16::MAX as usize {
            return Err(ContextBytesError::TooMany { what: "items" });
        }

        let presence_words = entry_count.div_ceil(64);
        let mut presence = smallvec::SmallVec::<[u64; 2]>::from_elem(0, presence_words);
        let mut members =
            smallvec::SmallVec::<[smallvec::SmallVec<[u16; 4]>; 16]>::with_capacity(entry_count);
        members.resize_with(entry_count, smallvec::SmallVec::new);
        for (index, header) in headers.iter().enumerate() {
            if let Some(entry) = header.entry {
                let entry = entry as usize;
                if entry >= entry_count {
                    return Err(ContextBytesError::InvalidEnvelope);
                }
                presence[entry / 64] |= 1u64 << (entry % 64);
                members[entry].push(index as u16);
            }
        }
        let member_count = members.iter().map(|entry| entry.len()).sum();
        let blob_len = headers
            .iter()
            .try_fold(0usize, |total, header| {
                total.checked_add(input_blob_len(header))
            })
            .ok_or(ContextBytesError::TooLarge)?;
        let layout = Layout::new(
            entry_count,
            headers.len(),
            member_count,
            presence_words,
            blob_len,
        )?;
        let mut output = vec![0u8; layout.total_len];
        write_header(&mut output, &layout);

        let mut at = HEADER_LEN;
        for word in &presence {
            output[at..at + 8].copy_from_slice(&word.to_le_bytes());
            at += 8;
        }
        let mut member_cursor = 0usize;
        for (slot, entry_members) in members.iter().enumerate() {
            let entry_at = layout.entry_at + slot * ENTRY_LEN;
            write_u32(&mut output, entry_at, member_cursor)?;
            write_u32(&mut output, entry_at + 4, entry_members.len())?;
            write_u64(
                &mut output,
                entry_at + 8,
                entry_hash(slot, entry_members, &headers),
            )?;
            member_cursor += entry_members.len();
        }
        let mut member_at = layout.member_at;
        for entry_members in &members {
            for member in entry_members {
                output[member_at..member_at + 2].copy_from_slice(&member.to_le_bytes());
                member_at += 2;
            }
        }
        let mut blob_cursor = 0usize;
        for (index, header) in headers.iter().enumerate() {
            write_item(&mut output, &layout, index, header, &mut blob_cursor)?;
        }
        Ok(Self::from_vec(output))
    }

    /// Validates and adopts an encoded context.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, ContextBytesError> {
        validate(&bytes)?;
        Ok(Self { bytes })
    }

    fn from_vec(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Bytes::from(bytes),
        }
    }

    /// Starts a projection accumulator that preserves this context.
    #[must_use]
    pub fn project(&self) -> ContextProjectionAccumulator<'_> {
        ContextProjectionAccumulator { input: self }
    }

    /// Returns the single reference-counted allocation.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Iterates all bag items in arrival order.
    pub fn items(&self) -> ContextItems<'_> {
        let layout = Layout::parse(&self.bytes).ok();
        ContextItems {
            context: self,
            next: 0,
            count: layout.map_or(0, |layout| layout.item_count),
            item_at: layout.map_or(0, |layout| layout.item_at),
            blob_at: layout.map_or(0, |layout| layout.blob_at),
        }
    }

    /// Number of packed bag items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.item_count().unwrap_or(0)
    }

    /// Whether the packed bag is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates packed items in arrival order.
    pub fn iter(&self) -> ContextItems<'_> {
        self.items()
    }

    /// Finds headers by stored name.
    pub fn find_by_name<'a>(&'a self, name: &'a str) -> impl Iterator<Item = ContextItem<'a>> {
        self.iter()
            .filter(move |item| item.stored_name() == Some(name))
    }

    /// Applies a transport-header propagation policy to the packed bag.
    pub fn propagate<'a>(&'a self, policy: &'a HeaderPropagationPolicy) -> ContextPropagation<'a> {
        ContextPropagation {
            items: self.items(),
            policy,
        }
    }

    /// Returns one bag item.
    #[must_use]
    pub fn item(&self, index: usize) -> Option<ContextItem<'_>> {
        let (item_at, blob_at) = self.item_layout()?;
        (index < self.item_count()?).then_some(())?;
        Some(ContextItem {
            context: self,
            descriptor_at: item_at + index * ITEM_LEN,
            blob_at,
        })
    }

    /// Returns one typed value.
    #[must_use]
    pub fn value(&self, index: usize) -> Option<(HeaderValueKind, &[u8])> {
        let (item_at, blob_at) = self.item_layout()?;
        (index < self.item_count()?).then_some(())?;
        self.value_at(item_at + index * ITEM_LEN, blob_at)
    }

    /// Returns a present entry through its schema-local slot.
    #[must_use]
    pub fn entry(&self, slot: usize) -> Option<ContextEntry<'_>> {
        let entry_count = read_u16(&self.bytes, ENTRY_COUNT_AT)? as usize;
        let presence_words = read_u16(&self.bytes, PRESENCE_WORDS_AT)? as usize;
        if slot >= entry_count {
            return None;
        }
        let word = read_u64(&self.bytes, HEADER_LEN + (slot / 64) * 8)?;
        if word & (1u64 << (slot % 64)) == 0 {
            return None;
        }
        let entry_at = HEADER_LEN + presence_words * 8;
        let item_at = entry_at + entry_count * ENTRY_LEN;
        let item_count = self.item_count()?;
        let member_at = item_at + item_count * ITEM_LEN;
        let blob_at = read_u32(&self.bytes, BLOB_OFFSET_AT)? as usize;
        let at = entry_at + slot * ENTRY_LEN;
        Some(ContextEntry {
            context: self,
            first_member: read_u32(&self.bytes, at)? as usize,
            member_count: read_u32(&self.bytes, at + 4)? as usize,
            member_at,
            item_at,
            blob_at,
            hash: read_u64(&self.bytes, at + 8)?,
        })
    }

    fn item_count(&self) -> Option<usize> {
        read_u16(&self.bytes, ITEM_COUNT_AT).map(usize::from)
    }

    fn item_layout(&self) -> Option<(usize, usize)> {
        let entry_count = read_u16(&self.bytes, ENTRY_COUNT_AT)? as usize;
        let presence_words = read_u16(&self.bytes, PRESENCE_WORDS_AT)? as usize;
        let item_at = HEADER_LEN + presence_words * 8 + entry_count * ENTRY_LEN;
        let blob_at = read_u32(&self.bytes, BLOB_OFFSET_AT)? as usize;
        Some((item_at, blob_at))
    }

    fn range_bytes(&self, at: usize, blob_at: usize) -> Option<&[u8]> {
        let offset = read_u32(&self.bytes, at)? as usize;
        let len = read_u32(&self.bytes, at + 4)? as usize;
        self.bytes.get(blob_at + offset..blob_at + offset + len)
    }

    fn value_at(&self, descriptor_at: usize, blob_at: usize) -> Option<(HeaderValueKind, &[u8])> {
        let kind = decode_kind(*self.bytes.get(descriptor_at + 4)?)?;
        Some((kind, self.range_bytes(descriptor_at + 24, blob_at)?))
    }
}

#[derive(Clone, Copy)]
struct Layout {
    entry_count: usize,
    item_count: usize,
    member_count: usize,
    presence_words: usize,
    entry_at: usize,
    item_at: usize,
    member_at: usize,
    blob_at: usize,
    blob_len: usize,
    total_len: usize,
}

impl Layout {
    fn new(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        presence_words: usize,
        blob_len: usize,
    ) -> Result<Self, ContextBytesError> {
        let entry_at = checked_add(HEADER_LEN, presence_words * 8)?;
        let item_at = checked_add(entry_at, entry_count * ENTRY_LEN)?;
        let member_at = checked_add(item_at, item_count * ITEM_LEN)?;
        let blob_at = checked_add(member_at, member_count * 2)?;
        let total_len = checked_add(blob_at, blob_len)?;
        if total_len > u32::MAX as usize {
            return Err(ContextBytesError::TooLarge);
        }
        Ok(Self {
            entry_count,
            item_count,
            member_count,
            presence_words,
            entry_at,
            item_at,
            member_at,
            blob_at,
            blob_len,
            total_len,
        })
    }

    fn parse(bytes: &[u8]) -> Result<Self, ContextBytesError> {
        if read_u32(bytes, MAGIC_AT) != Some(MAGIC) || read_u16(bytes, VERSION_AT) != Some(VERSION)
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        let entry_count =
            read_u16(bytes, ENTRY_COUNT_AT).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let item_count =
            read_u16(bytes, ITEM_COUNT_AT).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let presence_words =
            read_u16(bytes, PRESENCE_WORDS_AT).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let member_count =
            read_u32(bytes, MEMBER_COUNT_AT).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let blob_at =
            read_u32(bytes, BLOB_OFFSET_AT).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let layout = Self::new(
            entry_count,
            item_count,
            member_count,
            presence_words,
            bytes.len().saturating_sub(blob_at),
        )?;
        if presence_words != entry_count.div_ceil(64)
            || layout.blob_at != blob_at
            || layout.total_len != bytes.len()
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        Ok(layout)
    }
}

fn write_header(bytes: &mut [u8], layout: &Layout) {
    bytes[MAGIC_AT..MAGIC_AT + 4].copy_from_slice(&MAGIC.to_le_bytes());
    bytes[VERSION_AT..VERSION_AT + 2].copy_from_slice(&VERSION.to_le_bytes());
    bytes[ENTRY_COUNT_AT..ENTRY_COUNT_AT + 2]
        .copy_from_slice(&(layout.entry_count as u16).to_le_bytes());
    bytes[ITEM_COUNT_AT..ITEM_COUNT_AT + 2]
        .copy_from_slice(&(layout.item_count as u16).to_le_bytes());
    bytes[PRESENCE_WORDS_AT..PRESENCE_WORDS_AT + 2]
        .copy_from_slice(&(layout.presence_words as u16).to_le_bytes());
    bytes[MEMBER_COUNT_AT..MEMBER_COUNT_AT + 4]
        .copy_from_slice(&(layout.member_count as u32).to_le_bytes());
    bytes[BLOB_OFFSET_AT..BLOB_OFFSET_AT + 4]
        .copy_from_slice(&(layout.blob_at as u32).to_le_bytes());
}

fn write_item(
    bytes: &mut [u8],
    layout: &Layout,
    index: usize,
    input: &HeaderInput<'_>,
    blob_cursor: &mut usize,
) -> Result<(), ContextBytesError> {
    let at = layout.item_at + index * ITEM_LEN;
    bytes[at..at + 2].copy_from_slice(&input.rule_id.to_le_bytes());
    bytes[at + 2..at + 4].copy_from_slice(&input.entry.unwrap_or(NO_ENTRY).to_le_bytes());
    bytes[at + 4] = input.kind as u8;
    write_blob_range(
        bytes,
        layout,
        at + 8,
        input.wire_name.as_bytes(),
        blob_cursor,
    )?;
    write_blob_range(
        bytes,
        layout,
        at + 16,
        input.stored_name.as_bytes(),
        blob_cursor,
    )?;
    write_blob_range(bytes, layout, at + 24, input.value, blob_cursor)
}

fn write_blob_range(
    bytes: &mut [u8],
    layout: &Layout,
    descriptor_at: usize,
    value: &[u8],
    blob_cursor: &mut usize,
) -> Result<(), ContextBytesError> {
    let end = blob_cursor
        .checked_add(value.len())
        .ok_or(ContextBytesError::TooLarge)?;
    if end > layout.blob_len {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    write_u32(bytes, descriptor_at, *blob_cursor)?;
    write_u32(bytes, descriptor_at + 4, value.len())?;
    bytes[layout.blob_at + *blob_cursor..layout.blob_at + end].copy_from_slice(value);
    *blob_cursor = end;
    Ok(())
}

fn validate(bytes: &[u8]) -> Result<(), ContextBytesError> {
    let layout = Layout::parse(bytes)?;
    for slot in 0..layout.entry_count {
        let at = layout.entry_at + slot * ENTRY_LEN;
        let first = read_u32(bytes, at).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        let count = read_u32(bytes, at + 4).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
        if first
            .checked_add(count)
            .is_none_or(|end| end > layout.member_count)
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }
    }
    for member in 0..layout.member_count {
        if read_u16(bytes, layout.member_at + member * 2)
            .is_none_or(|item| item as usize >= layout.item_count)
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }
    }
    for item in 0..layout.item_count {
        let at = layout.item_at + item * ITEM_LEN;
        let _kind = decode_kind(
            *bytes
                .get(at + 4)
                .ok_or(ContextBytesError::InvalidEnvelope)?,
        )
        .ok_or(ContextBytesError::InvalidEnvelope)?;
        let entry = read_u16(bytes, at + 2).ok_or(ContextBytesError::InvalidEnvelope)?;
        if entry != NO_ENTRY && entry as usize >= layout.entry_count {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        for range_at in [8, 16, 24] {
            let offset =
                read_u32(bytes, at + range_at).ok_or(ContextBytesError::InvalidEnvelope)? as usize;
            let len = read_u32(bytes, at + range_at + 4)
                .ok_or(ContextBytesError::InvalidEnvelope)? as usize;
            if offset
                .checked_add(len)
                .is_none_or(|end| end > layout.blob_len)
            {
                return Err(ContextBytesError::InvalidEnvelope);
            }
        }
        let wire_offset = read_u32(bytes, at + 8).ok_or(ContextBytesError::InvalidEnvelope)?;
        let wire_len = read_u32(bytes, at + 12).ok_or(ContextBytesError::InvalidEnvelope)?;
        let stored_offset = read_u32(bytes, at + 16).ok_or(ContextBytesError::InvalidEnvelope)?;
        let stored_len = read_u32(bytes, at + 20).ok_or(ContextBytesError::InvalidEnvelope)?;
        let wire = bytes
            .get(
                layout.blob_at + wire_offset as usize
                    ..layout.blob_at + wire_offset as usize + wire_len as usize,
            )
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        let stored = bytes
            .get(
                layout.blob_at + stored_offset as usize
                    ..layout.blob_at + stored_offset as usize + stored_len as usize,
            )
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        if std::str::from_utf8(wire).is_err() || std::str::from_utf8(stored).is_err() {
            return Err(ContextBytesError::InvalidEnvelope);
        }
    }
    Ok(())
}

fn input_blob_len(input: &HeaderInput<'_>) -> usize {
    input.wire_name.len() + input.stored_name.len() + input.value.len()
}

fn decode_kind(value: u8) -> Option<HeaderValueKind> {
    match value {
        0 => Some(HeaderValueKind::Text),
        1 => Some(HeaderValueKind::Binary),
        _ => None,
    }
}

fn entry_hash(slot: usize, members: &[u16], headers: &[HeaderInput<'_>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    hash_bytes(&mut hash, &(slot as u64).to_le_bytes());
    for member in members {
        let header = &headers[*member as usize];
        hash_bytes(&mut hash, &[header.kind as u8]);
        hash_bytes(&mut hash, &(header.value.len() as u32).to_le_bytes());
        hash_bytes(&mut hash, header.value);
    }
    hash
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, ContextBytesError> {
    left.checked_add(right).ok_or(ContextBytesError::TooLarge)
}

fn read_u16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn write_u32(bytes: &mut [u8], at: usize, value: usize) -> Result<(), ContextBytesError> {
    let value = u32::try_from(value).map_err(|_| ContextBytesError::TooLarge)?;
    bytes
        .get_mut(at..at + 4)
        .ok_or(ContextBytesError::InvalidEnvelope)?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(bytes: &mut [u8], at: usize, value: u64) -> Result<(), ContextBytesError> {
    bytes
        .get_mut(at..at + 8)
        .ok_or(ContextBytesError::InvalidEnvelope)?
        .copy_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_config::transport_headers_policy::{
        PropagationDefault, PropagationMatch, PropagationOverride, PropagationSelector,
        PropagationSelectorType,
    };

    /// Scenario: an entry has duplicate typed values interleaved with a
    /// bag-only header.
    /// Guarantees: names and values occupy one packed blob, bag order is
    /// preserved, and the entry index resolves only its ordered members.
    #[test]
    fn packed_context_indexes_entries_and_bag() {
        let context = PdataContextBytes::build(
            1,
            [
                HeaderInput {
                    wire_name: "X-Tenant",
                    stored_name: "tenant",
                    value: b"acme",
                    kind: HeaderValueKind::Text,
                    rule_id: 0,
                    entry: Some(0),
                },
                HeaderInput {
                    wire_name: "X-Request-Id",
                    stored_name: "x-request-id",
                    value: b"request-1",
                    kind: HeaderValueKind::Text,
                    rule_id: 1,
                    entry: None,
                },
                HeaderInput {
                    wire_name: "X-Tenant-Bin",
                    stored_name: "tenant",
                    value: &[0x01, 0x02],
                    kind: HeaderValueKind::Binary,
                    rule_id: 0,
                    entry: Some(0),
                },
            ],
        )
        .expect("context encodes");

        let items: Vec<_> = context.items().collect();
        assert_eq!(items[0].wire_name(), Some("X-Tenant"));
        assert_eq!(items[0].stored_name(), Some("tenant"));
        assert_eq!(items[1].wire_name(), Some("X-Request-Id"));
        assert_eq!(
            items[2].value(),
            Some((HeaderValueKind::Binary, &[0x01u8, 0x02][..]))
        );

        let entry = context.entry(0).expect("entry is present");
        assert_ne!(entry.hash(), 0);
        assert_eq!(
            entry.values().collect::<Vec<_>>(),
            vec![
                (HeaderValueKind::Text, b"acme".as_slice()),
                (HeaderValueKind::Binary, &[0x01u8, 0x02][..]),
            ]
        );
        assert_eq!(
            context.bytes().len(),
            Layout::parse(context.bytes()).unwrap().total_len
        );
    }

    /// Scenario: a partition projection appends one bag-only item to a
    /// context containing a tenant entry.
    /// Guarantees: existing descriptor offsets, entry hash, and values remain
    /// stable while the projected item is appended in one new packed buffer.
    #[test]
    fn projection_appends_bag_item_without_decoding_input() {
        let input = PdataContextBytes::build(
            1,
            [
                HeaderInput {
                    wire_name: "x-tenant",
                    stored_name: "tenant",
                    value: b"acme",
                    kind: HeaderValueKind::Text,
                    rule_id: 0,
                    entry: Some(0),
                },
                HeaderInput {
                    wire_name: "x-request-id",
                    stored_name: "x-request-id",
                    value: b"request-1",
                    kind: HeaderValueKind::Text,
                    rule_id: 1,
                    entry: None,
                },
            ],
        )
        .expect("input context");
        let old_hash = input.entry(0).expect("tenant entry").hash();

        let output = input
            .project()
            .append_bag_header(HeaderInput {
                wire_name: "Partition",
                stored_name: "partition",
                value: b"west",
                kind: HeaderValueKind::Text,
                rule_id: u16::MAX,
                entry: None,
            })
            .expect("projected context");

        assert_eq!(output.entry(0).expect("tenant entry").hash(), old_hash);
        assert_eq!(
            output.item(2).and_then(|item| item.wire_name()),
            Some("Partition")
        );
        assert_eq!(
            output.item(2).and_then(|item| item.stored_name()),
            Some("partition")
        );
        assert_eq!(
            output.value(2),
            Some((HeaderValueKind::Text, b"west".as_slice()))
        );
    }

    /// Scenario: named propagation selects a stored entry, renames it to the
    /// stored name, and an override drops another bag item.
    /// Guarantees: packed propagation matches legacy selector, override, and
    /// name-strategy semantics without materializing header objects.
    #[test]
    fn packed_propagation_applies_named_selector_and_override() {
        let context = PdataContextBytes::build(
            1,
            [
                HeaderInput {
                    wire_name: "X-Tenant",
                    stored_name: "tenant",
                    value: b"acme",
                    kind: HeaderValueKind::Text,
                    rule_id: 0,
                    entry: Some(0),
                },
                HeaderInput {
                    wire_name: "Authorization",
                    stored_name: "authorization",
                    value: b"secret",
                    kind: HeaderValueKind::Text,
                    rule_id: 1,
                    entry: None,
                },
            ],
        )
        .expect("context");
        let policy = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::Named,
                    named: Some(vec!["tenant".to_string(), "authorization".to_string()]),
                },
                name: NameStrategy::StoredName,
                ..PropagationDefault::default()
            },
            vec![PropagationOverride {
                match_rule: PropagationMatch {
                    stored_names: vec!["authorization".to_string()],
                },
                action: PropagationAction::Drop,
                name: None,
                on_error: None,
            }],
        );

        let propagated: Vec<_> = context.propagate(&policy).collect();
        assert_eq!(propagated.len(), 1);
        assert_eq!(propagated[0].header_name, "tenant");
        assert_eq!(propagated[0].value, b"acme");
        assert_eq!(propagated[0].value_kind, HeaderValueKind::Text);
    }
}
