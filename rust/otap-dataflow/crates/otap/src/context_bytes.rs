// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context.

use std::ops::Range;

use bytes::Bytes;
use otap_df_config::transport_headers_policy::{
    CaptureStats, CompiledHeaderCapturePolicy, HeaderPropagationPolicy, NameStrategy,
    PropagationAction, ValueKindConfig,
};

const MAGIC: u32 = 0x4354_5832; // CTX2
const VERSION: u16 = 2;
const NO_ENTRY: u16 = u16::MAX;

const HEADER_LEN: usize = 24;
const HEADER_MAGIC_AT: usize = 0;
const HEADER_VERSION_AT: usize = 4;
const HEADER_ENTRY_COUNT_AT: usize = 6;
const HEADER_ITEM_COUNT_AT: usize = 8;
const HEADER_PRESENCE_WORDS_AT: usize = 10;
const HEADER_MEMBER_COUNT_AT: usize = 12;
const HEADER_BLOB_OFFSET_AT: usize = 16;
const HEADER_RESERVED_AT: usize = 20;

const ENTRY_DESCRIPTOR_LEN: usize = 16;
const ENTRY_FIRST_MEMBER_AT: usize = 0;
const ENTRY_MEMBER_COUNT_AT: usize = 4;
const ENTRY_HASH_AT: usize = 8;

const ITEM_DESCRIPTOR_LEN: usize = 32;
const ITEM_RULE_ID_AT: usize = 0;
const ITEM_ENTRY_AT: usize = 2;
const ITEM_KIND_AT: usize = 4;
const ITEM_RESERVED_AT: usize = 5;
const ITEM_WIRE_NAME_AT: usize = 8;
const ITEM_STORED_NAME_AT: usize = 16;
const ITEM_VALUE_AT: usize = 24;

/// Header value kind preserved in the item descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderValueKind {
    /// A transport text value, which may contain arbitrary bytes.
    Text = 0,
    /// A transport binary value.
    Binary = 1,
}

impl HeaderValueKind {
    fn captured(config: Option<ValueKindConfig>, wire_name: &str) -> Self {
        match config {
            Some(ValueKindConfig::Binary) => Self::Binary,
            Some(ValueKindConfig::Text) => Self::Text,
            None if wire_name.ends_with("-bin") => Self::Binary,
            None => Self::Text,
        }
    }

    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Text),
            1 => Some(Self::Binary),
            _ => None,
        }
    }
}

/// One borrowed header supplied by a receiver or projector.
#[derive(Clone, Copy, Debug)]
pub struct HeaderInput<'a> {
    /// Original transport wire name.
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

impl PdataContextBytes {
    /// Captures headers with a compiled policy.
    pub fn capture<'a>(
        policy: &CompiledHeaderCapturePolicy,
        pairs: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    ) -> Result<(Option<Self>, Option<CaptureStats>), ContextBytesError> {
        let defaults = policy.defaults();
        let mut headers = smallvec::SmallVec::<[HeaderInput<'_>; 32]>::new();
        let mut skipped = SkippedHeaders::default();

        for (wire_name, value) in pairs {
            let Some(matched) = policy.match_header(wire_name) else {
                continue;
            };
            if headers.len() >= defaults.max_entries {
                skipped.max_entries += 1;
                continue;
            }
            if wire_name.len() > defaults.max_name_bytes {
                skipped.name_too_long += 1;
                continue;
            }
            if value.len() > defaults.max_value_bytes {
                skipped.value_too_long += 1;
                continue;
            }
            headers.push(HeaderInput {
                wire_name,
                stored_name: matched.stored_name,
                value,
                kind: HeaderValueKind::captured(matched.value_kind, wire_name),
                rule_id: matched.rule_id,
                entry: matched.entry,
            });
        }

        let context = (!headers.is_empty())
            .then(|| Self::build(policy.entry_count(), headers))
            .transpose()?;
        Ok((context, skipped.into_stats()))
    }

    /// Builds a packed context in one retained allocation.
    pub fn build<'a>(
        entry_count: usize,
        headers: impl IntoIterator<Item = HeaderInput<'a>>,
    ) -> Result<Self, ContextBytesError> {
        let headers: smallvec::SmallVec<[HeaderInput<'_>; 32]> = headers.into_iter().collect();
        let entries = EntryIndex::new(entry_count, &headers)?;
        let blob_len = headers.iter().try_fold(0usize, |total, header| {
            total
                .checked_add(header.encoded_len()?)
                .ok_or(ContextBytesError::TooLarge)
        })?;
        let layout = Layout::new(entry_count, headers.len(), entries.member_count(), blob_len)?;
        let mut encoder = Encoder::new(layout)?;
        entries.write_to(&mut encoder, &headers)?;
        for (index, header) in headers.iter().enumerate() {
            encoder.write_item(index, header)?;
        }
        encoder.finish()
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

    /// Returns the single reference-counted allocation.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Starts a projection accumulator that preserves this context.
    #[must_use]
    pub fn project(&self) -> ContextProjectionAccumulator<'_> {
        ContextProjectionAccumulator { input: self }
    }

    /// Iterates all bag items in arrival order.
    pub fn items(&self) -> ContextItems<'_> {
        ContextItems {
            context: self,
            layout: self.layout().ok(),
            next: 0,
        }
    }

    /// Applies a transport-header propagation policy to the packed bag.
    pub fn propagate<'a>(&'a self, policy: &'a HeaderPropagationPolicy) -> ContextPropagation<'a> {
        ContextPropagation {
            items: self.items(),
            policy,
        }
    }

    /// Returns a present entry through its schema-local slot.
    #[must_use]
    pub fn entry(&self, slot: usize) -> Option<ContextEntry<'_>> {
        let layout = self.layout().ok()?;
        if !layout.entry_present(&self.bytes, slot)? {
            return None;
        }
        Some(ContextEntry {
            context: self,
            layout,
            descriptor: layout.entry_descriptor(&self.bytes, slot)?,
        })
    }

    fn layout(&self) -> Result<Layout, ContextBytesError> {
        Layout::read(&self.bytes).ok_or(ContextBytesError::InvalidEnvelope)
    }

    fn item_with_layout(&self, index: usize, layout: Layout) -> Option<ContextItem<'_>> {
        Some(ContextItem {
            context: self,
            layout,
            descriptor_at: layout.item_offset(index).ok()?,
        })
    }

    fn blob_bytes(&self, layout: Layout, range: BlobRange) -> Option<&[u8]> {
        range.slice(layout.blob(&self.bytes)?)
    }

    fn blob_text(&self, layout: Layout, range: BlobRange) -> Option<&str> {
        std::str::from_utf8(self.blob_bytes(layout, range)?).ok()
    }
}

/// Borrowed view of one packed context item.
#[derive(Clone, Copy, Debug)]
pub struct ContextItem<'a> {
    context: &'a PdataContextBytes,
    layout: Layout,
    descriptor_at: usize,
}

impl<'a> ContextItem<'a> {
    /// Original transport wire name.
    #[must_use]
    pub fn wire_name(&self) -> Option<&'a str> {
        self.context.blob_text(
            self.layout,
            BlobRange::read(&self.context.bytes, self.descriptor_at + ITEM_WIRE_NAME_AT)?,
        )
    }

    /// Stored name used by propagation selectors and overrides.
    #[must_use]
    pub fn stored_name(&self) -> Option<&'a str> {
        self.context.blob_text(
            self.layout,
            BlobRange::read(
                &self.context.bytes,
                self.descriptor_at + ITEM_STORED_NAME_AT,
            )?,
        )
    }

    /// Typed raw value.
    #[must_use]
    pub fn value(&self) -> Option<(HeaderValueKind, &'a [u8])> {
        Some((
            HeaderValueKind::decode(*self.context.bytes.get(self.descriptor_at + ITEM_KIND_AT)?)?,
            self.context.blob_bytes(
                self.layout,
                BlobRange::read(&self.context.bytes, self.descriptor_at + ITEM_VALUE_AT)?,
            )?,
        ))
    }

    /// Returns a text value as UTF-8.
    #[must_use]
    pub fn value_as_str(&self) -> Option<&'a str> {
        let (kind, value) = self.value()?;
        (kind == HeaderValueKind::Text)
            .then(|| std::str::from_utf8(value).ok())
            .flatten()
    }

    /// Compiled capture-rule identifier.
    #[must_use]
    pub fn rule_id(&self) -> Option<u16> {
        read_u16(&self.context.bytes, self.descriptor_at + ITEM_RULE_ID_AT)
    }

    /// Optional context-entry slot.
    #[must_use]
    pub fn entry_slot(&self) -> Option<u16> {
        let entry = read_u16(&self.context.bytes, self.descriptor_at + ITEM_ENTRY_AT)?;
        (entry != NO_ENTRY).then_some(entry)
    }
}

/// Iterator over bag items in arrival order.
pub struct ContextItems<'a> {
    context: &'a PdataContextBytes,
    layout: Option<Layout>,
    next: usize,
}

impl<'a> Iterator for ContextItems<'a> {
    type Item = ContextItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let layout = self.layout?;
        let item = self.context.item_with_layout(self.next, layout)?;
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
    layout: Layout,
    descriptor: EntryDescriptor,
}

impl ContextEntry<'_> {
    /// Returns the typed hash. Callers must compare values on a hash hit.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.descriptor.hash
    }

    /// Iterates the entry's typed values in arrival order.
    pub fn values(&self) -> impl Iterator<Item = (HeaderValueKind, &[u8])> {
        self.descriptor.members().filter_map(move |member| {
            let item = read_u16(&self.context.bytes, self.layout.member_offset(member)?)?;
            self.context
                .item_with_layout(usize::from(item), self.layout)?
                .value()
        })
    }
}

/// Projection accumulator for deriving one immutable output context.
pub struct ContextProjectionAccumulator<'a> {
    input: &'a PdataContextBytes,
}

impl ContextProjectionAccumulator<'_> {
    /// Appends one bag-only header in one new allocation.
    pub fn append_bag_header(
        self,
        header: HeaderInput<'_>,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        if header.entry.is_some() {
            return Err(ContextBytesError::TooMany {
                what: "entry-producing projections",
            });
        }

        let old = self.input.layout()?;
        let item_count = old
            .item_count
            .checked_add(1)
            .ok_or(ContextBytesError::TooMany { what: "items" })?;
        let layout = Layout::new(
            old.entry_count,
            item_count,
            old.member_count,
            old.blob_len
                .checked_add(header.encoded_len()?)
                .ok_or(ContextBytesError::TooLarge)?,
        )?;
        let mut encoder = Encoder::new(layout)?;
        encoder.copy_section(
            layout.index_section(),
            &self.input.bytes,
            old.index_section(),
        )?;
        encoder.copy_section(
            layout.items_prefix(old.item_count),
            &self.input.bytes,
            old.items_section(),
        )?;
        encoder.copy_section(
            layout.members_section(),
            &self.input.bytes,
            old.members_section(),
        )?;
        encoder.copy_section(
            layout.blob_prefix(old.blob_len),
            &self.input.bytes,
            old.blob_section(),
        )?;
        encoder.blob_cursor = old.blob_len;
        encoder.write_item(old.item_count, &header)?;
        encoder.finish()
    }
}

#[derive(Default)]
struct SkippedHeaders {
    max_entries: usize,
    name_too_long: usize,
    value_too_long: usize,
}

impl SkippedHeaders {
    fn into_stats(self) -> Option<CaptureStats> {
        (self.max_entries > 0 || self.name_too_long > 0 || self.value_too_long > 0).then_some(
            CaptureStats {
                skipped_max_entries: self.max_entries,
                skipped_name_too_long: self.name_too_long,
                skipped_value_too_long: self.value_too_long,
            },
        )
    }
}

impl HeaderInput<'_> {
    fn encoded_len(&self) -> Result<usize, ContextBytesError> {
        self.wire_name
            .len()
            .checked_add(self.stored_name.len())
            .and_then(|len| len.checked_add(self.value.len()))
            .ok_or(ContextBytesError::TooLarge)
    }
}

struct EntryIndex {
    presence: smallvec::SmallVec<[u64; 2]>,
    members: smallvec::SmallVec<[smallvec::SmallVec<[u16; 4]>; 16]>,
}

impl EntryIndex {
    fn new(entry_count: usize, headers: &[HeaderInput<'_>]) -> Result<Self, ContextBytesError> {
        if entry_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }
        if headers.len() > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "items" });
        }

        let mut presence = smallvec::SmallVec::<[u64; 2]>::from_elem(0, entry_count.div_ceil(64));
        let mut members =
            smallvec::SmallVec::<[smallvec::SmallVec<[u16; 4]>; 16]>::with_capacity(entry_count);
        members.resize_with(entry_count, smallvec::SmallVec::new);

        for (item, header) in headers.iter().enumerate() {
            let Some(entry) = header.entry.map(usize::from) else {
                continue;
            };
            if entry >= entry_count {
                return Err(ContextBytesError::InvalidEnvelope);
            }
            presence[entry / 64] |= 1u64 << (entry % 64);
            members[entry].push(
                u16::try_from(item).map_err(|_| ContextBytesError::TooMany { what: "items" })?,
            );
        }
        Ok(Self { presence, members })
    }

    fn member_count(&self) -> usize {
        self.members.iter().map(smallvec::SmallVec::len).sum()
    }

    fn write_to(
        &self,
        encoder: &mut Encoder,
        headers: &[HeaderInput<'_>],
    ) -> Result<(), ContextBytesError> {
        for (index, word) in self.presence.iter().copied().enumerate() {
            write_u64(
                &mut encoder.bytes,
                HEADER_LEN + index * size_of::<u64>(),
                word,
            )?;
        }

        let mut first_member = 0;
        for (slot, members) in self.members.iter().enumerate() {
            EntryDescriptor {
                first_member,
                member_count: members.len(),
                hash: entry_hash(
                    slot,
                    members.iter().map(|member| {
                        let header = &headers[usize::from(*member)];
                        (header.kind, header.value)
                    }),
                )?,
            }
            .write(&mut encoder.bytes, encoder.layout.entry_offset(slot)?)?;
            first_member += members.len();
        }

        let mut member = 0;
        for members in &self.members {
            for item in members {
                write_u16(
                    &mut encoder.bytes,
                    encoder
                        .layout
                        .member_offset(member)
                        .ok_or(ContextBytesError::InvalidEnvelope)?,
                    *item,
                )?;
                member += 1;
            }
        }
        Ok(())
    }
}

struct Encoder {
    bytes: Vec<u8>,
    layout: Layout,
    blob_cursor: usize,
}

impl Encoder {
    fn new(layout: Layout) -> Result<Self, ContextBytesError> {
        let mut bytes = vec![0; layout.total_len];
        layout.write_header(&mut bytes)?;
        Ok(Self {
            bytes,
            layout,
            blob_cursor: 0,
        })
    }

    fn write_item(
        &mut self,
        index: usize,
        input: &HeaderInput<'_>,
    ) -> Result<(), ContextBytesError> {
        let descriptor = ItemDescriptor {
            rule_id: input.rule_id,
            entry: input.entry,
            kind: input.kind,
            wire_name: self.append_blob(input.wire_name.as_bytes())?,
            stored_name: self.append_blob(input.stored_name.as_bytes())?,
            value: self.append_blob(input.value)?,
        };
        descriptor.write(&mut self.bytes, self.layout.item_offset(index)?)
    }

    fn append_blob(&mut self, value: &[u8]) -> Result<BlobRange, ContextBytesError> {
        let range = BlobRange {
            offset: self.blob_cursor,
            len: value.len(),
        };
        let target = range
            .absolute(self.layout.blob_at)
            .ok_or(ContextBytesError::TooLarge)?;
        self.bytes
            .get_mut(target)
            .ok_or(ContextBytesError::InvalidEnvelope)?
            .copy_from_slice(value);
        self.blob_cursor = range.end().ok_or(ContextBytesError::TooLarge)?;
        Ok(range)
    }

    fn copy_section(
        &mut self,
        target: Range<usize>,
        source: &[u8],
        source_range: Range<usize>,
    ) -> Result<(), ContextBytesError> {
        if target.len() != source_range.len() {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        self.bytes
            .get_mut(target)
            .zip(source.get(source_range))
            .ok_or(ContextBytesError::InvalidEnvelope)
            .map(|(target, source)| target.copy_from_slice(source))
    }

    fn finish(self) -> Result<PdataContextBytes, ContextBytesError> {
        if self.blob_cursor != self.layout.blob_len {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        Ok(PdataContextBytes::from_vec(self.bytes))
    }
}

#[derive(Clone, Copy, Debug)]
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
        blob_len: usize,
    ) -> Result<Self, ContextBytesError> {
        if entry_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }
        if item_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "items" });
        }
        if u32::try_from(member_count).is_err() {
            return Err(ContextBytesError::TooMany { what: "members" });
        }

        let presence_words = entry_count.div_ceil(64);
        let entry_at = table_end(HEADER_LEN, presence_words, size_of::<u64>())?;
        let item_at = table_end(entry_at, entry_count, ENTRY_DESCRIPTOR_LEN)?;
        let member_at = table_end(item_at, item_count, ITEM_DESCRIPTOR_LEN)?;
        let blob_at = table_end(member_at, member_count, size_of::<u16>())?;
        let total_len = blob_at
            .checked_add(blob_len)
            .ok_or(ContextBytesError::TooLarge)?;
        if u32::try_from(total_len).is_err() {
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
        if read_u32(bytes, HEADER_MAGIC_AT) != Some(MAGIC)
            || read_u16(bytes, HEADER_VERSION_AT) != Some(VERSION)
            || bytes
                .get(HEADER_RESERVED_AT..HEADER_LEN)
                .is_none_or(|reserved| reserved.iter().any(|byte| *byte != 0))
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }

        let presence_words = read_u16(bytes, HEADER_PRESENCE_WORDS_AT)
            .map(usize::from)
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        let blob_at = read_u32(bytes, HEADER_BLOB_OFFSET_AT)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        let layout = Self::read(bytes).ok_or(ContextBytesError::InvalidEnvelope)?;
        if presence_words != layout.presence_words
            || blob_at != layout.blob_at
            || bytes.len() != layout.total_len
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        Ok(layout)
    }

    fn read(bytes: &[u8]) -> Option<Self> {
        let entry_count = usize::from(read_u16(bytes, HEADER_ENTRY_COUNT_AT)?);
        let item_count = usize::from(read_u16(bytes, HEADER_ITEM_COUNT_AT)?);
        let member_count = usize::try_from(read_u32(bytes, HEADER_MEMBER_COUNT_AT)?).ok()?;
        let presence_words = entry_count.div_ceil(64);
        let entry_at = table_end(HEADER_LEN, presence_words, size_of::<u64>()).ok()?;
        let item_at = table_end(entry_at, entry_count, ENTRY_DESCRIPTOR_LEN).ok()?;
        let member_at = table_end(item_at, item_count, ITEM_DESCRIPTOR_LEN).ok()?;
        let blob_at = table_end(member_at, member_count, size_of::<u16>()).ok()?;
        Some(Self {
            entry_count,
            item_count,
            member_count,
            presence_words,
            entry_at,
            item_at,
            member_at,
            blob_at,
            blob_len: bytes.len().checked_sub(blob_at)?,
            total_len: bytes.len(),
        })
    }

    fn write_header(self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        write_u32(bytes, HEADER_MAGIC_AT, MAGIC)?;
        write_u16(bytes, HEADER_VERSION_AT, VERSION)?;
        write_u16(
            bytes,
            HEADER_ENTRY_COUNT_AT,
            u16::try_from(self.entry_count)
                .map_err(|_| ContextBytesError::TooMany { what: "entries" })?,
        )?;
        write_u16(
            bytes,
            HEADER_ITEM_COUNT_AT,
            u16::try_from(self.item_count)
                .map_err(|_| ContextBytesError::TooMany { what: "items" })?,
        )?;
        write_u16(
            bytes,
            HEADER_PRESENCE_WORDS_AT,
            u16::try_from(self.presence_words).map_err(|_| ContextBytesError::TooMany {
                what: "presence words",
            })?,
        )?;
        write_usize_u32(bytes, HEADER_MEMBER_COUNT_AT, self.member_count)?;
        write_usize_u32(bytes, HEADER_BLOB_OFFSET_AT, self.blob_at)
    }

    fn entry_offset(self, slot: usize) -> Result<usize, ContextBytesError> {
        table_offset(self.entry_at, slot, self.entry_count, ENTRY_DESCRIPTOR_LEN)
    }

    fn item_offset(self, index: usize) -> Result<usize, ContextBytesError> {
        table_offset(self.item_at, index, self.item_count, ITEM_DESCRIPTOR_LEN)
    }

    fn member_offset(self, index: usize) -> Option<usize> {
        (index < self.member_count).then(|| self.member_at + index * size_of::<u16>())
    }

    fn item_descriptor(self, bytes: &[u8], index: usize) -> Option<ItemDescriptor> {
        ItemDescriptor::read(bytes, self.item_offset(index).ok()?)
    }

    fn entry_descriptor(self, bytes: &[u8], slot: usize) -> Option<EntryDescriptor> {
        EntryDescriptor::read(bytes, self.entry_offset(slot).ok()?)
    }

    fn entry_present(self, bytes: &[u8], slot: usize) -> Option<bool> {
        if slot >= self.entry_count {
            return None;
        }
        let word = read_u64(bytes, HEADER_LEN + (slot / 64) * size_of::<u64>())?;
        Some(word & (1u64 << (slot % 64)) != 0)
    }

    fn blob(self, bytes: &[u8]) -> Option<&[u8]> {
        bytes.get(self.blob_section())
    }

    fn index_section(self) -> Range<usize> {
        HEADER_LEN..self.item_at
    }

    fn items_section(self) -> Range<usize> {
        self.item_at..self.member_at
    }

    fn items_prefix(self, count: usize) -> Range<usize> {
        self.item_at..self.item_at + count * ITEM_DESCRIPTOR_LEN
    }

    fn members_section(self) -> Range<usize> {
        self.member_at..self.blob_at
    }

    fn blob_section(self) -> Range<usize> {
        self.blob_at..self.total_len
    }

    fn blob_prefix(self, len: usize) -> Range<usize> {
        self.blob_at..self.blob_at + len
    }
}

#[derive(Clone, Copy, Debug)]
struct EntryDescriptor {
    first_member: usize,
    member_count: usize,
    hash: u64,
}

impl EntryDescriptor {
    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            first_member: read_u32(bytes, at + ENTRY_FIRST_MEMBER_AT)?
                .try_into()
                .ok()?,
            member_count: read_u32(bytes, at + ENTRY_MEMBER_COUNT_AT)?
                .try_into()
                .ok()?,
            hash: read_u64(bytes, at + ENTRY_HASH_AT)?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_usize_u32(bytes, at + ENTRY_FIRST_MEMBER_AT, self.first_member)?;
        write_usize_u32(bytes, at + ENTRY_MEMBER_COUNT_AT, self.member_count)?;
        write_u64(bytes, at + ENTRY_HASH_AT, self.hash)
    }

    fn members(self) -> Range<usize> {
        self.first_member..self.first_member + self.member_count
    }

    fn valid_for(self, member_count: usize) -> bool {
        self.first_member
            .checked_add(self.member_count)
            .is_some_and(|end| end <= member_count)
    }
}

#[derive(Clone, Copy, Debug)]
struct ItemDescriptor {
    rule_id: u16,
    entry: Option<u16>,
    kind: HeaderValueKind,
    wire_name: BlobRange,
    stored_name: BlobRange,
    value: BlobRange,
}

impl ItemDescriptor {
    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        if bytes
            .get(at + ITEM_RESERVED_AT..at + ITEM_WIRE_NAME_AT)?
            .iter()
            .any(|byte| *byte != 0)
        {
            return None;
        }
        let entry = read_u16(bytes, at + ITEM_ENTRY_AT)?;
        Some(Self {
            rule_id: read_u16(bytes, at + ITEM_RULE_ID_AT)?,
            entry: (entry != NO_ENTRY).then_some(entry),
            kind: HeaderValueKind::decode(*bytes.get(at + ITEM_KIND_AT)?)?,
            wire_name: BlobRange::read(bytes, at + ITEM_WIRE_NAME_AT)?,
            stored_name: BlobRange::read(bytes, at + ITEM_STORED_NAME_AT)?,
            value: BlobRange::read(bytes, at + ITEM_VALUE_AT)?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_u16(bytes, at + ITEM_RULE_ID_AT, self.rule_id)?;
        write_u16(bytes, at + ITEM_ENTRY_AT, self.entry.unwrap_or(NO_ENTRY))?;
        write_slice(bytes, at + ITEM_KIND_AT, &[self.kind as u8])?;
        self.wire_name.write(bytes, at + ITEM_WIRE_NAME_AT)?;
        self.stored_name.write(bytes, at + ITEM_STORED_NAME_AT)?;
        self.value.write(bytes, at + ITEM_VALUE_AT)
    }

    fn valid_for(self, layout: Layout, blob: &[u8]) -> bool {
        self.entry
            .is_none_or(|entry| usize::from(entry) < layout.entry_count)
            && self.wire_name.text(blob).is_some()
            && self.stored_name.text(blob).is_some()
            && self.value.slice(blob).is_some()
    }
}

#[derive(Clone, Copy, Debug)]
struct BlobRange {
    offset: usize,
    len: usize,
}

impl BlobRange {
    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            offset: read_u32(bytes, at)?.try_into().ok()?,
            len: read_u32(bytes, at + size_of::<u32>())?.try_into().ok()?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_usize_u32(bytes, at, self.offset)?;
        write_usize_u32(bytes, at + size_of::<u32>(), self.len)
    }

    fn end(self) -> Option<usize> {
        self.offset.checked_add(self.len)
    }

    fn absolute(self, blob_at: usize) -> Option<Range<usize>> {
        Some(blob_at.checked_add(self.offset)?..blob_at.checked_add(self.end()?)?)
    }

    fn slice(self, blob: &[u8]) -> Option<&[u8]> {
        blob.get(self.offset..self.end()?)
    }

    fn text(self, blob: &[u8]) -> Option<&str> {
        std::str::from_utf8(self.slice(blob)?).ok()
    }
}

fn validate(bytes: &[u8]) -> Result<(), ContextBytesError> {
    let layout = Layout::parse(bytes)?;
    validate_unused_presence_bits(bytes, layout)?;
    let blob = layout
        .blob(bytes)
        .ok_or(ContextBytesError::InvalidEnvelope)?;
    let items: Vec<_> = (0..layout.item_count)
        .map(|index| {
            layout
                .item_descriptor(bytes, index)
                .filter(|item| item.valid_for(layout, blob))
                .ok_or(ContextBytesError::InvalidEnvelope)
        })
        .collect::<Result<_, _>>()?;
    let mut indexed_items = vec![false; layout.item_count];

    for slot in 0..layout.entry_count {
        let descriptor = layout
            .entry_descriptor(bytes, slot)
            .filter(|entry| entry.valid_for(layout.member_count))
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        if layout.entry_present(bytes, slot) != Some(descriptor.member_count > 0) {
            return Err(ContextBytesError::InvalidEnvelope);
        }

        let mut values = smallvec::SmallVec::<[(HeaderValueKind, &[u8]); 4]>::new();
        for member in descriptor.members() {
            let item = read_u16(
                bytes,
                layout
                    .member_offset(member)
                    .ok_or(ContextBytesError::InvalidEnvelope)?,
            )
            .map(usize::from)
            .filter(|item| *item < items.len())
            .ok_or(ContextBytesError::InvalidEnvelope)?;
            if indexed_items[item]
                || items[item].entry
                    != Some(u16::try_from(slot).map_err(|_| ContextBytesError::InvalidEnvelope)?)
            {
                return Err(ContextBytesError::InvalidEnvelope);
            }
            indexed_items[item] = true;
            values.push((
                items[item].kind,
                items[item]
                    .value
                    .slice(blob)
                    .ok_or(ContextBytesError::InvalidEnvelope)?,
            ));
        }
        if descriptor.hash != entry_hash(slot, values)? {
            return Err(ContextBytesError::InvalidEnvelope);
        }
    }

    if items
        .iter()
        .zip(indexed_items)
        .any(|(item, indexed)| item.entry.is_some() != indexed)
    {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    Ok(())
}

fn validate_unused_presence_bits(bytes: &[u8], layout: Layout) -> Result<(), ContextBytesError> {
    let used_bits = layout.entry_count % 64;
    if used_bits == 0 || layout.presence_words == 0 {
        return Ok(());
    }
    let last_word = read_u64(
        bytes,
        HEADER_LEN + (layout.presence_words - 1) * size_of::<u64>(),
    )
    .ok_or(ContextBytesError::InvalidEnvelope)?;
    let unused_mask = !((1u64 << used_bits) - 1);
    if last_word & unused_mask != 0 {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    Ok(())
}

fn entry_hash<'a>(
    slot: usize,
    values: impl IntoIterator<Item = (HeaderValueKind, &'a [u8])>,
) -> Result<u64, ContextBytesError> {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    hash_bytes(
        &mut hash,
        &u64::try_from(slot)
            .map_err(|_| ContextBytesError::TooLarge)?
            .to_le_bytes(),
    );
    for (kind, value) in values {
        hash_bytes(&mut hash, &[kind as u8]);
        hash_bytes(
            &mut hash,
            &u32::try_from(value.len())
                .map_err(|_| ContextBytesError::TooLarge)?
                .to_le_bytes(),
        );
        hash_bytes(&mut hash, value);
    }
    Ok(hash)
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn table_end(start: usize, count: usize, width: usize) -> Result<usize, ContextBytesError> {
    count
        .checked_mul(width)
        .and_then(|len| start.checked_add(len))
        .ok_or(ContextBytesError::TooLarge)
}

fn table_offset(
    start: usize,
    index: usize,
    count: usize,
    width: usize,
) -> Result<usize, ContextBytesError> {
    if index >= count {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    table_end(start, index, width)
}

fn read_u16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(read_array(bytes, at)?))
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(read_array(bytes, at)?))
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(read_array(bytes, at)?))
}

fn read_array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

fn write_u16(bytes: &mut [u8], at: usize, value: u16) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

fn write_u32(bytes: &mut [u8], at: usize, value: u32) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

fn write_u64(bytes: &mut [u8], at: usize, value: u64) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

fn write_usize_u32(bytes: &mut [u8], at: usize, value: usize) -> Result<(), ContextBytesError> {
    write_u32(
        bytes,
        at,
        u32::try_from(value).map_err(|_| ContextBytesError::TooLarge)?,
    )
}

fn write_slice(bytes: &mut [u8], at: usize, value: &[u8]) -> Result<(), ContextBytesError> {
    bytes
        .get_mut(
            at..at
                .checked_add(value.len())
                .ok_or(ContextBytesError::TooLarge)?,
        )
        .ok_or(ContextBytesError::InvalidEnvelope)?
        .copy_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_config::transport_headers_policy::{
        CaptureDefaults, CaptureRule, HeaderCapturePolicy, PropagationDefault, PropagationMatch,
        PropagationOverride, PropagationSelector, PropagationSelectorType,
    };

    /// Scenario: an entry has duplicate typed values interleaved with a bag-only header.
    /// Guarantees: bag order is preserved and the entry resolves only its ordered members.
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

    /// Scenario: capture rules match duplicate text and binary headers while limits drop excess.
    /// Guarantees: capture preserves kinds and reports every limit violation category.
    #[test]
    fn capture_applies_policy_and_reports_limits() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults {
                max_entries: 2,
                max_name_bytes: 12,
                max_value_bytes: 4,
                ..CaptureDefaults::default()
            },
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["trace-bin".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-long-name-value".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");

        let (context, stats) = PdataContextBytes::capture(
            &policy,
            [
                ("x-long-name-value", b"ok".as_slice()),
                ("x-tenant", b"extra".as_slice()),
                ("X-Tenant", b"acme".as_slice()),
                ("trace-bin", &[0x01, 0x02]),
                ("x-tenant", b"more".as_slice()),
            ],
        )
        .expect("capture");

        let context = context.expect("captured context");
        assert_eq!(context.items().count(), 2);
        assert_eq!(
            context.items().nth(1).and_then(|item| item.value()),
            Some((HeaderValueKind::Binary, &[0x01, 0x02][..]))
        );
        assert_eq!(
            stats,
            Some(CaptureStats {
                skipped_max_entries: 1,
                skipped_name_too_long: 1,
                skipped_value_too_long: 1,
            })
        );
    }

    /// Scenario: a partition projection appends one bag-only item to an indexed context.
    /// Guarantees: existing entry hashes and values remain stable while the item is appended.
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
            output.items().nth(2).and_then(|item| item.wire_name()),
            Some("Partition")
        );
        assert_eq!(
            output.items().nth(2).and_then(|item| item.value()),
            Some((HeaderValueKind::Text, b"west".as_slice()))
        );
    }

    /// Scenario: named propagation selects one entry and drops another item by override.
    /// Guarantees: propagation applies selector, override, and stored-name semantics in place.
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

    /// Scenario: encoded bytes have corrupt metadata, ranges, names, membership, or hashes.
    /// Guarantees: every malformed envelope is rejected before it can be adopted.
    #[test]
    fn from_bytes_rejects_corrupt_envelopes() {
        let context = PdataContextBytes::build(
            1,
            [HeaderInput {
                wire_name: "x-tenant",
                stored_name: "tenant",
                value: b"acme",
                kind: HeaderValueKind::Text,
                rule_id: 0,
                entry: Some(0),
            }],
        )
        .expect("context");
        let layout = context.layout().expect("layout");

        let mut corruptions = Vec::new();
        let mut bad_magic = context.bytes().to_vec();
        bad_magic[HEADER_MAGIC_AT] = 0;
        corruptions.push(bad_magic);

        let mut bad_range = context.bytes().to_vec();
        write_u32(&mut bad_range, layout.item_at + ITEM_WIRE_NAME_AT, u32::MAX)
            .expect("corrupt range");
        corruptions.push(bad_range);

        let mut bad_name = context.bytes().to_vec();
        bad_name[layout.blob_at] = 0xff;
        corruptions.push(bad_name);

        let mut bad_member = context.bytes().to_vec();
        write_u16(&mut bad_member, layout.member_at, u16::MAX).expect("corrupt member");
        corruptions.push(bad_member);

        let mut bad_hash = context.bytes().to_vec();
        bad_hash[layout.entry_at + ENTRY_HASH_AT] ^= 1;
        corruptions.push(bad_hash);

        for bytes in corruptions {
            assert!(matches!(
                PdataContextBytes::from_bytes(Bytes::from(bytes)),
                Err(ContextBytesError::InvalidEnvelope)
            ));
        }
    }
}
