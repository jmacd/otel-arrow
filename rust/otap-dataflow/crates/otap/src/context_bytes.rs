// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context.
//!
//! - An item is one captured header: its names, value, kind, capture rule,
//!   and optional entry slot.
//! - An entry is a compiled logical slot, such as `tenant`, that groups the
//!   values of zero or more items.
//! - Presence is one bit per entry indicating whether that entry has any
//!   members, allowing absent entries to be rejected without scanning items.
//! - A member is an item-table index belonging to an entry.
//! - Counts determine the length of each fixed-size table.
//! - An offset locates bytes. The header's blob offset is envelope-relative;
//!   item name and value offsets are blob-relative.
//! - Padding aligns the item's u16 blob ranges after its one-byte kind.
//!
//! The envelope keeps fixed-size indexes before one variable-size blob:
//!
//! ```text
//! +------------------------------------------------------------------------------+
//! | envelope header (12 bytes)                                                   |
//! +------------------------------------------------------------------------------+
//! | version u16 | entries u16 | items u16 | presence words u16                   |
//! | member count u16 | blob offset u16                                            |
//! +------------------------------------------------------------------------------+
//! | entry presence bitmap (presence words * 8 bytes)                             |
//! +------------------------------------------------------------------------------+
//! | entry descriptors (entry count * 12 bytes)                                   |
//! +------------------------------------------------------------------------------+
//! | item descriptors (item count * 18 bytes, in arrival order)                   |
//! +------------------------------------------------------------------------------+
//! | entry members (member count * 2-byte item indexes)                           |
//! +------------------------------------------------------------------------------+
//! | blob: wire names, stored names, and values                                   |
//! +------------------------------------------------------------------------------+
//! ```
//!
//! Each present entry selects an ordered range from the member table. Each
//! member is an item-table index, so entries reuse the canonical item values:
//!
//! ```text
//! entry descriptor (12 bytes)
//! +------------------+------------------+----------------------------------------+
//! | first member u16 | member count u16 | typed value hash u64                   |
//! +------------------+------------------+----------------------------------------+
//!
//! item descriptor (18 bytes)
//! fixed fields (6 bytes)
//! +-------------+-----------+---------+------------------------------------------+
//! | rule id u16 | entry u16 | kind u8 | padding u8                               |
//! +-------------+-----------+---------+------------------------------------------+
//! blob ranges (3 * 4 bytes)
//! +----------------+---------------------------+---------------------------------+
//! | wire name      | blob offset u16           | byte length u16                 |
//! | stored name    | blob offset u16           | byte length u16                 |
//! | value          | blob offset u16           | byte length u16                 |
//! +----------------+---------------------------+---------------------------------+
//! ```
//!
//! Item ranges are relative to `blob offset`. `NO_ENTRY` marks bag-only items;
//! the presence bitmap makes absent entry lookup constant-time.

use std::{fmt, marker::PhantomData, ops::Range};

use bytes::Bytes;
use otap_df_config::transport_headers_policy::{
    CaptureStats, CompiledHeaderCapturePolicy, CompiledHeaderPropagationPolicy, NameStrategy,
    PropagationAction, ValueKindConfig,
};

const VERSION: u16 = 3;
const NO_ENTRY: u16 = u16::MAX;
const MAX_CONTEXT_LEN: usize = u16::MAX as usize;

trait Scalar: Copy {
    const WIDTH: usize;

    fn read(bytes: &[u8], at: usize) -> Option<Self>;
    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError>;
}

impl Scalar for u8 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        bytes.get(at).copied()
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_slice(bytes, at, &[self])
    }
}

impl Scalar for u16 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        read_u16(bytes, at)
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_u16(bytes, at, self)
    }
}

impl Scalar for u64 {
    const WIDTH: usize = size_of::<Self>();

    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        read_u64(bytes, at)
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        write_u64(bytes, at, self)
    }
}

#[derive(Clone, Copy)]
struct Field<T> {
    offset: usize,
    value: PhantomData<fn() -> T>,
}

impl<T: Scalar> Field<T> {
    const fn new(offset: usize) -> Self {
        Self {
            offset,
            value: PhantomData,
        }
    }

    const fn end(self) -> usize {
        self.offset + T::WIDTH
    }

    const fn at(self, base: usize) -> usize {
        base + self.offset
    }

    fn read(self, bytes: &[u8], base: usize) -> Option<T> {
        T::read(bytes, self.at(base))
    }

    fn write(self, bytes: &mut [u8], base: usize, value: T) -> Result<(), ContextBytesError> {
        value.write(bytes, self.at(base))
    }
}

impl Field<u16> {
    fn write_usize(
        self,
        bytes: &mut [u8],
        base: usize,
        value: usize,
    ) -> Result<(), ContextBytesError> {
        self.write(
            bytes,
            base,
            u16::try_from(value).map_err(|_| ContextBytesError::TooLarge)?,
        )
    }
}

type U8Field = Field<u8>;
type U16Field = Field<u16>;
type U64Field = Field<u64>;

#[derive(Clone, Copy)]
struct ByteSpan {
    offset: usize,
    len: usize,
}

impl ByteSpan {
    const fn new(offset: usize, len: usize) -> Self {
        Self { offset, len }
    }

    const fn end(self) -> usize {
        self.offset + self.len
    }

    fn is_zero(self, bytes: &[u8], base: usize) -> Option<bool> {
        Some(
            bytes
                .get(base + self.offset..base + self.end())?
                .iter()
                .all(|byte| *byte == 0),
        )
    }
}

#[derive(Clone, Copy)]
struct BlobRangeField {
    offset: U16Field,
    len: U16Field,
}

impl BlobRangeField {
    const fn new(offset: usize) -> Self {
        let offset = U16Field::new(offset);
        Self {
            len: U16Field::new(offset.end()),
            offset,
        }
    }

    const fn end(self) -> usize {
        self.len.end()
    }

    fn read(self, bytes: &[u8], base: usize) -> Option<BlobRange> {
        Some(BlobRange {
            offset: usize::from(self.offset.read(bytes, base)?),
            len: usize::from(self.len.read(bytes, base)?),
        })
    }

    fn write(
        self,
        bytes: &mut [u8],
        base: usize,
        range: BlobRange,
    ) -> Result<(), ContextBytesError> {
        self.offset.write_usize(bytes, base, range.offset)?;
        self.len.write_usize(bytes, base, range.len)
    }
}

struct HeaderFields;

impl HeaderFields {
    const VERSION: U16Field = U16Field::new(0);
    const ENTRY_COUNT: U16Field = U16Field::new(Self::VERSION.end());
    const ITEM_COUNT: U16Field = U16Field::new(Self::ENTRY_COUNT.end());
    const PRESENCE_WORDS: U16Field = U16Field::new(Self::ITEM_COUNT.end());
    const MEMBER_COUNT: U16Field = U16Field::new(Self::PRESENCE_WORDS.end());
    const BLOB_OFFSET: U16Field = U16Field::new(Self::MEMBER_COUNT.end());
    const LEN: usize = Self::BLOB_OFFSET.end();
}

struct EntryFields;

impl EntryFields {
    const FIRST_MEMBER: U16Field = U16Field::new(0);
    const MEMBER_COUNT: U16Field = U16Field::new(Self::FIRST_MEMBER.end());
    const HASH: U64Field = U64Field::new(Self::MEMBER_COUNT.end());
    const LEN: usize = Self::HASH.end();
}

struct ItemFields;

impl ItemFields {
    const RULE_ID: U16Field = U16Field::new(0);
    const ENTRY: U16Field = U16Field::new(Self::RULE_ID.end());
    const KIND: U8Field = U8Field::new(Self::ENTRY.end());
    const PADDING: ByteSpan = ByteSpan::new(Self::KIND.end(), 1);
    const WIRE_NAME: BlobRangeField = BlobRangeField::new(Self::PADDING.end());
    const STORED_NAME: BlobRangeField = BlobRangeField::new(Self::WIRE_NAME.end());
    const VALUE: BlobRangeField = BlobRangeField::new(Self::STORED_NAME.end());
    const LEN: usize = Self::VALUE.end();
}

const _: () = {
    assert!(HeaderFields::LEN == 12);
    assert!(EntryFields::LEN == 12);
    assert!(ItemFields::LEN == 18);
};

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
#[derive(Clone, Copy)]
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

impl fmt::Debug for HeaderInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeaderInput")
            .field("wire_name", &self.wire_name)
            .field("stored_name", &self.stored_name)
            .field("value_len", &self.value.len())
            .field("kind", &self.kind)
            .field("rule_id", &self.rule_id)
            .field("entry", &self.entry)
            .finish()
    }
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
#[derive(Clone, PartialEq, Eq)]
pub struct PdataContextBytes {
    bytes: Bytes,
}

impl fmt::Debug for PdataContextBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PdataContextBytes")
            .field("byte_len", &self.bytes.len())
            .finish_non_exhaustive()
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
        let mut skipped = SkippedHeaders::default();
        let mut encoded_len = HeaderFields::LEN
            + policy.entry_count().div_ceil(64) * size_of::<u64>()
            + policy.entry_count() * EntryFields::LEN;

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
            let header = HeaderInput {
                wire_name,
                stored_name: matched.stored_name,
                value,
                kind: HeaderValueKind::captured(matched.value_kind, wire_name),
                rule_id: matched.rule_id,
                entry: matched.entry,
            };
            let added_len = ItemFields::LEN
                + usize::from(header.entry.is_some()) * size_of::<u16>()
                + header.encoded_len()?;
            if encoded_len
                .checked_add(added_len)
                .is_none_or(|len| len > MAX_CONTEXT_LEN)
            {
                skipped.context_too_large += 1;
                continue;
            }
            encoded_len += added_len;
            headers.push(header);
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

    /// Scans the bag and applies a transport-header propagation policy.
    pub fn propagate<'a>(
        &'a self,
        policy: &'a CompiledHeaderPropagationPolicy,
    ) -> ContextPropagation<'a> {
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
        self.text(ItemFields::WIRE_NAME)
    }

    /// Stored name used by propagation selectors and overrides.
    #[must_use]
    pub fn stored_name(&self) -> Option<&'a str> {
        self.text(ItemFields::STORED_NAME)
    }

    /// Typed raw value.
    #[must_use]
    pub fn value(&self) -> Option<(HeaderValueKind, &'a [u8])> {
        Some((
            HeaderValueKind::decode(
                ItemFields::KIND.read(&self.context.bytes, self.descriptor_at)?,
            )?,
            self.bytes(ItemFields::VALUE)?,
        ))
    }

    /// Compiled capture-rule identifier.
    #[must_use]
    pub fn rule_id(&self) -> Option<u16> {
        ItemFields::RULE_ID.read(&self.context.bytes, self.descriptor_at)
    }

    /// Optional context-entry slot.
    #[must_use]
    pub fn entry_slot(&self) -> Option<u16> {
        let entry = ItemFields::ENTRY.read(&self.context.bytes, self.descriptor_at)?;
        (entry != NO_ENTRY).then_some(entry)
    }

    fn bytes(&self, field: BlobRangeField) -> Option<&'a [u8]> {
        self.context.blob_bytes(
            self.layout,
            field.read(&self.context.bytes, self.descriptor_at)?,
        )
    }

    fn text(&self, field: BlobRangeField) -> Option<&'a str> {
        std::str::from_utf8(self.bytes(field)?).ok()
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
    policy: &'a CompiledHeaderPropagationPolicy,
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
    /// Copies the envelope and appends one bag-only header in one new allocation.
    pub fn copy_and_append_bag_header(
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
    context_too_large: usize,
}

impl SkippedHeaders {
    fn into_stats(self) -> Option<CaptureStats> {
        (self.max_entries > 0
            || self.name_too_long > 0
            || self.value_too_long > 0
            || self.context_too_large > 0)
            .then_some(CaptureStats {
                skipped_max_entries: self.max_entries,
                skipped_name_too_long: self.name_too_long,
                skipped_value_too_long: self.value_too_long,
                skipped_context_too_large: self.context_too_large,
            })
    }
}

impl HeaderInput<'_> {
    fn encoded_len(&self) -> Result<usize, ContextBytesError> {
        let stored_name_len = if self.names_share_blob() {
            0
        } else {
            self.stored_name.len()
        };
        self.wire_name
            .len()
            .checked_add(stored_name_len)
            .and_then(|len| len.checked_add(self.value.len()))
            .ok_or(ContextBytesError::TooLarge)
    }

    fn names_share_blob(&self) -> bool {
        self.wire_name == self.stored_name
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
                HeaderFields::LEN + index * size_of::<u64>(),
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
        let wire_name = self.append_blob(input.wire_name.as_bytes())?;
        let stored_name = if input.names_share_blob() {
            wire_name
        } else {
            self.append_blob(input.stored_name.as_bytes())?
        };
        let descriptor = ItemDescriptor {
            rule_id: input.rule_id,
            entry: input.entry,
            kind: input.kind,
            wire_name,
            stored_name,
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
struct TableOffsets {
    presence_words: usize,
    entry_at: usize,
    item_at: usize,
    member_at: usize,
    blob_at: usize,
}

impl TableOffsets {
    fn calculate(entry_count: usize, item_count: usize, member_count: usize) -> Option<Self> {
        let presence_words = entry_count.div_ceil(64);
        let entry_at = table_end(HeaderFields::LEN, presence_words, size_of::<u64>()).ok()?;
        let item_at = table_end(entry_at, entry_count, EntryFields::LEN).ok()?;
        let member_at = table_end(item_at, item_count, ItemFields::LEN).ok()?;
        let blob_at = table_end(member_at, member_count, size_of::<u16>()).ok()?;
        Some(Self {
            presence_words,
            entry_at,
            item_at,
            member_at,
            blob_at,
        })
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
        if u16::try_from(member_count).is_err() {
            return Err(ContextBytesError::TooMany { what: "members" });
        }

        let layout = Self::calculate(entry_count, item_count, member_count, blob_len)
            .ok_or(ContextBytesError::TooLarge)?;
        if layout.total_len > MAX_CONTEXT_LEN {
            return Err(ContextBytesError::TooLarge);
        }
        Ok(layout)
    }

    fn parse(bytes: &[u8]) -> Result<Self, ContextBytesError> {
        if HeaderFields::VERSION.read(bytes, 0) != Some(VERSION) {
            return Err(ContextBytesError::InvalidEnvelope);
        }

        let presence_words = HeaderFields::PRESENCE_WORDS
            .read(bytes, 0)
            .map(usize::from)
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        let blob_at = HeaderFields::BLOB_OFFSET
            .read(bytes, 0)
            .map(usize::from)
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
        let entry_count = usize::from(HeaderFields::ENTRY_COUNT.read(bytes, 0)?);
        let item_count = usize::from(HeaderFields::ITEM_COUNT.read(bytes, 0)?);
        let member_count = usize::from(HeaderFields::MEMBER_COUNT.read(bytes, 0)?);
        let offsets = TableOffsets::calculate(entry_count, item_count, member_count)?;
        Self::from_offsets(
            entry_count,
            item_count,
            member_count,
            bytes.len().checked_sub(offsets.blob_at)?,
            offsets,
        )
    }

    fn calculate(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        blob_len: usize,
    ) -> Option<Self> {
        Self::from_offsets(
            entry_count,
            item_count,
            member_count,
            blob_len,
            TableOffsets::calculate(entry_count, item_count, member_count)?,
        )
    }

    fn from_offsets(
        entry_count: usize,
        item_count: usize,
        member_count: usize,
        blob_len: usize,
        offsets: TableOffsets,
    ) -> Option<Self> {
        Some(Self {
            entry_count,
            item_count,
            member_count,
            presence_words: offsets.presence_words,
            entry_at: offsets.entry_at,
            item_at: offsets.item_at,
            member_at: offsets.member_at,
            blob_at: offsets.blob_at,
            blob_len,
            total_len: offsets.blob_at.checked_add(blob_len)?,
        })
    }

    fn write_header(self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        HeaderFields::VERSION.write(bytes, 0, VERSION)?;
        HeaderFields::ENTRY_COUNT.write_usize(bytes, 0, self.entry_count)?;
        HeaderFields::ITEM_COUNT.write_usize(bytes, 0, self.item_count)?;
        HeaderFields::PRESENCE_WORDS.write_usize(bytes, 0, self.presence_words)?;
        HeaderFields::MEMBER_COUNT.write_usize(bytes, 0, self.member_count)?;
        HeaderFields::BLOB_OFFSET.write_usize(bytes, 0, self.blob_at)
    }

    fn entry_offset(self, slot: usize) -> Result<usize, ContextBytesError> {
        table_offset(self.entry_at, slot, self.entry_count, EntryFields::LEN)
    }

    fn item_offset(self, index: usize) -> Result<usize, ContextBytesError> {
        table_offset(self.item_at, index, self.item_count, ItemFields::LEN)
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
        let word = read_u64(bytes, HeaderFields::LEN + (slot / 64) * size_of::<u64>())?;
        Some(word & (1u64 << (slot % 64)) != 0)
    }

    fn blob(self, bytes: &[u8]) -> Option<&[u8]> {
        bytes.get(self.blob_section())
    }

    fn index_section(self) -> Range<usize> {
        HeaderFields::LEN..self.item_at
    }

    fn items_section(self) -> Range<usize> {
        self.item_at..self.member_at
    }

    fn items_prefix(self, count: usize) -> Range<usize> {
        self.item_at..self.item_at + count * ItemFields::LEN
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
            first_member: usize::from(EntryFields::FIRST_MEMBER.read(bytes, at)?),
            member_count: usize::from(EntryFields::MEMBER_COUNT.read(bytes, at)?),
            hash: EntryFields::HASH.read(bytes, at)?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        EntryFields::FIRST_MEMBER.write_usize(bytes, at, self.first_member)?;
        EntryFields::MEMBER_COUNT.write_usize(bytes, at, self.member_count)?;
        EntryFields::HASH.write(bytes, at, self.hash)
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
        if !ItemFields::PADDING.is_zero(bytes, at)? {
            return None;
        }
        let entry = ItemFields::ENTRY.read(bytes, at)?;
        Some(Self {
            rule_id: ItemFields::RULE_ID.read(bytes, at)?,
            entry: (entry != NO_ENTRY).then_some(entry),
            kind: HeaderValueKind::decode(ItemFields::KIND.read(bytes, at)?)?,
            wire_name: ItemFields::WIRE_NAME.read(bytes, at)?,
            stored_name: ItemFields::STORED_NAME.read(bytes, at)?,
            value: ItemFields::VALUE.read(bytes, at)?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        ItemFields::RULE_ID.write(bytes, at, self.rule_id)?;
        ItemFields::ENTRY.write(bytes, at, self.entry.unwrap_or(NO_ENTRY))?;
        ItemFields::KIND.write(bytes, at, self.kind as u8)?;
        ItemFields::WIRE_NAME.write(bytes, at, self.wire_name)?;
        ItemFields::STORED_NAME.write(bytes, at, self.stored_name)?;
        ItemFields::VALUE.write(bytes, at, self.value)
    }

    fn valid_for(self, layout: Layout, blob: &[u8]) -> bool {
        self.entry
            .is_none_or(|entry| usize::from(entry) < layout.entry_count)
            && self.wire_name.text(blob).is_some()
            && self.stored_name.text(blob).is_some()
            && self.value.slice(blob).is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlobRange {
    offset: usize,
    len: usize,
}

impl BlobRange {
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
        HeaderFields::LEN + (layout.presence_words - 1) * size_of::<u64>(),
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
            &u16::try_from(value.len())
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

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(read_array(bytes, at)?))
}

fn read_array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

fn write_u16(bytes: &mut [u8], at: usize, value: u16) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
}

fn write_u64(bytes: &mut [u8], at: usize, value: u64) -> Result<(), ContextBytesError> {
    write_slice(bytes, at, &value.to_le_bytes())
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
        CaptureDefaults, CaptureRule, HeaderCapturePolicy, HeaderPropagationPolicy,
        PropagationDefault, PropagationMatch, PropagationOverride, PropagationSelector,
        PropagationSelectorType,
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
            context.bytes.len(),
            Layout::parse(&context.bytes).unwrap().total_len
        );
    }

    /// Scenario: an arriving header already uses its normalized stored name.
    /// Guarantees: wire and stored descriptors share one blob range and one name copy.
    #[test]
    fn normalized_names_share_blob_storage() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile()
        .expect("capture policy");
        let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let layout = context.layout().expect("layout");
        let item = layout
            .item_descriptor(&context.bytes, 0)
            .expect("item descriptor");

        assert_eq!(item.wire_name, item.stored_name);
        assert_eq!(
            context.bytes.len(),
            HeaderFields::LEN + ItemFields::LEN + "x-tenant".len() + b"acme".len()
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
                skipped_context_too_large: 0,
            })
        );
    }

    /// Scenario: a direct build exactly fills the 65,535-byte context envelope.
    /// Guarantees: the u16 format accepts its boundary and rejects one additional byte.
    #[test]
    fn build_enforces_u16_context_size() {
        let value = vec![0; MAX_CONTEXT_LEN - HeaderFields::LEN - ItemFields::LEN - 2];
        let context = PdataContextBytes::build(
            0,
            [HeaderInput {
                wire_name: "w",
                stored_name: "s",
                value: &value,
                kind: HeaderValueKind::Binary,
                rule_id: 0,
                entry: None,
            }],
        )
        .expect("maximum-size context");
        assert_eq!(context.bytes.len(), MAX_CONTEXT_LEN);

        let oversized = vec![0; value.len() + 1];
        assert!(matches!(
            PdataContextBytes::build(
                0,
                [HeaderInput {
                    wire_name: "w",
                    stored_name: "s",
                    value: &oversized,
                    kind: HeaderValueKind::Binary,
                    rule_id: 0,
                    entry: None,
                }]
            ),
            Err(ContextBytesError::TooLarge)
        ));
    }

    /// Scenario: individually valid captured headers would exceed the 64 KiB envelope.
    /// Guarantees: capture drops only the overflowing header and reports the aggregate limit.
    #[test]
    fn capture_drops_header_that_exceeds_context_size() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile()
        .expect("capture policy");
        let value = vec![0; CaptureDefaults::default().max_value_bytes];
        let pairs = (0..16).map(|_| ("x", value.as_slice()));

        let (context, stats) = PdataContextBytes::capture(&policy, pairs).expect("capture");

        assert_eq!(context.expect("context").items().count(), 15);
        assert_eq!(
            stats,
            Some(CaptureStats {
                skipped_max_entries: 0,
                skipped_name_too_long: 0,
                skipped_value_too_long: 0,
                skipped_context_too_large: 1,
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
            .copy_and_append_bag_header(HeaderInput {
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
        )
        .compile()
        .expect("propagation policy");

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
        let mut bad_version = context.bytes.to_vec();
        bad_version[HeaderFields::VERSION.at(0)] = 0;
        corruptions.push(bad_version);

        let mut bad_range = context.bytes.to_vec();
        ItemFields::WIRE_NAME
            .offset
            .write(&mut bad_range, layout.item_at, u16::MAX)
            .expect("corrupt range");
        corruptions.push(bad_range);

        let mut bad_name = context.bytes.to_vec();
        bad_name[layout.blob_at] = 0xff;
        corruptions.push(bad_name);

        let mut bad_member = context.bytes.to_vec();
        write_u16(&mut bad_member, layout.member_at, u16::MAX).expect("corrupt member");
        corruptions.push(bad_member);

        let mut bad_hash = context.bytes.to_vec();
        bad_hash[EntryFields::HASH.at(layout.entry_at)] ^= 1;
        corruptions.push(bad_hash);

        for bytes in corruptions {
            assert!(matches!(
                PdataContextBytes::from_bytes(Bytes::from(bytes)),
                Err(ContextBytesError::InvalidEnvelope)
            ));
        }
    }
}
