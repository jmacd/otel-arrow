use std::{
    fmt,
    marker::PhantomData,
    ops::{Deref, Range},
    sync::Arc,
};

use otel_arrow_dfe_config::context::{
    CompiledContext, ContextFieldId, ContextRecordShape, ContextRegisterId, ContextRegisterShape,
    ContextScalarType,
};
use otel_arrow_dfe_config::transport_headers_policy::{
    CaptureStats, CompiledHeaderCapturePolicy, CompiledHeaderSchema, CompiledHeaderSchemaItemRef,
    CompiledOutputName, CompiledSchemaPropagation, PropagationAction, ValueKindConfig,
};
use tonic::metadata::{KeyAndValueRef, MetadataMap};

// --- types ---

/// Scalar value representation preserved in the item descriptor.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ContextValueKind {
    /// UTF-8 text.
    Text = 0,
    /// Arbitrary bytes.
    Binary = 1,
}

impl ContextValueKind {
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

/// Compatibility name for the former transport-oriented value kind.
pub type HeaderValueKind = ContextValueKind;

/// One value assigned to a compiled field in a record register.
#[derive(Clone, Copy, Debug)]
pub struct ContextRecordValue<'a> {
    field: ContextFieldId,
    kind: ContextValueKind,
    value: &'a [u8],
}

impl<'a> ContextRecordValue<'a> {
    /// Creates a value for one compiled record field.
    #[must_use]
    pub const fn new(field: ContextFieldId, kind: ContextValueKind, value: &'a [u8]) -> Self {
        Self { field, kind, value }
    }

    /// Returns the record-local compiled field position.
    #[must_use]
    pub const fn field(&self) -> ContextFieldId {
        self.field
    }

    /// Returns the encoded scalar kind.
    #[must_use]
    pub const fn kind(&self) -> ContextValueKind {
        self.kind
    }

    /// Returns the raw scalar bytes.
    #[must_use]
    pub const fn value(&self) -> &'a [u8] {
        self.value
    }
}

/// Failure while constructing or validating a context envelope.
#[derive(Debug, thiserror::Error)]
#[allow(variant_size_differences)]
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
    /// A producer attempted to construct a record in a non-record register.
    #[error("context register {register:?} is not a record")]
    NotARecord {
        /// Register supplied by the producer.
        register: ContextRegisterId,
    },
    /// A producer attempted to materialize a present register without values.
    #[error("context register {register:?} has no values")]
    EmptyRegisterValue {
        /// Empty register supplied by the producer.
        register: ContextRegisterId,
    },
    /// A producer supplied a field outside the compiled record shape.
    #[error("field {field:?} does not exist in context register {register:?}")]
    InvalidRecordField {
        /// Record register being constructed.
        register: ContextRegisterId,
        /// Invalid record-local field.
        field: ContextFieldId,
    },
    /// A producer supplied more than one value for a scalar record field.
    #[error("scalar field {field:?} occurs more than once in context register {register:?}")]
    DuplicateRecordField {
        /// Record register being constructed.
        register: ContextRegisterId,
        /// Repeated scalar field.
        field: ContextFieldId,
    },
    /// A producer supplied bytes that do not match a field's compiled scalar type.
    #[error("value for field {field:?} does not match its compiled scalar type")]
    RecordFieldTypeMismatch {
        /// Field whose value did not match.
        field: ContextFieldId,
    },
}

// --- primitives ---

const VERSION: u16 = 7;
const MAX_CONTEXT_LEN: usize = u16::MAX as usize;
const NO_FIELD: u16 = u16::MAX;
const NO_SCHEMA_ITEM: u16 = u16::MAX;

struct PresenceBitmap;

impl PresenceBitmap {
    const WORD_BITS: usize = u64::BITS as usize;

    const fn word_count(register_count: usize) -> usize {
        register_count.div_ceil(Self::WORD_BITS)
    }

    const fn word_index(register: usize) -> usize {
        register / Self::WORD_BITS
    }

    const fn mask(register: usize) -> u64 {
        1u64 << (register % Self::WORD_BITS)
    }

    fn set_words(words: &mut [u64], register: usize) -> Result<(), ContextBytesError> {
        let word = words
            .get_mut(Self::word_index(register))
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        *word |= Self::mask(register);
        Ok(())
    }

    fn is_set_encoded(bytes: &[u8], register: usize) -> Option<bool> {
        let word = read_u64(bytes, Self::word_index(register) * size_of::<u64>())?;
        Some(word & Self::mask(register) != 0)
    }

    fn set_encoded(bytes: &mut [u8], register: usize) -> Result<(), ContextBytesError> {
        let at = Self::word_index(register) * size_of::<u64>();
        let word = read_u64(bytes, at).ok_or(ContextBytesError::InvalidEnvelope)?;
        write_u64(bytes, at, word | Self::mask(register))
    }
}

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

// --- layout ---

struct HeaderFields;

impl HeaderFields {
    const VERSION: U16Field = U16Field::new(0);
    const ENTRY_COUNT: U16Field = U16Field::new(Self::VERSION.end());
    const ITEM_COUNT: U16Field = U16Field::new(Self::ENTRY_COUNT.end());
    const MEMBER_COUNT: U16Field = U16Field::new(Self::ITEM_COUNT.end());
    const LEN: usize = Self::MEMBER_COUNT.end();
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
    const SCHEMA_INDEX: U16Field = U16Field::new(0);
    const KIND: U8Field = U8Field::new(Self::SCHEMA_INDEX.end());
    const _PAD: U8Field = U8Field::new(Self::KIND.end());
    const WIRE_NAME: BlobRangeField = BlobRangeField::new(Self::_PAD.end());
    const VALUE: BlobRangeField = BlobRangeField::new(Self::WIRE_NAME.end());
    const LEN: usize = Self::VALUE.end();
}

struct MemberFields;

impl MemberFields {
    const ITEM: U16Field = U16Field::new(0);
    const FIELD: U16Field = U16Field::new(Self::ITEM.end());
    const LEN: usize = Self::FIELD.end();
}

const _: () = {
    assert!(HeaderFields::LEN == 8);
    assert!(EntryFields::LEN == 12);
    assert!(ItemFields::LEN == 12);
    assert!(MemberFields::LEN == 4);
};

#[derive(Clone, Copy, Debug)]
struct TableOffsets {
    entry_at: usize,
    item_at: usize,
    member_at: usize,
    blob_at: usize,
}

impl TableOffsets {
    fn calculate(entry_count: usize, item_count: usize, member_count: usize) -> Option<Self> {
        let presence_words = PresenceBitmap::word_count(entry_count);
        let entry_at = table_end(HeaderFields::LEN, presence_words, size_of::<u64>()).ok()?;
        let item_at = table_end(entry_at, entry_count, EntryFields::LEN).ok()?;
        let member_at = table_end(item_at, item_count, ItemFields::LEN).ok()?;
        let blob_at = table_end(member_at, member_count, MemberFields::LEN).ok()?;
        Some(Self {
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
    offsets: TableOffsets,
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

    fn read(bytes: &[u8]) -> Option<Self> {
        if HeaderFields::VERSION.read(bytes, 0)? != VERSION {
            return None;
        }
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
            offsets,
            blob_len,
            total_len: offsets.blob_at.checked_add(blob_len)?,
        })
    }

    fn write_header(self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        HeaderFields::VERSION.write(bytes, 0, VERSION)?;
        for (field, value) in [
            (HeaderFields::ENTRY_COUNT, self.entry_count),
            (HeaderFields::ITEM_COUNT, self.item_count),
            (HeaderFields::MEMBER_COUNT, self.member_count),
        ] {
            field.write_usize(bytes, 0, value)?;
        }
        Ok(())
    }

    fn entry_offset(self, slot: usize) -> Result<usize, ContextBytesError> {
        table_offset(
            self.offsets.entry_at,
            slot,
            self.entry_count,
            EntryFields::LEN,
        )
    }

    fn item_offset(self, index: usize) -> Result<usize, ContextBytesError> {
        table_offset(
            self.offsets.item_at,
            index,
            self.item_count,
            ItemFields::LEN,
        )
    }

    fn member_offset(self, index: usize) -> Option<usize> {
        (index < self.member_count).then(|| self.offsets.member_at + index * MemberFields::LEN)
    }

    #[cfg(test)]
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
        PresenceBitmap::is_set_encoded(bytes.get(self.presence_section())?, slot)
    }

    fn blob(self, bytes: &[u8]) -> Option<&[u8]> {
        bytes.get(self.blob_section())
    }

    fn presence_section(self) -> Range<usize> {
        HeaderFields::LEN..self.offsets.entry_at
    }

    fn entries_section(self) -> Range<usize> {
        self.offsets.entry_at..self.offsets.item_at
    }

    fn items_section(self) -> Range<usize> {
        self.offsets.item_at..self.offsets.member_at
    }

    fn members_section(self) -> Range<usize> {
        self.offsets.member_at..self.offsets.blob_at
    }

    fn blob_section(self) -> Range<usize> {
        self.offsets.blob_at..self.total_len
    }
}

// --- writer ---

#[derive(Clone, Copy, PartialEq, Eq)]
enum EnvelopeSection {
    Presence,
    Entries,
    Items,
    Members,
    Blob,
    Complete,
}

struct EnvelopeWriter {
    layout: Layout,
    bytes: Vec<u8>,
    next: EnvelopeSection,
}

impl EnvelopeWriter {
    fn new(layout: Layout) -> Result<Self, ContextBytesError> {
        let mut bytes = vec![0; layout.total_len];
        layout.write_header(&mut bytes)?;
        Ok(Self {
            layout,
            bytes,
            next: EnvelopeSection::Presence,
        })
    }

    fn presence(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Presence,
            EnvelopeSection::Entries,
            self.layout.presence_section(),
            write,
        )
    }

    fn entries(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Entries,
            EnvelopeSection::Items,
            self.layout.entries_section(),
            write,
        )
    }

    fn items(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Items,
            EnvelopeSection::Members,
            self.layout.items_section(),
            write,
        )
    }

    fn members(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Members,
            EnvelopeSection::Blob,
            self.layout.members_section(),
            write,
        )
    }

    fn blob(
        &mut self,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        self.write_section(
            EnvelopeSection::Blob,
            EnvelopeSection::Complete,
            self.layout.blob_section(),
            write,
        )
    }

    fn finish(self) -> Result<Vec<u8>, ContextBytesError> {
        if self.next != EnvelopeSection::Complete || self.bytes.len() != self.layout.total_len {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        Ok(self.bytes)
    }

    fn write_section(
        &mut self,
        expected: EnvelopeSection,
        next: EnvelopeSection,
        range: Range<usize>,
        write: impl FnOnce(&mut [u8]) -> Result<(), ContextBytesError>,
    ) -> Result<(), ContextBytesError> {
        if self.next != expected {
            return Err(ContextBytesError::InvalidEnvelope);
        }
        write(
            self.bytes
                .get_mut(range)
                .ok_or(ContextBytesError::InvalidEnvelope)?,
        )?;
        self.next = next;
        Ok(())
    }
}

// --- capture ---

enum CapturedValue<'a> {
    Borrowed(&'a [u8]),
    Owned(bytes::Bytes),
}

impl AsRef<[u8]> for CapturedValue<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(value) => value,
            Self::Owned(value) => value,
        }
    }
}

struct CapturedHeader<'a, V> {
    // Name observed on the transport.
    wire_name: &'a str,
    // Borrowed or owned raw header bytes.
    value: V,
    // Text or binary transport semantics.
    kind: HeaderValueKind,
    // Index of the ingress instruction and register in the retained schema.
    schema_index: u16,
}

impl<'a, V> CapturedHeader<'a, V> {
    fn schema_item<'s>(
        &self,
        schema: &'s CompiledHeaderSchema,
    ) -> Result<CompiledHeaderSchemaItemRef<'s>, ContextBytesError> {
        schema
            .item(self.schema_index)
            .ok_or(ContextBytesError::InvalidEnvelope)
    }

    fn wire_name_occurrence(
        &self,
        schema_item: CompiledHeaderSchemaItemRef<'_>,
    ) -> Option<&'a str> {
        (schema_item.retain_observed_name && schema_item.wire_name != self.wire_name)
            .then_some(self.wire_name)
    }
}

impl<V: AsRef<[u8]>> CapturedHeader<'_, V> {
    fn entry(&self, schema: &CompiledHeaderSchema) -> Result<Option<u16>, ContextBytesError> {
        Ok(self.schema_item(schema)?.register.map(|id| id.as_u16()))
    }

    fn encoded_len(&self, schema: &CompiledHeaderSchema) -> Result<usize, ContextBytesError> {
        let schema_item = self.schema_item(schema)?;
        let wire_name_len = self.wire_name_occurrence(schema_item).map_or(0, str::len);
        wire_name_len
            .checked_add(self.value.as_ref().len())
            .ok_or(ContextBytesError::TooLarge)
    }

    fn write_blob(
        &self,
        schema_item: CompiledHeaderSchemaItemRef<'_>,
        bytes: &mut [u8],
        cursor: &mut usize,
    ) -> Result<(), ContextBytesError> {
        if let Some(wire_name) = self.wire_name_occurrence(schema_item) {
            write_slice(bytes, *cursor, wire_name.as_bytes())?;
            *cursor = cursor
                .checked_add(wire_name.len())
                .ok_or(ContextBytesError::TooLarge)?;
        }
        write_slice(bytes, *cursor, self.value.as_ref())?;
        *cursor = cursor
            .checked_add(self.value.as_ref().len())
            .ok_or(ContextBytesError::TooLarge)?;
        Ok(())
    }
}

/// Failure while constructing or validating a context envelope.
impl PdataContextBytes {
    /// Captures headers with a compiled policy.
    pub fn capture<'a>(
        policy: &CompiledHeaderCapturePolicy,
        pairs: impl IntoIterator<Item = (&'a str, &'a [u8])>,
    ) -> Result<(Option<Self>, Option<CaptureStats>), ContextBytesError> {
        let candidates = pairs.into_iter().filter_map(|(wire_name, value)| {
            let matched = policy.match_header(wire_name)?;
            Some(CapturedHeader {
                wire_name,
                value,
                kind: HeaderValueKind::captured(matched.schema_item.value_kind, wire_name),
                schema_index: matched.schema_index,
            })
        });
        Self::capture_candidates(policy, candidates)
    }

    /// Captures gRPC metadata after screening names with the compiled policy.
    pub fn capture_grpc_metadata(
        policy: &CompiledHeaderCapturePolicy,
        metadata: &MetadataMap,
    ) -> Result<(Option<Self>, Option<CaptureStats>), ContextBytesError> {
        let candidates = metadata.iter().filter_map(|metadata_entry| {
            Some(match metadata_entry {
                KeyAndValueRef::Ascii(key, value) => {
                    let wire_name = key.as_str();
                    let matched = policy.match_header(wire_name)?;
                    CapturedHeader {
                        wire_name,
                        value: CapturedValue::Borrowed(value.as_bytes()),
                        kind: HeaderValueKind::captured(matched.schema_item.value_kind, wire_name),
                        schema_index: matched.schema_index,
                    }
                }
                KeyAndValueRef::Binary(key, value) => {
                    let wire_name = key.as_str();
                    let matched = policy.match_header(wire_name)?;
                    let decoded = value.to_bytes().ok()?;
                    CapturedHeader {
                        wire_name,
                        value: CapturedValue::Owned(decoded),
                        kind: HeaderValueKind::captured(matched.schema_item.value_kind, wire_name),
                        schema_index: matched.schema_index,
                    }
                }
            })
        });
        Self::capture_candidates(policy, candidates)
    }

    fn capture_candidates<'a, V: AsRef<[u8]>>(
        policy: &CompiledHeaderCapturePolicy,
        candidates: impl IntoIterator<Item = CapturedHeader<'a, V>>,
    ) -> Result<(Option<Self>, Option<CaptureStats>), ContextBytesError> {
        let defaults = policy.defaults();
        let schema = policy.schema();
        let entry_count = schema.entry_count();
        let mut captured = smallvec::SmallVec::<[CapturedHeader<'a, V>; 32]>::new();
        let mut skipped = SkippedHeaders::default();
        let mut blob_len = 0;
        let mut encoded_len = HeaderFields::LEN
            + PresenceBitmap::word_count(entry_count) * size_of::<u64>()
            + entry_count * EntryFields::LEN;

        for candidate in candidates {
            let wire_name = candidate.wire_name;
            if captured.len() >= defaults.max_entries {
                skipped.max_entries += 1;
                continue;
            }
            if wire_name.len() > defaults.max_name_bytes {
                skipped.name_too_long += 1;
                continue;
            }
            let value_len = candidate.value.as_ref().len();
            if value_len > defaults.max_value_bytes {
                skipped.value_too_long += 1;
                continue;
            }
            let header_blob_len = candidate.encoded_len(schema)?;
            let added_len = ItemFields::LEN
                + usize::from(candidate.entry(schema)?.is_some()) * MemberFields::LEN
                + header_blob_len;
            if encoded_len
                .checked_add(added_len)
                .is_none_or(|len| len > MAX_CONTEXT_LEN)
            {
                skipped.context_too_large += 1;
                continue;
            }
            encoded_len += added_len;
            blob_len += header_blob_len;
            captured.push(candidate);
        }

        let context = (!captured.is_empty())
            .then(|| Self::build_captured(&captured, blob_len, schema.clone()))
            .transpose()?;
        Ok((context, skipped.into_stats()))
    }

    fn build_captured<V: AsRef<[u8]>>(
        headers: &[CapturedHeader<'_, V>],
        blob_len: usize,
        schema: Arc<CompiledHeaderSchema>,
    ) -> Result<Self, ContextBytesError> {
        let entry_count = schema.entry_count();
        let entries = EntryIndex::new(entry_count, headers, &schema)?;
        let layout = Layout::new(entry_count, headers.len(), entries.member_count(), blob_len)?;
        let mut writer = EnvelopeWriter::new(layout)?;
        writer.presence(|section| entries.write_presence(section))?;
        writer.entries(|section| entries.write_entries(section))?;
        let mut blob_cursor = 0;
        writer.items(|section| {
            for (index, header) in headers.iter().enumerate() {
                let schema_item = header.schema_item(&schema)?;
                ItemDescriptor::for_captured(header, schema_item, &mut blob_cursor)?
                    .write(section, index * ItemFields::LEN)?;
            }
            Ok(())
        })?;
        writer.members(|section| {
            for (index, member) in entries.members.iter().copied().enumerate() {
                member.write(section, index * MemberFields::LEN)?;
            }
            Ok(())
        })?;
        writer.blob(|section| {
            let mut cursor = 0;
            for header in headers {
                header.write_blob(header.schema_item(&schema)?, section, &mut cursor)?;
            }
            (cursor == section.len())
                .then_some(())
                .ok_or(ContextBytesError::InvalidEnvelope)
        })?;
        Ok(Self::from_vec(writer.finish()?, schema))
    }

    fn from_vec(bytes: Vec<u8>, schema: Arc<CompiledHeaderSchema>) -> Self {
        Self {
            bytes: Arc::new(ContextStorage {
                encoded: bytes,
                schema,
            }),
        }
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

struct EntryIndex {
    presence: smallvec::SmallVec<[u64; 2]>,
    entries: smallvec::SmallVec<[EntryBuild; 16]>,
    members: smallvec::SmallVec<[MemberDescriptor; 32]>,
}

#[derive(Clone, Copy)]
struct EntryBuild {
    first_member: usize,
    member_count: usize,
    next_member: usize,
    hash: u64,
}

impl EntryIndex {
    fn new<V: AsRef<[u8]>>(
        entry_count: usize,
        headers: &[CapturedHeader<'_, V>],
        schema: &CompiledHeaderSchema,
    ) -> Result<Self, ContextBytesError> {
        if entry_count > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }
        if headers.len() > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "items" });
        }

        let mut presence =
            smallvec::SmallVec::<[u64; 2]>::from_elem(0, PresenceBitmap::word_count(entry_count));
        let mut entries = smallvec::SmallVec::<[EntryBuild; 16]>::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(EntryBuild {
                first_member: 0,
                member_count: 0,
                next_member: 0,
                hash: entry_hash_seed(),
            });
        }

        for header in headers {
            let Some(entry) = header.entry(schema)?.map(usize::from) else {
                continue;
            };
            if entry >= entry_count {
                return Err(ContextBytesError::InvalidEnvelope);
            }
            PresenceBitmap::set_words(&mut presence, entry)?;
            entries[entry].member_count += 1;
            entry_hash_value(&mut entries[entry].hash, header.kind, header.value.as_ref())?;
        }

        let mut member_count = 0;
        for entry in &mut entries {
            entry.first_member = member_count;
            entry.next_member = member_count;
            member_count += entry.member_count;
        }
        let mut members = smallvec::SmallVec::<[MemberDescriptor; 32]>::from_elem(
            MemberDescriptor {
                item: 0,
                field: None,
            },
            member_count,
        );
        for (item, header) in headers.iter().enumerate() {
            let Some(entry) = header.entry(schema)?.map(usize::from) else {
                continue;
            };
            let next = entries[entry].next_member;
            members[next] = MemberDescriptor {
                item: u16::try_from(item)
                    .map_err(|_| ContextBytesError::TooMany { what: "items" })?,
                field: None,
            };
            entries[entry].next_member += 1;
        }
        Ok(Self {
            presence,
            entries,
            members,
        })
    }

    fn member_count(&self) -> usize {
        self.members.len()
    }

    fn write_presence(&self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        for (index, word) in self.presence.iter().copied().enumerate() {
            write_u64(bytes, index * size_of::<u64>(), word)?;
        }
        Ok(())
    }

    fn write_entries(&self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        for (index, entry) in self.entries.iter().enumerate() {
            EntryDescriptor {
                first_member: entry.first_member,
                member_count: entry.member_count,
                hash: entry.hash,
            }
            .write(bytes, index * EntryFields::LEN)?;
        }
        Ok(())
    }
}

fn copy_section(
    target_bytes: &mut [u8],
    target: Range<usize>,
    source_bytes: &[u8],
    source_range: Range<usize>,
) -> Result<(), ContextBytesError> {
    if target.len() != source_range.len() {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    target_bytes
        .get_mut(target)
        .zip(source_bytes.get(source_range))
        .ok_or(ContextBytesError::InvalidEnvelope)
        .map(|(t, s)| t.copy_from_slice(s))
}

// --- descriptors ---

#[derive(Clone, Copy, Debug)]
struct MemberDescriptor {
    item: u16,
    field: Option<ContextFieldId>,
}

impl MemberDescriptor {
    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        let field = MemberFields::FIELD.read(bytes, at)?;
        Some(Self {
            item: MemberFields::ITEM.read(bytes, at)?,
            field: (field != NO_FIELD).then(|| ContextFieldId::from_u16(field)),
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        MemberFields::ITEM.write(bytes, at, self.item)?;
        MemberFields::FIELD.write(
            bytes,
            at,
            self.field.map_or(NO_FIELD, ContextFieldId::as_u16),
        )
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

    #[cfg(test)]
    fn valid_for(self, member_count: usize) -> bool {
        self.first_member
            .checked_add(self.member_count)
            .is_some_and(|end| end <= member_count)
    }
}

#[derive(Clone, Copy, Debug)]
struct ItemDescriptor {
    schema_index: u16,
    kind: HeaderValueKind,
    wire_name: BlobRange,
    value: BlobRange,
}

impl ItemDescriptor {
    fn for_captured<V: AsRef<[u8]>>(
        header: &CapturedHeader<'_, V>,
        schema_item: CompiledHeaderSchemaItemRef<'_>,
        cursor: &mut usize,
    ) -> Result<Self, ContextBytesError> {
        let wire_name = if let Some(wire_name) = header.wire_name_occurrence(schema_item) {
            let range = BlobRange {
                offset: *cursor,
                len: wire_name.len(),
            };
            *cursor = range.end().ok_or(ContextBytesError::TooLarge)?;
            range
        } else {
            BlobRange { offset: 0, len: 0 }
        };
        let value = BlobRange {
            offset: *cursor,
            len: header.value.as_ref().len(),
        };
        *cursor = value.end().ok_or(ContextBytesError::TooLarge)?;
        Ok(Self {
            schema_index: header.schema_index,
            kind: header.kind,
            wire_name,
            value,
        })
    }

    #[cfg(test)]
    fn read(bytes: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            schema_index: ItemFields::SCHEMA_INDEX.read(bytes, at)?,
            kind: HeaderValueKind::decode(ItemFields::KIND.read(bytes, at)?)?,
            wire_name: ItemFields::WIRE_NAME.read(bytes, at)?,
            value: ItemFields::VALUE.read(bytes, at)?,
        })
    }

    fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        ItemFields::SCHEMA_INDEX.write(bytes, at, self.schema_index)?;
        ItemFields::KIND.write(bytes, at, self.kind as u8)?;
        ItemFields::_PAD.write(bytes, at, 0)?;
        ItemFields::WIRE_NAME.write(bytes, at, self.wire_name)?;
        ItemFields::VALUE.write(bytes, at, self.value)
    }

    #[cfg(test)]
    fn valid_for(self, layout: Layout, blob: &[u8], schema: &CompiledHeaderSchema) -> bool {
        if self.schema_index == NO_SCHEMA_ITEM {
            if self.wire_name.len != 0 {
                return false;
            }
        } else {
            let Some(schema_item) = schema.item(self.schema_index) else {
                return false;
            };
            if schema_item
                .register
                .is_some_and(|register| register.index() >= layout.entry_count)
            {
                return false;
            }
        }
        // Wire name occurrence: zero-length is valid (uses schema), else must be valid UTF-8
        if self.wire_name.len > 0 && self.wire_name.text(blob).is_none() {
            return false;
        }
        // Value must be in range
        self.value.slice(blob).is_some()
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

    fn slice(self, blob: &[u8]) -> Option<&[u8]> {
        blob.get(self.offset..self.end()?)
    }

    #[cfg(test)]
    fn text(self, blob: &[u8]) -> Option<&str> {
        std::str::from_utf8(self.slice(blob)?).ok()
    }
}

#[cfg(test)]
fn validate(bytes: &[u8], schema: &CompiledHeaderSchema) -> Result<(), ContextBytesError> {
    let layout = Layout::read(bytes).ok_or(ContextBytesError::InvalidEnvelope)?;
    if bytes.len() != layout.total_len {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    if layout.entry_count != schema.compiled_context().register_file().len() {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    validate_unused_presence_bits(bytes, layout)?;
    let blob = layout
        .blob(bytes)
        .ok_or(ContextBytesError::InvalidEnvelope)?;
    let items: Vec<_> = (0..layout.item_count)
        .map(|index| {
            layout
                .item_descriptor(bytes, index)
                .filter(|item| item.valid_for(layout, blob, schema))
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
        let register = ContextRegisterId::from_u16(
            u16::try_from(slot).map_err(|_| ContextBytesError::InvalidEnvelope)?,
        );
        let shape = schema
            .compiled_context()
            .register_file()
            .shape(register)
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        if matches!(
            shape,
            ContextRegisterShape::Scalar(_) | ContextRegisterShape::KeyValue(_)
        ) && descriptor.member_count > 1
        {
            return Err(ContextBytesError::InvalidEnvelope);
        }

        let record = match shape {
            ContextRegisterShape::Record(record) => schema
                .compiled_context()
                .register_file()
                .record(*record)
                .ok_or(ContextBytesError::InvalidEnvelope)?
                .into(),
            _ => None,
        };
        let mut record_constraints = record.map(|record| RecordConstraints::new(record, true));
        let mut hash = entry_hash_seed();
        for member in descriptor.members() {
            let member = MemberDescriptor::read(
                bytes,
                layout
                    .member_offset(member)
                    .ok_or(ContextBytesError::InvalidEnvelope)?,
            )
            .ok_or(ContextBytesError::InvalidEnvelope)?;
            let item = usize::from(member.item);
            let item = Some(item)
                .filter(|item| *item < items.len())
                .ok_or(ContextBytesError::InvalidEnvelope)?;
            let item_entry = (items[item].schema_index != NO_SCHEMA_ITEM)
                .then(|| schema.item(items[item].schema_index))
                .flatten()
                .and_then(|item| item.register);
            if indexed_items[item]
                || (items[item].schema_index != NO_SCHEMA_ITEM && item_entry != Some(register))
            {
                return Err(ContextBytesError::InvalidEnvelope);
            }
            indexed_items[item] = true;
            let value = items[item]
                .value
                .slice(blob)
                .ok_or(ContextBytesError::InvalidEnvelope)?;
            if record.is_some() {
                let field = member.field.ok_or(ContextBytesError::InvalidEnvelope)?;
                record_constraints
                    .as_mut()
                    .ok_or(ContextBytesError::InvalidEnvelope)?
                    .accept(field, items[item].kind, value)
                    .map_err(|_| ContextBytesError::InvalidEnvelope)?;
                record_hash_value(&mut hash, field, items[item].kind, value)?;
            } else {
                if member.field.is_some() {
                    return Err(ContextBytesError::InvalidEnvelope);
                }
                let scalar_type = match shape {
                    ContextRegisterShape::Scalar(scalar_type)
                    | ContextRegisterShape::ScalarList(scalar_type)
                    | ContextRegisterShape::KeyValue(scalar_type)
                    | ContextRegisterShape::KeyValueList(scalar_type) => *scalar_type,
                    ContextRegisterShape::Record(_) => {
                        return Err(ContextBytesError::InvalidEnvelope);
                    }
                };
                if !scalar_value_matches(scalar_type, items[item].kind, value) {
                    return Err(ContextBytesError::InvalidEnvelope);
                }
                entry_hash_value(&mut hash, items[item].kind, value)?;
            }
        }
        if descriptor.hash != hash {
            return Err(ContextBytesError::InvalidEnvelope);
        }
    }

    if indexed_items.into_iter().any(|indexed| !indexed) {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    Ok(())
}

#[cfg(test)]
fn validate_unused_presence_bits(bytes: &[u8], layout: Layout) -> Result<(), ContextBytesError> {
    let used_bits = layout.entry_count % PresenceBitmap::WORD_BITS;
    let presence_words = PresenceBitmap::word_count(layout.entry_count);
    if used_bits == 0 || presence_words == 0 {
        return Ok(());
    }
    let presence = bytes
        .get(layout.presence_section())
        .ok_or(ContextBytesError::InvalidEnvelope)?;
    let last_word = read_u64(presence, (presence_words - 1) * size_of::<u64>())
        .ok_or(ContextBytesError::InvalidEnvelope)?;
    let unused_mask = !((1u64 << used_bits) - 1);
    if last_word & unused_mask != 0 {
        return Err(ContextBytesError::InvalidEnvelope);
    }
    Ok(())
}

// --- hash ---

const fn entry_hash_seed() -> u64 {
    0xcbf2_9ce4_8422_2325_u64
}

enum RecordConstraintError {
    InvalidField(ContextFieldId),
    DuplicateField(ContextFieldId),
    TypeMismatch(ContextFieldId),
    OutOfOrder,
}

impl RecordConstraintError {
    fn into_context_error(self, register: ContextRegisterId) -> ContextBytesError {
        match self {
            Self::InvalidField(field) => ContextBytesError::InvalidRecordField { register, field },
            Self::DuplicateField(field) => {
                ContextBytesError::DuplicateRecordField { register, field }
            }
            Self::TypeMismatch(field) => ContextBytesError::RecordFieldTypeMismatch { field },
            Self::OutOfOrder => ContextBytesError::InvalidEnvelope,
        }
    }
}

struct RecordConstraints<'a> {
    record: &'a ContextRecordShape,
    scalar_seen: Vec<bool>,
    previous_field: Option<ContextFieldId>,
    require_order: bool,
}

impl<'a> RecordConstraints<'a> {
    fn new(record: &'a ContextRecordShape, require_order: bool) -> Self {
        Self {
            record,
            scalar_seen: vec![false; record.fields().len()],
            previous_field: None,
            require_order,
        }
    }

    fn accept(
        &mut self,
        field: ContextFieldId,
        kind: ContextValueKind,
        value: &[u8],
    ) -> Result<(), RecordConstraintError> {
        if self.require_order && self.previous_field.is_some_and(|previous| previous > field) {
            return Err(RecordConstraintError::OutOfOrder);
        }
        self.previous_field = Some(field);
        let field_shape = self
            .record
            .fields()
            .get(field.index())
            .ok_or(RecordConstraintError::InvalidField(field))?;
        let seen = self
            .scalar_seen
            .get_mut(field.index())
            .ok_or(RecordConstraintError::InvalidField(field))?;
        if !field_shape.is_repeated() && std::mem::replace(seen, true) {
            return Err(RecordConstraintError::DuplicateField(field));
        }
        if !scalar_value_matches(field_shape.scalar_type(), kind, value) {
            return Err(RecordConstraintError::TypeMismatch(field));
        }
        Ok(())
    }
}

fn scalar_value_matches(
    scalar_type: ContextScalarType,
    kind: ContextValueKind,
    value: &[u8],
) -> bool {
    match scalar_type {
        ContextScalarType::Text => {
            kind == ContextValueKind::Text && std::str::from_utf8(value).is_ok()
        }
        ContextScalarType::Bytes => kind == ContextValueKind::Binary,
        ContextScalarType::AnyValue => true,
    }
}

fn record_hash(values: &[ContextRecordValue<'_>]) -> Result<u64, ContextBytesError> {
    let mut hash = entry_hash_seed();
    for value in values {
        record_hash_value(&mut hash, value.field, value.kind, value.value)?;
    }
    Ok(hash)
}

fn record_hash_value(
    hash: &mut u64,
    field: ContextFieldId,
    kind: ContextValueKind,
    value: &[u8],
) -> Result<(), ContextBytesError> {
    hash_bytes(hash, &field.as_u16().to_le_bytes());
    entry_hash_value(hash, kind, value)
}

fn entry_hash_value(
    hash: &mut u64,
    kind: HeaderValueKind,
    value: &[u8],
) -> Result<(), ContextBytesError> {
    hash_bytes(hash, &[kind as u8]);
    hash_bytes(
        hash,
        &u16::try_from(value.len())
            .map_err(|_| ContextBytesError::TooLarge)?
            .to_le_bytes(),
    );
    hash_bytes(hash, value);
    Ok(())
}

fn entry_hash_for_single(kind: HeaderValueKind, value: &[u8]) -> Result<u64, ContextBytesError> {
    let mut hash = entry_hash_seed();
    entry_hash_value(&mut hash, kind, value)?;
    Ok(hash)
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
}

// --- context ---

/// Immutable encoded pdata context.
#[derive(Clone, PartialEq, Eq)]
pub struct PdataContextBytes {
    bytes: Arc<ContextStorage>,
}

#[derive(PartialEq, Eq)]
struct ContextStorage {
    encoded: Vec<u8>,
    schema: Arc<CompiledHeaderSchema>,
}

impl ContextStorage {
    fn schema(&self) -> &Arc<CompiledHeaderSchema> {
        &self.schema
    }
}

impl Deref for ContextStorage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.encoded
    }
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
    fn project(&self) -> ContextProjectionAccumulator<'_> {
        ContextProjectionAccumulator { input: self }
    }

    /// Iterates all bag items in arrival order.
    #[must_use]
    pub fn items(&self) -> ContextItems<'_> {
        ContextItems {
            context: self,
            layout: self.layout().ok(),
            next: 0,
        }
    }

    /// Scans the bag using decisions compiled for this context's schema.
    #[must_use]
    pub fn propagate<'a>(&'a self, plan: &'a CompiledSchemaPropagation) -> ContextPropagation<'a> {
        ContextPropagation {
            items: self.items(),
            plan,
        }
    }

    /// Returns a present register through its compiled numeric identity.
    #[must_use]
    pub fn register(&self, register: ContextRegisterId) -> Option<ContextRegister<'_>> {
        let layout = self.layout().ok()?;
        let slot = register.index();
        if !layout.entry_present(&self.bytes, slot)? {
            return None;
        }

        Some(ContextRegister {
            context: self,
            layout,
            register,
            descriptor: layout.entry_descriptor(&self.bytes, slot)?,
        })
    }

    /// Returns a present register through its schema-local slot.
    ///
    /// This compatibility accessor is retained while callers migrate to
    /// [`Self::register`].
    #[must_use]
    pub fn entry(&self, slot: usize) -> Option<ContextRegister<'_>> {
        let register = u16::try_from(slot).ok().map(ContextRegisterId::from_u16)?;
        self.register(register)
    }

    /// Returns the immutable capture schema referenced by this context.
    #[must_use]
    pub fn schema(&self) -> &Arc<CompiledHeaderSchema> {
        self.bytes.schema()
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
    /// Original transport wire name (or schema-normalized name if occurrence
    /// data was not stored).
    #[must_use]
    pub fn wire_name(&self) -> Option<&'a str> {
        let wire_range = ItemFields::WIRE_NAME.read(&self.context.bytes, self.descriptor_at)?;
        if wire_range.len == 0 {
            // No occurrence data -- use schema-normalized wire name
            return Some(self.schema_item()?.wire_name);
        }
        self.text(ItemFields::WIRE_NAME)
    }

    /// Source symbol retained only for compatibility inspection.
    ///
    /// Executable register values do not contain this name. New bindings
    /// should use compiled register identifiers.
    #[must_use]
    pub fn stored_name(&self) -> Option<&'a str> {
        let schema_item = self.schema_item()?;
        match schema_item.register {
            Some(register) => self
                .context
                .schema()
                .compiled_context()
                .linker()
                .compatibility_symbol(register),
            None => Some(schema_item.wire_name),
        }
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
        self.schema_item()?.rule_id
    }

    /// Optional context-entry slot.
    #[must_use]
    pub fn entry_slot(&self) -> Option<u16> {
        self.schema_item()?.register.map(|id| id.as_u16())
    }

    /// Index in the retained compiled schema.
    #[must_use]
    pub fn schema_index(&self) -> Option<u16> {
        let index = ItemFields::SCHEMA_INDEX.read(&self.context.bytes, self.descriptor_at)?;
        (index != NO_SCHEMA_ITEM).then_some(index)
    }

    /// Whether the wire name resolves directly from the compiled schema
    /// (no occurrence data stored).
    #[must_use]
    pub fn uses_schema_wire_name(&self) -> bool {
        ItemFields::WIRE_NAME
            .read(&self.context.bytes, self.descriptor_at)
            .is_some_and(|range| range.len == 0)
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

    fn schema_item(&self) -> Option<CompiledHeaderSchemaItemRef<'a>> {
        let id = self.schema_index()?;
        self.context.bytes.schema().item(id)
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
    plan: &'a CompiledSchemaPropagation,
}

impl<'a> Iterator for ContextPropagation<'a> {
    type Item = PropagatedContextItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.items.by_ref() {
            let Some(schema_index) = item.schema_index() else {
                continue;
            };
            let Some(decision) = self.plan.decision(schema_index) else {
                continue;
            };
            if decision.action == PropagationAction::Drop {
                continue;
            }
            let header_name = match &decision.output_name {
                CompiledOutputName::Observed => item.wire_name()?,
                CompiledOutputName::Static(name) => name,
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

/// Borrowed view of one compiled context register.
pub struct ContextRegister<'a> {
    context: &'a PdataContextBytes,
    layout: Layout,
    register: ContextRegisterId,
    descriptor: EntryDescriptor,
}

impl<'a> ContextRegister<'a> {
    /// Returns this value's register-file-local identity.
    #[must_use]
    pub const fn id(&self) -> ContextRegisterId {
        self.register
    }

    /// Returns this register's compiled runtime shape.
    #[must_use]
    pub fn shape(&self) -> Option<&ContextRegisterShape> {
        self.context
            .schema()
            .compiled_context()
            .register_file()
            .shape(self.register)
    }

    /// Returns the typed hash. Callers must compare values on a hash hit.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.descriptor.hash
    }

    /// Iterates the entry's typed values in arrival order.
    pub fn values(&self) -> impl Iterator<Item = (HeaderValueKind, &'a [u8])> + '_ {
        self.descriptor.members().filter_map(move |member| {
            let item =
                MemberDescriptor::read(&self.context.bytes, self.layout.member_offset(member)?)?
                    .item;
            self.context
                .item_with_layout(usize::from(item), self.layout)?
                .value()
        })
    }

    /// Iterates a record register in compiled field order.
    ///
    /// Scalar fields occur at most once. Repeated field values retain their
    /// producer order within the field.
    pub fn record_fields(&self) -> impl Iterator<Item = ContextRecordValue<'a>> + '_ {
        let is_record = matches!(self.shape(), Some(ContextRegisterShape::Record(_)));
        self.descriptor.members().filter_map(move |member| {
            if !is_record {
                return None;
            }
            let member =
                MemberDescriptor::read(&self.context.bytes, self.layout.member_offset(member)?)?;
            let field = member.field?;
            let item = self
                .context
                .item_with_layout(usize::from(member.item), self.layout)?;
            let (kind, value) = item.value()?;
            Some(ContextRecordValue::new(field, kind, value))
        })
    }

    /// Iterates ordered runtime key/value associations.
    ///
    /// The key comes from retained provenance when required, otherwise from
    /// the compiled ingress instruction.
    pub fn key_values(&self) -> impl Iterator<Item = (&'a str, HeaderValueKind, &'a [u8])> + '_ {
        let is_keyed = matches!(
            self.shape(),
            Some(ContextRegisterShape::KeyValue(_) | ContextRegisterShape::KeyValueList(_))
        );
        self.descriptor.members().filter_map(move |member| {
            if !is_keyed {
                return None;
            }
            let item =
                MemberDescriptor::read(&self.context.bytes, self.layout.member_offset(member)?)?
                    .item;
            let item = self
                .context
                .item_with_layout(usize::from(item), self.layout)?;
            let key = item.wire_name()?;
            let (kind, value) = item.value()?;
            Some((key, kind, value))
        })
    }
}

/// Compatibility name for a compiled context register.
pub type ContextEntry<'a> = ContextRegister<'a>;

struct ContextProjectionAccumulator<'a> {
    input: &'a PdataContextBytes,
}

impl ContextProjectionAccumulator<'_> {
    /// Copies the input envelope and appends one schema-backed item with a new
    /// singleton entry slot.
    ///
    /// Extends entry_count by 1, sets presence for the new entry, appends its
    /// EntryDescriptor and member, and appends the item/value. Preserves all
    /// existing entries, members, items, hashes, and blob content.
    fn copy_and_append_entry_item(
        self,
        schema_index: u16,
        entry_slot: u16,
        wire_name: &str,
        value: &[u8],
        kind: HeaderValueKind,
        derived_schema: Arc<CompiledHeaderSchema>,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        let old = self.input.layout()?;
        let schema_item = derived_schema
            .item(schema_index)
            .ok_or(ContextBytesError::InvalidEnvelope)?;
        let wire_name_occurrence = (schema_item.retain_observed_name
            && schema_item.wire_name != wire_name)
            .then_some(wire_name.as_bytes());
        let wire_name_len = wire_name_occurrence.map_or(0, |name| name.len());
        let new_entry_count = old
            .entry_count
            .checked_add(1)
            .ok_or(ContextBytesError::TooMany { what: "entries" })?;
        let new_item_count = old
            .item_count
            .checked_add(1)
            .ok_or(ContextBytesError::TooMany { what: "items" })?;
        let new_member_count = old
            .member_count
            .checked_add(1)
            .ok_or(ContextBytesError::TooMany { what: "members" })?;
        let new_blob_len = old
            .blob_len
            .checked_add(wire_name_len)
            .and_then(|len| len.checked_add(value.len()))
            .ok_or(ContextBytesError::TooLarge)?;

        let layout = Layout::new(
            new_entry_count,
            new_item_count,
            new_member_count,
            new_blob_len,
        )?;
        let mut writer = EnvelopeWriter::new(layout)?;
        writer.presence(|section| {
            copy_section(
                section,
                0..old.presence_section().len(),
                &self.input.bytes,
                old.presence_section(),
            )?;
            PresenceBitmap::set_encoded(section, usize::from(entry_slot))
        })?;

        let new_member_index = old.member_count;
        let new_item_index = u16::try_from(old.item_count)
            .map_err(|_| ContextBytesError::TooMany { what: "items" })?;
        let entry_hash_val = entry_hash_for_single(kind, value)?;
        let new_entry_desc = EntryDescriptor {
            first_member: new_member_index,
            member_count: 1,
            hash: entry_hash_val,
        };
        writer.entries(|section| {
            let prefix_len = old.entry_count * EntryFields::LEN;
            copy_section(
                section,
                0..prefix_len,
                &self.input.bytes,
                old.entries_section(),
            )?;
            new_entry_desc.write(section, prefix_len)
        })?;

        let value_range = BlobRange {
            offset: old
                .blob_len
                .checked_add(wire_name_len)
                .ok_or(ContextBytesError::TooLarge)?,
            len: value.len(),
        };
        let descriptor = ItemDescriptor {
            schema_index,
            kind,
            wire_name: wire_name_occurrence.map_or(BlobRange { offset: 0, len: 0 }, |wire_name| {
                BlobRange {
                    offset: old.blob_len,
                    len: wire_name.len(),
                }
            }),
            value: value_range,
        };
        writer.items(|section| {
            let prefix_len = old.item_count * ItemFields::LEN;
            copy_section(
                section,
                0..prefix_len,
                &self.input.bytes,
                old.items_section(),
            )?;
            descriptor.write(section, prefix_len)
        })?;
        writer.members(|section| {
            let prefix_len = old.member_count * MemberFields::LEN;
            copy_section(
                section,
                0..prefix_len,
                &self.input.bytes,
                old.members_section(),
            )?;
            MemberDescriptor {
                item: new_item_index,
                field: None,
            }
            .write(section, prefix_len)
        })?;
        writer.blob(|section| {
            copy_section(
                section,
                0..old.blob_len,
                &self.input.bytes,
                old.blob_section(),
            )?;
            if let Some(wire_name) = wire_name_occurrence {
                write_slice(section, old.blob_len, wire_name)?;
            }
            write_slice(section, value_range.offset, value)
        })?;

        Ok(PdataContextBytes::from_vec(
            writer.finish()?,
            derived_schema,
        ))
    }
}

// --- record ---

impl PdataContextBytes {
    /// Builds one standalone schema-defined record register.
    ///
    /// Field names have already been compiled into [`ContextFieldId`] values.
    /// Members are encoded in field order, while repeated values retain their
    /// input order within each field.
    pub fn from_record<'a>(
        compiled_context: Arc<CompiledContext>,
        register: ContextRegisterId,
        values: impl IntoIterator<Item = ContextRecordValue<'a>>,
    ) -> Result<Self, ContextBytesError> {
        let register_file = compiled_context.register_file();
        let record_id = match register_file.shape(register) {
            Some(ContextRegisterShape::Record(record)) => *record,
            _ => return Err(ContextBytesError::NotARecord { register }),
        };
        let record = register_file
            .record(record_id)
            .ok_or(ContextBytesError::NotARecord { register })?;
        let mut values: smallvec::SmallVec<[ContextRecordValue<'a>; 8]> =
            values.into_iter().collect();
        if values.is_empty() {
            return Err(ContextBytesError::EmptyRegisterValue { register });
        }
        if values.len() > usize::from(u16::MAX) {
            return Err(ContextBytesError::TooMany { what: "items" });
        }

        let mut constraints = RecordConstraints::new(record, false);
        for value in &values {
            constraints
                .accept(value.field, value.kind, value.value)
                .map_err(|error| error.into_context_error(register))?;
        }
        values.sort_by_key(|value| value.field.index());

        let blob_len = values.iter().try_fold(0usize, |total, value| {
            total
                .checked_add(value.value.len())
                .ok_or(ContextBytesError::TooLarge)
        })?;
        let layout = Layout::new(register_file.len(), values.len(), values.len(), blob_len)?;
        let mut writer = EnvelopeWriter::new(layout)?;
        writer.presence(|section| PresenceBitmap::set_encoded(section, register.index()))?;
        writer.entries(|section| {
            for slot in 0..register_file.len() {
                let descriptor = if slot == register.index() {
                    EntryDescriptor {
                        first_member: 0,
                        member_count: values.len(),
                        hash: record_hash(&values)?,
                    }
                } else {
                    EntryDescriptor {
                        first_member: 0,
                        member_count: 0,
                        hash: entry_hash_seed(),
                    }
                };
                descriptor.write(section, slot * EntryFields::LEN)?;
            }
            Ok(())
        })?;
        let mut blob_cursor = 0;
        writer.items(|section| {
            for (index, value) in values.iter().enumerate() {
                let range = BlobRange {
                    offset: blob_cursor,
                    len: value.value.len(),
                };
                blob_cursor = range.end().ok_or(ContextBytesError::TooLarge)?;
                ItemDescriptor {
                    schema_index: NO_SCHEMA_ITEM,
                    kind: value.kind,
                    wire_name: BlobRange { offset: 0, len: 0 },
                    value: range,
                }
                .write(section, index * ItemFields::LEN)?;
            }
            Ok(())
        })?;
        writer.members(|section| {
            for (item, value) in values.iter().enumerate() {
                MemberDescriptor {
                    item: u16::try_from(item)
                        .map_err(|_| ContextBytesError::TooMany { what: "items" })?,
                    field: Some(value.field),
                }
                .write(section, item * MemberFields::LEN)?;
            }
            Ok(())
        })?;
        writer.blob(|section| {
            let mut cursor = 0;
            for value in &values {
                write_slice(section, cursor, value.value)?;
                cursor = cursor
                    .checked_add(value.value.len())
                    .ok_or(ContextBytesError::TooLarge)?;
            }
            (cursor == section.len())
                .then_some(())
                .ok_or(ContextBytesError::InvalidEnvelope)
        })?;
        Ok(Self::from_vec(
            writer.finish()?,
            CompiledHeaderSchema::context_only(compiled_context),
        ))
    }
}

// --- facade ---

impl PdataContextBytes {
    pub(super) fn project_scalar(
        &self,
        schema_index: u16,
        entry_slot: u16,
        wire_name: &str,
        value: &[u8],
        kind: HeaderValueKind,
        derived_schema: Arc<CompiledHeaderSchema>,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        self.project().copy_and_append_entry_item(
            schema_index,
            entry_slot,
            wire_name,
            value,
            kind,
            derived_schema,
        )
    }

    pub(super) fn from_scalar(
        wire_name: &str,
        value: &[u8],
        kind: HeaderValueKind,
        schema_index: u16,
        schema: Arc<CompiledHeaderSchema>,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        let header = CapturedHeader {
            wire_name,
            value,
            kind,
            schema_index,
        };
        Self::build_captured(&[header], value.len(), schema)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
