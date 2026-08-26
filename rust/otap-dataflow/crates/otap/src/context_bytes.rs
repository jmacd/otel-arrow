// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context -- schema-backed only.
//!
//! Configuration symbols are compiled into dense register identifiers. The
//! executable register file contains value shapes and numeric slots, but no
//! logical names or transport names. Each context retains its immutable
//! compiler version so several generations can remain in flight concurrently.
//!
//! - A register is a scalar, scalar list, key/value, key/value list, or record.
//! - A value item carries a schema instruction index, typed bytes, and optional
//!   source-name provenance.
//! - Presence is one bit per register.
//! - A member is a value-item index belonging to a register.
//! - Counts determine the length of each fixed-size table.
//! - Transport names remain in ingress/egress instructions. Observed-name bytes
//!   enter the context only when the compiler marks provenance as live.
//!
//! The envelope keeps fixed-size indexes before one variable-size blob:
//!
//! ```text
//! +--------------------------------------------------------------------------+
//! | envelope header (8 bytes)                                                |
//! +--------------------------------------------------------------------------+
//! | version u16 | registers u16 | items u16 | member count u16               |
//! +--------------------------------------------------------------------------+
//! | register presence bitmap (presence words * 8 bytes)                      |
//! +--------------------------------------------------------------------------+
//! | register descriptors (register count * 12 bytes)                         |
//! +--------------------------------------------------------------------------+
//! | item descriptors (item count * 12 bytes, in arrival order)               |
//! +--------------------------------------------------------------------------+
//! | register members (member count * 4-byte member descriptors)              |
//! +--------------------------------------------------------------------------+
//! | blob: observed wire-name occurrences and values                          |
//! +--------------------------------------------------------------------------+
//! ```
//!
//! Each present register selects an ordered range from the member table. Each
//! member identifies an item and, for records, its compiled field position:
//!
//! ```text
//! register descriptor (12 bytes)
//! +------------------+------------------+------------------------------------+
//! | first member u16 | member count u16 | typed value hash u64               |
//! +------------------+------------------+------------------------------------+
//!
//! item descriptor (12 bytes)
//! fixed fields (4 bytes)
//! +--------------------+--------+------+
//! | schema_index u16   | kind u8| _pad |
//! +--------------------+--------+------+
//! blob ranges (2 * 4 bytes)
//! +----------------+---------------------------+-----------------------------+
//! | wire name occ  | blob offset u16           | byte length u16             |
//! | value          | blob offset u16           | byte length u16             |
//! +----------------+---------------------------+-----------------------------+
//! ```
//!
//! ```text
//! member descriptor (4 bytes)
//! +----------------+------------------+
//! | item index u16 | field ordinal u16|
//! +----------------+------------------+
//! ```
//!
//! Non-record registers use `u16::MAX` for the field ordinal. Record registers
//! use only numeric field positions; field names never enter the envelope.
//!
//! A zero-length source-name range means the ingress instruction supplies any
//! statically known key. A non-empty range is explicit runtime provenance.

use std::{
    fmt,
    marker::PhantomData,
    ops::{Deref, Range},
    sync::Arc,
};

use otel_arrow_dfe_config::context::{
    CompiledContext, ContextFieldId, ContextRecordShape, ContextRegisterId, ContextRegisterShape,
    ContextScalarType, ContextVersion,
};
#[cfg(test)]
use otel_arrow_dfe_config::transport_headers_policy::NameStrategy;
use otel_arrow_dfe_config::transport_headers_policy::{
    CaptureStats, CompiledHeaderCapturePolicy, CompiledHeaderSchema, CompiledHeaderSchemaItemRef,
    CompiledOutputName, CompiledSchemaPropagation, PropagationAction, ValueKindConfig,
};
use tonic::metadata::{KeyAndValueRef, MetadataMap};

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

/// Maximum number of cached schema plans per binding instance.
const SCHEMA_CACHE_CAPACITY: usize = 8;

struct SchemaPlanCache<P> {
    entries: Vec<(ContextVersion, P)>,
}

impl<P> SchemaPlanCache<P> {
    const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn get(&self, version: ContextVersion) -> Option<&P> {
        self.entries
            .iter()
            .find(|(cached, _)| *cached == version)
            .map(|(_, plan)| plan)
    }

    fn insert(&mut self, version: ContextVersion, plan: P) -> &P {
        if self.entries.len() >= SCHEMA_CACHE_CAPACITY {
            let _ = self.entries.remove(0);
        }
        self.entries.push((version, plan));
        &self.entries.last().expect("inserted schema plan").1
    }

    fn get_or_insert_with(&mut self, version: ContextVersion, build: impl FnOnce() -> P) -> &P {
        if let Some(index) = self
            .entries
            .iter()
            .position(|(cached, _)| *cached == version)
        {
            return &self.entries[index].1;
        }
        self.insert(version, build())
    }
}

/// Version-linked lookup binding for the first value of one context register.
///
/// This is a transitional predicate-binding primitive: it resolves the
/// configured name once per input schema, then evaluates contexts using only
/// schema indices. The bounded cache is local to the node that owns the
/// binding, so replacing the binding also replaces its configuration
/// generation.
pub struct ContextRegisterValueBinding {
    symbol: String,
    cache: SchemaPlanCache<Option<ContextRegisterId>>,
}

impl ContextRegisterValueBinding {
    /// Creates a binding for one source-level register symbol.
    #[must_use]
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_ascii_lowercase(),
            cache: SchemaPlanCache::new(),
        }
    }

    /// Returns the first matching value in context arrival order.
    pub fn value<'a>(
        &mut self,
        context: &'a PdataContextBytes,
    ) -> Option<(HeaderValueKind, &'a [u8])> {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let register = *self.cache.get_or_insert_with(version, || {
            context
                .schema()
                .compiled_context()
                .linker()
                .resolve(&self.symbol)
                .ok()
        });
        register
            .and_then(|register| context.register(register))
            .and_then(|entry| entry.values().next())
    }
}

/// Compatibility name for a single-register value binding.
pub type ContextValueBinding = ContextRegisterValueBinding;

/// Version-linked binding for an ordered set of context registers.
///
/// Configuration symbols are linked once per compiler version. Evaluation
/// visits numeric registers in configuration order without name lookup.
pub struct ContextRegisterSetBinding {
    symbols: Box<[String]>,
    cache: SchemaPlanCache<Box<[Option<ContextRegisterId>]>>,
}

impl ContextRegisterSetBinding {
    /// Creates a binding for an ordered set of register symbols.
    #[must_use]
    pub fn new<'a>(symbols: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            symbols: symbols.into_iter().map(str::to_ascii_lowercase).collect(),
            cache: SchemaPlanCache::new(),
        }
    }

    /// Visits each configured register that is present in the context.
    ///
    /// Returns the number of entries visited.
    pub fn visit_present(
        &mut self,
        context: &PdataContextBytes,
        mut visitor: impl FnMut(usize, ContextRegister<'_>),
    ) -> usize {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let symbols = &self.symbols;
        let slots = self.cache.get_or_insert_with(version, || {
            let linker = context.schema().compiled_context().linker();
            symbols
                .iter()
                .map(|symbol| linker.resolve(symbol).ok())
                .collect()
        });
        let mut visited = 0;
        for (ordinal, register) in slots.iter().copied().enumerate() {
            let Some(entry) = register.and_then(|register| context.register(register)) else {
                continue;
            };
            visitor(ordinal, entry);
            visited += 1;
        }
        visited
    }
}

/// Compatibility name for an ordered register-set binding.
pub type ContextEntrySetBinding = ContextRegisterSetBinding;

/// Compiled singleton context-entry projector for partition output.
///
/// This is a narrowly scoped initial projector that:
/// - Owns the configured output context-entry schema definition
/// - Caches prefix-preserving derived schemas keyed by input schema identity
/// - Appends a schema-indexed item with a singleton context entry
/// - Never creates inline items
///
/// Composite entry support remains future work. This binding is
/// processor-local and mutable (no shared state or locks). The cache is
/// bounded to [`SCHEMA_CACHE_CAPACITY`] entries with FIFO eviction.
pub struct ContextScalarProjectionBinding {
    /// Source-level symbol compiled for the projected output register.
    register_symbol: String,
    /// Schema with one item and one entry, used when there is no input context.
    standalone_schema: Arc<CompiledHeaderSchema>,
    /// schema_index within the standalone schema.
    standalone_schema_index: u16,
    /// Bounded FIFO cache of derived schemas keyed by input schema Arc pointer.
    /// Each plan contains (derived_schema, schema_index, entry_slot).
    cache: SchemaPlanCache<(Arc<CompiledHeaderSchema>, u16, u16)>,
}

impl ContextScalarProjectionBinding {
    /// Creates a scalar projection for the given register symbol.
    pub fn new(register_symbol: &str) -> Self {
        let (standalone_schema, standalone_schema_index, _entry_slot) =
            CompiledHeaderSchema::singleton_entry(register_symbol);
        Self {
            register_symbol: register_symbol.to_string(),
            standalone_schema_index,
            standalone_schema,
            cache: SchemaPlanCache::new(),
        }
    }

    /// Projects a partition value onto a context as a singleton context entry.
    ///
    /// If `input` is `Some`, the existing context entries/items are preserved
    /// and the partition entry is appended with a derived schema. If `input` is
    /// `None`, a new single-entry context is created using the standalone schema.
    pub fn project(
        &mut self,
        input: Option<&PdataContextBytes>,
        value: &[u8],
        kind: HeaderValueKind,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        match input {
            Some(ctx) => {
                let (derived_schema, schema_index, entry_slot) =
                    self.derived_schema(ctx.schema())?;
                ctx.project().copy_and_append_entry_item(
                    schema_index,
                    entry_slot,
                    &self.register_symbol,
                    value,
                    kind,
                    derived_schema,
                )
            }
            None => {
                // Build a single-item context with the standalone schema (1 entry)
                let header = CapturedHeader {
                    wire_name: &self.register_symbol,
                    value,
                    kind,
                    schema_index: self.standalone_schema_index,
                };
                PdataContextBytes::build_captured(
                    &[header],
                    value.len(),
                    self.standalone_schema.clone(),
                )
            }
        }
    }

    /// Returns (derived_schema, schema_index, entry_slot) for the given input
    /// schema. Uses Arc pointer identity for cache lookup. Bounded to
    /// [`SCHEMA_CACHE_CAPACITY`] with FIFO eviction.
    fn derived_schema(
        &mut self,
        input_schema: &Arc<CompiledHeaderSchema>,
    ) -> Result<(Arc<CompiledHeaderSchema>, u16, u16), ContextBytesError> {
        let version = input_schema.compiled_context().register_file().version();
        if let Some((derived, index, slot)) = self.cache.get(version) {
            return Ok((derived.clone(), *index, *slot));
        }
        let (derived, schema_index, entry_slot) =
            CompiledHeaderSchema::derive_with_entry(input_schema, &self.register_symbol).map_err(
                |_| ContextBytesError::TooMany {
                    what: "schema items",
                },
            )?;
        let _ = self
            .cache
            .insert(version, (derived.clone(), schema_index, entry_slot));
        Ok((derived, schema_index, entry_slot))
    }
}

/// Compatibility name for the initial partition-processor projection binding.
pub type PartitionProjectionBinding = ContextScalarProjectionBinding;

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
    use otel_arrow_dfe_config::context::{ContextCompiler, ContextRegisterField};
    use otel_arrow_dfe_config::transport_headers_policy::{
        CaptureDefaults, CaptureRule, HeaderCapturePolicy, HeaderPropagationPolicy,
        PropagationDefault, PropagationMatch, PropagationOverride, PropagationSelector,
        PropagationSelectorType,
    };

    /// Scenario: register presence spans both sides of a bitmap word boundary.
    /// Guarantees: sizing, mutation, and encoded lookup share the same bit arithmetic.
    #[test]
    fn presence_bitmap_operations_agree_across_words() {
        let mut words = vec![0; PresenceBitmap::word_count(65)];
        for register in [0, 63, 64] {
            PresenceBitmap::set_words(&mut words, register).expect("valid register");
        }
        let mut encoded = vec![0; words.len() * size_of::<u64>()];
        for (index, word) in words.into_iter().enumerate() {
            write_u64(&mut encoded, index * size_of::<u64>(), word).expect("encode word");
        }

        assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 0), Some(true));
        assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 62), Some(false));
        assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 63), Some(true));
        assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 64), Some(true));
    }

    /// Scenario: an envelope builder attempts to write sections out of format order.
    /// Guarantees: the shared writer rejects construction before producing an envelope.
    #[test]
    fn envelope_writer_enforces_section_order() {
        let layout = Layout::new(1, 0, 0, 0).expect("layout");
        let mut writer = EnvelopeWriter::new(layout).expect("writer");

        assert!(writer.entries(|_| Ok(())).is_err());
        assert!(writer.finish().is_err());
    }

    /// Scenario: a record producer supplies compiled fields out of schema order.
    /// Guarantees: the envelope contains numeric fields in canonical order, preserves repeated
    /// values, and stores neither field symbols nor transport provenance.
    #[test]
    fn record_register_encodes_compiled_field_ordinals() {
        let mut compiler = ContextCompiler::new(3);
        let record = compiler
            .declare_record([
                (
                    "tenant_id",
                    ContextRegisterField::scalar(ContextScalarType::Text),
                ),
                (
                    "roles",
                    ContextRegisterField::repeated(ContextScalarType::Text),
                ),
                (
                    "digest",
                    ContextRegisterField::scalar(ContextScalarType::Bytes),
                ),
            ])
            .expect("record shape");
        let register = compiler
            .declare("routing", ContextRegisterShape::Record(record))
            .expect("record register");
        let compiled = compiler.finish();
        let tenant = compiled
            .linker()
            .resolve_field(record, "tenant_id")
            .expect("tenant field");
        let roles = compiled
            .linker()
            .resolve_field(record, "roles")
            .expect("roles field");
        let digest = compiled
            .linker()
            .resolve_field(record, "digest")
            .expect("digest field");

        let context = PdataContextBytes::from_record(
            compiled,
            register,
            [
                ContextRecordValue::new(roles, ContextValueKind::Text, b"reader"),
                ContextRecordValue::new(digest, ContextValueKind::Binary, &[1, 2, 3]),
                ContextRecordValue::new(tenant, ContextValueKind::Text, b"acme"),
                ContextRecordValue::new(roles, ContextValueKind::Text, b"writer"),
            ],
        )
        .expect("record context");

        let fields: Vec<_> = context
            .register(register)
            .expect("present record")
            .record_fields()
            .map(|value| (value.field(), value.kind(), value.value()))
            .collect();
        assert_eq!(
            fields,
            vec![
                (tenant, ContextValueKind::Text, b"acme".as_slice()),
                (roles, ContextValueKind::Text, b"reader".as_slice()),
                (roles, ContextValueKind::Text, b"writer".as_slice()),
                (digest, ContextValueKind::Binary, &[1, 2, 3]),
            ]
        );
        assert!(context.items().all(|item| item.schema_index().is_none()
            && item.wire_name().is_none()
            && item.stored_name().is_none()));
        assert!(
            !context
                .bytes
                .windows(b"tenant_id".len())
                .any(|bytes| bytes == b"tenant_id")
        );
        assert!(
            !context
                .bytes
                .windows(b"roles".len())
                .any(|bytes| bytes == b"roles")
        );
        validate(&context.bytes, context.schema()).expect("valid record envelope");
    }

    /// Scenario: a record producer repeats a scalar field and supplies invalid text bytes.
    /// Guarantees: shape and scalar-type violations are rejected before bytes are emitted.
    #[test]
    fn record_register_rejects_invalid_field_values() {
        let mut compiler = ContextCompiler::new(4);
        let record = compiler
            .declare_record([(
                "tenant",
                ContextRegisterField::scalar(ContextScalarType::Text),
            )])
            .expect("record shape");
        let register = compiler
            .declare("routing", ContextRegisterShape::Record(record))
            .expect("record register");
        let compiled = compiler.finish();
        let tenant = compiled
            .linker()
            .resolve_field(record, "tenant")
            .expect("tenant field");

        assert!(matches!(
            PdataContextBytes::from_record(
                compiled.clone(),
                register,
                [
                    ContextRecordValue::new(tenant, ContextValueKind::Text, b"one"),
                    ContextRecordValue::new(tenant, ContextValueKind::Text, b"two"),
                ],
            ),
            Err(ContextBytesError::DuplicateRecordField { .. })
        ));
        assert!(matches!(
            PdataContextBytes::from_record(
                compiled,
                register,
                [ContextRecordValue::new(
                    tenant,
                    ContextValueKind::Text,
                    &[0xff],
                )],
            ),
            Err(ContextBytesError::RecordFieldTypeMismatch { .. })
        ));
    }

    /// Scenario: an entry has duplicate typed values interleaved with a bag-only header.
    /// Guarantees: bag order is preserved and the entry resolves only its ordered members.
    #[test]
    fn packed_context_indexes_entries_and_bag() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-tenant-bin".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: Some(ValueKindConfig::Binary),
                },
                CaptureRule {
                    match_names: vec!["x-request-id".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");

        let context = PdataContextBytes::capture(
            &policy,
            [
                ("X-Tenant", b"acme".as_slice()),
                ("X-Request-Id", b"request-1".as_slice()),
                ("X-Tenant-Bin", &[0x01, 0x02]),
            ],
        )
        .expect("capture")
        .0
        .expect("context");

        let items: Vec<_> = context.items().collect();
        assert_eq!(items.len(), 3);
        // Wire name preserves transport spelling (occurrence data)
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
        validate(&context.bytes, context.schema()).expect("validates");
    }

    /// Scenario: one captured item occupies the first of several compiled entry slots.
    /// Guarantees: trailing absent entry slots are correctly empty and the envelope validates.
    #[test]
    fn single_item_context_validates_with_trailing_entry_slots() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-region".to_string()],
                    store_as: Some("region".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-extra".to_string()],
                    store_as: Some("extra".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");

        let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
            .expect("capture")
            .0
            .expect("context");

        validate(&context.bytes, context.schema()).expect("validates");
        assert!(context.entry(0).is_some());
        assert!(context.entry(1).is_none());
        assert!(context.entry(2).is_none());
    }

    /// Scenario: a captured header uses a schema-backed stored name.
    /// Guarantees: only the value is inline; wire and stored names resolve from schema.
    #[test]
    fn captured_stored_names_resolve_from_schema() {
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
        validate(&context.bytes, context.schema()).expect("schema-backed context validates");
        let layout = context.layout().expect("layout");
        let item = layout
            .item_descriptor(&context.bytes, 0)
            .expect("item descriptor");

        assert_eq!(item.wire_name, BlobRange { offset: 0, len: 0 });
        assert_eq!(
            context.items().next().and_then(|item| item.wire_name()),
            Some("x-tenant")
        );
        assert_eq!(
            context.items().next().and_then(|item| item.stored_name()),
            Some("x-tenant")
        );
        // Only value bytes are in the blob. The fixed-size register tables are
        // outside the blob.
        assert_eq!(
            context.bytes.len(),
            HeaderFields::LEN
                + size_of::<u64>()
                + EntryFields::LEN
                + ItemFields::LEN
                + MemberFields::LEN
                + b"acme".len()
        );
    }

    /// Scenario: gRPC metadata contains unmatched ASCII plus matched ASCII and binary values.
    /// Guarantees: capture screens by name and preserves decoded binary bytes and entry indexing.
    #[test]
    fn grpc_capture_screens_names_before_preserving_values() {
        use tonic::metadata::{Ascii, Binary, MetadataKey, MetadataMap, MetadataValue};

        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["trace-bin".to_string()],
                    store_as: Some("trace".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");
        let mut metadata = MetadataMap::new();
        let _ = metadata.append(
            "ignored"
                .parse::<MetadataKey<Ascii>>()
                .expect("metadata key"),
            MetadataValue::try_from("skip").expect("metadata value"),
        );
        let _ = metadata.append(
            "x-tenant"
                .parse::<MetadataKey<Ascii>>()
                .expect("metadata key"),
            MetadataValue::try_from("acme").expect("metadata value"),
        );
        let _ = metadata.append_bin(
            "trace-bin"
                .parse::<MetadataKey<Binary>>()
                .expect("metadata key"),
            MetadataValue::from_bytes(&[0x01, 0x02]),
        );

        let context = PdataContextBytes::capture_grpc_metadata(&policy, &metadata)
            .expect("capture")
            .0
            .expect("context");
        let items: Vec<_> = context.items().collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].stored_name(), Some("tenant"));
        assert_eq!(
            items[1].value(),
            Some((HeaderValueKind::Binary, &[0x01, 0x02][..]))
        );
        assert_eq!(context.entry(0).expect("tenant entry").values().count(), 1);
        assert_eq!(context.entry(1).expect("trace entry").values().count(), 1);
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
            context.items().next().and_then(|item| item.wire_name()),
            Some("X-Tenant")
        );
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

    /// Scenario: schema-backed capture produces a context from a compiled policy.
    /// Guarantees: the item carries a schema_index and the schema Arc is retained.
    #[test]
    fn schema_backed_capture_retains_schema() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
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

        assert!(Arc::ptr_eq(context.schema(), policy.schema()));
        let item = context.items().next().expect("one item");
        assert_eq!(item.schema_index(), Some(0));
        assert_eq!(item.stored_name(), Some("tenant"));
    }

    /// Scenario: a register is captured for an explicitly named egress mapping.
    /// Guarantees: observed transport spelling is omitted from context bytes.
    #[test]
    fn capture_omits_unrequested_transport_name_provenance() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile_for_generation_with_provenance(4, false)
        .expect("capture policy");
        let context = PdataContextBytes::capture(&policy, [("X-Tenant", b"acme".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let item = context.items().next().expect("captured register value");

        assert!(item.uses_schema_wire_name());
        assert_eq!(item.wire_name(), Some("x-tenant"));
        assert_eq!(
            context
                .schema()
                .compiled_context()
                .register_file()
                .version()
                .deployment_generation(),
            4
        );
    }

    /// Scenario: propagation maps a logical register to an explicit output header.
    /// Guarantees: egress uses its compiled constant without reading a transport name from context.
    #[test]
    fn propagation_uses_explicit_compiled_output_name() {
        let capture = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-input-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile_for_generation_with_provenance(8, false)
        .expect("capture policy");
        let context =
            PdataContextBytes::capture(&capture, [("X-Input-Tenant", b"acme".as_slice())])
                .expect("capture")
                .0
                .expect("context");
        let propagation = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::Named,
                    named: Some(vec!["tenant".to_string()]),
                },
                output_name: Some("x-output-tenant".to_string()),
                ..PropagationDefault::default()
            },
            vec![],
        )
        .compile()
        .expect("propagation policy")
        .compile_schema(context.schema());

        let propagated = context.propagate(&propagation).collect::<Vec<_>>();

        assert_eq!(propagated.len(), 1);
        assert_eq!(propagated[0].header_name, "x-output-tenant");
        assert_eq!(propagated[0].value, b"acme");
    }

    /// Scenario: a value binding evaluates contexts sharing and changing schemas.
    /// Guarantees: lookup preserves first-value order without runtime name matching.
    #[test]
    fn context_value_binding_resolves_schema_indices() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-tenant-bin".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: Some(ValueKindConfig::Binary),
                },
                CaptureRule {
                    match_names: vec!["x-region".to_string()],
                    store_as: Some("region".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");
        let context = PdataContextBytes::capture(
            &policy,
            [
                ("X-Tenant", b"first".as_slice()),
                ("x-tenant-bin", b"second".as_slice()),
            ],
        )
        .expect("capture")
        .0
        .expect("context");
        let other = PdataContextBytes::capture(&policy, [("x-region", b"west".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let mut binding = ContextValueBinding::new("TENANT");

        assert_eq!(
            binding.value(&context),
            Some((HeaderValueKind::Text, b"first".as_slice()))
        );
        assert_eq!(binding.value(&other), None);
        assert_eq!(
            binding.value(&context),
            Some((HeaderValueKind::Text, b"first".as_slice()))
        );
    }

    /// Scenario: an entry-set binding evaluates equivalent entries across different schemas.
    /// Guarantees: present entries are visited in configuration order, independent of schema order.
    #[test]
    fn context_entry_set_binding_uses_configuration_order_across_schemas() {
        let compile = |rules| {
            HeaderCapturePolicy::new(CaptureDefaults::default(), rules)
                .compile()
                .expect("capture policy")
        };
        let tenant = || CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: Some("tenant".to_string()),
            sensitive: false,
            value_kind: None,
        };
        let region = || CaptureRule {
            match_names: vec!["x-region".to_string()],
            store_as: Some("region".to_string()),
            sensitive: false,
            value_kind: None,
        };
        let first_policy = compile(vec![tenant(), region()]);
        let second_policy = compile(vec![region(), tenant()]);
        let first = PdataContextBytes::capture(
            &first_policy,
            [
                ("x-tenant", b"acme".as_slice()),
                ("x-region", b"west".as_slice()),
            ],
        )
        .expect("capture")
        .0
        .expect("context");
        let second = PdataContextBytes::capture(
            &second_policy,
            [
                ("x-region", b"west".as_slice()),
                ("x-tenant", b"acme".as_slice()),
            ],
        )
        .expect("capture")
        .0
        .expect("context");
        let mut binding = ContextEntrySetBinding::new(["REGION", "TENANT"]);
        let collect = |binding: &mut ContextEntrySetBinding, context: &PdataContextBytes| {
            let mut visited = Vec::new();
            let count = binding.visit_present(context, |ordinal, entry| {
                visited.push((
                    ordinal,
                    entry
                        .values()
                        .map(|(_, value)| value.to_vec())
                        .collect::<Vec<_>>(),
                ));
            });
            (count, visited)
        };

        let expected = (
            2,
            vec![(0, vec![b"west".to_vec()]), (1, vec![b"acme".to_vec()])],
        );
        assert_eq!(collect(&mut binding, &first), expected);
        assert_eq!(collect(&mut binding, &second), expected);
        assert_eq!(collect(&mut binding, &first), expected);
    }

    /// Scenario: configured entries are absent from either the schema or the captured message.
    /// Guarantees: missing and absent entries are skipped without hiding a present entry.
    #[test]
    fn context_entry_set_binding_skips_missing_and_absent_entries() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-region".to_string()],
                    store_as: Some("region".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");
        let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let mut binding = ContextEntrySetBinding::new(["unknown", "region", "tenant"]);
        let mut visited = Vec::new();

        let count = binding.visit_present(&context, |ordinal, _| visited.push(ordinal));

        assert_eq!(count, 1);
        assert_eq!(visited, [2]);
    }

    /// Scenario: a captured exact-name header has no explicit logical alias.
    /// Guarantees: the compiler still assigns a register and eliminates its source name.
    #[test]
    fn context_entry_set_binding_selects_unaliased_exact_header() {
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
        let mut binding = ContextEntrySetBinding::new(["x-tenant"]);
        let mut values = Vec::new();
        let register = context
            .schema()
            .compiled_context()
            .linker()
            .resolve("x-tenant")
            .expect("compiled register");

        let count = binding.visit_present(&context, |ordinal, entry| {
            values.push((
                ordinal,
                entry
                    .values()
                    .map(|(_, value)| value.to_vec())
                    .collect::<Vec<_>>(),
            ));
        });

        assert_eq!(count, 1);
        assert_eq!(values, [(0, vec![b"acme".to_vec()])]);
        assert_eq!(
            context
                .schema()
                .compiled_context()
                .register_file()
                .shape(register),
            Some(&ContextRegisterShape::KeyValueList(
                ContextScalarType::AnyValue
            ))
        );
        assert_eq!(
            context
                .register(register)
                .expect("present register")
                .key_values()
                .collect::<Vec<_>>(),
            vec![("x-tenant", HeaderValueKind::Text, b"acme".as_slice())]
        );
    }

    /// Scenario: projection preserves input entries and appends a new singleton entry.
    /// Guarantees: old entries/hashes/items are unchanged; the new entry slot is present,
    ///   contains the projected value, and uses the derived schema index.
    #[test]
    fn projection_preserves_input_and_appends_entry() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-request-id".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");
        let input = PdataContextBytes::capture(
            &policy,
            [
                ("x-tenant", b"acme".as_slice()),
                ("x-request-id", b"req-1".as_slice()),
            ],
        )
        .expect("capture")
        .0
        .expect("context");
        let old_hash = input.entry(0).expect("tenant entry").hash();

        let mut binding = PartitionProjectionBinding::new("partition");
        let output = binding
            .project(Some(&input), b"west", HeaderValueKind::Text)
            .expect("projection");

        // Old entry hash preserved
        assert_eq!(output.entry(0).expect("tenant entry").hash(), old_hash);
        // Original items preserved with original schema indices
        assert_eq!(
            output.items().nth(0).and_then(|i| i.schema_index()),
            Some(0)
        );
        assert_eq!(
            output.items().nth(1).and_then(|i| i.schema_index()),
            Some(1)
        );
        // Appended item has derived schema index and entry slot
        let appended = output.items().nth(2).expect("appended item");
        assert_eq!(appended.schema_index(), Some(2));
        assert_eq!(appended.entry_slot(), Some(2));
        assert_eq!(appended.wire_name(), Some("partition"));
        assert_eq!(
            appended.value(),
            Some((HeaderValueKind::Text, b"west".as_slice()))
        );
        // New entry is present and contains the projected value
        let new_entry = output.entry(2).expect("partition entry must be present");
        assert_eq!(
            new_entry.values().collect::<Vec<_>>(),
            vec![(HeaderValueKind::Text, b"west".as_slice())]
        );
        // Output schema is different from input schema
        assert!(!Arc::ptr_eq(output.schema(), input.schema()));
        assert_eq!(output.schema().len(), 3);
    }

    /// Scenario: projection with no input creates a singleton entry context.
    /// Guarantees: the context has one item in one present entry, using the
    ///   standalone schema (not schema-less).
    #[test]
    fn projection_without_input_creates_singleton_entry() {
        let mut binding = PartitionProjectionBinding::new("partition");
        let output = binding
            .project(None, b"east", HeaderValueKind::Text)
            .expect("projection");

        assert_eq!(output.items().count(), 1);
        let item = output.items().next().expect("one item");
        assert_eq!(item.schema_index(), Some(0));
        assert_eq!(item.entry_slot(), Some(0));
        assert_eq!(item.wire_name(), Some("partition"));
        assert_eq!(
            item.value(),
            Some((HeaderValueKind::Text, b"east".as_slice()))
        );
        // The singleton entry is present
        let entry = output.entry(0).expect("singleton entry must be present");
        assert_eq!(
            entry.values().collect::<Vec<_>>(),
            vec![(HeaderValueKind::Text, b"east".as_slice())]
        );
        assert_eq!(output.schema().len(), 1);
    }

    /// Scenario: a mixed-case register symbol is projected with and without input context.
    /// Guarantees: both projection paths expose the canonical compile-time symbol.
    #[test]
    fn projection_canonicalizes_configured_register_symbol() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-input".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile()
        .expect("capture policy");
        let input = PdataContextBytes::capture(&policy, [("x-input", b"value".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let mut binding = PartitionProjectionBinding::new("X-Partition");

        let standalone = binding
            .project(None, b"east", HeaderValueKind::Text)
            .expect("standalone projection");
        let appended = binding
            .project(Some(&input), b"west", HeaderValueKind::Text)
            .expect("appended projection");

        assert_eq!(
            standalone.items().next().and_then(|item| item.wire_name()),
            Some("x-partition")
        );
        assert_eq!(
            appended.items().nth(1).and_then(|item| item.wire_name()),
            Some("x-partition")
        );
    }

    /// Scenario: distinct input schemas produce isolated derived schemas in the binding cache.
    /// Guarantees: two different input schemas produce different derived schemas with
    ///   independent schema_index and entry_slot assignments.
    #[test]
    fn distinct_input_schemas_produce_isolated_derived_schemas() {
        let policy_a = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec!["x-a".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile()
        .expect("policy a");
        let policy_b = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-b1".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["x-b2".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("policy b");

        let ctx_a = PdataContextBytes::capture(&policy_a, [("x-a", b"val-a".as_slice())])
            .expect("capture a")
            .0
            .expect("context a");
        let ctx_b = PdataContextBytes::capture(
            &policy_b,
            [
                ("x-b1", b"val-b1".as_slice()),
                ("x-b2", b"val-b2".as_slice()),
            ],
        )
        .expect("capture b")
        .0
        .expect("context b");

        let mut binding = PartitionProjectionBinding::new("part");
        let out_a = binding
            .project(Some(&ctx_a), b"1", HeaderValueKind::Text)
            .expect("project a");
        let out_b = binding
            .project(Some(&ctx_b), b"2", HeaderValueKind::Text)
            .expect("project b");

        // Derived schemas are distinct
        assert!(!Arc::ptr_eq(out_a.schema(), out_b.schema()));
        // Schema A has 1 original + 1 appended = 2
        assert_eq!(out_a.schema().len(), 2);
        // Schema B has 2 original + 1 appended = 3
        assert_eq!(out_b.schema().len(), 3);
        // Appended item index differs
        assert_eq!(out_a.items().nth(1).and_then(|i| i.schema_index()), Some(1));
        assert_eq!(out_b.items().nth(2).and_then(|i| i.schema_index()), Some(2));
        // The projected register follows each input register file.
        let entry_a = out_a.entry(1).expect("partition entry in a");
        assert_eq!(
            entry_a.values().collect::<Vec<_>>(),
            vec![(HeaderValueKind::Text, b"1".as_slice())]
        );
        let entry_b = out_b.entry(2).expect("partition entry in b");
        assert_eq!(
            entry_b.values().collect::<Vec<_>>(),
            vec![(HeaderValueKind::Text, b"2".as_slice())]
        );
    }

    /// Scenario: a near-full context cannot fit the projected partition header.
    /// Guarantees: projection returns TooLarge error without corrupting the input.
    #[test]
    fn projection_overflow_remains_error() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults {
                max_value_bytes: 65_470,
                ..CaptureDefaults::default()
            },
            vec![CaptureRule {
                match_names: vec!["x".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile()
        .expect("capture policy");
        // Create a context near the 64 KiB limit
        let big_value = vec![0u8; 65_470];
        let input = PdataContextBytes::capture(&policy, [("x", big_value.as_slice())])
            .expect("capture")
            .0
            .expect("context");

        let mut binding = PartitionProjectionBinding::new("partition");
        let result = binding.project(Some(&input), b"overflow-value", HeaderValueKind::Text);
        assert!(
            result.is_err(),
            "projection should fail on near-limit context"
        );
    }

    /// Scenario: named propagation selects one entry and drops another item by override.
    /// Guarantees: propagation applies selector, override, and stored-name semantics in place.
    #[test]
    fn packed_propagation_applies_named_selector_and_override() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["authorization".to_string()],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
            ],
        )
        .compile()
        .expect("capture policy");
        let context = PdataContextBytes::capture(
            &policy,
            [
                ("X-Tenant", b"acme".as_slice()),
                ("Authorization", b"secret".as_slice()),
            ],
        )
        .expect("capture")
        .0
        .expect("context");
        let propagation = HeaderPropagationPolicy::new(
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
                output_name: None,
                on_error: None,
            }],
        )
        .compile()
        .expect("propagation policy");
        let propagation = propagation.compile_schema(context.schema());

        let propagated: Vec<_> = context.propagate(&propagation).collect();
        assert_eq!(propagated.len(), 1);
        assert_eq!(propagated[0].header_name, "tenant");
        assert_eq!(propagated[0].value, b"acme");
        assert_eq!(propagated[0].value_kind, HeaderValueKind::Text);
    }

    /// Scenario: the binding cache is hit on repeated projections with the same input schema.
    /// Guarantees: the same derived schema Arc is reused (pointer equality).
    #[test]
    fn binding_cache_reuses_derived_schema() {
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
        let ctx1 = PdataContextBytes::capture(&policy, [("x-tenant", b"a".as_slice())])
            .expect("capture")
            .0
            .expect("context");
        let ctx2 = PdataContextBytes::capture(&policy, [("x-tenant", b"b".as_slice())])
            .expect("capture")
            .0
            .expect("context");

        let mut binding = PartitionProjectionBinding::new("part");
        let out1 = binding
            .project(Some(&ctx1), b"1", HeaderValueKind::Text)
            .expect("project 1");
        let out2 = binding
            .project(Some(&ctx2), b"2", HeaderValueKind::Text)
            .expect("project 2");

        // Same input schema Arc -> same derived schema Arc
        assert!(Arc::ptr_eq(out1.schema(), out2.schema()));
    }

    /// Scenario: the binding cache exceeds SCHEMA_CACHE_CAPACITY distinct schemas.
    /// Guarantees: the oldest entry is evicted (FIFO) and the binding still produces
    ///   correct projections for both cached and evicted schemas.
    #[test]
    fn binding_cache_evicts_oldest_when_full() {
        // Create SCHEMA_CACHE_CAPACITY + 1 distinct policies/schemas
        let policies: Vec<_> = (0..SCHEMA_CACHE_CAPACITY + 1)
            .map(|i| {
                HeaderCapturePolicy::new(
                    CaptureDefaults::default(),
                    vec![CaptureRule {
                        match_names: vec![format!("x-h{i}")],
                        store_as: None,
                        sensitive: false,
                        value_kind: None,
                    }],
                )
                .compile()
                .expect("capture policy")
            })
            .collect();
        let contexts: Vec<_> = policies
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let name = format!("x-h{i}");
                PdataContextBytes::capture(p, [(name.as_str(), b"v".as_slice())])
                    .expect("capture")
                    .0
                    .expect("context")
            })
            .collect();

        let mut binding = PartitionProjectionBinding::new("part");

        // Fill the cache to capacity
        let mut schemas: Vec<Arc<CompiledHeaderSchema>> =
            Vec::with_capacity(SCHEMA_CACHE_CAPACITY + 1);
        for ctx in &contexts[..SCHEMA_CACHE_CAPACITY] {
            let out = binding
                .project(Some(ctx), b"x", HeaderValueKind::Text)
                .expect("project");
            schemas.push(out.schema().clone());
        }

        // The first schema is still cached
        let out_first = binding
            .project(Some(&contexts[0]), b"y", HeaderValueKind::Text)
            .expect("project first again");
        assert!(Arc::ptr_eq(out_first.schema(), &schemas[0]));

        // Add one more distinct schema -- should evict the first
        let out_overflow = binding
            .project(
                Some(&contexts[SCHEMA_CACHE_CAPACITY]),
                b"z",
                HeaderValueKind::Text,
            )
            .expect("project overflow");
        assert_eq!(out_overflow.schema().len(), 2);

        // The first schema is now evicted -- re-projection produces a new Arc
        let out_first_again = binding
            .project(Some(&contexts[0]), b"w", HeaderValueKind::Text)
            .expect("project first after eviction");
        assert!(
            !Arc::ptr_eq(out_first_again.schema(), &schemas[0]),
            "evicted schema should produce a fresh derived Arc"
        );
        // But the result is still correct
        let entry = out_first_again
            .entry(1)
            .expect("partition entry after eviction");
        assert_eq!(
            entry.values().collect::<Vec<_>>(),
            vec![(HeaderValueKind::Text, b"w".as_slice())]
        );
    }
}
