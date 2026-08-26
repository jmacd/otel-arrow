use super::*;

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
    pub(super) fn captured(config: Option<ValueKindConfig>, wire_name: &str) -> Self {
        match config {
            Some(ValueKindConfig::Binary) => Self::Binary,
            Some(ValueKindConfig::Text) => Self::Text,
            None if wire_name.ends_with("-bin") => Self::Binary,
            None => Self::Text,
        }
    }

    pub(super) fn decode(value: u8) -> Option<Self> {
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
    pub(super) field: ContextFieldId,
    pub(super) kind: ContextValueKind,
    pub(super) value: &'a [u8],
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
