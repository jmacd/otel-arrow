use super::*;

pub(super) const fn entry_hash_seed() -> u64 {
    0xcbf2_9ce4_8422_2325_u64
}

pub(super) enum RecordConstraintError {
    InvalidField(ContextFieldId),
    DuplicateField(ContextFieldId),
    TypeMismatch(ContextFieldId),
    OutOfOrder,
}

impl RecordConstraintError {
    pub(super) fn into_context_error(self, register: ContextRegisterId) -> ContextBytesError {
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

pub(super) struct RecordConstraints<'a> {
    record: &'a ContextRecordShape,
    scalar_seen: Vec<bool>,
    previous_field: Option<ContextFieldId>,
    require_order: bool,
}

impl<'a> RecordConstraints<'a> {
    pub(super) fn new(record: &'a ContextRecordShape, require_order: bool) -> Self {
        Self {
            record,
            scalar_seen: vec![false; record.fields().len()],
            previous_field: None,
            require_order,
        }
    }

    pub(super) fn accept(
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

pub(super) fn scalar_value_matches(
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

pub(super) fn record_hash(values: &[ContextRecordValue<'_>]) -> Result<u64, ContextBytesError> {
    let mut hash = entry_hash_seed();
    for value in values {
        record_hash_value(&mut hash, value.field, value.kind, value.value)?;
    }
    Ok(hash)
}

pub(super) fn record_hash_value(
    hash: &mut u64,
    field: ContextFieldId,
    kind: ContextValueKind,
    value: &[u8],
) -> Result<(), ContextBytesError> {
    hash_bytes(hash, &field.as_u16().to_le_bytes());
    entry_hash_value(hash, kind, value)
}

pub(super) fn entry_hash_value(
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

pub(super) fn entry_hash_for_single(
    kind: HeaderValueKind,
    value: &[u8],
) -> Result<u64, ContextBytesError> {
    let mut hash = entry_hash_seed();
    entry_hash_value(&mut hash, kind, value)?;
    Ok(hash)
}

pub(super) fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
}
