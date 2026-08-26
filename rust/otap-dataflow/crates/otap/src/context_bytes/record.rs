use super::*;

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
