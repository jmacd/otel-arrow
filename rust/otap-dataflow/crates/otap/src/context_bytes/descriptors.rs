use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct MemberDescriptor {
    pub(super) item: u16,
    pub(super) field: Option<ContextFieldId>,
}

impl MemberDescriptor {
    pub(super) fn read(bytes: &[u8], at: usize) -> Option<Self> {
        let field = MemberFields::FIELD.read(bytes, at)?;
        Some(Self {
            item: MemberFields::ITEM.read(bytes, at)?,
            field: (field != NO_FIELD).then(|| ContextFieldId::from_u16(field)),
        })
    }

    pub(super) fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        MemberFields::ITEM.write(bytes, at, self.item)?;
        MemberFields::FIELD.write(
            bytes,
            at,
            self.field.map_or(NO_FIELD, ContextFieldId::as_u16),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct EntryDescriptor {
    pub(super) first_member: usize,
    pub(super) member_count: usize,
    pub(super) hash: u64,
}

impl EntryDescriptor {
    pub(super) fn read(bytes: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            first_member: usize::from(EntryFields::FIRST_MEMBER.read(bytes, at)?),
            member_count: usize::from(EntryFields::MEMBER_COUNT.read(bytes, at)?),
            hash: EntryFields::HASH.read(bytes, at)?,
        })
    }

    pub(super) fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        EntryFields::FIRST_MEMBER.write_usize(bytes, at, self.first_member)?;
        EntryFields::MEMBER_COUNT.write_usize(bytes, at, self.member_count)?;
        EntryFields::HASH.write(bytes, at, self.hash)
    }

    pub(super) fn members(self) -> Range<usize> {
        self.first_member..self.first_member + self.member_count
    }

    #[cfg(test)]
    pub(super) fn valid_for(self, member_count: usize) -> bool {
        self.first_member
            .checked_add(self.member_count)
            .is_some_and(|end| end <= member_count)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ItemDescriptor {
    pub(super) schema_index: u16,
    pub(super) kind: HeaderValueKind,
    pub(super) wire_name: BlobRange,
    pub(super) value: BlobRange,
}

impl ItemDescriptor {
    pub(super) fn for_captured<V: AsRef<[u8]>>(
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
    pub(super) fn read(bytes: &[u8], at: usize) -> Option<Self> {
        Some(Self {
            schema_index: ItemFields::SCHEMA_INDEX.read(bytes, at)?,
            kind: HeaderValueKind::decode(ItemFields::KIND.read(bytes, at)?)?,
            wire_name: ItemFields::WIRE_NAME.read(bytes, at)?,
            value: ItemFields::VALUE.read(bytes, at)?,
        })
    }

    pub(super) fn write(self, bytes: &mut [u8], at: usize) -> Result<(), ContextBytesError> {
        ItemFields::SCHEMA_INDEX.write(bytes, at, self.schema_index)?;
        ItemFields::KIND.write(bytes, at, self.kind as u8)?;
        ItemFields::_PAD.write(bytes, at, 0)?;
        ItemFields::WIRE_NAME.write(bytes, at, self.wire_name)?;
        ItemFields::VALUE.write(bytes, at, self.value)
    }

    #[cfg(test)]
    pub(super) fn valid_for(
        self,
        layout: Layout,
        blob: &[u8],
        schema: &CompiledHeaderSchema,
    ) -> bool {
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
pub(super) struct BlobRange {
    pub(super) offset: usize,
    pub(super) len: usize,
}

impl BlobRange {
    pub(super) fn end(self) -> Option<usize> {
        self.offset.checked_add(self.len)
    }

    pub(super) fn slice(self, blob: &[u8]) -> Option<&[u8]> {
        blob.get(self.offset..self.end()?)
    }

    #[cfg(test)]
    pub(super) fn text(self, blob: &[u8]) -> Option<&str> {
        std::str::from_utf8(self.slice(blob)?).ok()
    }
}

#[cfg(test)]
pub(super) fn validate(
    bytes: &[u8],
    schema: &CompiledHeaderSchema,
) -> Result<(), ContextBytesError> {
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
pub(super) fn validate_unused_presence_bits(
    bytes: &[u8],
    layout: Layout,
) -> Result<(), ContextBytesError> {
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
