use super::*;

/// Immutable encoded pdata context.
#[derive(Clone, PartialEq, Eq)]
pub struct PdataContextBytes {
    pub(super) bytes: Arc<ContextStorage>,
}

#[derive(PartialEq, Eq)]
pub(super) struct ContextStorage {
    pub(super) encoded: Vec<u8>,
    pub(super) schema: Arc<CompiledHeaderSchema>,
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
    pub(super) fn project(&self) -> ContextProjectionAccumulator<'_> {
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

    pub(super) fn layout(&self) -> Result<Layout, ContextBytesError> {
        Layout::read(&self.bytes).ok_or(ContextBytesError::InvalidEnvelope)
    }

    pub(super) fn item_with_layout(&self, index: usize, layout: Layout) -> Option<ContextItem<'_>> {
        Some(ContextItem {
            context: self,
            layout,
            descriptor_at: layout.item_offset(index).ok()?,
        })
    }

    pub(super) fn blob_bytes(&self, layout: Layout, range: BlobRange) -> Option<&[u8]> {
        range.slice(layout.blob(&self.bytes)?)
    }
}

/// Borrowed view of one packed context item.
#[derive(Clone, Copy, Debug)]
pub struct ContextItem<'a> {
    pub(super) context: &'a PdataContextBytes,
    pub(super) layout: Layout,
    pub(super) descriptor_at: usize,
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

    pub(super) fn bytes(&self, field: BlobRangeField) -> Option<&'a [u8]> {
        self.context.blob_bytes(
            self.layout,
            field.read(&self.context.bytes, self.descriptor_at)?,
        )
    }

    pub(super) fn text(&self, field: BlobRangeField) -> Option<&'a str> {
        std::str::from_utf8(self.bytes(field)?).ok()
    }

    pub(super) fn schema_item(&self) -> Option<CompiledHeaderSchemaItemRef<'a>> {
        let id = self.schema_index()?;
        self.context.bytes.schema().item(id)
    }
}

/// Iterator over bag items in arrival order.
pub struct ContextItems<'a> {
    pub(super) context: &'a PdataContextBytes,
    pub(super) layout: Option<Layout>,
    pub(super) next: usize,
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
    pub(super) items: ContextItems<'a>,
    pub(super) plan: &'a CompiledSchemaPropagation,
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
    pub(super) context: &'a PdataContextBytes,
    pub(super) layout: Layout,
    pub(super) register: ContextRegisterId,
    pub(super) descriptor: EntryDescriptor,
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

pub(super) struct ContextProjectionAccumulator<'a> {
    pub(super) input: &'a PdataContextBytes,
}

impl ContextProjectionAccumulator<'_> {
    /// Copies the input envelope and appends one schema-backed item with a new
    /// singleton entry slot.
    ///
    /// Extends entry_count by 1, sets presence for the new entry, appends its
    /// EntryDescriptor and member, and appends the item/value. Preserves all
    /// existing entries, members, items, hashes, and blob content.
    pub(super) fn copy_and_append_entry_item(
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
