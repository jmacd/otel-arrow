use super::*;

pub(super) enum CapturedValue<'a> {
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

pub(super) struct CapturedHeader<'a, V> {
    // Name observed on the transport.
    pub(super) wire_name: &'a str,
    // Borrowed or owned raw header bytes.
    pub(super) value: V,
    // Text or binary transport semantics.
    pub(super) kind: HeaderValueKind,
    // Index of the ingress instruction and register in the retained schema.
    pub(super) schema_index: u16,
}

impl<'a, V> CapturedHeader<'a, V> {
    pub(super) fn schema_item<'s>(
        &self,
        schema: &'s CompiledHeaderSchema,
    ) -> Result<CompiledHeaderSchemaItemRef<'s>, ContextBytesError> {
        schema
            .item(self.schema_index)
            .ok_or(ContextBytesError::InvalidEnvelope)
    }

    pub(super) fn wire_name_occurrence(
        &self,
        schema_item: CompiledHeaderSchemaItemRef<'_>,
    ) -> Option<&'a str> {
        (schema_item.retain_observed_name && schema_item.wire_name != self.wire_name)
            .then_some(self.wire_name)
    }
}

impl<V: AsRef<[u8]>> CapturedHeader<'_, V> {
    pub(super) fn entry(
        &self,
        schema: &CompiledHeaderSchema,
    ) -> Result<Option<u16>, ContextBytesError> {
        Ok(self.schema_item(schema)?.register.map(|id| id.as_u16()))
    }

    pub(super) fn encoded_len(
        &self,
        schema: &CompiledHeaderSchema,
    ) -> Result<usize, ContextBytesError> {
        let schema_item = self.schema_item(schema)?;
        let wire_name_len = self.wire_name_occurrence(schema_item).map_or(0, str::len);
        wire_name_len
            .checked_add(self.value.as_ref().len())
            .ok_or(ContextBytesError::TooLarge)
    }

    pub(super) fn write_blob(
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

    pub(super) fn capture_candidates<'a, V: AsRef<[u8]>>(
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

    pub(super) fn build_captured<V: AsRef<[u8]>>(
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

    pub(super) fn from_vec(bytes: Vec<u8>, schema: Arc<CompiledHeaderSchema>) -> Self {
        Self {
            bytes: Arc::new(context::ContextStorage {
                encoded: bytes,
                schema,
            }),
        }
    }
}

#[derive(Default)]
pub(super) struct SkippedHeaders {
    pub(super) max_entries: usize,
    pub(super) name_too_long: usize,
    pub(super) value_too_long: usize,
    pub(super) context_too_large: usize,
}

impl SkippedHeaders {
    pub(super) fn into_stats(self) -> Option<CaptureStats> {
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

pub(super) struct EntryIndex {
    pub(super) presence: smallvec::SmallVec<[u64; 2]>,
    pub(super) entries: smallvec::SmallVec<[EntryBuild; 16]>,
    pub(super) members: smallvec::SmallVec<[MemberDescriptor; 32]>,
}

#[derive(Clone, Copy)]
pub(super) struct EntryBuild {
    pub(super) first_member: usize,
    pub(super) member_count: usize,
    pub(super) next_member: usize,
    pub(super) hash: u64,
}

impl EntryIndex {
    pub(super) fn new<V: AsRef<[u8]>>(
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

    pub(super) fn member_count(&self) -> usize {
        self.members.len()
    }

    pub(super) fn write_presence(&self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
        for (index, word) in self.presence.iter().copied().enumerate() {
            write_u64(bytes, index * size_of::<u64>(), word)?;
        }
        Ok(())
    }

    pub(super) fn write_entries(&self, bytes: &mut [u8]) -> Result<(), ContextBytesError> {
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

pub(super) fn copy_section(
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
