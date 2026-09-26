// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compiled mixed-source request context projection.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use otel_arrow_dfe_config::ContextEntryName;
use otel_arrow_dfe_config::context_layout::ContextSource;
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::transport_headers::ValueKind;
use otel_arrow_dfe_engine::capability::auth::ClaimValue;
pub use otel_arrow_dfe_engine::context_declaration::CompiledContextLayout as CompiledLayout;
use smallvec::SmallVec;

const HEADER_SIZE: usize = 16;
const FIELD_SIZE: usize = 16;
const VALUE_SIZE: usize = 24;
const NONE: u32 = u32::MAX;

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidUserConfig {
        error: message.into(),
    }
}

fn field(layout: &CompiledLayout, source: ContextSource, name: &str) -> Result<usize, Error> {
    layout
        .layout()
        .fields()
        .iter()
        .position(|field| field.source == source && field.name.as_str() == name)
        .ok_or_else(|| invalid(format!("uncompiled {source:?} field `{name}`")))
}

/// Borrowed transport value before packing. Original-name retention is explicit.
#[derive(Clone, Copy)]
pub struct CapturedHeader<'a> {
    /// Stored name selected by the capture policy.
    pub name: &'a str,
    /// Retained wire name, when different from the stored name.
    pub original_name: Option<&'a str>,
    /// Text or binary.
    pub kind: ValueKind,
    /// Raw value.
    pub value: &'a [u8],
}

/// Borrowed verified claim before packing.
pub struct CapturedClaim<'a> {
    /// Configured destination name.
    pub name: &'a str,
    /// Preserves single-versus-many cardinality, including empty many.
    pub value: &'a ClaimValue,
}

/// All source values and compiled metadata in one shared allocation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackedContext {
    bytes: Option<Arc<[u8]>>,
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(
        bytes[at..at + 4]
            .try_into()
            .expect("packed descriptor was constructed in bounds"),
    )
}

fn write_u32(bytes: &mut [u8], at: usize, value: usize) {
    bytes[at..at + 4].copy_from_slice(
        &u32::try_from(value)
            .expect("packed context length was checked")
            .to_le_bytes(),
    );
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(
        bytes[at..at + 8]
            .try_into()
            .expect("packed descriptor was constructed in bounds"),
    )
}

fn write_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

const fn presence_len(entry_count: usize) -> usize {
    entry_count.div_ceil(8)
}

const fn entry_hashes_at(entry_count: usize) -> usize {
    HEADER_SIZE + presence_len(entry_count)
}

const fn fields_at(entry_count: usize) -> usize {
    entry_hashes_at(entry_count) + entry_count * 8
}

fn entry_present(bytes: &[u8], entry: usize) -> bool {
    bytes[HEADER_SIZE + entry / 8] & (1 << (entry % 8)) != 0
}

fn set_entry_present(bytes: &mut [u8], entry: usize) {
    bytes[HEADER_SIZE + entry / 8] |= 1 << (entry % 8);
}

impl PackedContext {
    /// Pack borrowed header and claim values with no temporary heap allocation.
    ///
    /// Sources can arrive in any order. Per-field chains preserve value order,
    /// including interleaved repeated headers. Uncompiled sources are errors.
    pub fn pack(
        layout: &CompiledLayout,
        headers: &[CapturedHeader<'_>],
        claims: &[CapturedClaim<'_>],
    ) -> Result<Self, Error> {
        if headers.is_empty() && claims.is_empty() {
            return Ok(Self::default());
        }
        let mut count = headers.len();
        let mut blob_len = 0usize;
        for header in headers {
            _ = field(layout, ContextSource::TransportHeader, header.name)?;
            blob_len = blob_len
                .checked_add(header.value.len())
                .and_then(|len| len.checked_add(header.original_name.map_or(0, str::len)))
                .ok_or_else(|| invalid("context blob size overflow"))?;
        }
        for claim in claims {
            _ = field(layout, ContextSource::AuthorizedIdentity, claim.name)?;
            count = count
                .checked_add(claim.value.as_slice().len())
                .ok_or_else(|| invalid("context value count overflow"))?;
            for value in claim.value.as_slice() {
                blob_len = blob_len
                    .checked_add(value.len())
                    .ok_or_else(|| invalid("context blob size overflow"))?;
            }
        }
        let entry_count = layout.layout().entries().len();
        let fields_at = fields_at(entry_count);
        let values_at = layout
            .layout()
            .fields()
            .len()
            .checked_mul(FIELD_SIZE)
            .and_then(|len| fields_at.checked_add(len))
            .ok_or_else(|| invalid("context fields exceed packed size"))?;
        let blob_at = count
            .checked_mul(VALUE_SIZE)
            .and_then(|len| values_at.checked_add(len))
            .ok_or_else(|| invalid("context values exceed packed size"))?;
        let total = blob_at
            .checked_add(blob_len)
            .ok_or_else(|| invalid("context blob exceeds packed size"))?;
        if total > u32::MAX as usize || count > u32::MAX as usize {
            return Err(invalid("context exceeds 32-bit packed range"));
        }

        // Small requests use stack scratch space; only larger requests spill.
        let mut scratch = SmallVec::<[u8; 512]>::from_elem(0, total);
        let data = scratch.as_mut_slice();
        write_u64(data, 0, layout.generation());
        write_u32(data, 8, headers.len());
        write_u32(data, 12, claims.len());

        let mut value_index = 0usize;
        let mut blob_index = blob_at;
        for header in headers {
            let field = field(layout, ContextSource::TransportHeader, header.name)?;
            Self::write_value(
                data,
                fields_at,
                values_at,
                &mut value_index,
                &mut blob_index,
                field,
                header.kind as u8,
                header.original_name,
                header.value,
            );
        }
        for claim in claims {
            let field = field(layout, ContextSource::AuthorizedIdentity, claim.name)?;
            let field_at = fields_at + field * FIELD_SIZE;
            data[field_at + 8] = 1;
            data[field_at + 9] = u8::from(matches!(claim.value, ClaimValue::Many(_)));
            for value in claim.value.as_slice() {
                Self::write_value(
                    data,
                    fields_at,
                    values_at,
                    &mut value_index,
                    &mut blob_index,
                    field,
                    2,
                    None,
                    value.as_bytes(),
                );
            }
        }
        debug_assert_eq!(value_index, count);
        debug_assert_eq!(blob_index, total);

        for (entry_id, entry) in layout.layout().entries().iter().enumerate() {
            if entry
                .members
                .iter()
                .any(|member| data[fields_at + member.field.index() * FIELD_SIZE + 8] == 0)
            {
                continue;
            }
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            layout.generation().hash(&mut hasher);
            entry_id.hash(&mut hasher);
            for member in entry.members.iter() {
                Self::hash_field(
                    data,
                    fields_at,
                    values_at,
                    member.field.index(),
                    &mut hasher,
                );
            }
            set_entry_present(data, entry_id);
            write_u64(
                data,
                entry_hashes_at(entry_count) + entry_id * 8,
                hasher.finish(),
            );
        }
        Ok(Self {
            bytes: Some(Arc::from(scratch.as_slice())),
        })
    }

    fn write_value(
        bytes: &mut [u8],
        fields_at: usize,
        values_at: usize,
        value_index: &mut usize,
        blob_index: &mut usize,
        field: usize,
        kind: u8,
        original: Option<&str>,
        value: &[u8],
    ) {
        let field_at = fields_at + field * FIELD_SIZE;
        let previous = read_u32(bytes, field_at + 12);
        let at = values_at + *value_index * VALUE_SIZE;
        if read_u32(bytes, field_at + 4) == 0 {
            write_u32(bytes, field_at, *value_index);
        } else {
            write_u32(
                bytes,
                values_at + previous as usize * VALUE_SIZE + 16,
                *value_index,
            );
        }
        write_u32(
            bytes,
            field_at + 4,
            read_u32(bytes, field_at + 4) as usize + 1,
        );
        write_u32(bytes, field_at + 12, *value_index);
        bytes[field_at + 8] = 1;
        write_u32(bytes, at, *blob_index);
        write_u32(bytes, at + 4, value.len());
        bytes[*blob_index..*blob_index + value.len()].copy_from_slice(value);
        *blob_index += value.len();
        if let Some(name) = original {
            write_u32(bytes, at + 8, *blob_index);
            write_u32(bytes, at + 12, name.len());
            bytes[*blob_index..*blob_index + name.len()].copy_from_slice(name.as_bytes());
            *blob_index += name.len();
        }
        write_u32(bytes, at + 16, NONE as usize);
        bytes[at + 20] = kind;
        *value_index += 1;
    }

    fn hash_field(
        bytes: &[u8],
        fields_at: usize,
        values_at: usize,
        field: usize,
        hasher: &mut impl Hasher,
    ) {
        let at = fields_at + field * FIELD_SIZE;
        bytes[at + 9].hash(hasher);
        let count = read_u32(bytes, at + 4);
        count.hash(hasher);
        for value in Self::field_values(bytes, fields_at, values_at, field) {
            value.0.hash(hasher);
            value.1.len().hash(hasher);
            hasher.write(value.1);
        }
    }

    fn field_values<'a>(
        bytes: &'a [u8],
        fields_at: usize,
        values_at: usize,
        field: usize,
    ) -> impl Iterator<Item = (u8, &'a [u8])> + 'a {
        let at = fields_at + field * FIELD_SIZE;
        let mut next = (read_u32(bytes, at + 4) > 0).then(|| read_u32(bytes, at));
        let mut remaining = read_u32(bytes, at + 4);
        std::iter::from_fn(move || {
            let index = next?;
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            let at = values_at + index as usize * VALUE_SIZE;
            let start = read_u32(bytes, at) as usize;
            let len = read_u32(bytes, at + 4) as usize;
            let link = read_u32(bytes, at + 16);
            next = (link != NONE).then_some(link);
            Some((bytes[at + 20], &bytes[start..start + len]))
        })
    }

    /// Count the allocations retained per request for the packed domain.
    #[must_use]
    pub fn storage_strong_count(&self) -> usize {
        self.bytes.as_ref().map_or(0, Arc::strong_count)
    }

    /// Returns the cached atomic-presence hash for a compatible bound entry.
    pub fn bound_entry_hash(
        &self,
        binding: &otel_arrow_dfe_engine::context_declaration::BoundContextEntry,
    ) -> Result<Option<u64>, Error> {
        let Some(bytes) = self.bytes.as_deref() else {
            return Ok(None);
        };
        if read_u64(bytes, 0) != binding.compiled_layout().generation() {
            return Err(invalid("incompatible packed context layout generation"));
        }
        let entry = binding.binding().presence.index();
        Ok(entry_present(bytes, entry).then(|| {
            read_u64(
                bytes,
                entry_hashes_at(binding.layout().entries().len()) + entry * 8,
            )
        }))
    }

    /// Borrows one complete bound entry without allocating or hashing.
    pub fn bound_entry<'a>(
        &'a self,
        binding: &'a otel_arrow_dfe_engine::context_declaration::BoundContextEntry,
    ) -> Result<Option<PackedEntryKey<'a>>, Error> {
        let Some(bytes) = self.bytes.as_deref() else {
            return Ok(None);
        };
        if read_u64(bytes, 0) != binding.compiled_layout().generation() {
            return Err(invalid("incompatible packed context layout generation"));
        }
        let entry = binding.binding().presence.index();
        Ok(entry_present(bytes, entry).then_some(PackedEntryKey { binding, bytes }))
    }
}

/// Borrowed precomputed entry key used by the batch lookup fast path.
pub struct PackedEntryKey<'a> {
    binding: &'a otel_arrow_dfe_engine::context_declaration::BoundContextEntry,
    bytes: &'a [u8],
}

impl PackedEntryKey<'_> {
    fn entry(&self) -> usize {
        self.binding.binding().presence.index()
    }

    fn key_bytes_equal(
        context: &[u8],
        fields_at: usize,
        values_at: usize,
        fields: &[otel_arrow_dfe_config::context_layout::ContextFieldId],
        mut expected: &[u8],
    ) -> bool {
        for field in fields.iter().map(|field| field.index()) {
            let field_at = fields_at + field * FIELD_SIZE;
            let Some((&many, rest)) = expected.split_first() else {
                return false;
            };
            if many != context[field_at + 9] {
                return false;
            }
            let Some((count, rest)) = rest.split_first_chunk::<4>() else {
                return false;
            };
            if u32::from_le_bytes(*count) != read_u32(context, field_at + 4) {
                return false;
            }
            expected = rest;
            for (kind, value) in PackedContext::field_values(context, fields_at, values_at, field) {
                let Some((&expected_kind, rest)) = expected.split_first() else {
                    return false;
                };
                if expected_kind != kind {
                    return false;
                }
                let Some((len, rest)) = rest.split_first_chunk::<4>() else {
                    return false;
                };
                let len = u32::from_le_bytes(*len) as usize;
                let Some((expected_value, rest)) = rest.split_at_checked(len) else {
                    return false;
                };
                if expected_value != value {
                    return false;
                }
                expected = rest;
            }
        }
        expected.is_empty()
    }

    fn owned_key(
        generation: u64,
        entry: usize,
        hash: u64,
        context: &[u8],
        fields_at: usize,
        values_at: usize,
        fields: &[otel_arrow_dfe_config::context_layout::ContextFieldId],
    ) -> OwnedContextKey {
        let mut bytes = Vec::new();
        for field in fields.iter().map(|field| field.index()) {
            let at = fields_at + field * FIELD_SIZE;
            bytes.push(context[at + 9]);
            bytes.extend_from_slice(&read_u32(context, at + 4).to_le_bytes());
            for (kind, value) in PackedContext::field_values(context, fields_at, values_at, field) {
                bytes.push(kind);
                bytes.extend_from_slice(
                    &u32::try_from(value.len())
                        .expect("packed value length is bounded")
                        .to_le_bytes(),
                );
                bytes.extend_from_slice(value);
            }
        }
        OwnedContextKey {
            generation,
            entry,
            hash,
            bytes: bytes.into_boxed_slice(),
        }
    }

    fn fields_at(&self) -> usize {
        fields_at(self.binding.layout().entries().len())
    }

    fn values_at(&self) -> usize {
        self.fields_at() + self.binding.layout().fields().len() * FIELD_SIZE
    }

    /// Returns the hash computed when the request context was packed.
    #[must_use]
    pub fn hash(&self) -> u64 {
        read_u64(
            self.bytes,
            entry_hashes_at(self.binding.layout().entries().len()) + self.entry() * 8,
        )
    }

    /// Compares the full typed value tuple after a hash-table candidate match.
    #[must_use]
    pub fn eq_owned(&self, key: &OwnedContextKey) -> bool {
        if self.binding.compiled_layout().generation() != key.generation
            || self.entry() != key.entry
        {
            return false;
        }
        Self::key_bytes_equal(
            self.bytes,
            self.fields_at(),
            self.values_at(),
            &self.binding.binding().fields,
            &key.bytes,
        )
    }

    /// Compares against a compact serialized collision-checking tuple.
    #[must_use]
    pub fn eq_bytes(&self, expected: &[u8]) -> bool {
        Self::key_bytes_equal(
            self.bytes,
            self.fields_at(),
            self.values_at(),
            &self.binding.binding().fields,
            expected,
        )
    }

    /// Allocates the stored hash and collision-checking tuple for a new partition.
    #[must_use]
    pub fn to_parts(&self) -> (u64, Box<[u8]>) {
        let key = self.to_owned();
        (key.hash, key.bytes)
    }

    /// Allocates the compact collision-checking key for a new partition.
    #[must_use]
    pub fn to_owned(&self) -> OwnedContextKey {
        Self::owned_key(
            self.binding.compiled_layout().generation(),
            self.entry(),
            self.hash(),
            self.bytes,
            self.fields_at(),
            self.values_at(),
            &self.binding.binding().fields,
        )
    }
}

/// A compiled whole-entry hash and full-equality projection.
#[derive(Debug, Clone)]
pub struct ContextKeyBinding {
    layout: Arc<CompiledLayout>,
    entry: usize,
    fields: Box<[otel_arrow_dfe_config::context_layout::ContextFieldId]>,
}

impl ContextKeyBinding {
    /// Bind one complete entry before processing messages.
    pub fn bind(layout: Arc<CompiledLayout>, name: &ContextEntryName) -> Result<Self, Error> {
        let binding = layout.layout().bind(&name.clone().into())?;
        Ok(Self {
            layout,
            entry: binding.presence.index(),
            fields: binding.fields,
        })
    }

    /// Project an atomic entry; absent or incomplete entries return `None`.
    pub fn project<'a>(
        &'a self,
        context: &'a PackedContext,
    ) -> Result<Option<ContextKey<'a>>, Error> {
        let Some(bytes) = context.bytes.as_deref() else {
            return Ok(None);
        };
        if read_u64(bytes, 0) != self.layout.generation() {
            return Err(invalid("incompatible packed context layout generation"));
        }
        Ok(entry_present(bytes, self.entry).then_some(ContextKey {
            binding: self,
            bytes,
        }))
    }
}

/// Borrowed key with no allocation on successful lookup.
pub struct ContextKey<'a> {
    binding: &'a ContextKeyBinding,
    bytes: &'a [u8],
}

impl ContextKey<'_> {
    /// Cached per-entry hash, computed only once when packing.
    #[must_use]
    pub fn hash(&self) -> u64 {
        read_u64(
            self.bytes,
            entry_hashes_at(self.binding.layout.layout().entries().len()) + self.binding.entry * 8,
        )
    }

    fn fields_at(&self) -> usize {
        fields_at(self.binding.layout.layout().entries().len())
    }

    fn values_at(&self) -> usize {
        self.fields_at() + self.binding.layout.layout().fields().len() * FIELD_SIZE
    }

    /// Full collision comparison without allocating a temporary key.
    #[must_use]
    pub fn eq_owned(&self, key: &OwnedContextKey) -> bool {
        if self.binding.layout.generation() != key.generation || self.binding.entry != key.entry {
            return false;
        }
        let mut cursor = key.bytes.as_ref();
        for field in self.binding.fields.iter().map(|field| field.index()) {
            let field_at = self.fields_at() + field * FIELD_SIZE;
            let Some((&many, rest)) = cursor.split_first() else {
                return false;
            };
            if many != self.bytes[field_at + 9] {
                return false;
            }
            let Some((count, rest)) = rest.split_first_chunk::<4>() else {
                return false;
            };
            if u32::from_le_bytes(*count) != read_u32(self.bytes, field_at + 4) {
                return false;
            }
            cursor = rest;
            for (kind, value) in
                PackedContext::field_values(self.bytes, self.fields_at(), self.values_at(), field)
            {
                let Some((&expected_kind, rest)) = cursor.split_first() else {
                    return false;
                };
                if expected_kind != kind {
                    return false;
                }
                let Some((len, rest)) = rest.split_first_chunk::<4>() else {
                    return false;
                };
                let len = u32::from_le_bytes(*len) as usize;
                let Some((expected, rest)) = rest.split_at_checked(len) else {
                    return false;
                };
                if expected != value {
                    return false;
                }
                cursor = rest;
            }
        }
        cursor.is_empty()
    }

    /// Allocates a compact key for insertion into an owned-key table.
    #[must_use]
    pub fn to_owned(&self) -> OwnedContextKey {
        let mut size = 0usize;
        for field in self.binding.fields.iter().map(|field| field.index()) {
            size = size.checked_add(5).expect("packed key size must fit");
            for (_, value) in
                PackedContext::field_values(self.bytes, self.fields_at(), self.values_at(), field)
            {
                size = size
                    .checked_add(5)
                    .and_then(|size| size.checked_add(value.len()))
                    .expect("packed key size must fit");
            }
        }
        let mut bytes = Vec::with_capacity(size);
        for field in self.binding.fields.iter().map(|field| field.index()) {
            let at = self.fields_at() + field * FIELD_SIZE;
            bytes.push(self.bytes[at + 9]);
            bytes.extend_from_slice(&read_u32(self.bytes, at + 4).to_le_bytes());
            for (kind, value) in
                PackedContext::field_values(self.bytes, self.fields_at(), self.values_at(), field)
            {
                bytes.push(kind);
                bytes.extend_from_slice(
                    &u32::try_from(value.len())
                        .expect("packed value length is bounded")
                        .to_le_bytes(),
                );
                bytes.extend_from_slice(value);
            }
        }
        debug_assert_eq!(bytes.len(), size);
        OwnedContextKey {
            generation: self.binding.layout.generation(),
            entry: self.binding.entry,
            hash: self.hash(),
            bytes: bytes.into_boxed_slice(),
        }
    }
}

/// Compact projection key that never retains unrelated request context.
#[derive(Clone, Debug)]
pub struct OwnedContextKey {
    generation: u64,
    entry: usize,
    hash: u64,
    bytes: Box<[u8]>,
}

impl OwnedContextKey {
    /// Precomputed hash, to be paired with full equality on a map hit.
    #[must_use]
    pub fn hash(&self) -> u64 {
        self.hash
    }
}

impl PartialEq for OwnedContextKey {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation
            && self.entry == other.entry
            && self.bytes == other.bytes
    }
}

impl Eq for OwnedContextKey {}

impl Hash for OwnedContextKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::context_layout::ContextPrimitive;
    use otel_arrow_dfe_config::context_policy::{
        ContextEntryDeclaration, ContextEntryDefinition, ContextEntryPart, ContextScope,
    };
    use std::mem::size_of;

    fn name(value: &str) -> ContextEntryName {
        value.try_into().expect("test name")
    }

    fn layout() -> Arc<CompiledLayout> {
        CompiledLayout::compile(
            vec![
                ContextPrimitive {
                    source: ContextSource::AuthorizedIdentity,
                    name: name("customer_id"),
                },
                ContextPrimitive {
                    source: ContextSource::TransportHeader,
                    name: name("workspace_id"),
                },
            ],
            &[ContextEntryDeclaration {
                scope: ContextScope::Engine,
                name: name("product_user"),
                definition: ContextEntryDefinition(vec![
                    ContextEntryPart::AuthorizedIdentity {
                        name: name("customer_id").into(),
                        store_as: None,
                    },
                    ContextEntryPart::TransportHeader {
                        name: name("workspace_id").into(),
                        store_as: None,
                    },
                ]),
            }],
        )
        .expect("valid layout")
    }

    fn claim<'a>(value: &'a ClaimValue) -> CapturedClaim<'a> {
        CapturedClaim {
            name: "customer_id",
            value,
        }
    }

    fn header<'a>(value: &'a [u8], kind: ValueKind) -> CapturedHeader<'a> {
        CapturedHeader {
            name: "workspace_id",
            original_name: None,
            kind,
            value,
        }
    }

    /// Scenario: a composite has one verified claim and one transport member.
    /// Guarantees: each missing member makes the whole key absent.
    #[test]
    fn composite_presence_requires_both_sources() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let value = ClaimValue::One("customer-a".into());
        assert!(
            binding
                .project(&PackedContext::pack(&layout, &[], &[claim(&value)]).expect("packed"))
                .expect("compatible")
                .is_none()
        );
        assert!(
            binding
                .project(
                    &PackedContext::pack(&layout, &[header(b"workspace-a", ValueKind::Text)], &[])
                        .expect("packed")
                )
                .expect("compatible")
                .is_none()
        );
    }

    /// Scenario: a request has neither captured headers nor verified claims.
    /// Guarantees: absent context is allocation-free, stays within two pointer
    /// widths, and yields no projected key.
    #[test]
    fn empty_context_does_not_allocate() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let packed = PackedContext::pack(&layout, &[], &[]).expect("empty context");
        assert_eq!(size_of::<PackedContext>(), 2 * size_of::<usize>());
        assert_eq!(packed.storage_strong_count(), 0);
        assert!(
            binding
                .project(&packed)
                .expect("empty projection")
                .is_none()
        );
    }

    /// Scenario: two contexts have the same mixed-source composite.
    /// Guarantees: packed hash, full equality, and cloning preserve its identity.
    #[test]
    fn mixed_source_keys_share_cached_hash_and_compare_fully() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let value = ClaimValue::One("customer-a".into());
        let packed = PackedContext::pack(
            &layout,
            &[header(b"workspace-a", ValueKind::Text)],
            &[claim(&value)],
        )
        .expect("packed");
        let cloned = packed.clone();
        assert_eq!(packed.storage_strong_count(), 2);
        let key = binding
            .project(&packed)
            .expect("compatible")
            .expect("present");
        let owned = key.to_owned();
        let same = binding
            .project(&cloned)
            .expect("compatible")
            .expect("present");
        assert_eq!(same.hash(), owned.hash());
        assert!(same.eq_owned(&owned));
        let other = PackedContext::pack(
            &layout,
            &[header(b"workspace-b", ValueKind::Text)],
            &[claim(&value)],
        )
        .expect("packed");
        assert!(
            !binding
                .project(&other)
                .expect("compatible")
                .expect("present")
                .eq_owned(&owned)
        );
    }

    /// Scenario: one verified claim has zero values but was captured as a multi-value claim.
    /// Guarantees: present-but-empty is distinct from an absent source.
    #[test]
    fn empty_multi_claim_is_present() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let value = ClaimValue::Many(vec![]);
        let packed = PackedContext::pack(
            &layout,
            &[header(b"workspace-a", ValueKind::Text)],
            &[claim(&value)],
        )
        .expect("packed");
        assert!(binding.project(&packed).expect("compatible").is_some());
    }

    /// Scenario: two physical sources store values under the same configured name.
    /// Guarantees: typed fields remain distinct when a composite aliases its members.
    #[test]
    fn same_name_across_source_domains_remains_distinct() {
        let field = name("tenant");
        let layout = CompiledLayout::compile(
            vec![
                ContextPrimitive {
                    source: ContextSource::AuthorizedIdentity,
                    name: field.clone(),
                },
                ContextPrimitive {
                    source: ContextSource::TransportHeader,
                    name: field.clone(),
                },
            ],
            &[ContextEntryDeclaration {
                scope: ContextScope::Engine,
                name: name("identity"),
                definition: ContextEntryDefinition(vec![
                    ContextEntryPart::AuthorizedIdentity {
                        name: field.clone().into(),
                        store_as: Some(name("verified")),
                    },
                    ContextEntryPart::TransportHeader {
                        name: field.into(),
                        store_as: Some(name("untrusted")),
                    },
                ]),
            }],
        )
        .expect("typed layout");
        let binding = ContextKeyBinding::bind(Arc::clone(&layout), &name("identity"))
            .expect("composite entry");
        let claim = ClaimValue::One("trusted".into());
        let packed = PackedContext::pack(
            &layout,
            &[CapturedHeader {
                name: "tenant",
                original_name: None,
                kind: ValueKind::Text,
                value: b"untrusted",
            }],
            &[CapturedClaim {
                name: "tenant",
                value: &claim,
            }],
        )
        .expect("packed");
        let key = binding
            .project(&packed)
            .expect("compatible")
            .expect("present");
        assert!(key.eq_owned(&key.to_owned()));
    }

    /// Scenario: a claim switches from one value to a one-element many claim.
    /// Guarantees: cardinality remains part of the composite identity.
    #[test]
    fn claim_cardinality_affects_key() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let one = ClaimValue::One("customer".into());
        let many = ClaimValue::Many(vec!["customer".into()]);
        let header = [header(b"workspace", ValueKind::Text)];
        let first = PackedContext::pack(&layout, &header, &[claim(&one)]).expect("packed");
        let second = PackedContext::pack(&layout, &header, &[claim(&many)]).expect("packed");
        let key = binding
            .project(&first)
            .expect("compatible")
            .expect("present")
            .to_owned();
        let other = binding
            .project(&second)
            .expect("compatible")
            .expect("present");
        assert!(!other.eq_owned(&key));
    }

    /// Scenario: a header has text and binary occurrences separated by another field.
    /// Guarantees: all matching occurrences retain source order and kind in the key.
    #[test]
    fn repeated_transport_values_preserve_order_and_kind() {
        let layout = layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let value = ClaimValue::One("customer-a".into());
        let first = PackedContext::pack(
            &layout,
            &[
                header(b"first", ValueKind::Text),
                header(b"second", ValueKind::Binary),
            ],
            &[claim(&value)],
        )
        .expect("packed");
        let reversed = PackedContext::pack(
            &layout,
            &[
                header(b"second", ValueKind::Binary),
                header(b"first", ValueKind::Text),
            ],
            &[claim(&value)],
        )
        .expect("packed");
        let key = binding
            .project(&first)
            .expect("compatible")
            .expect("present")
            .to_owned();
        assert!(
            !binding
                .project(&reversed)
                .expect("compatible")
                .expect("present")
                .eq_owned(&key)
        );
    }

    /// Scenario: a context from a previous pipeline generation is projected.
    /// Guarantees: an incompatible layout is an error, not a missing key.
    #[test]
    fn generation_mismatch_is_not_absence() {
        let old = layout();
        let value = ClaimValue::One("customer-a".into());
        let packed = PackedContext::pack(
            &old,
            &[header(b"workspace-a", ValueKind::Text)],
            &[claim(&value)],
        )
        .expect("packed");
        let new = CompiledLayout::compile(
            old.layout().fields().to_vec(),
            &[ContextEntryDeclaration {
                scope: ContextScope::Engine,
                name: name("product_user"),
                definition: ContextEntryDefinition(vec![
                    ContextEntryPart::AuthorizedIdentity {
                        name: name("customer_id").into(),
                        store_as: None,
                    },
                    ContextEntryPart::TransportHeader {
                        name: name("workspace_id").into(),
                        store_as: None,
                    },
                ]),
            }],
        )
        .expect("valid new layout");
        assert!(
            ContextKeyBinding::bind(Arc::clone(&new), &name("product_user"))
                .expect("entry")
                .project(&packed)
                .is_err()
        );
    }
}
