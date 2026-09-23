// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Experimental single-allocation context projection for retained-work lookup.
//!
//! This is deliberately separate from the active request context. It measures
//! a compiled, allocation-free lookup before replacing the public capture APIs.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use otel_arrow_dfe_config::ContextEntryName;
use otel_arrow_dfe_config::context_layout::{ContextLayout, ContextPrimitive, ContextSource};
use otel_arrow_dfe_config::context_policy::ContextEntryDeclaration;
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::transport_headers::ValueKind;
use otel_arrow_dfe_engine::capability::auth::ClaimValue;
use smallvec::SmallVec;

const HEADER_SIZE: usize = 16;
const FIELD_SIZE: usize = 16;
const ENTRY_SIZE: usize = 16;
const VALUE_SIZE: usize = 24;
const NONE: u32 = u32::MAX;

// Assigned only while compiling a layout, never on the message-processing path.
// Monotonic identities prevent an in-flight context from being decoded using
// another pipeline's layout, even if the prior layout has been dropped.
static NEXT_LAYOUT_GENERATION: AtomicU64 = AtomicU64::new(1);

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidUserConfig {
        error: message.into(),
    }
}

/// One pipeline generation's resolved source and grouping declarations.
#[derive(Debug)]
pub struct CompiledLayout {
    generation: u64,
    layout: ContextLayout,
}

impl CompiledLayout {
    /// Compile flat source references into ordered, atomic grouping entries.
    pub fn compile(
        fields: Vec<ContextPrimitive>,
        declarations: &[ContextEntryDeclaration],
    ) -> Result<Arc<Self>, Error> {
        let layout = ContextLayout::compile(fields, declarations)?;
        let generation = NEXT_LAYOUT_GENERATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| invalid("context layout generation overflow"))?;
        Ok(Arc::new(Self { generation, layout }))
    }

    /// Bind one complete entry before processing messages.
    pub fn bind(self: &Arc<Self>, name: &ContextEntryName) -> Result<ContextKeyBinding, Error> {
        let binding = self.layout.bind(&name.clone().into())?;
        Ok(ContextKeyBinding {
            layout: Arc::clone(self),
            entry: binding.presence.index(),
            fields: binding.fields,
        })
    }

    fn field(&self, source: ContextSource, name: &str) -> Result<usize, Error> {
        self.layout
            .fields()
            .iter()
            .position(|field| field.source == source && field.name.as_str() == name)
            .ok_or_else(|| invalid(format!("uncompiled {source:?} field `{name}`")))
    }
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
#[derive(Clone, Debug, Default)]
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
            _ = layout.field(ContextSource::TransportHeader, header.name)?;
            blob_len = blob_len
                .checked_add(header.value.len())
                .and_then(|len| len.checked_add(header.original_name.map_or(0, str::len)))
                .ok_or_else(|| invalid("context blob size overflow"))?;
        }
        for claim in claims {
            _ = layout.field(ContextSource::AuthorizedIdentity, claim.name)?;
            count = count
                .checked_add(claim.value.as_slice().len())
                .ok_or_else(|| invalid("context value count overflow"))?;
            for value in claim.value.as_slice() {
                blob_len = blob_len
                    .checked_add(value.len())
                    .ok_or_else(|| invalid("context blob size overflow"))?;
            }
        }
        let fields_at = layout
            .layout
            .entries()
            .len()
            .checked_mul(ENTRY_SIZE)
            .and_then(|len| HEADER_SIZE.checked_add(len))
            .ok_or_else(|| invalid("context entries exceed packed size"))?;
        let values_at = layout
            .layout
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
        write_u64(data, 0, layout.generation);
        write_u32(data, 8, headers.len());
        write_u32(data, 12, claims.len());

        let mut value_index = 0usize;
        let mut blob_index = blob_at;
        for header in headers {
            let field = layout.field(ContextSource::TransportHeader, header.name)?;
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
            let field = layout.field(ContextSource::AuthorizedIdentity, claim.name)?;
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

        for (entry_id, entry) in layout.layout.entries().iter().enumerate() {
            if entry
                .members
                .iter()
                .any(|member| data[fields_at + member.field.index() * FIELD_SIZE + 8] == 0)
            {
                continue;
            }
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            layout.generation.hash(&mut hasher);
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
            let at = HEADER_SIZE + entry_id * ENTRY_SIZE;
            data[at] = 1;
            write_u64(data, at + 8, hasher.finish());
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
}

/// A compiled whole-entry hash and full-equality projection.
#[derive(Debug, Clone)]
pub struct ContextKeyBinding {
    layout: Arc<CompiledLayout>,
    entry: usize,
    fields: Box<[otel_arrow_dfe_config::context_layout::ContextFieldId]>,
}

impl ContextKeyBinding {
    /// Project an atomic entry; absent or incomplete entries return `None`.
    pub fn project<'a>(
        &'a self,
        context: &'a PackedContext,
    ) -> Result<Option<ContextKey<'a>>, Error> {
        let Some(bytes) = context.bytes.as_deref() else {
            return Ok(None);
        };
        if read_u64(bytes, 0) != self.layout.generation {
            return Err(invalid("incompatible packed context layout generation"));
        }
        let at = HEADER_SIZE + self.entry * ENTRY_SIZE;
        Ok((bytes[at] != 0).then_some(ContextKey {
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
            HEADER_SIZE + self.binding.entry * ENTRY_SIZE + 8,
        )
    }

    fn fields_at(&self) -> usize {
        HEADER_SIZE + self.binding.layout.layout.entries().len() * ENTRY_SIZE
    }

    fn values_at(&self) -> usize {
        self.fields_at() + self.binding.layout.layout.fields().len() * FIELD_SIZE
    }

    /// Full collision comparison without allocating a temporary key.
    #[must_use]
    pub fn eq_owned(&self, key: &OwnedContextKey) -> bool {
        if self.binding.layout.generation != key.generation || self.binding.entry != key.entry {
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

    /// Allocate a compact key only when a new accounting bucket is inserted.
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
            generation: self.binding.layout.generation,
            entry: self.binding.entry,
            hash: self.hash(),
            bytes: bytes.into_boxed_slice(),
        }
    }
}

/// Compact accounting-bucket key; never retains unrelated request context.
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
    use otel_arrow_dfe_config::context_policy::{
        ContextEntryDefinition, ContextEntryPart, ContextScope,
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
        let binding = layout.bind(&name("identity")).expect("composite entry");
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
        let binding = layout.bind(&name("product_user")).expect("entry");
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
            old.layout.fields().to_vec(),
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
            new.bind(&name("product_user"))
                .expect("entry")
                .project(&packed)
                .is_err()
        );
    }
}
