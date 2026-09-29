// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compiled mixed-source request context projection.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use otel_arrow_dfe_config::ContextEntryName;
use otel_arrow_dfe_config::context_layout::ContextSource;
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::transport_headers::{
    ContextEntryNameRef, TransportHeader, TransportHeaderRef, TransportHeaderSource,
    TransportHeaderValueRef, TransportHeaders, ValueKind,
};
use otel_arrow_dfe_engine::capability::auth::ClaimValue;
pub use otel_arrow_dfe_engine::context_declaration::CompiledContextLayout as CompiledLayout;

use crate::pdata::AuthorizedIdentityEntries;

const HEADER_SIZE: usize = 16;
const FIELD_SIZE: usize = 16;
const VALUE_SIZE: usize = 28;
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
    bytes: Option<Box<[u8]>>,
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
        Self::pack_impl(layout, headers, claims, true)
    }

    pub(crate) fn pack_materialized(
        layout: &CompiledLayout,
        headers: &TransportHeaders,
        claims: &AuthorizedIdentityEntries,
    ) -> Result<Self, Error> {
        Self::pack_materialized_impl(layout, headers, claims)
    }

    /// Packs source values without evaluating or hashing compiled entries.
    #[cfg(feature = "bench")]
    pub fn pack_deferred(
        layout: &CompiledLayout,
        headers: &[CapturedHeader<'_>],
        claims: &[CapturedClaim<'_>],
    ) -> Result<Self, Error> {
        Self::pack_impl(layout, headers, claims, false)
    }

    fn pack_impl(
        layout: &CompiledLayout,
        headers: &[CapturedHeader<'_>],
        claims: &[CapturedClaim<'_>],
        cache_entries: bool,
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
        Self::pack_sized(
            layout,
            headers.len(),
            claims.len(),
            count,
            blob_len,
            cache_entries,
            |data, fields_at, values_at, value_index, blob_index| {
                for header in headers {
                    let field = field(layout, ContextSource::TransportHeader, header.name)?;
                    Self::write_value(
                        data,
                        fields_at,
                        values_at,
                        value_index,
                        blob_index,
                        field,
                        header.kind as u8,
                        header.original_name,
                        header.value,
                    );
                }
                for (claim_index, claim) in claims.iter().enumerate() {
                    let field = field(layout, ContextSource::AuthorizedIdentity, claim.name)?;
                    let field_at = fields_at + field * FIELD_SIZE;
                    if data[field_at + 8] != 0 {
                        return Err(invalid(format!(
                            "duplicate authorized identity field `{}`",
                            claim.name
                        )));
                    }
                    data[field_at + 8] = 1;
                    data[field_at + 9] = u8::from(matches!(claim.value, ClaimValue::Many(_)));
                    for value in claim.value.as_slice() {
                        Self::write_value(
                            data,
                            fields_at,
                            values_at,
                            value_index,
                            blob_index,
                            field,
                            2,
                            None,
                            value.as_bytes(),
                        );
                    }
                    // Claims are unique, so the completed value chain no longer needs
                    // its last-value cursor. Reuse it to retain capture-policy order.
                    write_u32(data, field_at + 12, claim_index);
                }
                Ok(())
            },
        )
    }

    fn pack_materialized_impl(
        layout: &CompiledLayout,
        headers: &TransportHeaders,
        claims: &AuthorizedIdentityEntries,
    ) -> Result<Self, Error> {
        if headers.is_empty() && claims.is_empty() {
            return Ok(Self::default());
        }
        let mut count = headers.len();
        let mut blob_len = 0usize;
        for header in headers.iter() {
            _ = field(layout, ContextSource::TransportHeader, header.name.as_str())?;
            blob_len = blob_len
                .checked_add(header.value.bytes.len())
                .and_then(|len| len.checked_add(header.value.original_name.map_or(0, str::len)))
                .ok_or_else(|| invalid("context blob size overflow"))?;
        }
        for claim in claims.iter() {
            _ = field(layout, ContextSource::AuthorizedIdentity, claim.name())?;
            count = count
                .checked_add(claim.value().len())
                .ok_or_else(|| invalid("context value count overflow"))?;
            for value in claim.value().values() {
                blob_len = blob_len
                    .checked_add(value.len())
                    .ok_or_else(|| invalid("context blob size overflow"))?;
            }
        }
        Self::pack_sized(
            layout,
            headers.len(),
            claims.len(),
            count,
            blob_len,
            true,
            |data, fields_at, values_at, value_index, blob_index| {
                for header in headers.iter() {
                    let field =
                        field(layout, ContextSource::TransportHeader, header.name.as_str())?;
                    Self::write_value(
                        data,
                        fields_at,
                        values_at,
                        value_index,
                        blob_index,
                        field,
                        header.value.value_kind as u8,
                        header.value.original_name,
                        header.value.bytes,
                    );
                }
                for (claim_index, claim) in claims.iter().enumerate() {
                    let value = claim.value();
                    let field = field(layout, ContextSource::AuthorizedIdentity, claim.name())?;
                    Self::write_claim(
                        data,
                        fields_at,
                        values_at,
                        value_index,
                        blob_index,
                        field,
                        claim.name(),
                        value.is_many(),
                        claim_index,
                        value.values(),
                    )?;
                }
                Ok(())
            },
        )
    }

    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    fn pack_sized(
        layout: &CompiledLayout,
        header_count: usize,
        claim_count: usize,
        count: usize,
        blob_len: usize,
        cache_entries: bool,
        write: impl FnOnce(&mut [u8], usize, usize, &mut usize, &mut usize) -> Result<(), Error>,
    ) -> Result<Self, Error> {
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

        let mut bytes = vec![0; total].into_boxed_slice();
        let data = bytes.as_mut();
        write_u64(data, 0, layout.generation());
        write_u32(data, 8, header_count);
        write_u32(data, 12, claim_count);

        let mut value_index = 0usize;
        let mut blob_index = blob_at;
        write(
            data,
            fields_at,
            values_at,
            &mut value_index,
            &mut blob_index,
        )?;
        debug_assert_eq!(value_index, count);
        debug_assert_eq!(blob_index, total);

        if cache_entries {
            for entry_id in layout.cached_entries().iter().map(|entry| entry.index()) {
                let entry = &layout.layout().entries()[entry_id];
                if entry
                    .members
                    .iter()
                    .any(|member| data[fields_at + member.field.index() * FIELD_SIZE + 8] == 0)
                    || entry.conditions.iter().any(|condition| {
                        !Self::field_values(data, fields_at, values_at, condition.field.index())
                            .any(|(_, value)| value == condition.value.as_ref())
                    })
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
        }
        Ok(Self { bytes: Some(bytes) })
    }

    #[allow(clippy::too_many_arguments)]
    fn write_claim<'a>(
        bytes: &mut [u8],
        fields_at: usize,
        values_at: usize,
        value_index: &mut usize,
        blob_index: &mut usize,
        field: usize,
        name: &str,
        many: bool,
        claim_index: usize,
        values: impl Iterator<Item = &'a str>,
    ) -> Result<(), Error> {
        let field_at = fields_at + field * FIELD_SIZE;
        if bytes[field_at + 8] != 0 {
            return Err(invalid(format!(
                "duplicate authorized identity field `{name}`"
            )));
        }
        bytes[field_at + 8] = 1;
        bytes[field_at + 9] = u8::from(many);
        for value in values {
            Self::write_value(
                bytes,
                fields_at,
                values_at,
                value_index,
                blob_index,
                field,
                2,
                None,
                value.as_bytes(),
            );
        }
        // Claims are unique, so the completed value chain no longer needs
        // its last-value cursor. Reuse it to retain capture-policy order.
        write_u32(bytes, field_at + 12, claim_index);
        Ok(())
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
        write_u32(bytes, at + 24, field);
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

    /// Returns whether this context retains a packed byte allocation.
    #[must_use]
    pub fn has_storage(&self) -> bool {
        self.bytes.is_some()
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

    /// Borrows all captured transport headers directly from packed storage.
    #[must_use]
    pub(crate) fn transport_headers<'a>(
        &'a self,
        layout: &'a CompiledLayout,
    ) -> PackedTransportHeaders<'a> {
        if let Some(bytes) = self.bytes.as_deref() {
            assert_eq!(
                read_u64(bytes, 0),
                layout.generation(),
                "packed context layout generation mismatch"
            );
        }
        PackedTransportHeaders {
            layout,
            bytes: self.bytes.as_deref(),
        }
    }

    /// Borrows all verified identity entries directly from packed storage.
    #[must_use]
    pub(crate) fn authorized_identity<'a>(
        &'a self,
        layout: &'a CompiledLayout,
    ) -> PackedAuthorizedIdentityEntries<'a> {
        if let Some(bytes) = self.bytes.as_deref() {
            assert_eq!(
                read_u64(bytes, 0),
                layout.generation(),
                "packed context layout generation mismatch"
            );
        }
        PackedAuthorizedIdentityEntries {
            layout,
            bytes: self.bytes.as_deref(),
        }
    }

    /// Returns the number of captured transport headers without decoding.
    #[must_use]
    pub fn transport_header_count(&self) -> usize {
        self.bytes
            .as_deref()
            .map_or(0, |bytes| read_u32(bytes, 8) as usize)
    }

    /// Formats authorized identity metadata without exposing values or allocating views.
    pub(crate) fn authorized_identity_debug<'a>(
        &'a self,
        layout: &'a CompiledLayout,
    ) -> impl std::fmt::Debug + 'a {
        struct DebugIdentity<'a> {
            packed: &'a PackedContext,
            layout: &'a CompiledLayout,
        }
        impl std::fmt::Debug for DebugIdentity<'_> {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let Some(bytes) = self.packed.bytes.as_deref() else {
                    return formatter.debug_list().finish();
                };
                let fields_at = fields_at(self.layout.layout().entries().len());
                let mut list = formatter.debug_list();
                for (field, primitive) in self.layout.layout().fields().iter().enumerate() {
                    if primitive.source != ContextSource::AuthorizedIdentity {
                        continue;
                    }
                    let at = fields_at + field * FIELD_SIZE;
                    if bytes[at + 8] != 0 {
                        _ = list.entry(&format_args!(
                            "{{ name: {:?}, value_count: {} }}",
                            primitive.name.as_str(),
                            read_u32(bytes, at + 4)
                        ));
                    }
                }
                list.finish()
            }
        }
        DebugIdentity {
            packed: self,
            layout,
        }
    }
}

/// Borrowed read-only view of transport headers in canonical packed storage.
#[derive(Clone, Copy)]
pub struct PackedTransportHeaders<'a> {
    layout: &'a CompiledLayout,
    bytes: Option<&'a [u8]>,
}

impl std::fmt::Debug for PackedTransportHeaders<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq<TransportHeaders> for PackedTransportHeaders<'_> {
    fn eq(&self, other: &TransportHeaders) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl PartialEq<&TransportHeaders> for PackedTransportHeaders<'_> {
    fn eq(&self, other: &&TransportHeaders) -> bool {
        self == *other
    }
}

impl<'a> PackedTransportHeaders<'a> {
    /// Returns the number of captured headers.
    #[must_use]
    pub fn len(&self) -> usize {
        TransportHeaderSource::len(self)
    }

    /// Returns whether there are no captured headers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        TransportHeaderSource::is_empty(self)
    }

    /// Returns one captured header by capture-order index.
    #[must_use]
    pub fn get(self, index: usize) -> Option<TransportHeaderRef<'a>> {
        let bytes = self.bytes?;
        if index >= self.len() {
            return None;
        }
        let fields_at = fields_at(self.layout.layout().entries().len());
        let values_at = fields_at + self.layout.layout().fields().len() * FIELD_SIZE;
        let at = values_at + index * VALUE_SIZE;
        let field = read_u32(bytes, at + 24) as usize;
        let primitive = self.layout.layout().fields().get(field)?;
        let value_at = read_u32(bytes, at) as usize;
        let value_len = read_u32(bytes, at + 4) as usize;
        let original_at = read_u32(bytes, at + 8) as usize;
        let original_len = read_u32(bytes, at + 12) as usize;
        let value_kind = match bytes[at + 20] {
            0 => ValueKind::Text,
            1 => ValueKind::Binary,
            value => panic!("invalid packed transport value kind {value}"),
        };
        let original_name = (original_len != 0).then(|| {
            std::str::from_utf8(&bytes[original_at..original_at + original_len])
                .expect("packed original header name remains UTF-8")
        });
        Some(TransportHeaderRef {
            name: ContextEntryNameRef::new(primitive.name.as_str()),
            value: TransportHeaderValueRef {
                original_name,
                value_kind,
                bytes: &bytes[value_at..value_at + value_len],
            },
        })
    }

    /// Iterates over captured headers in capture order.
    #[must_use]
    pub fn iter(self) -> PackedTransportHeadersIter<'a> {
        PackedTransportHeadersIter {
            headers: self,
            index: 0,
        }
    }

    /// Iterates over headers with an exact stored-name match.
    pub fn find_by_name<'b>(
        self,
        name: &'b str,
    ) -> impl Iterator<Item = TransportHeaderRef<'b>> + 'b
    where
        'a: 'b,
    {
        self.iter()
            .filter(move |header| header.name.as_str() == name)
    }

    /// Materializes an owned collection for mutation or ownership transfer.
    #[must_use]
    pub fn to_owned(self) -> TransportHeaders {
        let mut headers = TransportHeaders::with_capacity(self.len());
        for header in self.iter() {
            headers.push(TransportHeader::from(header));
        }
        headers
    }
}

/// Iterator over canonical packed transport headers.
pub struct PackedTransportHeadersIter<'a> {
    headers: PackedTransportHeaders<'a>,
    index: usize,
}

impl<'a> Iterator for PackedTransportHeadersIter<'a> {
    type Item = TransportHeaderRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let header = self.headers.get(self.index)?;
        self.index += 1;
        Some(header)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.headers.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PackedTransportHeadersIter<'_> {}

impl TransportHeaderSource for PackedTransportHeaders<'_> {
    fn len(&self) -> usize {
        self.bytes.map_or(0, |bytes| read_u32(bytes, 8) as usize)
    }

    fn get(&self, index: usize) -> Option<TransportHeaderRef<'_>> {
        (*self).get(index)
    }
}

/// Borrowed read-only view of verified identity in canonical packed storage.
#[derive(Clone, Copy)]
pub struct PackedAuthorizedIdentityEntries<'a> {
    layout: &'a CompiledLayout,
    bytes: Option<&'a [u8]>,
}

impl<'a> PackedAuthorizedIdentityEntries<'a> {
    /// Returns the number of captured identity entries.
    #[must_use]
    pub fn len(self) -> usize {
        self.bytes.map_or(0, |bytes| read_u32(bytes, 12) as usize)
    }

    /// Returns whether no identity entries were captured.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Iterates over identity entries in capture-policy order.
    #[must_use]
    pub fn iter(self) -> PackedAuthorizedIdentityIter<'a> {
        PackedAuthorizedIdentityIter {
            entries: self,
            ordinal: 0,
        }
    }

    /// Finds an identity entry by exact configured name.
    #[must_use]
    pub fn get(self, name: &str) -> Option<PackedAuthorizedIdentityEntry<'a>> {
        let field = self.layout.layout().fields().iter().position(|field| {
            field.source == ContextSource::AuthorizedIdentity && field.name.as_str() == name
        })?;
        self.decode_field(field)
    }

    /// Materializes all entries for mutation or ownership transfer.
    #[must_use]
    pub fn to_owned(self) -> AuthorizedIdentityEntries {
        AuthorizedIdentityEntries::from_decoded(self.iter().map(DecodedClaim::from).collect())
    }

    /// Materializes only entries selected by exact configured name.
    #[must_use]
    pub fn select(self, names: &[&str]) -> AuthorizedIdentityEntries {
        AuthorizedIdentityEntries::from_decoded(
            self.iter()
                .filter(|entry| names.contains(&entry.name()))
                .map(DecodedClaim::from)
                .collect(),
        )
    }

    fn decode_ordinal(self, ordinal: usize) -> Option<PackedAuthorizedIdentityEntry<'a>> {
        let bytes = self.bytes?;
        let fields_at = fields_at(self.layout.layout().entries().len());
        self.layout
            .layout()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.source == ContextSource::AuthorizedIdentity)
            .find_map(|(field, _)| {
                let at = fields_at + field * FIELD_SIZE;
                (bytes[at + 8] != 0 && read_u32(bytes, at + 12) as usize == ordinal)
                    .then(|| self.decode_field(field))
                    .flatten()
            })
    }

    fn decode_field(self, field: usize) -> Option<PackedAuthorizedIdentityEntry<'a>> {
        let bytes = self.bytes?;
        let fields_at = fields_at(self.layout.layout().entries().len());
        let values_at = fields_at + self.layout.layout().fields().len() * FIELD_SIZE;
        let at = fields_at + field * FIELD_SIZE;
        if bytes[at + 8] == 0 {
            return None;
        }
        Some(PackedAuthorizedIdentityEntry {
            name: self.layout.layout().fields()[field].name.as_str(),
            value: PackedAuthorizedClaimValue {
                bytes,
                values_at,
                first_value: read_u32(bytes, at) as usize,
                value_count: read_u32(bytes, at + 4) as usize,
                many: bytes[at + 9] != 0,
            },
        })
    }
}

/// Iterator over verified identity entries in capture-policy order.
pub struct PackedAuthorizedIdentityIter<'a> {
    entries: PackedAuthorizedIdentityEntries<'a>,
    ordinal: usize,
}

impl<'a> Iterator for PackedAuthorizedIdentityIter<'a> {
    type Item = PackedAuthorizedIdentityEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ordinal >= self.entries.len() {
            return None;
        }
        let entry = self.entries.decode_ordinal(self.ordinal);
        self.ordinal += 1;
        entry
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.entries.len().saturating_sub(self.ordinal);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for PackedAuthorizedIdentityIter<'_> {}

/// One verified identity entry borrowed from canonical packed storage.
#[derive(Clone, Copy)]
pub struct PackedAuthorizedIdentityEntry<'a> {
    name: &'a str,
    value: PackedAuthorizedClaimValue<'a>,
}

impl<'a> PackedAuthorizedIdentityEntry<'a> {
    /// Returns the configured context entry name.
    #[must_use]
    pub const fn name(self) -> &'a str {
        self.name
    }

    /// Returns the verified claim without flattening its cardinality.
    #[must_use]
    pub const fn value(self) -> PackedAuthorizedClaimValue<'a> {
        self.value
    }
}

/// Borrowed single- or multi-valued verified claim in canonical packed storage.
#[derive(Clone, Copy)]
pub struct PackedAuthorizedClaimValue<'a> {
    bytes: &'a [u8],
    values_at: usize,
    first_value: usize,
    value_count: usize,
    many: bool,
}

impl<'a> PackedAuthorizedClaimValue<'a> {
    /// Returns the single value, or `None` for a multi-valued claim.
    #[must_use]
    pub fn as_str(self) -> Option<&'a str> {
        (!self.many && self.value_count == 1).then(|| self.decode_value(self.first_value))
    }

    /// Iterates values in source order.
    pub fn values(self) -> impl Iterator<Item = &'a str> {
        (0..self.value_count).map(move |offset| self.decode_value(self.first_value + offset))
    }

    /// Returns the number of values.
    #[must_use]
    pub const fn len(self) -> usize {
        self.value_count
    }

    /// Returns whether the claim contains no values.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.value_count == 0
    }

    /// Returns whether the source claim used multi-valued cardinality.
    #[must_use]
    pub const fn is_many(self) -> bool {
        self.many
    }

    fn decode_value(self, index: usize) -> &'a str {
        let at = self.values_at + index * VALUE_SIZE;
        let start = read_u32(self.bytes, at) as usize;
        let len = read_u32(self.bytes, at + 4) as usize;
        std::str::from_utf8(&self.bytes[start..start + len])
            .expect("packed authorized identity remains UTF-8")
    }
}

impl From<PackedAuthorizedIdentityEntry<'_>> for DecodedClaim {
    fn from(entry: PackedAuthorizedIdentityEntry<'_>) -> Self {
        Self {
            name: ContextEntryName::try_from(entry.name())
                .expect("packed authorized identity name remains valid"),
            many: entry.value().is_many(),
            values: entry.value().values().map(str::to_owned).collect(),
        }
    }
}

/// One owned authorized-identity field used at materialization boundaries.
pub struct DecodedClaim {
    /// Stored context entry name.
    pub name: ContextEntryName,
    /// Whether the source claim was multi-valued.
    pub many: bool,
    /// Values in source order.
    pub values: Vec<String>,
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
        let length = fields
            .iter()
            .map(|field| field.index())
            .map(|field| {
                5 + PackedContext::field_values(context, fields_at, values_at, field)
                    .map(|(_, value)| 5 + value.len())
                    .sum::<usize>()
            })
            .sum();
        let mut bytes = Vec::with_capacity(length);
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
        debug_assert_eq!(bytes.len(), length);
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

    /// Evaluates and hashes one bound entry from packed source fields.
    #[cfg(feature = "bench")]
    pub fn project_recomputed<'a>(
        &'a self,
        context: &'a PackedContext,
    ) -> Result<Option<RecomputedContextKey<'a>>, Error> {
        let Some(bytes) = context.bytes.as_deref() else {
            return Ok(None);
        };
        if read_u64(bytes, 0) != self.layout.generation() {
            return Err(invalid("incompatible packed context layout generation"));
        }
        let fields_at = fields_at(self.layout.layout().entries().len());
        let values_at = fields_at + self.layout.layout().fields().len() * FIELD_SIZE;
        let entry = &self.layout.layout().entries()[self.entry];
        if entry
            .members
            .iter()
            .any(|member| bytes[fields_at + member.field.index() * FIELD_SIZE + 8] == 0)
            || entry.conditions.iter().any(|condition| {
                !PackedContext::field_values(bytes, fields_at, values_at, condition.field.index())
                    .any(|(_, value)| value == condition.value.as_ref())
            })
        {
            return Ok(None);
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.layout.generation().hash(&mut hasher);
        self.entry.hash(&mut hasher);
        for field in &self.fields {
            PackedContext::hash_field(bytes, fields_at, values_at, field.index(), &mut hasher);
        }
        Ok(Some(RecomputedContextKey {
            binding: self,
            bytes,
            hash: hasher.finish(),
        }))
    }
}

/// One bound entry evaluated and hashed on demand.
#[cfg(feature = "bench")]
pub struct RecomputedContextKey<'a> {
    binding: &'a ContextKeyBinding,
    bytes: &'a [u8],
    hash: u64,
}

#[cfg(feature = "bench")]
impl RecomputedContextKey<'_> {
    /// Returns the hash computed during this projection.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.hash
    }

    /// Performs the same full collision comparison as a cached projection.
    #[must_use]
    pub fn eq_owned(&self, key: &OwnedContextKey) -> bool {
        ContextKey {
            binding: self.binding,
            bytes: self.bytes,
        }
        .eq_owned(key)
    }

    /// Materializes an owned key for benchmark table setup.
    #[must_use]
    pub fn to_owned(&self) -> OwnedContextKey {
        PackedEntryKey::owned_key(
            self.binding.layout.generation(),
            self.binding.entry,
            self.hash,
            self.bytes,
            fields_at(self.binding.layout.layout().entries().len()),
            fields_at(self.binding.layout.layout().entries().len())
                + self.binding.layout.layout().fields().len() * FIELD_SIZE,
            &self.binding.fields,
        )
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

    fn conditional_layout() -> Arc<CompiledLayout> {
        CompiledLayout::compile(
            vec![
                ContextPrimitive {
                    source: ContextSource::AuthorizedIdentity,
                    name: name("customer_id"),
                },
                ContextPrimitive {
                    source: ContextSource::TransportHeader,
                    name: name("environment"),
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
                    ContextEntryPart::TransportHeaderMatch {
                        name: name("environment").into(),
                        value: "production".to_owned(),
                    },
                ]),
            }],
        )
        .expect("valid conditional layout")
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

    fn environment(value: &[u8]) -> CapturedHeader<'_> {
        CapturedHeader {
            name: "environment",
            original_name: None,
            kind: ValueKind::Text,
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

    /// Scenario: a mixed-source composite also requires an exact transport-header value.
    /// Guarantees: its precomputed presence bit is set only when all members and the condition match.
    #[test]
    fn conditional_composite_presence_is_precomputed() {
        let layout = conditional_layout();
        let binding =
            ContextKeyBinding::bind(Arc::clone(&layout), &name("product_user")).expect("entry");
        let customer = ClaimValue::One("customer-a".into());
        let workspace = header(b"workspace-a", ValueKind::Text);

        let matching = PackedContext::pack(
            &layout,
            &[
                workspace,
                environment(b"development"),
                environment(b"production"),
            ],
            &[claim(&customer)],
        )
        .expect("packed");
        assert!(binding.project(&matching).expect("compatible").is_some());

        for headers in [
            vec![workspace],
            vec![workspace, environment(b"development")],
        ] {
            let packed =
                PackedContext::pack(&layout, &headers, &[claim(&customer)]).expect("packed");
            assert!(binding.project(&packed).expect("compatible").is_none());
        }
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
        assert!(!packed.has_storage());
        assert!(
            binding
                .project(&packed)
                .expect("empty projection")
                .is_none()
        );
    }

    /// Scenario: two contexts have the same mixed-source composite.
    /// Guarantees: packed hash, full equality, and value cloning preserve its identity.
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
        assert!(packed.has_storage());
        assert_eq!(packed, cloned);
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

    /// Scenario: two physical sources use the same configured stored name.
    /// Guarantees: layout compilation rejects the ambiguous primitive before packing.
    #[test]
    fn same_name_across_source_domains_is_rejected() {
        let field = name("tenant");
        assert!(
            CompiledLayout::compile(
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
            .is_err()
        );
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

    /// Scenario: policy capture order differs from the layout's sorted field order.
    /// Guarantees: the packed view preserves policy order, including empty many claims.
    #[test]
    fn authorized_identity_view_preserves_capture_order() {
        let layout = CompiledLayout::compile(
            vec![
                ContextPrimitive {
                    source: ContextSource::AuthorizedIdentity,
                    name: name("alpha"),
                },
                ContextPrimitive {
                    source: ContextSource::AuthorizedIdentity,
                    name: name("zeta"),
                },
            ],
            &[],
        )
        .expect("valid layout");
        let zeta = ClaimValue::Many(vec![]);
        let alpha = ClaimValue::One("value".into());
        let packed = PackedContext::pack(
            &layout,
            &[],
            &[
                CapturedClaim {
                    name: "zeta",
                    value: &zeta,
                },
                CapturedClaim {
                    name: "alpha",
                    value: &alpha,
                },
            ],
        )
        .expect("packed");
        let entries = packed
            .authorized_identity(&layout)
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(
            entries.iter().map(|entry| entry.name()).collect::<Vec<_>>(),
            ["zeta", "alpha"]
        );
        assert!(entries[0].value().is_many());
        assert!(entries[0].value().is_empty());
    }

    /// Scenario: two verified claims target the same packed identity field.
    /// Guarantees: packing rejects ambiguity instead of corrupting its value chain cursor.
    #[test]
    fn duplicate_authorized_claims_are_rejected() {
        let layout = layout();
        let first = ClaimValue::One("first".into());
        let second = ClaimValue::One("second".into());
        let error = PackedContext::pack(&layout, &[], &[claim(&first), claim(&second)])
            .expect_err("duplicate claims must fail");
        assert!(
            error
                .to_string()
                .contains("duplicate authorized identity field")
        );
    }

    /// Scenario: a packed header view is requested with another pipeline generation.
    /// Guarantees: incompatible descriptor offsets fail loudly instead of being interpreted.
    #[test]
    #[should_panic(expected = "packed context layout generation mismatch")]
    fn transport_header_view_rejects_generation_mismatch() {
        let old = layout();
        let packed = PackedContext::pack(&old, &[header(b"workspace-a", ValueKind::Text)], &[])
            .expect("packed");
        let new =
            CompiledLayout::compile(old.layout().fields().to_vec(), &[]).expect("valid new layout");
        _ = packed.transport_headers(&new);
    }

    /// Scenario: callers inspect a captured header through the request-context view.
    /// Guarantees: names and values are borrowed from canonical storage without materialization.
    #[test]
    fn transport_header_view_borrows_packed_storage() {
        let layout = layout();
        let packed = PackedContext::pack(
            &layout,
            &[CapturedHeader {
                name: "workspace_id",
                original_name: Some("X-Workspace"),
                kind: ValueKind::Binary,
                value: b"workspace-a",
            }],
            &[],
        )
        .expect("packed");
        let bytes = packed.bytes.as_deref().expect("packed storage");
        let header = packed
            .transport_headers(&layout)
            .iter()
            .next()
            .expect("captured header");

        assert_eq!(header.name.as_str(), "workspace_id");
        assert_eq!(header.wire_name(), "X-Workspace");
        assert_eq!(header.value.value_kind, ValueKind::Binary);
        assert_eq!(header.value.bytes, b"workspace-a");
        let storage = bytes.as_ptr_range();
        assert!(storage.contains(&header.value.bytes.as_ptr()));
    }

    /// Scenario: callers inspect a verified claim through the request-context view.
    /// Guarantees: claim values are borrowed from canonical storage without materialization.
    #[test]
    fn authorized_identity_view_borrows_packed_storage() {
        let layout = layout();
        let value = ClaimValue::One("customer-a".into());
        let packed = PackedContext::pack(&layout, &[], &[claim(&value)]).expect("packed");
        let bytes = packed.bytes.as_deref().expect("packed storage");
        let value = packed
            .authorized_identity(&layout)
            .get("customer_id")
            .expect("captured identity")
            .value()
            .as_str()
            .expect("single identity value");

        assert_eq!(value, "customer-a");
        let storage = bytes.as_ptr_range();
        assert!(storage.contains(&value.as_ptr()));
    }
}
