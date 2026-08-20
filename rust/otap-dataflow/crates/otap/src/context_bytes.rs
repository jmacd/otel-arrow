// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One-allocation pdata context prototype.
//!
//! The trailing bytes are one OTLP `Span` whose attributes form an ordered,
//! duplicate-preserving header bag. The fixed prefix holds context-entry
//! presence bits, entry ranges, typed identities, and attribute descriptors.
//! Nothing here is wired into [`crate::pdata::Context`] yet.

use bytes::Bytes;
use otap_df_pdata::otlp::common::Dropped;
use otap_df_pdata::otlp::{BoundedBuf, ProtoBuffer};

const MAGIC: u32 = 0x4354_5831; // CTX1
const VERSION: u16 = 1;
const FIXED_PREFIX_LEN: usize = 16;
const ENTRY_LEN: usize = 16;
const ATTRIBUTE_LEN: usize = 16;

/// Header value kind preserved independently of the OTLP bytes encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HeaderValueKind {
    /// A transport text value, which may contain arbitrary bytes.
    Text = 0,
    /// A transport binary value.
    Binary = 1,
}

/// One borrowed header supplied by a receiver.
#[derive(Clone, Copy, Debug)]
pub struct HeaderInput<'a> {
    /// Original wire name, retained in the OTLP attribute key.
    pub wire_name: &'a str,
    /// Raw header bytes.
    pub value: &'a [u8],
    /// Text or binary transport semantics.
    pub kind: HeaderValueKind,
    /// Compiled capture-rule identifier.
    pub rule_id: u16,
    /// Optional compiled `store_as` entry slot.
    pub entry: Option<u16>,
}

/// Failure while constructing a bounded protobuf envelope.
#[derive(Debug, thiserror::Error)]
pub enum ContextBytesError {
    /// A context would exceed its representable fixed-prefix bounds.
    #[error("context envelope has too many {what}")]
    TooMany {
        /// Bounded item category.
        what: &'static str,
    },
    /// The OTLP encoder rejected a buffer write.
    #[error("context envelope exceeded its protobuf buffer")]
    Dropped,
}

impl From<Dropped> for ContextBytesError {
    fn from(_: Dropped) -> Self {
        Self::Dropped
    }
}

/// Immutable encoded pdata context.
#[derive(Clone, Debug)]
pub struct PdataContextBytes {
    bytes: Bytes,
    span_offset: usize,
}

/// Borrowed view of one compiled context entry.
pub struct ContextEntry<'a> {
    context: &'a PdataContextBytes,
    first_member: usize,
    member_count: usize,
    hash: u64,
}

impl ContextEntry<'_> {
    /// Returns the precomputed typed hash. Callers must compare values on a
    /// hash hit because transport metadata is attacker-influenced.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.hash
    }

    /// Iterates the entry's typed values in arrival order.
    pub fn values(&self) -> impl Iterator<Item = (HeaderValueKind, &[u8])> {
        (0..self.member_count).filter_map(move |index| {
            let member = self.context.member(self.first_member + index)?;
            self.context.value(member)
        })
    }
}

impl PdataContextBytes {
    /// Builds a context envelope and OTLP span bag in one `ProtoBuffer`.
    pub fn build<'a>(
        entry_count: usize,
        headers: impl IntoIterator<Item = HeaderInput<'a>>,
    ) -> Result<Self, ContextBytesError> {
        let headers: smallvec::SmallVec<[HeaderInput<'_>; 32]> = headers.into_iter().collect();
        if headers.len() > u16::MAX as usize {
            return Err(ContextBytesError::TooMany { what: "headers" });
        }
        if entry_count > u16::MAX as usize {
            return Err(ContextBytesError::TooMany { what: "entries" });
        }

        let presence_words = entry_count.div_ceil(64);
        let prefix_len = FIXED_PREFIX_LEN
            + presence_words * 8
            + entry_count * ENTRY_LEN
            + headers.len() * ATTRIBUTE_LEN
            + headers.len() * 2;
        let capacity = prefix_len
            + headers
                .iter()
                .map(|header| header.wire_name.len() + header.value.len() + 16)
                .sum::<usize>();
        let mut buffer = ProtoBuffer::with_capacity(capacity);
        buffer.try_extend(&vec![0; prefix_len])?;
        let span_offset = prefix_len;

        let mut presence = vec![0u64; presence_words];
        let mut members: Vec<smallvec::SmallVec<[u16; 4]>> = (0..entry_count)
            .map(|_| smallvec::SmallVec::new())
            .collect();
        let mut values = smallvec::SmallVec::<[(usize, usize); 32]>::new();

        for (attribute, header) in headers.iter().enumerate() {
            let value_start = encode_attribute(&mut buffer, header)?;
            values.push((value_start - span_offset, header.value.len()));
            if let Some(entry) = header.entry {
                let entry = entry as usize;
                if entry >= entry_count {
                    return Err(ContextBytesError::TooMany {
                        what: "entry references",
                    });
                }
                presence[entry / 64] |= 1u64 << (entry % 64);
                members[entry].push(attribute as u16);
            }
        }

        patch_prefix(
            buffer.as_mut(),
            span_offset,
            &presence,
            &members,
            &headers,
            &values,
        );
        Ok(Self {
            bytes: buffer.into_bytes(),
            span_offset,
        })
    }

    /// Returns the single reference-counted allocation.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns the serialized OTLP `Span` backing the header bag.
    #[must_use]
    pub fn span_bytes(&self) -> &[u8] {
        &self.bytes[self.span_offset..]
    }

    /// Returns a typed value range for one captured attribute.
    #[must_use]
    pub fn value(&self, attribute: usize) -> Option<(HeaderValueKind, &[u8])> {
        let count = u16::from_le_bytes(self.bytes[8..10].try_into().ok()?) as usize;
        if attribute >= count {
            return None;
        }
        let words = u16::from_le_bytes(self.bytes[10..12].try_into().ok()?) as usize;
        let offset = FIXED_PREFIX_LEN
            + words * 8
            + self.entry_count()? * ENTRY_LEN
            + attribute * ATTRIBUTE_LEN;
        let kind = match self.bytes.get(offset + 2)? {
            0 => HeaderValueKind::Text,
            1 => HeaderValueKind::Binary,
            _ => return None,
        };
        let start =
            u32::from_le_bytes(self.bytes[offset + 4..offset + 8].try_into().ok()?) as usize;
        let len = u32::from_le_bytes(self.bytes[offset + 8..offset + 12].try_into().ok()?) as usize;
        Some((kind, self.span_bytes().get(start..start + len)?))
    }

    /// Returns a present entry through its schema-local slot.
    #[must_use]
    pub fn entry(&self, slot: usize) -> Option<ContextEntry<'_>> {
        let words = u16::from_le_bytes(self.bytes[10..12].try_into().ok()?) as usize;
        let entry_count = self.entry_count()?;
        if slot >= entry_count {
            return None;
        }
        let word = u64::from_le_bytes(
            self.bytes[FIXED_PREFIX_LEN + (slot / 64) * 8..FIXED_PREFIX_LEN + (slot / 64 + 1) * 8]
                .try_into()
                .ok()?,
        );
        if word & (1u64 << (slot % 64)) == 0 {
            return None;
        }
        let at = FIXED_PREFIX_LEN + words * 8 + slot * ENTRY_LEN;
        Some(ContextEntry {
            context: self,
            first_member: u32::from_le_bytes(self.bytes[at..at + 4].try_into().ok()?) as usize,
            member_count: u32::from_le_bytes(self.bytes[at + 4..at + 8].try_into().ok()?) as usize,
            hash: u64::from_le_bytes(self.bytes[at + 8..at + 16].try_into().ok()?),
        })
    }

    fn member(&self, index: usize) -> Option<usize> {
        let count = u16::from_le_bytes(self.bytes[8..10].try_into().ok()?) as usize;
        let words = u16::from_le_bytes(self.bytes[10..12].try_into().ok()?) as usize;
        let at = FIXED_PREFIX_LEN
            + words * 8
            + self.entry_count()? * ENTRY_LEN
            + count * ATTRIBUTE_LEN
            + index * 2;
        Some(u16::from_le_bytes(self.bytes.get(at..at + 2)?.try_into().ok()?) as usize)
    }

    fn entry_count(&self) -> Option<usize> {
        (u32::from_le_bytes(self.bytes[0..4].try_into().ok()?) == MAGIC).then_some(())?;
        Some(u16::from_le_bytes(self.bytes[6..8].try_into().ok()?) as usize)
    }
}

fn encode_attribute(
    buffer: &mut ProtoBuffer,
    header: &HeaderInput<'_>,
) -> Result<usize, ContextBytesError> {
    // Span.attributes = 9; KeyValue.key = 1; KeyValue.value = 2;
    // AnyValue.bytes_value = 7. The prefix retains Text/Binary semantics.
    buffer.encode_len_delimited(9, |key_value| {
        key_value.encode_string(1, header.wire_name)?;
        key_value.encode_len_delimited(2, |any_value| {
            any_value.encode_bytes(7, header.value)?;
            Ok::<(), Dropped>(())
        })?;
        Ok::<(), Dropped>(())
    })?;
    Ok(buffer.len() - header.value.len())
}

fn patch_prefix(
    bytes: &mut [u8],
    span_offset: usize,
    presence: &[u64],
    members: &[smallvec::SmallVec<[u16; 4]>],
    headers: &[HeaderInput<'_>],
    values: &[(usize, usize)],
) {
    bytes[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
    bytes[6..8].copy_from_slice(&(members.len() as u16).to_le_bytes());
    bytes[8..10].copy_from_slice(&(headers.len() as u16).to_le_bytes());
    bytes[10..12].copy_from_slice(&(presence.len() as u16).to_le_bytes());
    bytes[12..16].copy_from_slice(&(span_offset as u32).to_le_bytes());
    let mut at = FIXED_PREFIX_LEN;
    for word in presence {
        bytes[at..at + 8].copy_from_slice(&word.to_le_bytes());
        at += 8;
    }
    let mut member_base = 0u32;
    for (slot, members) in members.iter().enumerate() {
        bytes[at..at + 4].copy_from_slice(&member_base.to_le_bytes());
        bytes[at + 4..at + 8].copy_from_slice(&(members.len() as u32).to_le_bytes());
        bytes[at + 8..at + 16].copy_from_slice(&entry_hash(slot, members, headers).to_le_bytes());
        member_base += members.len() as u32;
        at += ENTRY_LEN;
    }
    for (header, (offset, len)) in headers.iter().zip(values) {
        bytes[at..at + 2].copy_from_slice(&header.rule_id.to_le_bytes());
        bytes[at + 2] = header.kind as u8;
        bytes[at + 4..at + 8].copy_from_slice(&(*offset as u32).to_le_bytes());
        bytes[at + 8..at + 12].copy_from_slice(&(*len as u32).to_le_bytes());
        at += ATTRIBUTE_LEN;
    }
    for members in members {
        for member in members {
            bytes[at..at + 2].copy_from_slice(&member.to_le_bytes());
            at += 2;
        }
    }
}

fn entry_hash(slot: usize, members: &[u16], headers: &[HeaderInput<'_>]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in (slot as u64).to_le_bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    for member in members {
        let header = &headers[*member as usize];
        hash = (hash ^ u64::from(header.kind as u8)).wrapping_mul(0x0000_0100_0000_01b3);
        for byte in header.value {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: an entry has duplicate header values interleaved with a
    /// bag-only header.
    /// Guarantees: the entry range preserves its values' arrival order while
    /// the encoded carrier remains one reference-counted `Bytes` value.
    #[test]
    fn entry_range_indexes_typed_header_values() {
        let context = PdataContextBytes::build(
            1,
            [
                HeaderInput {
                    wire_name: "x-tenant",
                    value: b"acme",
                    kind: HeaderValueKind::Text,
                    rule_id: 0,
                    entry: Some(0),
                },
                HeaderInput {
                    wire_name: "x-request-id",
                    value: b"request-1",
                    kind: HeaderValueKind::Text,
                    rule_id: 1,
                    entry: None,
                },
                HeaderInput {
                    wire_name: "x-tenant",
                    value: &[0x01, 0x02],
                    kind: HeaderValueKind::Binary,
                    rule_id: 0,
                    entry: Some(0),
                },
            ],
        )
        .expect("context encodes");

        let entry = context.entry(0).expect("entry is present");
        assert_ne!(entry.hash(), 0);
        let values: Vec<_> = entry.values().collect();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], (HeaderValueKind::Text, b"acme".as_slice()));
        assert_eq!(values[1], (HeaderValueKind::Binary, &[0x01u8, 0x02][..]));
        assert_eq!(
            context.value(1),
            Some((HeaderValueKind::Text, b"request-1".as_slice()))
        );
        assert!(!context.bytes().is_empty());
    }
}
