// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Building a context, once per request.
//!
//! A producer offers what the request carried, then finishes. Offering is a
//! probe and a copy into scratch, so a receiver walks its inbound headers once
//! and hands each one over without deciding anything and without allocating. An
//! offer for something no downstream consumer observes is dropped on the spot,
//! which is the second level of reachability pruning doing its work.
//!
//! Finishing runs the same sequence every time:
//!
//! 1. resolve tokens, all-or-nothing;
//! 2. dictionary-encode value-matched extractors into the packed symbol field;
//! 3. measure every region, then write the whole context into one allocation.
//!
//! PairSlot words and branch-table reads belong to the consumer and do not
//! happen here. This is the same separation as Envoy building descriptors in a
//! filter and looking them up at the limiter that is actually reached.
//!
//! Measuring before writing is what keeps it to one allocation: every length
//! follows from what was staged, so the buffer is sized exactly and filled once.
//! A request that resolved no token writes nothing at all and yields an empty
//! context, which allocates nothing.
//!
//! The trust boundary is in this API rather than in a convention. A header can
//! only reach an extractor through [`ContextEncoder::offer_transport_header`],
//! and a claim only through [`ContextEncoder::offer_authorized_claim`], which an
//! authorization extension owns. Naming a header after a claim cannot forge one.

use crate::bag::{ByteCursor, member_len, write_member};
use crate::compiled::CompiledMetadata;
use crate::context::MetadataContext;
use crate::error::EncodeError;
use crate::ids::{BagId, ExtractorId, ProducerId, ValueSlotId};
use crate::layout::{CONTEXT_ID_BYTES, EPOCH_BYTES, LAYOUT_FINGERPRINT_BYTES, write_bits};
use crate::plan::ExtractionPlan;
use crate::scratch::{MetadataScratch, RegionPlan};
use crate::source::{PeerAddressPart, Repetition};
use bytes::BytesMut;
use std::net::SocketAddr;

/// Bytes of length prefix in front of each value of a repeated slot.
pub(crate) const REPEATED_VALUE_PREFIX: usize = size_of::<u16>();

/// Builds one context for one request.
#[derive(Debug)]
pub struct ContextEncoder<'a> {
    compiled: &'a CompiledMetadata,
    plan: &'a ExtractionPlan,
    scratch: &'a mut MetadataScratch,
    /// The first reason this request cannot be encoded. Offers after it are
    /// ignored, so a caller may finish offering and handle the failure once.
    rejected: Option<EncodeError>,
}

impl<'a> ContextEncoder<'a> {
    pub(crate) fn new(
        compiled: &'a CompiledMetadata,
        producer: ProducerId,
        scratch: &'a mut MetadataScratch,
    ) -> Self {
        scratch.begin(compiled);
        Self {
            compiled,
            plan: compiled.plan(producer),
            scratch,
            rejected: None,
        }
    }

    /// Offers a header the sender supplied.
    ///
    /// The value is untrusted. It can only fill a key that some extractor bound
    /// to this name, and it can never fill an authorized claim.
    pub fn offer_transport_header(&mut self, wire_name: &str, value: &[u8]) {
        let compiled = self.compiled;
        for target in compiled.header_targets(wire_name) {
            self.stage(target.extractor, target.ordinal, value);
        }
    }

    /// Offers a claim an authorization extension proved.
    pub fn offer_authorized_claim(&mut self, claim: &str, value: &[u8]) {
        let compiled = self.compiled;
        for target in compiled.claim_targets(claim) {
            self.stage(target.extractor, target.ordinal, value);
        }
    }

    /// Offers a value the producing node computed, such as a partition value or
    /// an idempotency key.
    pub fn offer_derived_value(&mut self, name: &str, value: &[u8]) {
        let compiled = self.compiled;
        for target in compiled.derived_targets(name) {
            self.stage(target.extractor, target.ordinal, value);
        }
    }

    /// Offers the peer's network address, as the transport observed it.
    pub fn offer_peer_address(&mut self, peer: SocketAddr) {
        let compiled = self.compiled;
        for entry in &compiled.peer_address_extractors {
            if !self.plan.wants(entry.extractor) {
                continue;
            }
            // The longest textual SocketAddr is an IPv6 address, a port, and
            // brackets. Reserve against that upper bound before formatting so
            // even a deliberately tiny scratch limit cannot grow unchecked.
            const MAX_SOCKET_ADDRESS_BYTES: usize = 64;
            if self.scratch.staged_bytes() + MAX_SOCKET_ADDRESS_BYTES
                > self.compiled.limits.scratch_bytes
            {
                self.reject(EncodeError::ScratchTooLarge {
                    needed: self.scratch.staged_bytes() + MAX_SOCKET_ADDRESS_BYTES,
                    limit: self.compiled.limits.scratch_bytes,
                });
                return;
            }
            let part = entry.part;
            self.scratch
                .stage_formatted(entry.extractor, 0, |out| match part {
                    PeerAddressPart::Address => append_display(out, format_args!("{}", peer.ip())),
                    PeerAddressPart::AddressAndPort => append_display(out, format_args!("{peer}")),
                });
        }
    }

    /// Resolves everything offered and packs it into one allocation.
    pub fn finish(mut self) -> Result<MetadataContext, EncodeError> {
        if let Some(rejected) = self.rejected {
            return Err(rejected);
        }

        // A request that resolved no token has nothing to say, and saying
        // nothing costs no allocation.
        let resolved = self.resolve_tokens();
        if resolved == 0 {
            return Ok(MetadataContext::empty());
        }

        let data_bytes = self.measure_regions(resolved);
        self.write(resolved, data_bytes)
    }

    /// Stages a value, honouring the extractor's repetition rule.
    fn stage(&mut self, extractor: ExtractorId, ordinal: u8, value: &[u8]) {
        if self.rejected.is_some() || !self.plan.wants(extractor) {
            return;
        }

        let compiled = self.compiled.extractor(extractor);
        let key = compiled.key;
        let repetition = compiled.repetition;
        if value.len() > compiled.value_limit {
            self.reject(EncodeError::ValueTooLarge {
                key: self.compiled.key_name(key).to_owned(),
            });
            return;
        }
        if self.scratch.staged_bytes() + value.len() > self.compiled.limits.scratch_bytes {
            self.reject(EncodeError::ScratchTooLarge {
                needed: self.scratch.staged_bytes() + value.len(),
                limit: self.compiled.limits.scratch_bytes,
            });
            return;
        }
        if self.compiled.key_value_kind(key) == crate::source::ValueKind::Text
            && std::str::from_utf8(value).is_err()
        {
            self.reject(EncodeError::InvalidTextValue {
                key: self.compiled.key_name(key).to_owned(),
            });
            return;
        }

        let staged = self.scratch.staged_count(extractor);
        match repetition {
            Repetition::First if staged > 0 => {}
            Repetition::Reject if staged > 0 => self.reject(EncodeError::UnexpectedRepetition {
                key: self.compiled.key_name(key).to_owned(),
            }),
            Repetition::All if staged > 0 => {
                if staged >= self.compiled.limits.values_per_key {
                    self.reject(EncodeError::ValueTooLarge {
                        key: self.compiled.key_name(key).to_owned(),
                    });
                } else {
                    self.scratch.stage_additional(extractor, value);
                }
            }
            _ => self.scratch.stage_first(extractor, ordinal, value),
        }
    }

    fn reject(&mut self, error: EncodeError) {
        if self.rejected.is_none() {
            self.rejected = Some(error);
        }
    }

    /// A token resolves only when every one of its extractors produced a value.
    fn resolve_tokens(&self) -> u64 {
        let mut resolved = 0u64;
        for index in set_bits(self.plan.live_tokens()) {
            let token = &self.compiled.tokens[index];
            let extractors = &self.compiled.token_extractors[token.extractors.as_usize()];
            if extractors
                .iter()
                .all(|&extractor| self.scratch.is_staged(extractor))
            {
                resolved |= 1 << index;
            }
        }
        resolved
    }

    /// Measures every region and records where it lands, returning the total.
    fn measure_regions(&mut self, resolved: u64) -> usize {
        let (compiled, plan) = (self.compiled, self.plan);
        self.scratch.reset_regions(compiled.layout.regions);

        for &slot_id in &plan.value_slots {
            let Some(source) = self.field_source(slot_id, resolved) else {
                continue;
            };
            let values: usize = self
                .scratch
                .staged_values(source)
                .map(<[u8]>::len)
                .sum::<usize>();
            let framing = if compiled.value_slot_at(slot_id).repeated {
                self.scratch.staged_count(source) * REPEATED_VALUE_PREFIX
            } else {
                0
            };
            self.scratch.set_region(
                slot_id.index(),
                RegionPlan {
                    source: Some(source),
                    bytes: values + framing,
                    offset: 0,
                },
            );
        }

        for &bag_id in &plan.bags {
            let bytes = self.measure_bag(bag_id, resolved);
            self.scratch.set_region(
                self.bag_region(bag_id),
                RegionPlan {
                    source: None,
                    bytes,
                    offset: 0,
                },
            );
        }

        self.scratch.place_regions(compiled.layout.regions)
    }

    fn measure_bag(&self, bag_id: BagId, resolved: u64) -> usize {
        let bag = self.compiled.bag(bag_id);
        let tag = &self.compiled.bag_bytes[bag.field_tag.as_usize()];
        let mut bytes = 0;
        for member in &self.compiled.bag_members[bag.members.as_usize()] {
            let Some(source) = self.field_source(member.value_slot, resolved) else {
                continue;
            };
            let key_field = &self.compiled.bag_bytes[member.key_field.as_usize()];
            for value in self.scratch.staged_values(source) {
                bytes += member_len(tag, key_field, value.len());
            }
        }
        bytes
    }

    /// Returns the one extractor this token-qualified slot carries, when its
    /// token resolved. There is deliberately no fallback to another token with
    /// the same key.
    fn field_source(&self, slot_id: ValueSlotId, resolved: u64) -> Option<ExtractorId> {
        let slot = self.compiled.value_slot_at(slot_id);
        is_resolved(resolved, slot.token).then_some(slot.extractor)
    }

    fn bag_region(&self, bag: BagId) -> usize {
        self.compiled
            .bag(bag)
            .region
            .expect("extraction plans contain only live bags")
    }

    fn write(self, resolved: u64, data_bytes: usize) -> Result<MetadataContext, EncodeError> {
        let layout = &self.compiled.layout;
        let total = layout.data_offset + data_bytes;
        if total > self.compiled.limits.context_bytes {
            return Err(EncodeError::ContextTooLarge {
                needed: total,
                limit: self.compiled.limits.context_bytes,
            });
        }

        let mut packed = BytesMut::zeroed(total);
        packed[..EPOCH_BYTES].copy_from_slice(&self.compiled.epoch.value().to_le_bytes());
        packed[EPOCH_BYTES..EPOCH_BYTES + LAYOUT_FINGERPRINT_BYTES]
            .copy_from_slice(&layout.fingerprint.to_le_bytes());
        packed[CONTEXT_ID_BYTES..CONTEXT_ID_BYTES + layout.token_bitmap_bytes]
            .copy_from_slice(&resolved.to_le_bytes()[..layout.token_bitmap_bytes]);

        self.write_symbols(&mut packed);
        self.write_name_ordinals(&mut packed);
        self.write_region_index(&mut packed);
        self.write_data(&mut packed, resolved);

        Ok(MetadataContext::new(packed.freeze()))
    }

    /// Writes every value-matched extractor's dictionary symbol.
    ///
    /// A slot is independent of tokens: the extractor supplied the value, the
    /// encoder encoded it once, and each reached consumer later selects the
    /// symbols its own PairSlots need. A slot left at zero is `ABSENT`.
    fn write_symbols(&self, packed: &mut BytesMut) {
        let layout = &self.compiled.layout;
        let field = &mut packed
            [layout.symbol_field_offset..layout.symbol_field_offset + layout.symbol_field_bytes];
        for &slot_id in &self.plan.symbol_slots {
            let slot = self.compiled.symbol_slot_at(slot_id);
            if !self.scratch.is_staged(slot.extractor) {
                continue;
            }
            write_bits(
                field,
                slot.bit_offset,
                slot.bits,
                self.compiled
                    .dictionary(slot.dictionary)
                    .symbol(self.scratch.staged_single(slot.extractor))
                    .as_word(),
            );
        }
    }

    fn write_name_ordinals(&self, packed: &mut BytesMut) {
        let layout = &self.compiled.layout;
        for &slot_id in &self.plan.value_slots {
            let Some(at) = self.compiled.value_slot_at(slot_id).name_ordinal else {
                continue;
            };
            let Some(source) = self.scratch.region(slot_id.index()).source else {
                continue;
            };
            packed[layout.name_ordinals_offset + at as usize] = self.scratch.staged_ordinal(source);
        }
    }

    fn write_region_index(&self, packed: &mut BytesMut) {
        let layout = &self.compiled.layout;
        if layout.regions == 0 {
            return;
        }
        for index in 0..=layout.regions {
            let offset = if index == layout.regions {
                self.scratch.region(index - 1).offset + self.scratch.region(index - 1).bytes
            } else {
                self.scratch.region(index).offset
            };
            let at = layout.region_index_offset + index * size_of::<u16>();
            packed[at..at + size_of::<u16>()].copy_from_slice(&(offset as u16).to_le_bytes());
        }
    }

    fn write_data(&self, packed: &mut BytesMut, resolved: u64) {
        let layout = &self.compiled.layout;

        for &slot_id in &self.plan.value_slots {
            let region = self.scratch.region(slot_id.index());
            let Some(source) = region.source else {
                continue;
            };
            let start = layout.data_offset + region.offset;
            let mut cursor = ByteCursor::new(&mut packed[start..start + region.bytes]);
            let repeated = self.compiled.value_slot_at(slot_id).repeated;
            for value in self.scratch.staged_values(source) {
                if repeated {
                    cursor.extend(&(value.len() as u16).to_le_bytes());
                }
                cursor.extend(value);
            }
        }

        for &bag_id in &self.plan.bags {
            let region = self.scratch.region(self.bag_region(bag_id));
            let start = layout.data_offset + region.offset;
            let mut cursor = ByteCursor::new(&mut packed[start..start + region.bytes]);
            let bag = self.compiled.bag(bag_id);
            let tag = &self.compiled.bag_bytes[bag.field_tag.as_usize()];
            for member in &self.compiled.bag_members[bag.members.as_usize()] {
                let Some(source) = self.field_source(member.value_slot, resolved) else {
                    continue;
                };
                let key_field = &self.compiled.bag_bytes[member.key_field.as_usize()];
                for value in self.scratch.staged_values(source) {
                    write_member(&mut cursor, tag, key_field, member.value_kind, value);
                }
            }
            debug_assert_eq!(cursor.position(), region.bytes);
        }
    }
}

/// Returns whether a token resolved for this request.
fn is_resolved(resolved: u64, token: crate::ids::TokenId) -> bool {
    resolved & (1 << token.index()) != 0
}

/// Walks the set bits of a bitmap, lowest first.
fn set_bits(mut bitmap: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        if bitmap == 0 {
            return None;
        }
        let index = bitmap.trailing_zeros() as usize;
        bitmap &= bitmap - 1;
        Some(index)
    })
}

/// Appends a formatted value, which cannot fail when the sink is a `Vec`.
fn append_display(out: &mut Vec<u8>, args: std::fmt::Arguments<'_>) {
    use std::io::Write as _;
    let _ = out.write_fmt(args);
}
