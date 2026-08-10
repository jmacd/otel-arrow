// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The packed context, and the view a consumer reads it through.
//!
//! A context is one reference-counted allocation, so passing it along a pipeline
//! or splitting a batch into several outbound requests costs a refcount bump. A
//! context that carries nothing holds no allocation at all.
//!
//! Reading goes through a [`MetadataView`], which is obtained once by pairing a
//! context with the compiled state of its epoch. That pairing is where the epoch
//! is checked, so every read after it is infallible and costs an indexed load. It
//! also states plainly what the alternative would hide: a context outlives the
//! configuration that produced it, and a consumer that has already moved to a
//! newer epoch must fail the request rather than read a slot that has moved.

use crate::compiled::CompiledMetadata;
use crate::condition::ConditionMatch;
use crate::encoder::REPEATED_VALUE_PREFIX;
use crate::ids::{ConditionSetId, ConsumerId, Epoch, TokenId, ValueSlotId};
use crate::layout::{CONTEXT_ID_BYTES, EPOCH_BYTES, read_bits};
use bytes::Bytes;

/// One request's metadata, packed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataContext {
    bytes: Bytes,
}

/// Why bytes could not be read as a metadata context for compiled state.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum ContextViewError {
    /// The context was built by a different compiler epoch.
    #[error(transparent)]
    EpochMismatch(#[from] EpochMismatch),

    /// The context's layout differs even though its epoch number happened to
    /// agree, for example after an operational epoch counter was reused.
    #[error(
        "metadata context layout fingerprint {context:#018x} does not match compiled layout \
         {compiled:#018x}"
    )]
    LayoutMismatch {
        /// Fingerprint stamped on the context.
        context: u64,
        /// Fingerprint the compiled state expects.
        compiled: u64,
    },

    /// The context is truncated or its region index is inconsistent.
    #[error("malformed metadata context: {reason}")]
    Malformed {
        /// A static explanation suitable for Nack telemetry.
        reason: &'static str,
    },
}

impl MetadataContext {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    /// Returns a context that carries nothing, without allocating.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
        }
    }

    /// Returns whether this context carries nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Returns the epoch that built this context, if it carries anything.
    ///
    /// A consumer uses this to find the compiled state to read it with, and to
    /// recognise a context that outlived a reconfiguration.
    #[must_use]
    pub fn epoch(&self) -> Option<Epoch> {
        if self.bytes.len() < EPOCH_BYTES {
            return None;
        }
        let mut raw = [0u8; EPOCH_BYTES];
        raw.copy_from_slice(&self.bytes[..EPOCH_BYTES]);
        Some(Epoch::new(u32::from_le_bytes(raw)))
    }

    /// Borrows the packed bytes, for propagating a context across a transport
    /// that carries it opaquely.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Rebuilds a context from bytes that crossed a transport.
    ///
    /// The bytes are structurally validated by [`CompiledMetadata::view`]
    /// before any accessor may read them.
    #[must_use]
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

/// A context read through the compiled state that built it.
///
/// Every accessor is an indexed load and a shift, because the epoch was checked
/// when the view was made and every offset is a compile-time constant.
#[derive(Debug, Clone, Copy)]
pub struct MetadataView<'a> {
    compiled: &'a CompiledMetadata,
    bytes: &'a [u8],
}

/// A context was read with compiled state from a different epoch.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("metadata context was built by epoch {context:?} but read against epoch {compiled:?}")]
pub struct EpochMismatch {
    /// The epoch stamped on the context.
    pub context: Option<Epoch>,
    /// The epoch of the compiled state it was read against.
    pub compiled: Epoch,
}

impl CompiledMetadata {
    /// Pairs a context with the compiled state that built it.
    ///
    /// An empty context pairs with any epoch, because it says nothing that an
    /// epoch could disagree with.
    pub fn view<'a>(
        &'a self,
        context: &'a MetadataContext,
    ) -> Result<MetadataView<'a>, ContextViewError> {
        if context.is_empty() {
            return Ok(MetadataView {
                compiled: self,
                bytes: &[],
            });
        }
        let bytes = context.as_bytes();
        if bytes.len() < CONTEXT_ID_BYTES {
            return Err(ContextViewError::Malformed {
                reason: "context is shorter than its identity header",
            });
        }
        let context_epoch = context.epoch();
        if context_epoch != Some(self.epoch) {
            return Err(ContextViewError::EpochMismatch(EpochMismatch {
                context: context_epoch,
                compiled: self.epoch,
            }));
        }
        let fingerprint = read_u64(bytes, EPOCH_BYTES);
        if fingerprint != self.layout.fingerprint {
            return Err(ContextViewError::LayoutMismatch {
                context: fingerprint,
                compiled: self.layout.fingerprint,
            });
        }
        validate_layout(bytes, &self.layout)?;
        Ok(MetadataView {
            compiled: self,
            bytes,
        })
    }
}

impl<'a> MetadataView<'a> {
    /// Returns whether a token resolved for this request.
    ///
    /// A token is all-or-nothing, so this also answers whether every key it
    /// produces is present. That is why a key's presence costs no bits of its
    /// own.
    #[must_use]
    pub fn has_token(&self, token: TokenId) -> bool {
        let layout = &self.compiled.layout;
        if self.bytes.is_empty() {
            return false;
        }
        let byte = token.index() / 8;
        if byte >= layout.token_bitmap_bytes {
            return false;
        }
        self.bytes[CONTEXT_ID_BYTES + byte] & (1 << (token.index() % 8)) != 0
    }

    /// Admits a consumer to this context.
    ///
    /// A required token that did not resolve is an admission failure: the
    /// caller Nacks the request and does not obtain a [`ConsumerMetadataView`],
    /// so none of the consumer's condition sets can be tested. An optional
    /// token that did not resolve is not an error; any condition that could
    /// have matched it simply contributes nothing, exactly as Envoy does for a
    /// descriptor that was not produced.
    pub fn consumer(
        self,
        consumer: ConsumerId,
    ) -> Result<ConsumerMetadataView<'a>, MissingRequiredTokens> {
        let required = self.compiled.token_requirements(consumer).required;
        let missing = required & !self.resolved_tokens();
        if missing != 0 {
            return Err(MissingRequiredTokens { missing });
        }
        Ok(ConsumerMetadataView {
            view: self,
            consumer,
        })
    }

    fn resolved_tokens(&self) -> u64 {
        let layout = &self.compiled.layout;
        if self.bytes.is_empty() {
            return 0;
        }
        let mut encoded = [0u8; size_of::<u64>()];
        encoded[..layout.token_bitmap_bytes].copy_from_slice(
            &self.bytes[CONTEXT_ID_BYTES..CONTEXT_ID_BYTES + layout.token_bitmap_bytes],
        );
        u64::from_le_bytes(encoded)
    }

    /// Borrows the value a slot carries, for a key that holds one value.
    #[must_use]
    pub fn slot_value(&self, slot: ValueSlotId) -> Option<&'a [u8]> {
        let region = self.region(slot.index())?;
        if region.is_empty() {
            return None;
        }
        Some(region)
    }

    /// Walks the values a slot carries, for a key that keeps every value it was
    /// offered.
    #[must_use]
    pub fn slot_values(&self, slot: ValueSlotId) -> SlotValues<'a> {
        SlotValues {
            remaining: self.region(slot.index()).unwrap_or_default(),
        }
    }

    fn region(&self, index: usize) -> Option<&'a [u8]> {
        if self.bytes.is_empty() {
            return None;
        }
        let layout = &self.compiled.layout;
        if index >= layout.regions {
            return None;
        }
        let at = layout.region_index_offset + index * size_of::<u16>();
        let start = read_u16(self.bytes, at) as usize;
        let end = read_u16(self.bytes, at + size_of::<u16>()) as usize;
        Some(&self.bytes[layout.data_offset + start..layout.data_offset + end])
    }

    fn pair_slot_word(&self, pair_slot: crate::ids::PairSlotId) -> u64 {
        let slot = self.compiled.pair_slot(pair_slot);
        self.compiled.pair_slot_fields[slot.fields.as_usize()]
            .iter()
            .fold(0, |word, field| {
                word | (self.symbol(field.symbol_slot) << field.shift)
            })
    }

    fn symbol(&self, symbol_slot: crate::ids::SymbolSlotId) -> u64 {
        let symbol = self.compiled.symbol_slot_at(symbol_slot);
        let layout = &self.compiled.layout;
        let field = &self.bytes
            [layout.symbol_field_offset..layout.symbol_field_offset + layout.symbol_field_bytes];
        read_bits(field, symbol.bit_offset, symbol.bits)
    }
}

/// A context admitted to one consumer.
///
/// This wrapper exists to make the engine's `Required` extension explicit:
/// matching is unavailable until the caller has checked the tokens that govern
/// admission. Optional tokens retain Envoy behavior and simply contribute no
/// descriptors when absent.
#[derive(Debug, Clone, Copy)]
pub struct ConsumerMetadataView<'a> {
    view: MetadataView<'a>,
    consumer: ConsumerId,
}

impl<'a> ConsumerMetadataView<'a> {
    /// Walks the descriptors this consumer's condition set selects.
    ///
    /// The iterator allocates nothing and preserves the source token of each
    /// match. A limiter therefore sees two applications when two tokens select
    /// the same entry, instead of a branch bitmask collapsing them into one.
    ///
    /// Condition-set identifiers are issued by the same compiler as the
    /// consumer. Passing another consumer's identifier is a programmer error,
    /// caught in debug builds; it cannot arise from request data.
    #[must_use]
    pub fn matches(&self, set: ConditionSetId) -> ConditionMatches<'a> {
        debug_assert_eq!(
            self.view.compiled.condition_set(set).consumer,
            self.consumer
        );
        let compiled_set = self.view.compiled.condition_set(set);
        ConditionMatches {
            view: self.view,
            participants: self.view.compiled.participants[compiled_set.participants.as_usize()]
                .iter(),
        }
    }

    /// Returns the admitted context view for reading retained values.
    #[must_use]
    pub fn metadata(&self) -> MetadataView<'a> {
        self.view
    }
}

/// A zero-allocation walk of one condition set's selected descriptors.
///
/// The compiler rejects condition entries that overlap for one token, so every
/// resolved token yields at most one item. The iterator's cardinality is
/// therefore bounded by the consumer's declared token count. Items are yielded
/// in that token declaration order, matching Envoy's descriptor vector order.
#[derive(Debug)]
pub struct ConditionMatches<'a> {
    view: MetadataView<'a>,
    participants: std::slice::Iter<'a, crate::condition::TableParticipant>,
}

impl<'a> Iterator for ConditionMatches<'a> {
    type Item = ConditionMatch;

    fn next(&mut self) -> Option<Self::Item> {
        for participant in self.participants.by_ref() {
            let pair_slot = self.view.compiled.pair_slot(participant.pair_slot);
            if !self.view.has_token(pair_slot.token) {
                continue;
            }
            let entry = self.view.compiled.tables.entry(
                participant.table_offset,
                self.view.pair_slot_word(participant.pair_slot),
            );
            if entry != 0 {
                return Some(ConditionMatch {
                    token: pair_slot.token,
                    entry: crate::ids::BranchIndex::from_index(usize::from(entry - 1)),
                });
            }
        }
        None
    }
}

/// The required tokens missing from a context.
///
/// The pipeline turns this into a Nack. The bitmap is deliberately compact:
/// names are available through [`CompiledMetadata::token_name`].
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("metadata context is missing one or more required tokens")]
pub struct MissingRequiredTokens {
    missing: u64,
}

impl MissingRequiredTokens {
    /// Returns whether a particular token was missing.
    #[must_use]
    pub fn contains(self, token: TokenId) -> bool {
        self.missing & (1 << token.index()) != 0
    }
}

/// Walks the values of a slot whose key keeps every value it was offered.
#[derive(Debug, Clone)]
pub struct SlotValues<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for SlotValues<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.len() < REPEATED_VALUE_PREFIX {
            return None;
        }
        let (prefix, rest) = self.remaining.split_at(REPEATED_VALUE_PREFIX);
        let len = u16::from_le_bytes([prefix[0], prefix[1]]) as usize;
        if len > rest.len() {
            self.remaining = &[];
            return None;
        }
        let (value, rest) = rest.split_at(len);
        self.remaining = rest;
        Some(value)
    }
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut encoded = [0u8; size_of::<u64>()];
    encoded.copy_from_slice(&bytes[at..at + size_of::<u64>()]);
    u64::from_le_bytes(encoded)
}

fn validate_layout(
    bytes: &[u8],
    layout: &crate::layout::ContextLayout,
) -> Result<(), ContextViewError> {
    if bytes.len() < layout.data_offset {
        return Err(ContextViewError::Malformed {
            reason: "context is shorter than its fixed layout",
        });
    }
    if layout.regions == 0 {
        return Ok(());
    }

    let data_bytes = bytes.len() - layout.data_offset;
    let mut previous = 0usize;
    for index in 0..=layout.regions {
        let at = layout.region_index_offset + index * size_of::<u16>();
        let offset = usize::from(read_u16(bytes, at));
        if offset < previous {
            return Err(ContextViewError::Malformed {
                reason: "context region index is not monotonic",
            });
        }
        if offset > data_bytes {
            return Err(ContextViewError::Malformed {
                reason: "context region index extends beyond data",
            });
        }
        previous = offset;
    }
    Ok(())
}
