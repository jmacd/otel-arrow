// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The compiled artifact: one immutable epoch of metadata state.
//!
//! Everything here is built once and never mutated, so request-time construction
//! and consumer lookups take no locks and every per-core pipeline shares one
//! copy. Each collection is a single `Box<[_]>` addressed by a dense identifier,
//! and anything variable-length is flattened with a range beside it, so the whole
//! artifact is a handful of allocations rather than one per declaration.
//!
//! Live reconfiguration compiles a new artifact with a new epoch rather than
//! editing this one. Contexts stamped with the old epoch stay readable for as
//! long as the old artifact is held, which is what lets a slow exporter finish
//! work that started before the change.

use crate::branch_table::BranchTables;
use crate::condition::{CompiledConditionSet, Range, TableParticipant};
use crate::dictionary::ValueDictionary;
use crate::error::CompileWarning;
use crate::hashing::CaseFolding;
use crate::ids::{
    ConditionSetId, ConsumerId, Epoch, ExtractorId, KeyId, PairSlotId, ProducerId, SymbolSlotId,
    TokenId, ValueSlotId,
};
use crate::layout::ContextLayout;
use crate::limits::Limits;
use crate::name_index::NameIndex;
use crate::pair_slot::{CompiledPairSlot, PairSlotField};
use crate::plan::ExtractionPlan;
use crate::scratch::MetadataScratch;
use crate::source::{PeerAddressPart, Repetition, ValueKind};

/// One rule that produces a value for a key, as the encoder needs it.
#[derive(Debug)]
pub(crate) struct CompiledExtractor {
    pub(crate) key: KeyId,
    pub(crate) repetition: Repetition,
    /// The source-specific limit tightened by [`Limits::value_bytes`].
    pub(crate) value_limit: usize,
}

/// A named group of extractors that resolves all-or-nothing.
#[derive(Debug)]
pub(crate) struct CompiledToken {
    /// Its extractors, as a range into [`CompiledMetadata::token_extractors`].
    pub(crate) extractors: Range,
}

/// One value-matched extractor's field in the packed symbol region.
#[derive(Debug)]
pub(crate) struct CompiledSymbolSlot {
    pub(crate) extractor: ExtractorId,
    /// Which dictionary encodes it.
    pub(crate) dictionary: u16,
    /// Where its bits start within the symbol region.
    pub(crate) bit_offset: u32,
    /// How many bits it occupies, which is what its dictionary needs.
    pub(crate) bits: u32,
}

/// One extractor that reads the peer's network address.
#[derive(Debug)]
pub(crate) struct PeerAddressExtractor {
    pub(crate) extractor: ExtractorId,
    pub(crate) part: PeerAddressPart,
}

/// One token-qualified field whose bytes the packed context carries.
#[derive(Debug)]
pub(crate) struct CompiledValueSlot {
    pub(crate) field: crate::ids::MetadataFieldId,
    pub(crate) key: KeyId,
    pub(crate) token: TokenId,
    pub(crate) extractor: ExtractorId,
    /// Whether the slot holds a sequence of values rather than one.
    pub(crate) repeated: bool,
    pub(crate) value_kind: ValueKind,
}

/// One consumer's admission contract, as bitmaps over tokens.
///
/// Requiring a token is a property of the node, not of the token, so it is kept
/// beside the consumer that declared it rather than beside the token itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokenRequirements {
    /// Tokens the consumer fails the request without.
    pub required: u64,
    /// Tokens the consumer uses when they are there.
    pub optional: u64,
}

/// One immutable epoch of compiled metadata state.
#[derive(Debug)]
pub struct CompiledMetadata {
    pub(crate) epoch: Epoch,
    pub(crate) limits: Limits,

    pub(crate) key_names: Box<[Box<str>]>,
    pub(crate) key_value_kinds: Box<[ValueKind]>,
    pub(crate) token_names: Box<[Box<str>]>,
    /// One entry per consumer, recording its admission contract.
    pub(crate) token_requirements: Box<[TokenRequirements]>,

    pub(crate) extractors: Box<[CompiledExtractor]>,
    pub(crate) header_names: NameIndex,
    pub(crate) claim_names: NameIndex,
    pub(crate) derived_names: NameIndex,
    pub(crate) peer_address_extractors: Box<[PeerAddressExtractor]>,

    pub(crate) tokens: Box<[CompiledToken]>,
    pub(crate) token_extractors: Box<[ExtractorId]>,

    pub(crate) symbol_slots: Box<[CompiledSymbolSlot]>,
    pub(crate) dictionaries: Box<[ValueDictionary]>,

    /// How many distinct signatures the conditions reduced to, kept for
    /// diagnostics: it is one dimension of the token-by-signature matrix.
    pub(crate) signature_count: usize,
    pub(crate) pair_slots: Box<[CompiledPairSlot]>,
    pub(crate) pair_slot_fields: Box<[PairSlotField]>,

    pub(crate) condition_sets: Box<[CompiledConditionSet]>,
    pub(crate) participants: Box<[TableParticipant]>,
    pub(crate) tables: BranchTables,

    pub(crate) value_slots: Box<[CompiledValueSlot]>,
    /// Maps exact token-qualified fields to their carried value slots.
    pub(crate) field_value_slots: Box<[(crate::ids::MetadataFieldId, ValueSlotId)]>,

    pub(crate) layout: ContextLayout,
    pub(crate) plans: Box<[ExtractionPlan]>,
}

impl CompiledMetadata {
    /// Returns the epoch this state was compiled as.
    ///
    /// A consumer compares this against the epoch stamped on a context it is
    /// handed. They differ only while a reconfiguration is in flight, and a
    /// consumer that finds a mismatch fails the request rather than reading a
    /// slot that has since moved.
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Returns the limits this state was compiled under.
    #[must_use]
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Returns where a token-qualified field's value is carried, if retained.
    ///
    /// A consumer resolves this once, at configuration time, and then reads by
    /// slot forever after. A field that is only ever matched has no slot,
    /// because its value was compiled down to a symbol and its bytes were never
    /// carried.
    #[must_use]
    pub fn value_slot(&self, field: crate::ids::MetadataFieldId) -> Option<ValueSlotId> {
        self.field_value_slots
            .iter()
            .find_map(|&(candidate, slot)| (candidate == field).then_some(slot))
    }

    /// Returns a key's declared name, for diagnostics.
    #[must_use]
    pub fn key_name(&self, key: KeyId) -> &str {
        &self.key_names[key.index()]
    }

    /// Returns the value kind declared for a key.
    #[must_use]
    pub fn key_value_kind(&self, key: KeyId) -> ValueKind {
        self.key_value_kinds[key.index()]
    }

    /// Returns a token's declared name, for diagnostics.
    #[must_use]
    pub fn token_name(&self, token: TokenId) -> &str {
        &self.token_names[token.index()]
    }

    /// Returns a producer's declared name, for diagnostics.
    #[must_use]
    pub fn producer_name(&self, producer: ProducerId) -> &str {
        &self.plans[producer.index()].name
    }

    /// Returns a consumer's admission contract.
    ///
    /// A node checks this against
    /// [`MetadataView::has_token`](crate::MetadataView::has_token) on ingress and
    /// fails the request when a required token did not resolve.
    #[must_use]
    pub fn token_requirements(&self, consumer: ConsumerId) -> TokenRequirements {
        self.token_requirements[consumer.index()]
    }

    /// Returns a condition set's declared name, for diagnostics.
    #[must_use]
    pub fn condition_set_name(&self, set: ConditionSetId) -> &str {
        &self.condition_sets[set.index()].name
    }

    /// Returns how many entries a set declared, which bounds the entry indices
    /// its matches can name.
    #[must_use]
    pub fn condition_set_branches(&self, set: ConditionSetId) -> usize {
        self.condition_sets[set.index()].branches
    }

    /// Returns which key a value slot carries.
    #[must_use]
    pub fn value_slot_key(&self, slot: ValueSlotId) -> KeyId {
        self.value_slots[slot.index()].key
    }

    /// Returns the exact token-qualified field a value slot carries.
    #[must_use]
    pub fn value_slot_field(&self, slot: ValueSlotId) -> crate::ids::MetadataFieldId {
        self.value_slots[slot.index()].field
    }

    /// Returns whether a value slot carries text or bytes.
    ///
    /// An exporter re-emitting a captured value needs this to choose between a
    /// text header and a binary one.
    #[must_use]
    pub fn value_slot_kind(&self, slot: ValueSlotId) -> ValueKind {
        self.value_slots[slot.index()].value_kind
    }

    /// Returns whether a value slot carries a sequence of values rather than
    /// one, so a caller knows to use
    /// [`MetadataView::slot_values`](crate::MetadataView::slot_values).
    #[must_use]
    pub fn value_slot_is_repeated(&self, slot: ValueSlotId) -> bool {
        self.value_slots[slot.index()].repeated
    }

    /// Returns how many distinct signatures the conditions reduced to.
    #[must_use]
    pub fn signature_count(&self) -> usize {
        self.signature_count
    }

    /// Returns how many bytes every compiled branch table occupies together.
    #[must_use]
    pub fn branch_table_bytes(&self) -> usize {
        self.tables.byte_len()
    }

    /// Returns how many bytes the fixed part of every packed context occupies.
    #[must_use]
    pub fn context_header_bytes(&self) -> usize {
        self.layout.data_offset
    }

    /// Borrows a producer's extraction plan.
    pub(crate) fn plan(&self, producer: ProducerId) -> &ExtractionPlan {
        &self.plans[producer.index()]
    }

    /// Starts building a context for one producer.
    ///
    /// The scratch buffer is reused across requests and does not allocate at
    /// steady state, so a producer keeps one and lends it to each request.
    pub fn encoder<'a>(
        &'a self,
        producer: ProducerId,
        scratch: &'a mut MetadataScratch,
    ) -> crate::encoder::ContextEncoder<'a> {
        crate::encoder::ContextEncoder::new(self, producer, scratch)
    }

    /// Returns the extractors that want an offered transport header name,
    /// compared without regard to case.
    pub(crate) fn header_targets(&self, name: &str) -> &[ExtractorId] {
        self.header_names.lookup(name.as_bytes())
    }

    /// Returns the extractors that want an offered claim name, compared exactly.
    pub(crate) fn claim_targets(&self, name: &str) -> &[ExtractorId] {
        self.claim_names.lookup(name.as_bytes())
    }

    /// Returns the extractors that want a value the producing node computed.
    pub(crate) fn derived_targets(&self, name: &str) -> &[ExtractorId] {
        self.derived_names.lookup(name.as_bytes())
    }

    pub(crate) fn extractor(&self, id: ExtractorId) -> &CompiledExtractor {
        &self.extractors[id.index()]
    }

    pub(crate) fn dictionary(&self, index: u16) -> &ValueDictionary {
        &self.dictionaries[index as usize]
    }

    pub(crate) fn pair_slot(&self, id: PairSlotId) -> &CompiledPairSlot {
        &self.pair_slots[id.index()]
    }

    pub(crate) fn condition_set(&self, id: ConditionSetId) -> &CompiledConditionSet {
        &self.condition_sets[id.index()]
    }

    pub(crate) fn symbol_slot_at(&self, id: SymbolSlotId) -> &CompiledSymbolSlot {
        &self.symbol_slots[id.index()]
    }

    pub(crate) fn value_slot_at(&self, id: ValueSlotId) -> &CompiledValueSlot {
        &self.value_slots[id.index()]
    }
}

/// What compilation found worth telling the caller about.
///
/// Warnings never fail compilation. The caller logs them once at startup, which
/// is how an operator learns that a configured header is going nowhere.
#[derive(Debug, Default)]
pub struct CompileReport {
    /// What reachability removed, and what it noticed.
    pub warnings: Vec<CompileWarning>,
}

impl CompileReport {
    /// Returns whether anything is worth reporting.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty()
    }
}

/// The folding rule each name namespace uses.
pub(crate) const HEADER_FOLDING: CaseFolding = CaseFolding::AsciiInsensitive;
/// Claim names and derived value names are compared exactly.
pub(crate) const EXACT_FOLDING: CaseFolding = CaseFolding::Exact;
