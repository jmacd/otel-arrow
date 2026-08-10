// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Explicit bounds on every dimension of compiled and request-time state.
//!
//! The engine must not grow state without bound because of a configuration or
//! an input, so each dimension has a limit and exceeding it is an error rather
//! than an allocation. Defaults are chosen to fit realistic configurations
//! while keeping the compiled tables and the packed context small.

/// The upper bound on every compiled and request-time dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Distinct qualified field names.
    pub keys: usize,
    /// Extractors across all tokens, before deduplication.
    pub extractors: usize,
    /// Tokens. Bounded by 64 because the resolved-token bitmap is a `u64` word
    /// during construction.
    pub tokens: usize,
    /// Keys one token may hold.
    pub keys_per_token: usize,
    /// Wire names one extractor may match.
    pub names_per_extractor: usize,
    /// Condition sets across all consumers.
    pub condition_sets: usize,
    /// Entries one condition set may declare. Bounded by 64 to keep matching
    /// work and compile-time validation strictly small.
    pub branches_per_condition_set: usize,
    /// Distinct signatures after wildcard keys are dropped.
    pub signatures: usize,
    /// Compatible (token, signature) pairs a configuration may use.
    pub pair_slots: usize,
    /// Bits one PairSlot word may occupy. Bounded by 64, the width of the word.
    pub pair_slot_bits: u32,
    /// Entries across every branch table, which bounds compiled table memory.
    pub branch_table_entries: usize,
    /// Literals one key's dictionary may hold, including the two reserved
    /// symbols.
    pub dictionary_entries_per_key: usize,
    /// Bytes one literal may occupy.
    pub literal_bytes: usize,
    /// Bytes one extracted value may occupy.
    pub value_bytes: usize,
    /// Values one repeated key may retain.
    pub values_per_key: usize,
    /// Keys the packed context may retain, which sizes its value index.
    pub value_slots: usize,
    /// Bytes one packed context may occupy. Bounded by 65535 because the value
    /// index holds `u16` offsets.
    pub context_bytes: usize,
    /// Bytes the reusable construction scratch may retain between requests.
    pub scratch_bytes: usize,
    /// Context-producing sites.
    pub producers: usize,
    /// Context-observing sites.
    pub consumers: usize,
}

impl Limits {
    /// The widest values the packed representations can express.
    ///
    /// A caller may lower any of these but not raise them, because each is a
    /// property of the encoding rather than a policy choice.
    pub const MAX_TOKENS: usize = 64;
    /// The widest entry count the table's one-byte entry code supports.
    pub const MAX_BRANCHES: usize = 64;
    /// The widest PairSlot word.
    pub const MAX_PAIR_SLOT_BITS: u32 = 64;
    /// The largest context a `u16` value index can address.
    pub const MAX_CONTEXT_BYTES: usize = u16::MAX as usize;

    /// Returns the limits clamped to what the packed representations can hold.
    #[must_use]
    pub fn clamped(mut self) -> Self {
        self.tokens = self.tokens.min(Self::MAX_TOKENS);
        self.branches_per_condition_set = self.branches_per_condition_set.min(Self::MAX_BRANCHES);
        self.pair_slot_bits = self.pair_slot_bits.min(Self::MAX_PAIR_SLOT_BITS);
        self.context_bytes = self.context_bytes.min(Self::MAX_CONTEXT_BYTES);
        self
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            keys: 128,
            extractors: 256,
            tokens: Self::MAX_TOKENS,
            keys_per_token: 16,
            names_per_extractor: 16,
            condition_sets: 128,
            branches_per_condition_set: Self::MAX_BRANCHES,
            signatures: 128,
            pair_slots: 512,
            pair_slot_bits: Self::MAX_PAIR_SLOT_BITS,
            branch_table_entries: 1 << 16,
            dictionary_entries_per_key: 1024,
            literal_bytes: 256,
            value_bytes: 4096,
            values_per_key: 16,
            value_slots: 64,
            context_bytes: Self::MAX_CONTEXT_BYTES,
            scratch_bytes: 1 << 16,
            producers: 256,
            consumers: 1024,
        }
    }
}
