// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-producer extraction plans: the second level of reachability pruning.
//!
//! The first level removes what nothing anywhere observes. This level removes
//! what nothing *downstream of a particular producer* observes. A receiver that
//! feeds only an exporter reading `tenant_id` should not pay for the token a
//! router elsewhere in the engine matches on.
//!
//! A plan prunes work, never layout. The packed context keeps the epoch's single
//! layout, so a condition set this producer cannot reach simply stays at its
//! no-match value. That keeps every identifier stable across producers, which is
//! what lets a context cross node boundaries unchanged.

use crate::ids::{ExtractorId, SymbolSlotId, TokenId, ValueSlotId};

/// What one producer has to do to build a context.
#[derive(Debug)]
pub(crate) struct ExtractionPlan {
    /// The declared name, kept for diagnostics.
    pub(crate) name: Box<str>,
    /// Extractors worth staging a value for, as a bitmap indexed by
    /// [`ExtractorId`]. An offer for an extractor outside this set is dropped
    /// without touching scratch.
    live_extractors: Box<[u64]>,
    /// Tokens worth resolving, as a bitmap indexed by [`TokenId`].
    live_tokens: u64,
    /// The (token, key) symbols to encode, once their tokens have resolved.
    pub(crate) symbol_slots: Box<[SymbolSlotId]>,
    /// The values to retain.
    pub(crate) value_slots: Box<[ValueSlotId]>,
}

impl ExtractionPlan {
    pub(crate) fn new(
        name: Box<str>,
        extractor_count: usize,
        live_extractors: &[ExtractorId],
        live_tokens: &[TokenId],
        symbol_slots: Box<[SymbolSlotId]>,
        value_slots: Box<[ValueSlotId]>,
    ) -> Self {
        let mut extractor_bitmap = vec![0u64; extractor_count.div_ceil(64)].into_boxed_slice();
        for extractor in live_extractors {
            extractor_bitmap[extractor.index() / 64] |= 1 << (extractor.index() % 64);
        }
        let mut token_bitmap = 0u64;
        for token in live_tokens {
            token_bitmap |= 1 << token.index();
        }

        Self {
            name,
            live_extractors: extractor_bitmap,
            live_tokens: token_bitmap,
            symbol_slots,
            value_slots,
        }
    }

    /// Returns whether this producer has any reason to stage a value for an
    /// extractor. This is the check an offer pays, and it is one indexed load
    /// and a test.
    pub(crate) fn wants(&self, extractor: ExtractorId) -> bool {
        self.live_extractors[extractor.index() / 64] & (1 << (extractor.index() % 64)) != 0
    }

    /// Returns the tokens this producer resolves, as a bitmap.
    pub(crate) fn live_tokens(&self) -> u64 {
        self.live_tokens
    }
}
