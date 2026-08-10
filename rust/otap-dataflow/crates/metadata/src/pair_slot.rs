// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PairSlots: one word per (token, signature), and the whole lookup key.
//!
//! A PairSlot exists for each (token, signature) pair some condition uses. It
//! gives every key of the signature a bit field wide enough for that key's
//! dictionary, so the slot's `u64` word is the concatenation of the symbols the
//! request produced for that token. That word is the index into the branch
//! tables: no hashing, no probing, one indexed load.
//!
//! Slots are shared. Two condition sets that both constrain `tenant_id` under
//! token `edge` read the same word, assembled from the same packed symbols.
//! The word is deliberately not stored in a context: it is a consumer-local
//! lookup key, not request metadata.
//!
//! A consumer only assembles the word when the slot's token resolved. An
//! unresolved token is therefore skipped before any branch table is read.

use crate::condition::Range;
use crate::ids::{PairSlotId, SignatureId, SymbolSlotId, TokenId};

/// One key's bit field within a PairSlot word.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PairSlotField {
    /// The extractor symbol this field packs.
    pub(crate) symbol_slot: SymbolSlotId,
    /// How far to shift the symbol into the word.
    pub(crate) shift: u32,
}

/// A compatible (token, signature) pair and the layout of its word.
#[derive(Debug)]
pub(crate) struct CompiledPairSlot {
    /// The token whose symbols this word holds.
    pub(crate) token: TokenId,
    /// The fields of the word, as a range into the flat field array.
    pub(crate) fields: Range,
    /// How many bits the word occupies, which sizes every table indexed by it.
    pub(crate) bits: u32,
}

impl CompiledPairSlot {
    /// Returns how many distinct words this slot can take, which is the length
    /// of every branch table indexed by it.
    pub(crate) const fn table_len(&self) -> Option<usize> {
        1usize.checked_shl(self.bits)
    }
}

/// Assigns bit fields to keys as PairSlots are created.
///
/// Fields are laid out in signature order, each as wide as its key's dictionary
/// needs. A key with four literals needs three bits: two reserved symbols plus
/// four literals is six values.
#[derive(Debug, Default)]
pub(crate) struct PairSlotBuilder {
    pub(crate) slots: Vec<CompiledPairSlot>,
    pub(crate) fields: Vec<PairSlotField>,
    /// Maps (token, signature) to the slot that already serves it.
    index: hashbrown::HashMap<(TokenId, SignatureId), PairSlotId>,
}

impl PairSlotBuilder {
    /// Returns the slot for a (token, signature) pair, creating it if this is
    /// the first condition to need it.
    ///
    /// `field_widths` gives the bit width of each key of the signature, in
    /// signature order.
    pub(crate) fn intern(
        &mut self,
        token: TokenId,
        signature: SignatureId,
        symbol_slots: &[SymbolSlotId],
        field_widths: &[u32],
    ) -> PairSlotId {
        if let Some(&existing) = self.index.get(&(token, signature)) {
            return existing;
        }

        let start = self.fields.len() as u32;
        let mut shift = 0;
        for (&symbol_slot, &width) in symbol_slots.iter().zip(field_widths) {
            self.fields.push(PairSlotField { symbol_slot, shift });
            shift += width;
        }

        let id = PairSlotId::from_index(self.slots.len());
        self.slots.push(CompiledPairSlot {
            token,
            fields: Range {
                start,
                end: self.fields.len() as u32,
            },
            bits: shift,
        });
        let _ = self.index.insert((token, signature), id);
        id
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

/// Returns how many bits a field of `cardinality` distinct symbols needs.
pub(crate) const fn field_bits(cardinality: usize) -> u32 {
    if cardinality <= 1 {
        0
    } else {
        usize::BITS - (cardinality - 1).leading_zeros()
    }
}
