// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Conditions and condition sets.
//!
//! A condition is Envoy's configured descriptor: a key sequence, with each key
//! either demanding a literal or wildcarded. It names no token, exactly as
//! Envoy's descriptor map names no producer -- `matchDescriptorEntries` compares
//! key sequences and never asks which action list produced the descriptor. The
//! compiler resolves a condition's candidate tokens by finding the consumer's
//! declared tokens whose key set is that sequence.
//!
//! A condition set is one consumer's descriptor map. Evaluating it is Envoy's
//! `requestAllowed(span<Descriptor>)`: every token the consumer declared that
//! resolved is looked up, and the matches accumulate.

use crate::ids::{BranchIndex, ConsumerId, KeyId, PairSlotId, TokenId};

/// What one key of a condition demands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPredicate {
    /// The key may hold anything. This is Envoy's empty descriptor value.
    ///
    /// A wildcard cannot change whether a condition matches, because its token
    /// having resolved already guarantees the key is present. The compiler
    /// therefore drops wildcards when it derives the condition's signature, and
    /// they cost neither table entries nor context bits.
    Any,
    /// The key must equal this literal, byte for byte.
    ///
    /// Every literal is interned at compile time. A request value that no
    /// condition declared resolves to the reserved unknown symbol and matches
    /// nothing, so an unexpected value fails closed and equality never
    /// degenerates into a hash comparison.
    Equals(Vec<u8>),
}

/// One key of a condition, and what it demands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionEntry {
    /// The key this entry speaks about.
    pub key: KeyId,
    /// What the key must hold.
    pub predicate: KeyPredicate,
}

impl ConditionEntry {
    /// Demands that a key equal a literal.
    #[must_use]
    pub fn equals(key: KeyId, literal: impl Into<Vec<u8>>) -> Self {
        Self {
            key,
            predicate: KeyPredicate::Equals(literal.into()),
        }
    }

    /// Names a key without constraining it.
    #[must_use]
    pub fn any(key: KeyId) -> Self {
        Self {
            key,
            predicate: KeyPredicate::Any,
        }
    }
}

/// One branch of a condition set: a key sequence, and a demand on each key.
///
/// The sequence must be the whole key set of one of the consumer's declared
/// tokens. Keys the condition does not constrain are named as wildcards, which
/// is how Envoy writes a catch-all entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Condition {
    /// One entry per key of the token it matches, in that token's declaration
    /// order. Descriptor entry order is significant in Envoy and remains so
    /// here.
    pub entries: Vec<ConditionEntry>,
}

impl Condition {
    /// Builds a condition from its entries.
    #[must_use]
    pub fn new(entries: Vec<ConditionEntry>) -> Self {
        Self { entries }
    }
}

/// One descriptor a condition set selected.
///
/// Token provenance is part of the result, not incidental diagnostics. Two
/// resolved tokens selecting the same entry represent two descriptors and
/// therefore two limiter applications, exactly as Envoy passes two descriptors
/// to its rate-limit service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConditionMatch {
    /// The resolved token that produced the descriptor.
    pub token: TokenId,
    /// The selected entry in declaration order.
    pub entry: BranchIndex,
}

/// One compiled condition set: the tables its consumer reads.
#[derive(Debug)]
pub(crate) struct CompiledConditionSet {
    /// The declared name, kept for diagnostics.
    pub(crate) name: Box<str>,
    /// The consumer that declared it, whose token list scopes what it matches.
    pub(crate) consumer: ConsumerId,
    /// How many entries it declared, which bounds the branch indices a match
    /// can name.
    pub(crate) branches: usize,
    /// The PairSlots it consults, as a range into the flat participant array.
    pub(crate) participants: Range,
}

/// One (condition set, PairSlot) pairing, and the branch table it reads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TableParticipant {
    /// The slot whose word indexes the table.
    pub(crate) pair_slot: PairSlotId,
    /// Where this table starts in the flat mask store.
    pub(crate) table_offset: u32,
}

/// A half-open range into a flat array, stored compactly.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Range {
    pub(crate) start: u32,
    pub(crate) end: u32,
}

impl Range {
    pub(crate) const fn as_usize(self) -> std::ops::Range<usize> {
        self.start as usize..self.end as usize
    }
}
