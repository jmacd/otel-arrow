// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-key literal dictionaries.
//!
//! Only a key that some condition tests by value gets a dictionary, and it holds
//! only the literals those conditions declared. Encoding a request value is a
//! hash probe followed by a byte-for-byte comparison against the literal the
//! dictionary owns: the hash finds a candidate, the comparison decides. A hash
//! collision therefore cannot route one tenant's data to another tenant's
//! destination.
//!
//! Two symbols are reserved. [`Symbol::ABSENT`] is the value of an unset field,
//! so a zeroed PairSlot word is safe. [`Symbol::UNKNOWN`] is every value no
//! condition declared, which is what makes an unexpected value fail closed.

use crate::hashing::{CaseFolding, Hasher64, hash_bytes};
use hashbrown::HashTable;

/// A literal's position in its key's dictionary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(u32);

impl Symbol {
    /// The field is not set.
    pub const ABSENT: Self = Self(0);
    /// The field is set to something no condition declared.
    pub const UNKNOWN: Self = Self(1);

    /// The first symbol a literal may take.
    const FIRST_LITERAL: u32 = 2;

    /// Returns the symbol as a word, for packing into a PairSlot.
    pub(crate) const fn as_word(self) -> u64 {
        self.0 as u64
    }
}

/// Literal bytes, concatenated, with one more bound than there are literals.
#[derive(Debug)]
struct LiteralStore {
    bytes: Vec<u8>,
    bounds: Vec<u32>,
}

impl LiteralStore {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bounds: vec![0],
        }
    }

    fn len(&self) -> usize {
        self.bounds.len() - 1
    }

    fn get(&self, index: u32) -> &[u8] {
        let start = self.bounds[index as usize] as usize;
        let end = self.bounds[index as usize + 1] as usize;
        &self.bytes[start..end]
    }

    fn push(&mut self, literal: &[u8]) -> u32 {
        let index = self.len() as u32;
        self.bytes.extend_from_slice(literal);
        self.bounds.push(self.bytes.len() as u32);
        index
    }

    fn widest(&self) -> usize {
        self.bounds
            .windows(2)
            .map(|bound| (bound[1] - bound[0]) as usize)
            .max()
            .unwrap_or(0)
    }
}

/// The interned literals of one value-matched key.
#[derive(Debug)]
pub(crate) struct ValueDictionary {
    literals: LiteralStore,
    index: HashTable<u32>,
    hasher: Hasher64,
}

impl ValueDictionary {
    /// Returns the symbol for a request value, or [`Symbol::UNKNOWN`].
    pub(crate) fn symbol(&self, value: &[u8]) -> Symbol {
        let hash = hash_bytes(&self.hasher, value, CaseFolding::Exact);
        match self
            .index
            .find(hash, |&literal| self.literals.get(literal) == value)
        {
            Some(&literal) => Symbol(literal + Symbol::FIRST_LITERAL),
            None => Symbol::UNKNOWN,
        }
    }
}

/// Accumulates one key's literals while conditions are being declared.
#[derive(Debug)]
pub(crate) struct ValueDictionaryBuilder {
    literals: LiteralStore,
    index: HashTable<u32>,
    hasher: Hasher64,
}

impl ValueDictionaryBuilder {
    pub(crate) fn new() -> Self {
        Self {
            literals: LiteralStore::new(),
            index: HashTable::new(),
            hasher: Hasher64::default(),
        }
    }

    /// Interns a literal and returns its symbol. Conditions that declare the
    /// same literal share one dictionary entry, and therefore one symbol.
    pub(crate) fn intern(&mut self, literal: &[u8]) -> Symbol {
        let Self {
            literals,
            index,
            hasher,
        } = self;

        let hash = hash_bytes(hasher, literal, CaseFolding::Exact);
        if let Some(&found) = index.find(hash, |&other| literals.get(other) == literal) {
            return Symbol(found + Symbol::FIRST_LITERAL);
        }

        let inserted = literals.push(literal);
        let _ = index.insert_unique(hash, inserted, |&other| {
            hash_bytes(hasher, literals.get(other), CaseFolding::Exact)
        });
        Symbol(inserted + Symbol::FIRST_LITERAL)
    }

    /// Returns how many literals have been interned.
    pub(crate) fn len(&self) -> usize {
        self.literals.len()
    }

    /// Returns the widest literal interned, which validation compares against
    /// [`Limits::literal_bytes`](crate::Limits::literal_bytes).
    pub(crate) fn widest_literal(&self) -> usize {
        self.literals.widest()
    }

    pub(crate) fn build(self) -> ValueDictionary {
        ValueDictionary {
            literals: self.literals,
            index: self.index,
            hasher: self.hasher,
        }
    }
}
