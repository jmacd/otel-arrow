// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Branch tables: the PairSlot word in, one selected condition entry out.
//!
//! A token produces one Envoy descriptor. A descriptor selects at most one
//! configured entry of a condition set, never an arbitrary set of branches.
//! The compiler rejects overlapping entries for one token, so a branch table
//! needs only one byte per possible PairSlot word: zero means no matching entry;
//! a nonzero byte is the selected entry's one-based [`BranchIndex`](crate::BranchIndex).
//!
//! Evaluation preserves token provenance. A consumer walks the PairSlots for
//! its condition set, looks each resolved token up once, and yields
//! `ConditionMatch { token, entry }`. Thus two resolved tokens that select the
//! same entry remain two limiter applications rather than collapsing into one
//! bit in a mask.
//!
//! Dense means one byte per `2^bits` possible slot word. That is why a key's
//! field is only as wide as its dictionary and why wildcards contribute no bits.
//! [`Limits::branch_table_entries`](crate::Limits::branch_table_entries) bounds
//! the total.

/// Every branch table, flattened into one allocation.
#[derive(Debug)]
pub(crate) struct BranchTables {
    entries: Box<[u8]>,
}

impl BranchTables {
    /// Returns the selected entry code for `index`.
    ///
    /// Zero means no entry matched. A nonzero code is one more than the
    /// declaration-order [`BranchIndex`](crate::BranchIndex).
    pub(crate) fn entry(&self, offset: u32, index: u64) -> u8 {
        self.entries[offset as usize + index as usize]
    }

    /// Returns how many bytes every table occupies together.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.entries.len()
    }
}

/// Accumulates tables as condition sets are compiled.
#[derive(Debug, Default)]
pub(crate) struct BranchTableBuilder {
    entries: Vec<u8>,
    total_entries: usize,
}

impl BranchTableBuilder {
    /// Reserves a zeroed table of `len` entries and returns its byte offset.
    pub(crate) fn reserve(&mut self, len: usize) -> u32 {
        let offset = self.entries.len() as u32;
        self.entries.resize(self.entries.len() + len, 0);
        self.total_entries += len;
        offset
    }

    /// Sets the selected entry for one PairSlot word.
    ///
    /// The compiler checked that entries do not overlap before it builds
    /// tables, so the cell must still be zero. The assertion protects that
    /// invariant if a future compiler pass changes the construction order.
    pub(crate) fn set(&mut self, offset: u32, index: u64, entry: u8) {
        let cell = &mut self.entries[offset as usize + index as usize];
        debug_assert_eq!(*cell, 0);
        *cell = entry;
    }

    /// Returns how many entries every table holds together, which validation
    /// compares against [`Limits::branch_table_entries`](crate::Limits::branch_table_entries).
    pub(crate) fn total_entries(&self) -> usize {
        self.total_entries
    }

    pub(crate) fn build(self) -> BranchTables {
        BranchTables {
            entries: self.entries.into_boxed_slice(),
        }
    }
}
