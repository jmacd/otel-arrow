// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The dispatch table that turns an offered name into the extractors that want
//! it.
//!
//! A receiver walks its inbound headers once and offers each to the encoder. The
//! cost of an offer must therefore be one probe, and it must not allocate, even
//! when the name has to be compared without regard to case. That is what this
//! table provides: names are folded and interned at compile time, and a probe
//! folds the offered name into the hasher in place.
//!
//! One name may feed several extractors, because two tokens may read the same
//! header into different keys. Each target also records which of its extractor's
//! names matched, which is how a preserved wire name costs one byte instead of a
//! string.

use crate::condition::Range;
use crate::hashing::{CaseFolding, Hasher64, hash_bytes};
use crate::ids::ExtractorId;
use hashbrown::HashTable;

/// One extractor that wants a given name, and which of its names this is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NameTarget {
    /// The extractor to stage the value under.
    pub(crate) extractor: ExtractorId,
    /// The position of this name in the extractor's declared name list.
    pub(crate) ordinal: u8,
}

/// Names, interned, mapped to the extractors that want them.
#[derive(Debug)]
pub(crate) struct NameIndex {
    /// Name bytes, concatenated, as declared rather than folded, so that a
    /// preserved wire name is reproduced exactly.
    names: Box<[u8]>,
    /// One more entry than there are names.
    bounds: Box<[u32]>,
    /// Per name, the range of [`Self::targets`] that wants it.
    target_ranges: Box<[Range]>,
    targets: Box<[NameTarget]>,
    index: HashTable<u32>,
    hasher: Hasher64,
    folding: CaseFolding,
}

impl NameIndex {
    /// Returns the extractors that want `name`, or an empty slice.
    pub(crate) fn lookup(&self, name: &[u8]) -> &[NameTarget] {
        let hash = hash_bytes(&self.hasher, name, self.folding);
        match self
            .index
            .find(hash, |&entry| self.folding.eq(self.name(entry), name))
        {
            Some(&entry) => &self.targets[self.target_ranges[entry as usize].as_usize()],
            None => &[],
        }
    }

    /// Returns a declared name by its position in the flat name store.
    pub(crate) fn name(&self, entry: u32) -> &[u8] {
        let start = self.bounds[entry as usize] as usize;
        let end = self.bounds[entry as usize + 1] as usize;
        &self.names[start..end]
    }
}

/// Accumulates names while extractors are being declared.
#[derive(Debug)]
pub(crate) struct NameIndexBuilder {
    names: Vec<u8>,
    bounds: Vec<u32>,
    targets: Vec<Vec<NameTarget>>,
    index: HashTable<u32>,
    hasher: Hasher64,
    folding: CaseFolding,
}

impl NameIndexBuilder {
    pub(crate) fn new(folding: CaseFolding) -> Self {
        Self {
            names: Vec::new(),
            bounds: vec![0],
            targets: Vec::new(),
            index: HashTable::new(),
            hasher: Hasher64::default(),
            folding,
        }
    }

    /// Records that `extractor` wants `name` as its `ordinal`-th name.
    pub(crate) fn insert(&mut self, name: &str, extractor: ExtractorId, ordinal: u8) {
        let Self {
            names,
            bounds,
            targets,
            index,
            hasher,
            folding,
        } = self;
        let folding = *folding;

        let hash = hash_bytes(hasher, name.as_bytes(), folding);
        let existing = index.find(hash, |&entry| {
            folding.eq(slice_at(names, bounds, entry), name.as_bytes())
        });

        let entry = match existing {
            Some(&found) => found,
            None => {
                let inserted = (bounds.len() - 1) as u32;
                names.extend_from_slice(name.as_bytes());
                bounds.push(names.len() as u32);
                targets.push(Vec::new());
                let _ = index.insert_unique(hash, inserted, |&other| {
                    hash_bytes(hasher, slice_at(names, bounds, other), folding)
                });
                inserted
            }
        };

        targets[entry as usize].push(NameTarget { extractor, ordinal });
    }

    pub(crate) fn build(self) -> NameIndex {
        let mut flat = Vec::with_capacity(self.targets.iter().map(Vec::len).sum());
        let mut ranges = Vec::with_capacity(self.targets.len());
        for group in &self.targets {
            let start = flat.len() as u32;
            flat.extend_from_slice(group);
            ranges.push(Range {
                start,
                end: flat.len() as u32,
            });
        }

        NameIndex {
            names: self.names.into_boxed_slice(),
            bounds: self.bounds.into_boxed_slice(),
            target_ranges: ranges.into_boxed_slice(),
            targets: flat.into_boxed_slice(),
            index: self.index,
            hasher: self.hasher,
            folding: self.folding,
        }
    }
}

fn slice_at<'a>(names: &'a [u8], bounds: &[u32], entry: u32) -> &'a [u8] {
    let start = bounds[entry as usize] as usize;
    let end = bounds[entry as usize + 1] as usize;
    &names[start..end]
}
