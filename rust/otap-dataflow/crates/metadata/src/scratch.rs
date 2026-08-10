// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reusable construction scratch.
//!
//! Building a context needs somewhere to stage the values a request offered.
//! The finished context holds any dictionary symbols; PairSlot words are
//! assembled later by the consumer that needs them. Nothing in scratch survives
//! the request, so a producer keeps one buffer and lends it to each one. At
//! steady state it allocates nothing: its vectors reach their compiled
//! dimensions on the first request and are reused thereafter.
//!
//! Resetting between requests does not clear anything. Staged values carry a
//! stamp, and a reset increments it, so a stale entry is recognised rather than
//! erased. Symbols and words need no reset at all, because they are only read for
//! tokens that resolved, and a token that resolved had every one of its values
//! staged this request.

use crate::compiled::CompiledMetadata;
use crate::ids::ExtractorId;

/// How many bytes one region of the packed context will occupy, where its bytes
/// come from, and where it lands.
///
/// Regions are measured before anything is written, so the packed context can be
/// sized exactly and filled in one pass.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RegionPlan {
    /// The extractor whose staged values fill it, when it is a value slot.
    pub(crate) source: Option<ExtractorId>,
    /// How many bytes it occupies.
    pub(crate) bytes: usize,
    /// Where it begins, relative to the start of the data region.
    pub(crate) offset: usize,
}

/// Where one staged value lives in the scratch byte buffer.
///
/// Values of one extractor are chained rather than laid out consecutively,
/// because a request may interleave repetitions of one header with other
/// headers, and staging must not care about the order they arrive in.
#[derive(Debug, Clone, Copy)]
struct StagedRange {
    start: u32,
    end: u32,
    /// The next value of the same extractor, or [`NO_NEXT`].
    next: u32,
}

/// Terminates a chain of staged values.
const NO_NEXT: u32 = u32::MAX;

/// What a request offered for one extractor.
#[derive(Debug, Clone, Copy)]
struct StagedExtractor {
    /// The reset this entry belongs to. An entry from an earlier reset is stale
    /// and reads as absent.
    stamp: u32,
    /// The first of its values, as an index into the scratch range chain.
    first: u32,
    /// The last of its values, so that appending is constant time.
    last: u32,
    /// How many values were kept.
    count: u16,
    /// Which of the extractor's names matched.
    ordinal: u8,
}

/// Reusable working memory for building contexts.
#[derive(Debug, Default)]
pub struct MetadataScratch {
    bytes: Vec<u8>,
    ranges: Vec<StagedRange>,
    staged: Vec<StagedExtractor>,
    regions: Vec<RegionPlan>,
    stamp: u32,
}

impl MetadataScratch {
    /// Creates empty scratch. It sizes itself to the compiled state on first use.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns how many bytes of working memory are currently retained, which a
    /// caller compares against [`Limits::scratch_bytes`](crate::Limits::scratch_bytes).
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.bytes.capacity()
            + self.ranges.capacity() * size_of::<StagedRange>()
            + self.staged.capacity() * size_of::<StagedExtractor>()
            + self.regions.capacity() * size_of::<RegionPlan>()
    }

    /// Returns bytes staged for the current request.
    pub(crate) fn staged_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Sizes the scratch to a compiled epoch and begins a new request.
    pub(crate) fn begin(&mut self, compiled: &CompiledMetadata) {
        let blank = StagedExtractor {
            stamp: 0,
            first: NO_NEXT,
            last: NO_NEXT,
            count: 0,
            ordinal: 0,
        };
        self.staged.resize(compiled.extractors.len(), blank);

        self.bytes.clear();
        self.ranges.clear();
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            // The stamp wrapped, so an entry from long ago could be mistaken for
            // a current one. This happens once every four billion requests and
            // costs one pass.
            self.staged.fill(blank);
            self.stamp = 1;
        }
    }

    /// Records a value for an extractor, replacing whatever was there.
    pub(crate) fn stage_first(&mut self, extractor: ExtractorId, ordinal: u8, value: &[u8]) {
        let index = self.push_value(value);
        self.staged[extractor.index()] = StagedExtractor {
            stamp: self.stamp,
            first: index,
            last: index,
            count: 1,
            ordinal,
        };
    }

    /// Appends a value to an extractor that keeps every one it is offered.
    pub(crate) fn stage_additional(&mut self, extractor: ExtractorId, value: &[u8]) {
        let index = self.push_value(value);
        let entry = self.staged[extractor.index()];
        self.ranges[entry.last as usize].next = index;
        let entry = &mut self.staged[extractor.index()];
        entry.last = index;
        entry.count = entry.count.saturating_add(1);
    }

    /// Returns whether a value was staged for an extractor this request.
    pub(crate) fn is_staged(&self, extractor: ExtractorId) -> bool {
        let entry = self.staged[extractor.index()];
        entry.stamp == self.stamp && entry.count > 0
    }

    /// Returns how many values were staged for an extractor this request.
    pub(crate) fn staged_count(&self, extractor: ExtractorId) -> usize {
        if self.is_staged(extractor) {
            usize::from(self.staged[extractor.index()].count)
        } else {
            0
        }
    }

    /// Returns which of an extractor's names matched.
    pub(crate) fn staged_ordinal(&self, extractor: ExtractorId) -> u8 {
        self.staged[extractor.index()].ordinal
    }

    /// Borrows every value staged for an extractor, in the order offered.
    pub(crate) fn staged_values(&self, extractor: ExtractorId) -> StagedValues<'_> {
        StagedValues {
            scratch: self,
            next: if self.is_staged(extractor) {
                self.staged[extractor.index()].first
            } else {
                NO_NEXT
            },
        }
    }

    /// Borrows the first value staged for an extractor.
    ///
    /// A value-matched key can only ever have one, because equality against a
    /// repeated value is not defined and the compiler rejects the combination.
    pub(crate) fn staged_single(&self, extractor: ExtractorId) -> &[u8] {
        let range = self.ranges[self.staged[extractor.index()].first as usize];
        &self.bytes[range.start as usize..range.end as usize]
    }

    /// Reserves room to format a value in place, avoiding a temporary.
    pub(crate) fn stage_formatted(
        &mut self,
        extractor: ExtractorId,
        ordinal: u8,
        write: impl FnOnce(&mut Vec<u8>),
    ) {
        let start = self.bytes.len() as u32;
        write(&mut self.bytes);
        let index = self.ranges.len() as u32;
        self.ranges.push(StagedRange {
            start,
            end: self.bytes.len() as u32,
            next: NO_NEXT,
        });
        self.staged[extractor.index()] = StagedExtractor {
            stamp: self.stamp,
            first: index,
            last: index,
            count: 1,
            ordinal,
        };
    }

    /// Clears the region measurements for a new request.
    pub(crate) fn reset_regions(&mut self, regions: usize) {
        self.regions.clear();
        self.regions.resize(regions, RegionPlan::default());
    }

    pub(crate) fn set_region(&mut self, index: usize, plan: RegionPlan) {
        self.regions[index] = plan;
    }

    pub(crate) fn region(&self, index: usize) -> RegionPlan {
        self.regions[index]
    }

    /// Assigns each region its offset within the data area and returns the
    /// total number of data bytes.
    pub(crate) fn place_regions(&mut self, regions: usize) -> usize {
        let mut offset = 0;
        for index in 0..regions {
            self.regions[index].offset = offset;
            offset += self.regions[index].bytes;
        }
        offset
    }

    fn push_value(&mut self, value: &[u8]) -> u32 {
        let start = self.bytes.len() as u32;
        self.bytes.extend_from_slice(value);
        let index = self.ranges.len() as u32;
        self.ranges.push(StagedRange {
            start,
            end: self.bytes.len() as u32,
            next: NO_NEXT,
        });
        index
    }
}

/// Walks the values one extractor staged, in the order they were offered.
#[derive(Debug)]
pub(crate) struct StagedValues<'a> {
    scratch: &'a MetadataScratch,
    next: u32,
}

impl<'a> Iterator for StagedValues<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == NO_NEXT {
            return None;
        }
        let range = self.scratch.ranges[self.next as usize];
        self.next = range.next;
        Some(&self.scratch.bytes[range.start as usize..range.end as usize])
    }
}
