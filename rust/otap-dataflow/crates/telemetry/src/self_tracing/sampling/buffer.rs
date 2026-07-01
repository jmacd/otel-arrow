// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The per-worker integrated emit/sample buffer.
//!
//! Each engine worker owns one [`LocalSampleBuffer`]. A log record exists in it
//! once, whether it is logically part of the global log sample, the span-log
//! sample, both, or neither. The buffer is the first level of the two-level
//! sampler: it produces a reduced per-worker sample that the all-CPU processor
//! aggregates.
//!
//! It exploits one asymmetry of bottom-k sampling. A *kept* record is
//! provisional: a later arrival with a smaller key can evict it. A *displaced*
//! record is terminal: once past the cut it can never re-enter, because the
//! threshold only tightens as the window fills. So:
//!
//! - The criterion-one [`BottomFloor`] reservoir holds its kept records,
//!   reserved until the window closes, because their membership is provisional.
//! - When a record is displaced, on arrival or by eviction, and it is
//!   **span-selected** (in a locally-sampled span, or a span boundary), it is
//!   moved to the overflow buffer, because the span-log sample needs every
//!   in-span record raw. The load-bearing detail is that an evicted
//!   span-selected record must be moved at eviction time, which
//!   [`BottomFloor::observe`] makes possible by returning the displaced payload.
//! - A displaced record that is not span-selected is dropped; it was only a
//!   global-log candidate and lost.
//!
//! At the window boundary [`flush`](LocalSampleBuffer::flush) drains both: the
//! reserved criterion-one sample, Horvitz-Thompson weighted, and the raw
//! overflow. Each record is therefore flushed exactly once, and a span-selected
//! record that the reservoir kept serves both populations.
//!
//! This buffer covers the emit/sample mechanism. Criterion one applies the
//! stage-two heavy-hitter binding gate when constructed with
//! [`new_gated`](LocalSampleBuffer::new_gated), reading the process-global
//! heavy-hitter table so a globally abundant callsite is throttled here in
//! proportion to the fleet-wide congestion; the local-only fail-safe table never
//! binds, leaving plain inverse-frequency local sampling.

use super::bottom_floor::BottomFloor;
use super::heavy_hitter::SharedHeavyHitterTable;

/// A record together with whether it belongs to a locally-sampled span.
struct Held<R> {
    record: R,
    span_selected: bool,
}

/// One record kept in the criterion-one sample at flush.
pub struct SampledRecord<R> {
    /// The emitted record.
    pub record: R,
    /// The local Horvitz-Thompson adjusted count, the estimated number of
    /// arrivals this representative stands for in this worker's window. The
    /// all-CPU aggregator weights the representative by this count.
    pub adjusted_count: f64,
    /// Whether the record is in a locally-sampled span, so the aggregator also
    /// routes it to the per-span reservoir.
    pub span_selected: bool,
}

/// The two streams a window flush produces.
pub struct BufferFlush<R> {
    /// The criterion-one sample: reserved representatives, Horvitz-Thompson
    /// weighted.
    pub sample: Vec<SampledRecord<R>>,
    /// The raw span-selected records the reservoir did not keep, plus span
    /// boundaries. Each stands for one arrival, weight one, and feeds the
    /// per-span reservoir at the global level.
    pub raw_span: Vec<R>,
}

/// The per-worker integrated emit/sample buffer.
pub struct LocalSampleBuffer<R> {
    reservoir: BottomFloor<Held<R>>,
    overflow: Vec<R>,
}

impl<R> LocalSampleBuffer<R> {
    /// Create a buffer whose criterion-one sample holds up to `budget` records,
    /// with a deterministic seed.
    #[must_use]
    pub fn new(budget: usize, seed: u64) -> Self {
        Self {
            reservoir: BottomFloor::new(budget, seed),
            overflow: Vec::new(),
        }
    }

    /// Create a buffer whose criterion one applies the stage-two heavy-hitter
    /// binding gate from `gate`, so a globally abundant callsite is throttled at
    /// this worker in proportion to the fleet-wide congestion. The local-only
    /// fail-safe table never binds, leaving plain local sampling.
    #[must_use]
    pub fn new_gated(budget: usize, seed: u64, gate: SharedHeavyHitterTable) -> Self {
        Self {
            reservoir: BottomFloor::new_gated(budget, seed, gate),
            overflow: Vec::new(),
        }
    }

    /// Offer one emitted record.
    ///
    /// `log_callsite` is the criterion-one grouping key. `span_selected` is true
    /// when the record is inside a locally-sampled span or is a span boundary;
    /// such records are never dropped, because the span-log sample needs them
    /// all. The record is either reserved in the criterion-one sample or, if it
    /// loses or is later evicted, routed to the overflow when span-selected and
    /// dropped otherwise.
    pub fn observe(&mut self, record: R, log_callsite: u64, span_selected: bool) {
        let held = Held {
            record,
            span_selected,
        };
        if let Some(displaced) = self.reservoir.observe(log_callsite, held) {
            if displaced.span_selected {
                self.overflow.push(displaced.record);
            }
        }
    }

    /// Close the window: drain the reserved criterion-one sample and the raw
    /// overflow. The buffer is left empty for the next window.
    pub fn flush(&mut self) -> BufferFlush<R> {
        let out = self.reservoir.finalize();
        let mut raw_span = std::mem::take(&mut self.overflow);
        // The boundary record defines the threshold and is not in the sample,
        // so conserve it in the raw stream when it is span-selected.
        if let Some(boundary) = out.boundary {
            if boundary.span_selected {
                raw_span.push(boundary.record);
            }
        }
        let sample = out
            .kept
            .into_iter()
            .map(|kept| SampledRecord {
                record: kept.payload.record,
                adjusted_count: kept.adjusted_count,
                span_selected: kept.payload.span_selected,
            })
            .collect();
        BufferFlush { sample, raw_span }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn under_budget_keeps_everything_in_sample() {
        let mut buf = LocalSampleBuffer::<u32>::new(16, 1);
        for i in 0..5u32 {
            buf.observe(i, u64::from(i), false);
        }
        let out = buf.flush();
        assert_eq!(out.sample.len(), 5);
        assert!(out.raw_span.is_empty());
        // Nothing was sampled away, so each stands for one arrival.
        for rec in &out.sample {
            assert!((rec.adjusted_count - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn non_span_selected_rejects_are_dropped() {
        let budget = 8;
        let mut buf = LocalSampleBuffer::<u32>::new(budget, 7);
        for i in 0..1000u32 {
            buf.observe(i, u64::from(i % 4), false);
        }
        let out = buf.flush();
        assert_eq!(out.sample.len(), budget);
        // No span-selected records, so the overflow is empty: ordinary rejects
        // are dropped, not buffered.
        assert!(out.raw_span.is_empty());
    }

    #[test]
    fn span_selected_records_are_never_lost() {
        // The key correctness property: every span-selected record is flushed
        // exactly once, either as a reserved sample representative or in the raw
        // overflow, even as the reservoir evicts provisional members.
        let budget = 8;
        let mut buf = LocalSampleBuffer::<u32>::new(budget, 99);
        let n = 500u32;
        for i in 0..n {
            buf.observe(i, u64::from(i % 16), true);
        }
        let out = buf.flush();
        let span_in_sample = out.sample.iter().filter(|r| r.span_selected).count();
        assert_eq!(
            span_in_sample + out.raw_span.len(),
            n as usize,
            "span-selected records must be conserved across reserve and eviction"
        );
        // The criterion-one sample is still bounded by the budget.
        assert!(out.sample.len() <= budget);
    }

    #[test]
    fn mixed_streams_conserve_span_and_bound_sample() {
        let budget = 8;
        let mut buf = LocalSampleBuffer::<(u32, bool)>::new(budget, 5);
        let mut span_total = 0usize;
        for i in 0..600u32 {
            let span = i % 3 == 0;
            if span {
                span_total += 1;
            }
            buf.observe((i, span), u64::from(i % 10), span);
        }
        let out = buf.flush();
        let span_in_sample = out.sample.iter().filter(|r| r.span_selected).count();
        // Every span-selected record survives; ordinary ones only if sampled.
        assert_eq!(span_in_sample + out.raw_span.len(), span_total);
        assert!(out.sample.len() <= budget);
        // Overflow holds only span-selected records.
        assert!(out.raw_span.iter().all(|(_, span)| *span));
    }

    #[test]
    fn flush_resets_for_next_window() {
        let mut buf = LocalSampleBuffer::<u32>::new(4, 3);
        for i in 0..50u32 {
            buf.observe(i, 0, true);
        }
        let first = buf.flush();
        assert!(!first.sample.is_empty() || !first.raw_span.is_empty());
        // A fresh window starts empty.
        let second = buf.flush();
        assert!(second.sample.is_empty());
        assert!(second.raw_span.is_empty());
    }
}
