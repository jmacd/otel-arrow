// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The integrated sampling processor.
//!
//! A single windowed processor consumes the one emitted log stream and
//! maintains the three estimators of the integrated model at once:
//!
//! 1. **Criterion one**, a [`BottomFloor`] keyed by log callsite over every
//!    record, the global-independent log population. Output attribute
//!    [`ATTR_LOGS_ADJUSTED_COUNT`].
//! 2. **Criterion two**, one [`UniformReservoir`] per `span_id` fed by the
//!    in-span logs of locally-sampled spans, the span-log population. Output
//!    attribute [`ATTR_SPAN_LOGS_ADJUSTED_COUNT`].
//! 3. The **span-start value table**, a surprisal accumulator keyed by
//!    span-start callsite over every in-span record plus a span-start counter
//!    over START events, which builds the next window's span-start threshold
//!    table for the [`SpanStartSampler`](super::span_start::SpanStartSampler).
//!
//! Spans surface as two log events: START and END are kept verbatim whenever
//! their span is locally sampled, and carry [`ATTR_TRACES_ADJUSTED_COUNT`] equal
//! to the span's adjusted count, with span-log count one.
//!
//! The hot path emits each log exactly once; this processor decides which
//! populations a record belongs to. At the window boundary it flushes its
//! retained set, annotating each record with all three adjusted counts under
//! the exact-zero rule: a zero count states the record is present but not a
//! member of that population. Memory is bounded to the criterion-one budget plus
//! the per-span reservoirs of concurrent sampled spans plus the per-callsite
//! tables; nothing grows with traffic.
//!
//! The processor is generic over the emitted payload `R`, so it can be driven
//! directly in tests or wrapped by a pipeline adapter that carries
//! `LogRecord`s. A record is retained by reference count, so a record kept by
//! both criteria is stored once and freed as soon as neither keeps it.

use std::collections::HashMap;
use std::rc::Rc;

use super::super::span::{SpanContext, Threshold};
use super::bottom_floor::{BottomFloor, UniformReservoir};
use super::record::{Annotated, FlushedRecord, Retained, trace_adjusted_count};
use super::span_start::{
    SharedSpanStartTable, SpanStartSampler, SpanStartValues, shared_always,
};

/// Which kind of log event a record is, for routing in the processor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpanPhase {
    /// An ordinary log event, in or out of a span.
    #[default]
    Plain,
    /// A span START boundary event.
    Start,
    /// A span END boundary event.
    End,
}

/// Configuration for the integrated sampling processor.
#[derive(Clone, Copy, Debug)]
pub struct ProcessorConfig {
    /// The criterion-one Bottom-Floor budget `k`.
    pub k_logs: usize,
    /// The per-span reservoir size `r`, criterion two.
    pub logs_per_span_per_window: usize,
    /// The number of span starts to admit per window across all callsites.
    pub target_span_starts_per_window: f64,
    /// Whether span-start thresholds are weighted by the surprisal value
    /// `V_s`; off falls back to equal coverage.
    pub enable_span_value_weighting: bool,
    /// The pseudo-count `kappa` pulling an under-observed span kind's value
    /// score toward the average surprisal.
    pub span_value_shrinkage: f64,
    /// The threshold for a span-start callsite not yet in the table.
    pub default_span_start_threshold: Threshold,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            k_logs: 1024,
            logs_per_span_per_window: 16,
            target_span_starts_per_window: 100.0,
            enable_span_value_weighting: true,
            span_value_shrinkage: 4.0,
            default_span_start_threshold: Threshold::ALWAYS,
        }
    }
}

/// One record entering the processor, classified by the hot path.
pub struct IncomingRecord<R> {
    /// The emitted payload, for example a `LogRecord`.
    pub payload: R,
    /// The log callsite identity for criterion one.
    pub log_callsite: u64,
    /// The trace context, present for an in-span record or a span boundary.
    pub trace: Option<SpanContext>,
    /// Whether the record is an ordinary log, a span START, or a span END.
    pub phase: SpanPhase,
}

/// The integrated sampling processor.
pub struct IntegratedSampler<R> {
    config: ProcessorConfig,
    criterion_one: BottomFloor<Rc<Retained<R>>>,
    reservoirs: HashMap<u64, UniformReservoir<Rc<Retained<R>>>>,
    /// Records kept verbatim because they are span boundaries of sampled spans.
    verbatim: Vec<Rc<Retained<R>>>,

    /// The span-start value accumulator and threshold-table feedback.
    values: SpanStartValues,

    /// The shared span-start threshold table published to the tracer.
    span_start_table: SharedSpanStartTable,
    /// A monotonically increasing seed source for per-span reservoirs.
    reservoir_seed: u64,
}

impl<R: Clone> IntegratedSampler<R> {
    /// Create a processor publishing onto `span_start_table`, with a
    /// deterministic seed for reproducible windows.
    #[must_use]
    pub fn new(config: ProcessorConfig, span_start_table: SharedSpanStartTable, seed: u64) -> Self {
        Self {
            criterion_one: BottomFloor::new(config.k_logs, seed),
            reservoirs: HashMap::new(),
            verbatim: Vec::new(),
            values: SpanStartValues::new(),
            span_start_table,
            reservoir_seed: seed ^ 0x51ED_2701_99B0_F7A3,
            config,
        }
    }

    /// Create a processor together with a [`SpanStartSampler`] that shares its
    /// span-start threshold table.
    ///
    /// The returned sampler is installed on the tracer hot path, and the
    /// processor consumes the emitted stream and republishes the table each
    /// window. The table starts in the fail-safe "sample everything" state until
    /// the first window flush.
    #[must_use]
    pub fn with_sampler(config: ProcessorConfig, seed: u64) -> (Self, SpanStartSampler) {
        let table = shared_always();
        let sampler = SpanStartSampler::new(table.clone());
        (Self::new(config, table, seed), sampler)
    }

    /// Offer one emitted record to all estimators.
    pub fn observe(&mut self, incoming: IncomingRecord<R>) {
        let IncomingRecord {
            payload,
            log_callsite,
            trace,
            phase,
        } = incoming;

        let record = Rc::new(Retained {
            payload,
            trace,
        });

        // Criterion one samples every record by its log callsite. The displaced
        // record (if any) is dropped here; the retained set holds it only while
        // a criterion keeps it.
        let _ = self
            .criterion_one
            .observe(log_callsite, Rc::clone(&record));

        let Some(ctx) = trace else {
            return;
        };

        // The surprisal accumulator sees every in-span record, sampled or not,
        // so its bucket totals are exact window values.
        self.values.observe_in_span(ctx.start_callsite, log_callsite);

        if !ctx.locally_sampled {
            return;
        }

        match phase {
            SpanPhase::Start => {
                // Count the estimated true span starts from the START's own
                // adjusted count, then keep the boundary verbatim.
                self.values
                    .add_start(ctx.start_callsite, trace_adjusted_count(&ctx));
                self.verbatim.push(record);
            }
            SpanPhase::End => {
                self.verbatim.push(record);
            }
            SpanPhase::Plain => {
                let seed = self.next_reservoir_seed();
                let r = self.config.logs_per_span_per_window;
                self.reservoirs
                    .entry(ctx.span_id.0)
                    .or_insert_with(|| UniformReservoir::new(r, seed))
                    .observe(record);
            }
        }
    }

    /// Close the window: publish the next span-start threshold table, finalize
    /// the estimators, and return the retained set annotated with three
    /// adjusted counts. Per-window state is reset.
    pub fn flush(&mut self) -> Vec<FlushedRecord<R>> {
        // Build the next window's span-start table before the surprisals are
        // rolled forward, so it uses this window's accumulators and last
        // window's surprisal scale `H`.
        self.publish_span_start_table();

        // Criterion one. Roll its per-callsite estimates into next window's
        // surprisals after the table is built.
        let c1 = self.criterion_one.finalize();
        self.values.update_log_surprisals(&c1.group_estimates);

        let mut merged: HashMap<usize, Annotated<R>> = HashMap::new();

        for kept in c1.kept {
            let slot = Annotated::slot(&mut merged, &kept.payload);
            slot.set_logs(kept.adjusted_count);
        }

        for (_span_id, reservoir) in std::mem::take(&mut self.reservoirs) {
            for kept in reservoir.finalize().kept {
                let slot = Annotated::slot(&mut merged, &kept.payload);
                slot.set_span_logs(kept.adjusted_count);
            }
        }

        for record in std::mem::take(&mut self.verbatim) {
            let slot = Annotated::slot(&mut merged, &record);
            // START and END are their span's boundary markers: one span-log
            // arrival each, never reservoir-sampled.
            slot.set_span_logs(1.0);
        }

        // Reset the per-window accumulators; the surprisal scale carries over.
        self.values.reset_window();

        merged
            .into_values()
            .map(Annotated::into_flushed)
            .collect()
    }

    /// The shared span-start table handle, for wiring the tracer's sampler.
    #[must_use]
    pub fn span_start_table(&self) -> SharedSpanStartTable {
        self.span_start_table.clone()
    }

    fn next_reservoir_seed(&mut self) -> u64 {
        self.reservoir_seed = self
            .reservoir_seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.reservoir_seed
    }

    /// Publish the next window's span-start threshold table through the
    /// `ArcSwap`. Uses equal coverage unless value weighting is enabled and a
    /// surprisal scale is available.
    fn publish_span_start_table(&self) {
        let table = self.values.build_table(
            self.config.target_span_starts_per_window,
            self.config.enable_span_value_weighting,
            self.config.span_value_shrinkage,
            self.config.default_span_start_threshold,
        );
        self.span_start_table.store(std::sync::Arc::new(table));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_tracing::span::{OtelTraceState, SpanId, TraceId};

    fn ctx(span_id: u64, start_callsite: u64, sampled: bool, th: Threshold) -> SpanContext {
        SpanContext {
            trace_id: TraceId(u128::from(span_id) | (1 << 64)),
            span_id: SpanId(span_id),
            flags: Default::default(),
            ot: OtelTraceState {
                rv: None,
                th: Some(th),
            },
            locally_sampled: sampled,
            start_callsite,
        }
    }

    fn config() -> ProcessorConfig {
        ProcessorConfig {
            k_logs: 64,
            logs_per_span_per_window: 4,
            target_span_starts_per_window: 10.0,
            enable_span_value_weighting: false,
            span_value_shrinkage: 4.0,
            default_span_start_threshold: Threshold::ALWAYS,
        }
    }

    #[test]
    fn plain_logs_get_logs_count_only() {
        let table = shared_always();
        let mut p = IntegratedSampler::<u32>::new(config(), table, 1);
        for i in 0..200u32 {
            p.observe(IncomingRecord {
                payload: i,
                log_callsite: u64::from(i % 8),
                trace: None,
                phase: SpanPhase::Plain,
            });
        }
        let out = p.flush();
        assert!(!out.is_empty());
        for r in &out {
            // Out-of-span records are not in any span population.
            assert_eq!(r.traces_adjusted_count, 0.0);
            assert_eq!(r.span_logs_adjusted_count, 0.0);
            assert!(r.logs_adjusted_count > 0.0);
        }
    }

    #[test]
    fn span_boundaries_are_kept_verbatim_with_span_count() {
        let table = shared_always();
        let mut p = IntegratedSampler::<&'static str>::new(config(), table, 2);
        // A sampled span at one-in-four: threshold for probability 0.25.
        let th = Threshold::from_probability(0.25);
        let c = ctx(7, 0xABCD, true, th);
        p.observe(IncomingRecord {
            payload: "start",
            log_callsite: 0xABCD,
            trace: Some(c),
            phase: SpanPhase::Start,
        });
        p.observe(IncomingRecord {
            payload: "end",
            log_callsite: 0xABCD,
            trace: Some(c),
            phase: SpanPhase::End,
        });
        let out = p.flush();
        let starts: Vec<_> = out
            .iter()
            .filter(|r| r.payload == "start" || r.payload == "end")
            .collect();
        assert_eq!(starts.len(), 2);
        for r in starts {
            assert_eq!(r.span_logs_adjusted_count, 1.0);
            // Span count is the threshold's adjusted count, about 4.
            assert!((r.traces_adjusted_count - 4.0).abs() < 0.001);
        }
    }

    #[test]
    fn long_span_reservoir_bounds_and_estimates_in_span_volume() {
        let table = shared_always();
        let mut p = IntegratedSampler::<u32>::new(config(), table, 3);
        let c = ctx(11, 0x1111, true, Threshold::ALWAYS);
        let n = 400u32;
        for i in 0..n {
            p.observe(IncomingRecord {
                payload: i,
                log_callsite: 0x2000 + u64::from(i), // each a distinct log callsite
                trace: Some(c),
                phase: SpanPhase::Plain,
            });
        }
        let out = p.flush();
        // Records carrying a positive span-log count are the reservoir
        // representatives, bounded by r = 4.
        let span_logs: Vec<_> = out
            .iter()
            .filter(|r| r.span_logs_adjusted_count > 0.0)
            .collect();
        assert_eq!(span_logs.len(), config().logs_per_span_per_window);
        // Their span-log counts estimate the in-span volume n.
        let est: f64 = span_logs.iter().map(|r| r.span_logs_adjusted_count).sum();
        let ratio = est / f64::from(n);
        assert!((ratio - 1.0).abs() < 0.6, "in-span volume estimate ratio {ratio}");
        // Every in-span record carries the span count.
        for r in span_logs {
            assert_eq!(r.traces_adjusted_count, 1.0);
        }
    }

    #[test]
    fn feedback_table_throttles_chatty_span_kind() {
        // Two span kinds with very different start volumes. After one window the
        // published table should throttle the chatty kind below the rare kind.
        let table = shared_always();
        let mut cfg = config();
        cfg.target_span_starts_per_window = 4.0;
        let mut p = IntegratedSampler::<u32>::new(cfg, table.clone(), 9);

        let chatty = ctx(1, 0xAAAA, true, Threshold::ALWAYS);
        let rare = ctx(2, 0xBBBB, true, Threshold::ALWAYS);
        for i in 0..500u32 {
            p.observe(IncomingRecord {
                payload: i,
                log_callsite: 0xAAAA,
                trace: Some(chatty),
                phase: SpanPhase::Start,
            });
        }
        for i in 0..5u32 {
            p.observe(IncomingRecord {
                payload: 10_000 + i,
                log_callsite: 0xBBBB,
                trace: Some(rare),
                phase: SpanPhase::Start,
            });
        }
        let _ = p.flush();

        let published = table.load();
        let p_chatty = 1.0 / published.threshold_for(0xAAAA).adjusted_count();
        let p_rare = 1.0 / published.threshold_for(0xBBBB).adjusted_count();
        assert!(
            p_chatty < p_rare,
            "chatty {p_chatty} should be throttled below rare {p_rare}"
        );
    }

    #[test]
    fn record_kept_by_both_criteria_carries_both_counts() {
        // One short sampled span with a single in-span log. Criterion one keeps
        // it (budget is generous) and criterion two keeps it (reservoir not
        // full), so it carries positive logs and span-log counts at once.
        let table = shared_always();
        let mut p = IntegratedSampler::<u32>::new(config(), table, 4);
        let c = ctx(21, 0x3333, true, Threshold::ALWAYS);
        p.observe(IncomingRecord {
            payload: 42,
            log_callsite: 0x4444,
            trace: Some(c),
            phase: SpanPhase::Plain,
        });
        let out = p.flush();
        let rec = out.iter().find(|r| r.payload == 42).expect("record kept");
        assert!(rec.logs_adjusted_count > 0.0, "global sample");
        assert!(rec.span_logs_adjusted_count > 0.0, "span-log sample");
        assert_eq!(rec.traces_adjusted_count, 1.0, "span count");
    }
}
