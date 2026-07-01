// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The all-CPU windowed aggregator, Subsystem 3 of the engine mechanism.
//!
//! Every engine worker keeps a thread-local
//! [`LocalSampleBuffer`](super::buffer::LocalSampleBuffer) and flushes a reduced
//! per-worker sample each window. This aggregator is the second side of the
//! two-level sampler: it receives those flushes in memory, combines them into
//! one process-global sample over the same window, and republishes the two
//! feedback tables. See
//! [`docs/integrated-sampler-engine-mechanism.md`][mechanism] for the wiring and
//! the resolved decisions.
//!
//! It maintains the three estimators of the integrated model over the union of
//! the per-worker flushes, reusing the library:
//!
//! 1. **Criterion one, second level.** A [`BottomFloor`] over the per-worker
//!    criterion-one representatives, each observed with
//!    [`observe_weighted`](BottomFloor::observe_weighted) by its carried local
//!    adjusted count, so the group totals sum the fleet-wide counts `N_c`. It
//!    yields the process-global log sample and the
//!    [`HeavyHitterTable`](super::heavy_hitter::HeavyHitterTable) fed back to the
//!    workers.
//! 2. **Criterion two.** One [`UniformReservoir`] per `span_id` over the union
//!    of the raw span-selected records and the span-selected representatives.
//!    Keying by `span_id` reassembles a span whose records were produced on
//!    different workers.
//! 3. **The span-start value table.** A [`SpanStartValues`] surprisal
//!    accumulator over the in-span records plus a span-start counter over the
//!    distinct spans, which builds the next window's span-start threshold table
//!    for the [`SpanStartSampler`](super::span_start::SpanStartSampler).
//!
//! At the window boundary [`finalize`](WindowAggregator::finalize) annotates
//! every retained record with all three adjusted counts, republishes both
//! tables into their `ArcSwap`s, and returns the flushed records for the caller
//! to emit.
//!
//! Unlike the single-stream [`IntegratedSampler`](super::processor::IntegratedSampler),
//! the aggregator consumes a pre-sampled stream. It never sees every in-span
//! record, so its span-start volume comes from the distinct spans it observes
//! rather than from counting START events, which removes the need to recover the
//! span phase from a flushed record. The span boundaries ride the per-span
//! reservoir with the other in-span records rather than being kept verbatim.
//!
//! The aggregator is generic over the payload `R`, so it can be driven directly
//! in tests; the live adapter classifies each flushed `LogRecord` into the input
//! types below.
//!
//! [mechanism]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-sampler-engine-mechanism.md

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use super::super::span::SpanContext;
use super::bottom_floor::{BottomFloor, UniformReservoir};
use super::heavy_hitter::{HeavyHitterTable, SharedHeavyHitterTable, shared_local_only};
use super::processor::ProcessorConfig;
use super::record::{Annotated, FlushedRecord, Retained, trace_adjusted_count};
use super::span_start::{SharedSpanStartTable, SpanStartValues, shared_always};

/// One criterion-one representative from a worker flush, carrying the local
/// adjusted count it stands for.
pub struct RepInput<R> {
    /// The emitted payload, for example a buffered `LogRecord`.
    pub payload: R,
    /// The log callsite identity, the criterion-one grouping key.
    pub log_callsite: u64,
    /// The trace context, present for an in-span record or a span boundary.
    pub trace: Option<SpanContext>,
    /// The representative's local adjusted count, the number of arrivals it
    /// stood for in the worker's window.
    pub local_nhat: f64,
}

/// One raw span-selected record from a worker flush. It lost the worker's
/// criterion-one sample, so it belongs only to the span-log population and
/// carries weight one.
pub struct RawInput<R> {
    /// The emitted payload.
    pub payload: R,
    /// The log callsite identity, used only for the surprisal accumulator.
    pub log_callsite: u64,
    /// The trace context. A raw record is always inside a locally-sampled span
    /// or is one of its boundaries.
    pub trace: SpanContext,
}

/// One worker's window flush, handed to the aggregator in memory with every
/// field intact.
pub struct WorkerFlush<R> {
    /// The criterion-one representatives, Horvitz-Thompson weighted locally.
    pub reps: Vec<RepInput<R>>,
    /// The raw span-selected records the worker's reservoir did not keep.
    pub raw: Vec<RawInput<R>>,
}

/// The per-span state the aggregator keeps for one `span_id` in a window.
struct SpanState<R> {
    /// The span-start callsite, for the span-start volume and value tables.
    start_callsite: u64,
    /// The span's adjusted count, contributed once to its start callsite.
    trace_count: f64,
    /// The criterion-two reservoir over this span's in-span records.
    reservoir: UniformReservoir<Rc<Retained<R>>>,
}

/// The all-CPU windowed aggregator.
pub struct WindowAggregator<R> {
    config: ProcessorConfig,
    /// Criterion one, second level: the recursion of Bottom-Floor over the
    /// per-worker representatives.
    criterion_one: BottomFloor<Rc<Retained<R>>>,
    /// Per `span_id`: the criterion-two reservoir and the span's start volume.
    spans: HashMap<u64, SpanState<R>>,
    /// The span-start value accumulator and threshold-table feedback.
    values: SpanStartValues,
    /// The shared span-start threshold table published to the tracer.
    span_start_table: SharedSpanStartTable,
    /// The shared heavy-hitter table published to the workers.
    heavy_hitter_table: SharedHeavyHitterTable,
    /// A monotonically increasing seed source for per-span reservoirs.
    reservoir_seed: u64,
}

impl<R: Clone> WindowAggregator<R> {
    /// Create an aggregator publishing onto the given shared tables, with a
    /// deterministic seed for reproducible windows.
    #[must_use]
    pub fn new(
        config: ProcessorConfig,
        span_start_table: SharedSpanStartTable,
        heavy_hitter_table: SharedHeavyHitterTable,
        seed: u64,
    ) -> Self {
        Self {
            criterion_one: BottomFloor::new(config.k_logs, seed),
            spans: HashMap::new(),
            values: SpanStartValues::new(),
            span_start_table,
            heavy_hitter_table,
            reservoir_seed: seed ^ 0x9E37_79B9_7F4A_7C15,
            config,
        }
    }

    /// Create an aggregator together with fresh shared tables in their fail-safe
    /// states: the span-start table samples everything and the heavy-hitter
    /// table is local-only until the first window flush.
    #[must_use]
    pub fn with_tables(
        config: ProcessorConfig,
        seed: u64,
    ) -> (Self, SharedSpanStartTable, SharedHeavyHitterTable) {
        let span_start = shared_always();
        let heavy_hitter = shared_local_only();
        let agg = Self::new(config, span_start.clone(), heavy_hitter.clone(), seed);
        (agg, span_start, heavy_hitter)
    }

    /// The shared span-start table handle, for wiring the tracer's sampler.
    #[must_use]
    pub fn span_start_table(&self) -> SharedSpanStartTable {
        self.span_start_table.clone()
    }

    /// The shared heavy-hitter table handle, for wiring the workers' gate.
    #[must_use]
    pub fn heavy_hitter_table(&self) -> SharedHeavyHitterTable {
        self.heavy_hitter_table.clone()
    }

    /// Take in one worker's window flush.
    pub fn push(&mut self, flush: WorkerFlush<R>) {
        for rep in flush.reps {
            let record = Rc::new(Retained {
                payload: rep.payload,
                trace: rep.trace,
            });
            // Criterion one, second level: the representative stands for its
            // local adjusted count. A displaced record is dropped here; the
            // retained set holds it only while a criterion keeps it.
            let _ = self.criterion_one.observe_weighted(
                rep.log_callsite,
                rep.local_nhat,
                Rc::clone(&record),
            );
            if let Some(ctx) = rep.trace {
                self.observe_in_span(&record, rep.log_callsite, ctx);
            }
        }
        for raw in flush.raw {
            let ctx = raw.trace;
            let record = Rc::new(Retained {
                payload: raw.payload,
                trace: Some(ctx),
            });
            // A raw record lost criterion one at the worker, so it is a span-log
            // member only and never re-enters criterion one.
            self.observe_in_span(&record, raw.log_callsite, ctx);
        }
    }

    /// Route one in-span record into the surprisal accumulator and, for a
    /// locally-sampled span, its per-span reservoir.
    fn observe_in_span(&mut self, record: &Rc<Retained<R>>, log_callsite: u64, ctx: SpanContext) {
        // The surprisal accumulator sees every in-span record the aggregator is
        // handed. It sees a sample rather than the full stream, so its totals
        // are sample-based estimates that feed the optional value weighting,
        // which falls back to equal coverage when no scale is available.
        self.values.observe_in_span(ctx.start_callsite, log_callsite);

        if !ctx.locally_sampled {
            return;
        }
        let seed = self.next_reservoir_seed();
        let r = self.config.logs_per_span_per_window;
        let state = self.spans.entry(ctx.span_id.0).or_insert_with(|| SpanState {
            start_callsite: ctx.start_callsite,
            trace_count: trace_adjusted_count(&ctx),
            reservoir: UniformReservoir::new(r, seed),
        });
        state.reservoir.observe(Rc::clone(record));
    }

    /// Close the window: publish the next span-start and heavy-hitter tables,
    /// finalize the estimators, and return the retained set annotated with the
    /// three adjusted counts. Per-window state is reset.
    pub fn finalize(&mut self) -> Vec<FlushedRecord<R>> {
        // Span-start volumes come from the distinct spans observed this window,
        // each contributing its adjusted count once to its start callsite.
        let starts: Vec<(u64, f64)> = self
            .spans
            .values()
            .map(|state| (state.start_callsite, state.trace_count))
            .collect();
        for (start_callsite, trace_count) in starts {
            self.values.add_start(start_callsite, trace_count);
        }

        // Build the next window's span-start table before the surprisals are
        // rolled forward, so it uses this window's accumulators and last
        // window's surprisal scale.
        self.publish_span_start_table();

        // Criterion one, second level: the global sample and the heavy-hitter
        // table. Roll its per-callsite estimates into next window's surprisals
        // after the span-start table is built.
        let c1 = self.criterion_one.finalize();
        self.values.update_log_surprisals(&c1.group_estimates);
        self.heavy_hitter_table.store(Arc::new(HeavyHitterTable::from_estimates(
            &c1.group_estimates,
            c1.tau,
        )));

        let mut merged: HashMap<usize, Annotated<R>> = HashMap::new();

        for kept in c1.kept {
            let slot = Annotated::slot(&mut merged, &kept.payload);
            // The global log count scales the representative's local count up by
            // the second-level inclusion correction: value * (1 / pi_g).
            slot.set_logs(kept.value * kept.adjusted_count);
        }

        for (_span_id, state) in std::mem::take(&mut self.spans) {
            for kept in state.reservoir.finalize().kept {
                let slot = Annotated::slot(&mut merged, &kept.payload);
                slot.set_span_logs(kept.adjusted_count);
            }
        }

        // Reset the per-window accumulators; the surprisal scale carries over.
        self.values.reset_window();

        merged
            .into_values()
            .map(Annotated::into_flushed)
            .collect()
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
        self.span_start_table.store(Arc::new(table));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_tracing::span::{OtelTraceState, SpanId, TraceId, Threshold};

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

    fn rep(payload: u32, log_callsite: u64, trace: Option<SpanContext>, nhat: f64) -> RepInput<u32> {
        RepInput {
            payload,
            log_callsite,
            trace,
            local_nhat: nhat,
        }
    }

    #[test]
    fn aggregates_representatives_into_one_log_sample() {
        // Two workers each contribute representatives for the same callsites,
        // carrying local counts. Under budget every representative is kept at
        // unit global probability, so the summed global log count is the sum of
        // the carried local counts.
        let (mut agg, _sst, _hht) = WindowAggregator::<u32>::with_tables(config(), 1);

        let worker_a = WorkerFlush {
            reps: vec![rep(1, 10, None, 5.0), rep(2, 11, None, 3.0)],
            raw: vec![],
        };
        let worker_b = WorkerFlush {
            reps: vec![rep(3, 10, None, 8.0), rep(4, 12, None, 1.0)],
            raw: vec![],
        };
        agg.push(worker_a);
        agg.push(worker_b);

        let flushed = agg.finalize();
        let total_logs: f64 = flushed.iter().map(|f| f.logs_adjusted_count).sum();
        // 5 + 3 + 8 + 1 local arrivals, recovered globally.
        assert!((total_logs - 17.0).abs() < 1e-9, "global log total {total_logs}");
        // Out-of-span representatives carry no span populations.
        for f in &flushed {
            assert_eq!(f.traces_adjusted_count, 0.0);
            assert_eq!(f.span_logs_adjusted_count, 0.0);
        }
    }

    #[test]
    fn heavy_hitter_table_ranks_global_abundance() {
        // Callsite 10 is globally abundant across two workers; callsite 12 is
        // rare. The published heavy-hitter table must weight the abundant one
        // lower (g_c = 1 / N_c), so its binding gate throttles harder.
        let (mut agg, _sst, hht) = WindowAggregator::<u32>::with_tables(config(), 7);
        agg.push(WorkerFlush {
            reps: vec![rep(1, 10, None, 900.0), rep(2, 12, None, 2.0)],
            raw: vec![],
        });
        agg.push(WorkerFlush {
            reps: vec![rep(3, 10, None, 1100.0)],
            raw: vec![],
        });
        let _ = agg.finalize();

        let table = hht.load();
        // N_10 ~ 2000, N_12 ~ 2, so g_10 << g_12: the abundant callsite carries
        // the smaller global weight and is throttled harder at every worker.
        assert!(table.weight_for(10) < table.weight_for(12));
        assert!((table.weight_for(10) - 1.0 / 2000.0).abs() < 1e-9);
        assert!((table.weight_for(12) - 1.0 / 2.0).abs() < 1e-9);
    }

    #[test]
    fn under_budget_publishes_no_global_throttle() {
        // When the aggregator keeps every representative, there is no global
        // backpressure yet: tau_g is infinite, so the binding gate's global
        // score never binds and criterion one stays local-only.
        let (mut agg, _sst, hht) = WindowAggregator::<u32>::with_tables(config(), 8);
        agg.push(WorkerFlush {
            reps: vec![rep(1, 10, None, 3.0), rep(2, 11, None, 9.0)],
            raw: vec![],
        });
        let _ = agg.finalize();
        assert!(hht.load().tau().is_infinite());
    }

    #[test]
    fn over_budget_publishes_a_finite_global_threshold() {
        // With a tight budget and many callsites the window fills, so the
        // aggregator settles a finite tau_g and global backpressure engages.
        // Single-window per-callsite scores are noisy under inverse-frequency
        // weighting, which the design smooths across windows at the collector;
        // the deterministic abundance ranking is covered above via `weight_for`.
        let mut cfg = config();
        cfg.k_logs = 8;
        let (mut agg, _sst, hht) = WindowAggregator::<u32>::with_tables(cfg, 21);
        for _ in 0..30 {
            let mut reps = vec![rep(0, 500, None, 5000.0)];
            for c in 1..200u64 {
                reps.push(rep(c as u32, 600 + c, None, 1.0));
            }
            agg.push(WorkerFlush { reps, raw: vec![] });
            let _ = agg.finalize();
        }
        assert!(
            hht.load().tau().is_finite(),
            "a filled window settles a finite tau_g"
        );
    }

    #[test]
    fn span_records_reassemble_across_workers_by_span_id() {
        // One sampled span emits records on two different workers. Keying by
        // span_id, the aggregator reservoir-samples them together and every
        // in-span record carries the span adjusted count.
        let th = Threshold::from_probability(0.5);
        let span = ctx(0x5EED, 77, true, th);
        let (mut agg, _sst, _hht) = WindowAggregator::<u32>::with_tables(config(), 3);

        agg.push(WorkerFlush {
            reps: vec![rep(1, 20, Some(span), 1.0)],
            raw: vec![RawInput {
                payload: 2,
                log_callsite: 20,
                trace: span,
            }],
        });
        agg.push(WorkerFlush {
            reps: vec![],
            raw: vec![
                RawInput {
                    payload: 3,
                    log_callsite: 21,
                    trace: span,
                },
                RawInput {
                    payload: 4,
                    log_callsite: 21,
                    trace: span,
                },
            ],
        });

        let flushed = agg.finalize();
        // Every retained in-span record carries the span adjusted count 1/0.5.
        let with_span = flushed
            .iter()
            .filter(|f| f.traces_adjusted_count > 0.0)
            .count();
        assert!(with_span > 0, "expected span-annotated records");
        for f in &flushed {
            if f.traces_adjusted_count > 0.0 {
                assert!((f.traces_adjusted_count - 2.0).abs() < 1e-9);
            }
        }
        // The span-log population is non-empty: at least one record carries a
        // positive span-log count from the per-span reservoir.
        assert!(flushed.iter().any(|f| f.span_logs_adjusted_count > 0.0));
    }

    #[test]
    fn finalize_resets_for_next_window() {
        let (mut agg, _sst, _hht) = WindowAggregator::<u32>::with_tables(config(), 5);
        agg.push(WorkerFlush {
            reps: vec![rep(1, 10, None, 4.0)],
            raw: vec![],
        });
        let first = agg.finalize();
        assert!(!first.is_empty());
        // A fresh window starts empty.
        let second = agg.finalize();
        assert!(second.is_empty());
    }

    #[test]
    fn span_start_table_throttles_a_chatty_span_kind() {
        // A chatty span kind produces far more starts than a rare one. After a
        // window the published span-start table admits the chatty kind at a
        // lower probability than the rare kind, the equal-coverage feedback.
        let mut cfg = config();
        cfg.target_span_starts_per_window = 4.0;
        let (mut agg, sst, _hht) = WindowAggregator::<u32>::with_tables(cfg, 9);

        let chatty = 100u64;
        let rare = 200u64;
        let th = Threshold::ALWAYS;
        // Many distinct chatty spans, few rare spans.
        for i in 0..50u64 {
            let span = ctx(1_000 + i, chatty, true, th);
            agg.push(WorkerFlush {
                reps: vec![],
                raw: vec![RawInput {
                    payload: i as u32,
                    log_callsite: 30,
                    trace: span,
                }],
            });
        }
        for i in 0..3u64 {
            let span = ctx(9_000 + i, rare, true, th);
            agg.push(WorkerFlush {
                reps: vec![],
                raw: vec![RawInput {
                    payload: i as u32,
                    log_callsite: 31,
                    trace: span,
                }],
            });
        }
        let _ = agg.finalize();

        let table = sst.load();
        let p_chatty = 1.0 / table.threshold_for(chatty).adjusted_count();
        let p_rare = 1.0 / table.threshold_for(rare).adjusted_count();
        assert!(
            p_chatty < p_rare,
            "chatty probability {p_chatty} should be below rare {p_rare}"
        );
    }
}
