// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Cost of deriving local-root span-start probabilities from the span-logs sample.
//!
//! At each window boundary the integrated sampler turns the previous window's
//! span-logs sample into the next window's span-start threshold table. That
//! derivation, specified in
//! [`docs/integrated-logs-traces-reservoir.md`](../../../docs/integrated-logs-traces-reservoir.md)
//! and in the `SPAN_LOGS.md` study, is two steps over the accumulated sample:
//!
//! 1. `update_log_surprisals`: recompute the per-log-callsite surprisal scale
//!    `g_l = -ln c_l`, the unseen floor, and the mean surprisal `H` from
//!    criterion one's estimated counts. This is `O(k)` in the number of log
//!    callsites `k`.
//! 2. `build_table`: for each local-root kind `s`, form the shrinkage-adjusted
//!    value score `V~_s = (sum g + kappa*H)/(N[s,.]+kappa)`, spread the target
//!    budget across kinds by `A_s = V_s * T / sum V`, and derive the admission
//!    probability `P_s = clamp(A_s / N_starts[s], 0, 1)` as an OTEP-235
//!    threshold. This is `O(S*)` in the number of span-start kinds `S*`.
//!
//! The design claims the whole allocation runs "in a few hundred microseconds
//! per window at a typical scale of a few tens of root kinds and a few hundred
//! log callsites". These benchmarks measure it directly, isolating the two
//! steps and their sum, across scales from that typical point up to a stress
//! case an order of magnitude larger.

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use otap_df_telemetry::self_tracing::sampling::span_start::bench_support::SpanStartValuesHarness;
use otap_df_telemetry::self_tracing::span::Threshold;

criterion_group!(benches, bench_derivation);
criterion_main!(benches);

/// (span-start kinds `S*`, log callsites `k`, label). The first is the design's
/// stated typical scale; the rest scale both dimensions up to a stress case.
const SCALES: &[(usize, usize, &str)] = &[
    (10, 100, "typical_10k_100l"),
    (50, 500, "medium_50k_500l"),
    (200, 2000, "large_200k_2000l"),
    (1000, 10000, "stress_1000k_10000l"),
];

/// The operator's single volume knob: admitted span starts per window.
const TARGET_SPAN_STARTS: f64 = 1000.0;
/// The shrinkage pseudo-count pulling under-observed kinds toward the mean.
const SHRINKAGE: f64 = 4.0;
/// Average in-span logs observed per span kind when priming the accumulator.
/// Only affects setup, not the `O(S*)` build cost, which is per kind.
const LOGS_PER_KIND: usize = 40;

/// A small deterministic LCG so the workload is reproducible without an rng dep.
struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 1.0) / (1u64 << 53) as f64
    }

    /// A heavy-tailed positive weight: most values small, a few large, matching
    /// the heavy-head long-tail shape the sampler is built for.
    fn heavy_tailed(&mut self) -> f64 {
        // Uniform u in (0,1], then 1/u gives a Pareto-like tail.
        1.0 / self.unit()
    }

    /// A log-callsite index skewed toward the low indices, so a few callsites
    /// dominate the column mass and the tail is rare.
    fn skewed_index(&mut self, k: usize) -> u64 {
        let u = self.unit();
        // Square biases toward 0; scale into [0, k).
        let idx = (u * u * k as f64) as usize;
        idx.min(k - 1) as u64
    }
}

/// Build criterion one's estimated per-log-callsite counts, a heavy-tailed
/// distribution over `k` callsites keyed `0..k`.
fn build_estimates(k: usize, rng: &mut Lcg) -> HashMap<u64, f64> {
    let mut estimates = HashMap::with_capacity(k);
    for l in 0..k as u64 {
        let _ = estimates.insert(l, rng.heavy_tailed());
    }
    estimates
}

/// Prime a fresh accumulator to represent one window's span-logs sample:
/// establish the surprisal scale, observe in-span logs under each span kind,
/// and record heavy-tailed span-start volumes. Span-start callsites are keyed
/// `0..span_kinds`; log callsites are keyed `0..log_callsites`.
fn primed_accumulator(
    span_kinds: usize,
    estimates: &HashMap<u64, f64>,
    log_callsites: usize,
    rng: &mut Lcg,
) -> SpanStartValuesHarness {
    let mut acc = SpanStartValuesHarness::new();
    // Establish last window's surprisal scale so `observe_in_span` and the value
    // scores read real `g_l` values rather than the flat fallback.
    acc.update_log_surprisals(estimates);
    for s in 0..span_kinds as u64 {
        for _ in 0..LOGS_PER_KIND {
            acc.observe_in_span(s, rng.skewed_index(log_callsites));
        }
        acc.add_start(s, rng.heavy_tailed() * 100.0);
    }
    acc
}

fn bench_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("span_start_probabilities");

    for &(span_kinds, log_callsites, label) in SCALES {
        let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
        let estimates = build_estimates(log_callsites, &mut rng);
        let acc = primed_accumulator(span_kinds, &estimates, log_callsites, &mut rng);

        // Step 1 in isolation: recompute the surprisal scale from the sample,
        // O(k) in the number of log callsites.
        let _ = group.bench_with_input(
            BenchmarkId::new("update_log_surprisals", label),
            &estimates,
            |b, estimates| {
                let mut acc = SpanStartValuesHarness::new();
                b.iter(|| {
                    acc.update_log_surprisals(std::hint::black_box(estimates));
                });
            },
        );

        // Step 2 in isolation: derive one admission probability per local-root
        // kind, O(S*) in the number of span-start kinds, with value weighting on.
        let _ = group.bench_with_input(
            BenchmarkId::new("build_table_value_weighted", label),
            &acc,
            |b, acc| {
                b.iter(|| {
                    let _ = std::hint::black_box(acc.build_table(
                        TARGET_SPAN_STARTS,
                        true,
                        SHRINKAGE,
                        Threshold::ALWAYS,
                    ));
                });
            },
        );

        // Equal-coverage baseline: value weighting off, to isolate the cost the
        // surprisal-value refinement adds over the plain inverse-frequency rule.
        let _ = group.bench_with_input(
            BenchmarkId::new("build_table_equal_coverage", label),
            &acc,
            |b, acc| {
                b.iter(|| {
                    let _ = std::hint::black_box(acc.build_table(
                        TARGET_SPAN_STARTS,
                        false,
                        SHRINKAGE,
                        Threshold::ALWAYS,
                    ));
                });
            },
        );

        // The full per-window derivation as `IntegratedSampler::flush` runs it:
        // build the next table from this window's accumulators, then roll the
        // surprisal scale forward for the window after. The accumulator is
        // primed with span-start volumes so `build_table` does real work.
        let _ = group.bench_with_input(
            BenchmarkId::new("full_window_derivation", label),
            &(span_kinds, log_callsites),
            |b, &(span_kinds, log_callsites)| {
                let mut setup_rng = Lcg(0x9E37_79B9_7F4A_7C15);
                let estimates = build_estimates(log_callsites, &mut setup_rng);
                let mut acc =
                    primed_accumulator(span_kinds, &estimates, log_callsites, &mut setup_rng);
                b.iter(|| {
                    let _ = std::hint::black_box(acc.build_table(
                        TARGET_SPAN_STARTS,
                        true,
                        SHRINKAGE,
                        Threshold::ALWAYS,
                    ));
                    acc.update_log_surprisals(std::hint::black_box(&estimates));
                });
            },
        );
    }

    group.finish();
}

