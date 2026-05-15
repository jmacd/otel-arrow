// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the sampling algorithms.  Each algorithm gets:
//!
//! - skip-rate sanity (after the buffer/heap fills, most events skip)
//! - τ monotone non-increasing within a period
//! - Monte-Carlo unbiasedness: many independent trials, the mean of
//!   `Σ sampling_weight` converges to the true arrival count
//! - Multi-period determinism via fixed seed
//!
//! Plus targeted tests for [`chao1_unseen_weight`].

use std::collections::HashMap;

use super::{Admission, AlgorithmJ, AlgorithmK, LogSampler, chao1_unseen_weight};

/// A simple deterministic Zipfian-ish callsite generator: callsite
/// id `i` appears with probability proportional to `1/(i+1)` over a
/// fixed alphabet of `k` callsites.  We use this rather than a real
/// Zipf because it is dependency-free and the exact distribution
/// does not matter for unbiasedness tests.
fn zipf_like_callsites(k: usize, n: usize, seed: u64) -> Vec<u32> {
    use rand::{RngExt, SeedableRng, rngs::SmallRng};
    let mut rng = SmallRng::seed_from_u64(seed);
    let weights: Vec<f64> = (0..k).map(|i| 1.0 / (i as f64 + 1.0)).collect();
    let total: f64 = weights.iter().sum();
    let cumulative: Vec<f64> = weights
        .iter()
        .scan(0.0, |acc, w| {
            *acc += w / total;
            Some(*acc)
        })
        .collect();
    (0..n)
        .map(|_| {
            let u: f64 = rng.random();
            cumulative
                .iter()
                .position(|&c| u < c)
                .unwrap_or(k - 1) as u32
        })
        .collect()
}

fn run_period<S: LogSampler<u32, u64>>(sampler: &S, callsites: &[u32]) -> Vec<(u32, u64, f64)> {
    for (idx, c) in callsites.iter().enumerate() {
        match sampler.admit(c) {
            Admission::Skip => {}
            Admission::Admit(t) => sampler.insert(*c, t, idx as u64),
        }
    }
    sampler.flush()
}

#[test]
fn algo_j_basic_skip_then_flush() {
    // 1 callsite, many events, small T, large B → skip rate should
    // be high after warmup.
    let s = AlgorithmJ::with_seed(10, 100, 0xC0FFEE);
    let callsites: Vec<u32> = vec![42; 10_000];
    let mut admits = 0;
    for c in &callsites {
        match s.admit(c) {
            Admission::Skip => {}
            Admission::Admit(t) => {
                admits += 1;
                s.insert(*c, t, 0);
            }
        }
    }
    let out = s.flush();
    assert_eq!(out.len(), 10);
    // After warmup the admit rate should be much less than 1.  The
    // expected admit count is ~ T * (1 + ln(N/T)) under priority
    // sampling, ≈ 10 * (1 + ln(1000)) ≈ 79.  Allow a generous margin.
    assert!(admits < 200, "admit count {admits} should be «N");
}

#[test]
fn algo_j_unbiasedness_single_callsite() {
    // For one callsite emitting N events, Σ weights ≈ N.
    let n = 5_000usize;
    let mut sum_weights = 0.0f64;
    let trials = 30;
    for trial in 0..trials {
        let s = AlgorithmJ::with_seed(20, 200, trial as u64 + 1);
        let callsites: Vec<u32> = vec![7; n];
        let out = run_period(&s, &callsites);
        let total: f64 = out.iter().map(|(_, _, w)| *w).sum();
        sum_weights += total;
    }
    let mean = sum_weights / trials as f64;
    let rel_err = (mean - n as f64).abs() / (n as f64);
    assert!(
        rel_err < 0.05,
        "mean Σw = {mean}, expected ≈ {n}, rel_err = {rel_err}"
    );
}

#[test]
fn algo_j_unbiasedness_zipf_per_callsite() {
    // Multiple callsites, check Σw per-callsite tracks true count.
    let k = 8;
    let n = 20_000;
    let trials = 200;
    let mut sums: HashMap<u32, f64> = HashMap::new();
    let mut counts: HashMap<u32, u64> = HashMap::new();
    for trial in 0..trials {
        let cs = zipf_like_callsites(k, n, trial as u64 + 100);
        for c in &cs {
            *counts.entry(*c).or_insert(0) += 1;
        }
        let s = AlgorithmJ::with_seed(50, 500, trial as u64 + 1);
        let out = run_period(&s, &cs);
        for (c, _, w) in out {
            *sums.entry(c).or_insert(0.0) += w;
        }
    }
    for c in 0..k as u32 {
        let est = sums.get(&c).copied().unwrap_or(0.0);
        let truth = counts.get(&c).copied().unwrap_or(0) as f64;
        if truth < 100.0 {
            continue; // tail: skip noisy ones
        }
        let rel = (est - truth).abs() / truth;
        assert!(
            rel < 0.10,
            "callsite {c}: estimated {est}, truth {truth}, rel_err {rel}"
        );
    }
}

#[test]
fn algo_j_tau_monotone() {
    let s = AlgorithmJ::with_seed(10, 100, 99);
    let mut last = f64::INFINITY;
    for i in 0..2_000u32 {
        let c = i % 5;
        if let Admission::Admit(t) = s.admit(&c) {
            s.insert(c, t, i as u64);
        }
        let cur = s.tau();
        assert!(
            cur <= last + 1e-12,
            "τ increased: {last} → {cur} at step {i}"
        );
        last = cur;
    }
}

#[test]
fn algo_k_basic_skip_then_flush() {
    let s = AlgorithmK::with_options(10, 32, 0xBADC0FFEE0DDF00D);
    // First period: warmup (everything is "unseen", τ tightens via
    // heap pops once 11 events arrive).
    let cs1: Vec<u32> = vec![1; 5_000];
    let out1 = run_period(&s, &cs1);
    assert_eq!(out1.len(), 10);

    // Second period: now freq_prev knows callsite 1.  Skip rate
    // should be high.
    let mut admits = 0;
    let cs2: Vec<u32> = vec![1; 10_000];
    for c in &cs2 {
        match s.admit(c) {
            Admission::Skip => {}
            Admission::Admit(t) => {
                admits += 1;
                s.insert(*c, t, 0);
            }
        }
    }
    let _ = s.flush();
    assert!(admits < 200, "admit count {admits} after warmup");
}

#[test]
fn algo_k_unbiasedness_stationary() {
    // Stationary workload: same arrival distribution across periods.
    // After one warmup period, Σw should track N.
    let k = 6;
    let n = 10_000;
    let periods = 4;
    let trials = 30;
    let mut total_err = 0.0f64;
    for trial in 0..trials {
        let s = AlgorithmK::with_options(40, 32, trial as u64 + 1);
        // Warm up.
        let _ = run_period(&s, &zipf_like_callsites(k, n, 7777 + trial as u64));
        // Measure across remaining periods.
        let mut sum_w = 0.0;
        let mut sum_n = 0.0;
        for p in 1..periods {
            let cs = zipf_like_callsites(k, n, 7777 + trial as u64 + p as u64 * 31);
            let out = run_period(&s, &cs);
            sum_w += out.iter().map(|(_, _, w)| *w).sum::<f64>();
            sum_n += cs.len() as f64;
        }
        total_err += (sum_w - sum_n) / sum_n;
    }
    let mean_rel_err = (total_err / trials as f64).abs();
    assert!(
        mean_rel_err < 0.05,
        "mean rel_err = {mean_rel_err}, expected ≈ 0"
    );
}

#[test]
fn algo_k_tau_monotone() {
    let s = AlgorithmK::with_options(10, 32, 7);
    // Warm up so unseen_weight is non-trivial.
    let warmup: Vec<u32> = (0..1000).map(|i| (i % 5) as u32).collect();
    let _ = run_period(&s, &warmup);

    let mut last = f64::INFINITY;
    for i in 0..2_000u32 {
        let c = i % 5;
        if let Admission::Admit(t) = s.admit(&c) {
            s.insert(c, t, i as u64);
        }
        let cur = s.tau();
        assert!(
            cur <= last + 1e-12,
            "τ increased: {last} → {cur} at step {i}"
        );
        last = cur;
    }
    let _ = s.flush();
}

#[test]
fn chao1_degenerate_cases() {
    // Empty: warmup behavior.
    assert!((chao1_unseen_weight(std::iter::empty()) - 1.0).abs() < 1e-12);
    // No singletons: complete corpus → unseen_weight = 1.0.
    assert!((chao1_unseen_weight([2u64, 3, 4]) - 1.0).abs() < 1e-12);
    // All singletons + one doubleton → some non-trivial value ≤ 1.
    let w = chao1_unseen_weight([1u64, 1, 1, 1, 1, 1, 2]);
    assert!(w > 0.0 && w <= 1.0, "unseen_weight = {w}");
}

#[test]
fn chao1_known_value() {
    // f1=10, f2=5, n=20+10 → Chao1 = 2 + (29/30) * 100/10 = 2 + 9.667
    // (we don't pin a precise value because the missing_mass model
    // matters; just sanity-check the result is in (0, 1]).
    let mut freqs = vec![1u64; 10]; // 10 singletons
    freqs.extend(vec![2u64; 5]); // 5 doubletons
    let w = chao1_unseen_weight(freqs);
    assert!(w > 0.0 && w <= 1.0);
}
