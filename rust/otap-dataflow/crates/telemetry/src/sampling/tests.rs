// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the sampling module.
//!
//! Three layers of tests:
//!
//! 1. **Unit tests** for [`PriorityKey`], [`priority_weight`], and
//!    [`CallsiteBucket`] cover deterministic invariants (ordering,
//!    saturation, shrink semantics).
//! 2. **Integration tests** for [`StratifiedSampler`] cover the
//!    admit / insert / flush flow and the K_active rebalancing on
//!    new-stratum first appearance.
//! 3. **Monte Carlo tests** verify the headline statistical claim:
//!    the sum of priority-sampling weights over a sampled period is
//!    an unbiased estimator of the true event count, both per
//!    stratum and globally.  These tests use a fixed RNG seed and
//!    average over many independent trials so they are
//!    deterministic and tight.

use std::time::Duration;

use super::bucket::CallsiteBucket;
use super::probability::{PriorityKey, priority_weight};
use super::sampler::{Admission, SamplerConfig, StratifiedSampler};

// -- probability primitives ---------------------------------------------------

#[test]
fn priority_key_orders_by_value() {
    let a = PriorityKey::new(0.10);
    let b = PriorityKey::new(0.20);
    assert!(a < b);
    assert!(b > a);
    assert_eq!(a, PriorityKey::new(0.10));
}

#[test]
fn priority_key_max_is_strict_upper_bound() {
    // Every valid u in [0, 1) must be strictly less than MAX.
    for &u in &[0.0_f64, 0.5, 0.999, 0.9999999, 0.99999999999] {
        assert!(PriorityKey::new(u) < PriorityKey::MAX, "u={u}");
    }
}

#[test]
fn priority_weight_is_one_over_threshold_for_kept_items() {
    // Kept items satisfy u < τ, so weight = 1/τ.
    let tau = PriorityKey::new(0.10);
    let u = PriorityKey::new(0.05);
    let w = priority_weight(u, tau);
    assert!((w - 10.0).abs() < 1e-12);
}

#[test]
fn priority_weight_collapses_when_unsaturated() {
    // When τ = MAX the bucket never saturated; each kept item should
    // be weighted ~1.0 (it represents only itself).
    let u = PriorityKey::new(0.42);
    let w = priority_weight(u, PriorityKey::MAX);
    assert!((w - 1.0).abs() < 1e-12);
}

// -- CallsiteBucket -----------------------------------------------------------

#[test]
fn bucket_admits_until_saturated_then_evicts_largest() {
    let mut b: CallsiteBucket<&'static str> = CallsiteBucket::new(2);
    // Fill cap + 1 = 3 items.
    assert!(b.try_admit(PriorityKey::new(0.5), "a"));
    assert!(b.try_admit(PriorityKey::new(0.1), "b"));
    assert!(b.try_admit(PriorityKey::new(0.9), "c"));
    assert!(b.is_saturated());
    // Threshold is the largest of the 3 = 0.9.
    assert_eq!(b.threshold(), PriorityKey::new(0.9));
    // A new item with u=0.95 must be rejected.
    assert!(!b.try_admit(PriorityKey::new(0.95), "d"));
    // A new item with u=0.05 displaces the root (0.9) and tightens τ.
    assert!(b.try_admit(PriorityKey::new(0.05), "e"));
    assert_eq!(b.threshold(), PriorityKey::new(0.5));
}

#[test]
fn bucket_unsaturated_threshold_is_max() {
    let mut b: CallsiteBucket<u32> = CallsiteBucket::new(4);
    let _ = b.try_admit(PriorityKey::new(0.3), 1);
    let _ = b.try_admit(PriorityKey::new(0.7), 2);
    // |heap| = 2 < cap + 1 = 5: not saturated.
    assert_eq!(b.threshold(), PriorityKey::MAX);
}

#[test]
fn bucket_drain_returns_kept_items_with_uniform_weight() {
    let mut b: CallsiteBucket<u32> = CallsiteBucket::new(2);
    // Push 4 items with priorities 0.1, 0.3, 0.6, 0.9.
    // After saturation (heap = cap+1 = 3) the root holds the largest
    // of {0.1, 0.3, 0.6, 0.9} = 0.9.  Pop on drain → τ = 0.9, kept =
    // {0.1, 0.3, 0.6}? No — only m = 2 items survive after the τ pop.
    //
    // Walk it through:
    //   admit 0.1 → heap = {0.1}, not saturated.
    //   admit 0.3 → heap = {0.1, 0.3}, not saturated.
    //   admit 0.6 → heap = {0.1, 0.3, 0.6}, saturated, root = 0.6.
    //                                          threshold = 0.6.
    //   admit 0.9 → 0.9 >= 0.6, rejected.
    //   admit 0.5 → 0.5 < 0.6, push → {0.1, 0.3, 0.5, 0.6}, then
    //               pop_overflow → {0.1, 0.3, 0.5}, root = 0.5.
    let _ = b.try_admit(PriorityKey::new(0.1), 1);
    let _ = b.try_admit(PriorityKey::new(0.3), 2);
    let _ = b.try_admit(PriorityKey::new(0.6), 3);
    assert!(!b.try_admit(PriorityKey::new(0.9), 4));
    let _ = b.try_admit(PriorityKey::new(0.5), 5);
    let drained = b.drain_with_weights();
    assert_eq!(drained.len(), 2);
    // After drain the τ popped is 0.5, kept = {0.1, 0.3}.  Both get
    // weight 1/0.5 = 2.0.
    for (_, w) in &drained {
        assert!((w - 2.0).abs() < 1e-12, "got weight {w}");
    }
}

#[test]
fn bucket_shrink_evicts_to_new_cap() {
    let mut b: CallsiteBucket<u32> = CallsiteBucket::new(5);
    for i in 0..6 {
        let u = (i as f64) / 10.0;
        let _ = b.try_admit(PriorityKey::new(u), i);
    }
    // |heap| = 6 = cap + 1.  Shrink to cap = 2: must keep heap ≤ 3.
    b.shrink_to(2);
    assert_eq!(b.cap(), 2);
    assert!(b.threshold() < PriorityKey::new(0.5));
}

// -- StratifiedSampler --------------------------------------------------------

fn cfg(target: usize, buffer: usize) -> SamplerConfig {
    SamplerConfig {
        period: Duration::from_secs(1),
        buffer_size: buffer,
        target_count: target,
        min_per_stratum: 1,
    }
}

#[test]
fn sampler_validate_rejects_bad_configs() {
    assert!(cfg(10, 5).validate().is_err()); // buffer < target
    let mut c = cfg(10, 20);
    c.min_per_stratum = 0;
    assert!(c.validate().is_err());
    c.min_per_stratum = 1;
    c.period = Duration::ZERO;
    assert!(c.validate().is_err());
}

#[test]
fn sampler_single_stratum_keeps_at_most_cap_items() {
    let s: StratifiedSampler<&'static str, u32> =
        StratifiedSampler::with_seed(cfg(8, 16), 0xDEADBEEF);
    for i in 0..1000 {
        if let Admission::Admit { priority } = s.admit(&"only") {
            s.insert(&"only", priority, i);
        }
    }
    let batch = s.flush_period();
    assert_eq!(batch.len(), 8, "expected exactly target_count items");
    // Single stratum: K_active = 1, cap = target = 8.
    assert_eq!(s.current_cap(), 8);
}

#[test]
fn sampler_three_strata_rebalance_to_equal_caps() {
    let s: StratifiedSampler<u32, u32> = StratifiedSampler::with_seed(cfg(9, 30), 0xCAFE);
    // Push events round-robin across 3 strata.
    for i in 0..600 {
        let key = i % 3;
        if let Admission::Admit { priority } = s.admit(&key) {
            s.insert(&key, priority, i);
        }
    }
    assert_eq!(s.active_strata(), 3);
    assert_eq!(s.current_cap(), 3); // 9 / 3 = 3 per stratum
    let batch = s.flush_period();
    // Each stratum should contribute exactly 3 (its full cap).
    let mut counts = std::collections::HashMap::<u32, usize>::new();
    for (k, _, _) in &batch {
        *counts.entry(*k).or_default() += 1;
    }
    for k in 0..3u32 {
        assert_eq!(counts.get(&k).copied().unwrap_or(0), 3, "stratum {k}");
    }
}

#[test]
fn sampler_skip_path_returns_skip_when_threshold_tight() {
    // Run a single stratum with target=2 long enough to saturate the
    // bucket, then verify that subsequent admits return Skip the
    // majority of the time.  After N admissions on one stratum the
    // saturation threshold is approximately (m+1)/N, so for m=2 and
    // N=200 the skip rate should be ≥ 90%.
    let s: StratifiedSampler<&'static str, u32> = StratifiedSampler::with_seed(cfg(2, 8), 1);
    // Warm up.
    for i in 0..200 {
        if let Admission::Admit { priority } = s.admit(&"k") {
            s.insert(&"k", priority, i);
        }
    }
    let mut skips = 0;
    let mut total = 0;
    for _ in 0..1000 {
        match s.admit(&"k") {
            Admission::Skip => skips += 1,
            Admission::Admit { priority } => s.insert(&"k", priority, 0),
        }
        total += 1;
    }
    let skip_rate = skips as f64 / total as f64;
    assert!(
        skip_rate > 0.9,
        "expected high skip rate after warmup, got {skip_rate}"
    );
}

#[test]
fn sampler_flush_resets_period_timer() {
    let mut c = cfg(4, 8);
    c.period = Duration::from_millis(50);
    let s: StratifiedSampler<u32, u32> = StratifiedSampler::with_seed(c, 7);
    std::thread::sleep(Duration::from_millis(60));
    assert!(s.period_elapsed());
    let _ = s.flush_period();
    assert!(!s.period_elapsed());
}

#[test]
fn sampler_reset_strata_clears_buckets_and_cap() {
    let s: StratifiedSampler<u32, u32> = StratifiedSampler::with_seed(cfg(10, 30), 9);
    for k in 0..5u32 {
        if let Admission::Admit { priority } = s.admit(&k) {
            s.insert(&k, priority, 0);
        }
    }
    assert_eq!(s.active_strata(), 5);
    assert_eq!(s.current_cap(), 2); // 10/5
    s.reset_strata();
    assert_eq!(s.active_strata(), 0);
    assert_eq!(s.current_cap(), 10); // back to target
}

// -- Monte Carlo unbiasedness -------------------------------------------------

/// Sum of weights in a sampled period is an unbiased estimator of
/// the true number of arrivals.  We verify this empirically by
/// running many independent trials with different RNG seeds and
/// checking that the sample mean is close to the true count.
#[test]
fn weighted_count_is_unbiased_single_stratum() {
    const TRIALS: usize = 400;
    const ARRIVALS: usize = 500;
    const TARGET: usize = 10;
    let mut sum = 0.0;
    for trial in 0..TRIALS {
        let s: StratifiedSampler<&'static str, ()> =
            StratifiedSampler::with_seed(cfg(TARGET, TARGET * 4), trial as u64 + 1);
        for _ in 0..ARRIVALS {
            if let Admission::Admit { priority } = s.admit(&"k") {
                s.insert(&"k", priority, ());
            }
        }
        let batch = s.flush_period();
        let est: f64 = batch.iter().map(|(_, _, w)| *w).sum();
        sum += est;
    }
    let mean = sum / TRIALS as f64;
    let truth = ARRIVALS as f64;
    let rel_err = (mean - truth).abs() / truth;
    // With m=10 items per trial the per-trial estimator has CV ≈
    // 1/sqrt(m) ≈ 0.32, so the SE of the mean over 400 trials is
    // about 0.32 * 500 / sqrt(400) ≈ 8.  Allow 5% relative error
    // (i.e., ±25) to keep the test stable.
    assert!(
        rel_err < 0.05,
        "mean estimate {mean} vs truth {truth} (rel_err={rel_err})"
    );
}

/// Per-stratum unbiasedness: with two strata generating different
/// numbers of events, the per-stratum weighted count tracks the true
/// per-stratum arrival count.
#[test]
fn weighted_count_is_unbiased_per_stratum() {
    const TRIALS: usize = 400;
    let arrivals = [800usize, 200];
    const TARGET_PER_STRATUM: usize = 8; // we'll set target = 16, K=2 → m=8
    let mut sums = [0.0_f64; 2];
    for trial in 0..TRIALS {
        let s: StratifiedSampler<u32, ()> =
            StratifiedSampler::with_seed(cfg(TARGET_PER_STRATUM * 2, 64), trial as u64 + 100);
        for (k, &n) in arrivals.iter().enumerate() {
            for _ in 0..n {
                let key = k as u32;
                if let Admission::Admit { priority } = s.admit(&key) {
                    s.insert(&key, priority, ());
                }
            }
        }
        let batch = s.flush_period();
        for (key, _, w) in batch {
            sums[key as usize] += w;
        }
    }
    for (k, &truth) in arrivals.iter().enumerate() {
        let mean = sums[k] / TRIALS as f64;
        let rel_err = (mean - truth as f64).abs() / truth as f64;
        assert!(
            rel_err < 0.07,
            "stratum {k}: mean {mean} vs truth {truth} (rel_err={rel_err})"
        );
    }
}
