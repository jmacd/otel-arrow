// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Stratified adaptive priority sampler.
//!
//! See [the module docs](super) for the design and statistical
//! background.  This file glues together [`CallsiteBucket`]s for
//! each observed stratum, performs the per-stratum cap rebalancing
//! when new strata appear, and exposes the
//! [`admit`](StratifiedSampler::admit) / [`insert`](StratifiedSampler::insert)
//! / [`flush_period`](StratifiedSampler::flush_period) hot path.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use rand::{RngExt, SeedableRng, rngs::SmallRng};

use super::bucket::CallsiteBucket;
use super::probability::PriorityKey;

/// Outcome of an admission attempt.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Admission {
    /// The event must be skipped: do **not** format the record.
    /// Returned when the per-stratum threshold rejected the drawn
    /// priority.
    Skip,
    /// The event was admitted.  The caller must build the payload
    /// and pass it back to [`StratifiedSampler::insert`] together
    /// with the same [`PriorityKey`] returned here.
    Admit {
        /// Priority drawn for this event.  Must be passed verbatim
        /// to [`StratifiedSampler::insert`].
        priority: PriorityKey,
    },
}

/// Configuration for [`StratifiedSampler`].
#[derive(Clone, Debug)]
pub struct SamplerConfig {
    /// How long a sampling period lasts before
    /// [`flush_period`](StratifiedSampler::flush_period) should be
    /// invoked.  Used only by [`period_elapsed`](StratifiedSampler::period_elapsed);
    /// the sampler does not drive its own timer.
    pub period: Duration,

    /// Hard upper bound on the total number of in-flight retained
    /// items across all strata at any moment.  The sampler enforces
    /// this only as a soft hint at construction time (see
    /// [`SamplerConfig::validate`]) — runtime allocation is governed
    /// by `target_count` and `min_per_stratum`.
    pub buffer_size: usize,

    /// Desired total sample size per period across all strata.  The
    /// per-stratum cap is `max(min_per_stratum, target_count /
    /// K_active)`.
    pub target_count: usize,

    /// Floor on the per-stratum cap `m_c`.  Must be ≥ 1.  Setting
    /// this above 1 prevents over-shrinking when many strata are
    /// active, at the cost of allowing total kept items to exceed
    /// `target_count` when `K_active > target_count / min_per_stratum`.
    pub min_per_stratum: usize,
}

impl SamplerConfig {
    /// Sanity-check the configuration.  Returns an error message if
    /// the values are inconsistent.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.min_per_stratum < 1 {
            return Err("min_per_stratum must be >= 1");
        }
        if self.target_count < self.min_per_stratum {
            return Err("target_count must be >= min_per_stratum");
        }
        if self.buffer_size < self.target_count {
            return Err("buffer_size must be >= target_count");
        }
        if self.period.is_zero() {
            return Err("period must be > 0");
        }
        Ok(())
    }
}

/// Per-stratum priority sampler with adaptive thresholds.
///
/// Generic over the stratum key type `K` (the *callsite identity* in
/// the intended use) and the payload type `T` (the formatted log
/// record).  Single-threaded: the type is `!Sync` by virtue of its
/// [`RefCell`] interior.
pub struct StratifiedSampler<K, T> {
    cfg: SamplerConfig,
    inner: RefCell<Inner<K, T>>,
}

struct Inner<K, T> {
    period_start: Instant,
    /// Current per-stratum cap `m_c`.  Recomputed whenever a new
    /// stratum appears; held constant otherwise within a period.
    per_stratum_cap: usize,
    buckets: HashMap<K, CallsiteBucket<T>>,
    rng: SmallRng,
}

impl<K, T> StratifiedSampler<K, T>
where
    K: Hash + Eq + Clone,
{
    /// Construct a new sampler.  Panics if [`SamplerConfig::validate`]
    /// fails — callers may call `validate` first to surface a
    /// recoverable error.
    #[must_use]
    pub fn new(cfg: SamplerConfig) -> Self {
        cfg.validate().expect("invalid SamplerConfig");
        Self::with_seed(cfg, rand::random())
    }

    /// Construct a sampler with a deterministic RNG seed.  Used by
    /// tests; production callers should prefer [`new`](Self::new).
    #[must_use]
    pub fn with_seed(cfg: SamplerConfig, seed: u64) -> Self {
        cfg.validate().expect("invalid SamplerConfig");
        // Until the first stratum appears the per-stratum cap is the
        // entire target; once the first event is admitted K_active
        // becomes 1 and the cap is reaffirmed (a no-op).
        let per_stratum_cap = cfg.target_count.max(cfg.min_per_stratum);
        Self {
            cfg,
            inner: RefCell::new(Inner {
                period_start: Instant::now(),
                per_stratum_cap,
                buckets: HashMap::new(),
                rng: SmallRng::seed_from_u64(seed),
            }),
        }
    }

    /// True when the elapsed wall-clock time since the last
    /// [`flush_period`](Self::flush_period) (or construction) exceeds
    /// the configured period.  Convenience for callers that want to
    /// piggyback flush checks on event arrivals rather than running
    /// a dedicated timer.
    pub fn period_elapsed(&self) -> bool {
        let inner = self.inner.borrow();
        inner.period_start.elapsed() >= self.cfg.period
    }

    /// Number of strata currently tracked in this period.
    pub fn active_strata(&self) -> usize {
        self.inner.borrow().buckets.len()
    }

    /// Current per-stratum cap `m_c`.
    pub fn current_cap(&self) -> usize {
        self.inner.borrow().per_stratum_cap
    }

    /// Hot path: try to admit an event for stratum `key`.
    ///
    /// On the first appearance of a previously-unseen `key` in the
    /// current period this method also rebalances all existing
    /// buckets — see the module docs.  All other admissions touch
    /// only the target bucket.
    ///
    /// On [`Admission::Admit`] the caller must build the payload and
    /// invoke [`insert`](Self::insert) with the returned
    /// [`PriorityKey`].  On [`Admission::Skip`] the caller must
    /// discard the event without formatting.
    pub fn admit(&self, key: &K) -> Admission {
        let mut inner = self.inner.borrow_mut();
        let priority = PriorityKey::new(inner.rng.random::<f64>());

        // First-appearance check.  We do not use the entry API here
        // because we need to know whether we just *added* a bucket
        // (so we can rebalance) without holding two mutable borrows.
        let is_new = !inner.buckets.contains_key(key);
        if is_new {
            // Adding a new stratum: K_active grows.  Recompute cap
            // and shrink existing buckets to the new (smaller) cap.
            let new_k = inner.buckets.len() + 1;
            let new_cap = self.compute_cap(new_k);
            if new_cap < inner.per_stratum_cap {
                for bucket in inner.buckets.values_mut() {
                    bucket.shrink_to(new_cap);
                }
            }
            inner.per_stratum_cap = new_cap;
            let _ = inner
                .buckets
                .insert(key.clone(), CallsiteBucket::new(new_cap));
        }

        let bucket = inner
            .buckets
            .get_mut(key)
            .expect("bucket exists after first-appearance check");
        bucket.record_attempt();
        if priority >= bucket.threshold() {
            Admission::Skip
        } else {
            Admission::Admit { priority }
        }
    }

    /// Insert a formatted payload that was previously approved by
    /// [`admit`](Self::admit).
    ///
    /// `key` and `priority` must match the values from the matching
    /// `admit` call.  It is a programmer error to call `insert`
    /// after an [`Admission::Skip`].
    pub fn insert(&self, key: &K, priority: PriorityKey, payload: T) {
        let mut inner = self.inner.borrow_mut();
        let bucket = inner
            .buckets
            .get_mut(key)
            .expect("insert requires admit to have created the bucket");
        // Note: we do not call `record_attempt` again — `admit`
        // already did it.  We also re-check the threshold, since the
        // arrival of *other* events between admit and insert could
        // have lowered it.  In single-threaded use this is a no-op
        // (admit and insert are interleaved on the same call stack
        // for a given event), but the check is cheap and defensive.
        if !bucket.try_admit(priority, payload) {
            // The threshold tightened underneath us; the payload
            // would have been rejected by a fresh admit.  Drop it
            // silently — this preserves unbiasedness because the
            // dropped item's priority lay above the new threshold.
        }
    }

    /// Drain the current period's sample.
    ///
    /// Returns each kept item paired with its stratum key and its
    /// priority-sampling weight `1 / max(u, τ)`.  Resets the period
    /// timer and clears all buckets but **retains** the bucket map
    /// shape; callers that want to forget unseen strata across
    /// periods should call [`reset_strata`](Self::reset_strata)
    /// after flushing.
    pub fn flush_period(&self) -> Vec<(K, T, f64)> {
        let mut inner = self.inner.borrow_mut();
        let mut out = Vec::new();
        // Drain each bucket; we keep the bucket struct (so cap is
        // preserved across periods if the same key returns) but its
        // contents are emptied.
        for (key, bucket) in inner.buckets.iter_mut() {
            for (payload, weight) in bucket.drain_with_weights() {
                out.push((key.clone(), payload, weight));
            }
        }
        inner.period_start = Instant::now();
        out
    }

    /// Forget all strata.  Subsequent admissions start over with
    /// `K_active = 0`.  Typically called once per period after
    /// [`flush_period`](Self::flush_period) when callers do not want
    /// the per-stratum cap to remain pinned at the prior period's
    /// (possibly small) value.
    pub fn reset_strata(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.buckets.clear();
        inner.per_stratum_cap = self.cfg.target_count.max(self.cfg.min_per_stratum);
    }

    fn compute_cap(&self, k_active: usize) -> usize {
        let raw = self.cfg.target_count / k_active.max(1);
        raw.max(self.cfg.min_per_stratum)
    }
}
