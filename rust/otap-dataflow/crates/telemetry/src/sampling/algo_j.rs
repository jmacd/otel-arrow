// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Algorithm J (R1 frozen-key variant): inverse-frequency priority
//! sampling with a buffer + bulk sample-down.
//!
//! See `~/.copilot/session-state/<id>/files/algorithm-J.md` for the
//! design narrative and history.  In one paragraph:
//!
//! For each event from callsite `c`, increment `freq[c]`, draw
//! `u ∈ [0, 1)`, compute the **frozen** key `k = u · freq[c]` (the
//! freq value is captured at admission and never recomputed).  Skip
//! iff `k ≥ τ`; otherwise format the payload and push it into a
//! buffer of capacity `B` along with `(k, f_at_admit)`.  When the
//! buffer fills, run a bulk sample-down: sort by frozen `k`, keep
//! the `T+1` items with the smallest keys, and tighten `τ` to the
//! `(T+1)`-th smallest key.  At flush, do one final bulk, drop the
//! pivot, and emit each kept item with HT weight
//! `max(1, f_at_admit/τ_final)` (raw-count estimator).
//!
//! ## Why R1 (frozen) keys instead of R2 (live)
//!
//! An earlier design recomputed `k_i = u_i · f_c_now` at every
//! sample-down.  That made `τ` non-monotone in the arrival order
//! (every callsite's keys grow as more events arrive), so the skip
//! test `u · f_now ≥ τ_now` was no longer conservative w.r.t. the
//! final R2 sample.  Empirically that produced a +10% positive bias
//! in `Σw`.  Freezing the key at admission restores monotone `τ`
//! and conservative skipping at the cost of a slightly higher
//! per-callsite admit count: instead of `min(N_c, τ)` admits per
//! callsite, R1 admits roughly `τ · (1 + ln(N_c / τ))`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::Hash;

use rand::{RngExt, SeedableRng, rngs::SmallRng};

use super::{Admission, LogSampler};

/// One buffered admitted item awaiting sample-down or flush.
struct Item<C, P> {
    /// Frozen priority key `u · f_at_admit`.
    key: f64,
    /// Frequency of `callsite` at the moment of admission.  Used to
    /// compute the HT weight at flush.
    f_at_admit: u64,
    callsite: C,
    payload: P,
}

/// Algorithm J sampler.  See module docs.
pub struct AlgorithmJ<C, P> {
    target: usize,
    buffer_cap: usize,
    inner: RefCell<Inner<C, P>>,
    tau: Cell<f64>,
}

struct Inner<C, P> {
    freq: HashMap<C, u64>,
    buffer: Vec<Item<C, P>>,
    rng: SmallRng,
}

impl<C, P> AlgorithmJ<C, P>
where
    C: Hash + Eq + Clone,
{
    /// Construct a sampler with target sample size `T` and buffer
    /// capacity `B`.  Panics if `B <= T` or `T == 0`.
    #[must_use]
    pub fn new(target: usize, buffer_cap: usize) -> Self {
        Self::with_seed(target, buffer_cap, rand::random())
    }

    /// Construct a sampler with a deterministic RNG seed.  Used by
    /// tests and benchmarks for reproducibility.
    #[must_use]
    pub fn with_seed(target: usize, buffer_cap: usize, seed: u64) -> Self {
        assert!(target >= 1, "target must be >= 1");
        assert!(
            buffer_cap > target,
            "buffer_cap ({buffer_cap}) must be > target ({target})"
        );
        Self {
            target,
            buffer_cap,
            tau: Cell::new(f64::INFINITY),
            inner: RefCell::new(Inner {
                freq: HashMap::new(),
                buffer: Vec::with_capacity(buffer_cap),
                rng: SmallRng::seed_from_u64(seed),
            }),
        }
    }

    /// Current threshold τ (non-increasing within a period; resets
    /// to `+∞` at flush).  Exposed for tests / benchmarks.
    #[must_use]
    pub fn tau(&self) -> f64 {
        self.tau.get()
    }

    /// Number of items currently in the buffer.  Exposed for tests.
    #[must_use]
    pub fn buffer_len(&self) -> usize {
        self.inner.borrow().buffer.len()
    }

    /// Number of distinct callsites observed in the current period.
    #[must_use]
    pub fn distinct_callsites(&self) -> usize {
        self.inner.borrow().freq.len()
    }

    /// Run a bulk sample-down: keep the `target + 1` items with the
    /// smallest frozen keys (the extra slot retains the threshold-
    /// determining item so τ stays correct across subsequent bulks),
    /// tighten `τ`.  Idempotent on a buffer holding `≤ target + 1`
    /// items.
    fn bulk_sample_down(&self) {
        let mut inner = self.inner.borrow_mut();
        let keep = self.target + 1;
        if inner.buffer.len() <= keep {
            return;
        }
        let nth = self.target;
        let _ = inner
            .buffer
            .select_nth_unstable_by(nth, |a, b| a.key.partial_cmp(&b.key).expect("finite keys"));
        let pivot_key = inner.buffer[nth].key;
        let cur = self.tau.get();
        if pivot_key < cur {
            self.tau.set(pivot_key);
        }
        // Keep `target + 1` items: the T below-pivot plus the pivot
        // itself, which holds the threshold key for the next bulk.
        inner.buffer.truncate(keep);
    }
}

impl<C, P> LogSampler<C, P> for AlgorithmJ<C, P>
where
    C: Hash + Eq + Clone,
{
    type Ticket = (f64, u64);

    fn admit(&self, callsite: &C) -> Admission<Self::Ticket> {
        let mut inner = self.inner.borrow_mut();
        let f = {
            let entry = inner.freq.entry(callsite.clone()).or_insert(0);
            *entry += 1;
            *entry
        };
        let u: f64 = inner.rng.random();
        let key = u * (f as f64);
        // Conservative skip: τ is non-increasing across the period
        // and per-item key is frozen, so `key ≥ τ_now ≥ τ_final`.
        if key >= self.tau.get() {
            Admission::Skip
        } else {
            Admission::Admit((key, f))
        }
    }

    fn insert(&self, callsite: C, ticket: Self::Ticket, payload: P) {
        let (key, f_at_admit) = ticket;
        let need_bulk = {
            let mut inner = self.inner.borrow_mut();
            inner.buffer.push(Item {
                key,
                f_at_admit,
                callsite,
                payload,
            });
            inner.buffer.len() >= self.buffer_cap
        };
        if need_bulk {
            self.bulk_sample_down();
        }
    }

    fn flush_into(&self, out: &mut Vec<(C, P, f64)>) {
        // Final bulk: leaves at most `target + 1` items with τ
        // pinned to the pivot's frozen key.
        self.bulk_sample_down();
        let final_tau = self.tau.get();
        let mut inner = self.inner.borrow_mut();
        // After bulk_sample_down, buffer.len() is either ≤ target
        // (warmup case, buffer never saturated) or exactly target+1
        // (saturated, pivot at index `target`).  Drop the pivot.
        if inner.buffer.len() > self.target {
            let _ = inner.buffer.swap_remove(self.target);
        }
        out.reserve(inner.buffer.len());
        for it in inner.buffer.drain(..) {
            let weight = if final_tau.is_finite() {
                (it.f_at_admit as f64 / final_tau).max(1.0)
            } else {
                1.0
            };
            out.push((it.callsite, it.payload, weight));
        }
        inner.freq.clear();
        drop(inner);
        self.tau.set(f64::INFINITY);
    }
}
