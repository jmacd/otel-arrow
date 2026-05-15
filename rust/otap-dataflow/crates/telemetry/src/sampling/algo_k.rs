// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Algorithm K: lagged-frequency weighted priority sampling with
//! Chao1/Good-Turing imputation for unseen callsites.
//!
//! See `~/.copilot/session-state/<id>/files/algorithm-K.md` for the
//! full design.  In one paragraph:
//!
//! Each event from callsite `c` looks up a *frozen* weight
//! `w_c = 1/freq_prev[c]` (or `unseen_weight` if `c` was not in the
//! prior period), draws `u ∈ [0, 1)`, and computes the priority
//! sampling key `k = u/w_c = u · freq_prev[c]`.  A bottom-`T` heap
//! holds the smallest keys.  The threshold τ is the heap root.
//! Events with `k ≥ τ` are skipped.  At flush, each kept item is
//! emitted with HT weight `max(1, 1/(τ_final · w_c))`, then the
//! current-period frequency table is frozen as the next period's
//! reference (with a freshly computed `unseen_weight`).

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;

use rand::{RngExt, SeedableRng, rngs::SmallRng};

use super::{Admission, LogSampler};

/// One heap entry — `key` is the priority-sampling key `u · freq_prev[c]`
/// (or `u / unseen_weight`).  A `BinaryHeap` of these is a max-heap,
/// so the root is τ.
struct HeapEntry<C, P> {
    key: f64,
    callsite: C,
    payload: P,
}

impl<C, P> PartialEq for HeapEntry<C, P> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl<C, P> Eq for HeapEntry<C, P> {}
impl<C, P> PartialOrd for HeapEntry<C, P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<C, P> Ord for HeapEntry<C, P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .partial_cmp(&other.key)
            .expect("finite priority-sampling keys")
    }
}

/// Algorithm K sampler.  See module docs.
pub struct AlgorithmK<C, P> {
    target: usize,
    /// Minimum prior-period arrival count below which we discard the
    /// frozen table and treat the next period as warmup.
    min_period_count: u64,
    inner: RefCell<Inner<C, P>>,
    tau: Cell<f64>,
    unseen_weight: Cell<f64>,
}

struct Inner<C, P> {
    /// Frozen table from the previous period.
    freq_prev: HashMap<C, u64>,
    /// Accumulator for the next period's frozen table.
    freq_curr: HashMap<C, u64>,
    /// Bottom-`T+1` max-heap on key.
    heap: BinaryHeap<HeapEntry<C, P>>,
    rng: SmallRng,
}

impl<C, P> AlgorithmK<C, P>
where
    C: Hash + Eq + Clone,
{
    /// Construct a sampler with target sample size `T`.  The
    /// `min_period_count` knob (default 32 if you use [`Self::new`])
    /// guards Chao1 stability — see [`Self::with_options`].  Panics
    /// if `T == 0`.
    #[must_use]
    pub fn new(target: usize) -> Self {
        Self::with_options(target, 32, rand::random())
    }

    /// Construct a sampler with explicit `min_period_count` and RNG
    /// seed.
    #[must_use]
    pub fn with_options(target: usize, min_period_count: u64, seed: u64) -> Self {
        assert!(target >= 1, "target must be >= 1");
        Self {
            target,
            min_period_count,
            tau: Cell::new(f64::INFINITY),
            // Until the first flush, every callsite is "unseen" with
            // weight 1.0 — i.e., we behave as a uniform reservoir.
            unseen_weight: Cell::new(1.0),
            inner: RefCell::new(Inner {
                freq_prev: HashMap::new(),
                freq_curr: HashMap::new(),
                heap: BinaryHeap::with_capacity(target + 1),
                rng: SmallRng::seed_from_u64(seed),
            }),
        }
    }

    /// Current threshold τ (decreasing within a period; resets at
    /// flush).  Exposed for tests.
    #[must_use]
    pub fn tau(&self) -> f64 {
        self.tau.get()
    }

    /// Current `unseen_weight` (frozen at last flush).  Exposed for
    /// tests.
    #[must_use]
    pub fn unseen_weight(&self) -> f64 {
        self.unseen_weight.get()
    }

    /// Number of callsites in the prior-period frozen table.
    /// Exposed for tests.
    #[must_use]
    pub fn frozen_callsites(&self) -> usize {
        self.inner.borrow().freq_prev.len()
    }

    fn weight_for(freq_prev: &HashMap<C, u64>, callsite: &C, unseen_weight: f64) -> f64 {
        match freq_prev.get(callsite) {
            Some(f) => 1.0 / (*f as f64),
            None => unseen_weight,
        }
    }
}

impl<C, P> LogSampler<C, P> for AlgorithmK<C, P>
where
    C: Hash + Eq + Clone,
{
    /// The priority key `k = u/w_c` derived at admit time.
    type Ticket = f64;

    fn admit(&self, callsite: &C) -> Admission<Self::Ticket> {
        let mut inner = self.inner.borrow_mut();
        // Always count for the next period's frozen table.
        let entry = inner.freq_curr.entry(callsite.clone()).or_insert(0);
        *entry += 1;

        let w_c = Self::weight_for(&inner.freq_prev, callsite, self.unseen_weight.get());
        let u: f64 = inner.rng.random();
        let k = u / w_c;
        if k >= self.tau.get() {
            Admission::Skip
        } else {
            Admission::Admit(k)
        }
    }

    fn insert(&self, callsite: C, ticket: Self::Ticket, payload: P) {
        let mut inner = self.inner.borrow_mut();
        inner.heap.push(HeapEntry {
            key: ticket,
            callsite,
            payload,
        });
        if inner.heap.len() > self.target {
            let _ = inner.heap.pop();
            // The root after pop is the new τ — the (T+1)-th smallest
            // is gone, the new root is the largest of the remaining
            // T smallest, which equals the new threshold.
            let new_tau = inner.heap.peek().expect("heap has >= T items").key;
            self.tau.set(new_tau);
        }
    }

    fn flush_into(&self, out: &mut Vec<(C, P, f64)>) {
        let final_tau = self.tau.get();
        let mut inner = self.inner.borrow_mut();
        let unseen = self.unseen_weight.get();
        out.reserve(inner.heap.len());
        // Destructure to split the borrow: we drain `heap` while
        // immutably reading `freq_prev` inside the loop.
        let Inner {
            freq_prev, heap, ..
        } = &mut *inner;
        for e in heap.drain() {
            let w_c = Self::weight_for(freq_prev, &e.callsite, unseen);
            let weight = if final_tau.is_finite() {
                (1.0 / (final_tau * w_c)).max(1.0)
            } else {
                1.0
            };
            out.push((e.callsite, e.payload, weight));
        }

        // Freeze current-period stats as next period's reference.
        let n_curr: u64 = inner.freq_curr.values().sum();
        if n_curr < self.min_period_count {
            inner.freq_prev.clear();
            self.unseen_weight.set(1.0);
        } else {
            self.unseen_weight
                .set(chao1_unseen_weight(inner.freq_curr.values().copied()));
            let Inner {
                freq_prev,
                freq_curr,
                ..
            } = &mut *inner;
            std::mem::swap(freq_prev, freq_curr);
        }
        inner.freq_curr.clear();

        self.tau.set(f64::INFINITY);
    }
}

/// Compute the Good-Turing-derived `unseen_weight` from a sample of
/// per-callsite frequencies.
///
/// The returned value is `1.0 / f_unseen` where `f_unseen` is the
/// expected per-unseen-callsite frequency in a period of size `n`.
/// Falls back to `1.0` (treat unseen as singletons) in degenerate
/// cases — `n == 0`, `f1 == 0`, no unseen mass, or non-positive
/// `S_unseen` from Chao1.
///
/// Mirrors the math in
/// <https://github.com/jmacd/essay/blob/87f081e0fe03998caf66cd950a9d76f824e6d50d/examples/internal/datashape/chao1.go>.
#[must_use]
pub fn chao1_unseen_weight<I>(freqs: I) -> f64
where
    I: IntoIterator<Item = u64>,
{
    let mut n: u64 = 0;
    let mut s_seen: u64 = 0;
    let mut f1: u64 = 0;
    let mut f2: u64 = 0;
    for f in freqs {
        if f == 0 {
            continue;
        }
        n = n.saturating_add(f);
        s_seen += 1;
        if f == 1 {
            f1 += 1;
        } else if f == 2 {
            f2 += 1;
        }
    }
    if n == 0 || f1 == 0 {
        return 1.0;
    }
    let n_f = n as f64;
    let f1_f = f1 as f64;
    let f2_f = f2 as f64;
    let s_seen_f = s_seen as f64;

    // Chao1 richness (lower bound on total species).
    let chao1 = if f2 > 0 {
        s_seen_f + ((n_f - 1.0) / n_f) * (f1_f * f1_f) / (2.0 * f2_f)
    } else {
        s_seen_f + ((n_f - 1.0) / n_f) * (f1_f * (f1_f - 1.0)) / 2.0
    };
    let s_unseen = chao1 - s_seen_f;
    if s_unseen <= 0.0 {
        return 1.0;
    }
    // Good-Turing missing-mass estimate.  Chao1.go uses
    //   missing_mass = f1 / n
    // (the simple Good-Turing form).  See chao1.go for derivation.
    let missing_mass = f1_f / n_f;
    if missing_mass <= 0.0 {
        return 1.0;
    }
    let p_unseen_per_species = missing_mass / s_unseen;
    let f_unseen = n_f * p_unseen_per_species;
    if !f_unseen.is_finite() || f_unseen <= 0.0 {
        return 1.0;
    }
    // Cap at 1.0: an unseen callsite should never be weighted as if
    // it had been seen *more* than once on average.  In practice
    // f_unseen < 1 is the regime where new callsites trickle in.
    (1.0 / f_unseen).min(1.0)
}
