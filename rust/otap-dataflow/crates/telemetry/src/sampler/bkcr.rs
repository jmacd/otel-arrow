// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! BKCR — Bottom-K Chao-Reserve sampler.
//!
//! See [`crate::sampler::design`](../design.md) for the algorithm
//! specification, the three foundational references (Duffield/Lund/
//! Thorup priority sampling, Cohen/Kaplan bottom-k sketches, Chao1
//! richness estimator), the properties proof sketch, and the
//! experimental results.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::Hash;

use hashbrown::{HashMap, HashSet};
use rand::{RngExt, SeedableRng, rngs::SmallRng};

use super::{Admission, LogSampler};

/// One heap entry.  `key` is the EXP rank `-ln(u) / w_c`.  A
/// `BinaryHeap` of these is a max-heap, so its root is the current
/// threshold τ.
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
            .expect("finite EXP-rank keys")
    }
}

/// The BKCR sampler.
///
/// Single-threaded by design.  All hot-path methods take `&self`
/// with interior mutability via `RefCell` / `Cell`; the type is
/// intentionally `!Sync`.
pub struct Bkcr<C, P> {
    target: usize,
    reserve_capacity: usize,
    /// Minimum prior-period arrival count below which we discard
    /// the frozen table and treat the next period as warmup.
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
    /// Novelty reserve: first-rejected callsites this period, FIFO.
    reserve: Vec<(C, P)>,
    /// Membership index for the reserve.  Same size as `reserve`.
    reserved_set: HashSet<C>,
    rng: SmallRng,
}

impl<C, P> Bkcr<C, P>
where
    C: Hash + Eq + Clone,
{
    /// Construct a sampler with target sample size `T`.  Uses the
    /// default reserve capacity (`R = T`), Chao1 stability threshold
    /// (`min_period_count = 32`), and a fresh OS-derived seed.
    /// Panics if `T == 0`.
    #[must_use]
    pub fn new(target: usize) -> Self {
        Self::with_options(target, target, 32, rand::random())
    }

    /// Construct a sampler with explicit `target`,
    /// `reserve_capacity`, `min_period_count`, and RNG seed.
    /// Panics if `target == 0`.
    #[must_use]
    pub fn with_options(
        target: usize,
        reserve_capacity: usize,
        min_period_count: u64,
        seed: u64,
    ) -> Self {
        assert!(target >= 1, "target must be >= 1");
        Self {
            target,
            reserve_capacity,
            min_period_count,
            tau: Cell::new(f64::INFINITY),
            // Until the first flush, every callsite is "unseen" with
            // weight 1.0 — i.e., we behave as a uniform reservoir.
            unseen_weight: Cell::new(1.0),
            inner: RefCell::new(Inner {
                freq_prev: HashMap::with_capacity(target),
                freq_curr: HashMap::with_capacity(target),
                heap: BinaryHeap::with_capacity(target + 1),
                reserve: Vec::with_capacity(reserve_capacity),
                reserved_set: HashSet::with_capacity(reserve_capacity),
                rng: SmallRng::seed_from_u64(seed),
            }),
        }
    }

    /// Current threshold τ (decreases monotonically within a
    /// period; resets at flush).  Exposed for tests and metrics.
    #[must_use]
    pub fn tau(&self) -> f64 {
        self.tau.get()
    }

    /// Current `unseen_weight` (frozen at last flush).  Exposed for
    /// tests and metrics.
    #[must_use]
    pub fn unseen_weight(&self) -> f64 {
        self.unseen_weight.get()
    }

    /// Number of callsites in the prior-period frozen table.
    #[must_use]
    pub fn frozen_callsites(&self) -> usize {
        self.inner.borrow().freq_prev.len()
    }

    /// Current reserve occupancy.
    #[must_use]
    pub fn reserve_len(&self) -> usize {
        self.inner.borrow().reserve.len()
    }

    fn weight_for(freq_prev: &HashMap<C, u64>, callsite: &C, unseen_weight: f64) -> f64 {
        match freq_prev.get(callsite) {
            Some(f) => 1.0 / (*f as f64),
            None => unseen_weight,
        }
    }
}

impl<C, P> LogSampler<C, P> for Bkcr<C, P>
where
    C: Hash + Eq + Clone,
{
    /// The EXP rank `k = -ln(u) / w_c` derived at admit time.
    type Ticket = f64;

    fn admit(&self, callsite: &C) -> Admission<Self::Ticket> {
        let mut inner = self.inner.borrow_mut();
        // Always count for the next period's frozen table.
        let entry = inner.freq_curr.entry(callsite.clone()).or_insert(0);
        *entry += 1;

        let w_c = Self::weight_for(&inner.freq_prev, callsite, self.unseen_weight.get());
        let u: f64 = inner.rng.random();
        // EXP rank.  -ln(U[0,1]) / w  ~  Exp(w).  See Cohen & Kaplan
        // §2 for the family-of-rank-functions discussion.
        let k = -u.ln() / w_c;
        if k < self.tau.get() {
            return Admission::Admit(k);
        }
        // K rejected — consider the novelty reserve.
        if inner.reserve.len() < self.reserve_capacity && !inner.reserved_set.contains(callsite) {
            return Admission::Reserve;
        }
        Admission::Skip
    }

    fn insert(&self, callsite: C, admission: Admission<Self::Ticket>, payload: P) {
        let mut inner = self.inner.borrow_mut();
        match admission {
            Admission::Admit(key) => {
                inner.heap.push(HeapEntry {
                    key,
                    callsite,
                    payload,
                });
                if inner.heap.len() > self.target {
                    let _ = inner.heap.pop();
                    // The (T+1)-th smallest is gone; the new root is
                    // the largest of the remaining T smallest, which
                    // equals the new threshold.
                    let new_tau = inner.heap.peek().expect("heap has >= T items").key;
                    self.tau.set(new_tau);
                }
            }
            Admission::Reserve => {
                let _ = inner.reserved_set.insert(callsite.clone());
                inner.reserve.push((callsite, payload));
            }
            Admission::Skip => {
                debug_assert!(false, "insert called for Skip admission");
            }
        }
    }

    fn flush_into(&self, out: &mut Vec<(C, P, f64)>) {
        let final_tau = self.tau.get();
        let mut inner = self.inner.borrow_mut();
        let unseen = self.unseen_weight.get();
        out.reserve(inner.heap.len() + inner.reserve.len());

        // (1) Drain the bottom-T heap with Horvitz–Thompson weights.
        //     Track the set of kept callsites so the reserve can be
        //     deduplicated against the statistical sample.
        let mut kept: HashSet<C> = HashSet::with_capacity(inner.heap.len());
        let Inner {
            freq_prev,
            heap,
            reserve,
            reserved_set,
            freq_curr,
            rng: _,
        } = &mut *inner;
        for e in heap.drain() {
            let w_c = Self::weight_for(freq_prev, &e.callsite, unseen);
            // EXP-rank inclusion probability: π = 1 - exp(-τ·w_c).
            // In the τ·w_c → 0 limit this agrees with the PRI
            // estimator τ·w_c; for larger products it gives a
            // larger π (smaller weight), which is the source of
            // EXP's variance advantage on subpopulation queries.
            let weight = if final_tau.is_finite() {
                let pi = -(-final_tau * w_c).exp_m1();
                (1.0 / pi).max(1.0)
            } else {
                1.0
            };
            let _ = kept.insert(e.callsite.clone());
            out.push((e.callsite, e.payload, weight));
        }

        // (2) Drain the novelty reserve, suppressing entries that
        //     are already represented in the statistical sample.
        //     Surviving entries carry weight 0: they contribute no
        //     statistical mass but provide observational coverage
        //     of callsites that fired and would otherwise be absent.
        for (c, p) in reserve.drain(..) {
            if !kept.contains(&c) {
                out.push((c, p, 0.0));
            }
        }
        reserved_set.clear();

        // (3) Period-boundary bookkeeping.
        let n_curr: u64 = freq_curr.values().sum();
        if n_curr < self.min_period_count {
            freq_prev.clear();
            self.unseen_weight.set(1.0);
        } else {
            self.unseen_weight
                .set(chao1_unseen_weight(freq_curr.values().copied()));
            std::mem::swap(freq_prev, freq_curr);
        }
        freq_curr.clear();

        self.tau.set(f64::INFINITY);
    }
}

/// Compute the Good–Turing-derived `unseen_weight` from a sample of
/// per-callsite frequencies, using Chao1's species-richness lower
/// bound (Chao, *Scand. J. Statist.* 1984).
///
/// The returned value is `1.0 / f_unseen` where `f_unseen` is the
/// expected per-unseen-callsite frequency in a period of size `n`.
/// Falls back to `1.0` (treat unseen as singletons) in degenerate
/// cases — `n == 0`, `f1 == 0`, no unseen mass, or non-positive
/// `S_unseen`.
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
    // Good–Turing missing-mass estimate: f1 / n.
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
    // it had been seen *more* than once on average.
    (1.0 / f_unseen).min(1.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::SmallRng};

    /// Drive one period of events end-to-end and return the flushed
    /// `(callsite, payload, weight)` records.
    fn run_period<C, P, S>(sampler: &S, events: &[(C, P)]) -> Vec<(C, P, f64)>
    where
        C: Clone,
        P: Clone,
        S: LogSampler<C, P, Ticket = f64>,
    {
        for (c, p) in events {
            let adm = sampler.admit(c);
            match adm {
                Admission::Skip => {}
                Admission::Admit(_) | Admission::Reserve => {
                    sampler.insert(c.clone(), adm, p.clone());
                }
            }
        }
        sampler.flush()
    }

    fn zipf_like_stream(k: usize, n: usize, seed: u64) -> Vec<(u32, u8)> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let weights: Vec<f64> = (0..k).map(|i| 1.0 / (i as f64 + 1.0)).collect();
        let total: f64 = weights.iter().sum();
        let cumulative: Vec<f64> = weights
            .iter()
            .scan(0.0, |a, w| {
                *a += w / total;
                Some(*a)
            })
            .collect();
        (0..n)
            .map(|_| {
                let u: f64 = rng.random();
                let c = cumulative.iter().position(|&c| u < c).unwrap_or(k - 1);
                (c as u32, 0u8)
            })
            .collect()
    }

    // ---------- Basic correctness ----------

    #[test]
    fn admit_skip_and_flush_bounds() {
        let s: Bkcr<u32, u8> = Bkcr::with_options(10, 10, 32, 0xBADC0FFEE0DDF00D);
        // Warmup period: all the same callsite; everything is
        // "unseen" until τ tightens via heap pops.
        let evs: Vec<(u32, u8)> = (0..5_000).map(|_| (1u32, 0u8)).collect();
        let out = run_period(&s, &evs);
        // Single callsite, so heap holds at most T=10 of it; once τ
        // tightens the first rejection of callsite 1 may also land
        // in the reserve (callsite 1 is "first-rejected this period",
        // not "novel").  Bound on output is T + 1 distinct reserve
        // entry = 11.
        assert!(out.len() <= 11);
        let n_heap = out.iter().filter(|(_, _, w)| *w >= 1.0).count();
        let n_reserve = out.iter().filter(|(_, _, w)| *w == 0.0).count();
        assert!((1..=10).contains(&n_heap));
        assert!(n_reserve <= 1);

        // Steady state: with one callsite, τ tightens hard and most
        // events are skipped or reserved-once-then-skipped.
        let mut admits = 0usize;
        for _ in 0..10_000 {
            let adm = s.admit(&1u32);
            if matches!(adm, Admission::Admit(_)) {
                admits += 1;
                s.insert(1u32, adm, 0u8);
            } else if matches!(adm, Admission::Reserve) {
                s.insert(1u32, adm, 0u8);
            }
        }
        let _ = s.flush();
        assert!(admits < 500, "admit count {admits} after warmup");
    }

    #[test]
    fn tau_monotone_within_period() {
        let s: Bkcr<u32, u8> = Bkcr::with_options(10, 0, 32, 7);
        // Warm up so unseen_weight becomes Chao1-derived.
        let warmup: Vec<(u32, u8)> = (0..1_000).map(|i| ((i % 5) as u32, 0u8)).collect();
        let _ = run_period(&s, &warmup);

        let mut prev = s.tau();
        for c in 0u32..200 {
            let adm = s.admit(&(c % 7));
            if let Admission::Admit(t) = adm {
                s.insert(c % 7, Admission::Admit(t), 0u8);
            }
            let now = s.tau();
            assert!(now <= prev, "tau increased: {prev} -> {now}");
            prev = now;
        }
    }

    // ---------- Unbiasedness (HT estimator) ----------

    #[test]
    fn ht_unbiasedness_stationary() {
        // Stationary Zipf-like workload.  After one warmup period,
        // Σ weight should track total event count.
        let k = 6;
        let n = 10_000;
        let periods = 4;
        let trials = 30;
        let mut total_err = 0.0f64;
        for trial in 0..trials {
            let s: Bkcr<u32, u8> = Bkcr::with_options(40, 0, 32, trial as u64 + 1);
            let _ = run_period(&s, &zipf_like_stream(k, n, 7777 + trial as u64));
            let mut sum_w = 0.0;
            let mut sum_n = 0.0;
            for p in 1..periods {
                let evs = zipf_like_stream(k, n, 7777 + trial as u64 + p as u64 * 31);
                let out = run_period(&s, &evs);
                sum_w += out.iter().map(|(_, _, w)| *w).sum::<f64>();
                sum_n += evs.len() as f64;
            }
            total_err += (sum_w - sum_n) / sum_n;
        }
        let mean_rel_err = (total_err / trials as f64).abs();
        assert!(
            mean_rel_err < 0.05,
            "mean rel_err = {mean_rel_err}, expected ≈ 0"
        );
    }

    // ---------- Reserve semantics ----------

    #[test]
    fn reserve_zero_means_vanilla_bottom_k() {
        // R=0: no admission should ever produce Admission::Reserve,
        // and every output weight is ≥ 1.
        let s: Bkcr<u32, u8> = Bkcr::with_options(5, 0, 32, 42);
        // Construct a stream guaranteed to overflow the heap.
        let evs: Vec<(u32, u8)> = (0..1000).map(|i| ((i % 100) as u32, 0u8)).collect();
        let mut saw_reserve = false;
        for (c, p) in &evs {
            let adm = s.admit(c);
            if matches!(adm, Admission::Reserve) {
                saw_reserve = true;
            }
            if matches!(adm, Admission::Admit(_)) {
                s.insert(*c, adm, *p);
            }
        }
        let out = s.flush();
        assert!(!saw_reserve, "Reserve admission emitted with R=0");
        assert!(out.iter().all(|(_, _, w)| *w >= 1.0));
        assert!(out.len() <= 5);
    }

    #[test]
    fn reserve_captures_squeezed_singleton() {
        // T=1, R=2 so both the chatty-first-rejection AND the
        // singleton can be reserved.  Strategy: warm up with the
        // chatty callsite, then a window with chatty events
        // followed by a single novel singleton.
        let s: Bkcr<u32, u8> = Bkcr::with_options(1, 2, 32, 0xC0FFEE);
        let warmup: Vec<(u32, u8)> = (0..1_000).map(|_| (1u32, 0u8)).collect();
        let _ = run_period(&s, &warmup);
        // Window 2: many chatty events followed by one singleton.
        let mut evs: Vec<(u32, u8)> = (0..1_000).map(|_| (1u32, 0u8)).collect();
        evs.push((42u32, 0u8));
        let out = run_period(&s, &evs);
        // At most 1 heap + 2 reserve = 3, deduped.
        assert!(out.len() <= 3);
        let has_singleton = out.iter().any(|(c, _, _)| *c == 42);
        assert!(has_singleton, "novel singleton lost: out={out:?}");
    }

    #[test]
    fn reserve_cap_is_respected() {
        // R=3: flood the stream with novel callsites; the reserve
        // must not exceed 3 entries.
        let r = 3usize;
        let s: Bkcr<u32, u8> = Bkcr::with_options(2, r, 32, 0xCABBA9E);
        // Warm up so the heap τ tightens.
        let warmup: Vec<(u32, u8)> = (0..200).map(|i| ((i % 3) as u32, 0u8)).collect();
        let _ = run_period(&s, &warmup);

        // Window 2: 100 brand-new callsites, each appearing once.
        let evs: Vec<(u32, u8)> = (0..100).map(|i| (1000 + i, 0u8)).collect();
        let out = run_period(&s, &evs);
        let reserve_emitted = out.iter().filter(|(_, _, w)| *w == 0.0).count();
        assert!(
            reserve_emitted <= r,
            "reserve emitted {reserve_emitted} > R={r}; out={out:?}"
        );
    }

    #[test]
    fn reserve_dedups_against_sample() {
        // If a callsite is both reserved AND later admitted to the
        // heap, the flushed output must contain only the heap copy
        // (no weight-0 duplicate).
        //
        // Strategy: tiny T=2 with R=5, fresh sampler so every
        // callsite is "unseen" with the same weight.  Fire many
        // events from many callsites; for any callsite that ends
        // up in the heap, ensure it does not also appear as
        // weight=0.
        let s: Bkcr<u32, u8> = Bkcr::with_options(2, 5, 32, 0xDEADBEEF);
        let evs: Vec<(u32, u8)> = (0..50).map(|i| (i as u32, 0u8)).collect();
        let out = run_period(&s, &evs);
        for (c, _, w) in &out {
            if *w == 0.0 {
                let in_heap = out.iter().any(|(c2, _, w2)| c == c2 && *w2 != 0.0);
                assert!(!in_heap, "callsite {c} duplicated: heap+reserve");
            }
        }
    }

    #[test]
    fn reserve_resets_between_periods() {
        // After a flush, reserve_len must be 0 and a fresh
        // appearance of a callsite that was reserved last period
        // can again be reserved.
        let s: Bkcr<u32, u8> = Bkcr::with_options(1, 5, 32, 0x12345);
        let warmup: Vec<(u32, u8)> = (0..100).map(|_| (1u32, 0u8)).collect();
        let _ = run_period(&s, &warmup);
        // Window 2: chatty + a few novel.
        let mut evs: Vec<(u32, u8)> = (0..500).map(|_| (1u32, 0u8)).collect();
        evs.push((42u32, 0u8));
        evs.push((43u32, 0u8));
        let _ = run_period(&s, &evs);
        assert_eq!(s.reserve_len(), 0, "reserve not cleared after flush");
        // Window 3: same novel callsite — should be eligible for
        // reserve again (since reserved_set was cleared).
        let mut evs: Vec<(u32, u8)> = (0..500).map(|_| (1u32, 0u8)).collect();
        evs.push((42u32, 0u8));
        let out = run_period(&s, &evs);
        assert!(out.iter().any(|(c, _, _)| *c == 42));
    }

    #[test]
    fn output_size_bounded_by_t_plus_r() {
        let t = 7usize;
        let r = 11usize;
        let s: Bkcr<u32, u8> = Bkcr::with_options(t, r, 32, 0xAA55);
        let evs: Vec<(u32, u8)> = (0..10_000).map(|i| (i as u32, 0u8)).collect();
        let out = run_period(&s, &evs);
        assert!(
            out.len() <= t + r,
            "output {} exceeds T+R = {}",
            out.len(),
            t + r
        );
    }

    #[test]
    fn reserve_entries_have_weight_zero() {
        let s: Bkcr<u32, u8> = Bkcr::with_options(1, 50, 32, 0xBEAD);
        // Fresh sampler, lots of unique callsites: many will land
        // in the reserve.
        let evs: Vec<(u32, u8)> = (0..1000).map(|i| (i as u32, 0u8)).collect();
        let out = run_period(&s, &evs);
        let n_reserve = out.iter().filter(|(_, _, w)| *w == 0.0).count();
        assert!(n_reserve > 0, "no reserve entries emitted; out={out:?}");
        // Every weight is either 0 or ≥ 1; no weights in (0, 1).
        for (_, _, w) in &out {
            assert!(*w == 0.0 || *w >= 1.0, "anomalous weight {w}");
        }
    }

    #[test]
    fn reserve_unbiasedness_preserved() {
        // Vanilla BKCR (R=0) vs BKCR with R=T should agree on
        // Σ weight (HT estimator), because reserve entries
        // contribute exactly 0.
        let k = 8;
        let n = 5_000;
        let trials = 20;
        let mut sum_diff = 0.0f64;
        let mut sum_n = 0.0f64;
        for trial in 0..trials {
            let s0: Bkcr<u32, u8> = Bkcr::with_options(20, 0, 32, 9000 + trial as u64);
            let s_r: Bkcr<u32, u8> = Bkcr::with_options(20, 20, 32, 9000 + trial as u64);
            let evs = zipf_like_stream(k, n, 12345 + trial as u64);
            // Warm up both samplers with the same data so freq_prev
            // is identical and the RNG advance is identical.
            let _ = run_period(&s0, &evs);
            let _ = run_period(&s_r, &evs);
            let o0 = run_period(&s0, &evs);
            let or = run_period(&s_r, &evs);
            let w0: f64 = o0.iter().map(|(_, _, w)| *w).sum();
            let wr: f64 = or.iter().map(|(_, _, w)| *w).sum();
            sum_diff += (wr - w0).abs();
            sum_n += w0;
        }
        // Reserve adds only weight-0 entries → ΣwR == Σw0 exactly.
        // Allow a tiny floating tolerance because the two samplers
        // share a seed and so produce identical RNG sequences.
        assert!(
            sum_diff / sum_n < 1e-9,
            "Σ weight diverged between R=0 and R=T: diff/sum = {}",
            sum_diff / sum_n
        );
    }

    // ---------- Chao1 helper ----------

    #[test]
    fn chao1_known_value() {
        // Construct a frequency profile with known answers.
        // f1=3, f2=2, S_seen=5, n=3*1 + 2*2 + 0 = 7.
        let freqs = vec![1u64, 1, 1, 2, 2];
        let w = chao1_unseen_weight(freqs);
        // Chao1 = 5 + (6/7) * 9/4 = 5 + 1.9286 = 6.9286
        // S_unseen = 1.9286
        // missing_mass = 3/7 = 0.4286
        // p_per_species = 0.4286/1.9286 = 0.2222
        // f_unseen = 7 * 0.2222 = 1.5556
        // 1/f_unseen = 0.6429
        assert!((w - 0.6428571).abs() < 1e-4, "got {w}");
    }

    #[test]
    fn chao1_degenerate_cases() {
        assert_eq!(chao1_unseen_weight(std::iter::empty()), 1.0);
        assert_eq!(chao1_unseen_weight(vec![5u64, 5, 5]), 1.0); // f1=0
        // f1 > 0 but f2 == 0: alternate branch.
        let w = chao1_unseen_weight(vec![1u64, 1, 1]);
        assert!(w > 0.0 && w <= 1.0, "got {w}");
    }
}

// ---------------------------------------------------------------------------
// Criterion benchmark (in-source, runnable as an ignored test).
//
// Invoke with:
//   cargo test --release -p otap-df-telemetry \
//       -- --ignored --nocapture bkcr_criterion_bench
//
// Output is full criterion-formatted timing per workload point.
// Kept in this file (rather than a separate benches/ harness) so
// the algorithm, its tests, and its benchmark live together.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod criterion_bench {
    use std::hint::black_box;
    use std::time::Duration;

    use criterion::{BenchmarkId, Criterion, Throughput};
    use rand::{RngExt, SeedableRng, rngs::SmallRng};

    use super::*;

    const SEED: u64 = 0xD15EA5E_5A112024;
    const N_VALUES: &[usize] = &[10_000, 100_000];
    const K_VALUES: &[usize] = &[8, 64];
    const TARGET: usize = 100;

    fn gen_uniform(k: usize, n: usize, seed: u64) -> Vec<u32> {
        let mut rng = SmallRng::seed_from_u64(seed);
        (0..n).map(|_| (rng.random::<u64>() % k as u64) as u32).collect()
    }

    fn gen_zipf_like(k: usize, n: usize, seed: u64) -> Vec<u32> {
        let mut rng = SmallRng::seed_from_u64(seed);
        let weights: Vec<f64> = (0..k).map(|i| 1.0 / (i as f64 + 1.0)).collect();
        let total: f64 = weights.iter().sum();
        let cumulative: Vec<f64> = weights
            .iter()
            .scan(0.0, |a, w| {
                *a += w / total;
                Some(*a)
            })
            .collect();
        (0..n)
            .map(|_| {
                let u: f64 = rng.random();
                cumulative.iter().position(|&c| u < c).unwrap_or(k - 1) as u32
            })
            .collect()
    }

    fn gen_bursty(k: usize, n: usize, seed: u64) -> Vec<u32> {
        let mut rng = SmallRng::seed_from_u64(seed);
        (0..n)
            .map(|i| {
                if i < n / 2 {
                    0u32
                } else {
                    1 + (rng.random::<u64>() % (k as u64 - 1)) as u32
                }
            })
            .collect()
    }

    fn drive(sampler: &Bkcr<u32, u64>, cs: &[u32]) {
        for (i, c) in cs.iter().enumerate() {
            let adm = sampler.admit(c);
            match adm {
                Admission::Skip => {}
                Admission::Admit(_) | Admission::Reserve => {
                    sampler.insert(*c, adm, i as u64);
                }
            }
        }
        let n = sampler.flush().len();
        let _ = black_box(n);
    }

    fn bench_workload(c: &mut Criterion, name: &str, genfn: fn(usize, usize, u64) -> Vec<u32>) {
        let mut group = c.benchmark_group(format!("bkcr/{name}"));
        for &n in N_VALUES {
            for &k in K_VALUES {
                let cs = genfn(k, n, SEED);
                let _ = group.throughput(Throughput::Elements(n as u64));
                let _ = group.bench_with_input(
                    BenchmarkId::new("warm", format!("k={k}/n={n}")),
                    &cs,
                    |b, cs| {
                        b.iter_with_setup(
                            || {
                                // Warm the sampler so freq_prev is
                                // populated and we measure
                                // steady-state cost.
                                let s = Bkcr::<u32, u64>::with_options(TARGET, TARGET, 32, SEED);
                                drive(&s, cs);
                                s
                            },
                            |s| drive(&s, cs),
                        );
                    },
                );
            }
        }
        group.finish();
    }

    #[test]
    #[ignore = "long-running criterion benchmark"]
    fn bkcr_criterion_bench() {
        let mut c = Criterion::default()
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        bench_workload(&mut c, "uniform", gen_uniform);
        bench_workload(&mut c, "zipf_like", gen_zipf_like);
        bench_workload(&mut c, "bursty", gen_bursty);
        c.final_summary();
    }
}
