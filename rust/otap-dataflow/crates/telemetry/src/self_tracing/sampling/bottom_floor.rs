// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The Bottom-Floor weighted bottom-k sampler.
//!
//! This is the statistical core every level of the integrated sampler reuses.
//! It is standard bottom-k sampling with the Horvitz-Thompson inclusion
//! correction and one extra rule, the rarest-seen floor, that protects the
//! rare tail. See the
//! [design][design] for the full derivation; the summary is:
//!
//! - Each arrival of callsite `c` draws a fresh uniform `u` in `(0, 1]` and
//!   forms the exponential key `key = -ln(u) / w_c`. A larger weight makes the
//!   key smaller on average, so heavier-weighted callsites are kept more often.
//! - The sampler keeps the `k + 1` smallest keys in a window. The `(k + 1)`-th
//!   key is the threshold `tau`; it is not configured but falls out of the
//!   data, so the sampler adapts to an unknown arrival rate.
//! - The inclusion probability of one arrival is `pi_c = 1 - exp(-tau * w_c)`,
//!   and the Horvitz-Thompson count `nhat_c = m_c / pi_c` is unbiased. Each kept
//!   record therefore represents `1 / pi_c` arrivals.
//! - The weight is the inverse of the estimated count, `w_c = 1 / nhat_c`,
//!   measured last window and fed back, which drives the sampler toward equal
//!   coverage across callsites.
//! - A callsite not seen last window uses `w_unseen`, the largest weight in the
//!   map, so anything new is kept at least as readily as the rarest known
//!   callsite. This is the floor that gives Bottom-Floor its name.
//!
//! [`BottomFloor`] is the full self-calibrating sampler used by criterion one
//! and the optional SDK-wide span-log stage. [`UniformReservoir`] is the
//! degenerate single-weight case used by the criterion-two per-span reservoir,
//! where every record inside one span shares one weight so no feedback is
//! needed.
//!
//! [design]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-logs-traces-reservoir.md

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

use super::heavy_hitter::{HeavyHitterTable, SharedHeavyHitterTable};

/// One retained arrival together with the data needed to finalize it.
struct Entry<P> {
    /// The exponential key `-ln(u) / w`. Smaller keys are higher priority.
    key: f64,
    /// The callsite (or other grouping key) the arrival belongs to.
    group: u64,
    /// The uniform draw that produced the key, retained so a downstream stage
    /// can reuse the same `u` for a consistent binding decision.
    u: f64,
    /// The number of arrivals this entry stands for. One for a first-level
    /// arrival; a second-level representative carries the local adjusted count
    /// of the arrivals it summarizes, so a group total sums the values rather
    /// than counting entries.
    value: f64,
    /// The caller's payload, for example a handle to the retained record.
    payload: P,
}

impl<P> PartialEq for Entry<P> {
    fn eq(&self, other: &Self) -> bool {
        self.key.total_cmp(&other.key) == Ordering::Equal
    }
}

impl<P> Eq for Entry<P> {}

impl<P> PartialOrd for Entry<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> Ord for Entry<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        // A `BinaryHeap` is a max-heap, so ordering by key means `peek` returns
        // the largest key, the first candidate for eviction.
        self.key.total_cmp(&other.key)
    }
}

/// One record kept by a window, annotated with its adjusted count.
pub struct Kept<P> {
    /// The caller's payload for the kept record.
    pub payload: P,
    /// The grouping key the record was counted under.
    pub group: u64,
    /// The Horvitz-Thompson adjusted count, `1 / pi`, the estimated number of
    /// arrivals this single kept record represents.
    pub adjusted_count: f64,
    /// The number of arrivals this kept entry stood for on input, one for a
    /// first-level arrival. A second-level caller multiplies `value` by
    /// `adjusted_count` to recover the arrivals the representative stands for
    /// after the inclusion correction.
    pub value: f64,
    /// The uniform draw the record used, for a downstream stage that reuses it.
    pub u: f64,
}

/// The result of finalizing one window.
pub struct WindowOutput<P> {
    /// The kept records with their adjusted counts.
    pub kept: Vec<Kept<P>>,
    /// The threshold `tau` the window settled on, or `+inf` when the window did
    /// not fill its budget and every arrival was kept.
    pub tau: f64,
    /// The Horvitz-Thompson estimated count `nhat_c` per grouping key present in
    /// the sample. Used by callers that need the population profile, for example
    /// to derive per-callsite surprisals.
    pub group_estimates: HashMap<u64, f64>,
    /// The boundary record dropped to define `tau`, the `(budget + 1)`-th
    /// smallest key, when the window filled its budget. It is not part of the
    /// sample, but a caller that must not lose any record, such as the per-worker
    /// buffer conserving span-selected records, can recover it here. `None` when
    /// the window did not fill its budget.
    pub boundary: Option<P>,
}

/// A fixed-capacity max-heap that keeps the `cap` smallest-key arrivals seen in
/// a window. The capacity is the budget plus one, so the largest retained key
/// is the `(budget + 1)`-th smallest and becomes the threshold at finalize.
struct KeyedHeap<P> {
    heap: BinaryHeap<Entry<P>>,
    cap: usize,
}

impl<P> KeyedHeap<P> {
    fn new(budget: usize) -> Self {
        let cap = budget.saturating_add(1);
        Self {
            heap: BinaryHeap::with_capacity(cap),
            cap,
        }
    }

    /// Offer one arrival. Keeps it when there is room or when its key beats the
    /// current largest; otherwise the displaced arrival is returned.
    ///
    /// The returned payload is *terminal*: it is either the new arrival when it
    /// lost the comparison, or the previously-kept arrival it evicted. In
    /// bottom-k a displaced arrival can never re-enter the sample, because the
    /// threshold only tightens as the window fills, so the caller may route it
    /// to its final destination immediately. `None` means the arrival was kept.
    fn offer(&mut self, key: f64, group: u64, u: f64, value: f64, payload: P) -> Option<P> {
        let entry = Entry {
            key,
            group,
            u,
            value,
            payload,
        };
        if self.heap.len() < self.cap {
            self.heap.push(entry);
            return None;
        }
        if let Some(mut top) = self.heap.peek_mut() {
            if entry.key < top.key {
                // Evict the current largest, returning its payload. Dropping the
                // `PeekMut` guard sifts the new entry down into place.
                let evicted = std::mem::replace(&mut *top, entry);
                return Some(evicted.payload);
            }
        }
        // The arrival lost the comparison and is itself the displaced payload.
        Some(entry.payload)
    }

    /// Remove the boundary entry and return it together with its key as the
    /// threshold and the remaining sample. When the heap never filled its
    /// capacity the threshold is `+inf`, the boundary is `None`, and every
    /// arrival is kept.
    fn drain_with_boundary(&mut self) -> (f64, Option<Entry<P>>, Vec<Entry<P>>) {
        let boundary = if self.heap.len() == self.cap {
            // The largest of the `budget + 1` smallest keys defines the
            // threshold and is not part of the sample.
            self.heap.pop()
        } else {
            None
        };
        let tau = boundary.as_ref().map_or(f64::INFINITY, |e| e.key);
        let sample = self.heap.drain().collect();
        (tau, boundary, sample)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.heap.len()
    }
}

/// Draw a fresh uniform in `(0, 1]` from an xorshift64* generator and form the
/// exponential key `-ln(u) / weight`. The interval excludes zero so the
/// logarithm is finite.
fn exponential_key(rng: &mut Rng, weight: f64) -> (f64, f64) {
    let u = rng.next_uniform_pos();
    (-u.ln() / weight, u)
}

/// The inclusion probability `pi = 1 - exp(-tau * weight)`, computed accurately
/// for small exponents and clamped into `[0, 1]`. An infinite threshold means
/// the window kept everything, so the probability is one.
#[must_use]
pub fn inclusion_probability(tau: f64, weight: f64) -> f64 {
    if !tau.is_finite() {
        return 1.0;
    }
    let x = tau * weight;
    // `(-x).exp_m1()` is `exp(-x) - 1`, so `1 - exp(-x)` is its negation, which
    // keeps precision when `x` is small.
    (-((-x).exp_m1())).clamp(0.0, 1.0)
}

/// The inclusion probability `1 - exp(-cut)` for a combined cutoff, computed
/// accurately for small cutoffs and clamped into `[0, 1]`. An infinite cutoff
/// means the arrival was never gated on that side, so it contributes no
/// suppression. Used for the composed local-and-global cutoff of the binding
/// gate.
#[must_use]
fn inclusion_probability_from_cut(cut: f64) -> f64 {
    if !cut.is_finite() {
        return 1.0;
    }
    (-((-cut).exp_m1())).clamp(0.0, 1.0)
}

/// A small seedable xorshift64* generator, matching the dependency-free style of
/// the surrounding self-tracing code and keeping windows reproducible in tests.
struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero fixed point.
        Self {
            state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A uniform double in `(0, 1]`. The 53-bit mantissa plus one keeps the
    /// result strictly positive so `-ln(u)` is finite.
    fn next_uniform_pos(&mut self) -> f64 {
        let bits = self.next_u64() >> 11; // 53 bits
        (bits as f64 + 1.0) / 9_007_199_254_740_992.0 // (bits + 1) / 2^53
    }
}

/// Group the kept sample by its grouping key, summing the value `m_c` each
/// group stands for and computing the effective inclusion probability `pi_c`.
/// For a first-level window every entry has unit value, so `m_c` is the kept
/// count; for a second-level window each entry carries the local adjusted count
/// it represents. When `gate` is present the probability composes the local
/// cutoff `tau_l * w_local_c` with the global cutoff `tau_g * g_c` by their
/// minimum, matching the binding gate that produced the sample.
fn group_counts<P>(
    tau: f64,
    sample: &[Entry<P>],
    weight_of: impl Fn(u64) -> f64,
    gate: Option<&HeavyHitterTable>,
) -> HashMap<u64, (f64, f64)> {
    let mut counts: HashMap<u64, (f64, f64)> = HashMap::new();
    for entry in sample {
        let slot = counts.entry(entry.group).or_insert_with(|| {
            let local_cut = tau * weight_of(entry.group);
            let cut = match gate {
                Some(gate) => local_cut.min(gate.global_score(entry.group)),
                None => local_cut,
            };
            (0.0, inclusion_probability_from_cut(cut))
        });
        slot.0 += entry.value;
    }
    counts
}

/// The self-calibrating Bottom-Floor sampler.
///
/// It keys arrivals by callsite, weights each callsite by the inverse of its
/// estimated count from the previous window, applies the rarest-seen floor to
/// callsites it has not seen, and produces unbiased adjusted counts. The
/// persistent state between windows is one weight per kept callsite, at most
/// `budget` of them, plus the single `w_unseen` scalar.
pub struct BottomFloor<P> {
    heap: KeyedHeap<P>,
    /// Per-callsite weights measured last window. At most `budget` entries.
    weights: HashMap<u64, f64>,
    /// The rarest-seen floor weight for callsites absent from `weights`.
    w_unseen: f64,
    rng: Rng,
    /// The optional stage-two heavy-hitter gate. When present, an arrival is
    /// additionally subject to the global cutoff `tau_g * g_c`, and kept records
    /// are corrected by the composed inclusion probability. Absent for the
    /// aggregator's own second level and the per-span reservoirs.
    gate: Option<SharedHeavyHitterTable>,
    /// The heavy-hitter table snapshot taken for the current window, so observe
    /// and finalize agree on the same global cutoffs. Reset each window.
    window_gate: Option<Arc<HeavyHitterTable>>,
}

impl<P> BottomFloor<P> {
    /// Create a sampler with the memory budget `k` and a deterministic seed.
    #[must_use]
    pub fn new(budget: usize, seed: u64) -> Self {
        Self {
            heap: KeyedHeap::new(budget),
            weights: HashMap::new(),
            w_unseen: 1.0,
            rng: Rng::new(seed),
            gate: None,
            window_gate: None,
        }
    }

    /// Create a sampler with a stage-two heavy-hitter binding gate.
    ///
    /// Each window it snapshots `gate` and rejects an arrival whose global score
    /// `tau_g * g_c` binds before the local threshold, correcting the kept counts
    /// by the composed inclusion probability
    /// `pi_eff_c = 1 - exp(-min(tau_l * w_local_c, tau_g * g_c))`. Used by the
    /// per-worker criterion one; the local-only fail-safe table never binds.
    #[must_use]
    pub fn new_gated(budget: usize, seed: u64, gate: SharedHeavyHitterTable) -> Self {
        Self {
            heap: KeyedHeap::new(budget),
            weights: HashMap::new(),
            w_unseen: 1.0,
            rng: Rng::new(seed),
            gate: Some(gate),
            window_gate: None,
        }
    }

    /// The weight currently assigned to a callsite, the floor when unseen.
    #[must_use]
    fn weight_of(&self, callsite: u64) -> f64 {
        self.weights.get(&callsite).copied().unwrap_or(self.w_unseen)
    }

    /// The global binding cutoff `tau_g * g_c` for this window, snapshotting the
    /// heavy-hitter table once so observe and finalize agree. Infinite, never
    /// binding, when ungated.
    fn window_global_cut(&mut self, callsite: u64) -> f64 {
        let Some(gate) = &self.gate else {
            return f64::INFINITY;
        };
        if self.window_gate.is_none() {
            self.window_gate = Some(gate.load_full());
        }
        self.window_gate
            .as_ref()
            .expect("snapshot set above")
            .global_score(callsite)
    }

    /// Offer one arrival of `callsite` carrying `payload`, returning the
    /// displaced payload if the arrival was not kept.
    ///
    /// The returned payload is terminal: either this arrival when it lost a heap
    /// slot, or the previously-kept arrival it evicted. `None` means the arrival
    /// is now reserved in the sample. A caller that does not need the displaced
    /// payload may ignore the result.
    pub fn observe(&mut self, callsite: u64, payload: P) -> Option<P> {
        self.observe_weighted(callsite, 1.0, payload)
    }

    /// Offer one arrival of `callsite` that stands for `value` arrivals, for a
    /// second-level window that aggregates representatives.
    ///
    /// A first-level [`observe`](Self::observe) is the `value == 1.0` case. At
    /// the second level `value` is the representative's local adjusted count, so
    /// the group total sums the values it stood for rather than counting
    /// representatives, and the weight feedback becomes the inverse of the
    /// aggregated count. The returned payload is terminal, exactly as in
    /// [`observe`](Self::observe).
    pub fn observe_weighted(&mut self, callsite: u64, value: f64, payload: P) -> Option<P> {
        let weight = self.weight_of(callsite);
        if weight <= 0.0 {
            return Some(payload);
        }
        let (key, u) = exponential_key(&mut self.rng, weight);
        // Stage-two binding gate: reject when the global score binds this window,
        // sharing the one draw `u` across the local and global cutoffs. A no-op
        // when ungated, since the cutoff is then infinite.
        if self.gate.is_some() {
            let x = key * weight; // -ln(u)
            if x >= self.window_global_cut(callsite) {
                return Some(payload);
            }
        }
        self.heap.offer(key, callsite, u, value, payload)
    }

    /// Finalize the window: settle the threshold, emit the kept records with
    /// their adjusted counts, and fold the estimated counts back into the
    /// weights for the next window.
    pub fn finalize(&mut self) -> WindowOutput<P> {
        let (tau, boundary, sample) = self.heap.drain_with_boundary();

        // The inclusion probabilities use the weights that drove this window and,
        // when gated, the same heavy-hitter snapshot the observes used. Take the
        // snapshot so it resets for the next window.
        let gate = self.window_gate.take();
        let counts = group_counts(tau, &sample, |c| self.weight_of(c), gate.as_deref());

        let kept = sample
            .into_iter()
            .map(|entry| {
                let pi = counts[&entry.group].1;
                Kept {
                    adjusted_count: adjusted_count(pi),
                    group: entry.group,
                    u: entry.u,
                    value: entry.value,
                    payload: entry.payload,
                }
            })
            .collect();

        // Feed the estimated counts back into the weights: w_next = 1 / nhat,
        // and the floor is the largest such weight, the rarest kept callsite.
        let mut next_weights = HashMap::with_capacity(counts.len());
        let mut group_estimates = HashMap::with_capacity(counts.len());
        let mut max_weight = 0.0_f64;
        for (group, (sum_value, pi)) in counts {
            let nhat = sum_value / pi;
            let weight = if nhat > 0.0 { 1.0 / nhat } else { 1.0 };
            max_weight = max_weight.max(weight);
            let _ = next_weights.insert(group, weight);
            let _ = group_estimates.insert(group, nhat);
        }
        self.weights = next_weights;
        self.w_unseen = if max_weight > 0.0 { max_weight } else { 1.0 };

        WindowOutput {
            kept,
            tau,
            group_estimates,
            boundary: boundary.map(|entry| entry.payload),
        }
    }
}

/// The uniform per-span reservoir, the criterion-two sampler.
///
/// Every record inside one span shares one span-start weight, so the reservoir
/// is the degenerate Bottom-Floor with a single constant weight and no
/// feedback. It keeps the `r + 1` smallest uniform keys and discards itself at
/// the window boundary, so a span that runs across windows is sampled
/// independently in each.
pub struct UniformReservoir<P> {
    heap: KeyedHeap<P>,
    rng: Rng,
}

impl<P> UniformReservoir<P> {
    /// Create a reservoir of size `r` with a deterministic seed.
    #[must_use]
    pub fn new(size: usize, seed: u64) -> Self {
        Self {
            heap: KeyedHeap::new(size),
            rng: Rng::new(seed),
        }
    }

    /// Offer one in-span record. The grouping key is irrelevant within a single
    /// span, so all records share group zero and a unit weight.
    pub fn observe(&mut self, payload: P) {
        let (key, u) = exponential_key(&mut self.rng, 1.0);
        let _ = self.heap.offer(key, 0, u, 1.0, payload);
    }

    /// Finalize the reservoir into its representatives and adjusted counts.
    #[must_use]
    pub fn finalize(mut self) -> WindowOutput<P> {
        let (tau, boundary, sample) = self.heap.drain_with_boundary();
        let pi = inclusion_probability(tau, 1.0);
        let adjusted = adjusted_count(pi);
        let mut total = 0.0_f64;
        let kept = sample
            .into_iter()
            .map(|entry| {
                total += adjusted;
                Kept {
                    adjusted_count: adjusted,
                    group: entry.group,
                    u: entry.u,
                    value: 1.0,
                    payload: entry.payload,
                }
            })
            .collect();
        let mut group_estimates = HashMap::with_capacity(1);
        if total > 0.0 {
            let _ = group_estimates.insert(0u64, total);
        }
        WindowOutput {
            kept,
            tau,
            group_estimates,
            boundary: boundary.map(|entry| entry.payload),
        }
    }
}

/// The adjusted count `1 / pi` for a kept record, guarding the degenerate
/// zero-probability case that only arises when a record was kept under a unit
/// inclusion probability.
fn adjusted_count(pi: f64) -> f64 {
    if pi > 0.0 { 1.0 / pi } else { 1.0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::heavy_hitter::shared_local_only;

    #[test]
    fn keeps_everything_when_under_budget() {
        let mut bf = BottomFloor::<u32>::new(8, 1);
        for i in 0..5 {
            let _ = bf.observe(u64::from(i), i);
        }
        let out = bf.finalize();
        assert_eq!(out.kept.len(), 5);
        assert!(out.tau.is_infinite());
        // Each record was kept under unit probability and represents one
        // arrival.
        for kept in &out.kept {
            assert!((kept.adjusted_count - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn heap_memory_is_bounded_by_budget_plus_one() {
        let budget = 4;
        let mut bf = BottomFloor::<u32>::new(budget, 7);
        for i in 0..10_000u32 {
            let _ = bf.observe(u64::from(i % 32), i);
            assert!(bf.heap.len() <= budget + 1);
        }
        let out = bf.finalize();
        assert_eq!(out.kept.len(), budget);
    }

    #[test]
    fn adjusted_counts_recover_the_total_within_tolerance() {
        // A heavy-head, long-tail population over many callsites. After a few
        // warmup windows the summed adjusted count should track the true total
        // of arrivals each window. Average over windows to suppress variance.
        let budget = 64;
        let mut bf = BottomFloor::<()>::new(budget, 0x1234_5678);

        let windows = 200;
        let mut true_total = 0u64;
        let mut est_total = 0.0_f64;
        for w in 0..windows {
            let mut n = 0u64;
            // 1 head callsite with 5000 arrivals, 400 tail callsites with a few.
            for _ in 0..5000 {
                let _ = bf.observe(0, ());
                n += 1;
            }
            for c in 1..=400u64 {
                let reps = (c % 5) + 1;
                for _ in 0..reps {
                    let _ = bf.observe(c, ());
                    n += 1;
                }
            }
            let out = bf.finalize();
            // Skip warmup windows where weights are still settling.
            if w >= 20 {
                true_total += n;
                est_total += out.kept.iter().map(|k| k.adjusted_count).sum::<f64>();
            }
        }
        let ratio = est_total / true_total as f64;
        assert!(
            (ratio - 1.0).abs() < 0.05,
            "estimated/true total ratio {ratio} not within 5%"
        );
    }

    #[test]
    fn rare_tail_is_recalled_better_than_uniform() {
        // With inverse-frequency weighting the rare tail should be recalled far
        // more often than a uniform bottom-k would manage at the same budget.
        let budget = 32;
        let mut bf = BottomFloor::<u64>::new(budget, 99);

        // Warm up the weights over several windows.
        let mut recalled_tail = 0usize;
        let tail = 1..=200u64;
        let windows = 40;
        for w in 0..windows {
            for _ in 0..10_000 {
                let _ = bf.observe(0, 0); // head
            }
            for c in tail.clone() {
                let _ = bf.observe(c, c); // one arrival each, the rare tail
            }
            let out = bf.finalize();
            if w >= 10 {
                for kept in &out.kept {
                    if kept.group != 0 {
                        recalled_tail += 1;
                    }
                }
            }
        }
        // A uniform bottom-32 over ~10200 arrivals would keep about 32 * 200 /
        // 10200 ~ 0.6 tail items per window, ~18 over 30 scored windows. The
        // floor plus inverse-frequency weighting should beat that comfortably.
        assert!(
            recalled_tail > 60,
            "tail recall {recalled_tail} too low; floor not protecting the tail"
        );
    }

    #[test]
    fn uniform_reservoir_keeps_size_and_estimates_volume() {
        let r = 16;
        let mut res = UniformReservoir::<u32>::new(r, 5);
        let n = 1000u32;
        for i in 0..n {
            res.observe(i);
        }
        let out = res.finalize();
        assert_eq!(out.kept.len(), r);
        // The summed adjusted count estimates the in-span volume n.
        let est: f64 = out.kept.iter().map(|k| k.adjusted_count).sum();
        let ratio = est / f64::from(n);
        assert!(
            (ratio - 1.0).abs() < 0.5,
            "reservoir volume estimate {est} (ratio {ratio}) off"
        );
    }

    #[test]
    fn inclusion_probability_edges() {
        assert_eq!(inclusion_probability(f64::INFINITY, 0.5), 1.0);
        // Large exponent saturates to one.
        assert!((inclusion_probability(1e9, 1.0) - 1.0).abs() < 1e-12);
        // Small exponent is approximately tau * weight.
        let p = inclusion_probability(1e-6, 1.0);
        assert!((p - 1e-6).abs() < 1e-9);
    }

    #[test]
    fn weighted_under_budget_recovers_summed_value() {
        // Second-level use: each representative stands for a local adjusted
        // count. Under budget every representative is kept at unit probability,
        // so its recovered count is exactly its carried value and the group
        // total is the sum of values.
        let mut bf = BottomFloor::<u32>::new(16, 1);
        let values = [3.0, 7.5, 100.0, 0.25];
        for (i, v) in values.iter().enumerate() {
            let _ = bf.observe_weighted(u64::from(i as u32 % 2), *v, i as u32);
        }
        let out = bf.finalize();
        // pi == 1 under budget, so value * adjusted_count == value.
        let recovered: f64 = out.kept.iter().map(|k| k.value * k.adjusted_count).sum();
        assert!((recovered - values.iter().sum::<f64>()).abs() < 1e-9);
        // The group estimates sum the carried values, not the entry count.
        let total_est: f64 = out.group_estimates.values().sum();
        assert!((total_est - values.iter().sum::<f64>()).abs() < 1e-9);
    }

    #[test]
    fn weighted_aggregation_recovers_global_total_over_budget() {
        // Over budget, the second-level Horvitz-Thompson correction still
        // recovers the summed value total on average across windows. Each window
        // presents many callsites, each representative carrying a value.
        let budget = 32;
        let mut bf = BottomFloor::<()>::new(budget, 0xABCD);
        let windows = 200;
        let mut true_total = 0.0_f64;
        let mut est_total = 0.0_f64;
        for w in 0..windows {
            let mut window_true = 0.0_f64;
            for c in 0..400u64 {
                // A per-callsite value that varies across callsites.
                let value = 1.0 + f64::from((c % 13) as u32);
                let _ = bf.observe_weighted(c, value, ());
                window_true += value;
            }
            let out = bf.finalize();
            if w >= 20 {
                true_total += window_true;
                est_total += out
                    .kept
                    .iter()
                    .map(|k| k.value * k.adjusted_count)
                    .sum::<f64>();
            }
        }
        let ratio = est_total / true_total;
        assert!(
            (ratio - 1.0).abs() < 0.05,
            "weighted estimated/true total ratio {ratio} not within 5%"
        );
    }

    fn shared_heavy_hitter(estimates: &[(u64, f64)], tau_g: f64) -> SharedHeavyHitterTable {
        use arc_swap::ArcSwap;
        use std::sync::Arc;
        let map: HashMap<u64, f64> = estimates.iter().copied().collect();
        Arc::new(ArcSwap::from_pointee(HeavyHitterTable::from_estimates(
            &map, tau_g,
        )))
    }

    #[test]
    fn gate_local_only_matches_ungated() {
        // The local-only fail-safe table has an infinite global threshold, so a
        // gated sampler makes exactly the same keep and drop decisions and the
        // same adjusted counts as an ungated one with the same seed.
        let mut gated = BottomFloor::<u32>::new_gated(16, 42, shared_local_only());
        let mut plain = BottomFloor::<u32>::new(16, 42);
        for i in 0..2000u32 {
            let g = gated.observe(u64::from(i % 8), i);
            let p = plain.observe(u64::from(i % 8), i);
            assert_eq!(g.is_some(), p.is_some(), "keep/drop diverged at {i}");
        }
        let go = gated.finalize();
        let po = plain.finalize();
        assert_eq!(go.kept.len(), po.kept.len());
        let ge: f64 = go.kept.iter().map(|k| k.adjusted_count).sum();
        let pe: f64 = po.kept.iter().map(|k| k.adjusted_count).sum();
        assert!((ge - pe).abs() < 1e-9, "adjusted totals diverged");
    }

    #[test]
    fn binding_gate_throttles_abundant_and_stays_unbiased() {
        // Callsite 1 is globally abundant (g_1 = 1/10000) and callsite 2 rare
        // (g_2 = 1/10). With a finite tau_g the global cut binds callsite 1 far
        // harder. The budget is large so the local threshold never binds, so the
        // effective inclusion is the global cut alone.
        let table = shared_heavy_hitter(&[(1, 10_000.0), (2, 10.0)], 5.0);
        let mut bf = BottomFloor::<u64>::new_gated(1024, 7, table);

        let windows = 400;
        let (n1, n2) = (2000u64, 200u64);
        let (mut true1, mut est1, mut kept1) = (0.0_f64, 0.0_f64, 0usize);
        let (mut true2, mut est2, mut kept2) = (0.0_f64, 0.0_f64, 0usize);
        for w in 0..windows {
            for _ in 0..n1 {
                let _ = bf.observe(1, 1);
            }
            for _ in 0..n2 {
                let _ = bf.observe(2, 2);
            }
            let out = bf.finalize();
            if w >= 20 {
                for k in &out.kept {
                    match k.group {
                        1 => {
                            est1 += k.adjusted_count;
                            kept1 += 1;
                        }
                        2 => {
                            est2 += k.adjusted_count;
                            kept2 += 1;
                        }
                        _ => {}
                    }
                }
                true1 += n1 as f64;
                true2 += n2 as f64;
            }
        }
        // Unbiased: the composed Horvitz-Thompson correction recovers each true
        // arrival total despite the gate dropping most abundant arrivals.
        let r1 = est1 / true1;
        let r2 = est2 / true2;
        assert!((r1 - 1.0).abs() < 0.1, "abundant callsite ratio {r1}");
        assert!((r2 - 1.0).abs() < 0.1, "rare callsite ratio {r2}");
        // Throttled: the abundant callsite, with ten times the arrivals, keeps
        // far fewer records than the rare one.
        assert!(
            kept1 < kept2,
            "abundant kept {kept1} should be far below rare kept {kept2}"
        );
    }
}
