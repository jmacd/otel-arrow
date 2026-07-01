// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Span-start sampling toward a target rate.
//!
//! The trace side of the integrated sampler selects whole spans at their start
//! by a consistent-probability threshold keyed on the span-start callsite. The
//! thresholds are not configured; they are derived each window from the
//! previous window's measured span-start counts so the admitted total tracks a
//! single configured target, the number of span starts to admit per window
//! across all callsites.
//!
//! [`SpanStartTable`] is the immutable per-window table the tracer reads
//! wait-free through an [`arc_swap::ArcSwap`]. [`SpanStartSampler`] is the
//! [`Sampler`] that consults it on root spans while honoring an inherited
//! threshold on children. [`build_span_start_table`] is the window-boundary
//! feedback that turns counts and value scores into the next table.
//!
//! The assignment gives each callsite an admitted share proportional to a value
//! score `V_s` and inversely proportional to its span-start volume, which is the
//! inverse-frequency rule that spreads the budget across span kinds rather than
//! letting the busiest few consume it. A flat `V_s` recovers plain equal
//! coverage, so value weighting never does worse than the baseline. See the
//! [design][design] for the derivation.
//!
//! [design]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-logs-traces-reservoir.md

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::super::sampler::{Sampler, SamplingIntent, SamplingParameters};
use super::super::span::Threshold;

/// An immutable, per-window table mapping span-start callsite to a
/// consistent-probability threshold, with a default for callsites not yet
/// present.
#[derive(Clone, Debug)]
pub struct SpanStartTable {
    thresholds: HashMap<u64, Threshold>,
    default: Threshold,
}

impl SpanStartTable {
    /// Create a table with the given per-callsite thresholds and a default for
    /// callsites not present.
    #[must_use]
    pub fn new(thresholds: HashMap<u64, Threshold>, default: Threshold) -> Self {
        Self {
            thresholds,
            default,
        }
    }

    /// A table that samples every span start. Used as the fail-safe default
    /// before any window has produced a table, so an absent table relaxes
    /// toward keeping more rather than incorrect over-suppression.
    #[must_use]
    pub fn always() -> Self {
        Self {
            thresholds: HashMap::new(),
            default: Threshold::ALWAYS,
        }
    }

    /// A table with a single uniform default threshold and no per-callsite
    /// entries.
    #[must_use]
    pub fn uniform(default: Threshold) -> Self {
        Self {
            thresholds: HashMap::new(),
            default,
        }
    }

    /// The threshold for a span-start callsite, the default when not present.
    #[must_use]
    pub fn threshold_for(&self, callsite: u64) -> Threshold {
        self.thresholds.get(&callsite).copied().unwrap_or(self.default)
    }

    /// The default threshold for callsites not in the table.
    #[must_use]
    pub fn default_threshold(&self) -> Threshold {
        self.default
    }

    /// The number of tracked span-start callsites.
    #[must_use]
    pub fn len(&self) -> usize {
        self.thresholds.len()
    }

    /// Whether the table has any per-callsite entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.thresholds.is_empty()
    }
}

/// A shared, atomically swappable handle to a [`SpanStartTable`].
///
/// The processor publishes a fresh immutable table per window with a single
/// store; the tracer reads it wait-free on its next span-start decision.
pub type SharedSpanStartTable = Arc<ArcSwap<SpanStartTable>>;

/// Create a shared table handle initialized to sample every span start.
#[must_use]
pub fn shared_always() -> SharedSpanStartTable {
    Arc::new(ArcSwap::from_pointee(SpanStartTable::always()))
}

/// A [`Sampler`] that selects root spans by the span-start threshold table and
/// honors an inherited threshold on child spans.
///
/// On a root span it reads the current table and applies the callsite's
/// consistent-probability threshold, marking it reliable so descendants inherit
/// it. On a child span it propagates the reconciled parent threshold, so the
/// table only changes how root and unparented span thresholds are chosen.
pub struct SpanStartSampler {
    table: SharedSpanStartTable,
}

impl SpanStartSampler {
    /// Create a sampler reading from the given shared table.
    #[must_use]
    pub fn new(table: SharedSpanStartTable) -> Self {
        Self { table }
    }

    /// The shared table handle, for the processor to publish updates onto.
    #[must_use]
    pub fn table(&self) -> SharedSpanStartTable {
        Arc::clone(&self.table)
    }
}

impl Sampler for SpanStartSampler {
    fn sampling_intent(&self, params: &SamplingParameters<'_>) -> SamplingIntent {
        if params.parent.is_some() {
            // A child inherits the trace decision; the table governs roots only.
            return SamplingIntent {
                threshold: params.parent_threshold,
                threshold_reliable: params.parent_threshold_reliable,
            };
        }
        let table = self.table.load();
        SamplingIntent {
            threshold: Some(table.threshold_for(params.start_callsite)),
            threshold_reliable: true,
        }
    }
}

/// Build the next window's span-start threshold table from the previous
/// window's estimated span-start counts and value scores.
///
/// `target` is the number of span starts to admit per window across all
/// callsites. `starts[s]` is the estimated true span-start count of callsite
/// `s`. `value_of` returns the value score `V_s`; pass a closure returning a
/// constant to get plain equal coverage. The admitted share of callsite `s` is
///
/// ```text
/// A_s = V_s * target / sum_s' V_s'
/// P_s = clamp(A_s / starts[s], 0, 1)
/// ```
///
/// and `T_s` is the threshold for `P_s`. The table's default is the most
/// permissive assigned threshold, so a brand-new span-start callsite is sampled
/// at least as readily as the most permissive known kind.
#[must_use]
pub fn build_span_start_table(
    target: f64,
    starts: &HashMap<u64, f64>,
    value_of: impl Fn(u64) -> f64,
    fallback_default: Threshold,
) -> SpanStartTable {
    let sum_v: f64 = starts.keys().map(|&s| value_of(s).max(0.0)).sum();
    if starts.is_empty() || sum_v <= 0.0 || target <= 0.0 {
        return SpanStartTable::uniform(fallback_default);
    }

    let mut thresholds = HashMap::with_capacity(starts.len());
    // The most permissive threshold has the smallest raw value; track it as the
    // default for unseen callsites.
    let mut most_permissive = Threshold::NEVER;
    for (&s, &n_s) in starts {
        let v_s = value_of(s).max(0.0);
        let admitted = v_s * target / sum_v;
        let p_s = if n_s > 0.0 {
            (admitted / n_s).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let t_s = Threshold::from_probability(p_s);
        if t_s.value() < most_permissive.value() {
            most_permissive = t_s;
        }
        let _ = thresholds.insert(s, t_s);
    }
    SpanStartTable::new(thresholds, most_permissive)
}

/// The span-start value accumulator shared by the single-stream processor and
/// the all-CPU aggregator.
///
/// It measures, per window, the surprisal value of each span kind and the
/// span-start volume per callsite, then builds the next window's span-start
/// threshold table. The per-log-callsite surprisals and the mean surprisal
/// scale are carried across windows, while the per-window sums are reset each
/// flush. See [`docs/span-start-value-sampling.md`][design] for the value math.
///
/// [design]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/span-start-value-sampling.md
pub(crate) struct SpanStartValues {
    /// Per span-start callsite: the summed surprisal of the in-span logs, the
    /// numerator of the value score.
    surprisal_sum: HashMap<u64, f64>,
    /// Per span-start callsite: the in-span log count `N[s,.]`.
    surprisal_count: HashMap<u64, f64>,
    /// Per span-start callsite: the estimated span-start volume `N_starts[s]`.
    start_counts: HashMap<u64, f64>,
    /// Per log callsite: the surprisal `g_l` measured last window.
    log_surprisals: HashMap<u64, f64>,
    /// The surprisal of an unseen log callsite, the rarest-seen column mass.
    log_surprisal_floor: f64,
    /// The mean surprisal `H` last window, used to shrink under-observed kinds.
    mean_surprisal: f64,
}

impl SpanStartValues {
    /// Create an accumulator with empty state and no surprisal scale, which
    /// falls back to equal coverage until the first window updates it.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            surprisal_sum: HashMap::new(),
            surprisal_count: HashMap::new(),
            start_counts: HashMap::new(),
            log_surprisals: HashMap::new(),
            log_surprisal_floor: 0.0,
            mean_surprisal: 0.0,
        }
    }

    /// Accumulate one in-span record's surprisal under its span's start
    /// callsite, using last window's per-log-callsite surprisals. Every in-span
    /// record is counted, sampled or not, so the bucket totals are exact window
    /// values.
    pub(crate) fn observe_in_span(&mut self, start_callsite: u64, log_callsite: u64) {
        let g_l = self
            .log_surprisals
            .get(&log_callsite)
            .copied()
            .unwrap_or(self.log_surprisal_floor);
        *self.surprisal_sum.entry(start_callsite).or_insert(0.0) += g_l;
        *self.surprisal_count.entry(start_callsite).or_insert(0.0) += 1.0;
    }

    /// Add an estimated span-start volume under its start callsite.
    pub(crate) fn add_start(&mut self, start_callsite: u64, adjusted_count: f64) {
        *self.start_counts.entry(start_callsite).or_insert(0.0) += adjusted_count;
    }

    /// Build the next window's span-start threshold table from this window's
    /// accumulators, using surprisal value weighting when it is enabled and a
    /// scale is available, and equal coverage otherwise.
    #[must_use]
    pub(crate) fn build_table(
        &self,
        target: f64,
        enable_value_weighting: bool,
        shrinkage: f64,
        fallback_default: Threshold,
    ) -> SpanStartTable {
        let value_weighting = enable_value_weighting && self.mean_surprisal > 0.0;
        build_span_start_table(
            target,
            &self.start_counts,
            |s| {
                if value_weighting {
                    self.value_score(s, shrinkage)
                } else {
                    1.0
                }
            },
            fallback_default,
        )
    }

    /// The shrinkage-adjusted value score `V~_s` for a span-start callsite,
    /// pulling an under-observed kind toward the mean surprisal.
    #[must_use]
    fn value_score(&self, s: u64, shrinkage: f64) -> f64 {
        let sum = self.surprisal_sum.get(&s).copied().unwrap_or(0.0);
        let n = self.surprisal_count.get(&s).copied().unwrap_or(0.0);
        (sum + shrinkage * self.mean_surprisal) / (n + shrinkage)
    }

    /// Recompute per-log-callsite surprisals, the unseen floor, and the mean
    /// surprisal from criterion one's estimated counts, for the next window.
    pub(crate) fn update_log_surprisals(&mut self, estimates: &HashMap<u64, f64>) {
        let total: f64 = estimates.values().sum();
        if total <= 0.0 {
            self.log_surprisals.clear();
            self.log_surprisal_floor = 0.0;
            self.mean_surprisal = 0.0;
            return;
        }
        // The rarest column mass sets the floor for unseen callsites.
        let c_min = estimates
            .values()
            .map(|&nhat| nhat / total)
            .fold(f64::INFINITY, f64::min);
        let floor = -c_min.ln();

        let mut surprisals = HashMap::with_capacity(estimates.len());
        let mut mean = 0.0_f64;
        for (&l, &nhat) in estimates {
            let c_l = nhat / total;
            let g_l = (-c_l.ln()).min(floor);
            mean += c_l * g_l;
            let _ = surprisals.insert(l, g_l);
        }
        self.log_surprisals = surprisals;
        self.log_surprisal_floor = floor;
        self.mean_surprisal = mean;
    }

    /// Clear the per-window accumulators; the surprisal scale carries over.
    pub(crate) fn reset_window(&mut self) {
        self.surprisal_sum.clear();
        self.surprisal_count.clear();
        self.start_counts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::sampler::{SpanKind, evaluate};
    use super::super::super::span::{SpanContext, SpanId, TraceId};

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() <= tol
    }

    #[test]
    fn equal_coverage_throttles_chatty_kinds() {
        // Two span kinds: one chatty (1000 starts), one rare (10 starts). With a
        // flat value score and a target of 20 admitted starts, equal coverage
        // gives each kind ~10 admitted, so the chatty kind's probability is far
        // lower than the rare kind's.
        let mut starts = HashMap::new();
        let _ = starts.insert(1u64, 1000.0);
        let _ = starts.insert(2u64, 10.0);
        let table = build_span_start_table(20.0, &starts, |_| 1.0, Threshold::ALWAYS);

        // P_chatty ~ 10/1000 = 0.01, P_rare ~ 10/10 = 1.0 (clamped).
        let p_chatty = 1.0 / table.threshold_for(1).adjusted_count();
        let p_rare = 1.0 / table.threshold_for(2).adjusted_count();
        assert!(p_chatty < p_rare, "chatty {p_chatty} should be below rare {p_rare}");
        assert!(approx(p_chatty, 0.01, 0.005), "chatty probability {p_chatty}");
        assert!(approx(p_rare, 1.0, 1e-6), "rare probability {p_rare}");
    }

    #[test]
    fn value_weighting_favors_high_value_kinds() {
        // Same volume for both kinds, but kind 2 has a higher value score, so it
        // earns a higher admission probability than kind 1.
        let mut starts = HashMap::new();
        let _ = starts.insert(1u64, 100.0);
        let _ = starts.insert(2u64, 100.0);
        let values: HashMap<u64, f64> =
            [(1u64, 1.0), (2u64, 3.0)].into_iter().collect();
        let table = build_span_start_table(
            50.0,
            &starts,
            |s| values.get(&s).copied().unwrap_or(1.0),
            Threshold::ALWAYS,
        );
        let p1 = 1.0 / table.threshold_for(1).adjusted_count();
        let p2 = 1.0 / table.threshold_for(2).adjusted_count();
        assert!(p2 > p1, "high-value kind {p2} should exceed low-value {p1}");
    }

    #[test]
    fn flat_values_recover_equal_coverage() {
        let mut starts = HashMap::new();
        let _ = starts.insert(1u64, 300.0);
        let _ = starts.insert(2u64, 700.0);
        let equal = build_span_start_table(40.0, &starts, |_| 1.0, Threshold::ALWAYS);
        let weighted = build_span_start_table(40.0, &starts, |_| 5.0, Threshold::ALWAYS);
        // A constant non-zero value score must give the same thresholds.
        assert_eq!(
            equal.threshold_for(1).value(),
            weighted.threshold_for(1).value()
        );
        assert_eq!(
            equal.threshold_for(2).value(),
            weighted.threshold_for(2).value()
        );
    }

    #[test]
    fn sampler_uses_table_for_roots() {
        // A table that never samples kind 1 should drop its root span.
        let mut thresholds = HashMap::new();
        let _ = thresholds.insert(super::super::callsite::span_start_identity("drop.me"), Threshold::NEVER);
        let table: SharedSpanStartTable =
            Arc::new(ArcSwap::from_pointee(SpanStartTable::new(thresholds, Threshold::ALWAYS)));
        let sampler = SpanStartSampler::new(table);

        let dropped = evaluate(
            &sampler,
            None,
            TraceId(u128::MAX),
            SpanId(1),
            "drop.me",
            super::super::callsite::span_start_identity("drop.me"),
            SpanKind::Internal,
        );
        assert!(!dropped.sampled);
        assert!(!dropped.context.locally_sampled);

        // A callsite absent from the table uses the ALWAYS default and is kept.
        let kept = evaluate(
            &sampler,
            None,
            TraceId(u128::MAX),
            SpanId(2),
            "keep.me",
            super::super::callsite::span_start_identity("keep.me"),
            SpanKind::Internal,
        );
        assert!(kept.sampled);
        assert!(kept.context.locally_sampled);
        // The start callsite identity rode onto the context.
        assert_eq!(
            kept.context.start_callsite,
            super::super::callsite::span_start_identity("keep.me")
        );
    }

    #[test]
    fn sampler_honors_parent_threshold_for_children() {
        let table = shared_always();
        let sampler = SpanStartSampler::new(table);
        // Establish a sampled root.
        let root = evaluate(
            &sampler,
            None,
            TraceId(0x00ff_ffff_ffff_ffff_ffff_ffff_ffff_ffff),
            SpanId(1),
            "root",
            super::super::callsite::span_start_identity("root"),
            SpanKind::Internal,
        );
        assert!(root.sampled);
        let parent: SpanContext = root.context;
        let child = evaluate(
            &sampler,
            Some(parent),
            parent.trace_id,
            SpanId(2),
            "child",
            super::super::callsite::span_start_identity("child"),
            SpanKind::Internal,
        );
        assert!(child.sampled, "child should inherit the sampled root decision");
    }
}
