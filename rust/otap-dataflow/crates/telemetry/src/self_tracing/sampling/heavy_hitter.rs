// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The global heavy-hitter table, criterion one's second-level feedback.
//!
//! The all-CPU aggregator recurses Bottom-Floor over the per-worker criterion
//! one representatives and, at each window, publishes a bounded table of global
//! inverse-frequency weights. A worker's binding gate reads it wait-free through
//! an [`arc_swap::ArcSwap`] and composes it with its local threshold, so a
//! globally abundant callsite is throttled at every contributor in proportion to
//! the fleet-wide congestion. See the [design][design] for the derivation.
//!
//! The table carries a weight `g_c = 1 / N_c` for the few globally abundant
//! callsites, a single scalar `g_unseen` for everything else, and the global
//! threshold `tau_g`. The binding gate admits an arrival when
//!
//! ```text
//! -ln(u) < min( tau_l * w_local_c , tau_g * g_c )
//! ```
//!
//! so the global score `tau_g * g_c` is the fleet-wide half of the condition.
//! An absent table degrades to local-only criterion one: [`local_only`] returns
//! an infinite `tau_g`, so the global score never binds and the local threshold
//! decides alone.
//!
//! [design]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-logs-traces-reservoir.md

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

/// An immutable, per-window table of global inverse-frequency weights for the
/// heavy-hitter callsites, with a scalar floor for everything else and the
/// global threshold.
#[derive(Clone, Debug)]
pub struct HeavyHitterTable {
    /// The global weight `g_c = 1 / N_c` for each abundant callsite.
    weights: HashMap<u64, f64>,
    /// The most permissive global weight, `max_c g_c`, for callsites absent
    /// from the map.
    g_unseen: f64,
    /// The global threshold `tau_g` the collector settled on this window.
    tau_g: f64,
}

impl HeavyHitterTable {
    /// The local-only fail-safe: an infinite global threshold, so the global
    /// score `tau_g * g_c` never binds and criterion one falls back to the
    /// worker's own local threshold. Used before any window has produced a
    /// table.
    #[must_use]
    pub fn local_only() -> Self {
        Self {
            weights: HashMap::new(),
            g_unseen: 1.0,
            tau_g: f64::INFINITY,
        }
    }

    /// Build a table from criterion one's second-level estimated counts `N_c`
    /// and the global threshold `tau_g`. The weight of an abundant callsite is
    /// `g_c = 1 / N_c`, and the floor for unseen callsites is the most
    /// permissive, `max_c g_c`, so a brand-new callsite is throttled no harder
    /// than the rarest known one.
    #[must_use]
    pub fn from_estimates(estimates: &HashMap<u64, f64>, tau_g: f64) -> Self {
        let mut weights = HashMap::with_capacity(estimates.len());
        let mut g_unseen = 0.0_f64;
        for (&callsite, &n_c) in estimates {
            if n_c > 0.0 {
                let g_c = 1.0 / n_c;
                g_unseen = g_unseen.max(g_c);
                let _ = weights.insert(callsite, g_c);
            }
        }
        if g_unseen <= 0.0 {
            g_unseen = 1.0;
        }
        Self {
            weights,
            g_unseen,
            tau_g,
        }
    }

    /// The global weight `g_c` for a callsite, the unseen floor when absent.
    #[must_use]
    pub fn weight_for(&self, callsite: u64) -> f64 {
        self.weights.get(&callsite).copied().unwrap_or(self.g_unseen)
    }

    /// The global threshold `tau_g`.
    #[must_use]
    pub fn tau(&self) -> f64 {
        self.tau_g
    }

    /// The global binding score `tau_g * g_c` for a callsite, the fleet-wide
    /// half of the binding-gate condition. Infinite under [`local_only`], so it
    /// never binds.
    #[must_use]
    pub fn global_score(&self, callsite: u64) -> f64 {
        self.tau_g * self.weight_for(callsite)
    }

    /// The number of tracked heavy-hitter callsites.
    #[must_use]
    pub fn len(&self) -> usize {
        self.weights.len()
    }

    /// Whether the table has any per-callsite entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }
}

/// A shared, atomically swappable handle to a [`HeavyHitterTable`].
///
/// The aggregator publishes a fresh immutable table per window with a single
/// store; every worker reads it wait-free on its next binding decision.
pub type SharedHeavyHitterTable = Arc<ArcSwap<HeavyHitterTable>>;

/// Create a shared table handle initialized to the local-only fail-safe.
#[must_use]
pub fn shared_local_only() -> SharedHeavyHitterTable {
    Arc::new(ArcSwap::from_pointee(HeavyHitterTable::local_only()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_only_never_binds() {
        let table = HeavyHitterTable::local_only();
        // An infinite global score means the local threshold always wins the
        // `min` in the binding gate.
        assert!(table.global_score(42).is_infinite());
        assert!(table.is_empty());
    }

    #[test]
    fn abundant_callsite_is_throttled_harder_than_rare() {
        // A callsite the fleet produces 1000 of is weighted 1/1000; a callsite
        // seen 10 times is weighted 1/10. With a shared tau_g the abundant one
        // has the smaller global score, so it binds sooner.
        let estimates: HashMap<u64, f64> = [(1u64, 1000.0), (2u64, 10.0)].into_iter().collect();
        let table = HeavyHitterTable::from_estimates(&estimates, 4.0);
        assert!(table.global_score(1) < table.global_score(2));
        // An unseen callsite carries the most permissive weight, so its global
        // score is at least the rarest known callsite's.
        assert!(table.global_score(999) >= table.global_score(2) - 1e-12);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn zero_and_negative_counts_are_skipped() {
        let estimates: HashMap<u64, f64> = [(1u64, 0.0), (2u64, 50.0)].into_iter().collect();
        let table = HeavyHitterTable::from_estimates(&estimates, 1.0);
        // Only the positive-count callsite is tracked.
        assert_eq!(table.len(), 1);
        assert!((table.weight_for(2) - 1.0 / 50.0).abs() < 1e-12);
        // The unseen floor is the most permissive positive weight.
        assert!((table.weight_for(1) - 1.0 / 50.0).abs() < 1e-12);
    }
}
