// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Load feedback: the owner-to-controller loop that drives load-aware placement
//! (Layer C of the partition-dispatch design, the "Load balancing across CPUs"
//! section of `docs/durable-dispatch-topic-design.md`).
//!
//! Owners measure the per-partition load of the keys they aggregate -- chiefly
//! active series, plus an ingest-rate term -- and report it. The
//! [`PlacementCoordinator`] merges those reports into per-partition weights,
//! runs the load-aware [`PartitionPlacement`] algorithms, and emits the
//! [`PartitionMove`]s the dispatch layer applies. Moves take effect at an
//! aggregation window boundary so the handoff carries no live state.
//!
//! This is the in-memory feedback brain. Wiring an owner's aggregator to produce
//! reports and a runtime to apply moves to a live partition-dispatch topic are
//! the surrounding steps; the durable backend later adds leases and generation
//! fencing to the handoff.

use std::num::NonZeroUsize;

use crate::topic::placement::{OwnerId, PartitionPlacement};

/// The owner-measured load of one partition over a report interval.
///
/// For a metrics gateway the dominant cost is per-series aggregation state, so
/// `active_series` is the primary term; `points` captures per-point ingest cost.
/// An owner buckets these by the partition tag the dispatch delivered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionLoad {
    /// Distinct series currently held in aggregation state for this partition.
    pub active_series: u64,
    /// Points ingested for this partition over the report interval.
    pub points: u64,
}

/// Coefficients turning a [`PartitionLoad`] into a scalar placement weight.
///
/// The default weights one active series and one point equally; a deployment
/// that finds series-state cost dominant raises `per_series`, and an
/// exponential-histogram-heavy workload can raise `per_point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadWeights {
    /// Weight applied per active series.
    pub per_series: u64,
    /// Weight applied per ingested point.
    pub per_point: u64,
}

impl Default for LoadWeights {
    fn default() -> Self {
        Self {
            per_series: 1,
            per_point: 1,
        }
    }
}

impl PartitionLoad {
    /// The scalar placement weight of this load under `weights` (saturating).
    #[must_use]
    pub fn weight(&self, weights: LoadWeights) -> u64 {
        self.active_series
            .saturating_mul(weights.per_series)
            .saturating_add(self.points.saturating_mul(weights.per_point))
    }
}

/// A reassignment the dispatch layer should apply: move `partition` from owner
/// `from` to owner `to`. In memory this takes effect at an aggregation window
/// boundary, so the old owner flushes the closed window and the new owner opens
/// the next one with no state migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionMove {
    /// The partition being reassigned.
    pub partition: usize,
    /// The owner relinquishing the partition.
    pub from: OwnerId,
    /// The owner adopting the partition.
    pub to: OwnerId,
}

/// Drives load-aware placement for one partition-dispatch topic from
/// owner-reported per-partition load.
///
/// It starts count-balanced (no load is known yet), accumulates reports, and on
/// demand recomputes ownership and returns the [`PartitionMove`]s to apply. Keys
/// never change partition; only the `partition -> owner` map moves.
#[derive(Debug, Clone)]
pub struct PlacementCoordinator {
    placement: PartitionPlacement,
    num_owners: NonZeroUsize,
    loads: Vec<PartitionLoad>,
    weights: LoadWeights,
    max_imbalance: f64,
}

impl PlacementCoordinator {
    /// Create a coordinator for `num_partitions` partitions across `num_owners`
    /// owners, starting from a count-balanced placement. `max_imbalance` is the
    /// allowed ratio of the busiest owner's load to the mean before a rebalance
    /// moves partitions (for example `1.1` for 10%).
    #[must_use]
    pub fn new(
        num_partitions: NonZeroUsize,
        num_owners: NonZeroUsize,
        weights: LoadWeights,
        max_imbalance: f64,
    ) -> Self {
        Self {
            placement: PartitionPlacement::balanced(num_partitions, num_owners),
            num_owners,
            loads: vec![PartitionLoad::default(); num_partitions.get()],
            weights,
            max_imbalance,
        }
    }

    /// Merge a batch of per-partition load reports from owners. The latest report
    /// for a partition replaces the previous one; out-of-range partitions are
    /// ignored.
    pub fn report(&mut self, reports: &[(usize, PartitionLoad)]) {
        for &(partition, load) in reports {
            if let Some(slot) = self.loads.get_mut(partition) {
                *slot = load;
            }
        }
    }

    /// The current per-partition scalar weights.
    fn weight_vec(&self) -> Vec<u64> {
        self.loads.iter().map(|l| l.weight(self.weights)).collect()
    }

    /// Churn-minimizing rebalance of the current placement from the latest loads,
    /// returning the moves to apply. Prefer this in steady state: it shifts the
    /// fewest partitions to bring the busiest owner within budget.
    pub fn rebalance(&mut self) -> Vec<PartitionMove> {
        let weights = self.weight_vec();
        let before = self.owners_snapshot();
        let _ = self
            .placement
            .rebalance_weighted(&weights, self.max_imbalance);
        self.diff(&before)
    }

    /// Full LPT recompute of the placement from the latest loads, returning the
    /// moves to apply. Use sparingly (for example on a large owner-count change):
    /// it finds a tighter assignment but can move many partitions.
    pub fn replan(&mut self) -> Vec<PartitionMove> {
        let weights = self.weight_vec();
        let before = self.owners_snapshot();
        self.placement = PartitionPlacement::weighted(&weights, self.num_owners);
        self.diff(&before)
    }

    /// The current placement.
    #[must_use]
    pub fn placement(&self) -> &PartitionPlacement {
        &self.placement
    }

    /// The current load on each owner, indexed by owner id.
    #[must_use]
    pub fn owner_loads(&self) -> Vec<u64> {
        self.placement.owner_loads(&self.weight_vec())
    }

    /// Partitions whose individual weight exceeds a fair owner budget
    /// (`max_imbalance * mean`); a single such partition overloads any owner, so
    /// it needs key sub-partitioning by series identity rather than a move.
    #[must_use]
    pub fn hot_partitions(&self) -> Vec<usize> {
        let weights = self.weight_vec();
        let total: u64 = weights.iter().sum();
        let mean = total as f64 / self.num_owners.get() as f64;
        let budget = (mean * self.max_imbalance).ceil() as u64;
        PartitionPlacement::hot_partitions(&weights, budget)
    }

    fn owners_snapshot(&self) -> Vec<OwnerId> {
        (0..self.placement.num_partitions())
            .map(|p| self.placement.owner_of(p))
            .collect()
    }

    fn diff(&self, before: &[OwnerId]) -> Vec<PartitionMove> {
        (0..self.placement.num_partitions())
            .filter_map(|p| {
                let to = self.placement.owner_of(p);
                let from = before[p];
                (to != from).then_some(PartitionMove {
                    partition: p,
                    from,
                    to,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::placement::PartitionPlacement;
    use std::num::NonZeroUsize;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("non-zero")
    }

    fn series(n: u64) -> PartitionLoad {
        PartitionLoad {
            active_series: n,
            points: 0,
        }
    }

    #[test]
    fn weight_combines_series_and_points() {
        let load = PartitionLoad {
            active_series: 10,
            points: 100,
        };
        assert_eq!(load.weight(LoadWeights::default()), 110);
        assert_eq!(
            load.weight(LoadWeights {
                per_series: 5,
                per_point: 1
            }),
            150
        );
    }

    #[test]
    fn starts_count_balanced_until_load_is_reported() {
        let coord = PlacementCoordinator::new(nz(8), nz(4), LoadWeights::default(), 1.1);
        // No load yet: weights are all zero, owners evenly hold two partitions.
        for owner in 0..4 {
            assert_eq!(coord.placement().partitions_of(owner).len(), 2);
        }
    }

    #[test]
    fn skewed_reports_trigger_a_rebalance_that_reduces_imbalance() {
        // 8 partitions, 4 owners, count-balanced start: owner0={0,1}, etc.
        let mut coord = PlacementCoordinator::new(nz(8), nz(4), LoadWeights::default(), 1.2);
        // Pile load on partitions 0 and 1, both owned by owner0.
        coord.report(&[
            (0, series(50)),
            (1, series(50)),
            (2, series(2)),
            (3, series(2)),
            (4, series(2)),
            (5, series(2)),
            (6, series(2)),
            (7, series(2)),
        ]);

        let max_before = *coord.owner_loads().iter().max().unwrap();
        let moves = coord.rebalance();
        let max_after = *coord.owner_loads().iter().max().unwrap();

        assert!(!moves.is_empty(), "skew must produce moves");
        assert!(
            max_after < max_before,
            "rebalance must lower the busiest owner"
        );
        // The two 50-weight partitions cannot share an owner and each exceeds a
        // fair share, so the achievable optimum places one per owner: max == 50.
        assert_eq!(max_after, 50, "rebalance reaches the indivisible optimum");
        assert!(
            moves.iter().any(|m| m.from == 0),
            "the hot owner sheds load"
        );
        assert_diff_reconstructs_placement(&coord, &moves, nz(8), nz(4));
    }

    #[test]
    fn replan_uses_lpt_and_emits_moves() {
        let mut coord = PlacementCoordinator::new(nz(8), nz(3), LoadWeights::default(), 1.1);
        coord.report(&[
            (0, series(10)),
            (1, series(8)),
            (2, series(6)),
            (3, series(4)),
            (4, series(3)),
            (5, series(3)),
            (6, series(2)),
            (7, series(2)),
        ]);
        let moves = coord.replan();
        // LPT reaches the optimal makespan of 13 (max(ceil(38/3), 10)).
        assert_eq!(*coord.owner_loads().iter().max().unwrap(), 13);
        assert_diff_reconstructs_placement(&coord, &moves, nz(8), nz(3));
    }

    /// Verify the emitted moves are exactly the difference from a count-balanced
    /// base: applying each move to the base lands every partition on the
    /// coordinator's owner, and no partition is reported moved unnecessarily.
    fn assert_diff_reconstructs_placement(
        coord: &PlacementCoordinator,
        moves: &[PartitionMove],
        num_partitions: NonZeroUsize,
        num_owners: NonZeroUsize,
    ) {
        let base = PartitionPlacement::balanced(num_partitions, num_owners);
        for p in 0..num_partitions.get() {
            let target = coord.placement().owner_of(p);
            match moves.iter().find(|m| m.partition == p) {
                Some(m) => {
                    assert_eq!(m.from, base.owner_of(p), "move.from must be the base owner");
                    assert_eq!(m.to, target, "move.to must be the new owner");
                    assert_ne!(m.from, m.to, "a move must change owner");
                }
                None => assert_eq!(base.owner_of(p), target, "unmoved partition unchanged"),
            }
        }
    }

    #[test]
    fn rebalance_is_a_noop_for_uniform_load() {
        let mut coord = PlacementCoordinator::new(nz(6), nz(3), LoadWeights::default(), 1.1);
        coord.report(&(0..6).map(|p| (p, series(5))).collect::<Vec<_>>());
        // weighted/balanced start is already even; rebalance yields no moves.
        let _ = coord.replan(); // normalize to LPT even split
        assert!(coord.rebalance().is_empty());
    }

    #[test]
    fn surfaces_indivisible_hot_partition() {
        let mut coord = PlacementCoordinator::new(nz(4), nz(3), LoadWeights::default(), 1.1);
        coord.report(&[
            (0, series(100)),
            (1, series(5)),
            (2, series(5)),
            (3, series(5)),
        ]);
        let _ = coord.replan();
        assert_eq!(coord.hot_partitions(), vec![0]);
    }

    #[test]
    fn report_ignores_out_of_range_partition() {
        let mut coord = PlacementCoordinator::new(nz(2), nz(2), LoadWeights::default(), 1.1);
        coord.report(&[(0, series(3)), (99, series(1000))]);
        assert_eq!(coord.owner_loads().iter().sum::<u64>(), 3);
    }
}
