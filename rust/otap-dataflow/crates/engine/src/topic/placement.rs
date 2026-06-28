// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Partition placement: the static `partition -> owner` map of the
//! partition-dispatch design (Layer C, decision D23 in
//! `docs/durable-dispatch-topic-design.md`).
//!
//! The two-level indirection is `key -> partition -> owner`: Layer A
//! (split-by-key) maps a row's key to a stable partition; this map then assigns
//! each partition to an owning subscriber. The indirection bounds churn -- a key
//! never changes partition, so rebalancing moves whole partitions between owners
//! rather than rehashing keys.
//!
//! This effort ships the static [`balanced`](PartitionPlacement::balanced) map
//! plus the load-aware methods that balance CPU load for a metrics gateway over
//! a skewed SDK fleet: [`weighted`](PartitionPlacement::weighted) (greedy LPT),
//! [`rebalance_weighted`](PartitionPlacement::rebalance_weighted)
//! (churn-minimizing), and [`hot_partitions`](PartitionPlacement::hot_partitions)
//! (indivisible-overload detection). The owner-reported load feedback loop, the
//! `partition -> node` form, leases, and durable handoff arrive with the durable
//! backend and the controller's placement plane (ingest-queue Phases 2-4). The
//! map carries no durability and no leases here. See "Load balancing across
//! CPUs" in `docs/durable-dispatch-topic-design.md`.

use std::num::NonZeroUsize;

/// An owner of one or more partitions: a subscriber slot index in the
/// partition-dispatch topic (a core now; a node later).
pub type OwnerId = usize;

/// A static assignment of `num_partitions` partitions to `num_owners` owners.
///
/// Each partition has exactly one owner, so an owner's per-core state is the
/// single writer for every key that hashes into its partitions. The default
/// [`balanced`](PartitionPlacement::balanced) assignment gives each owner a
/// contiguous block of partitions, balanced to within one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacement {
    /// `owner_of[p]` is the owner of partition `p`; length is `num_partitions`.
    owner_of: Vec<OwnerId>,
    /// The number of owners the partitions were assigned across.
    num_owners: usize,
}

impl PartitionPlacement {
    /// Assign `num_partitions` partitions across `num_owners` owners in
    /// contiguous, evenly-sized blocks (`owner(p) = p * num_owners /
    /// num_partitions`). Owner counts differ by at most one. When `num_owners >
    /// num_partitions` the surplus owners are assigned no partitions.
    #[must_use]
    pub fn balanced(num_partitions: NonZeroUsize, num_owners: NonZeroUsize) -> Self {
        let n = num_partitions.get();
        let m = num_owners.get();
        let owner_of = (0..n).map(|p| (p * m) / n).collect();
        Self {
            owner_of,
            num_owners: m,
        }
    }

    /// The owner of `partition`.
    ///
    /// # Panics
    ///
    /// Panics if `partition >= num_partitions`.
    #[must_use]
    pub fn owner_of(&self, partition: usize) -> OwnerId {
        self.owner_of[partition]
    }

    /// The partitions owned by `owner`, in ascending order. Empty if the owner
    /// owns no partitions (possible only when there are more owners than
    /// partitions).
    #[must_use]
    pub fn partitions_of(&self, owner: OwnerId) -> Vec<usize> {
        (0..self.owner_of.len())
            .filter(|&p| self.owner_of[p] == owner)
            .collect()
    }

    /// The number of partitions.
    #[must_use]
    pub fn num_partitions(&self) -> usize {
        self.owner_of.len()
    }

    /// The number of owners the partitions were assigned across.
    #[must_use]
    pub fn num_owners(&self) -> usize {
        self.num_owners
    }

    /// Load-aware assignment of weighted partitions to `num_owners` owners using
    /// greedy **longest-processing-time-first** (LPT): partitions are placed
    /// heaviest-first, each onto the currently-lightest owner.
    ///
    /// `weights[p]` is the load of partition `p` (for a metrics gateway, a scalar
    /// such as `active_series` plus a points-per-second term). The number of
    /// partitions is `weights.len()`. LPT is a 4/3-approximation to the
    /// (NP-hard) minimum-makespan assignment and runs in `O(n log n + n log m)`.
    ///
    /// Use this instead of [`balanced`](PartitionPlacement::balanced) when metric
    /// load is skewed (the common case across an SDK fleet), so balancing
    /// partition *count* would still leave hot cores. A single partition heavier
    /// than a fair share cannot be balanced by placement alone; see
    /// [`hot_partitions`](PartitionPlacement::hot_partitions).
    #[must_use]
    pub fn weighted(weights: &[u64], num_owners: NonZeroUsize) -> Self {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        let m = num_owners.get();
        let n = weights.len();
        let mut owner_of = vec![0usize; n];

        // Partition indices heaviest-first (LPT).
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by(|&a, &b| weights[b].cmp(&weights[a]).then(a.cmp(&b)));

        // Min-heap of (current load, owner); ties break on the lower owner index
        // for determinism.
        let mut heap: BinaryHeap<Reverse<(u64, usize)>> =
            (0..m).map(|o| Reverse((0u64, o))).collect();
        for &p in &order {
            let Reverse((load, owner)) = heap.pop().expect("at least one owner");
            owner_of[p] = owner;
            heap.push(Reverse((load + weights[p], owner)));
        }

        Self {
            owner_of,
            num_owners: m,
        }
    }

    /// The total load on each owner under `weights`, indexed by owner id.
    #[must_use]
    pub fn owner_loads(&self, weights: &[u64]) -> Vec<u64> {
        let mut loads = vec![0u64; self.num_owners];
        for (p, &owner) in self.owner_of.iter().enumerate() {
            loads[owner] += weights.get(p).copied().unwrap_or(0);
        }
        loads
    }

    /// The total load on `owner` under `weights`.
    #[must_use]
    pub fn owner_load(&self, weights: &[u64], owner: OwnerId) -> u64 {
        self.owner_of
            .iter()
            .enumerate()
            .filter(|&(_, &o)| o == owner)
            .map(|(p, _)| weights.get(p).copied().unwrap_or(0))
            .sum()
    }

    /// The `(max, min)` owner load under `weights`, the inputs to an imbalance
    /// check. `(0, 0)` when there are no partitions.
    #[must_use]
    pub fn load_extremes(&self, weights: &[u64]) -> (u64, u64) {
        let loads = self.owner_loads(weights);
        let max = loads.iter().copied().max().unwrap_or(0);
        let min = loads.iter().copied().min().unwrap_or(0);
        (max, min)
    }

    /// Rebalance ownership to bring the hottest owner's load within
    /// `max_imbalance * mean` by moving the fewest partitions from hot owners to
    /// cold ones, returning the number of partitions reassigned (the churn).
    ///
    /// Keys never change partition (the stable hash), so this only edits the
    /// `partition -> owner` map; on a metrics gateway the moved partitions' open
    /// aggregation windows hand off to the new owner (cleanly at a window
    /// boundary). The greedy step always moves a partition that strictly reduces
    /// the maximum owner load, so it converges; an owner left over budget by a
    /// single indivisible partition is reported by
    /// [`hot_partitions`](PartitionPlacement::hot_partitions).
    pub fn rebalance_weighted(&mut self, weights: &[u64], max_imbalance: f64) -> usize {
        let m = self.num_owners;
        if m <= 1 || self.owner_of.is_empty() {
            return 0;
        }
        let mut loads = self.owner_loads(weights);
        let total: u64 = loads.iter().sum();
        let mean = total as f64 / m as f64;
        let budget = (mean * max_imbalance).ceil() as u64;

        let mut moved = 0usize;
        loop {
            let (hot, hot_load) = loads
                .iter()
                .copied()
                .enumerate()
                .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
                .expect("at least one owner");
            if hot_load <= budget {
                break;
            }
            let (cold, cold_load) = loads
                .iter()
                .copied()
                .enumerate()
                .min_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)))
                .expect("at least one owner");
            if cold == hot {
                break;
            }

            // Among `hot`'s partitions, move the one that minimizes the resulting
            // larger of the two owners' loads.
            let best = self
                .owner_of
                .iter()
                .enumerate()
                .filter(|&(p, &o)| o == hot && weights.get(p).copied().unwrap_or(0) > 0)
                .map(|(p, _)| p)
                .min_by_key(|&p| {
                    let w = weights[p];
                    (hot_load - w).max(cold_load + w)
                });

            let Some(p) = best else {
                break;
            };
            let w = weights[p];
            // Only move when it strictly lowers the global maximum, which both
            // guarantees progress (termination) and avoids thrashing.
            if cold_load + w >= hot_load {
                break;
            }
            self.owner_of[p] = cold;
            loads[hot] -= w;
            loads[cold] += w;
            moved += 1;
        }
        moved
    }

    /// Partitions whose individual weight exceeds `per_owner_budget` -- a single
    /// such partition overloads any owner it lands on, so placement alone cannot
    /// balance it. On a metrics gateway these are hot metric names whose keys
    /// should be **sub-partitioned by series identity** (extra radix bits) so the
    /// name's series spread across owners; single-writer is per series, so each
    /// series still has one writer.
    #[must_use]
    pub fn hot_partitions(weights: &[u64], per_owner_budget: u64) -> Vec<usize> {
        weights
            .iter()
            .enumerate()
            .filter(|&(_, &w)| w > per_owner_budget)
            .map(|(p, _)| p)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("non-zero")
    }

    #[test]
    fn every_partition_has_exactly_one_owner() {
        let p = PartitionPlacement::balanced(nz(16), nz(4));
        assert_eq!(p.num_partitions(), 16);
        assert_eq!(p.num_owners(), 4);
        // Each owner's partition sets are disjoint and cover [0, 16).
        let mut covered = vec![false; 16];
        for owner in 0..4 {
            for part in p.partitions_of(owner) {
                assert!(!covered[part], "partition {part} owned twice");
                covered[part] = true;
                assert_eq!(p.owner_of(part), owner, "owner_of disagrees");
            }
        }
        assert!(covered.into_iter().all(|c| c), "every partition covered");
    }

    #[test]
    fn balanced_within_one() {
        let p = PartitionPlacement::balanced(nz(10), nz(3));
        let counts: Vec<usize> = (0..3).map(|o| p.partitions_of(o).len()).collect();
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        assert!(max - min <= 1, "owner counts {counts:?} not balanced");
        assert_eq!(counts.iter().sum::<usize>(), 10);
    }

    #[test]
    fn contiguous_blocks() {
        // N=8, M=3 -> owner0={0,1,2}, owner1={3,4,5}, owner2={6,7}.
        let p = PartitionPlacement::balanced(nz(8), nz(3));
        assert_eq!(p.partitions_of(0), vec![0, 1, 2]);
        assert_eq!(p.partitions_of(1), vec![3, 4, 5]);
        assert_eq!(p.partitions_of(2), vec![6, 7]);
    }

    #[test]
    fn single_owner_owns_all() {
        let p = PartitionPlacement::balanced(nz(8), nz(1));
        assert_eq!(p.partitions_of(0), (0..8).collect::<Vec<_>>());
        for part in 0..8 {
            assert_eq!(p.owner_of(part), 0);
        }
    }

    #[test]
    fn more_owners_than_partitions_leaves_some_empty() {
        let p = PartitionPlacement::balanced(nz(2), nz(3));
        // Partitions 0,1 land on the first owners; surplus owners get nothing.
        let owned: usize = (0..3).map(|o| p.partitions_of(o).len()).sum();
        assert_eq!(owned, 2);
        assert!((0..3).any(|o| p.partitions_of(o).is_empty()));
    }

    // Skewed per-partition load where balancing by count would pile the heavy
    // partitions on one owner. LPT spreads them near-optimally.
    const SKEWED: [u64; 8] = [10, 8, 6, 4, 3, 3, 2, 2];

    #[test]
    fn weighted_lpt_beats_count_balancing_on_skew() {
        let weights = SKEWED;
        let by_count = PartitionPlacement::balanced(nz(weights.len()), nz(3));
        let by_load = PartitionPlacement::weighted(&weights, nz(3));

        let (count_max, _) = by_count.load_extremes(&weights);
        let (load_max, load_min) = by_load.load_extremes(&weights);

        // Count balancing concentrates the heavy partitions (24); LPT does not.
        assert_eq!(count_max, 24);
        // Optimal makespan is max(ceil(38/3)=13, max_weight=10) = 13; LPT hits it.
        assert_eq!(load_max, 13);
        assert!(load_max - load_min <= 1, "LPT loads should be tight");
        assert!(load_max < count_max);
        // Conservation: every partition still has exactly one owner.
        assert_eq!(by_load.owner_loads(&weights).iter().sum::<u64>(), 38);
    }

    #[test]
    fn weighted_respects_lpt_makespan_bound() {
        // Random-ish skew; assert the LPT guarantee max <= 4/3 * OPT (loosely,
        // OPT >= max(mean, max_weight)).
        let weights: Vec<u64> = vec![50, 40, 30, 20, 11, 9, 8, 7, 6, 5, 4, 3];
        let m = 4usize;
        let p = PartitionPlacement::weighted(&weights, nz(m));
        let total: u64 = weights.iter().sum();
        let opt = (total as f64 / m as f64).max(*weights.iter().max().unwrap() as f64);
        let (max, _) = p.load_extremes(&weights);
        assert!(
            (max as f64) <= 4.0 / 3.0 * opt + 1.0,
            "LPT max {max} exceeds 4/3 * OPT ({opt})"
        );
    }

    #[test]
    fn rebalance_recovers_a_bad_count_placement_with_low_churn() {
        let weights = SKEWED;
        let mut p = PartitionPlacement::balanced(nz(weights.len()), nz(3));
        let (before, _) = p.load_extremes(&weights); // 24
        let moved = p.rebalance_weighted(&weights, 1.15);
        let (after, _) = p.load_extremes(&weights);

        assert!(after < before, "rebalance must reduce the hottest owner");
        // mean is 12.67; within the 1.15 budget (<= 15).
        assert!(after <= 15, "after={after} should be within budget");
        // Churn is a few partitions, not a full reshuffle.
        assert!(moved >= 1 && moved < weights.len(), "moved={moved}");
        assert_eq!(p.owner_loads(&weights).iter().sum::<u64>(), 38);
    }

    #[test]
    fn rebalance_is_a_noop_when_already_balanced() {
        let weights = [5u64; 6];
        let mut p = PartitionPlacement::weighted(&weights, nz(3));
        assert_eq!(p.rebalance_weighted(&weights, 1.1), 0);
    }

    #[test]
    fn indivisible_hot_partition_is_detected_and_not_thrashed() {
        // One partition alone exceeds any fair share; placement cannot balance it.
        let weights = [100u64, 5, 5, 5];
        let mut p = PartitionPlacement::weighted(&weights, nz(3));
        let (max_before, _) = p.load_extremes(&weights);
        assert_eq!(max_before, 100);

        // mean is 28.75; budget 1.1 -> ~32. The hot partition (100) is flagged.
        let budget = ((115.0_f64 / 4.0) * 1.1).ceil() as u64;
        let hot = PartitionPlacement::hot_partitions(&weights, budget);
        assert_eq!(hot, vec![0]);

        // Rebalancing cannot help and must not thrash (moving p0 onto a cold
        // owner would not lower the maximum).
        let moved = p.rebalance_weighted(&weights, 1.1);
        let (max_after, _) = p.load_extremes(&weights);
        assert_eq!(max_after, 100, "indivisible hot stays put");
        assert_eq!(moved, 0);
    }

    #[test]
    fn weighted_single_owner_gets_everything() {
        let weights = SKEWED;
        let mut p = PartitionPlacement::weighted(&weights, nz(1));
        assert_eq!(p.owner_load(&weights, 0), 38);
        assert_eq!(p.rebalance_weighted(&weights, 1.1), 0);
    }
}
