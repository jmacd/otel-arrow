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
//! This effort ships only the **static** map (D28); load-aware,
//! churn-minimizing rebalancing and the `partition -> node` form arrive with the
//! durable backend and the controller's placement plane (ingest-queue Phases
//! 2-4). The map carries no durability and no leases here.

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
}
