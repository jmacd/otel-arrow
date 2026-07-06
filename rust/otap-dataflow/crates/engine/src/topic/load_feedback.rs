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
use std::sync::mpsc::{Receiver, Sender, channel};

use crate::error::Error;
use crate::topic::handle::TopicHandle;
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

/// Owner-side per-partition load accounting: the producer end of the load
/// feedback loop, the counterpart to [`PlacementCoordinator`].
///
/// An owner updates it as it processes the partition-tagged batches the dispatch
/// delivered -- accumulating `points` per interval and setting the current
/// `active_series` gauge from the aggregator's per-partition series state -- then
/// [`snapshot`](PartitionLoadTracker::snapshot)s it for reporting. Tracking only
/// the partitions an owner holds keeps it sparse.
#[derive(Debug, Clone, Default)]
pub struct PartitionLoadTracker {
    loads: std::collections::BTreeMap<usize, PartitionLoad>,
}

impl PartitionLoadTracker {
    /// Create an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `points` ingested for `partition` during the current interval.
    pub fn add_points(&mut self, partition: usize, points: u64) {
        let entry = self.loads.entry(partition).or_default();
        entry.points = entry.points.saturating_add(points);
    }

    /// Set the current active-series count for `partition` (a gauge read from the
    /// aggregator's per-partition state).
    pub fn set_active_series(&mut self, partition: usize, active_series: u64) {
        self.loads.entry(partition).or_default().active_series = active_series;
    }

    /// Stop tracking `partition` -- called when the owner loses ownership of it
    /// to a reassignment, so its stale load is no longer reported.
    pub fn forget(&mut self, partition: usize) {
        let _ = self.loads.remove(&partition);
    }

    /// Snapshot the per-partition loads for reporting, ascending by partition,
    /// resetting the interval `points` counters while keeping the `active_series`
    /// gauges so the next interval measures a fresh rate.
    pub fn snapshot(&mut self) -> Vec<(usize, PartitionLoad)> {
        let out: Vec<(usize, PartitionLoad)> = self.loads.iter().map(|(&p, &l)| (p, l)).collect();
        for load in self.loads.values_mut() {
            load.points = 0;
        }
        out
    }

    /// The current per-partition loads without resetting, ascending by partition.
    #[must_use]
    pub fn peek(&self) -> Vec<(usize, PartitionLoad)> {
        self.loads.iter().map(|(&p, &l)| (p, l)).collect()
    }

    /// The number of partitions currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.loads.len()
    }

    /// Whether no partition is currently tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.loads.is_empty()
    }
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

    /// The number of owners this coordinator places partitions across.
    #[must_use]
    pub fn num_owners(&self) -> usize {
        self.num_owners.get()
    }

    /// Reconcile the placement to a topic's real `owner_of` routing so the owner
    /// indices this coordinator emits address the topic's actual owner slots,
    /// which are assigned in subscription order and need not match the initial
    /// placement order. A partition with no current owner keeps its existing
    /// assignment; a mismatched partition count is ignored.
    pub fn reconcile(&mut self, owner_of: &[Option<OwnerId>]) {
        if owner_of.len() != self.placement.num_partitions() {
            return;
        }
        let reconciled: Vec<OwnerId> = (0..self.placement.num_partitions())
            .map(|p| owner_of[p].unwrap_or_else(|| self.placement.owner_of(p)))
            .collect();
        self.placement = PartitionPlacement::from_owner_of(reconciled, self.num_owners);
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

/// A cloneable handle owners use to send their [`PartitionLoadTracker`]
/// snapshots to a [`PlacementScheduler`]. Sends are lock-free and non-blocking;
/// a send after the scheduler is gone is silently dropped.
#[derive(Debug, Clone)]
pub struct LoadReportSender {
    tx: Sender<Vec<(usize, PartitionLoad)>>,
}

impl LoadReportSender {
    /// Send one owner's per-partition load snapshot to the scheduler.
    pub fn send(&self, snapshot: Vec<(usize, PartitionLoad)>) {
        let _ = self.tx.send(snapshot);
    }
}

/// Ties the load feedback loop to a live partition-dispatch topic: it collects
/// owner load reports, drives the [`PlacementCoordinator`], and applies the
/// resulting [`PartitionMove`]s to the topic.
///
/// Owners send snapshots through a [`LoadReportSender`]; a scheduling thread
/// calls [`tick`](PlacementScheduler::tick) on a cadence aligned with the
/// aggregation window so a move's handoff carries no live state. The coordinator
/// owner ids must match the topic's owner indices, which hold when owners
/// subscribe in owner order.
pub struct PlacementScheduler<T: Send + Sync + 'static> {
    coordinator: PlacementCoordinator,
    topic: TopicHandle<T>,
    tx: Sender<Vec<(usize, PartitionLoad)>>,
    rx: Receiver<Vec<(usize, PartitionLoad)>>,
}

impl<T: Send + Sync + 'static> PlacementScheduler<T> {
    /// Create a scheduler for `topic`, starting from a count-balanced placement
    /// of `num_partitions` partitions across `num_owners` owners.
    #[must_use]
    pub fn new(
        topic: TopicHandle<T>,
        num_partitions: NonZeroUsize,
        num_owners: NonZeroUsize,
        weights: LoadWeights,
        max_imbalance: f64,
    ) -> Self {
        let (tx, rx) = channel();
        Self {
            coordinator: PlacementCoordinator::new(
                num_partitions,
                num_owners,
                weights,
                max_imbalance,
            ),
            topic,
            tx,
            rx,
        }
    }

    /// A sender owners use to report their load snapshots.
    #[must_use]
    pub fn report_sender(&self) -> LoadReportSender {
        LoadReportSender {
            tx: self.tx.clone(),
        }
    }

    /// Merge a load snapshot directly (without the channel), for manual driving.
    pub fn report(&mut self, reports: &[(usize, PartitionLoad)]) {
        self.coordinator.report(reports);
    }

    /// Run one scheduling cycle: drain pending owner reports, rebalance from the
    /// latest loads, and apply each resulting move to the live topic. Returns the
    /// moves applied.
    ///
    /// # Errors
    ///
    /// Returns the first error from applying a move (for example if the
    /// coordinator and topic disagree on the owner set).
    pub fn tick(&mut self) -> Result<Vec<PartitionMove>, Error> {
        // Reconcile to the topic's real subscriber slots before deciding, so the
        // owner indices in emitted moves address the right subscribers even when
        // subscription order does not match the initial placement order. If not
        // every subscriber has subscribed yet, wait for the full set rather than
        // rebalance a partial one.
        if let Some(routing) = self.topic.subscriber_routing() {
            if routing.num_owners < self.coordinator.num_owners() {
                return Ok(Vec::new());
            }
            self.coordinator.reconcile(&routing.owner_of);
        }
        while let Ok(snapshot) = self.rx.try_recv() {
            self.coordinator.report(&snapshot);
        }
        let moves = self.coordinator.rebalance();
        for mv in &moves {
            self.topic.apply_move(*mv)?;
        }
        Ok(moves)
    }

    /// The coordinator, for inspecting placement and owner loads.
    #[must_use]
    pub fn coordinator(&self) -> &PlacementCoordinator {
        &self.coordinator
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

    #[test]
    fn tracker_accumulates_points_and_sets_active_series() {
        let mut tracker = PartitionLoadTracker::new();
        assert!(tracker.is_empty());
        tracker.add_points(2, 10);
        tracker.add_points(2, 5);
        tracker.set_active_series(2, 100);
        tracker.add_points(7, 3);

        assert_eq!(tracker.len(), 2);
        assert_eq!(
            tracker.peek(),
            vec![
                (
                    2,
                    PartitionLoad {
                        active_series: 100,
                        points: 15
                    }
                ),
                (
                    7,
                    PartitionLoad {
                        active_series: 0,
                        points: 3
                    }
                ),
            ]
        );
    }

    #[test]
    fn snapshot_resets_points_but_keeps_active_series() {
        let mut tracker = PartitionLoadTracker::new();
        tracker.set_active_series(1, 50);
        tracker.add_points(1, 20);

        let first = tracker.snapshot();
        assert_eq!(
            first,
            vec![(
                1,
                PartitionLoad {
                    active_series: 50,
                    points: 20
                }
            )]
        );

        // Next interval: points reset to 0, the active-series gauge persists.
        let second = tracker.snapshot();
        assert_eq!(
            second,
            vec![(
                1,
                PartitionLoad {
                    active_series: 50,
                    points: 0
                }
            )]
        );

        // New points accumulate from zero.
        tracker.add_points(1, 7);
        assert_eq!(
            tracker.peek(),
            vec![(
                1,
                PartitionLoad {
                    active_series: 50,
                    points: 7
                }
            )]
        );
    }

    #[test]
    fn forget_drops_a_partition_on_loss_of_ownership() {
        let mut tracker = PartitionLoadTracker::new();
        tracker.set_active_series(3, 9);
        tracker.set_active_series(4, 9);
        tracker.forget(3);
        assert_eq!(
            tracker.peek().iter().map(|&(p, _)| p).collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[test]
    fn tracker_feeds_the_coordinator() {
        // An owner measures, snapshots, and the coordinator rebalances on it.
        let mut tracker = PartitionLoadTracker::new();
        tracker.set_active_series(0, 50);
        tracker.set_active_series(1, 50);
        for p in 2..8 {
            tracker.set_active_series(p, 2);
        }

        let mut coord = PlacementCoordinator::new(nz(8), nz(4), LoadWeights::default(), 1.2);
        coord.report(&tracker.snapshot());
        let max_before = *coord.owner_loads().iter().max().unwrap();
        let moves = coord.rebalance();
        let max_after = *coord.owner_loads().iter().max().unwrap();
        assert!(!moves.is_empty());
        assert!(max_after < max_before);
    }

    #[test]
    fn reconcile_adopts_the_topics_owner_of_routing() {
        let mut coord = PlacementCoordinator::new(nz(4), nz(2), LoadWeights::default(), 1.2);
        // Default balanced start: owner0 -> {0,1}, owner1 -> {2,3}.
        assert_eq!(coord.placement().owner_of(0), 0);
        assert_eq!(coord.placement().owner_of(2), 1);
        // Adopt a reversed routing: partitions 0,1 -> owner1; 2,3 -> owner0.
        coord.reconcile(&[Some(1), Some(1), Some(0), Some(0)]);
        assert_eq!(coord.placement().owner_of(0), 1);
        assert_eq!(coord.placement().owner_of(2), 0);
        // A partition with no current owner keeps its existing assignment.
        coord.reconcile(&[None, Some(1), Some(0), Some(0)]);
        assert_eq!(coord.placement().owner_of(0), 1);
        // A mismatched partition count is ignored.
        coord.reconcile(&[Some(0)]);
        assert_eq!(coord.placement().owner_of(0), 1);
    }

    #[test]
    fn scheduler_tick_reconciles_to_real_owner_slots() {
        use crate::topic::{
            PartitionDispatchBackend, SubscriberOptions, SubscriptionMode, TopicBroker,
            TopicOptions,
        };

        let broker = TopicBroker::<()>::new();
        let topic = broker
            .create_topic(
                "shuffle",
                TopicOptions::default(),
                PartitionDispatchBackend {
                    num_partitions: 4,
                    capacity: 16,
                },
            )
            .expect("create topic");
        // Subscribe in reversed order: the first subscriber (owner slot 0) owns
        // {2,3}; the second (slot 1) owns {0,1}. So the topic's owner_of is
        // [1,1,0,0], the opposite of balanced(4,2).
        let _s0 = topic
            .subscribe(
                SubscriptionMode::PartitionDispatch {
                    owned_partitions: vec![2, 3],
                },
                SubscriberOptions::default(),
            )
            .expect("owner slot 0");
        let _s1 = topic
            .subscribe(
                SubscriptionMode::PartitionDispatch {
                    owned_partitions: vec![0, 1],
                },
                SubscriberOptions::default(),
            )
            .expect("owner slot 1");

        let mut scheduler =
            PlacementScheduler::new(topic.clone(), nz(4), nz(2), LoadWeights::default(), 1.2);
        // Pile load on partitions 2 and 3, which owner slot 0 holds. Without
        // reconciling to the real routing the coordinator would assume the balanced
        // map (slot 0 owns {0,1}) and shed from the cold owner; reconciling makes it
        // shed from the actually-hot slot 0.
        scheduler.report(&[
            (2, series(50)),
            (3, series(50)),
            (0, series(1)),
            (1, series(1)),
        ]);
        let moves = scheduler.tick().expect("tick");

        assert!(!moves.is_empty(), "skew must produce a move");
        assert!(
            moves.iter().all(|m| m.from == 0 && m.to == 1),
            "sheds from the real hot slot 0 onto the idle slot 1: {moves:?}"
        );
        assert!(
            moves.iter().all(|m| m.partition == 2 || m.partition == 3),
            "moves one of the hot partitions: {moves:?}"
        );
        // The move was applied to the topic, so its routing now reflects it.
        let routing = topic
            .partition_routing()
            .expect("partition-dispatch routing");
        assert_eq!(routing.num_owners, 2);
        assert_eq!(routing.owner_of[moves[0].partition], Some(1));
    }

    #[test]
    fn scheduler_tick_waits_until_all_owners_subscribe() {
        use crate::topic::{
            PartitionDispatchBackend, SubscriberOptions, SubscriptionMode, TopicBroker,
            TopicOptions,
        };

        let broker = TopicBroker::<()>::new();
        let topic = broker
            .create_topic(
                "shuffle",
                TopicOptions::default(),
                PartitionDispatchBackend {
                    num_partitions: 4,
                    capacity: 16,
                },
            )
            .expect("create topic");
        // Only one of the two expected owners has subscribed so far.
        let _s0 = topic
            .subscribe(
                SubscriptionMode::PartitionDispatch {
                    owned_partitions: vec![0, 1],
                },
                SubscriberOptions::default(),
            )
            .expect("owner slot 0");

        let mut scheduler =
            PlacementScheduler::new(topic, nz(4), nz(2), LoadWeights::default(), 1.2);
        scheduler.report(&[(0, series(50)), (1, series(50))]);
        // The scheduler waits for the full owner set rather than rebalance onto an
        // owner slot that does not exist yet.
        assert!(
            scheduler.tick().expect("tick").is_empty(),
            "no moves until every owner has subscribed"
        );
    }
}
