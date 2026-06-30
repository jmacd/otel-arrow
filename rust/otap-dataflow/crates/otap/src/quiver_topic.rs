// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Durable, quiver-backed partition-dispatch topic (durable-dispatch Layer B).
//!
//! This is the durable form of the in-memory `PartitionDispatchTopic`
//! (`crates/engine/src/topic/topic.rs`). It implements the *same*
//! partition-dispatch / exclusive-ownership semantics behind the same
//! [`TopicState`] interface, but persists each partition's stream so published
//! data survives process restart and subscribers resume from durable progress
//! (durable-dispatch D21/D24/D27).
//!
//! # Granularity
//!
//! One [`QuiverEngine`] per partition (durable-dispatch D24's simpler
//! "one quiver per partition" option): clean isolation and a trivial handoff on
//! reassignment, at the cost of one WAL per partition. Each partition's engine
//! lives under `{data_dir}/{topic}/partition_{p}` and is drained by a single
//! fixed subscriber id, so *which owner drains a partition* (the `owner_of`
//! map) can change without disturbing durable progress.
//!
//! # Ownership and reassignment
//!
//! The topic tracks `partition -> owner`. A subscription backend (an owner)
//! drains partition `p` iff `owner_of[p]` equals its owner index, re-evaluated
//! on every poll. A [`reassign_partition`](TopicState::reassign_partition) just
//! repoints `owner_of[p]`: the previous owner stops polling `p` and the new
//! owner starts, resuming from the partition's durable progress. Applied at an
//! aggregation-window boundary (durable-dispatch Layer C), the handoff carries
//! no overlapping per-series state.
//!
//! # Durability lifecycle (PoC)
//!
//! - **Publish** ingests the OTAP batch into the partition's quiver and flushes,
//!   so the bundle is durable and immediately pollable before the publish
//!   resolves. Flush-per-publish keeps latency low and the prototype simple; a
//!   production path would batch flushes.
//! - **Receive** claims the next durable bundle for an owned partition and
//!   reconstructs `OtapPdata` (re-stamping the partition tag).
//! - **Commit** acks the bundle, advancing durable progress so it is not
//!   redelivered. **Abort / abandon** defers it for redelivery.
//! - **Restart** reopens the per-partition engines; un-acked bundles replay
//!   (open -> register -> activate picks up the on-disk segments).

use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use parking_lot::Mutex;

use quiver::{
    BundleHandle, DiskBudget, QuiverConfig, QuiverEngine, RegistryCallback, RetentionPolicy,
    SegmentStore, SubscriberId,
};

use otap_df_engine::error::Error;
use otap_df_engine::topic::{
    Delivery, DeliveryBackend, DurableDispatchConfig, DurableDispatchPayload, DurableRetentionPolicy,
    Envelope, PublishFuture, PublishOutcome, PublishTrackedFuture, RecvDelivery, SubscriberOptions,
    SubscriptionBackend, SubscriptionGroupName, TopicBroadcastOnLagPolicy, TopicName, TopicState,
    TrackedPublishOutcome, TrackedPublishPermit, TrackedPublishTracker, TrackedTryPublishOutcome,
};
use otap_df_pdata::OtapPayload;
use otap_df_telemetry::{otel_debug, otel_warn};

use crate::pdata::OtapPdata;
use crate::quiver_bundle::{OtapRecordBundleAdapter, OtlpBytesAdapter, convert_bundle_to_pdata};

/// Fixed subscriber id used to drain every partition engine. Progress is tied to
/// the partition (not the owner), so a reassigned partition resumes where the
/// previous owner left off.
const DISPATCH_SUBSCRIBER_ID: &str = "partition-dispatch";

/// Per-partition segment target size. Small so the per-partition disk-budget
/// minimum stays modest; flush-per-publish makes the size trigger irrelevant.
const SEGMENT_TARGET_BYTES: u64 = 1024 * 1024;
/// Per-partition WAL cap.
const WAL_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Per-partition WAL rotation target (must not exceed [`WAL_MAX_BYTES`]).
const WAL_ROTATION_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// Maps a quiver error into the engine's durable-backend error.
fn durable_err(message: impl std::fmt::Display) -> Error {
    Error::DurableBackendError {
        message: message.to_string(),
    }
}

/// A small set of wakers, registered by polling owners and woken when a publish
/// makes new data available. Mirrors the in-memory topic's `WakerSet`.
#[derive(Default)]
struct WakerSet {
    has_waiters: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl WakerSet {
    fn register(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock();
        for existing in wakers.iter_mut() {
            if existing.will_wake(waker) {
                existing.clone_from(waker);
                self.has_waiters.store(true, Ordering::Release);
                return;
            }
        }
        wakers.push(waker.clone());
        self.has_waiters.store(true, Ordering::Release);
    }

    fn wake_all(&self) {
        if !self.has_waiters.load(Ordering::Acquire) {
            return;
        }
        let drained = {
            let mut guard = self.wakers.lock();
            self.has_waiters.store(false, Ordering::Release);
            std::mem::take(&mut *guard)
        };
        for waker in drained {
            waker.wake();
        }
    }
}

/// Mutable `partition -> owner` routing, built as subscribers claim partitions.
struct Routing {
    /// The owner index assigned to the next subscriber.
    next_owner_idx: usize,
    /// `owner_of[p]` is the owner index currently draining partition `p`, or
    /// `None` while unclaimed. Length is `num_partitions`.
    owner_of: Vec<Option<usize>>,
}

/// Shared topic state. Held by the topic and cloned into each subscription so
/// owners can route, drain, and observe reassignment through one source of truth.
struct Shared {
    name: TopicName,
    num_partitions: usize,
    /// One durable engine per partition; index is the partition.
    engines: Vec<Arc<QuiverEngine>>,
    subscriber_id: SubscriberId,
    routing: Mutex<Routing>,
    wakers: WakerSet,
    next_id: AtomicU64,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
}

impl Shared {
    /// Partitions currently owned by `owner_idx`.
    fn owned_partitions(&self, owner_idx: usize) -> Vec<usize> {
        let routing = self.routing.lock();
        routing
            .owner_of
            .iter()
            .enumerate()
            .filter_map(|(p, owner)| (*owner == Some(owner_idx)).then_some(p))
            .collect()
    }

    /// Ingest one OTAP payload into partition `p`'s durable engine and flush so
    /// it is durable and immediately pollable.
    async fn ingest_and_flush(&self, p: usize, pdata: OtapPdata) -> Result<(), Error> {
        let engine = &self.engines[p];
        let (_context, payload) = pdata.into_parts();
        match payload {
            OtapPayload::OtapArrowRecords(records) => {
                let adapter = OtapRecordBundleAdapter::new(records);
                engine.ingest(&adapter).await.map_err(durable_err)?;
            }
            OtapPayload::OtlpBytes(bytes) => {
                let adapter = OtlpBytesAdapter::new(bytes).map_err(|(e, _)| durable_err(e))?;
                engine.ingest(&adapter).await.map_err(durable_err)?;
            }
        }
        engine.flush().await.map_err(durable_err)?;
        Ok(())
    }
}

/// Build the quiver config for one partition engine: small WAL and segment
/// sizes so the per-partition disk-budget minimum is modest.
fn partition_quiver_config() -> QuiverConfig {
    let mut config = QuiverConfig::default();
    config.segment.target_size_bytes =
        NonZeroU64::new(SEGMENT_TARGET_BYTES).expect("non-zero segment target");
    config.segment.max_open_duration = Duration::from_secs(1);
    config.wal.max_size_bytes = NonZeroU64::new(WAL_MAX_BYTES).expect("non-zero wal max");
    config.wal.rotation_target_bytes =
        NonZeroU64::new(WAL_ROTATION_TARGET_BYTES).expect("non-zero wal rotation");
    config
}

/// The per-partition disk-budget hard cap: an even split of the topic's total
/// budget, floored at the minimum a single engine requires so construction
/// never fails on a too-small budget.
fn per_partition_cap(total_budget_bytes: u64, num_partitions: usize, config: &QuiverConfig) -> u64 {
    let minimum = DiskBudget::minimum_hard_cap(
        config.segment.target_size_bytes.get(),
        DiskBudget::effective_wal_size(config),
    );
    let even_split = total_budget_bytes / num_partitions as u64;
    even_split.max(minimum)
}

/// Run a future to completion on a fresh thread with its own current-thread
/// runtime. Used to open the durable engines during the synchronous topic
/// construction path, regardless of whether the caller is already inside a
/// tokio runtime (a fresh thread has no ambient runtime, so `block_on` is safe).
fn block_on_init<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime for durable topic init")
            .block_on(fut)
    })
    .join()
    .expect("durable topic init thread panicked")
}

/// A durable partition-dispatch topic: each published message is persisted to
/// the single partition's quiver and delivered to the one subscriber that owns
/// that partition.
pub struct QuiverPartitionDispatchTopic {
    shared: Arc<Shared>,
}

impl QuiverPartitionDispatchTopic {
    /// Open (or reopen) a durable partition-dispatch topic. Reopening the same
    /// `data_dir` recovers persisted segments, so un-acked bundles from a prior
    /// run are redelivered.
    pub fn open(
        name: TopicName,
        config: DurableDispatchConfig,
    ) -> Result<Arc<dyn TopicState<OtapPdata>>, Error> {
        let DurableDispatchConfig {
            data_dir,
            num_partitions,
            capacity: _capacity,
            disk_budget_bytes,
            retention,
        } = config;

        if num_partitions == 0 {
            return Err(durable_err("durable partition-dispatch requires num_partitions >= 1"));
        }

        let subscriber_id = SubscriberId::new(DISPATCH_SUBSCRIBER_ID)
            .map_err(|e| durable_err(format!("invalid subscriber id: {e}")))?;
        let policy = match retention {
            DurableRetentionPolicy::Backpressure => RetentionPolicy::Backpressure,
            DurableRetentionPolicy::DropOldest => RetentionPolicy::DropOldest,
        };
        let topic_dir = data_dir.join(name.as_ref());

        let engines = block_on_init({
            let subscriber_id = subscriber_id.clone();
            async move {
                let mut engines = Vec::with_capacity(num_partitions);
                for p in 0..num_partitions {
                    let part_dir = topic_dir.join(format!("partition_{p}"));
                    let quiver_config = partition_quiver_config().with_data_dir(&part_dir);
                    let cap = per_partition_cap(disk_budget_bytes, num_partitions, &quiver_config);
                    let budget = Arc::new(
                        DiskBudget::for_config(cap, &quiver_config, policy)
                            .map_err(|e| durable_err(format!("invalid disk budget: {e}")))?,
                    );
                    let engine = QuiverEngine::builder(quiver_config)
                        .with_budget(budget)
                        .build()
                        .await
                        .map_err(|e| durable_err(format!("failed to open partition engine: {e}")))?;
                    engine
                        .register_subscriber(subscriber_id.clone())
                        .map_err(|e| durable_err(format!("failed to register subscriber: {e}")))?;
                    engine
                        .activate_subscriber(&subscriber_id)
                        .map_err(|e| durable_err(format!("failed to activate subscriber: {e}")))?;
                    engines.push(engine);
                }
                Ok::<_, Error>(engines)
            }
        })?;

        let shared = Arc::new(Shared {
            name,
            num_partitions,
            engines,
            subscriber_id,
            routing: Mutex::new(Routing {
                next_owner_idx: 0,
                owner_of: vec![None; num_partitions],
            }),
            wakers: WakerSet::default(),
            next_id: AtomicU64::new(1),
            outcomes: TrackedPublishTracker::new(),
            closed: AtomicBool::new(false),
        });

        Ok(Arc::new(Self { shared }))
    }

    /// The destination partition for a message, or `None` when it is untagged or
    /// out of range (such a message has no durable home and is dropped, matching
    /// the in-memory partition-dispatch topic).
    fn route(shared: &Shared, msg: &OtapPdata) -> Option<usize> {
        let partition = msg.partition()? as usize;
        (partition < shared.num_partitions).then_some(partition)
    }
}

impl TopicState<OtapPdata> for QuiverPartitionDispatchTopic {
    fn name(&self) -> &TopicName {
        &self.shared.name
    }

    fn publish(&self, msg: Arc<OtapPdata>) -> PublishFuture<'_> {
        let shared = self.shared.clone();
        Box::pin(async move {
            if shared.closed.load(Ordering::Relaxed) {
                return Err(Error::TopicClosed);
            }
            if let Some(partition) = Self::route(&shared, &msg) {
                shared.ingest_and_flush(partition, (*msg).clone()).await?;
                shared.wakers.wake_all();
            }
            Ok(())
        })
    }

    fn publish_tracked(
        &self,
        msg: Arc<OtapPdata>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> PublishTrackedFuture<'_> {
        let shared = self.shared.clone();
        Box::pin(async move {
            if shared.closed.load(Ordering::Relaxed) {
                return Err(Error::TopicClosed);
            }
            let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
            let receipt = shared.outcomes.register(id, timeout, permit);
            match Self::route(&shared, &msg) {
                Some(partition) => match shared.ingest_and_flush(partition, (*msg).clone()).await {
                    Ok(()) => {
                        shared.wakers.wake_all();
                        // Durability is the acknowledgement: once persisted, the
                        // tracked publish has succeeded.
                        let _ = shared.outcomes.resolve(id, TrackedPublishOutcome::Ack);
                    }
                    Err(e) => {
                        let _ = shared.outcomes.resolve(
                            id,
                            TrackedPublishOutcome::Nack {
                                reason: Arc::from(e.to_string().as_str()),
                            },
                        );
                    }
                },
                None => {
                    // No durable home; resolve as acked so the publisher is not
                    // left waiting (mirrors the in-memory drop-on-no-owner path).
                    let _ = shared.outcomes.resolve(id, TrackedPublishOutcome::Ack);
                }
            }
            Ok(receipt)
        })
    }

    fn try_publish(&self, msg: Arc<OtapPdata>) -> Result<PublishOutcome, Error> {
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(Error::TopicClosed);
        }
        if let Some(partition) = Self::route(&self.shared, &msg) {
            let shared = self.shared.clone();
            let pdata = (*msg).clone();
            // The durable write is asynchronous; submit it without blocking the
            // caller. Errors are surfaced through telemetry rather than the
            // synchronous return (PoC: try_publish is best-effort durable).
            spawn_durable_ingest(shared, partition, pdata, None);
        }
        Ok(PublishOutcome::Published)
    }

    fn try_publish_tracked(
        &self,
        msg: Arc<OtapPdata>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(Error::TopicClosed);
        }
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let receipt = self.shared.outcomes.register(id, timeout, permit);
        match Self::route(&self.shared, &msg) {
            Some(partition) => {
                let shared = self.shared.clone();
                let pdata = (*msg).clone();
                spawn_durable_ingest(shared, partition, pdata, Some(id));
            }
            None => {
                let _ = self.shared.outcomes.resolve(id, TrackedPublishOutcome::Ack);
            }
        }
        Ok(TrackedTryPublishOutcome::Published(receipt))
    }

    fn subscribe_balanced(
        &self,
        _group: SubscriptionGroupName,
        _opts: SubscriberOptions,
    ) -> Result<Box<dyn SubscriptionBackend<OtapPdata>>, Error> {
        Err(Error::SubscribeBalancedNotSupported)
    }

    fn subscribe_broadcast(
        &self,
        _opts: SubscriberOptions,
    ) -> Result<Box<dyn SubscriptionBackend<OtapPdata>>, Error> {
        Err(Error::SubscribeBroadcastNotSupported)
    }

    fn subscribe_partition_dispatch(
        &self,
        owned_partitions: Vec<u32>,
        _opts: SubscriberOptions,
    ) -> Result<Box<dyn SubscriptionBackend<OtapPdata>>, Error> {
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(Error::TopicClosed);
        }
        let mut routing = self.shared.routing.lock();
        // Validate the whole claim before mutating, so a rejected subscription
        // leaves no partial ownership.
        for &partition in &owned_partitions {
            if partition as usize >= self.shared.num_partitions {
                return Err(Error::PartitionOutOfRange {
                    partition,
                    num_partitions: self.shared.num_partitions,
                });
            }
            if routing.owner_of[partition as usize].is_some() {
                return Err(Error::PartitionAlreadyClaimed { partition });
            }
        }

        let owner_idx = routing.next_owner_idx;
        routing.next_owner_idx += 1;
        for &partition in &owned_partitions {
            routing.owner_of[partition as usize] = Some(owner_idx);
        }
        drop(routing);

        Ok(Box::new(QuiverDispatchSub {
            shared: self.shared.clone(),
            owner_idx,
            scan_cursor: 0,
        }))
    }

    fn reassign_partition(&self, partition: usize, to_owner: usize) -> Result<(), Error> {
        if self.shared.closed.load(Ordering::Relaxed) {
            return Err(Error::TopicClosed);
        }
        if partition >= self.shared.num_partitions {
            return Err(Error::PartitionOutOfRange {
                partition: partition as u32,
                num_partitions: self.shared.num_partitions,
            });
        }
        let mut routing = self.shared.routing.lock();
        if to_owner >= routing.next_owner_idx {
            return Err(Error::OwnerOutOfRange {
                owner: to_owner,
                num_owners: routing.next_owner_idx,
            });
        }
        // Repoint future drains of this partition to the new owner. The
        // partition's durable progress is unchanged, so the new owner resumes
        // exactly where the previous one stopped.
        routing.owner_of[partition] = Some(to_owner);
        drop(routing);
        // Wake owners so the new owner re-scans and picks up the partition.
        self.shared.wakers.wake_all();
        Ok(())
    }

    fn broadcast_on_lag_policy(&self) -> TopicBroadcastOnLagPolicy {
        TopicBroadcastOnLagPolicy::DropOldest
    }

    fn close(&self) {
        self.shared.closed.store(true, Ordering::Relaxed);
        self.shared.outcomes.close_all();
        self.shared.wakers.wake_all();
    }
}

/// Submit a durable ingest+flush for `pdata` into partition `p` without blocking
/// the caller. Resolves a tracked-publish outcome when `tracked_id` is set.
fn spawn_durable_ingest(
    shared: Arc<Shared>,
    partition: usize,
    pdata: OtapPdata,
    tracked_id: Option<u64>,
) {
    let task = async move {
        match shared.ingest_and_flush(partition, pdata).await {
            Ok(()) => {
                shared.wakers.wake_all();
                if let Some(id) = tracked_id {
                    let _ = shared.outcomes.resolve(id, TrackedPublishOutcome::Ack);
                }
            }
            Err(e) => {
                otel_warn!(
                    "quiver_topic.try_publish.failed",
                    error = e.to_string(),
                    message = "durable ingest dropped a message submitted via try_publish"
                );
                if let Some(id) = tracked_id {
                    let _ = shared.outcomes.resolve(
                        id,
                        TrackedPublishOutcome::Nack {
                            reason: Arc::from(e.to_string().as_str()),
                        },
                    );
                }
            }
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            drop(handle.spawn(task));
        }
        Err(_) => {
            // No ambient runtime (rare for try_publish): fall back to a blocking
            // bridge so the durable write still happens.
            block_on_init(task);
        }
    }
}

/// A partition-dispatch subscription over the durable backend. Drains the
/// partitions it currently owns (per `owner_of`) from their per-partition
/// engines; reassignment is observed because ownership is re-read each poll.
struct QuiverDispatchSub {
    shared: Arc<Shared>,
    owner_idx: usize,
    /// Round-robin cursor across owned partitions, for fairness.
    scan_cursor: usize,
}

impl SubscriptionBackend<OtapPdata> for QuiverDispatchSub {
    fn poll_recv_delivery(
        &mut self,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<RecvDelivery<OtapPdata>, Error>> {
        // Register before scanning so a publish that races with this poll still
        // wakes us rather than being lost.
        self.shared.wakers.register(cx.waker());

        let owned = self.shared.owned_partitions(self.owner_idx);
        if !owned.is_empty() {
            for k in 0..owned.len() {
                let idx = (self.scan_cursor + k) % owned.len();
                let partition = owned[idx];
                match self.shared.engines[partition].poll_next_bundle(&self.shared.subscriber_id) {
                    Ok(Some(handle)) => {
                        self.scan_cursor = (idx + 1) % owned.len();
                        match convert_bundle_to_pdata(handle.data()) {
                            Ok(mut pdata) => {
                                pdata.set_partition(partition as u32);
                                let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
                                let envelope = Envelope {
                                    id,
                                    tracked: false,
                                    payload: Arc::new(pdata),
                                };
                                let backend = QuiverDelivery {
                                    envelope,
                                    handle: Some(handle),
                                };
                                return Poll::Ready(Ok(RecvDelivery::Message(
                                    Delivery::new_opaque(Box::new(backend)),
                                )));
                            }
                            Err(e) => {
                                // A bundle we cannot reconstruct is poison;
                                // reject it so it does not redeliver forever.
                                handle.reject();
                                otel_warn!(
                                    "quiver_topic.bundle.unreconstructable",
                                    error = e.to_string(),
                                    partition = partition as u64,
                                    message = "rejected a durable bundle that failed to reconstruct"
                                );
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        otel_debug!(
                            "quiver_topic.poll.error",
                            error = e.to_string(),
                            partition = partition as u64
                        );
                    }
                }
            }
        }

        if self.shared.closed.load(Ordering::Relaxed) {
            Poll::Ready(Err(Error::SubscriptionClosed))
        } else {
            Poll::Pending
        }
    }

    fn ack(&self, id: u64) -> Result<(), Error> {
        // Durable progress is driven by the delivery's commit/abort; this resolves
        // only tracked-publish receipts (untracked ids report MessageNotTracked,
        // matching the in-memory backend).
        if self.shared.outcomes.resolve(id, TrackedPublishOutcome::Ack) {
            Ok(())
        } else {
            Err(Error::MessageNotTracked)
        }
    }

    fn nack(&self, id: u64, reason: Arc<str>) -> Result<(), Error> {
        if self
            .shared
            .outcomes
            .resolve(id, TrackedPublishOutcome::Nack { reason })
        {
            Ok(())
        } else {
            Err(Error::MessageNotTracked)
        }
    }
}

/// The delivery-resolution hook for a durably-delivered bundle. A successful
/// hand-off (`commit`) acks the bundle so durable progress advances; an
/// `abort`/`abandon` defers it for redelivery.
struct QuiverDelivery {
    envelope: Envelope<OtapPdata>,
    handle: Option<BundleHandle<RegistryCallback<SegmentStore>>>,
}

impl DeliveryBackend<OtapPdata> for QuiverDelivery {
    fn envelope(&self) -> &Envelope<OtapPdata> {
        &self.envelope
    }

    fn commit(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.ack();
        }
    }

    fn abort(&mut self, _reason: Arc<str>) -> Result<(), Error> {
        if let Some(handle) = self.handle.take() {
            let _ = handle.defer();
        }
        Ok(())
    }

    fn abandon(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.defer();
        }
    }
}

/// Durable partition-dispatch is the durable backend for `OtapPdata`
/// (durable-dispatch Layer B). The generic engine/controller construct it
/// through this trait without depending on quiver or the OTAP data model.
impl DurableDispatchPayload for OtapPdata {
    fn create_durable_partition_dispatch_topic(
        name: TopicName,
        config: DurableDispatchConfig,
    ) -> Result<Arc<dyn TopicState<Self>>, Error> {
        QuiverPartitionDispatchTopic::open(name, config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdata::{Context, OtapPdata};
    use otap_df_engine::topic::{RecvDelivery, Subscription, SubscriptionMode, TopicBroker};
    use otap_df_pdata::otap::{OtapArrowRecords, OtapBatchStore};
    use otap_df_pdata::{logs, record_batch};
    use tempfile::TempDir;

    /// Build a single-batch logs `OtapPdata` tagged with `partition`.
    fn tagged_logs(partition: u32) -> OtapPdata {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let mut pdata = OtapPdata::new(Context::default(), OtapPayload::OtapArrowRecords(records));
        pdata.set_partition(partition);
        pdata
    }

    fn config(dir: &TempDir, num_partitions: usize) -> DurableDispatchConfig {
        DurableDispatchConfig {
            data_dir: dir.path().to_path_buf(),
            num_partitions,
            capacity: 64,
            disk_budget_bytes: 256 * 1024 * 1024,
            retention: DurableRetentionPolicy::Backpressure,
        }
    }

    fn pd_mode(owned: Vec<u32>) -> SubscriptionMode {
        SubscriptionMode::PartitionDispatch {
            owned_partitions: owned,
        }
    }

    /// Drain a subscription to exhaustion (until closed), committing each
    /// delivery; returns the partition tag of each received message.
    async fn drain(sub: &mut Subscription<OtapPdata>) -> Vec<u32> {
        let mut got = Vec::new();
        loop {
            match sub.recv_delivery().await {
                Ok(RecvDelivery::Message(delivery)) => {
                    if let Some(p) = delivery.envelope().payload.partition() {
                        got.push(p);
                    }
                    delivery.commit();
                }
                Ok(RecvDelivery::Lagged { .. }) => {}
                Err(_) => break,
            }
        }
        got
    }

    /// Receive and commit exactly one message, returning its partition tag.
    async fn recv_one(sub: &mut Subscription<OtapPdata>) -> Option<u32> {
        match sub.recv_delivery().await {
            Ok(RecvDelivery::Message(delivery)) => {
                let p = delivery.envelope().payload.partition();
                delivery.commit();
                p
            }
            _ => None,
        }
    }

    // Each published message is persisted and delivered to the single owner of
    // its partition; every message reaches exactly one owner.
    #[tokio::test]
    async fn durable_routes_each_partition_to_its_owner() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-route".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 4)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub0 = handle.subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default()).unwrap();
        let mut sub1 = handle.subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default()).unwrap();

        for p in [0u32, 1, 2, 3, 0, 2] {
            handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
        }
        handle.close();

        let mut got0 = drain(&mut sub0).await;
        let mut got1 = drain(&mut sub1).await;
        got0.sort_unstable();
        got1.sort_unstable();
        assert_eq!(got0, vec![0, 0, 1]);
        assert_eq!(got1, vec![2, 2, 3]);
    }

    // A partition can be claimed by at most one subscriber.
    #[tokio::test]
    async fn durable_exclusive_claim_rejected() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-excl".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 4)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let _sub0 = handle.subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default()).unwrap();
        match handle.subscribe(pd_mode(vec![1, 2]), SubscriberOptions::default()) {
            Err(Error::PartitionAlreadyClaimed { partition }) => assert_eq!(partition, 1),
            Ok(_) => panic!("expected PartitionAlreadyClaimed, got Ok"),
            Err(e) => panic!("expected PartitionAlreadyClaimed, got {e:?}"),
        }
    }

    // Claiming a partition outside the topic's range is rejected.
    #[tokio::test]
    async fn durable_out_of_range_rejected() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-oor".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 4)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        match handle.subscribe(pd_mode(vec![4]), SubscriberOptions::default()) {
            Err(Error::PartitionOutOfRange { partition, num_partitions }) => {
                assert_eq!(partition, 4);
                assert_eq!(num_partitions, 4);
            }
            Ok(_) => panic!("expected PartitionOutOfRange, got Ok"),
            Err(e) => panic!("expected PartitionOutOfRange, got {e:?}"),
        }
    }

    // After a reassignment, the new owner resumes the partition from durable
    // progress: the message the old owner already acked is not redelivered, and
    // the post-reassign message goes to the new owner.
    #[tokio::test]
    async fn durable_reassign_resumes_from_progress() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-reassign".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 4)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub0 = handle.subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default()).unwrap();
        let mut sub1 = handle.subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default()).unwrap();

        // Owner0 drains and acks the first message on partition 0 (the old owner
        // draining up to the window boundary).
        handle.publish(Arc::new(tagged_logs(0))).await.unwrap();
        assert_eq!(recv_one(&mut sub0).await, Some(0));

        // Reassign partition 0 to owner1, then publish another message there.
        handle.reassign_partition(0, 1).unwrap();
        handle.publish(Arc::new(tagged_logs(0))).await.unwrap();
        handle.close();

        // Owner1 receives only the post-reassign message; owner0 receives nothing
        // further (the acked message is not redelivered).
        assert_eq!(drain(&mut sub1).await, vec![0]);
        assert_eq!(drain(&mut sub0).await, Vec::<u32>::new());
    }

    // Published-but-unconsumed data survives a restart: reopening the same data
    // directory replays the un-acked bundles.
    #[tokio::test]
    async fn durable_restart_replays_unacked() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-restart".into();
        let cfg = config(&dir, 2);

        // Run 1: publish three messages, never consume them.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name.clone(), state).unwrap();
            for p in [0u32, 1, 0] {
                handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
            }
            handle.close();
        }

        // Run 2: reopen the same directory; all three un-acked bundles replay.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name, state).unwrap();
            let mut sub =
                handle.subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default()).unwrap();
            handle.close();
            let mut got = drain(&mut sub).await;
            got.sort_unstable();
            assert_eq!(got, vec![0, 0, 1]);
        }
    }

    // The controller constructs the durable topic through the
    // `DurableDispatchPayload` trait; exercise that exact path end to end.
    #[tokio::test]
    async fn durable_dispatch_payload_trait_builds_working_topic() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-trait".into();
        let state = <OtapPdata as DurableDispatchPayload>::create_durable_partition_dispatch_topic(
            name.clone(),
            config(&dir, 2),
        )
        .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub = handle.subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default()).unwrap();
        for p in [0u32, 1, 0] {
            handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
        }
        handle.close();

        let mut got = drain(&mut sub).await;
        got.sort_unstable();
        assert_eq!(got, vec![0, 0, 1]);
    }
}
