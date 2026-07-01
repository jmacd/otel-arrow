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
//! One [`QuiverEngine`] per **owner** (durable-dispatch D24's production
//! "one quiver per owner" option): the `N` partitions are placed across `M`
//! owners by a static `balanced(N, M)` map, and each owner's store interleaves
//! every partition it holds, sharing one WAL and disk budget. Each owner's
//! engine lives under `{data_dir}/{topic}/owner_{o}`. The partition a bundle
//! belongs to is stamped into the bundle's opaque `user_meta` at publish and
//! recovered on drain, since the engine no longer implies the partition. With
//! `M == N` (the default) each owner holds exactly one partition, reproducing
//! the original per-partition layout.
//!
//! # Ownership and reassignment
//!
//! A subscription claims whole **static** owner blocks (durable-dispatch D23/D24)
//! and drains those owner stores; that claim is fixed at subscribe time. On top
//! of the static store membership sits a **dynamic routing overlay**
//! (durable-dispatch Phase 3): `reassign_partition` repoints a partition's new
//! publishes to a different owner store and bumps its lease generation, rather
//! than rejecting the move as Phase 2 did. Data already persisted for the
//! partition in its former owner's store is migrated lazily by **drain-and-forward**:
//! the former owner drains that residual, recognizes it is no longer the
//! partition's current owner, forwards it to the new owner's store, and acks it
//! in its own. The overlay is **persisted as a snapshot** under the topic's data
//! directory ([`PLACEMENT_FILE`]) whenever it changes, so reassignments survive
//! restart; `open()` restores it, falling back to the static placement when the
//! snapshot is absent or the topology changed. The snapshot is written before the
//! swap takes effect, so a reassignment is durable before it is observable.
//!
//! # Durability lifecycle (PoC)
//!
//! - **Publish** routes the message to its partition's owner store and ingests
//!   the OTAP batch there, durable via the WAL on return, stamping the partition
//!   into `user_meta`. A flush, which finalizes the segment that makes the data
//!   pollable, is batched: an owner flushes at most once per
//!   [`FLUSH_INTERVAL_MS`], so rapid publishes accumulate into one segment
//!   instead of finalizing one segment per publish. An owner's first publish
//!   flushes immediately, and `close` performs a final flush.
//! - **Receive** claims the next durable bundle for an owned store and
//!   reconstructs `OtapPdata`, re-stamping the partition recovered from
//!   `user_meta`.
//! - **Commit** acks the bundle, advancing durable progress so it is not
//!   redelivered. **Abort / abandon** defers it for redelivery.
//! - **Restart** reopens the per-owner engines; un-acked bundles replay
//!   (open -> register -> activate picks up the on-disk segments). Bundles
//!   recovered from a finalized segment keep their `user_meta` (clean restart);
//!   the crash window of WAL-only bundles now also recovers `user_meta` from the
//!   v2 WAL entry (durable-dispatch Phase 3, closed), so a partition survives an
//!   unclean stop. Legacy v1 WALs written before the stamping recover it as 0.

use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use quiver::{
    BundleHandle, DiskBudget, QuiverConfig, QuiverEngine, RegistryCallback, RetentionPolicy,
    SegmentStore, SubscriberId,
};

use otap_df_engine::error::Error;
use otap_df_engine::topic::{
    Delivery, DeliveryBackend, DurableDispatchConfig, DurableDispatchPayload,
    DurableRetentionPolicy, Envelope, PartitionPlacement, PublishFuture, PublishOutcome,
    PublishTrackedFuture, RecvDelivery, SubscriberOptions, SubscriptionBackend,
    SubscriptionGroupName, TopicBroadcastOnLagPolicy, TopicName, TopicState, TrackedPublishOutcome,
    TrackedPublishPermit, TrackedPublishTracker, TrackedTryPublishOutcome,
};
use otap_df_pdata::OtapPayload;
use otap_df_telemetry::{otel_debug, otel_warn};

use crate::pdata::OtapPdata;
use crate::quiver_bundle::{OtapRecordBundleAdapter, OtlpBytesAdapter, convert_bundle_to_pdata};

/// Fixed subscriber id used to drain every partition engine. Progress is tied to
/// the partition (not the owner), so a reassigned partition resumes where the
/// previous owner left off.
const DISPATCH_SUBSCRIBER_ID: &str = "partition-dispatch";

/// Per-owner-engine segment target size. Small so the per-owner disk-budget
/// minimum stays modest; flush-per-publish makes the size trigger irrelevant.
const SEGMENT_TARGET_BYTES: u64 = 1024 * 1024;
/// Per-owner-engine WAL cap.
const WAL_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Per-owner-engine WAL rotation target (must not exceed [`WAL_MAX_BYTES`]).
const WAL_ROTATION_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// Minimum wall-clock interval between flushes of a single owner engine.
/// Publishes within this window accumulate into one segment instead of
/// finalizing one segment per publish, trading a small visibility latency for
/// throughput. An owner's first publish always flushes (its last-flush timestamp
/// starts at 0), and `close` performs a final flush so no data is left unflushed.
const FLUSH_INTERVAL_MS: u64 = 50;

/// Wall-clock milliseconds since the Unix epoch, used only as a coarse flush
/// gate; a backwards clock at worst causes an extra or slightly delayed flush.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Maps a quiver error into the engine's durable-backend error.
fn durable_err(message: impl std::fmt::Display) -> Error {
    Error::DurableBackendError {
        message: message.to_string(),
    }
}

/// Pack a partition index and ownership lease generation into the opaque
/// per-bundle metadata quiver persists (Phase 1's `user_meta`). The low 32 bits
/// hold the partition; the high 32 hold the lease generation, bumped by
/// `reassign_partition` (durable-dispatch Phase 3). The owner recovers the
/// partition on drain (one-quiver-per-owner interleaves every owned partition in
/// one store, so the partition is not implied by the engine); the generation is
/// the durable ownership-epoch marker carried with each bundle.
const fn pack_user_meta(partition: u32, generation: u32) -> u64 {
    ((generation as u64) << 32) | (partition as u64)
}

/// Recover the partition index from a packed [`pack_user_meta`] value.
const fn unpack_partition(user_meta: u64) -> u32 {
    (user_meta & 0xFFFF_FFFF) as u32
}

/// Recover the ownership lease generation from a packed [`pack_user_meta`] value.
#[allow(dead_code)] // stamped and carried today; compared by cross-node fencing later
const fn unpack_generation(user_meta: u64) -> u32 {
    (user_meta >> 32) as u32
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

/// The dynamic routing overlay: for each partition, the owner store its *new*
/// publishes currently route to and its ownership lease generation
/// (durable-dispatch Phase 3). Held behind an [`ArcSwap`] so the hot publish and
/// drain paths read it **lock-free**; `reassign_partition` swaps in an updated
/// snapshot (copy-on-write, rare). Each partition is an independent point-in-time
/// snapshot, so no cross-partition consistency is required. Initialized from the
/// static `balanced` placement, then persisted as a snapshot so reassignments
/// survive restart (see [`persist_placement`] / [`load_placement`]).
struct RoutingOverlay {
    /// `partition_owner[p]` is the owner store partition `p`'s new publishes
    /// currently route to. Length is `num_partitions`.
    partition_owner: Vec<usize>,
    /// `partition_generation[p]` is partition `p`'s ownership lease generation,
    /// bumped on each reassignment. Length is `num_partitions`.
    partition_generation: Vec<u32>,
}

impl RoutingOverlay {
    /// The initial overlay: each partition routes to its static `balanced` owner
    /// at generation 0.
    fn from_static_placement(placement: &PartitionPlacement, num_partitions: usize) -> Self {
        Self {
            partition_owner: (0..num_partitions).map(|p| placement.owner_of(p)).collect(),
            partition_generation: vec![0; num_partitions],
        }
    }

    fn owner_and_generation(&self, partition: usize) -> (usize, u32) {
        (
            self.partition_owner[partition],
            self.partition_generation[partition],
        )
    }
}

/// Magic + version prefix of the placement snapshot file. The trailing digit is
/// the format version; bump it on any layout change.
const PLACEMENT_MAGIC: &[u8; 8] = b"QVPLACE1";
/// Filename of the placement snapshot under a topic's data directory.
const PLACEMENT_FILE: &str = "placement.v1";

/// Persist the routing overlay as an atomically-rewritten snapshot under
/// `topic_dir` so reassignments survive restart (durable-dispatch Phase 3).
///
/// The map is tiny and rewritten wholesale, so this is a snapshot rather than a
/// log; the write is made durable and atomic by writing a temp file, fsyncing it,
/// and renaming it over the target. Kept behind this function so the mechanism
/// could later be swapped for a Quiver-backed control stream without touching the
/// topic logic. Layout: `magic(8) | num_partitions(u32) | num_owners(u32) |
/// [owner(u32), generation(u32)] * num_partitions`, little-endian.
fn persist_placement(
    topic_dir: &Path,
    overlay: &RoutingOverlay,
    num_owners: usize,
) -> std::io::Result<()> {
    use std::io::Write as _;

    let np = overlay.partition_owner.len();
    let mut buf = Vec::with_capacity(PLACEMENT_MAGIC.len() + 8 + np * 8);
    buf.extend_from_slice(PLACEMENT_MAGIC);
    buf.extend_from_slice(&(np as u32).to_le_bytes());
    buf.extend_from_slice(&(num_owners as u32).to_le_bytes());
    for (owner, generation) in overlay
        .partition_owner
        .iter()
        .zip(&overlay.partition_generation)
    {
        buf.extend_from_slice(&(*owner as u32).to_le_bytes());
        buf.extend_from_slice(&generation.to_le_bytes());
    }

    let tmp = topic_dir.join("placement.v1.tmp");
    let final_path = topic_dir.join(PLACEMENT_FILE);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    // Best-effort directory fsync so the rename itself is durable across a crash.
    if let Ok(dir) = std::fs::File::open(topic_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Load a persisted placement snapshot, returning the restored overlay, or `None`
/// when the file is absent, corrupt, or does not match the current topology
/// (`num_partitions` / `num_owners` changed, or an owner is out of range). A
/// `None` result makes the caller fall back to the static `balanced` placement.
fn load_placement(
    topic_dir: &Path,
    num_partitions: usize,
    num_owners: usize,
) -> Option<RoutingOverlay> {
    let bytes = std::fs::read(topic_dir.join(PLACEMENT_FILE)).ok()?;
    let header_len = PLACEMENT_MAGIC.len() + 8;
    if bytes.len() < header_len || &bytes[..PLACEMENT_MAGIC.len()] != PLACEMENT_MAGIC {
        return None;
    }
    let mut cursor = PLACEMENT_MAGIC.len();
    let read_u32 = |bytes: &[u8], at: usize| -> Option<u32> {
        bytes.get(at..at + 4).map(|s| {
            let mut a = [0u8; 4];
            a.copy_from_slice(s);
            u32::from_le_bytes(a)
        })
    };
    let np = read_u32(&bytes, cursor)? as usize;
    cursor += 4;
    let no = read_u32(&bytes, cursor)? as usize;
    cursor += 4;
    // A topology change invalidates a persisted placement; fall back to static.
    if np != num_partitions || no != num_owners {
        return None;
    }
    if bytes.len() < cursor + np * 8 {
        return None;
    }
    let mut partition_owner = Vec::with_capacity(np);
    let mut partition_generation = Vec::with_capacity(np);
    for _ in 0..np {
        let owner = read_u32(&bytes, cursor)? as usize;
        cursor += 4;
        let generation = read_u32(&bytes, cursor)?;
        cursor += 4;
        if owner >= num_owners {
            return None;
        }
        partition_owner.push(owner);
        partition_generation.push(generation);
    }
    Some(RoutingOverlay {
        partition_owner,
        partition_generation,
    })
}

/// Subscription-claim state for a partition-dispatch topic, touched only at
/// subscribe time (off the hot path), so it stays behind a [`Mutex`]. The
/// *physical* `partition -> owner store` membership used for claims is static
/// (durable-dispatch D23/D24, held on [`Shared::placement`]): each subscription
/// claims whole static owner blocks and drains those stores, and that never
/// changes. The dynamic routing overlay that reassignment mutates lives
/// separately on [`Shared::routing`] as an [`ArcSwap`], off this lock.
struct Claims {
    /// The subscription index assigned to the next subscriber.
    next_sub_idx: usize,
    /// `partition_claimed[p]` is the subscription that claimed partition `p`, or
    /// `None` while unclaimed. Length is `num_partitions`. Recorded at subscribe
    /// time from the static placement; not used for runtime routing.
    partition_claimed: Vec<Option<usize>>,
    /// `engine_claimed[o]` is the subscription draining owner store `o`, or
    /// `None` while unclaimed. Length is `num_owners`. A second subscriber whose
    /// partitions land on an already-drained owner is rejected.
    engine_claimed: Vec<Option<usize>>,
}

/// Shared topic state. Held by the topic and cloned into each subscription so
/// owners can route, drain, and observe claims through one source of truth.
struct Shared {
    name: TopicName,
    num_partitions: usize,
    /// The static `partition -> owner` placement (durable-dispatch D23/D24):
    /// `balanced(num_partitions, num_owners)`. `owner == partition` when
    /// `num_owners == num_partitions` (the per-partition default). The owner
    /// count is `placement.num_owners()` / `engines.len()`.
    placement: PartitionPlacement,
    /// One durable engine per owner; index is the owner.
    engines: Vec<Arc<QuiverEngine>>,
    /// Last flush time (wall-clock ms) per owner, for the batched-flush gate.
    last_flush_ms: Vec<AtomicU64>,
    subscriber_id: SubscriberId,
    /// The dynamic routing overlay, read lock-free on the publish/drain hot paths
    /// and swapped by `reassign_partition` (durable-dispatch Phase 3). Persisted
    /// under [`Self::topic_dir`] so reassignments survive restart.
    routing: ArcSwap<RoutingOverlay>,
    /// The topic's data directory (`{data_dir}/{topic}`), where the placement
    /// snapshot ([`PLACEMENT_FILE`]) lives.
    topic_dir: PathBuf,
    /// Serializes reassignments so the persist-then-swap is atomic and concurrent
    /// reassignments do not race on the placement snapshot file. Publishers and
    /// drainers never take it (they read the overlay lock-free).
    reassign_lock: Mutex<()>,
    /// Subscription-claim state, touched only at subscribe time (off the hot
    /// path).
    claims: Mutex<Claims>,
    wakers: WakerSet,
    next_id: AtomicU64,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
}

impl Shared {
    /// Ingest one OTAP payload into owner `owner`'s durable engine, stamping
    /// `user_meta` (the packed partition + generation) so the partition is
    /// recoverable on drain. The write is durable on return (WAL append). Flush
    /// only if at least [`FLUSH_INTERVAL_MS`] has elapsed since this owner last
    /// flushed, so rapid publishes batch into one segment. Returns whether a
    /// flush happened, so the caller wakes subscribers only when new data became
    /// pollable.
    async fn ingest_and_maybe_flush(
        &self,
        owner: usize,
        pdata: OtapPdata,
        user_meta: u64,
    ) -> Result<bool, Error> {
        let engine = &self.engines[owner];
        let (_context, payload) = pdata.into_parts();
        match payload {
            OtapPayload::OtapArrowRecords(records) => {
                let adapter = OtapRecordBundleAdapter::new(records).with_user_meta(user_meta);
                engine.ingest(&adapter).await.map_err(durable_err)?;
            }
            OtapPayload::OtlpBytes(bytes) => {
                let adapter = OtlpBytesAdapter::new(bytes)
                    .map_err(|(e, _)| durable_err(e))?
                    .with_user_meta(user_meta);
                engine.ingest(&adapter).await.map_err(durable_err)?;
            }
        }
        let now = now_ms();
        let last = self.last_flush_ms[owner].load(Ordering::Relaxed);
        let flush = now.saturating_sub(last) >= FLUSH_INTERVAL_MS
            && self.last_flush_ms[owner]
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok();
        if flush {
            engine.flush().await.map_err(durable_err)?;
        }
        Ok(flush)
    }

    /// Flush every owner engine, finalizing any accumulated open segment. Used on
    /// close and after open (to finalize WAL-recovered data); finalizing an empty
    /// segment is a cheap no-op.
    async fn flush_all(&self) {
        for (o, engine) in self.engines.iter().enumerate() {
            if let Err(e) = engine.flush().await {
                otel_warn!(
                    "quiver_topic.flush.failed",
                    error = e.to_string(),
                    owner = o as u64
                );
            } else {
                self.last_flush_ms[o].store(now_ms(), Ordering::Relaxed);
            }
        }
    }
}

/// Build the quiver config for one owner engine: small WAL and segment sizes so
/// the per-owner disk-budget minimum is modest.
fn engine_quiver_config() -> QuiverConfig {
    let mut config = QuiverConfig::default();
    config.segment.target_size_bytes =
        NonZeroU64::new(SEGMENT_TARGET_BYTES).expect("non-zero segment target");
    config.segment.max_open_duration = Duration::from_secs(1);
    config.wal.max_size_bytes = NonZeroU64::new(WAL_MAX_BYTES).expect("non-zero wal max");
    config.wal.rotation_target_bytes =
        NonZeroU64::new(WAL_ROTATION_TARGET_BYTES).expect("non-zero wal rotation");
    config
}

/// The per-owner disk-budget hard cap: an even split of the topic's total
/// budget, floored at the minimum a single engine requires so construction
/// never fails on a too-small budget.
fn per_owner_cap(total_budget_bytes: u64, num_owners: usize, config: &QuiverConfig) -> u64 {
    let minimum = DiskBudget::minimum_hard_cap(
        config.segment.target_size_bytes.get(),
        DiskBudget::effective_wal_size(config),
    );
    let even_split = total_budget_bytes / num_owners as u64;
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
            num_owners,
            capacity: _capacity,
            disk_budget_bytes,
            retention,
        } = config;

        if num_partitions == 0 {
            return Err(durable_err(
                "durable partition-dispatch requires num_partitions >= 1",
            ));
        }
        if num_owners == 0 || num_owners > num_partitions {
            return Err(durable_err(format!(
                "durable partition-dispatch requires 1 <= num_owners <= num_partitions \
                 (num_owners={num_owners}, num_partitions={num_partitions})"
            )));
        }

        // Static placement: each partition is bound to one owner store. With
        // num_owners == num_partitions this is the identity (owner == partition),
        // reproducing the original per-partition layout.
        let placement = PartitionPlacement::balanced(
            NonZeroUsize::new(num_partitions).expect("num_partitions != 0"),
            NonZeroUsize::new(num_owners).expect("num_owners != 0"),
        );

        let subscriber_id = SubscriberId::new(DISPATCH_SUBSCRIBER_ID)
            .map_err(|e| durable_err(format!("invalid subscriber id: {e}")))?;
        let policy = match retention {
            DurableRetentionPolicy::Backpressure => RetentionPolicy::Backpressure,
            DurableRetentionPolicy::DropOldest => RetentionPolicy::DropOldest,
        };
        let topic_dir = data_dir.join(name.as_ref());

        let engines = block_on_init({
            let subscriber_id = subscriber_id.clone();
            let topic_dir = topic_dir.clone();
            async move {
                let mut engines = Vec::with_capacity(num_owners);
                for o in 0..num_owners {
                    let owner_dir = topic_dir.join(format!("owner_{o}"));
                    let quiver_config = engine_quiver_config().with_data_dir(&owner_dir);
                    let cap = per_owner_cap(disk_budget_bytes, num_owners, &quiver_config);
                    let budget = Arc::new(
                        DiskBudget::for_config(cap, &quiver_config, policy)
                            .map_err(|e| durable_err(format!("invalid disk budget: {e}")))?,
                    );
                    let engine = QuiverEngine::builder(quiver_config)
                        .with_budget(budget)
                        .build()
                        .await
                        .map_err(|e| durable_err(format!("failed to open owner engine: {e}")))?;
                    // Finalize any data recovered from the WAL on open so it is
                    // pollable, then register the drain subscriber. Flushing an
                    // empty segment (the fresh-topic case) is a no-op.
                    engine
                        .flush()
                        .await
                        .map_err(|e| durable_err(format!("failed to flush on open: {e}")))?;
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

        // Restore the routing overlay from the persisted placement snapshot so
        // reassignments survive restart; a fresh or topology-changed topic falls
        // back to the static balanced placement (durable-dispatch Phase 3).
        let overlay = load_placement(&topic_dir, num_partitions, num_owners)
            .unwrap_or_else(|| RoutingOverlay::from_static_placement(&placement, num_partitions));

        let shared = Arc::new(Shared {
            name,
            num_partitions,
            placement,
            last_flush_ms: (0..num_owners).map(|_| AtomicU64::new(0)).collect(),
            engines,
            subscriber_id,
            routing: ArcSwap::from_pointee(overlay),
            topic_dir,
            reassign_lock: Mutex::new(()),
            claims: Mutex::new(Claims {
                next_sub_idx: 0,
                partition_claimed: vec![None; num_partitions],
                engine_claimed: vec![None; num_owners],
            }),
            wakers: WakerSet::default(),
            next_id: AtomicU64::new(1),
            outcomes: TrackedPublishTracker::new(),
            closed: AtomicBool::new(false),
        });

        Ok(Arc::new(Self { shared }))
    }

    /// The destination `(owner, partition, generation)` for a message, or `None`
    /// when it is untagged or out of range (such a message has no durable home
    /// and is dropped, matching the in-memory partition-dispatch topic). The
    /// owner and generation come from the dynamic routing overlay, so a message
    /// routes to the partition's *current* owner store stamped with its current
    /// lease generation (durable-dispatch Phase 3).
    fn route(shared: &Shared, msg: &OtapPdata) -> Option<(usize, usize, u32)> {
        let partition = msg.partition()? as usize;
        if partition >= shared.num_partitions {
            return None;
        }
        // Lock-free read of the current routing snapshot (durable-dispatch
        // Phase 3): the ArcSwap load keeps the publish hot path off any mutex.
        let (owner, generation) = shared.routing.load().owner_and_generation(partition);
        Some((owner, partition, generation))
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
            if let Some((owner, partition, generation)) = Self::route(&shared, &msg) {
                let user_meta = pack_user_meta(partition as u32, generation);
                if shared
                    .ingest_and_maybe_flush(owner, (*msg).clone(), user_meta)
                    .await?
                {
                    shared.wakers.wake_all();
                }
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
                Some((owner, partition, generation)) => {
                    let user_meta = pack_user_meta(partition as u32, generation);
                    match shared
                        .ingest_and_maybe_flush(owner, (*msg).clone(), user_meta)
                        .await
                    {
                        Ok(flushed) => {
                            if flushed {
                                shared.wakers.wake_all();
                            }
                            // Durability is the acknowledgement: the WAL append in
                            // ingest persists the message, independent of when its
                            // segment is flushed for pollability.
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
                    }
                }
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
        if let Some((owner, partition, generation)) = Self::route(&self.shared, &msg) {
            let shared = self.shared.clone();
            let pdata = (*msg).clone();
            // The durable write is asynchronous; submit it without blocking the
            // caller. Errors are surfaced through telemetry rather than the
            // synchronous return (PoC: try_publish is best-effort durable).
            spawn_durable_ingest(shared, owner, partition, generation, pdata, None);
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
            Some((owner, partition, generation)) => {
                let shared = self.shared.clone();
                let pdata = (*msg).clone();
                spawn_durable_ingest(shared, owner, partition, generation, pdata, Some(id));
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
        let placement = &self.shared.placement;
        let mut claims = self.shared.claims.lock();
        // Validate the whole claim before mutating, so a rejected subscription
        // leaves no partial ownership.
        for &partition in &owned_partitions {
            if partition as usize >= self.shared.num_partitions {
                return Err(Error::PartitionOutOfRange {
                    partition,
                    num_partitions: self.shared.num_partitions,
                });
            }
            if claims.partition_claimed[partition as usize].is_some() {
                return Err(Error::PartitionAlreadyClaimed { partition });
            }
        }

        // The partitions map to a set of owner stores. Because each owner store
        // is a single durable engine drained by one subscriber id, a subscriber
        // must claim *every* partition an owner holds (a whole owner block), so
        // each store is drained by exactly one subscription. Collect the owner
        // stores and verify each is wholly claimed (and not already drained).
        let mut owned_engines: Vec<usize> = Vec::new();
        for &partition in &owned_partitions {
            let owner = placement.owner_of(partition as usize);
            if owned_engines.contains(&owner) {
                continue;
            }
            for q in placement.partitions_of(owner) {
                if !owned_partitions.contains(&(q as u32)) {
                    return Err(Error::PartitionOwnerBlockIncomplete {
                        partition: q as u32,
                        owner,
                    });
                }
            }
            if claims.engine_claimed[owner].is_some() {
                return Err(Error::PartitionAlreadyClaimed { partition });
            }
            owned_engines.push(owner);
        }

        let sub_idx = claims.next_sub_idx;
        claims.next_sub_idx += 1;
        for &partition in &owned_partitions {
            claims.partition_claimed[partition as usize] = Some(sub_idx);
        }
        for &owner in &owned_engines {
            claims.engine_claimed[owner] = Some(sub_idx);
        }
        drop(claims);

        owned_engines.sort_unstable();
        Ok(Box::new(QuiverDispatchSub {
            shared: self.shared.clone(),
            owned_engines,
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
        let num_owners = self.shared.engines.len();
        if to_owner >= num_owners {
            return Err(Error::OwnerOutOfRange {
                owner: to_owner,
                num_owners,
            });
        }
        // Repoint the partition's new publishes to `to_owner`'s store and bump its
        // lease generation. Data already persisted for the partition in its former
        // owner's store is migrated lazily: the former owner drains that residual,
        // recognizes it is no longer the current owner, and forwards it to the new
        // owner's store (see `QuiverDispatchSub::poll_recv_delivery`). Applied at
        // an aggregation-window boundary per Layer C, the residual is the closing
        // window, so no live per-series state crosses.
        //
        // Serialize reassignments so the persist-then-swap is atomic; readers on
        // the hot path keep loading the current snapshot lock-free. The new
        // placement is persisted **before** it is swapped in, so a reassignment is
        // durable before it takes effect: if the snapshot write fails the overlay
        // is left unchanged and the move is reported as failed. Reassignment is
        // rare, so the O(num_partitions) clone and fsync are not a concern.
        let _write = self.shared.reassign_lock.lock();
        let mut next = RoutingOverlay {
            partition_owner: self.shared.routing.load().partition_owner.clone(),
            partition_generation: self.shared.routing.load().partition_generation.clone(),
        };
        next.partition_owner[partition] = to_owner;
        next.partition_generation[partition] =
            next.partition_generation[partition].wrapping_add(1);
        persist_placement(&self.shared.topic_dir, &next, num_owners).map_err(|e| {
            durable_err(format!("failed to persist placement on reassignment: {e}"))
        })?;
        self.shared.routing.store(Arc::new(next));
        drop(_write);
        // Wake drainers so the deposed owner forwards its residual promptly.
        self.shared.wakers.wake_all();
        Ok(())
    }

    fn broadcast_on_lag_policy(&self) -> TopicBroadcastOnLagPolicy {
        TopicBroadcastOnLagPolicy::DropOldest
    }

    fn close(&self) {
        // Reject new publishes first, then flush any data accumulated since the
        // last batched flush so close-then-drain (and a clean restart) see it.
        // Run the final flush on a fresh thread so this synchronous call works
        // whether or not the caller is inside a tokio runtime.
        self.shared.closed.store(true, Ordering::Relaxed);
        let shared = self.shared.clone();
        block_on_init(async move { shared.flush_all().await });
        self.shared.outcomes.close_all();
        self.shared.wakers.wake_all();
    }
}

/// Submit a durable ingest for `pdata` into owner `owner`'s store (for
/// `partition` at lease `generation`) without blocking the caller (the durable
/// write and any batched flush happen asynchronously). Resolves a tracked-publish
/// outcome when `tracked_id` is set.
fn spawn_durable_ingest(
    shared: Arc<Shared>,
    owner: usize,
    partition: usize,
    generation: u32,
    pdata: OtapPdata,
    tracked_id: Option<u64>,
) {
    let task = async move {
        let user_meta = pack_user_meta(partition as u32, generation);
        match shared.ingest_and_maybe_flush(owner, pdata, user_meta).await {
            Ok(flushed) => {
                if flushed {
                    shared.wakers.wake_all();
                }
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

/// Forward a bundle drained from a store that is no longer the partition's owner
/// to the partition's current owner store, then ack it in the store it came from
/// (durable-dispatch Phase 3 drain-and-forward). The re-ingest stamps the current
/// `generation` so the new owner delivers it instead of re-forwarding.
///
/// Runs off the poll path: the bundle stays *claimed* until the ack, so it is not
/// re-drained (and so not re-forwarded) while the migration is in flight. On
/// failure the handle is dropped, which defers the bundle for a later retry
/// (at-least-once), so no residual is lost. A crash between the re-ingest and the
/// ack redelivers the bundle from the old store and forwards it again, a
/// duplicate the at-least-once contract tolerates.
fn forward_fenced_bundle(
    shared: Arc<Shared>,
    handle: BundleHandle<RegistryCallback<SegmentStore>>,
    partition: u32,
    to_owner: usize,
    generation: u32,
) {
    // Reconstruct the payload before moving the handle into the task. The
    // partition rides `user_meta`, so the engine's dropped context is irrelevant.
    let pdata = match convert_bundle_to_pdata(handle.data()) {
        Ok(pdata) => pdata,
        Err(e) => {
            // Unreconstructable residual is poison; reject so it does not loop.
            handle.reject();
            otel_warn!(
                "quiver_topic.forward.unreconstructable",
                error = e.to_string(),
                partition = partition as u64,
                message = "rejected an unreconstructable residual bundle during reassignment",
            );
            return;
        }
    };
    let user_meta = pack_user_meta(partition, generation);
    let task = async move {
        match shared.ingest_and_maybe_flush(to_owner, pdata, user_meta).await {
            Ok(_) => {
                // Force the forwarded data to a segment so the new owner can poll
                // it promptly instead of waiting for the batched-flush window;
                // reassignment is rare, so an extra flush here is cheap.
                if let Err(e) = shared.engines[to_owner].flush().await {
                    otel_warn!(
                        "quiver_topic.forward.flush_failed",
                        error = e.to_string(),
                        to_owner = to_owner as u64,
                    );
                } else {
                    shared.last_flush_ms[to_owner].store(now_ms(), Ordering::Relaxed);
                }
                // Migrated: ack in the old store so it is not redelivered there,
                // then wake drainers so the new owner picks it up and the old
                // owner forwards any remaining residual.
                handle.ack();
                shared.wakers.wake_all();
            }
            Err(e) => {
                otel_warn!(
                    "quiver_topic.forward.failed",
                    error = e.to_string(),
                    partition = partition as u64,
                    to_owner = to_owner as u64,
                    message = "failed to forward a reassigned partition's residual; will retry",
                );
                // Drop the handle: an implicit defer redelivers it for a retry.
                drop(handle);
            }
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            drop(rt.spawn(task));
        }
        Err(_) => {
            block_on_init(task);
        }
    }
}

/// A partition-dispatch subscription over the durable backend. Drains the owner
/// stores it claimed (whole owner blocks); each drained bundle's partition is
/// recovered from the bundle's `user_meta` (one store interleaves every owned
/// partition, so the partition is not implied by the engine).
struct QuiverDispatchSub {
    shared: Arc<Shared>,
    /// The owner stores this subscription drains, ascending (one quiver engine
    /// each). Fixed at subscribe time (durable-dispatch Phase 2 static placement).
    owned_engines: Vec<usize>,
    /// Round-robin cursor across owned engines, for fairness.
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

        let owned = &self.owned_engines;
        if !owned.is_empty() {
            for k in 0..owned.len() {
                let idx = (self.scan_cursor + k) % owned.len();
                let owner = owned[idx];
                match self.shared.engines[owner].poll_next_bundle(&self.shared.subscriber_id) {
                    Ok(Some(handle)) => {
                        self.scan_cursor = (idx + 1) % owned.len();
                        // The partition rides the bundle's opaque user_meta,
                        // stamped at publish; the owner store holds many.
                        let partition = unpack_partition(handle.user_meta());
                        // Fence (durable-dispatch Phase 3): if this partition was
                        // reassigned away from the store we drained it from, this
                        // bundle is residual from a superseded ownership epoch.
                        // Forward it to the partition's current owner store rather
                        // than delivering it here, then keep scanning. Publishing
                        // is centralized, so a live write always carries the
                        // current owner/generation; the only stale data is such
                        // residual sitting in a former owner's store.
                        let (current_owner, current_gen) = self
                            .shared
                            .routing
                            .load()
                            .owner_and_generation(partition as usize);
                        if current_owner != owner {
                            forward_fenced_bundle(
                                self.shared.clone(),
                                handle,
                                partition,
                                current_owner,
                                current_gen,
                            );
                            continue;
                        }
                        match convert_bundle_to_pdata(handle.data()) {
                            Ok(mut pdata) => {
                                pdata.set_partition(partition);
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
                                    owner = owner as u64,
                                    partition = partition as u64,
                                    message =
                                        "rejected a durable bundle that failed to reconstruct"
                                );
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        otel_debug!(
                            "quiver_topic.poll.error",
                            error = e.to_string(),
                            owner = owner as u64
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
        // Default to one owner per partition, the per-partition layout.
        config_with_owners(dir, num_partitions, num_partitions)
    }

    fn config_with_owners(
        dir: &TempDir,
        num_partitions: usize,
        num_owners: usize,
    ) -> DurableDispatchConfig {
        DurableDispatchConfig {
            data_dir: dir.path().to_path_buf(),
            num_partitions,
            num_owners,
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

    /// Receive exactly `n` messages from an open subscription, committing each;
    /// returns their partition tags. Unlike [`drain`], this waits for messages to
    /// arrive (including forwarded residual) rather than stopping at closure, so
    /// it is deterministic for the async drain-and-forward path.
    async fn drain_n(sub: &mut Subscription<OtapPdata>, n: usize) -> Vec<u32> {
        let mut got = Vec::new();
        while got.len() < n {
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

    // Each published message is persisted and delivered to the single owner of
    // its partition; every message reaches exactly one owner.
    #[tokio::test]
    async fn durable_routes_each_partition_to_its_owner() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-route".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 4)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub0 = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
        let mut sub1 = handle
            .subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default())
            .unwrap();

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

        let _sub0 = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
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
            Err(Error::PartitionOutOfRange {
                partition,
                num_partitions,
            }) => {
                assert_eq!(partition, 4);
                assert_eq!(num_partitions, 4);
            }
            Ok(_) => panic!("expected PartitionOutOfRange, got Ok"),
            Err(e) => panic!("expected PartitionOutOfRange, got {e:?}"),
        }
    }

    // After reassigning a partition to a new owner, that partition's *new*
    // publishes route to the new owner's store and are delivered by the
    // subscriber draining it (durable-dispatch Phase 3 routing overlay).
    #[tokio::test]
    async fn durable_reassign_routes_new_publishes_to_new_owner() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-reassign-route".into();
        // 4 partitions across 2 owners: owner0={0,1}, owner1={2,3}.
        let state =
            QuiverPartitionDispatchTopic::open(name.clone(), config_with_owners(&dir, 4, 2))
                .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let _sub0 = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
        let mut sub1 = handle
            .subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default())
            .unwrap();

        // Move partition 0 to owner1 before any publish, so there is no residual
        // to forward: this isolates the routing overlay.
        handle.reassign_partition(0, 1).unwrap();
        handle.publish(Arc::new(tagged_logs(0))).await.unwrap();

        // sub1 (draining owner1's store) receives the partition-0 message even
        // though its static claim is {2, 3}.
        let got = drain_n(&mut sub1, 1).await;
        assert_eq!(got, vec![0]);
    }

    // Data already persisted for a partition in its former owner's store is
    // migrated to the new owner's store by drain-and-forward: the former owner
    // recognizes the residual is no longer its own and forwards it, so the new
    // owner delivers it (durable-dispatch Phase 3).
    #[tokio::test]
    async fn durable_reassign_forwards_residual_to_new_owner() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-reassign-forward".into();
        // 4 partitions across 2 owners: owner0={0,1}, owner1={2,3}. One
        // subscriber claims both blocks so it drains store0 and store1, doing
        // both the forward (from store0) and the delivery (from store1).
        let state =
            QuiverPartitionDispatchTopic::open(name.clone(), config_with_owners(&dir, 4, 2))
                .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub = handle
            .subscribe(pd_mode(vec![0, 1, 2, 3]), SubscriberOptions::default())
            .unwrap();

        // Persist one partition-0 bundle in owner0's store (the first publish
        // flushes it), then reassign partition 0 to owner1.
        handle.publish(Arc::new(tagged_logs(0))).await.unwrap();
        handle.reassign_partition(0, 1).unwrap();

        // The residual is drained from store0, forwarded to store1, and delivered
        // from store1 with its partition tag intact.
        let got = drain_n(&mut sub, 1).await;
        assert_eq!(got, vec![0]);
    }

    // reassign_partition validates its arguments and rejects use after close.
    #[tokio::test]
    async fn durable_reassign_validates_arguments() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-reassign-validate".into();
        let state =
            QuiverPartitionDispatchTopic::open(name.clone(), config_with_owners(&dir, 4, 2))
                .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        match handle.reassign_partition(4, 0) {
            Err(Error::PartitionOutOfRange {
                partition,
                num_partitions,
            }) => {
                assert_eq!(partition, 4);
                assert_eq!(num_partitions, 4);
            }
            other => panic!("expected PartitionOutOfRange, got {other:?}"),
        }

        match handle.reassign_partition(0, 2) {
            Err(Error::OwnerOutOfRange { owner, num_owners }) => {
                assert_eq!(owner, 2);
                assert_eq!(num_owners, 2);
            }
            other => panic!("expected OwnerOutOfRange, got {other:?}"),
        }

        // A valid reassignment succeeds.
        handle.reassign_partition(0, 1).unwrap();

        handle.close();
        match handle.reassign_partition(0, 1) {
            Err(Error::TopicClosed) => {}
            other => panic!("expected TopicClosed, got {other:?}"),
        }
    }

    // The placement snapshot round-trips through persist/load, and load returns
    // None (so the caller falls back to the static placement) when the file is
    // missing or the topology changed (durable-dispatch Phase 3).
    #[test]
    fn placement_snapshot_round_trips_and_validates() {
        let dir = TempDir::new().unwrap();
        let topic_dir = dir.path().join("topic");
        std::fs::create_dir_all(&topic_dir).unwrap();

        // Fresh topic: no snapshot yet.
        assert!(load_placement(&topic_dir, 4, 2).is_none());

        let overlay = RoutingOverlay {
            partition_owner: vec![1, 0, 1, 1],
            partition_generation: vec![0, 0, 3, 0],
        };
        persist_placement(&topic_dir, &overlay, 2).unwrap();

        let restored = load_placement(&topic_dir, 4, 2).expect("snapshot restores");
        assert_eq!(restored.partition_owner, vec![1, 0, 1, 1]);
        assert_eq!(restored.partition_generation, vec![0, 0, 3, 0]);

        // A topology change invalidates the snapshot: fall back to static.
        assert!(load_placement(&topic_dir, 4, 3).is_none());
        assert!(load_placement(&topic_dir, 8, 2).is_none());
    }

    // A reassignment survives restart: reopening the same directory restores the
    // routing overlay from the persisted snapshot, so the moved partition still
    // routes to its new owner rather than reverting to the static placement
    // (durable-dispatch Phase 3).
    #[tokio::test]
    async fn durable_reassign_survives_restart() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-reassign-restart".into();
        // owner0={0,1}, owner1={2,3} statically.
        let cfg = config_with_owners(&dir, 4, 2);

        // Run 1: move partition 0 to owner1, persisting the placement.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name.clone(), state).unwrap();
            handle.reassign_partition(0, 1).unwrap();
            handle.close();
        }

        // Run 2: reopen; the restored overlay must still route partition 0 to
        // owner1. Publishing 0, 1, 2 fresh, owner0's subscriber sees only the
        // un-moved partition 1, while owner1's subscriber sees the moved 0 and
        // the static 2.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name, state).unwrap();
            let mut sub0 = handle
                .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
                .unwrap();
            let mut sub1 = handle
                .subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default())
                .unwrap();

            for p in [0u32, 1, 2] {
                handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
            }
            handle.close();

            let mut got0 = drain(&mut sub0).await;
            let mut got1 = drain(&mut sub1).await;
            got0.sort_unstable();
            got1.sort_unstable();
            assert_eq!(got0, vec![1], "partition 1 stays on owner0");
            assert_eq!(got1, vec![0, 2], "partition 0 moved to owner1 and survived restart");
        }
    }

    // With fewer owners than partitions, multiple partitions share one owner
    // store; each is delivered to the store's owner with its partition tag
    // recovered from the bundle's user_meta (one quiver per owner, D24).
    #[tokio::test]
    async fn durable_shares_owner_store_across_partitions() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-shared".into();
        // 4 partitions across 2 owners: balanced -> owner0={0,1}, owner1={2,3}.
        let state =
            QuiverPartitionDispatchTopic::open(name.clone(), config_with_owners(&dir, 4, 2))
                .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        // Each subscriber claims a whole owner block (one store).
        let mut sub0 = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
        let mut sub1 = handle
            .subscribe(pd_mode(vec![2, 3]), SubscriberOptions::default())
            .unwrap();

        for p in [0u32, 1, 2, 3, 0, 2] {
            handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
        }
        handle.close();

        // Partitions 0 and 1 (sharing owner0's store) are recovered distinctly;
        // likewise 2 and 3 on owner1.
        let mut got0 = drain(&mut sub0).await;
        let mut got1 = drain(&mut sub1).await;
        got0.sort_unstable();
        got1.sort_unstable();
        assert_eq!(got0, vec![0, 0, 1]);
        assert_eq!(got1, vec![2, 2, 3]);
    }

    // A subscriber must claim every partition an owner holds (a whole owner
    // block), since one store is drained by one subscriber.
    #[tokio::test]
    async fn durable_partial_owner_block_rejected() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-partial".into();
        // owner0={0,1}, owner1={2,3}.
        let state =
            QuiverPartitionDispatchTopic::open(name.clone(), config_with_owners(&dir, 4, 2))
                .unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        // Claiming only partition 0 leaves owner0's partition 1 unclaimed.
        match handle.subscribe(pd_mode(vec![0]), SubscriberOptions::default()) {
            Err(Error::PartitionOwnerBlockIncomplete { partition, owner }) => {
                assert_eq!(partition, 1);
                assert_eq!(owner, 0);
            }
            Ok(_) => panic!("expected PartitionOwnerBlockIncomplete, got Ok"),
            Err(e) => panic!("expected PartitionOwnerBlockIncomplete, got {e:?}"),
        }
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
            let mut sub = handle
                .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
                .unwrap();
            handle.close();
            let mut got = drain(&mut sub).await;
            got.sort_unstable();
            assert_eq!(got, vec![0, 0, 1]);
        }
    }

    // With multiple partitions sharing one owner store, the partition tag must
    // survive restart via the bundle's user_meta (the engine no longer implies
    // the partition). A clean close() finalizes to segments, where user_meta is
    // persisted, so the reopened owner recovers each partition distinctly.
    #[tokio::test]
    async fn durable_restart_recovers_partitions_from_user_meta() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-restart-shared".into();
        // 4 partitions, 1 owner: all partitions share owner0's single store, so
        // the partition is known only from user_meta, not the engine index.
        let cfg = config_with_owners(&dir, 4, 1);

        // Run 1: publish across all four partitions, never consume.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name.clone(), state).unwrap();
            for p in [0u32, 3, 1, 2, 3] {
                handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
            }
            handle.close();
        }

        // Run 2: reopen; the single owner's subscriber recovers every partition
        // tag from user_meta persisted in the finalized segment.
        {
            let state = QuiverPartitionDispatchTopic::open(name.clone(), cfg.clone()).unwrap();
            let broker = TopicBroker::<OtapPdata>::new();
            let handle = broker.create_topic_with_state(name, state).unwrap();
            let mut sub = handle
                .subscribe(pd_mode(vec![0, 1, 2, 3]), SubscriberOptions::default())
                .unwrap();
            handle.close();
            let mut got = drain(&mut sub).await;
            got.sort_unstable();
            assert_eq!(got, vec![0, 1, 2, 3, 3]);
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

        let mut sub = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
        for p in [0u32, 1, 0] {
            handle.publish(Arc::new(tagged_logs(p))).await.unwrap();
        }
        handle.close();

        let mut got = drain(&mut sub).await;
        got.sort_unstable();
        assert_eq!(got, vec![0, 0, 1]);
    }

    // Rapid publishes to a single partition are batched (most do not flush a
    // segment of their own), yet every message is delivered without loss.
    #[tokio::test]
    async fn durable_batches_rapid_publishes_without_loss() {
        let dir = TempDir::new().unwrap();
        let name: TopicName = "durable-batch".into();
        let state = QuiverPartitionDispatchTopic::open(name.clone(), config(&dir, 2)).unwrap();
        let broker = TopicBroker::<OtapPdata>::new();
        let handle = broker.create_topic_with_state(name, state).unwrap();

        let mut sub = handle
            .subscribe(pd_mode(vec![0, 1]), SubscriberOptions::default())
            .unwrap();
        for _ in 0..20 {
            handle.publish(Arc::new(tagged_logs(0))).await.unwrap();
        }
        handle.close();

        let got = drain(&mut sub).await;
        assert_eq!(got.len(), 20, "all batched publishes must be delivered");
        assert!(
            got.iter().all(|&p| p == 0),
            "every message is partition 0: {got:?}"
        );
    }
}
