# Partition-Dispatch, Split-by-Key, and Optional Durability

This document designs the engine infrastructure that the vertically-integrated
ingest queue ([`ingest-queue-design.md`](./ingest-queue-design.md)) needs before
its Phase 1 can proceed: a way to (1) split a telemetry batch by a key into
partitions, (2) deliver each partition deterministically to a stable owner
(load-balancing with key affinity), and (3) do so with durability as an
*optional, swappable* property -- not a requirement. It was extracted when
Phase 1 (shuffle by `metric_name` + registry durability) surfaced that the
engine has no key-deterministic cross-core dispatch and no durable topic
backend, and that both should be built on shared infrastructure rather than
bespoke parts.

> Status: design ratified; the **in-memory core and the durable backend are both
> implemented end-to-end**. The orthogonal-axis model (D27), the layer
> decomposition, and all component decisions are **ratified** (D18-D28). See
> "Decisions". Names are provisional. **Layer A (split-by-key), Layer C
> (placement map, the `Partitioned` seam, the in-memory partition-dispatch topic,
> the load feedback loop, and the exporter/receiver/config/controller wiring), and
> Layer B (the Quiver durable backend) are implemented and tested**, with the load
> feedback loop driven end-to-end by a real aggregating owner (the L2 event-time
> windower).
>
> **Layer B status (D21/D24): implemented.** The durable backend is the additive
> backend behind the *same* `TopicState` interface (D27): a topic with
> `backend: quiver` and `num_partitions` persists published data to durable
> Quiver stores (`crates/otap/src/quiver_topic.rs`,
> `QuiverPartitionDispatchTopic`), survives restart, and resumes subscribers from
> durable progress. The backend now uses **one Quiver engine per owner** (D24's
> production option): the `N` partitions are placed across `M` owners
> (`num_owners`, default `N`) by a static `balanced(N, M)` map, each owner's store
> interleaves the partitions it holds, and the partition is stamped into a generic
> opaque **per-bundle `user_meta`** in Quiver (the Phase 1 enabling primitive) so
> a drained bundle's partition is recovered without re-deriving its key. `M == N`
> reproduces the original one-engine-per-partition layout. Publishes are durable
> on return via the WAL, and an owner's segment flush is **batched**: at most once
> per a short interval so rapid publishes do not each finalize a segment. The
> controller arms that previously rejected `TopicBackendKind::Quiver` are now
> filled, and the durable backend is reached through the `DurableDispatchPayload`
> trait so the generic engine and controller stay free of quiver and OTAP
> specifics. **Phase 3 progress:** (1) the WAL now stamps the opaque per-bundle
> `user_meta` (partition + lease generation) into a versioned v2 entry, so a
> partition survives WAL replay after an unclean stop; (2) **live partition
> reassignment works**: `reassign_partition` repoints a partition's new publishes
> to a different owner store and bumps its lease generation, and the deposed owner
> drains-and-forwards the partition's residual to the new owner's store; and (3)
> **reassignments survive restart**: the routing overlay (owners + generations)
> is persisted as an atomically-rewritten snapshot and restored on `open()`.
> **Phase 3 progress (I4): the load-balancing loop is closed for both backends.**
> The controller computes the static `balanced(N, M)` map and assigns each
> partition-dispatch receiver its owned partitions, so `owned_partitions` is
> optional (half 1). It runs a `PlacementScheduler` on a background thread for
> in-memory and durable (quiver) topics alike: owners report load through the
> topic binding, and the scheduler reconciles to the topic's real routing --
> including a durable topic's restored `placement.v1` overlay -- then rebalances
> by moving whole partitions off overloaded owners (half 2). The scheduler
> balances *subscribers*, not quiver stores: the durable topic maps a subscriber
> to the stores it drains via `subscriber_routing` and
> `reassign_partition_to_subscriber`. I4 is complete; the remaining
> durable-dispatch work is off I4 (the durable registry and the D26 logs key).

## Motivation

The shuffle co-locates all data for a key on one owner so that owner's per-core
state -- the metric type registry, the event-time windower's aggregators, the
trace assembler -- is the single writer for that key. This is the property that
makes type-conflict flagging, delta-to-cumulative conversion, and trace assembly
local and race-free. The *same* shuffle, run **without** durable storage, is a
large-scale aggregating telemetry SDK: it shuffles, aggregates (by
`metric_name`, `trace_id`, or `tenant`), and load-balances entirely in memory.
With durable storage it is the disconnection-tolerant appliance. Durability is
the difference, and it is a configuration, not a redesign.

Two axes, independent (D27):

- **Delivery mode** -- how data routes to subscribers: `broadcast`, `balanced`
  (load-balance, no key affinity), or **`partition-dispatch`** (load-balance
  *with* key affinity: `hash(key) mod N` spreads keys across owners while pinning
  each key to one owner). Partition-dispatch is load-balancing and aggregation in
  one mechanism.
- **Backend / durability** -- whether delivery is persisted: `in-memory`
  (default) or `quiver` (durable, at-least-once, restart-replayable).

The cross-product is the agent's operating range:

| delivery \ backend | in-memory                             | quiver (durable)                 |
| ------------------ | ------------------------------------- | -------------------------------- |
| partition-dispatch | large-scale aggregating telemetry SDK | disconnection-tolerant appliance |
| balanced           | stateless load-balancer               | durable work queue               |

The shuffle/aggregate/load-balance path runs **identically** over both backends;
durability only changes the guarantee (in-memory: effective-once within a
process lifetime; durable: also across restart). Three capabilities are missing
today, none of which require durability: split-by-key, a partition-dispatch
delivery mode, and a placement map. Durability is then an *additive* backend.

The topic broker is the natural substrate -- it already abstracts a backend
(`InMemoryBackend` today; `TopicBackendKind::Quiver` reserved) behind a trait
seam, so "optional durability" is exactly "which backend." The shard key is a
data-sourced *sub-tenant identifier* (see the agent multitenancy design's
"Split-by-key and sub-tenant identifiers" section -- `agent-multitenancy-design.md`
in the Telemetry-Collection-Spec repo -- and ingest-queue D3).

## Decomposition: a durability-independent core plus an optional backend

The capabilities factor so that the entire shuffle/aggregate/load-balance path
is built and tested **in memory first**, with durability added as a backend swap.
The master diagram in
[`metrics-appliance-design.md`](./metrics-appliance-design.md#layered-architecture)
shows this in-memory path end to end, including the partition-dispatch shuffle and
the load-balancing feedback loop designed here.

```text
  Core (durability-independent):

  Layer A  Split-by-key         rows --f(key col)--> partition-tagged sub-batches
           (pdata / batch proc) radix partition via the OTAP selection-mask
                                cascade (the filter_otap_batch family)
                                    |
                                    v
  Layer C  Partition-dispatch   stable partition->owner ownership (new
           + placement          subscription mode) + placement map,
                                FIRST on the in-memory backend
                                    |
                                    v  (optional, additive)
  Layer B  Quiver TopicBackend  the same dispatch/ownership, made durable
           (engine/topic)       (implements reserved TopicBackendKind::Quiver)
```

- **Layer A** (split-by-key) is foundational and durability-independent; it
  reuses the cascade machinery already built for the admission processor's
  time-window filter, and is independently useful for descriptor/tenant batching.
- **Layer C** (partition-dispatch + placement) is the shuffle proper. Built on
  the **in-memory backend first**, A+C is a complete end-to-end aggregating
  load-balancer with no durability or quiver complexity -- the runnable prototype.
- **Layer B** (Quiver `TopicBackend`) is the **optional** durability plug-in: it
  implements the *same* partition-dispatch/ownership semantics durably, and is
  selected per location (per D27). It also backs per-owner state durability,
  replacing any bespoke WAL.

## How it maps onto existing primitives

| Capability                           | Engine primitive                                                         | Status                                   |
| ------------------------------------ | ------------------------------------------------------------------------ | ---------------------------------------- |
| Size-based batch split/merge         | `otap::groups` + `transform::split` (contiguous range split)             | exists                                   |
| Selection-mask cascade (root->child) | `otap::filter::filter_otap_batch` (+ `filter_metrics_time_window`)       | exists (used by filter + admission)      |
| Topic backend seam                   | `TopicBackend` / `TopicState` / `SubscriptionBackend`                    | exists; in-memory + Quiver backends      |
| Durable buffer                       | `quiver` (WAL + Arrow IPC segments, at-least-once, multi-stream, budget) | exists (experimental)                    |
| Reserved durable topic kind          | `TopicBackendKind::Quiver`                                               | implemented (durable partition-dispatch) |
| Split-by-key (Layer A)               | `otap::partition` + `partition` processor                                | implemented + tested                     |
| Quiver TopicBackend (Layer B)        | `otap::quiver_topic::QuiverPartitionDispatchTopic`                       | implemented + tested                     |
| Partition-dispatch + placement (C)   | `PartitionDispatchBackend` + placement map + load feedback loop          | implemented + tested                     |

## Layer A: Split-by-key

**Goal.** Given an OTAP batch and a configured key, partition the batch into
sub-batches such that all rows sharing a key value land in the same sub-batch,
tagged with a partition index. This partition is the ingest queue's **bucket**
(`ingest-queue-design.md` D5); the two terms name the same logical shard.

**Mechanism: selection-mask radix cascade, not range split.** The existing
`transform::split` produces *contiguous* row ranges over a parent/id-sorted
table -- correct for size-based batching, wrong for key grouping (rows sharing a
key are generally non-contiguous). Split-by-key is instead a radix partition
built on the **selection-mask cascade** -- the same `filter_otap_batch` family
the admission processor's `filter_metrics_time_window` already uses:

1. Compute a **partition index** per *root* row from the key column:
   `partition = part_fn(key) mod N` (power-of-two `N`, so `& (N-1)`).
2. For each partition `p`, build a boolean mask over the root (`partition == p`)
   and apply the cascade filter, which prunes child tables (attributes,
   data points, exemplars, events/links) by parent-id integrity.

For the ingest queue's keys the key column is always on the **root** table --
`metric_name` on `UnivariateMetrics`, `trace_id` on `Spans`, `trace_id` on the
`Logs` root for correlated logs -- so the existing root->child cascade applies
directly. (Splitting by a *parent-table* key -- resource or scope, for tenant
batching -- is a generalization that cascades from an interior table; designed
for, not required by the ingest queue.)

**Per-signal partition function** (ingest-queue D3 refinement):

- *Metrics:* `hash(metric_name) & (N-1)` -- names are skewed, so hashing spreads
  them.
- *Traces / correlated logs:* the low 56 bits of `trace_id` (its right-most 7
  bytes) `& (N-1)` -- W3C-random by the OTel randomness definition, no hash.
- *Tenant / resource / scope (general split-by-key):* value-match a descriptor
  condition to a *named* partition (low cardinality, for isolation) rather than
  a hash bucket.

**Home: a dedicated `partition` processor node.** Split-by-key is exposed as its
own composable OTAP processor node rather than folded into the size-based batch
processor, which keeps that node's intricate slot, timer, and ack/nack batching
logic separate from the stateless key split (D25). The operation is columnar
over OTAP, a group-by over a root column with child rows pruned by parent id, so
it reuses the `filter_otap_batch` cascade. Transport-optimized ids must be
decoded first, as the admission processor and filter processor already do.

**Output.** Each sub-batch is emitted as its own `OtapPdata` carrying its
partition index, so Layer C can route it without re-deriving the key. The tag
travels on the request `Context` via `Context::partition`, alongside `peer_addr`
and source-node tagging, so it survives to the dispatch hop.

**Optimization (later).** N independent cascade passes are simple and correct
for a prototype; a single scatter pass that distributes rows into N builders in
one traversal is the obvious optimization once the path is proven.

## Layer C: Partition-dispatch and placement (in-memory first)

> **Implemented (in-memory core, end-to-end).** The static `PartitionPlacement`
> map (`crates/engine/src/topic/placement.rs`), the `Partitioned` routing seam
> (`crates/engine/src/topic/partitioned.rs`, implemented for `OtapPdata` in
> `crates/otap`), and the in-memory `PartitionDispatchTopic` /
> `PartitionDispatchBackend` with `SubscriptionMode::PartitionDispatch`
> (`crates/engine/src/topic/`) are built and broker-tested. The pipeline is wired
> end-to-end: the topic exporter preserves the partition tag on publish, the
> topic receiver subscribes with its owned partitions
> (`TopicSubscriptionConfig::PartitionDispatch`), and the controller creates a
> partition-dispatch topic from `TopicSpec.num_partitions`. The durable backend
> (Layer B) has since been built behind this same interface, described below.

**Goal.** Deliver each partition deterministically to a stable, *exclusive*
owner, so the owner's per-core state is the single writer for that partition's
keys. This is the shuffle proper, and it is built on the **in-memory backend
first** -- it needs no durability (D27).

**The gap.** The current balanced subscription is a *single shared queue* that
consumers race on; there is no stable "subscriber N of M" identity, so a
partition cannot be pinned to an owner today. Broadcast has a per-subscriber
cursor and a `BroadcastSubscriberId` type, but it fans out to all subscribers.
Neither gives "this partition, that owner."

**Mechanism (D22).** A new subscription mode in which a subscriber *claims*
one or more partitions exclusively, giving a stable `partition -> subscriber`
mapping:

- Publish reads the item's partition tag (from Layer A) and enqueues it to the
  owning subscriber's queue (per-partition or per-owner queues, replacing the
  single shared group queue for this mode).
- A subscriber declares the partitions it owns at subscribe time; the set comes
  from the **placement map**.

This is the *opposite* of balanced (exclusive ownership, not sharing), and it is
backend-independent: the in-memory backend uses in-memory per-partition queues;
the quiver backend (Layer B) uses durable per-partition streams with the same
ownership.

**Placement map (the P2/P3 seam).** `partition -> owner` lives in the
controller: static `partition -> core` first; load-aware,
churn-minimizing rebalancing and `partition -> node` later. Ownership carries a
**lease with a monotonic generation** (stamped into quiver bundle metadata when
durable) so a deposed owner's late writes are fenced (Centrifuge/Slicer).
Durable reassignment uses the M3 `Initializing`/`Leaving` handoff, replaying the
partition's durable streams; the in-memory case simply reassigns (unflushed data
is best-effort, per D27).

**Load balancing across CPUs (the metrics-gateway case).** When the engine runs
as a metrics gateway aggregating on behalf of a large SDK fleet, the placement
map is the load balancer for the stateful path, and balancing partition *count*
is not enough. Metric load is heavily skewed: a few names carry most of the
series and points, so an even count of partitions per core still leaves some
cores hot and others idle. Four refinements make placement load-aware, all
implemented on `PartitionPlacement` (`crates/engine/src/topic/placement.rs`):

- **Load signal.** The dominant cost is per-series aggregation state, one
  aggregator per `(series, window)`. The owner already holds that state, so it
  reports a cheap per-partition weight, principally `active_series` plus a
  points-per-second term, with a heavier multiplier for exponential histograms.
  Owners report weights to the controller, which rebalances on them.

- **Weighted placement (LPT).** Assigning N weighted partitions to M cores to
  minimize the busiest core is minimum-makespan scheduling, which is NP-hard, so
  placement uses greedy longest-processing-time-first: place partitions
  heaviest-first, each onto the currently-lightest core. This is a
  4/3-approximation in `O(n log n)`, the `weighted` constructor.

- **Churn-minimizing rebalance.** On load drift or a core-count change the map is
  not recomputed from scratch, which would dump every core's aggregation state.
  Instead the busiest core sheds the fewest partitions to the coldest core until
  the maximum is within a configured fraction of the mean, and every move
  strictly lowers the maximum so the loop converges. Keys never change partition,
  so only the `partition -> owner` map moves: `rebalance_weighted`.

- **Hot-key sub-partitioning.** A partition is indivisible, so if one metric name
  alone exceeds a core's budget no placement helps. Those partitions are reported
  by `hot_partitions`, and the fix is to sub-partition that name by series
  identity, hashing the name together with the series attributes over extra radix
  bits so the name's series spread across cores. This stays correct because
  single-writer is **per series**, not per name, so each series still has one
  writer. The cost is that the name's series are no longer co-located, so its type
  authority and any cross-series attribute reduction move to a later merge stage.
  The common case of cold names stays whole, and N is sized (ingest-queue D5) so
  the hottest unsplit partition is tolerable.

Two structural points make this work in memory:

- **Pinned aggregation, stolen stateless work.** The engine is thread-per-core
  with work-stealing, but a series' aggregator is pinned to its owner core: two
  cores writing one series would corrupt the cumulative conversion. So the
  stateless stages -- decode, the admission filter, split-by-key -- work-steal
  freely for transient balance, while the stateful aggregation balances only
  through placement, deliberately and at coarse grain.

- **Rebalance at window boundaries.** Moving a partition mid-window would migrate
  live aggregator state. A reassignment instead takes effect at a window close:
  the old owner flushes the closed window and the new owner opens the next one,
  so the handoff carries no open-window aggregate. It does carry the per-stream
  cumulative anchor, which the gaining owner takes in memory here or restores from
  the durable completeness checkpoint on the quiver backend, so the move injects
  no reset into a cumulative series; see the appliance design's D29. Aggregation
  windows are the natural rebalance points; the durable backend later upgrades
  this to the lease and generation handoff above.

**The load feedback loop.** The signal above is closed into a loop by
`PlacementCoordinator` (`crates/engine/src/topic/load_feedback.rs`):

1. **Measure (owner).** Each owner buckets the load of the keys it aggregates by
   the partition tag the dispatch delivered, accumulating a `PartitionLoad`
   (`active_series` plus `points`) per owned partition in a
   `PartitionLoadTracker`. `active_series` is read from the aggregator's
   per-partition series state and `points` from the data-point rows of the
   batches received, so it is a cheap update, not a scan; `snapshot` resets the
   interval `points` while keeping the `active_series` gauge.
2. **Report.** Owners send their per-partition `PartitionLoad`s to the
   coordinator, which merges them (latest wins per partition) into a global
   weight vector via the configured `LoadWeights`.
3. **Decide.** On a tick or a threshold breach the coordinator runs the
   churn-minimizing `rebalance` (steady state) or a full LPT `replan` (a large
   owner-count change), and surfaces indivisible hot partitions for key
   sub-partitioning.
4. **Apply.** The coordinator emits the minimal set of `PartitionMove`s -- a
   `partition` changing `from` one owner `to` another -- which the runtime applies
   to the live topic via `TopicHandle::apply_move` (or `reassign_partition`),
   repointing the topic's `partition -> owner` routing. Keys never move; only the
   `partition -> owner` map changes. Messages already enqueued for the previous
   owner stay in its queue, so applying a move at an aggregation window boundary
   hands off no overlapping per-series state.

The coordinator is the feedback brain and `PlacementScheduler` is its driver:
owners send `PartitionLoadTracker` snapshots through a `LoadReportSender`, and the
scheduler's `tick` drains them, runs the coordinator's rebalance, and applies each
move to the live topic via `apply_move`. A scheduling thread calls `tick` on a
cadence aligned with the aggregation window so a move's handoff carries no live
state. The **event-time window processor**
(`crates/core-nodes/src/processors/event_time_window_processor/`, the L2 windower)
is the first real owner to drive this loop: it keeps an independent windower per
partition tag, so a partition's `active_series` is exactly its aggregator's stream
count, reports that plus the interval `points` through a `LoadReportSender` on each
telemetry collection, and a test shows skewed series counts rebalancing a live
partition-dispatch topic. The durable backend later adds leases and generation
fencing to the apply step so a deposed owner's late writes are fenced.

### Controller-driven placement lifecycle

The load feedback loop computes reassignments, and two further steps make placement
controller-driven rather than hand-specified. Both are in scope for the in-memory
core.

**Initial assignment.** The controller builds the placement map from the topic's
`num_partitions` and the receiver replica count, using `PartitionPlacement::balanced`
at first and `weighted` once a load history exists, and assigns each replica the
partitions that map to it as its `PartitionDispatch` owned set. This replaces
today's hand-specified owned partitions in receiver config. A replica's owner
identity is its subscriber slot, and the controller keeps the replica-to-owner
mapping so a later move addresses the right subscribers.

**Dynamic owned-set propagation.** A `PartitionMove` reaches the affected owners
through the routing repoint, not a separate owned-set edit. Applying a move at an
aggregation window boundary repoints the backend's partition-to-owner routing so
future publishes go to the new owner. For the in-memory backend the gaining
subscriber then reads its own per-owner queue and the losing one stops receiving
the partition, so no owned-set mutation or notification is needed. For the durable
backend `reassign_partition_to_subscriber` routes the partition into a store the
gaining subscriber drains, which delivers it through the whole-store drain and
fence-and-forward. Sequencing the change at a window boundary means the handoff
carries no open-window aggregate, because the old owner has flushed the closed
window and the new owner opens the next one; the per-stream cumulative anchor
travels with the partition through the completeness checkpoint of the appliance
design's D29, so the move injects no reset.

For the durable backend of Layer B, the same apply step stamps the lease
generation so a deposed owner's late writes are fenced. The in-memory path
reassigns directly and treats any unflushed data as best-effort, per decision D27.

**Relationship to tenancy.** When the multitenancy descriptor system lands,
condition-routing (named partitions for tenant/resource isolation) and
hash-partitioning (N buckets for co-location) are the same dispatch mode with
two partition functions over the same request/data projection. Layer C is
designed to accept either; the ingest queue uses the hash-partition form.

## Layer B: Quiver TopicBackend (optional durability)

> **Implemented (one quiver per owner).** `crates/otap/src/quiver_topic.rs`
> (`QuiverPartitionDispatchTopic`) implements `TopicState<OtapPdata>` durably; the
> controller constructs it through the `DurableDispatchPayload` trait when a topic
> sets `backend: quiver` and `num_partitions`. The backend uses one Quiver engine
> per owner over a static `balanced(N, M)` placement (`num_owners`, default `N`);
> each bundle's partition rides Quiver's opaque per-bundle `user_meta`. Segment
> flushes are batched per owner. Live reassignment works via a dynamic routing
> overlay that is persisted as a snapshot and restored on `open()`, so
> reassignments survive restart (Phase 3, below).

**Goal.** Make the *same* partition-dispatch topic durable: published items are
persisted before acknowledgement and survive restart; subscribers resume from
durable progress. This is an **additive backend** selected per location (D27),
not a separate mechanism.

**Mechanism (implemented).** The durable topic implements the same `TopicState`
and `SubscriptionBackend` surface against `quiver`:

- `TopicState::publish*` -> route the message to its partition's owner via the
  static placement, then quiver `ingest(bundle)` into that owner's store, durable
  on return via the WAL, stamping the partition into the bundle's opaque
  `user_meta`. The segment flush that makes data pollable is batched per owner (at
  most once per a short interval, plus a final flush on close), so rapid
  publishes accumulate into one segment instead of finalizing one per publish.
  The OTAP->bundle adapter is shared from `otap_df_otap::quiver_bundle`, moved out
  of the durable buffer processor so both use one implementation.
- the partition subscription -> the owner drains the owner stores it claimed (a
  subscription claims whole owner blocks), recovering each bundle's partition from
  `user_meta` since one store interleaves the owner's partitions.
- `SubscriptionBackend::poll_recv_delivery` -> quiver `poll_next_bundle`, with the
  delivery's `commit` acking the bundle and `abort`/`abandon` deferring it for
  redelivery (`BundleHandle::{ack, defer}`).

**QoS mapping** (ingest-queue D6): the durable topic's `retention` setting maps
to quiver's `RetentionPolicy` -- `Backpressure` (lossless) vs `DropOldest`
(loss-tolerant) -- wired to the `DiskBudget`.

**Wiring (implemented).** `TopicBackendKind::Quiver` was previously rejected at
startup. The controller's `declare_topic` now constructs a quiver-backed
`TopicState` for a partition-dispatch topic through the `DurableDispatchPayload`
trait and registers it via `TopicBroker::create_topic_with_state`; config
validation requires a quiver topic to set `num_partitions` and a `quiver`
settings block.

**Consequences.** Durability becomes a backend setting on the topic, so the
existing `durable_buffer_processor` reduces to "set the topic backend to quiver"
and can be deprecated once the backend reaches parity (no node-plus-WAL, no
topology fork). The same backend **backs per-owner state durability** -- the
metric type registry persists through a small Quiver-backed topic (snapshot +
log), replayed on startup -- so no hand-rolled WAL is needed.

## Phase 3: leases, generation fencing, and reassignment handoff

Phases 1 and 2 delivered the durable one-quiver-per-owner backend over a
**static** placement. Phase 3 makes the placement **dynamic** so a partition can
move between owners at runtime without losing or double-processing data. It is
the durable counterpart of the in-memory reassignment already wired for Layer C,
and it is the piece the load-feedback rebalancer needs before the durable backend
can rebalance under load.

**Status: implemented** (increments I1-I3 plus placement persistence): the WAL
`user_meta` stamping (below), a dynamic per-partition routing overlay with lease
generations, the drain-and-forward handoff, and a persisted placement snapshot so
reassignments survive restart. **Controller wiring (I4) is now complete for both
backends** -- initial placement and the scheduler loop, including the durable
topic reconciling to its restored overlay and balancing at subscriber
granularity through `reassign_partition_to_subscriber`.

### Current substrate (what Phases 1 and 2 already provide)

- A generic opaque per-bundle `user_meta` in quiver, recorded in the segment
  manifest **and (Phase 3, done) the v2 WAL entry**, exposed on the drain path as
  `BundleHandle::user_meta()` (`crates/quiver/`), so it survives WAL replay after
  an unclean stop.
- One quiver engine per owner over a static `balanced(N, M)` placement, with the
  partition packed into `user_meta` at publish and recovered on drain (Phase 2,
  `crates/otap/src/quiver_topic.rs`).
- `pack_user_meta(partition, generation)` packs the lease generation into the
  high 32 bits of `user_meta` and `unpack_partition` reads the low 32;
  `unpack_generation` reads the high 32 (stamped and carried today; the read-side
  comparison is used by cross-node fencing later).
- `reassign_partition` **now repoints the dynamic routing overlay and bumps the
  generation** (it no longer rejects the move); the drain path fences and forwards
  residual.

### Why reassignment is hard on the durable backend

One quiver per owner binds a partition's persisted data to its owner's store.
When partition `p` moves from owner A to owner B, the bundles A already persisted
for `p` live in A's engine, while new publishes for `p` must go to B's engine. B
drains only its own store, so A's residual `p` bundles are unreachable to B
unless they are moved. The in-memory backend avoids this because reassignment
just repoints an in-memory queue.

### Mechanism

Three pieces compose, following Centrifuge and Slicer (ingest-queue "Leases and
fencing"):

1. **Lease generation.** Each partition carries a monotonic generation in the
   routing. `reassign_partition` bumps that generation instead of rejecting the
   move, and the publish path stamps the current generation into the `user_meta`
   bits `pack_user_meta` already reserves.
2. **Drain-and-forward handoff.** After `p` moves from A to B, new publishes for
   `p` route to B's store at the new generation. A drains its residual `p`
   bundles at the old generation, re-ingests them into B's store, then acks them
   in A's store, migrating the partition tail without quiver extracting a stream.
   Sequenced at an aggregation-window boundary per Layer C, the forwarded tail is
   the closing window, so no live per-series state crosses.
3. **Generation fencing.** A write or ack carrying a generation older than the
   partition's current generation is rejected, so a deposed owner slow to observe
   the move cannot corrupt the new owner's state. The generation rides in
   `user_meta`, so a fence compares a bundle's unpacked generation against the
   routing's current generation for that partition.

**How the three questions resolved (in-process implementation).** Because this
topic runs in one process and publishing is centralized through the topic, a live
write always carries the partition's *current* owner and generation, so there is
no deposed writer to fence on the write path. The only stale data is residual
already persisted in a former owner's store, so the fence lives on the **drain
path**: a bundle drained from a store that is no longer the partition's current
owner is forwarded to the current owner's store and acked in the old one. The
forwarding owner reaches the new store directly through `Shared::engines`, since
the topic owns every engine. The generation is **per-partition**. In one process
the owner-identity check is the authoritative fence and the generation is stamped
as the durable ownership-epoch marker that a future cross-node fence compares.
The routing overlay is persisted as a snapshot and restored on `open()` (below),
so reassignments survive restart.

### WAL durability of `user_meta` (implemented)

Phase 1 recorded `user_meta` in the segment manifest, not the WAL, matching how
`item_count` behaves, so a partition and generation came back as 0 for bundles
recovered by WAL replay after an unclean stop. **Phase 3 now stamps `user_meta`
into the WAL entry** so a partition and generation survive a crash
(`crates/quiver/`).

The chosen mechanism is **version-aware replay** with self-describing entries.
Rather than keying the entry layout off the file-header version (which breaks
because `WalWriter::open` appends to a pre-existing WAL file instead of rotating,
so a v2-capable writer would write new-format entries into an old-header file),
each entry carries a type byte: `ENTRY_TYPE_RECORD_BUNDLE_V2 = 1` appends
`user_meta` after the slot bitmap, while the legacy `= 0` entry omits it and
recovers `user_meta` as 0. The file header `WAL_VERSION` is bumped to `2` and the
reader accepts `WAL_VERSION_MIN..=WAL_VERSION`, so existing v1 WALs still replay.
`item_count` remains manifest-only for now; a future field takes the next entry
type rather than a breaking re-layout.

### Placement persistence (implemented)

The routing overlay is persisted so reassignments survive restart. Because the
placement is a tiny map rewritten wholesale, it is a **snapshot, not a log**:
Quiver's WAL/segment/compaction machinery is built for high-volume append
streams and would be over-engineered here. `reassign_partition` writes the
updated `(partition -> owner, generation)` map to
`{data_dir}/{topic}/placement.v1` via a temp-file-fsync-rename so the write is
atomic and crash-durable, **before** the in-memory swap takes effect, so a
reassignment is durable before it is observable; a snapshot write failure leaves
the overlay unchanged and reports the move as failed. `open()` restores the
overlay from the snapshot, falling back to
the static `balanced` placement when the file is absent or the topology changed
(`num_partitions` / `num_owners`). The persist/load pair (`persist_placement` /
`load_placement` in `crates/otap/src/quiver_topic.rs`) is a small seam, so it
could later be swapped for a Quiver-backed control stream without touching the
topic logic.

### Implementation status by increment

- **I1 -- WAL `user_meta` durability (done):** `crates/quiver/` `wal/{mod,header,
  writer,reader,replay}.rs`, above.
- **I2/I3 -- generation lease + drain-and-forward (done):**
  `crates/otap/src/quiver_topic.rs`: the dynamic `partition_owner` /
  `partition_generation` `RoutingOverlay`, held in an `ArcSwap` so the publish and
  drain hot paths read it lock-free while `reassign_partition` swaps in an updated
  snapshot (subscribe-time claim state stays under a separate `Mutex`);
  `unpack_generation`; `reassign_partition` (repoint + bump); and fence-and-forward
  in `QuiverDispatchSub::poll_recv_delivery` / `forward_fenced_bundle`.
- **Placement persistence (done):** the overlay is persisted as a snapshot and
  restored on `open()` (above), so a reassignment survives restart.
- **I4 -- controller wiring (done):** the controller computes the initial
  placement and assigns each receiver replica its stores (replacing the
  hand-specified config), and drives reassignment from
  `crates/engine/src/topic/load_feedback.rs` (`PartitionMove`,
  `PlacementCoordinator`, `PlacementScheduler` -> `apply_move`), which the durable
  backend honors through `reassign_partition`. The scheduler runs for durable
  topics too, sized to the store count and reconciled to the restored overlay.

### Open questions -- resolved

- ~~Fence at the write path, the ack path, or both, and what the deposed owner
  does with a fenced bundle.~~ **Resolved:** fence on the drain path (publishing
  is centralized, so there is no stale writer); the deposed owner forwards the
  fenced residual to the current owner's store and acks it in its own.
- ~~How the forwarding owner reaches the new owner's store.~~ **Resolved:**
  directly through `Shared::engines`; the topic owns every engine.
- ~~Whether the generation is per-partition or per-owner-block.~~ **Resolved:**
  per-partition.
- ~~The WAL format-versioning approach for `user_meta`.~~ **Resolved:** per-entry
  type discriminator + `WAL_VERSION` bump to 2 with version-aware replay.

## How the ingest queue rides this

- **Phase 1 shuffle (in-memory).** `metrics_admission` (or its replicas)
  subscribe to a partition-dispatch topic (in-memory backend) keyed by the
  per-signal partitioner; each owner's `TypeRegistry` becomes the sole authority
  for its partitions' keys. No durability required.
- **Phase 1 registry durability (optional, additive).** Switching the registry's
  topic to the Quiver backend persists it (snapshot + log), replayed on startup.
  The `TypeDescriptor`/`observe` API already exists in
  `metrics_admission_processor/registry.rs`.
- **Phase 2 per-bucket durability + placement.** One quiver per owner with
  per-partition Arrow-IPC streams (ingest-queue D5); the controller placement
  map; leases + generation fencing. The durable-dispatch infrastructure this
  needs is now built: one quiver per owner (D24), the persisted routing overlay,
  lease-generation reassignment with drain-and-forward, and the controller
  placement/scheduler wiring (I4). What the ingest queue adds on top is
  subscribing its admission owners to that topic.
- **Raw buffer (L1) and stage-2 feed (L3 of the appliance).** Topics whose
  backend is chosen per location (D27): in-memory for the SDK profile, quiver for
  the durable appliance.

## Decisions

Continuing the shared decision ledger of the ingest-queue and appliance designs
(D1-D17). Status is per-decision: **ratified**, decided and ready to build, or
**implemented**, built and tested. The durable backend (D21/D24), previously
deferred, is now implemented (one quiver per owner) with live, restart-durable
reassignment and controller/scheduler wiring (I4) for both backends, balancing at
subscriber granularity. The remaining durable-dispatch work is off I4: the
durable registry (ingest-queue Phase 1) and the standalone-logs key (D26).

| ID  | Decision                            | Recommendation                                      | Status            |
| --- | ----------------------------------- | --------------------------------------------------- | ----------------- |
| D18 | Layer decomposition + build order   | A split-by-key -> C in-memory dispatch -> B durable | ratified          |
| D19 | Split-by-key mechanism              | selection-mask radix cascade; batch processor       | ratified          |
| D20 | Shard key source                    | data-sourced; descriptor/tenant composes later      | ratified          |
| D21 | Durable backend                     | Quiver durable `TopicState`; durability is additive | implemented       |
| D22 | Partition ownership                 | new partition-claim mode; in-memory first           | ratified          |
| D23 | Placement seam                      | controller map; static->rebalanced; leases          | ratified          |
| D24 | Quiver backend granularity          | one quiver per owner; user_meta carries partition   | implemented       |
| D25 | Split and dispatch packaging        | separate composable nodes; fused fast-path later    | ratified          |
| D26 | Standalone logs (no `trace_id`) key | configured projection; default even spread          | ratified          |
| D27 | Optional, per-location durability   | delivery mode x backend are orthogonal axes         | ratified          |
| D28 | Scope for this effort               | in-memory core (A+C) only; defer Quiver (B)         | ratified          |

### D18. Layer decomposition and build order

**Ratified (this session):** factor the work into a durability-independent core
-- split-by-key (A) and partition-dispatch + placement (C) -- plus an optional
durable backend (B), and build **A -> C on the in-memory backend -> B**. **Why:**
per D27 the shuffle/aggregate/load-balance path does not require durability, so
A+C on the in-memory backend is a complete, runnable end-to-end aggregating
load-balancer (the "large-scale SDK") that can be validated before any quiver
work; durability (B) is then an additive backend swap. **Implication:** the
first runnable prototype is in-memory; the ingest-queue Phase 1 begins on A+C
(in-memory shuffle + authority), and durable registry/buffer arrives with B.

### D19. Split-by-key mechanism

**Decided:** implement split-by-key as a **selection-mask radix cascade**
reusing the `filter_otap_batch` family, not by extending the contiguous,
size-based `transform::split`. Compute `partition = part_fn(root key) mod N`
per root row, then cascade-filter per partition so child tables -- attributes,
data points, exemplars, events and links -- are pruned by parent-id integrity.
**Why:** rows sharing a key are non-contiguous, so the range-split model does
not apply, whereas the mask-cascade already prunes child tables by parent id
and is proven by the admission processor's `filter_metrics_time_window`.
**Home:** a dedicated `partition` processor node that emits each sub-batch as
its own `OtapPdata`. The partition function and `N` live on the split side; each
sub-batch carries its integer partition index on the request `Context` via
`Context::partition`, alongside `peer_addr`, so Layer C routes without
re-deriving the key (couples to D25). **Alternative:** sorting the root by key
then range-splitting adds a full sort and reorders data; rejected.
**Implication:** `N` independent cascade passes initially; a single scatter pass
into `N` builders is the later optimization.

### D20. Shard key source

**Ratified (from facts established this session):** the shard key is
**data-sourced** -- computed from OTAP columns (`metric_name`, `trace_id`) by
Layer A -- and does **not** depend on the multitenancy descriptor system, which
is not yet implemented (only transport-header capture exists today). **Why:** the
shard key is a *sub-tenant identifier* intrinsic to the data, resolved per row;
this is simpler than and orthogonal to header-sourced tenant descriptors.
**Implication:** Layer A needs no descriptor plumbing; when descriptors land,
condition-routing composes as an additional partition function over the same
dispatch mode (couples to ingest-queue D9 and the multitenancy design).

### D21. Durable backend (additive) -- implemented

**Implemented:** `TopicBackendKind::Quiver` is realized as a durable
`TopicState` selected **per location** (D27). Durability is **additive
configuration, not a subsumption**: the same partition-dispatch topic runs over
the in-memory backend, the default with no durability, or the quiver backend, the
durable option, with identical dispatch/ownership semantics. The backend sits
behind the *same* interface, so it was added as a backend swap with no redesign:
`crates/otap/src/quiver_topic.rs` implements `TopicState<OtapPdata>`, the
controller's `declare_topic` constructs it through the `DurableDispatchPayload`
trait, and config validation requires a quiver topic to be partition-dispatch.
The existing `durable_buffer_processor` can now be reduced to "set the topic
backend to quiver" and deprecated once the durable backend reaches parity. Couples
to ingest-queue D4/D6. **Phase 3 done:** stamping `user_meta` into the WAL so a
partition survives WAL-replay after an unclean stop; leases and generation fencing
on reassignment (the drain-and-forward handoff); and persisting the routing
overlay as a snapshot so reassignments survive restart. **Open follow-ups:**
wiring the controller/scheduler to drive reassignment, and the durable registry
half of ingest-queue Phase 1.

### D22. Partition ownership

**Decided:** add a **partition-claim subscription mode** giving a stable,
*exclusive* `partition -> subscriber` mapping. The broker today offers only
`SubscriptionMode::Balanced { group }`, a single shared group queue that
consumers race on with no stable "subscriber N of M" identity, and
`SubscriptionMode::Broadcast`, which has a per-subscriber cursor
(`BroadcastSubscriberId`) but fans out to every subscriber; neither pins a
partition to one owner. A subscriber declares its owned partitions, sourced
from the placement map (D23), at subscribe time, and publish routes by the
item's partition tag to the owning subscriber. Built on the **in-memory backend
first** per D18/D27; the quiver backend (D21) implements the same ownership
durably. **Why:** stable *exclusive* ownership is what makes a
per-core registry or aggregator the single writer for its keys, the opposite of
balanced's sharing, and round-robin cannot provide it. **Implication:** a new
subscription mode and per-partition (or per-owner) queues; the partition tag
travels with the item; ack/nack fan-in across a split batch reuses the batch
processor's existing outbound-slot tracking.

### D23. Placement seam

**Decided:** `partition -> owner` lives in the controller as a placement map --
static `partition -> core` first; load-aware, churn-minimizing rebalancing and
`partition -> node` later (ingest-queue Phases 2-4). For this effort only the
**static in-memory map** is needed; leases with generation fencing and the M3
`Initializing`/`Leaving` durable handoff arrive with Layer B, where they
stamp quiver bundle metadata and replay durable streams. **Why:** the two-level
`key -> partition -> owner` indirection bounds churn and makes rebalancing
possible without rehashing keys (ingest-queue D3 / Auto-sharding).
**Implication:** this effort adds a minimal static placement map; the
controller's lease and key-ownership primitive arrives with durability.

**Status update (Phase 3):** the durable backend's placement is now **dynamic and
restart-durable** at the topic level: `reassign_partition` repoints a per-partition
routing overlay (owner + lease generation), migrates residual by drain-and-forward,
and persists the overlay as a snapshot restored on `open()`. The **controller half**
of this decision is now also done: the controller computes the initial placement
map, assigns each receiver replica its stores, and drives reassignment from the
load-feedback scheduler for both backends, balancing at subscriber granularity via
`reassign_partition_to_subscriber`.

### D24. Quiver backend granularity -- implemented (one quiver per owner)

**Implemented: one quiver per owner.** How many quiver instances back a
partition-dispatch topic? The backend takes the production option (ingest-queue
D5): **one quiver engine per owner**. The `N` partitions are placed across `M`
owners (`num_owners`, configured; default `N`) by a static `balanced(N, M)` map,
so each owner's single store interleaves the partitions it holds, sharing one
per-owner WAL and disk budget. Because the engine no longer implies the partition,
each bundle carries its partition in a generic opaque **per-bundle `user_meta`**
that quiver persists and returns on drain (Phase 1: a `u64` quiver never
interprets, threaded through the segment manifest exactly as `item_count` is; the
topic packs `(partition, generation)`). A subscription claims whole owner blocks
and drains those stores, recovering each bundle's partition from `user_meta`.
Setting `M == N` makes every owner hold exactly one partition, reproducing the
earlier one-engine-per-partition layout as a special case.

The build sequenced this as: Phase 1, the opaque `user_meta` primitive in quiver;
Phase 2, the one-quiver-per-owner topic over a *static* placement. The
intermediate PoC (one quiver per partition, drained by a fixed subscriber id) is
now the `M == N` case. **Implemented in Phase 3:** live reassignment over the
durable backend, which binds a partition's data to its owner's store and so needs
the lease + generation-fencing **drain-and-forward** handoff (the deposed owner
forwards a moved partition's residual to the new owner's store); a dynamic
`partition_owner` / `partition_generation` overlay repoints routing and
`reassign_partition` no longer rejects the move. The overlay is persisted as a
snapshot and restored on `open()`, so reassignments survive restart.
`user_meta` is now recorded in **both** the segment manifest and the v2 WAL entry
(Phase 3, done), so the partition of un-flushed bundles recovered by WAL replay
after an unclean stop survives; only legacy v1 WALs recover it as `0`. `item_count`
remains the manifest-only gap.

### D25. Split and dispatch packaging

**Decided: separate composable nodes, not a fused shuffle node.** Split-by-key
is an OTAP-aware pdata operation exposed as a dedicated `partition` processor
node that tags each sub-batch with an integer partition index; partition-dispatch
is a backend-agnostic topic delivery mode that routes by that integer tag through
the placement map.
**Why:** the topic broker is generic over its payload type (`TopicBackend<T>`,
`Envelope<T>`), so teaching it to extract an OTAP key would specialize the
backend-agnostic seam and couple the broker to pdata; keeping the key logic in
the pdata layer preserves that seam. Each half is independently useful -- split
for tenant or descriptor batching, the partition-dispatch topic for any keyed
in-memory or durable delivery -- and two configured nodes match the engine's
routing/batching idiom and D18's independently-testable build order. A dedicated
node rather than a mode of the size-based batch processor keeps the stateless key
split out of that node's slot/timer/ack machinery. The partition function and `N`
are configured on the split side; the topic and the placement map need only `N`.
**Implication:** the integer partition tag is the contract between the two nodes,
carried on the request `Context`; a fused single-scatter fast-path (D19's
optimization) stays available internally without changing the configured
topology.
**Implemented:** the split half is the `partition` processor node
(`urn:otel:processor:partition`,
`crates/core-nodes/src/processors/partition_processor/`) over the
`partition_otap_batch` primitive
(`crates/pdata/src/otap/partition.rs`); the integer tag rides `Context::partition`.
The partition-dispatch delivery mode (Layer C) is the other half, now built.

### D26. Standalone logs key

**Decided: a configured projection over log columns, defaulting to even
spread.** Trace-correlated logs continue to ride their trace's `trace_id`
partition (D3); standalone logs, those with no `trace_id`, take a configured
sub-tenant projection whose default spreads rows evenly -- round-robin, or a
hash over a resource-and-scope identity combined with a per-record discriminator
so a single-resource stream still distributes. **Why:** unlike metrics, which
need single-writer delta-to-cumulative, and traces, which need whole-trace
assembly, standalone logs have no co-location *correctness* requirement, so the
queue's even-load goal dominates; resource cardinality is also often low enough
that hashing resource identity alone would concentrate one service's logs on a
single owner. A **resource-identity co-location** mode remains available for
deployments that do per-resource downstream work such as dedup or per-resource
rate-limit and forward, with hot-resource sub-partitioning reusing the
metric-name mechanism. This completes the per-signal partitioner (D3): metrics
hash `metric_name`, traces slice `trace_id`, standalone logs default to even
spread with optional resource co-location. **Implication:** off the
metrics-appliance critical path, so implementation waits until logs are
addressed, but the keying is now decided rather than open.

### D27. Optional, per-location durability

**Ratified (this session):** durability is an **orthogonal axis**, not a
property of dispatch. Two independent axes -- **delivery mode**
(broadcast/balanced/partition-dispatch) and **backend** (in-memory/quiver) --
form a cross-product, and the shuffle/aggregate/load-balance path runs
identically over both backends. Durability changes only the guarantee
(in-memory: effective-once within a process lifetime; quiver: also across
restart). **Why:** some deployments want the agent as a large-scale aggregating
SDK (shuffle + aggregate by `metric_name`/`trace_id`/`tenant` + load-balance,
all in memory); others want the disconnection-tolerant appliance. These are the
same design with the backend swapped. **Per-location:** the appliance has two
durability locations, each chosen independently -- **stage-1** (L1 ingest/shuffle)
via the **topic backend** (in-memory/quiver), and **stage-2** (L3 aggregated
store) via the **store seam** (in-memory/queryable-from-RAM vs durable columnar
files, appliance D13). A third durability-optional state is the per-owner registry
(off = relearned on restart, which ingest-queue D8 already makes safe).
**Implication:** build the in-memory path first (D18); durability is an additive
backend per location; honors the engine's configurable-QoS posture
(ingest-queue D6).

### D28. Scope for this effort

**Ratified (this session):** build the **in-memory core** -- Layer A
(split-by-key) and Layer C (partition-dispatch on the in-memory backend, static
placement) -- which delivers the runnable aggregating load-balancer and the
in-memory form of ingest-queue Phase 1. By D27 durability sits behind the same
`TopicState` interface, so the in-memory core "gets most of the value" and
durability adds as a pure backend swap with no redesign. **Update:** Layer B (the
Quiver durable backend, D21/D24) has since been built behind that same interface,
one quiver per owner, so it is no longer deferred; the durable-registry half of
Phase 1 remains future work. D23's placement is now **dynamic and restart-durable**
(reassignment repoints a persisted routing overlay), and the controller-driven
placement lifecycle (I4) is now complete for both backends, balancing at
subscriber granularity; the registry stays in-memory
and is relearned on restart, which is safe because conflicts are warnings
rather than rejections, per ingest-queue D2 and D7.

## Implementation order

In scope for this effort (the in-memory core, D28):

1. **Layer A -- split-by-key (DONE).** Implemented as the `partition` processor
   node (`urn:otel:processor:partition`,
   `crates/core-nodes/src/processors/partition_processor/`) over the
   `partition_otap_batch` pdata primitive
   (`crates/pdata/src/otap/partition.rs`), on the cascade machinery. It splits a
   multi-key OTAP batch into N partition-tagged sub-batches, carrying the integer
   tag on `Context::partition`; cascade integrity, determinism, conservation, and
   the per-signal keys (hash `metric_name`, slice the low 56 bits of `trace_id`)
   are covered by tests. Durability-independent.
2. **Layer C -- partition-dispatch subscription + static placement on the
   in-memory backend (DONE).** With A, this is the complete, runnable in-memory
   aggregating load-balancer (the "large-scale SDK") -- end-to-end, no durability.
   Delivers the in-memory form of ingest-queue Phase 1 (shuffle + per-owner
   authority). It consumes the `Context::partition` tag the `partition` node sets.

Added later as an additive backend swap (D28/D21):

- **Layer B -- Quiver durable `TopicState` (DONE).** The same dispatch/ownership
  made durable, selected per location (D27): `crates/otap/src/quiver_topic.rs`
  persists each partition into its owner's quiver store (one quiver per owner over
  a static `balanced(N, M)` placement), survives restart, and resumes subscribers
  from durable progress; reached through the `DurableDispatchPayload` trait and
  a `backend: quiver` topic with `num_partitions` (and optional `num_owners`).
  Restart replay and partition recovery from `user_meta` are tested. Adds a path
  to replace `durable_buffer_processor`. **Phase 3 (done):** live, restart-durable
  reassignment (lease-generation routing overlay + drain-and-forward handoff +
  persisted placement snapshot) and the controller/scheduler wiring (I4) for both
  backends, balancing at subscriber granularity. The durable registry of Phase 1
  remains future work.

## Status and next steps

- **The in-memory core and the durable backend are both implemented end-to-end.**
  All component decisions are now ratified or implemented (D18-D28); the durable
  backend (D21/D24) is implemented as one quiver per owner.
- **Layer A delivered:** the `partition_otap_batch` primitive
  (`crates/pdata/src/otap/partition.rs`), the `partition` processor node
  (`crates/core-nodes/src/processors/partition_processor/`,
  `urn:otel:processor:partition`), and the `Context::partition` tag the A->C
  contract rides on. Acknowledgement fan-in is the documented, deferred part
  (D22): the request context rides the first partition only.
- **Layer C delivered:** the static `PartitionPlacement` map (D23); the
  `Partitioned` routing seam (implemented for `OtapPdata` in `crates/otap`, D25);
  the in-memory `PartitionDispatchTopic` / `PartitionDispatchBackend` with
  `SubscriptionMode::PartitionDispatch` (D22); and the pipeline wiring -- the
  topic exporter preserves the partition on publish, the topic receiver
  subscribes with its owned partitions, and the controller creates a
  partition-dispatch topic from `TopicSpec.num_partitions`. Broker-level and
  end-to-end tests cover routing, exclusivity, placement, and the full
  tagged-`OtapPdata` path.
- **Layer B delivered:** the durable `QuiverPartitionDispatchTopic`
  (`crates/otap/src/quiver_topic.rs`) over one quiver engine per owner with a
  static `balanced(N, M)` placement and the partition carried in Quiver's opaque
  per-bundle `user_meta`; the `DurableDispatchPayload` seam
  (`crates/engine/src/topic/durable.rs`) and the `OtapPdata` impl; the shared
  OTAP-to-bundle adapter (`otap_df_otap::quiver_bundle`); the `backend: quiver`
  config with a `quiver` settings block (`num_owners`, default `num_partitions`);
  and the controller wiring through `TopicBroker::create_topic_with_state`. Tests
  cover durable routing, exclusivity, whole-owner-block claims, a shared owner
  store, restart recovery of partitions from `user_meta`, and reassignment.
- The substrate exists: the topic backend seam, `quiver`, the cascade filter
  (`filter_otap_batch` / `filter_metrics_time_window`), and the size-based batch
  split.
- **Phase 3 progress (durable):** (1) the WAL now stamps `user_meta` into a
  versioned v2 entry (`crates/quiver/` `wal/`), so a partition and lease
  generation survive WAL replay after an unclean stop; version-aware replay still
  reads legacy v1 WALs. (2) **Live reassignment works and survives restart**
  (`crates/otap/src/quiver_topic.rs`): a dynamic `partition_owner` /
  `partition_generation` overlay repoints a partition's publishes and bumps its
  generation, the deposed owner drains-and-forwards the residual to the new
  owner's store, and the overlay is persisted as a snapshot and restored on
  `open()`. Covered by WAL round-trip, `decode_entry` v1/v2 dispatch, an engine
  replay test, and reassignment routing/forward/validation/restart tests.

### I4 -- controller/scheduler wiring

Layers A/C, Layer B, and Phase 3 I1-I3 plus placement persistence are implemented
and tested. **I4 is complete: half 1 (initial placement) and the scheduler loop
for both the in-memory and durable backends**, including a durable topic
reconciling to its restored `placement.v1` overlay and balancing at subscriber
granularity. The remaining durable-dispatch work is off I4: the durable registry
(ingest-queue Phase 1) and the standalone-logs key (D26).

I4 has two halves:

1. **Initial placement from the controller -- done.** Receiver replicas no
   longer hand-specify `owned_partitions`; it is optional in
   `TopicSubscriptionConfig::PartitionDispatch`. When omitted, the controller
   enumerates a topic's partition-dispatch receivers, computes the static
   `balanced(N, M)` map, and injects each receiver's owned set into its node
   config before the spec is resolved, in
   `Controller::assign_partition_dispatch_placement` called from `run_with_mode`.
   An explicit set is honored as an override, and a topic must not mix explicit
   and auto receivers. **Durable-backend reconciliation -- resolved: the topic
   is the source of truth.** The injected `balanced(N, M)` set feeds subscribe,
   which maps through the durable topic's *static* store layout and so agrees
   with the injection regardless of reassignments; the *dynamic* routing
   (post-reassignment, restored from `placement.v1` on `open()`) is read back by
   the scheduler through `partition_routing` and reconciled on each tick, so the
   controller never re-reads the snapshot.
2. **Scheduler-driven moves -- done for both backends.** The controller creates
   a `PlacementScheduler` per partition-dispatch topic and ticks it on a background
   thread (`spawn_placement_schedulers` in `crates/controller/`). Owners report
   load through the topic binding's load-report sender, which the event-time
   windower pulls when its `report_load_to` names the topic; each tick reconciles
   the coordinator to the topic's real `partition -> owner` routing via
   `TopicHandle::partition_routing`, so owner indices match the actual owner slots
   regardless of subscription order, and then rebalances. **Subscriber
   granularity (done).** A durable topic's owners are quiver stores, and a
   subscriber may drain several, so the scheduler balances *subscribers*, not
   stores: the topic implements `subscriber_routing` (mapping each partition's
   store to its claiming subscriber) and `reassign_partition_to_subscriber`
   (routing a partition into one of the target subscriber's stores), which
   default to the owner-level methods for the in-memory backend. A move reaches
   the gaining subscriber through the whole-store drain and fence-and-forward, so
   no owned-set change or per-subscriber notification is needed. The controller
   sizes the scheduler to the receiver count for both backends.

**Key files/seams for I4:**

- `crates/engine/src/topic/load_feedback.rs` -- `PlacementScheduler`,
  `PlacementCoordinator`, `PartitionMove`; the brain is built and unit-tested.
- `crates/engine/src/topic/handle.rs` -- `TopicHandle::apply_move` /
  `reassign_partition_to_subscriber` (the seam both backends honor).
- `crates/otap/src/quiver_topic.rs` -- `subscriber_routing` and
  `reassign_partition_to_subscriber` (the subscriber granularity the scheduler
  uses), plus `partition_routing` and `reassign_partition` (store-level, for
  inspection and the drain-and-forward handoff).
- `crates/controller/` -- `assign_partition_dispatch_placement` computes the
  initial placement (half 1), and `spawn_placement_schedulers` runs a scheduler
  per partition-dispatch topic, in-memory or durable (half 2), sized to the
  receiver count. `owned_partitions` in the topic receiver is optional, and the
  windower's `report_load_to` wires an owner's load reports.

**Out of scope / separate:** the durable registry half of ingest-queue Phase 1,
and the standalone-logs key (D26, decided but waits until logs are addressed).

**Related docs:** [`ingest-queue-design.md`](./ingest-queue-design.md) (D3
per-signal partitioner; the consumer of this infrastructure),
[`metrics-appliance-design.md`](./metrics-appliance-design.md),
[`topic-architecture.md`](./topic-architecture.md) (current topic runtime), the
agent multitenancy design (`agent-multitenancy-design.md`, "Split-by-key and
sub-tenant identifiers", in the Telemetry-Collection-Spec repo), and the
`quiver` crate README.
