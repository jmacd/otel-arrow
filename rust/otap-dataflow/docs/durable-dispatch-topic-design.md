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

> Status: design ratified; the **in-memory core is implemented end-to-end**. The
> orthogonal-axis model (D27), the layer decomposition, and all component
> decisions are **ratified** (D18-D20, D22-D23, D25-D28); only the durable backend
> (D21/D24) is **deferred** to a later effort, behind the same interface. See
> "Decisions". Names are provisional. **Layer A (split-by-key) and Layer C
> (placement map, the `Partitioned` seam, the in-memory partition-dispatch topic,
> and the exporter/receiver/config/controller wiring) are implemented and
> tested**; the durable backend (Layer B) is the remaining work.
>
> **Scope for this effort (D28):** build the **in-memory core only** -- Layer A
> (split-by-key) + Layer C (partition-dispatch on the in-memory backend) = the
> runnable aggregating load-balancer. **Layer B (the Quiver durable backend) is
> deferred**: because durability is an additive backend behind the *same*
> interface (D27), it can be added later as a pure backend swap with no redesign.
> Decisions D21/D24 (durable backend) are therefore deferred, not open.

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

| Capability                           | Engine primitive                                                         | Status                                  |
| ------------------------------------ | ------------------------------------------------------------------------ | --------------------------------------- |
| Size-based batch split/merge         | `otap::groups` + `transform::split` (contiguous range split)             | exists                                  |
| Selection-mask cascade (root->child) | `otap::filter::filter_otap_batch` (+ `filter_metrics_time_window`)       | exists (used by filter + admission)     |
| Topic backend seam                   | `TopicBackend` / `TopicState` / `SubscriptionBackend`                    | exists; `InMemoryBackend` only          |
| Durable buffer                       | `quiver` (WAL + Arrow IPC segments, at-least-once, multi-stream, budget) | exists (experimental)                   |
| Reserved durable topic kind          | `TopicBackendKind::Quiver`                                               | reserved; currently rejected at startup |
| Split-by-key (Layer A)               | (this design)                                                            | to build                                |
| Quiver TopicBackend (Layer B)        | (this design)                                                            | to build                                |
| Partition-dispatch + placement (C)   | (this design) + controller placement map                                 | to build                                |

## Layer A: Split-by-key

**Goal.** Given an OTAP batch and a configured key, partition the batch into
sub-batches such that all rows sharing a key value land in the same sub-batch,
tagged with a partition index.

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
> (Layer B) is the remaining work.

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
  so the handoff carries no state. Aggregation windows are natural, state-free
  rebalance points; the durable backend later upgrades this to the lease and
  generation handoff above.

**The load feedback loop.** The signal above is closed into a loop by
`PlacementCoordinator` (`crates/engine/src/topic/load_feedback.rs`):

1. **Measure (owner).** Each owner buckets the load of the keys it aggregates by
   the partition tag the dispatch delivered, producing a `PartitionLoad`
   (`active_series` plus `points`) per owned partition. Because the owner already
   holds the per-series aggregation state, this is a cheap read, not a scan.
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

The coordinator is the in-memory feedback brain, and `apply_move` is the runtime
primitive that applies its decisions to a live topic. Wiring an owner's
aggregator to emit reports, and a scheduler to call `apply_move` at the right
window boundary, are the surrounding steps; the durable backend adds leases and
generation fencing to the apply step so a deposed owner's late writes are fenced.

**Relationship to tenancy.** When the multitenancy descriptor system lands,
condition-routing (named partitions for tenant/resource isolation) and
hash-partitioning (N buckets for co-location) are the same dispatch mode with
two partition functions over the same request/data projection. Layer C is
designed to accept either; the ingest queue uses the hash-partition form.

## Layer B: Quiver TopicBackend (optional durability)

**Goal.** Make the *same* partition-dispatch topic durable: published items are
persisted before acknowledgement and survive restart; subscribers resume from
durable progress. This is an **additive backend** selected per location (D27),
not a separate mechanism.

**Mechanism.** Implement the three backend traits against a `quiver` instance:

- `TopicState::publish*` -> quiver `ingest(bundle)` (WAL append + segment
  accumulation), acknowledged once durable. The OTAP->bundle adapters already
  exist in `durable_buffer_processor`.
- the partition subscription -> a quiver subscriber per owned partition (stream).
- `SubscriptionBackend::poll_recv_delivery` -> quiver `poll_next_bundle`;
  `ack` / `nack` -> quiver `BundleHandle::{ack, reject/defer}`.

**QoS mapping** (ingest-queue D6): the topic's queue-on-full policy maps to
quiver's `RetentionPolicy` -- `Backpressure` (lossless) vs `DropOldest`
(loss-tolerant) -- wired to the `DiskBudget`.

**Wiring.** `TopicBackendKind::Quiver` is currently rejected at startup
(`topic_backend_capabilities` returns `None`,
`validate_topic_runtime_support` errors, and `declare_topic`'s Quiver arm is
`unreachable!`). Layer B fills those arms in and constructs a quiver-backed
`TopicState`.

**Consequences.** Durability becomes a backend setting on the topic, so the
existing `durable_buffer_processor` reduces to "set the topic backend to quiver"
and can be deprecated once the backend reaches parity (no node-plus-WAL, no
topology fork). The same backend **backs per-owner state durability** -- the
metric type registry persists through a small Quiver-backed topic (snapshot +
log), replayed on startup -- so no hand-rolled WAL is needed.

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
  map; leases + generation fencing.
- **Raw buffer (L1) and stage-2 feed (L3 of the appliance).** Topics whose
  backend is chosen per location (D27): in-memory for the SDK profile, quiver for
  the durable appliance.

## Decisions

Continuing the shared decision ledger of the ingest-queue and appliance designs
(D1-D17). Status is per-decision: **ratified**, decided and ready to build, or
**deferred**, intentionally postponed beyond this effort and addressed later as
an additive backend swap (D21/D24, the durable backend).

| ID  | Decision                            | Recommendation                                      | Status   |
| --- | ----------------------------------- | --------------------------------------------------- | -------- |
| D18 | Layer decomposition + build order   | A split-by-key -> C in-memory dispatch -> B durable | ratified |
| D19 | Split-by-key mechanism              | selection-mask radix cascade; batch processor       | ratified |
| D20 | Shard key source                    | data-sourced; descriptor/tenant composes later      | ratified |
| D21 | Durable backend                     | Quiver `TopicBackend`; durability is additive       | deferred |
| D22 | Partition ownership                 | new partition-claim mode; in-memory first           | ratified |
| D23 | Placement seam                      | controller map; static->rebalanced; leases          | ratified |
| D24 | Quiver backend granularity          | one quiver/owner, per-partition IPC streams         | deferred |
| D25 | Split and dispatch packaging        | separate composable nodes; fused fast-path later    | ratified |
| D26 | Standalone logs (no `trace_id`) key | configured projection; default even spread          | ratified |
| D27 | Optional, per-location durability   | delivery mode x backend are orthogonal axes         | ratified |
| D28 | Scope for this effort               | in-memory core (A+C) only; defer Quiver (B)         | ratified |

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

### D21. Durable backend (additive) -- deferred this effort

**Deferred (D28):** not built in this effort. The plan stands for when it is:
implement the reserved `TopicBackendKind::Quiver` as a durable `TopicBackend`
selected **per location** (D27). Durability is **additive configuration, not a
subsumption**: the same partition-dispatch topic runs over the in-memory backend
(the default -- no durability) or the quiver backend (the durable option), with
identical dispatch/ownership semantics. **Why deferrable:** because the backend
sits behind the *same* interface, deferring it costs no redesign -- it is a later
backend swap. **When built:** fill the rejected Quiver arms in the controller;
the existing `durable_buffer_processor` becomes "set the topic backend to
quiver" and can then be deprecated. Couples to ingest-queue D4/D6.

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
first** per D18/D27; the quiver backend (D21, deferred) implements the same
ownership durably. **Why:** stable *exclusive* ownership is what makes a
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
`Initializing`/`Leaving` durable handoff are deferred with Layer B, where they
stamp quiver bundle metadata and replay durable streams. **Why:** the two-level
`key -> partition -> owner` indirection bounds churn and makes rebalancing
possible without rehashing keys (ingest-queue D3 / Auto-sharding).
**Implication:** this effort adds a minimal static placement map; the
controller's lease and key-ownership primitive arrives with durability.

### D24. Quiver backend granularity -- deferred this effort

**Deferred (D28), scoped to the durable backend.** How many quiver instances and
streams back a partition-dispatch topic? Leading option (ingest-queue D5): **one
quiver per owner**, with **each partition a distinct Arrow-IPC stream** within
that owner's segments (quiver already supports multi-stream segments keyed by
`(SlotId, SchemaFingerprint)`), sharing one per-owner WAL and disk budget.
Alternative: one quiver per partition (simpler isolation and handoff, more WALs).
The choice hinges on whether quiver can hand off a *single stream* out of a
multi-stream instance on reassignment (to verify when Layer B is built).

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
The partition-dispatch delivery mode (Layer C) is the remaining half.

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
store) via the **store seam** (in-memory/queryable-from-RAM vs Vortex files,
appliance D13/D16). A third durability-optional state is the per-owner registry
(off = relearned on restart, which ingest-queue D8 already makes safe).
**Implication:** build the in-memory path first (D18); durability is an additive
backend per location; honors the engine's configurable-QoS posture
(ingest-queue D6).

### D28. Scope for this effort

**Ratified (this session):** build the **in-memory core only** -- Layer A
(split-by-key) and Layer C (partition-dispatch on the in-memory backend, static
placement) -- which delivers the runnable aggregating load-balancer and the
in-memory form of ingest-queue Phase 1. **Layer B (the Quiver durable backend)
and the durable-registry half of Phase 1 are deferred.** **Why:** by D27
durability sits behind the same `TopicBackend` interface, so the in-memory core
"gets most of the value" and durability adds later as a pure backend swap with
no redesign. **Implication:** D21 and D24 are deferred (not open); D23 needs only
the static in-memory placement map now (leases/handoff defer with B); the
registry stays in-memory and is relearned on restart (safe per ingest-queue D8).

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
   in-memory backend.** With A, this is the complete, runnable in-memory
   aggregating load-balancer (the "large-scale SDK") -- end-to-end, no durability.
   Delivers the in-memory form of ingest-queue Phase 1 (shuffle + per-owner
   authority). It consumes the `Context::partition` tag the `partition` node sets.

Deferred (added later as an additive backend swap, D28/D21):

- **Layer B -- Quiver `TopicBackend`** implementing the reserved kind: the same
  dispatch/ownership made durable, selected per location (D27). Adds durable
  registry/buffer and restart replay; replaces `durable_buffer_processor`.
  Completes ingest-queue Phase 1 (durable registry) and Phase 2 (per-bucket
  durability, leases, rebalancing).

## Status and next steps

- **The in-memory core is implemented end-to-end; the durable backend is not.**
  All component decisions are now ratified (D18-D20, D22-D23, D25-D28); only the
  durable backend (D21/D24) is deferred to Layer B.
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
- The substrate exists: the topic backend seam, `quiver`, the cascade filter
  (`filter_otap_batch` / `filter_metrics_time_window`), and the size-based batch
  split. The reserved `TopicBackendKind::Quiver` is the (deferred) durable-backend
  wiring point.
- **To proceed:** Layer B (durability) as an additive backend swap behind the
  same dispatch interface, plus the controller computing placement and assigning
  owned partitions to receiver replicas (now hand-specified in receiver config).
  The standalone-logs key (D26) is decided but waits until logs are addressed.

**Related docs:** [`ingest-queue-design.md`](./ingest-queue-design.md) (D3
per-signal partitioner; the consumer of this infrastructure),
[`metrics-appliance-design.md`](./metrics-appliance-design.md),
[`topic-architecture.md`](./topic-architecture.md) (current topic runtime), the
agent multitenancy design (`agent-multitenancy-design.md`, "Split-by-key and
sub-tenant identifiers", in the Telemetry-Collection-Spec repo), and the
`quiver` crate README.
