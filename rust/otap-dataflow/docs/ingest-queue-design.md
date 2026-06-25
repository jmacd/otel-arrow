# Vertically-Integrated Ingest Queue

This document sketches a design for a minimum-dependency, vertically-integrated
ingest queue for OTAP pipelines: type-identity admission control, auto-sharding,
and durable at-least-once delivery, with no external broker (no Kafka, etcd, or
ZooKeeper).

This is a Kafka alternative tailored for telemetry reliable delivery: rather
than depend on an external broker, the engine provides the durable,
at-least-once queue itself. The design follows the pattern proven by post-Kafka,
vertically-integrated telemetry queues: a per-core type registry records the
canonical identity of every metric and flags name conflicts without dropping
data (the OpenTelemetry behavior -- keep both streams, warn, let the recipient
resolve), and accepted data is auto-sharded onto durable storage.

The intended substrate is this engine: the `quiver` crate (durable Arrow IPC
buffering), the topic system (broadcast and balanced delivery), the controller
(placement and leases), and the query engine (DataFusion over OTAP for
vectorized validation).

> Status: early design, not yet implemented. Names are provisional. Decisions
> D1 through D9 (see "Decisions") are ratified and gate most of the detail
> below. See "Status and next steps" for where this stands and how to resume in
> a fresh session.

## Background and motivation

Sharding services such as Google's Slicer (and its Microsoft Research
predecessor Centrifuge) assign work to servers as a shared platform service.
Monarch, Google's planet-scale in-memory time-series database, deliberately
does *not* take a hard dependency on Slicer: as the system that monitors
everything else, it cannot depend on the systems it monitors without creating a
circular, blast-radius-coupled dependency. Instead the Monarch team built its
own minimum-dependency, vertically-integrated ingest queue with auto-sharding
and durable delivery, replacing an earlier Kafka-based queue.

OpenTelemetry reaches the same conclusion for a related reason. A
vendor-neutral project cannot bless a specific vendor backend (a managed Kafka,
an external time-series database, a proprietary sharder) as a required
dependency without a governance and adoption "blast radius" of its own. So,
like Monarch, the durable auto-sharding ingest queue is built natively, on
neutral primitives: Apache Arrow, DataFusion, and the OTAP representation,
implemented in Rust inside this engine.

## Prior art

What each system contributes to this design:

- **Slicer** (Google, OSDI 2016): general-purpose sharding service; the
  load-aware assignment and lease model borrowed for placement.
- **Centrifuge** (Microsoft Research, NSDI 2010): Slicer's predecessor,
  integrating lease management and partitioning; source of the lease and
  generation fencing approach.
- **Monarch** (Google): the motivating design; its vertically-integrated,
  minimum-dependency ingest queue contributes manifest-based type registration
  and a queue that avoids a sharder dependency.
- **M3 and M3DB** (Uber): explicit placement with Initializing, Available, and
  Leaving shard states and streamed handoff; the model for fixed logical shards
  and local-storage handoff.
- **Cortex and Mimir** (CNCF): coordinator-free hash-ring sharding, and
  shuffle-sharding for tenant blast-radius isolation (D9).
- **VictoriaMetrics**: shared-nothing, consistent-hash series distribution with
  no external coordination; the minimal-dependency posture targeted here.
- **Apache Helix** (LinkedIn): general partition and replica assignment, a
  reference if the placement plane outgrows the controller.

If deeper external review is ever wanted, the assignment-plane lineage above
(Centrifuge and Slicer, and the M3 placement model) is where the relevant
expertise sits.

## Goals and non-goals

**Goals:**

- Vertically integrated, with minimum external dependencies (no external
  broker, no external coordination service).
- Type-identity registry: record the canonical type of every metric and flag
  name conflicts as warnings without dropping data (optional strict-reject
  mode).
- Time-window admission: reject too-old and too-future points up front.
- Auto-sharding of accepted data, with churn-minimizing reassignment.
- Durable, at-least-once delivery with crash recovery.
- Arrow-native end to end; validation expressed as vectorized DataFusion
  operations over OTAP.

**Non-goals (initially):**

- Cross-region federation (Monarch's global plane). Regional first.
- Strong transport-edge exactly-once. We target *effective* once via idempotent
  identity plus dedup (see "Consistency and dedup").
- General range-query serving. This is an ingest queue, not the time-series
  store.

**Constraints (hard requirements):**

- Implemented in Rust, in this engine (`rust/otap-dataflow`).
- Arrow-native storage and data path; OTAP as the in-memory representation.
- Query and validation built on DataFusion.
- Minimum dependencies: no external broker or coordination service in the
  default deployment.
- Vendor-neutral: no required dependency on any specific vendor backend.

These align with `docs/design-principles.md`: graceful degradation, minimal
synchronization, serviceability, and configurable quality of service including a
lossless mode.

## How it maps onto existing primitives

| Capability                      | Engine primitive                                                            | Status                              |
| ------------------------------- | --------------------------------------------------------------------------- | ----------------------------------- |
| Durable buffer-queue            | `quiver` (WAL + Arrow IPC segments, multi-subscriber at-least-once, budget) | exists (experimental)               |
| Per-core type registry          | in-memory Arrow table + `quiver` snapshot                                   | exists                              |
| Balanced work distribution      | balanced topic + thread-per-core controller                                 | exists                              |
| Local control plane / placement | controller (core allocation, lifecycle, leases)                             | partial; local-only, no rebalancing |
| Validation compute              | query engine / DataFusion over OTAP Arrow                                   | exists                              |
| Manifest, admission, sharder    | (this design)                                                               | to build                            |

The durable-queue question is largely answered by `quiver` already. The new work
is the admission front-end (the manifest) and the assignment control plane (the
auto-sharder).

## Architecture

```text
 OTAP/OTLP    +----------+   +--------------+   +----------------------+   +--------------+
 ---------->  | Receiver |-->| Shuffle by   |-->| Admission +          |-->| Per-bucket   |--> balanced
   (gRPC)     | (decode) |   | signal key   |   | per-core type        |   | Quiver       |    topic:
              +----------+   | (metric_name |   | registry             |   | (WAL + IPC   |    processors,
                             |  / trace_id) |   | (time-window gate;   |   |  streams)    |    exporters
                             +--------------+   |  admit all, warn on  |   +--------------+
                                                |  name conflict)      |
                                                +----------------------+
                              +----------------------------------------+
                              | Placement control plane (bucket->core; |
                              | placement map, leases; rebalances)     |
                              +----------------------------------------+
```

A single data plane: incoming data is shuffled by a signal-specific key
(`metric_name` for metrics, `trace_id` for traces) to its owner core, where
admission, the per-core type registry, and per-bucket persistence are all
co-located. Because a metric's data and its type registry share one core, type
observation and conflict-flagging are a local lookup -- there is no separate
broadcast manifest plane on the hot path.

One control plane remains: **placement** maps buckets to owners (`bucket ->
core`; later `bucket -> node`), lives in the controller, and rebalances on load
and failure. This is the collapse of the earlier two-plane split: using the
signal key as the shard key co-locates type authority with data, so the
low-cardinality manifest plane and the high-cardinality data plane become one.

### Execution model

The engine runs a strict thread-per-core model with replicated pipeline groups.
Ingest is morsel-driven (Leis et al., SIGMOD 2014): incoming data is shuffled
by a signal-specific key -- `metric_name` for metrics, `trace_id` for traces --
into N buckets, one set per core, and processed by the fixed per-core worker
pool with work-stealing for balance. Each signal may shuffle and balance by a
different key. Because the metric shuffle key is also the identity key, a
metric's type registry is core-local (see Architecture), so conflict-flagging
needs no hot-path broadcast.

## Identity and type model

The queue applies to all three signals -- traces, metrics, and logs. Each
signal has its own *identity key* and its own manifest, because each has
different identifying properties (OTel's term for the fields that determine
identity). The reserved `signal` tenant descriptor distinguishes them. This
document develops the metric case in detail; traces and logs follow the same
admission-and-manifest pattern with signal-specific identity keys.

For metrics, two tuples are extracted from OTAP columns (via `pdata-views`):

- **Identity key** (manifest key and data bucket key, D1): the metric name
  within a tenant. Scope is not part of it -- OTel treats the name as
  identifying, so scope names are swappable. Low cardinality. It selects the
  bucket (`hash(tenant, name) mod N`) and keys the type registry, whose
  value is the canonical **type descriptor**: instrument type, monotonicity,
  and unit.
- **Series identity** (dedup key): the tenant projection (D9) plus resource
  attributes, metric name, and point attribute set; scope excluded as
  non-identifying. High cardinality. Used for effective-once dedup, not for
  sharding.

Temporality and value type (int versus double) are deliberately
*non-manifesting* for metrics: users are technically permitted to mix
temporalities and numeric value types within a single identity, so admission
never treats a difference in either as a type conflict. They are carried with
the data but excluded from the manifest descriptor.

The manifest is itself a small Arrow table (one row per known identity key),
which makes admission a columnar join and lets DataFusion introspect it.

## The type registry (manifest)

**Per-core registry:** the signal-key shuffle sends all of a name's data to one
owner core, so that core holds the authoritative registry for its names --
`hash(tenant, name) mod N` selects the owner. The registry records the observed
descriptor(s) for each name. Because a name's data and its registry are
co-located, there is no cross-core consensus, no first-wins race, and no
hot-path broadcast: the classic hard part collapses into ordinary sharding.

**Admit all, flag conflicts:** the registry never rejects on the hot path
(default). The first descriptor seen for a name is recorded as its primary
(canonical for display); a later point whose descriptor differs is a name
conflict -- both streams are admitted and persisted, and a `name_conflict`
warning is emitted for the recipient to resolve. This mirrors the OpenTelemetry
metrics SDK's instrument-conflict handling (warn, keep both). An optional
strict mode rejects the conflicting rows instead, for operators who want it.
The descriptor compares only the manifesting properties (instrument type,
monotonicity, unit); temporality and value type are non-manifesting and never
flag a conflict.

**Durability:** the registry must survive restart. It is persisted as a
compacted Arrow IPC snapshot plus a small append log (a dedicated `quiver`
instance or a `quiver`-style WAL). On startup the snapshot is loaded and the
log replayed before admission resumes. The registry is tiny relative to data,
so snapshots are cheap.

## Admission control (pre-disk)

Order matters; the cheapest, most protective filters run first. Both steps are
DataFusion / Arrow operations over the incoming `RecordBatch`, so they are
columnar with no per-point branching.

1. **Time-window filter:** keep rows with `ts` in `[now - max_lag, now +
   max_skew]`. Drop too-old rows (beyond the retention or compaction horizon;
   protects durable storage from unbounded backfill) and too-future rows (bad
   clocks). A single vectorized predicate on the timestamp column.
2. **Type-identity recording:** compute the distinct
   `identity key -> observed descriptor` set for the batch and merge it into
   the local per-core registry:
   - new identity key: record it (and its primary descriptor); admit the rows;
   - known key, matching descriptor: admit the rows;
   - known key, differing descriptor: admit the rows and emit a `name_conflict`
     warning (strict mode instead rejects them).

Admission admits all distinct identities. The time-window and poison-batch
gates emit `rejected_points{reason=too_old|too_future|malformed}`; name
conflicts emit `name_conflict{...}` warnings without dropping data. Accepted
rows proceed to sharding and disk.

## Auto-sharding

A two-level mapping (the Slicer and M3 separation of concerns):

```text
  signal key (metric_name | trace_id) --hash--> bucket (fixed N, stable)
  bucket --placement map--> owner (core now; node later; dynamic)
```

- Stable hashing means a key never changes its bucket; only the
  `bucket -> owner` mapping moves, which bounds churn.
- The placement map lives in the controller. v1: `bucket -> core`. Later:
  `bucket -> node`. It is rebalanced on observed load (Quiver depth, ingest
  rate) and on failure, minimizing the number of buckets moved.
- Hotspots are handled by moving whole buckets rather than resplitting keys.
  For metrics, a hot `metric_name` (many series under one name) can be
  adaptively sub-partitioned by series identity (extra radix bits) into several
  buckets; `trace_id` is high-entropy and needs none. Size N so the hottest
  bucket is tolerable.

**Leases and fencing (from Centrifuge and Slicer):** each bucket has an owner
lease with a monotonic generation. Writers stamp the (bucket, generation) into
Quiver per-bundle metadata; on reassignment the generation bumps and a deposed
owner's late writes are fenced (rejected). This makes handoff safe without
distributed locks.

## Durable delivery and handoff

- **Per-bucket durable log:** each owner core runs one `quiver` instance (a
  single sequential WAL plus immutable Arrow IPC segments, disk-budget
  backpressure); each bucket is a distinct Arrow IPC stream within that core's
  segments (D5). At-least-once is already provided; topic Ack/Nack carries the
  acknowledgement back through the hop.
- **Handoff on reassignment:** with local storage, adopt M3's state machine: a
  new owner goes `Initializing`, streams or replays the leaving owner's segments
  (or its replica's), then marks `Available`; the old owner goes `Leaving` until
  drained. The lease generation prevents dual-ownership writes during the window.
- **High availability:** replicate each bucket to R owners so a node loss does
  not lose un-exported data and handoff can read from a surviving replica.
  Replication is bucket-level fan-out on the balanced topic, not a new
  dependency.

## Consistency and dedup

At-least-once plus idempotent identity yields effective-once for metrics:
duplicates share `(series identity, timestamp)` and resolve deterministically
(first-wins for cumulative points; last-wins is configurable). Dedup happens at
the consuming processor or exporter, keyed by identity and timestamp within the
retention window. No transactional broker is required.

## Failure modes and mitigations

- Type registry: per-core and local (the signal-key shuffle co-locates a name's
  data with its registry), so there is no cross-core race or consensus.
- Registry loss on restart: snapshot plus log replay before admission resumes.
- Name-conflict flood: conflicts are warnings, not rejections, so a flood costs
  registry entries and `name_conflict` counters, not data loss.
- Clock skew: the `max_skew` and `max_lag` window rejects and counts; it never
  crashes.
- Rebalance during outage: generation fencing; the new owner replays durable
  segments; the old owner's stale writes are rejected.
- Disk pressure: the Quiver disk budget applies backpressure (lossless mode) or
  sheds (loss-tolerant mode) per configured quality of service.
- Poison batch: admission rejects structurally invalid OTAP before disk.

## Phased implementation plan

- **Phase 0 -- identity and registry (single core):** `pdata-views` extracts
  the identity key and type descriptor from OTAP columns. A new processor
  performs the time-window check and records observed descriptors in an
  in-memory registry (Arrow table) using DataFusion, admitting all rows and
  emitting `name_conflict` warnings. Tests cover conflict warning, window
  rejection, and the accept path.
- **Phase 1 -- per-core registry and durability:** shuffle by signal key so
  each core owns its names; maintain the registry locally (no broadcast).
  Persist the registry (snapshot plus WAL, Quiver-backed) with startup replay.
  Tests cover restart replay and conflict-flagging.
- **Phase 2 -- auto-sharder and per-bucket durability:** shuffle by signal key
  into N buckets; a placement map in the controller (`bucket -> core`); one
  `quiver` per core with per-bucket Arrow IPC streams (D5); balanced-topic
  delivery to consumers; leases plus generation fencing.
- **Phase 3 -- rebalancing and high availability:** load-aware,
  churn-minimizing reassignment; M3-style `Initializing` and `Leaving` streaming
  handoff; replication factor R.
- **Phase 4 -- distributed placement:** `bucket -> node`; cross-node
  handoff; the full Slicer-class plane.

Each phase is independently testable and maps to existing traits (`Receiver`,
`Processor`, `Exporter`, `EffectHandler`), the topic system, the controller, and
`quiver`.

## Decisions

These gate most of the detail above. Each records the decision, what is at
stake, the options and their trade-offs, and the implications of the choice.

**Status: all decisions (D1 through D9) are ratified. See each section for the
decision, rationale, and implications.**

| ID | Decision                   | Recommendation                       | Status  |
| -- | -------------------------- | ------------------------------------ | ------- |
| D1 | Type-identity domain       | identity key = name in tenant        | decided |
| D2 | New name on data path      | admit-and-record (no quarantine)     | decided |
| D3 | Sharding scheme            | hash by signal key                   | decided |
| D4 | Storage locality           | configurable: local + object-store   | decided |
| D5 | Sizing M and N             | N ~ 16-64x cores (pow2), streams     | decided |
| D6 | QoS default                | loss-tolerant default, lossless mode | decided |
| D7 | Type evolution / overrides | admit-all; override sets primary     | decided |
| D8 | Manifest eviction / GC     | never evict in v1                    | decided |
| D9 | Tenancy                    | tenant descriptor projection         | decided |

### D1. Type-identity domain

**Decided:** the identity key is the **metric name within a tenant**
(`(tenant, name)`); the instrumentation scope is not part of it. "Identity
key" is this document's term for what OTel calls a metric's *identifying
properties*.

**Why it matters:** this sets the cardinality of the manifest, where conflicts
are flagged, the manifest shard key, and the scope of a conflict warning
(tenant-wide per name).

**Rationale:** OTel defines metric identity by name the way Prometheus does --
instrumentation scope names are swappable, and the same name is meant to denote
the same metric regardless of the emitting library. Keying on the name (not
`scope + name`) is therefore the OTel-faithful choice, not merely a Prometheus
convenience: two libraries that emit the same name with different shapes are a
genuine conflict OTel intends to surface, not a legitimate reuse to preserve.
Keying on `scope + name` was considered and rejected -- it would grow the
manifest toward series cardinality and contradict OTel's identity model. The
`tenant` component is a configured descriptor projection (D9), so distinct
tenants never collide on a name.

**Implications:** fixes the manifest shard key (`hash(tenant, name) mod N`; the
manifest co-partitions with data, D5), the conflict-flagging scope, and identity
extraction (the name plus the tenant projection; no scope columns on the hot
path). Couples to D9.

### D2. Handling a brand-new name on the data path

**Decided:** admit and record. A name not yet in the local registry is recorded
(with its primary descriptor) and its rows are admitted immediately -- there is
no quarantine and no first-wins race to lose, because the signal-key shuffle
makes the owner core the sole authority for that name.

**Why it matters:** with admit-all-and-warn (see the type registry), type
conflicts no longer reject, so unvalidated data reaching disk is a non-issue for
the type dimension. The only pre-disk gates are the cheap, local time-window
predicate and poison-batch structural validation; neither needs to hold data.

**Implications:** no bounded quarantine buffer, no registration round-trip, no
broadcast on the hot path. First-point latency for a new series is just the
local record-and-admit. The earlier quarantine-and-register option (and its
overflow policy) is dropped.
Defines the first-point tail-latency expectation.

### D3. Sharding scheme: hash versus range

**Decided:** hash the signal key (`metric_name` for metrics, `trace_id` for
traces) into N fixed buckets -- the morsel-driven radix exchange (D5). Hashing
gives trivial routing and even load with no split/merge machinery; an ingest
queue needs no key locality or range scans. Range partitioning was considered
and rejected as unnecessary machinery for a queue.

**Why it matters:** it determines load evenness, routing complexity, and
whether a split/merge state machine (Monarch) is needed or fixed buckets (M3)
suffice.

**Implications:** drives the auto-sharder, the placement-map representation,
and the rebalancing strategy. The shuffle key co-locates a metric's data with
its type registry (see Architecture). Couples to D5 (the value of N) and the
hot-`metric_name` sub-partitioning noted under Auto-sharding.

### D4. Storage locality: local disk versus shared object store

**Decided:** the storage backend is a configuration choice -- both are
first-class, and operators pick per deployment. Quiver commits to a
segment-store backend seam (it already has a `SegmentProvider`) so local-disk
and object-store backends coexist.

**Always local -- the hot tier:** the WAL and the open (still-accumulating)
segment stay on local disk in every configuration: the ack path needs
low-latency fsync, and local segments use mmap zero-copy reads. The backend
choice applies to the **finalized immutable segments** (the cold tier), which
is exactly the split quiver's roadmap already anticipates.

**Two backends:**

- *Local disk (default)*: zero external dependencies. HA is replication factor
  R across owners' local disks (bucket-level fan-out on the balanced topic);
  handoff streams or replays segments (M3 `Initializing` and `Leaving`).
- *Object-store cold tier (opt-in)*: finalized segments go to a neutral
  S3-compatible store (MinIO, Ceph, SeaweedFS -- no specific vendor blessed).
  Durability and finalized-segment handoff (lease transfer plus tail replay)
  come from the store; the local WAL and in-flight handoff remain. Adds a
  dependency and object-store latency and cost, which the operator accepts.

**Why it matters:** this is the core dependency decision (the
minimum-dependency posture). Making it configurable keeps the default
zero-dependency while offering elasticity to operators who want it, rather than
mandating one.

**Implications:** commits the Quiver segment-store backend seam, two handoff
paths (local streaming versus store tail-replay), and the replication fan-out
on the balanced topic for the local path. Couples to D5 (per-bucket streams)
and D6 (the disk budget applies to the local tier).

### D5. Sizing: logical bucket count N

**Decided:** N (the number of logical buckets per signal) is sized as a small
multiple of the maximum core count -- roughly 16x to 64x, landing in the
hundreds to low thousands -- and is a power of two so the radix split is cheap.
This is the partition fan-out of a morsel-driven exchange (Leis et al., SIGMOD
2014): incoming data is shuffled by a signal-specific key into N buckets, each
owned by a core under the engine's thread-per-core pipeline groups.

**Persistence model:** buckets are physically separated, but not as N
independent logs. Each owner core runs one `quiver` instance with a single
sequential per-core WAL; each bucket is a distinct Arrow IPC stream within that
core's finalized segments (extending quiver's existing multi-stream segment
format, keyed by `bucket x schema`). This gives true physical separation in the
durable, independently-handoff-able artifact while sharing the per-core WAL and
open-segment budget. Size N so the streams-per-segment count (N / cores) stays
Arrow-efficient (tens to low hundreds).

**Why it matters:** N bounds rebalancing granularity and worst-case per-bucket
load, and it cannot be changed cheaply once keys are hashed into it (as with
M3's fixed shards). Changing N later requires a migration, so it is effectively
fixed.

**On M:** for metrics the manifest key (`metric_name`) is the same as the data
bucket key, so the manifest co-partitions with the data rather than forming a
separate plane of M shards (the two-plane-collapse follow-up under D2). M is
therefore not an independent number for metrics; where a separate manifest
plane is retained, keep it small (tens).

**Implications:** capacity planning is per-core (one WAL, one open segment),
independent of N; per-bucket cost is one stream slice plus one placement-map
row. Couples to D3 (shuffle key) and the execution model.

### D6. Quality of service default: lossless versus loss-tolerant

**Question:** when the durable buffer is full or a downstream is slow, do we
block (lossless) or shed (loss-tolerant)?

**Why it matters:** the design principles require supporting both. The default
sets behavior under sustained overload and interacts with admission
backpressure, the disk budget, and the process memory limiter.

**Options:**

- *Lossless*: apply backpressure to receivers; risks propagating impact
  upstream.
- *Loss-tolerant*: shed under pressure to protect the pipeline.

Both are configurable per signal and per pipeline.

**Decided:** make it explicit per pipeline. Default loss-tolerant for metrics
with a clearly documented lossless mode, wired to the Quiver disk budget and
the existing memory limiter and per-tenant limiters. Shedding (loss-tolerant)
and backpressure (lossless) both act pre-ack at admission; once a row is
admitted and acknowledged it is durable at-least-once.

**Implications:** connects to receiver admission, the disk-budget policy, and
`docs/memory-limiter-phase1.md`.

### D7. Type evolution and overrides

**Question:** when a name's canonical (primary) descriptor should change -- a
unit fix or an instrument-type correction -- how is that done?

**Scope note:** because temporality and value type are non-manifesting (see
"Identity and type model"), changing either is permitted variation, not a type
change. And because conflicts are admitted and warned rather than rejected (see
the type registry), a mistaken first observation no longer poisons a name: both
the wrong and right descriptors coexist, and the recipient (or an override)
picks the primary.

**Why it matters:** operators still want to choose which descriptor is
canonical for display and to silence a known-benign conflict warning, without a
restart.

**Decided:** keep the data path admit-all; add an explicit,
generation-stamped admin override that sets the primary descriptor for a name
and optionally suppresses its conflict warning. The override is logged durably.
Avoid silent relearn.

**Implications:** the registry needs a primary-descriptor field and an admin
control surface; the override must be logged durably. No data is dropped either
way.

### D8. Manifest eviction and garbage collection

**Question:** do manifest entries live forever, or are idle types evicted?

**Why it matters:** without eviction the registry grows with churned or
short-lived names; with eviction an idle name's primary descriptor is forgotten,
so the next observation re-records it. This is harmless now that conflicts are
warned, not rejected -- there is no relearn race.

**Options:**

- *Never evict*: bounded by the number of distinct names, which is usually
  modest.
- *TTL eviction of idle entries*: bounds memory; relearn simply re-records the
  primary descriptor (no race).
- *Reference-count by active series*: precise but more machinery.

**Decided:** never evict in v1. Add TTL eviction later only if name
cardinality proves problematic, coordinated with D7.

**Implications:** registry memory footprint, snapshot size, and the interaction
with D7.

### D9. Tenancy

**Decided:** the queue does not define its own tenant concept; it consumes the
engine's tenant model (see `agent-multitenancy-design.md`). Tenant is a
multi-dimensional **descriptor** resolved at the edge by descriptor actions
(`request_header`, `remote_address`, `generic_key`, and -- in single-resource
contexts -- `resource_key`). The tenant that scopes type identity and sharding
is a configured **projection** of that descriptor onto one or more coarse keys
(for example `workspace_id` or `customer_id`), defaulting to a single constant
tenant so simple deployments never see the concept.

**Why it matters:** OpenTelemetry deployments are frequently multi-tenant.
Tenant scopes the conflict domain (D1), shard load distribution
(noisy-neighbor), and quota enforcement. Type authority must be tenant-scoped
so one tenant's `requests` counter cannot conflict with another's gauge of the
same name.

**How it integrates:**

- *Identity and manifest:* the tenant projection is the high-order component of
  both the identity key (`hash(tenant, name) mod N`) and the series identity, so
  type decisions and registration load are isolated per tenant.
- *Quotas and QoS:* per-tenant disk and rate limits reuse the existing limiter
  extensions (`extension:semaphore_limiter` for bytes,
  `extension:token_bucket_limiter` for ops), as in the durable-buffer example
  of the multitenancy design -- not a bespoke mechanism. Couples to D6.
- *Sharding and routing:* tenant-aware placement plugs into the existing
  descriptor routing (`processor:tenant_router`, topic partitioning);
  shuffle-sharding (Mimir style) stays a later placement policy over the same
  keys.
- *Multi-resource batches:* because admission is a vectorized pass over a
  (typically multi-resource) `RecordBatch`, the queue can derive the tenant
  projection per row from resource columns, sidestepping the single-resource
  limit of `resource_key` actions.

**Implications:** identity extraction reads the tenant projection; both shard
keys and admission telemetry are labeled by tenant; per-tenant quotas build on
the limiter extensions. Trust in tenant values is the operator's responsibility
(authenticated headers, per the multitenancy design).

## Beyond the queue

This document is deliberately scoped to the ingest queue: admit, shuffle by
name, and durably buffer raw OTAP at-least-once. General range-query serving and
the time-series store are non-goals *here* on purpose -- they belong to the
layers built on top of this one.

Those layers are described in
[`metrics-appliance-design.md`](./metrics-appliance-design.md), which treats
this ingest queue as **L1** of a durable, disconnection-tolerant metrics
appliance and adds: **L2** event-time windowing and watermarks over the
shuffled streams (extending the `temporal_reaggregation` processor from
processing-time to event-time), **L3** a second `quiver` stage storing complete
aggregated batches keyed by `(metric_name, time_bucket)`, **L4** a DataFusion +
Grafana query interface, and **L5** store-and-forward to a central platform.

Two properties established here are load-bearing for L2: the shuffle by metric
name co-locates all of a name's points on one core (so windowing is local), and
the admission time-window (`max_lag`, `max_skew`) is the safety envelope within
which the downstream watermark operates.

## Status and next steps

**Where this stands:**

- This document is placed; nothing here is implemented yet.
- The durable buffer substrate (`quiver`) exists (experimental). The broadcast
  and balanced topics, the controller, and DataFusion query support exist.
- The manifest, the admission processor, and the auto-sharder do not yet exist.
- All decisions D1 through D9 are ratified. The two-plane collapse (metrics
  shuffle by `metric_name`) is adopted.

**To resume in a new session, start here:**

1. Confirm the metric identity-extraction columns (the name plus the tenant
   projection) feeding the registry, via `pdata-views`.
2. Begin Phase 0: a single-core admission processor (time-window check plus
   per-core registry) admitting all rows with `name_conflict` warning and
   window-rejection telemetry.
3. File tracking issues for the phases and for each ratified decision.

**Related engine docs:** `design-principles.md`, `topic-architecture.md`,
`load-balancing.md`, `memory-limiter-phase1.md`, and the `quiver` crate README.

## Glossary

- **OTAP**: OpenTelemetry Protocol with Apache Arrow; the columnar
  representation used on the data path.
- **Quiver**: this engine's Arrow-based durable segment store (WAL plus
  immutable Arrow IPC segments) used as the durable buffer.
- **Type registry (manifest)**: the per-core registry recording the observed
  descriptor(s) for each identity key and flagging name conflicts; one per
  signal.
- **Identity key**: the identifying properties that determine a record's
  identity within a signal and tenant; for metrics, the metric name within a
  tenant. The manifest key.
- **Identifying properties**: OTel's term for the fields that determine identity
  (as opposed to descriptive properties).
- **Type descriptor**: the manifesting (conflict-checked) properties of a
  metric: instrument type, monotonicity, and unit.
- **Non-manifesting**: a property carried with the data but excluded from the
  manifest descriptor, so a difference in it is not a conflict; for metrics,
  temporality and value type.
- **Series identity**: tenant projection, resource attributes, metric name, and
  point attributes (scope excluded as non-identifying); the effective-once dedup
  key (not the shard key).
- **Bucket (logical shard)**: one of N fixed hash buckets over the signal key;
  the unit of placement and rebalancing.
- **Lease and generation**: the per-shard ownership token and its monotonic
  counter, used to fence a deposed owner's writes.
- **Name conflict**: a name carrying more than one distinct descriptor; both
  streams are admitted and a `name_conflict` warning is emitted for the
  recipient to resolve (optional strict mode rejects instead).
