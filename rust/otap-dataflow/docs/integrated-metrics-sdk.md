# Integrated Metrics SDK: OTAP-direct, self-aggregating across cores

## Status and purpose

This document is a design summary and a team alignment guide. It sketches an
OTAP-direct metrics SDK for the otap-dataflow engine that consumes the engine's
own metrics the way the integrated logs/spans sampler already consumes its own
logs and spans. The target is a hyperscale node with a large CPU count, on which
the SDK aggregates its own metrics in a balanced way across its own cores and
NUMA regions and produces OTAP metric batches at minimum cost.

The engine's own metrics are not the only telemetry a node aggregates, and the
origin is not always an OpenTelemetry receiver. On a host, lightweight SDKs push
points over other channels: a StatsD receiver carrying metric-lines, a Linux
user_events probe, or ETW on Windows. These are not a separate origin bolted on
beside the cores. In the thread-per-core engine every core runs its own
receivers, internal telemetry alongside StatsD and user_events, and they share
nothing at the receiver end; the per-core data they produce stays on the core
that produced it. Sharing begins downstream, at the single-writer aggregation
core and the OTAP encoder they all feed. This is the small-scale form of the
durable metrics appliance, and it is why so many components are shared.

The goal is to let several engineers work in parallel on separate, reusable
sub-components with clear seams. The [Component decomposition](#component-decomposition)
section is the work breakdown; the sections before it establish the shared model
everyone builds against.

This design ties together prior work rather than starting fresh:

- The four-phase **OTAP-direct metrics SDK** proposal, otel-arrow#2623, which
  set the direction of replacing the OpenTelemetry-Rust metrics SDK with a
  schema-driven, OTAP-native path and named the global, NUMA-regional, and
  CPU-local telemetry topologies.
- The **granular vs agnostic signal-metric schemas** proposal, otel-arrow#3369,
  which designs the write-once instrumentation macro surface and the codegen
  that selects an output schema per configuration.
- The **integrated sampler** mechanism for logs and spans, described in
  [`integrated-sampler-engine-mechanism.md`](integrated-sampler-engine-mechanism.md),
  whose per-worker to all-CPU aggregation with `ArcSwap` feedback is the pattern
  this design generalizes to metrics.
- The **durable metrics appliance**, `metrics-appliance-design.md`, whose
  shuffle-by-name and event-time aggregation is the large-scale counterpart of
  the small-scale aggregation the SDK performs over its own telemetry.

## The model to follow

Logs and spans already flow through the engine as internal telemetry without the
OpenTelemetry SDK. The path has three parts, all implemented in the
`otap-df-telemetry` and `otap-df-engine` crates:

1. Each engine worker is a core-pinned thread-per-core runtime and owns a
   thread-local buffer. It records into non-atomic slots on the hot path and
   never blocks on telemetry.
2. On the telemetry-collection cadence, each worker flushes its buffer over a
   non-blocking channel to a single process-global aggregator. The aggregator is
   owned by the Internal Telemetry Receiver, the "ITR", which is the single
   per-process bridge from internal telemetry into an OTAP pipeline.
3. The ITR finalizes on each `CollectTelemetry` control tick, emits the
   aggregated result into the internal observability pipeline, and republishes
   feedback tables into a process-global `ArcSwap` registry that every worker
   reads wait-free.

Metrics do not yet use this path. Today they take a separate route through the
OpenTelemetry SDK, described next. Bringing metrics onto the ITR path, as OTAP
rather than OTLP bytes, is the core of this design.

## Background: how metrics work today

The current internal metrics path is a bespoke multivariate system followed by a
univariate bridge to the OpenTelemetry SDK.

1. **Declare.** A node declares a multivariate metric set with the
   `#[metric_set(name = ...)]` macro over a struct whose fields are instruments
   such as `Counter<u64>`, `Gauge<f64>`, or the `Mmsc` min/max/sum/count
   summary. See `crates/telemetry-macros/src/lib.rs` and, for an example,
   `crates/core-nodes/src/processors/filter_processor/metrics.rs`. The macro
   expands the struct into a static `MetricsDescriptor` and a `MetricSetHandler`
   implementation. The entity attributes are predeclared once per set through an
   attribute set and shared by every field.
2. **Record.** Hot paths mutate fields directly with `add` or `inc`. The slots
   are per-core and non-atomic, so there is no cross-core contention. The engine
   already carries a per-core identity through `pipeline_ctx.core_id()`.
3. **Report.** `MetricsReporter::report` in `crates/telemetry/src/reporter.rs`
   snapshots a set into a `MetricSetSnapshot` and sends it over a bounded
   `flume` channel. A full channel defers rather than blocks.
4. **Collect.** `InternalCollector` in `crates/telemetry/src/collector.rs`
   drains the channel and accumulates snapshots into one
   `TelemetryRegistryHandle`, a single global aggregation point keyed by entity.
5. **Dispatch.** `MetricsDispatcher` in `crates/telemetry/src/metrics/dispatcher.rs`
   periodically visits the registry and, for each set, emits **each field as a
   separate univariate instrument** into the OpenTelemetry SDK meter provider,
   which then exports OTLP or Prometheus. Entity attributes ride on the
   instrumentation scope; data-point attributes are hard-coded empty.

Three properties matter for what follows. The multivariate structure that the
macro captures is discarded at step 5, where a set becomes N unrelated
univariate instruments. All aggregation converges on one registry, which is a
single-tier fan-in. And the OpenTelemetry SDK sits on the export path, pulling in
the `opentelemetry` and `prometheus` dependencies and limiting the aggregations
and encodings available to what that SDK supports.

## What changes for metrics

The idealized internal metrics system aggregates metric sets and computes OTAP
batches at minimum cost. Three changes deliver that.

**Route metrics through the ITR as OTAP, not through the OpenTelemetry SDK.**
The ITR already owns the logs stream and the integrated-sampler aggregator and
already finalizes on the `CollectTelemetry` tick. A metrics aggregator owned by
the ITR, finalized on the same tick, emits `OtapArrowRecords::Metrics` into the
internal observability pipeline exactly as the sampler emits logs. This mirrors
otel-arrow#2623's `ITS` provider mode, which coexists with a `Builtin`
Prometheus mode and, during transition, the legacy `OpenTelemetry` mode.

**Encode directly as OTAP columns with codegen, preserving the multivariate
shape.** The metric set is already the natural unit of a shared scope and a
shared timestamp, which is what an OTAP metrics record group expresses. Schema
codegen turns each set into precomputed schema-side Arrow tables built once and
the per-tick data-point tables built from the aggregated values, with no
intermediate OTLP objects and no per-point attribute hashing on the hot path.
The encoders in `crates/pdata/src/encode/record/metrics.rs` and the
`OtapArrowRecords::Metrics` payload are the substrate.

**Aggregate in tiers that respect cores and NUMA regions.** The single-registry
fan-in of step 4 becomes a per-core to NUMA-regional to global reduction so that
most merging stays core-local and NUMA-local, and only compact partials cross a
socket boundary. This is the part that makes the SDK scale on a large machine,
and it is described next.

## Architecture: tiered, balanced aggregation

The SDK is a small-scale instance of the large-scale aggregation pipeline. It
uses the same ideas as the durable metrics appliance and the same industry
practice the appliance draws on, applied to the engine's own telemetry rather
than to ingested customer data.

![OTAP metrics on one Linux node: every thread-per-core core runs its own internal-telemetry, StatsD, and user_events receivers with share-nothing ingest, feeding per-core to NUMA-regional to global aggregation, a single-writer aggregation core, and an OTAP-direct encoder, with the ArcSwap feedback control plane](./images/metrics-sdk-hyperscale.svg)

The same flow in text:

```text
  tier 1: per-core            tier 2: NUMA-regional        tier 3: global
  +--------------------+
  | #[metric_set]      |  snapshot deltas
  | non-atomic slots   |----------------\
  | (one per core)     |                 \
  +--------------------+                  \--> +------------------+
  +--------------------+   per-core            | regional         |  regional
  | #[metric_set]      |-- deltas ------------>| aggregator       |  partials
  | (another core,     |                       | (one per NUMA    |----------\
  |  same region)      |                       |  node)           |           \
  +--------------------+                       +------------------+            \
                                                                               \--> +-----------+
  (other NUMA regions, same shape) ------------ regional partials ------------------>| global    |
                                                                                     | aggregator|
                        ^                                                            | in the ITR|
                        |  ArcSwap registry: schema/level, cardinality,              +-----+-----+
                        |  and ownership-assignment tables                                 |
                        \--------------------------------------------------------------------/
                                                            emit OtapArrowRecords::Metrics -->
```

**Tier 1, per-core.** Unchanged from today at the point of recording. Each core
owns its metric sets and mutates non-atomic slots. On the collection tick a core
snapshots its dirty sets into compact deltas using the existing
`needs_flush` and `snapshot` fast paths, touching only non-zero fields.

**Tier 2, NUMA-regional.** Within a NUMA region, a regional aggregator sums the
per-core deltas for the identities it owns. This merge is additive and holds no
cumulative state; it reduces many per-core partials to one regional partial.
Keeping this reduction inside the region is what avoids paying cross-socket
traffic on every core's every flush. This tier corresponds to the appliance's
observation that once all points for a series are co-located on one owner, the
aggregation for that series is a purely local operation with no cross-core
exchange and no distributed coordination.

**Tier 3, the temporal aggregation processor.** The regional partials converge,
still as deltas, on the temporal aggregation processor for their partition. That
processor is the single writer for those series: it windows them, resolves
delta-to-cumulative with reset, gap, and overlap, and produces the OTAP batch.
The ITR hosts it and finalizes on the `CollectTelemetry` tick, so the window is
the engine's existing telemetry cadence and needs no separate clock. At the
largest scale this stage is itself partitioned, and the partitions are unioned
rather than re-merged, because each series has exactly one owner.

**Feedback.** A process-global `ArcSwap` registry carries control tables from the
aggregator back to the producers, the same wait-free mechanism the sampler uses
for its span-start and heavy-hitter tables. For metrics the tables are the active
schema and metric level, the cardinality budget, and, when balancing is enabled,
the ownership assignment that maps an identity to its regional slot.

### Why this mirrors hyperscale metrics systems

The tiering is deliberately the shape that Google Monarch, Uber M3, and
VictoriaMetrics use at fleet scale, reduced to one machine. Monarch aggregates
in a hierarchy of leaves and zones close to where data is produced before a thin
global layer; the per-core to NUMA-regional to global reduction is that
hierarchy expressed as memory locality tiers. Ownership of a series by a single
aggregation slot, so that its cumulative state is single-writer, is the same
property M3 and VictoriaMetrics rely on for correct counter aggregation, and it
is the same single-writer destination the OpenTelemetry data model requires for
delta-to-cumulative conversion.

### When balancing is needed, and how

SDK-level metrics are bounded and predeclared, so in the common case the
per-identity load is even and static assignment by a hash of the identity is
enough. Two pressures at high core count justify dynamic balancing:

- The fan-in from many cores to a single global aggregator serializes the merge.
  The regional tier removes most of it; a hot region can be split further.
- A small number of high-cardinality dimensioned sets can dominate one slot.

For these, an assignment table moves ownership of hot identities to dedicated
slots, in the spirit of Google Slicer and Microsoft Centrifuge, which balance
key ranges across servers automatically. Reassignment is applied at a
collection-window boundary, where no live aggregator state is in flight, exactly
as the appliance rebalances at window boundaries so a moved key carries no
partial state. The assignment table travels over the same `ArcSwap` registry as
the other feedback, so producers pick up a new mapping wait-free.

## Three per-core receivers, one aggregation core

The architecture above aggregates the engine's own telemetry, and the same
machinery serves telemetry that arrives on the host by other means. The naive
picture puts the engine's cores on one side and a bank of receivers on the
other, but that is not the shape. In the thread-per-core engine each core runs
its own receivers side by side: the internal telemetry receiver for the engine's
own `#[metric_set]` recording, a StatsD receiver for metric-lines, and a
user_events receiver for kernel tracepoints. They share nothing at the receiver
end. Each core owns its own StatsD socket and its own user_events consumer, the
way the ETW receiver already fans out to a per-core channel, so a core receives,
records, and holds its share locally.

Because ingest is share-nothing, the data stays on the core that produced it.
What is shared is downstream. Each core snapshots its three receivers'
recordings into per-core deltas, and those deltas flow through the same
NUMA-regional and global aggregation, the same single-writer ownership of a
series, and the same OTAP-direct encoder that the engine's own metrics use.
Sharing begins at the aggregation core, not at ingest. The feedback control
plane reaches all three receivers on a core equally, since the cardinality
budget and the ownership assignment bind identities without regard to which
receiver produced them.

```text
  Core k  ·  thread-per-core, share-nothing
    +-- internal telemetry    #[metric_set]
    +-- StatsD receiver       metric-lines
    +-- user_events receiver  tracepoints
          |  record locally; data stays on core k
          v
    per-core deltas --> NUMA-regional --> shared aggregation core
                                              |  single writer per identity
                                              v
                                           OTAP-direct encoder --> pipeline
```

The clock that closes a window is the one thing that varies by source. The
engine's own metrics finalize on the `CollectTelemetry` tick, so the tick is the
window. Host-pushed points carry their own event time, which can lag or run
ahead of arrival, so they are windowed by event time with watermark-driven
completion and late-arrival handling. That windowing is the appliance's L2, and
the shuffle by name that gives each series one owner is the appliance's L1. With
bounded SDK metrics a static hash keeps most series on their origin core, and
the balancer moves only a hot identity. The whole node is therefore the durable
metrics appliance at small scale, with durability an optional backend at the
ingest and store seams rather than a separate design.

## The instrument and schema model

The recording surface and the schema codegen are where otel-arrow#2623 and
otel-arrow#3369 combine. The principle both share is a single code surface that
compiles to multiple output schemas, with the choice resolved at registration
from configuration.

**Instruments** stay the per-core, non-atomic, fully-bound types the engine has
today: `Counter`, `UpDownCounter`, `ObserveCounter`, `ObserveUpDownCounter`,
`Gauge`, and the `Mmsc` summary, with the exponential histogram added later.
Fully bound means there are no dynamic attributes on an instrument; every
dimension is declared in the schema.

**Metric levels** select memory and detail at runtime from a compiled-in set of
choices. otel-arrow#2623 defines Basic, Normal, and Detailed, generated as an
enum whose variants hold arrays sized to the active dimension count, so that a
lower level literally allocates fewer counters. A boxed variant keeps the level
choice from inflating the common case.

**The signal axis** is otel-arrow#3369's contribution. A `#[signal_metric]`
field with a `SignalMetric<I>` type is written once and recorded once with
`add(Signal::X, n)`, and the generated encoder emits it in whichever of two
shapes the configuration selects. Granular puts the signal in the metric name,
producing `consumed_log_records`, `consumed_metric_points`, and `consumed_spans`.
Agnostic puts the signal in a data-point attribute on a single `consumed_items`
metric. This is the same write-once-emit-many idea as metric levels, applied to
the signal dimension, and it is why the per-data-point attribute path is a shared
prerequisite rather than a schema-specific cost.

**Schema-driven codegen** binds these together. A `cargo xtask generate-metrics`
command reads schema definitions and a semantic-convention registry and emits the
level-aware instrument enums, the metric-set structs, the OTAP encoder
implementations, and the metric documentation. otel-arrow#2623 makes this a
Weaver-based registry so declared and emitted metrics cannot drift and so a
client SDK can be generated from the same source; otel-arrow#3369 recommends
extending the existing proc-macro first and adopting Weaver validation
immediately, with full Weaver codegen as the end state. Both agree on codegen as
the mechanism, which is the buy-in this design builds on.

## OTAP-direct encoding at minimum cost

The batch computation is split into a part that is built once and a part that is
built per tick, so the recurring cost is only proportional to the non-zero data.

**Precomputed once, at registration.** The schema-side tables do not change
between ticks: `UnivariateMetrics` with the metric name, type, unit, temporality,
and description; `ScopeAttrs` with the entity attributes of each set; and, when
the chosen encoding uses them, `NumberDPAttrs` for dimension attributes. All
columns are dictionary-encoded so repeated names and attribute values are stored
once. These tables are built with the record-batch builders in `pdata` and held
as Arc-shared columns.

**Built per tick, from the aggregated values.** Only the data-point value tables
are recomputed: `NumberDataPoint` for scalar instruments and the histogram
data-point table for `Mmsc` and, later, the exponential histogram. Because the
aggregator produces these columns directly, there is no OTLP object graph to
build and discard, and no per-point attribute hashing on the hot path.

otel-arrow#2623 catalogs the encoding choices and their row counts, from a flat
scope-per-set layout, to scope attributes carrying the dimensions, to
per-data-point attributes. The best choice depends on the number of metrics per
set and the dimensionality, and the schema codegen selects it per set. The
long-term direction is first-class OTAP multivariate, where the multivariate
metric set collapses to one scope with several measurement columns sharing a
timestamp, which is the most compact of all and the reason the SDK preserves the
multivariate shape end to end rather than splitting into univariate instruments.

**Batching is not aggregation, and the two live in different places.**
Turning an arriving `MetricSet` into OTAP rows is encoding and buffering; the
aggregation is the delta-to-cumulative merge keyed by identity. That merge wants
keyed state, a map from identity to a cumulative anchor, not a growing column,
because merging in columnar form would be a group-by over the accumulated rows
on every arrival. That keyed state belongs to the temporal aggregation
processor, and the OTAP builder runs only when a window emits. Per-core
snapshots are already deltas, deltas merge additively, and the pipeline carries
deltas until the temporal aggregation processor resolves the cumulative, once,
as the single writer for that series.

**Split before you encode.** When metric data is sharded onto partitions or
topics, compute the bucket as each `MetricSet` arrives and route the snapshot
into that partition's slot, so the encoder builds one batch per partition
directly. Building a single batch and then re-splitting it with take and filter
is a columnar re-shuffle worth avoiding. Shard on set identity, meaning the
scope plus the set, so a multivariate set stays whole and keeps its compact
one-scope record group; a hot dimensioned set is split below that granularity
only when the ownership table says so, at a window boundary. The schema-side
columns are built once and shared across every partition, so a per-partition
batch fills only its data-point columns and the recurring cost stays
proportional to the non-zero points in each partition.

**How many times you encode is a backend choice, not a design change.** With an
in-memory topic the snapshot is handed to the temporal aggregation processor by
move, merged into its anchors, and encoded once per partition when the window
emits. With a durable topic the seam must be serialized, so the sender builds
per-partition OTAP delta batches and the processor decodes, merges, and encodes
the result. The shuffle-then-aggregate-then-encode path is identical either way;
only the number of encodings and the presence of a serialized delta batch
differ, which is exactly the durability axis the appliance already treats as
orthogonal.

These choices also fix the assignment key as the set identity and carry
inter-tier partials as deltas, with the cumulative resolved in the temporal
aggregation processor.

## Component decomposition

The design factors into components with narrow interfaces so that teams can build
and test them independently. The two load-bearing seams are the **aggregation
core**, which is signal-agnostic and reused at all three tiers, and the **OTAP
encoder**, which is driven entirely by the precomputed schema. Everything else
plugs into those two. The same two seams also carry the host receivers: every
core runs a StatsD and a user_events receiver beside its internal telemetry,
sharing nothing at ingest, and all three feed the identical aggregation core and
encoder.

| # | Component | What it owns | Interface / seam | Reuses / status |
| - | --------- | ------------ | ---------------- | --------------- |
| A | Instrument library | Per-core non-atomic instrument types and the exponential histogram | Record methods; snapshot to a value column | `crates/telemetry/src/instrument.rs`; histogram is Phase 4 |
| B | Schema + codegen | `cargo xtask generate-metrics`; instrument enums, set structs, encoder impls, docs | Schema files plus semconv registry in, generated Rust out | New; otel-arrow#2623 Phase 2 |
| C | Signal axis macro | `Signal`, `SignalMetric<I>`, `#[signal_metric]`, granular/agnostic selection | Same `add(Signal::X, n)` call site; schema chosen at registration | otel-arrow#3369 |
| D | Per-core collector | Snapshot dirty sets into compact deltas on the tick | `needs_flush` and `snapshot`; sparse enumeration | Extends `reporter.rs` |
| E | Temporal aggregation processor | Event-time windows and watermarks; per-partition anchor map keyed by identity; spatial merge of partials and delta-to-cumulative with reset, gap, and overlap; single writer per series | Ingest deltas, drain closed windows; signal-agnostic | Reuses the appliance's `temporal_reaggregation` L2 |
| F | NUMA topology | Region discovery, aggregator placement, per-core to region and region to global channels | Non-blocking channels, drop-and-count | New; NUMA support is noted "soon" in `telemetry/src/lib.rs` |
| G | Balancing + feedback | `ArcSwap` control tables; optional ownership assignment for hot identities | Publish tables, read wait-free; reassign at window boundary | Pattern from the sampler registry and the appliance load feedback |
| H | OTAP encoder | Precomputed schema tables plus per-tick, per-partition data-point builders to `OtapArrowRecords::Metrics` | Aggregated values in, one Arrow batch per partition out | `crates/pdata/src/encode/record/metrics.rs` |
| I | ITR bridge + modes | ITR bridge and provider modes None, Builtin, OpenTelemetry, ITS; hosts an instance of E per partition and forwards deltas to it | Finalize on `CollectTelemetry`; emit into the pipeline | Extends `internal_telemetry_receiver` |
| J | MetricsTap | Built-in Prometheus endpoint and console formatter without a full pipeline | Reads the in-transit representation directly | otel-arrow#2623 Builtin mode |
| K | Per-core host receivers | A StatsD and a user_events receiver on every core, beside internal telemetry; share-nothing ingest, data stays on core | Decode to points or deltas into the per-core collector | Reuses `contrib-nodes` `user_events_receiver` and `etw_receiver`; `host_metrics` emits OTAP metrics |

Component E is the piece most worth designing carefully first. Its windowing and
its delta-to-cumulative logic with reset, gap, and overlap are the appliance's
`temporal_reaggregation`, reused rather than rebuilt. The per-core, regional, and
global tiers ahead of it only merge deltas additively to cut fan-in; E is the
single stage that resolves the cumulative, per partition, as the single writer.
Components B and C can proceed against a frozen aggregation and encoding
interface without waiting for F and G. Component H can be built and tested from
static aggregated inputs before the tiering exists. Component I can host a
single-tier aggregator first and gain the tiering later, so the ITS path is
usable early.

E keeps its running state as a per-partition anchor map, never in columnar form,
and H builds one batch per partition from those anchors while reusing the shared
schema columns. The split by bucket therefore happens at the F and G seam,
before any column is formed, so no stage ever builds a monolithic batch only to
re-split it.

The host receivers add one component per core and reuse the rest. Because
component K wraps receivers that already exist in `contrib-nodes`, host-pushed
metrics can join the shared core as soon as D, E, and H are stable, before the
tiering of F and G lands.

## Phasing

The phases extend otel-arrow#2623's four phases with the hyperscale tiering as a
final layer, so that a useful ITS metrics path exists long before the full
NUMA-regional machinery lands.

1. **Drop-in ITS path.** Add the metrics provider modes alongside the logs modes.
   Route the existing `#[metric_set]` structs through a single, ITR-hosted
   instance of the temporal aggregation processor that resolves cumulative and
   encodes OTAP directly, replacing the OpenTelemetry SDK dispatch. Components
   D, E in single-tier form, H, and I.
2. **Schema codegen and the signal axis.** Land B and C. Generate a `v0` schema
   matching current instrumentation, then streamlined `v1` schemas with metric
   levels and the granular or agnostic signal choice, selectable at runtime.
3. **Remove the OpenTelemetry SDK.** With all sets generated, drop the OTel SDK
   and its Prometheus dependency. Two modes remain, Builtin and ITS, with
   MetricsTap serving Prometheus and console. Component J.
4. **Exponential histograms.** Add the allocation-free exponential histogram as
   a Detailed-level alternative to `Mmsc`. Component A completion.
5. **Hyperscale tiering.** Introduce the NUMA-regional tier and balancing.
   Components F and G. This is the layer this document adds beyond
   otel-arrow#2623, and it is what lets the SDK aggregate its own metrics in a
   balanced way across a large core count.

## Open questions

- **Placement of the regional tier.** Whether tier 2 is a dedicated aggregation
  thread per NUMA node or a role rotated among that region's workers, and how it
  interacts with the one-ITR-per-region option.
- **Back-pressure symmetry with logs.** Confirming the metrics channels adopt the
  same never-block, drop-and-count discipline the sampler uses, and how a dropped
  per-core flush is accounted so the loss is visible.
- **Weaver timing.** When to move from the proc-macro source of truth to full
  Weaver-driven codegen, per the otel-arrow#3369 recommendation to validate with
  Weaver now and generate from it later.

## References

- otel-arrow#2623, Internal metrics telemetry pipeline and SDK design, the
  four-phase OTAP-direct proposal and the OTAP encoding forms.
- otel-arrow#3369, Dual signal-metric schemas, the write-once macro surface and
  granular versus agnostic codegen.
- [`integrated-sampler-engine-mechanism.md`](integrated-sampler-engine-mechanism.md),
  the per-worker to all-CPU aggregation with `ArcSwap` feedback this design
  generalizes.
- [`integrated-logs-traces-reservoir.md`](integrated-logs-traces-reservoir.md)
  and [`span-start-value-sampling.md`](span-start-value-sampling.md), the sampler
  algorithm and its span-start value math.
- `metrics-appliance-design.md`, the large-scale shuffle-by-name and event-time
  aggregation whose single-writer and window-boundary rebalance properties the
  SDK reuses at small scale.
- `crates/contrib-nodes/src/receivers/user_events_receiver` and `etw_receiver`,
  the on-host ingress channels, and `host-metrics-receiver.md`, the host metrics
  receiver that emits `OtapArrowRecords::Metrics` directly.
- [`telemetry/metrics-guide.md`](telemetry/metrics-guide.md), the entity-centric
  metric-set model and the per-core, NUMA-aware cold-path intent.
- `crates/telemetry/src/metrics.rs`, `metrics/dispatcher.rs`, `reporter.rs`,
  `collector.rs`, `instrument.rs`, and `crates/telemetry-macros/src/lib.rs`, the
  current instrumentation and bridge.
- `crates/pdata/src/encode/record/metrics.rs` and the `OtapArrowRecords::Metrics`
  payload, the OTAP metrics encoding substrate.
