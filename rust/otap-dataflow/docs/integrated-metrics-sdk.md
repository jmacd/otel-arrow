# Integrated Metrics SDK: OTAP-direct internal telemetry at minimum cost

## Purpose

The engine records its own metrics on every core through the `#[metric_set]`
macro and then, on a timer, bridges them into the OpenTelemetry SDK, which
re-aggregates and exports OTLP or Prometheus. This document proposes replacing
that bridge with an OTAP-direct path that reuses the machinery the engine
already has: per-core non-atomic recording, a slotmap-keyed registry, the
Internal Telemetry Receiver that logs and the integrated sampler already use,
and the OTAP metrics record-batch builders in `pdata`.

The objective is performance. The recording hot path is already close to
optimal and does not change. The cost this design removes is on the cold path,
where the current code allocates a dense value vector per set per tick and then
rebuilds an OpenTelemetry instrument per field per tick before discarding it.
Every change below is justified by a specific cost in the current code, and the
phasing is ordered so each step is measurable on its own.

This design covers the engine's own internal metrics. Host-pushed sources that
already produce OTAP metrics join the same encoder without a bridge, and the
final section says which receivers those are. It does not invent new receivers.

## The current path, and where the cost goes

The path has five stages, all real today.

1. **Declare.** A node declares a multivariate set with
   `#[metric_set(name = ...)]` over a struct of instruments such as
   `Counter<u64>`, `Gauge<f64>`, or the `Mmsc` min/max/sum/count summary. See
   `crates/telemetry-macros/src/lib.rs` and, for a live example, the six-counter
   `FilterPdataMetrics` in
   `crates/core-nodes/src/processors/filter_processor/metrics.rs`. The macro
   expands the struct into a static `MetricsDescriptor` and a `MetricSetHandler`.
2. **Record.** Hot paths mutate fields directly with `add` or `inc`. The
   instruments are `#[repr(transparent)]` non-atomic wrappers with `#[inline]`
   mutators, so recording is a plain memory write with no atomics and no
   contention. See `crates/telemetry/src/instrument.rs:19-120`.
3. **Report.** `MetricsReporter::report` gates on `needs_flush`, snapshots the
   set, and sends the snapshot over a bounded `flume` channel with `try_send`,
   deferring rather than blocking when the channel is full. See
   `crates/telemetry/src/reporter.rs:61-77`.
4. **Collect.** `InternalCollector` drains the channel and folds each snapshot
   into a single `Arc<Mutex<TelemetryRegistry>>` keyed by slotmap index. See
   `crates/telemetry/src/collector.rs:48-53` and
   `crates/telemetry/src/registry.rs:144-153`.
5. **Dispatch.** `MetricsDispatcher` runs its own timer and, for each set,
   emits each field as a separate univariate instrument into the OpenTelemetry
   SDK meter provider, which then exports. See
   `crates/telemetry/src/metrics/dispatcher.rs`.

Three costs live in stages 3 through 5.

**A dense, allocating snapshot on every flush.** The macro generates
`snapshot_values` as `Vec::with_capacity(N)` followed by a push of *every*
field, regardless of which fields changed. See
`crates/telemetry-macros/src/lib.rs:301-304`. `needs_flush` only decides whether
to snapshot at all; once any field is non-zero the whole set is copied. Each
element is a `MetricValue`, an enum sized to its 32-byte `Mmsc` variant even
when the set is entirely `u64` counters, as the `#[allow(variant_size_differences)]`
note at `crates/telemetry/src/metrics.rs:27` records. So a hot set pays one heap
allocation plus a full dense copy into oversized slots on every collection tick.

**Instrument churn through the SDK.** On each dispatch the code calls
`meter.u64_counter(field.name).with_description(..).with_unit(..).build()` and
then `.add(value, &[])`, for every field, and lets the instrument drop. See
`crates/telemetry/src/metrics/dispatcher.rs:182-187`. The multivariate set that
the macro captured is discarded into N unrelated univariate instruments, entity
attributes are carried on the instrumentation scope, and data-point attributes
are hard-coded empty at `dispatcher.rs:85`. The SDK is always constructed and
sits on the export path, as `crates/telemetry/src/lib.rs:281-283` shows, which
also fixes the available temporalities and encodings to what the SDK supports.

**A second clock.** Metrics dispatch runs on its own `reporting_interval`
ticker at `crates/telemetry/src/metrics/dispatcher.rs:45`, independent of the
`CollectTelemetry` control tick that logs and the integrated sampler finalize
on. Metrics are therefore the one internal signal not aligned to the engine's
telemetry cadence.

The merge in stage 4 is already efficient and is worth stating so it is not
disturbed. `accumulate_snapshot` zips the incoming values against the stored
values and the descriptor, and applies a per-field operation fixed by instrument
and temporality: delta sums and histograms accumulate with `add_in_place`, while
gauges and cumulative sums replace. See
`crates/telemetry/src/metrics.rs:431-470`. It is index-addressed, allocation-free,
and performs no attribute hashing because identity was resolved to a slotmap key
at registration. The registry mutex guards only registration and this
single-consumer drain, not the recording hot path, as the note at
`crates/telemetry/src/registry.rs:31-36` states.

## The redesign in three moves

### Move 1: emit through the ITR as OTAP on the CollectTelemetry tick

The Internal Telemetry Receiver already holds a handle to the metrics registry.
`MetricsSystem::new` passes the same `TelemetryRegistryHandle` into the
receiver's settings at `crates/telemetry/src/lib.rs:300`, and the receiver
already finalizes the logs stream and the sampler aggregator on each
`CollectTelemetry` tick, republishing its feedback tables through a
process-global `ArcSwap`. See
`crates/core-nodes/src/receivers/internal_telemetry_receiver/mod.rs:180-194`.

The move is to add a metrics finalize step there, on the same tick, that reads
the registry and emits `OtapArrowRecords::Metrics` into the internal
observability pipeline, and to delete the separate `MetricsDispatcher` timer and
its OpenTelemetry emission. `OtapArrowRecords::Metrics` is defined in
`crates/pdata/src/otap.rs`. The result is one clock, one bridge, and no
OpenTelemetry SDK on the metrics export path.

### Move 2: keep the metric set whole and build OTAP columns directly

A metric set is already one scope, one shared timestamp, and N fields. Rather
than explode it into N univariate instruments, encode it as it stands.

The schema-side columns do not change between ticks. Metric name, description,
unit, temporality, and monotonicity are built once at registration and reused.
The existing `MetricsRecordBatchBuilder` already dictionary-encodes name,
description, unit, and temporality with `DictionaryOptions::dict8()`, so repeated
strings are stored once. See
`crates/pdata/src/encode/record/metrics.rs:35-99`. Only the data-point value
columns are rebuilt per tick, from the aggregated values, using the
`NumberDataPointsRecordBatchBuilder` and the histogram builders in the same file.
There is no OTLP object graph to build and discard, and no per-point attribute
hashing, because identity is the slotmap key.

The row layout in `pdata` is univariate today, one row per metric. First-class
multivariate encoding, where a set collapses to one scope with several
measurement columns sharing a timestamp, is the most compact form and the reason
this design keeps the set whole end to end. It is a later step, not a
precondition, and is called out in the phasing.

### Move 3: eliminate the per-tick allocation on the flush path

The dense allocating snapshot in cost A is removable because the macro already
knows each field's static type and index at code-generation time. Instead of
`snapshot_values -> Vec<MetricValue>`, codegen can write each field's value
directly into a reused, typed column keyed by slot, so the flush path performs no
per-tick heap allocation and sizes each column to its actual scalar rather than
to the 32-byte `Mmsc` variant. Delta fields that did not change contribute zero
and can be skipped when the encoder walks the set, which keeps the recurring cost
proportional to the fields that actually moved.

This move and Move 2 meet at the same seam. The producer stages values into
columns, the registry merges by slot as it does today, and the encoder fills only
the data-point columns of the set before handing one batch to the pipeline.

## The hot path does not change

Recording stays exactly as it is: core-pinned worker threads own their metric
sets, mutate `#[repr(transparent)]` non-atomic slots with `#[inline]` methods,
and never touch a lock or an atomic to record. Core pinning is real through
`PipelineContextParams` and `core_id` in `crates/engine/src/context.rs`. This
design touches only the flush, aggregate, and encode cold path.

## Aggregation topology, and what is actually there

Today the topology is per-core reporters feeding a single mutex-guarded registry
that emits once per interval. Because the registry key is a slotmap index, the
per-tick merge is index-addressed with no attribute hashing, and the mutex is
held only by the single collector draining the channel, not by producers.

At very high core count the single fan-in serializes the drain, and the merge
for one series is a purely local operation once that series lands on one owner.
If measurement shows the drain is a bottleneck, the reduction can be split so
that most merging stays core-local before a final union, since each series has
exactly one owner and unions do not re-merge. This is the only place a further
tier would help, and it is gated on a measured need.

NUMA-aware placement is out of scope until a NUMA substrate exists. There is
none today: `numa_node_id` is a fixed `0` with a `ToDo` in
`crates/engine/src/context.rs`, and `crates/telemetry/src/lib.rs:22` lists
NUMA-aware architecture as future work. This design does not depend on NUMA and
does not claim a NUMA tier.

The engine's own metrics are delta at the point that matters for cheap merging.
`Counter` and `Mmsc` are generated as `Temporality::Delta` and merge additively,
while `UpDownCounter`, `ObserveCounter`, and `ObserveUpDownCounter` are
`Cumulative` and replace, and `Gauge` is instantaneous. See
`crates/telemetry-macros/src/lib.rs:138,191-219` and the merge at
`crates/telemetry/src/metrics.rs:448-470`. The OTAP batch carries each field with
its declared temporality. Converting delta to cumulative for an external
consumer is a downstream concern and is not part of this path.

One clarification prevents a wrong reuse. The
`temporal_reaggregation_processor` is not a delta-to-cumulative engine. It is a
downsampler modeled after the Go interval processor: it accepts cumulative
inputs and keeps the latest value per stream over a wall-clock interval. See
`crates/core-nodes/src/processors/temporal_reaggregation_processor/mod.rs:1139,1385-1397`
and its README. It is a standalone pipeline node, not something the ITR hosts,
and this design does not build on it.

## Relationship to the durable metrics appliance

The engine has a separate, larger design for arbitrary ingested metrics, the
durable metrics appliance. That design shuffles by metric name, windows by event
time with watermarks and late-arrival handling, gives each series a single-writer
owner, load-balances across cores, and can persist through disconnection. The
machinery exists because ingested customer data is unbounded, arrives out of
order, and must survive outages. With durability turned off it reduces to a
large-scale aggregating telemetry SDK, which is the point of contact with this
document.

This SDK sits at the very start of the pipeline and controls its own source, so
it can drop most of that machinery and is far cheaper for it. Its metrics are
predeclared, bounded, and fully bound, so identity is a slotmap key resolved once
at registration rather than a per-point hash, and no shuffle is needed to give a
series one owner. It records on the engine's own telemetry cadence, so the
`CollectTelemetry` tick is the window and there are no watermarks or late
arrivals to reconcile. Its counters are delta and merge additively. These are the
assumptions a general ingestion appliance cannot make and this SDK can, and they
are where the performance comes from.

The two are meant to share components, not a pipeline. The instrument library,
the `OtapArrowRecords::Metrics` payload and its record-batch builders in `pdata`,
and the thread-per-core engine context are the common ground. The appliance's
shuffle, event-time windowing, series ownership, and durable store are its own
and are not on this path. Where a component is genuinely signal-agnostic, such as
the OTAP metrics encoder, it should be written once and used by both.

## Instruments, schema, and codegen

Instruments stay the per-core, non-atomic, fully-bound types the engine has
today: `Counter`, `UpDownCounter`, `ObserveCounter`, `ObserveUpDownCounter`,
`Gauge`, and `Mmsc`, with an allocation-free exponential histogram added later.
Fully bound means every dimension is declared in the schema and there are no
dynamic attributes on an instrument.

Codegen is the mechanism for keeping declaration and emission in step. The
`#[metric_set]` proc-macro already generates the descriptor and the handler;
this design extends it to also generate the per-set OTAP encoder, so a set's
declared shape and its emitted columns cannot drift. Two open proposals set the
direction. otel-arrow#2623, now closed, laid out the four-phase OTAP-direct plan
and the OTAP encoding forms, with a Weaver-driven registry as the end state.
otel-arrow#3369, open, designs the write-once instrumentation surface and the
choice between a granular schema that puts a signal in the metric name and an
agnostic schema that puts it in a data-point attribute. The per-data-point
attribute column is the shared prerequisite for the agnostic form and for any
future dimensioned set, which is why Move 2 builds that column path once.

## Host-pushed metrics that already fit the path

Some host telemetry already arrives as OTAP metrics and can share the encoder
with no bridge. The host metrics receiver builds `OtapArrowRecords::Metrics`
directly without intermediate OTLP objects, as
`crates/core-nodes/src/receivers/host_metrics_receiver/otap_builder.rs`
shows. The `user_events` receiver runs one session per assigned core through
`pipeline.core_id()`, and the Windows ETW receiver creates one session with a
per-core consumer channel and round-robins events onto it. Both live in
`crates/contrib-nodes/src/receivers/`. These are OTAP-native ingress that feed
the same aggregation and encoding as the engine's own metrics.

There is no StatsD receiver in the repository today, and this design does not
assume one. If a metric-line source is wanted later, it decodes into the same
per-core staging as any other source and needs no special path.

## Component decomposition

The two load-bearing seams are the registry merge, which is signal-agnostic and
already index-addressed, and the OTAP encoder, which is driven entirely by the
precomputed schema. Everything else plugs into those two.

| # | Component | What it owns | Status today |
| - | --------- | ------------ | ------------ |
| A | Instrument library | Per-core non-atomic instruments; exponential histogram later | `crates/telemetry/src/instrument.rs`; histogram is future |
| B | `#[metric_set]` codegen | Descriptor, handler, and, new, the per-set OTAP encoder | `crates/telemetry-macros`; encoder generation is new |
| C | Signal axis | `Signal`, granular or agnostic selection at registration | otel-arrow#3369; new |
| D | Snapshot-into-columns | Flush that writes typed columns by slot with no per-tick allocation | Replaces the dense `snapshot_values` |
| E | Registry merge | Index-addressed per-field accumulate or replace by temporality | `crates/telemetry/src/metrics.rs:431-470`; reused as is |
| F | OTAP encoder | Precomputed schema columns plus per-tick data-point columns to `OtapArrowRecords::Metrics` | `crates/pdata/src/encode/record/metrics.rs` |
| G | ITR metrics finalize | Read the registry on `CollectTelemetry`, encode, emit; retire the dispatcher timer | Extends `internal_telemetry_receiver`; replaces `metrics/dispatcher.rs` |
| H | Host OTAP ingress | Sources that already emit `OtapArrowRecords::Metrics` share the encoder | `host_metrics_receiver`, `user_events`, ETW exist |

Component D and Component F meet at one seam, so no stage builds a dense
intermediate that a later stage discards. Component G is the smallest useful
step and can ship first with the current dense snapshot still in place, which
already removes the SDK and the second clock.

## Phasing

Each phase is independently shippable and independently measurable.

1. **OTAP-direct emit.** Add the metrics finalize to the ITR on the
   `CollectTelemetry` tick, encode the registry to `OtapArrowRecords::Metrics`
   with the existing builders, and delete `MetricsDispatcher` and its
   OpenTelemetry emission. This removes cost B and cost C. Components F and G.
2. **Allocation-lean snapshot.** Replace the dense `snapshot_values` with
   codegen that writes typed columns by slot. This removes cost A. Component D.
3. **Schema and signal axis.** Land the schema-driven encoder generation and the
   granular or agnostic signal choice, selectable at registration. Components B
   and C, per otel-arrow#2623 and otel-arrow#3369.
4. **Exponential histogram.** Add the allocation-free exponential histogram as a
   detailed-level alternative to `Mmsc`. Completes Component A.
5. **First-class multivariate encoding, if measured to pay.** Collapse a set to
   one scope with several measurement columns sharing a timestamp, the most
   compact OTAP form. Gate on row-count and allocation measurements.
6. **Split the fan-in, only if measured to pay.** Keep most merging core-local
   before a final union at high core count. Gate on drain-latency measurements.

## Measurement and acceptance

Performance claims are gated on numbers, not on structure. The repository
already benches the reaggregation processor under
`crates/core-nodes/benches/temporal_reaggregation_processor`, and this work adds
a metrics encode-and-emit bench that reports allocations and nanoseconds per set
per tick for a representative set such as `FilterPdataMetrics`. Each phase must
show a non-regression on the recording hot path and the intended reduction on the
cold path before it merges. The acceptance bar for Phase 1 is zero OpenTelemetry
instrument construction on the metrics path; for Phase 2 it is zero heap
allocation per set per tick on the flush path.

## Open questions

- **Emit granularity.** Whether the ITR emits one metrics batch per tick for all
  sets or one batch per scope, and how that interacts with pipeline batching
  downstream.
- **Back-pressure accounting.** Confirming the metrics finalize adopts the same
  never-block, drop-and-count discipline the sampler uses, and how a dropped
  per-core flush is counted so the loss is visible.
- **Weaver timing.** When to move from the proc-macro source of truth to full
  Weaver-driven codegen, per the otel-arrow#3369 recommendation to validate with
  Weaver first and generate from it later.

## References

- otel-arrow#2623, Internal metrics telemetry pipeline and SDK design, the
  four-phase OTAP-direct proposal and the OTAP encoding forms.
- otel-arrow#3369, granular and agnostic signal-metric schemas, the write-once
  macro surface and the two codegen forms.
- [`integrated-sampler-engine-mechanism.md`](integrated-sampler-engine-mechanism.md),
  the per-worker to all-CPU aggregation with `ArcSwap` feedback and the
  `CollectTelemetry` finalize this design reuses for metrics.
- [`telemetry/metrics-guide.md`](telemetry/metrics-guide.md), the entity-centric
  metric-set model and the per-core cold-path intent.
- `crates/telemetry/src/instrument.rs`, `metrics.rs`, `reporter.rs`,
  `collector.rs`, `registry.rs`, `metrics/dispatcher.rs`, and
  `crates/telemetry-macros/src/lib.rs`, the current instrumentation and bridge.
- `crates/pdata/src/encode/record/metrics.rs` and `OtapArrowRecords::Metrics`
  in `crates/pdata/src/otap.rs`, the OTAP metrics encoding substrate.
- `crates/core-nodes/src/receivers/host_metrics_receiver` and
  `crates/contrib-nodes/src/receivers/user_events_receiver` and `etw_receiver`,
  the OTAP-native and per-core host ingress.
