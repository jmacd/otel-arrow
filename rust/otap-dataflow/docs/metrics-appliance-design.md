# Durable Metrics Appliance

This document sketches a durable, disconnection-tolerant metrics appliance built
on the otap-dataflow engine. It layers event-time aggregation, a queryable
timeseries store, and a pull-based query interface on top of the
vertically-integrated ingest queue, so a single node can ingest, aggregate,
store, serve, and forward metrics -- continuing to operate (and answer local
queries) while disconnected from a central observability platform.

> Status: early design, not yet implemented. The ingest queue (L1) is designed
> separately in [`ingest-queue-design.md`](./ingest-queue-design.md) with
> ratified decisions D1-D9. This document covers the layers above it (L2-L5).
> Its decisions (D10-D17) are now **ratified** (see "Decisions"); what remains
> is implementation, laid out in "Implementation phases". Names are provisional.

## Motivation

A telemetry collector at the edge of a network -- an appliance, a gateway, an
on-premises node -- must keep working when the link to the central platform is
slow, congested, or down. The ingest queue already makes ingest durable and
loss-tolerant across such outages. This design takes the next step: aggregate
the buffered metrics into a compact, correct timeseries form, keep that form
locally queryable, and forward it upstream when connectivity allows.

The result is a self-contained metrics appliance:

- **Durable**: data survives restarts and outages (the ingest queue and a
  second durable stage).
- **Disconnection-tolerant**: aggregation, storage, and local queries continue
  with no central dependency; forwarding resumes on reconnect.
- **Vendor-neutral and Arrow-native**: built on Apache Arrow, DataFusion, and
  OTAP, with no required external broker, database, or vendor backend (the same
  posture as the ingest queue).

This is deliberately *not* a new time-series database project, and it does not
build or depend on a database. It is a thin, vertically-integrated assembly of
primitives the engine already has, plus two foundation-governed columnar file
formats: `quiver` for durable ingest, the `temporal_reaggregation` processor
for metric aggregation, the [Vortex](https://vortex.dev) format with DataFusion
for the queryable aggregated store, Parquet for archival interop, and the
existing exporters for forwarding. The engine *owns* the L1 ingest queue; it
*rents* the L3 store from neutral file formats (Vortex, Parquet).

## Layered architecture

```text
  OTAP/OTLP                                                       central
  in (gRPC)                                                       platform
     |                                                               ^
     v                                                               |
 +--------+   +-----------+   +-------------+   +---------+   +-------------+
 |  L1    |   |    L2     |   |     L3      |   |   L4    |   |     L5      |
 | Ingest |-->| Event-    |-->| Aggregated  |-->| Pull    |   | Forwarding  |
 | queue  |   | time      |   | TS store    |   | query   |   | (push OTAP, |
 | (shuf- |   | windowing |   | (stage-2    |   | (OPL -> |   |  store-and- |
 |  fle)  |   | + water-  |   |  store:     |   |  OTAP)  |   |  forward)   |
 |        |   | marks     |   |  Vortex)    |   |         |   |             |
 +--------+   +-----------+   +------+------+   +----+----+   +------+------+
 stage-1                            |                |               |
 quiver                             +----------------+---------------+
                                    |   (also) Parquet export -> object store
                                    v   for interop / archival (sibling path)
```

| Layer               | Role                                                        | Substrate / status                               |
| ------------------- | ----------------------------------------------------------- | ------------------------------------------------ |
| L1 Ingest queue     | Admit, shuffle by name, durable raw OTAP buffer             | ingest-queue-design.md; stage-1 `quiver`         |
| L2 Aggregation      | Event-time windowing + watermarks over shuffled streams     | `temporal_reaggregation` (processing-time today) |
| L3 Aggregated store | Complete windowed batches, keyed (name, resolution, window) | stage-2 store seam; Vortex (Parquet archival)    |
| L4 Query (pull)     | Pull-based retrieval; metrics + sample queries (spans/logs) | DataFusion `TableProvider`; OTAP out; greenfield |
| L5 Forwarding       | Push egress to a central observability platform             | OTAP exporters (exist)                           |

The crucial enabler is L1's shuffle by metric name: once all points for a name
are co-located on one core (the ingest queue's data bucket key), event-time
windowing for that name is a purely local operation -- no cross-core exchange,
no distributed watermark coordination. L2 windows what L1 has already gathered.

## L2: Time, windowing, and watermarks

This is the heart of the appliance and the part that most needs depth. The
reference model is the Dataflow/Beam streaming model (event time, watermarks,
windowing, triggers, accumulation) adapted to the OpenTelemetry metrics data
model and to OTAP's columnar representation.

### Three clocks: event, arrival, and processing time

Every metric point carries an **event time** in its OTAP columns: data points
expose `time_unix_nano` (the point's timestamp) and, for cumulative and delta
series, `start_time_unix_nano` (the start of the accumulation interval). These
are origin timestamps, stamped by the producer.

Two other clocks exist on the path:

- **Arrival time**: when the ingest queue admitted the point (L1).
- **Processing time**: when L2 actually aggregates it.

Aggregating by arrival or processing time -- which a naive interval aggregator
does -- skews the series by delivery latency: a burst delivered late lands in
the wrong bucket, a backfill collapses into one bucket, and two producers with
different network paths disagree about which interval a measurement belongs to.
Correct metric aggregation must bucket by **event time**, the only clock that
reflects when the measurement was actually taken.

The existing `temporal_reaggregation` processor today aggregates on a
processing-time `period` (default 60s), modeled on the Go interval processor.
L2 extends it from a processing-time interval to **event-time windows with
watermark-driven completion and late-arrival handling**. The identity model
(resource, scope, metric, and stream ids), the supported instrument types
(cumulative sums, histograms, exponential histograms, gauges, summaries), the
cardinality limit, and the ack/nack accounting carry over; what changes is the
clock that defines a window and the trigger that closes it.

### Tumbling event-time windows

Each metric stream is partitioned into **non-overlapping, fixed-width windows
over event time** (tumbling windows), aligned to the epoch so that all producers
and all appliances agree on window boundaries:

```text
  window_index = floor(event_time / window_size)
  window       = [window_index * window_size, (window_index + 1) * window_size)
```

Epoch alignment (rather than alignment to first-seen time) is what lets two
appliances, or an appliance and the central platform, produce mergeable buckets
for the same series. Window size is configurable per signal (for example 10s or
60s for metrics); sliding and session windows are out of scope for v1.

Because L1 has already shuffled by name, all points of a given `(metric_name,
tenant)` are local, so the windowing state for a name -- one open aggregator per
`(stream identity, window_index)` -- lives on a single core with no
coordination.

### Watermarks

A **watermark** is the appliance's estimate of event-time completeness for a
stream: a claim that no (or few) points with `event_time < watermark` will still
arrive. When the watermark passes a window's end, that window is considered
complete and can be emitted.

The watermark is derived heuristically **per stream** -- for example
`max(observed event_time) - allowed_lateness` -- with a **per-bucket idle
floor** of `processing_time - max_lag` so an idle stream's windows still close
(D11). Per-stream rather than per-bucket is deliberate: a single per-bucket
watermark advanced by the fastest stream would prematurely close a slower or
idle stream's window; per-stream state is bounded by the processor's existing
cardinality limit. Its safety
is bounded by the ingest queue's admission control: L1 already rejects points
older than `max_lag` and further-future than `max_skew` (the time-window
filter). Those bounds are the **floor and ceiling** of what L2 must consider:

- nothing older than `now - max_lag` can be admitted, so `allowed_lateness` need
  never exceed `max_lag`;
- nothing further ahead than `now + max_skew` can be admitted, so a misaligned
  (fast) clock cannot push the watermark arbitrarily into the future.

This is the key reuse: the ingest queue's admission window already does the
crude, protective time bounding; L2's watermark does the fine-grained,
per-stream completeness estimation within those bounds.

### Triggers: when a window emits

A window fires (emits an aggregated result downstream to L3) when the watermark
reaches `window_end + allowed_lateness` -- the on-time trigger. Two optional
trigger refinements are available:

- **Early firings**: emit a partial result on a processing-time timer before the
  watermark closes the window, for lower-latency visibility (the result is later
  superseded; see accumulation).
- **Late firings**: emit an updated result when a straggler arrives after the
  window already fired but still within a bounded late horizon.

The default -- and all of v1 (D12) -- is a single on-time firing per window: the
simplest correct behavior, matching the current processor's one-emit-per-period
model. Early and late firings (and the correction/restatement path they imply)
are deferred to appliance phase 2.

### Late arrival, early arrival, and misaligned clocks

These are the real-world conditions the design must name explicitly:

- **Late arrival** (`event_time < watermark` when admitted): the point belongs
  to an already-closed window. **v1 policy: drop-and-count** (`late_points`
  telemetry) -- the simplest correct behavior. Phase 2 adds **restate**: re-open
  the window, re-aggregate, and emit a correction to L3 (D12). The bounded late
  horizon never exceeds L1's `max_lag`, so late state is bounded either way.
- **Early / future-dated arrival** (`event_time > now + skew`): rejected at L1
  by `max_skew`; L2 never sees a point beyond the skew bound, so a single fast
  clock cannot corrupt far-future windows.
- **Misaligned clocks across producers**: epoch-aligned windows mean each
  producer's points land in the correct absolute-time bucket regardless of
  delivery order; per-producer clock offset shows up as ordinary lateness or
  skew and is handled by the two mechanisms above, not as a special case.
- **Resets, gaps, and overlap (per the OTel data model, not optional even in
  v1)**: the aggregator follows the metrics
  [data model rules](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#resets-and-gaps)
  exactly, driven by the `start_time_unix_nano`/`time_unix_nano` relationship
  (D17):
  - `start < time` -- a **"true" reset** at a known start (zero implicit);
  - `start == time` -- a **reset at unknown start** (zero-duration point; "data
    may have been lost"; zero rate contribution);
  - a range covered by no point's `[start, time]` is a **gap** -- implicitly
    *undefined*, never zero-filled (so an outage reads as a gap, the property a
    disconnected appliance must preserve);
  - two points overlapping in time are a **single-writer violation** -- L2 drops
    to de-overlap and emits telemetry, and MAY interpolate Sums at the
    change-over (per the data model's Overlap rules).

  Reset detection extends the existing processor's logic from processing-time to
  event-time windows.

### Accumulation and the OTel temporality interplay

When a window fires more than once (early or late firings, or a restatement),
the relationship between successive emissions follows one of the Beam-style
**accumulation modes** (D12):

- **Discarding**: each firing emits only the delta since the previous firing.
- **Accumulating**: each firing emits the full window result, superseding the
  prior one (last-writer-wins at L3).
- **Accumulating and retracting**: each firing emits a retraction of the prior
  result plus the new one, so a downstream that has already summed can correct
  exactly.

These modes interact with OTel **temporality** (D17). Temporality is
non-manifesting for identity (per ingest-queue D1), so L2 **accepts mixed
temporality** within a name, but temporality is central to aggregation:

- **Delta** sums/histograms aggregate by summing within the window.
- **Cumulative** sums/histograms aggregate by taking the latest cumulative value
  in the window relative to the series start, with reset detection.
- **Gauges** take last (or min/max/last per configuration); **summaries** are
  passed through or dropped as the existing processor does.

**Stored form: cumulative; output temporality: configurable (D17).** The
aggregated stream is stored cumulatively -- the OTel data model targets a
timeseries form that "does not support delta counters," and the spec names this
exact operation *inserting true reset points, a special case of reaggregation
for cumulative series.* Storing cumulative preserves the absolute value and is
losslessly projectable to delta (by differencing) at query (L4) or forward (L5)
time, so the **output temporality is a per-consumer choice**, not fixed here.
Delta inputs are converted to cumulative by the data model's stateful
delta-to-cumulative algorithm.

**Why this is correct here -- the single-writer synergy.** That conversion
*requires a single-writer destination* (the spec is explicit). L1's shuffle by
metric name provides exactly that: every point of a series is co-located on one
core, so the stateful conversion and reset/gap bookkeeping are local and
race-free. The classic hard part of delta-to-cumulative is handed to us by the
ingest queue's shard key.

The aggregation function for a stream is chosen by its **instrument type**,
which L1's per-core type registry already records (and which D7's primary
descriptor disambiguates when a name carries conflicting types). L2 thus reads
the same type identity L1 established, rather than re-deriving it.

**Metric attribute reduction (D17, designed-for, deferred).** A later capability
reduces cardinality by dropping attributes and re-aggregating across the
now-equivalent series. It is done at stage-2 on an additive (delta) basis --
where contributions are summable across the merged series within a window --
then projected to the chosen output temporality, reset-aware so series with
differing start times merge correctly. It is therefore *temporality-agnostic*
and belongs after window close and reset resolution.

### Output to L3

A fired window produces a **complete aggregated batch** -- the rows for one
`(metric_name, window)` across its streams -- handed to L3 keyed by
`(metric_name, resolution, window_index)` (the `resolution` component is
constant in v1 but reserved so downsampling adds levels without re-keying). In
v1 each window produces one primary record; phase 2's late firings or
restatements produce correction records that L3 resolves at read time (see L3).
Output is Arrow throughout, so there is no representation change between L2 and
L3.

## L3: Aggregated timeseries store (stage-2 store)

L3 is a durable, queryable store of the windowed, aggregated metrics. It is
**not** a bespoke store and **not** a database: aggregated windows are written
as immutable columnar files behind a small **store seam** (a writer plus a
DataFusion `TableProvider`), so the backend is a configuration choice the way
the ingest queue's storage backend is (ingest-queue D4).

- **Backend: Vortex (default), behind the store seam (D13/D16).** Closed windows
  are written as [Vortex](https://vortex.dev) files. Vortex is a
  Linux-Foundation-governed, Apache-2.0 columnar format with a file format
  stable since 0.36.0; it is Arrow-native (zero-copy to/from `RecordBatch`),
  Rust-native, object-store optimized, and offers ~100-200x faster random access
  than Parquet -- the property a per-series Grafana lookup needs. It is a
  *format plus a library*, not a server, so it adds no service dependency and
  preserves the Arrow-native, vendor-neutral posture. Parquet remains the
  archival/interop sibling (see "The live store and the archival sibling").
- **Key `(metric_name, resolution, window_index)`.** Range scans for a name over
  a time range are partition/min-max prunable. `resolution` is constant in v1;
  reserving it now is what lets rollup/downsampling add coarser levels later
  without a migration (D10).
- **Corrections (phase 2).** A restated window is written as a new file for the
  same key rather than rewriting the prior one (columnar files are immutable);
  the read path resolves the live value by the configured policy
  (last-writer-wins by default, or retraction-aware). v1 emits a single primary
  per window, so this path is dormant until late-restatement lands.
- **Retention: fixed window, drop-oldest (v1).** A fixed time/size budget; when
  it is reached the oldest files are dropped (D15/QoS). The aggregated form is
  far smaller than raw ingest, so the local horizon is long. Rollup later sheds
  fine resolution first and keeps coarse longer.
- **Queryable.** The stage-2 files are the substrate for L4 via a DataFusion
  `TableProvider` (see L4).

## L4: Query and serving (pull-based retrieval)

The appliance answers **pull-based queries** locally, so consumers can retrieve
data while the node is disconnected. OpenTelemetry specifies push protocols
(OTLP, OTAP) but **no pull-based retrieval protocol**; this layer fills that gap
using the engine's own query engine rather than adopting a vendor query
protocol.

- **Query interface -- query-engine language in, OTAP out (D14)**: queries are
  expressed in the query engine's language (OPL, the OpenTelemetry Processing
  Language, or OTTL), compiled to the engine's intermediate representation, and
  executed as DataFusion / Arrow pipeline stages over the stage-2 store through
  a **`TableProvider`** that scans the columnar files and prunes by partition
  and by `(metric_name, resolution, window_index)` min/max stats. The result is
  returned as OTAP, so the query path needs no representation change and no new
  wire protocol.
- **Serving contract -- an HTTP query endpoint**: the concrete, replaceable
  contract is a local HTTP endpoint that takes an OPL/OTTL query (with a time
  range, for range or instant queries) and returns Arrow/OTAP. Everything else
  (Grafana, scripts) is a client of this endpoint, so "OTAP out" is a real wire
  contract, not a library detail.
- **All three signals**: pull-based retrieval is the interesting case for
  **metrics** (range/instant queries over the aggregated timeseries). **Spans
  and logs** are served the same way as **sample queries over their
  temporally-aggregated data** -- the same query interface, returning OTAP.
- **Clients**: any OTAP-speaking consumer can pull directly. A dashboard such as
  Grafana connects through a thin datasource adapter that issues OPL and renders
  the OTAP/Arrow result; the adapter is a client of this interface, not a
  separate server protocol.
- **Disconnected operation**: because L1-L3 have no central dependency, L4
  serves from local storage regardless of upstream connectivity.

## L5: Forwarding

The appliance forwards data to a central observability platform by **pushing
OTAP** through the engine's existing exporters, with store-and-forward
semantics. (Pull-based retrieval is L4's concern; forwarding is push, and OTAP
is sufficient -- no Prometheus remote-write or other vendor egress protocol is
needed.)

- **What is forwarded** (D15): the aggregated stream by default (smaller,
  already windowed), with raw passthrough available when the central platform
  wants full-fidelity data.
- **Store-and-forward**: forwarding is a durable-stage subscriber (stage-1 for
  raw, stage-2 for aggregated), using the stage's at-least-once subscriber
  progress, so an outage simply delays delivery and progress resumes on
  reconnect. Under a *sustained* outage the stage hits its retention budget and
  sheds oldest-first (D15/QoS, the same drop-oldest policy as local retention),
  so a disconnected appliance degrades gracefully instead of blocking ingest.
- **No new dependency**: forwarding is an OTAP exporter on the balanced topic,
  the same mechanism the pipeline already uses.

## The live store and the archival sibling

The live store (Vortex) and the archival format (Parquet) play different roles,
and both sit behind configurable seams (per the engine's modularity posture):

- **Vortex in the stage-2 store (L3): the live, query-served store.** It is
  Arrow-native (zero-copy), Rust-native, and gives ~100-200x faster random
  access than Parquet -- exactly the recent-range, per-series lookup a Grafana
  dashboard issues. Late corrections are handled by writing a new file for the
  same key (immutable files, resolved at read), not by rewriting, so there is no
  row-group write amplification. Because windows close on the watermark, the
  encode happens off the hot path.
- **Parquet via the exporter (sibling of L2/L3): cold, widely-readable archival
  and interop.** Parquet's universal readability is what the *hand-off* to a
  lakehouse or the central platform needs, and the existing `parquet_exporter`
  already writes Arrow to an `object_store` backend (local or `s3://`),
  partitioned by date bucket and metric name.

The earlier objection to Parquet *as the live store* (mutation cost, per-series
query shape) is what makes Vortex the live store; the objection does not apply to
Parquet's archival role, where files are written once and read by broad
analytical scans. Both are foundation-governed columnar formats plus a library --
no database, no server, no vendor backend.

## Decisions

These gate the detail above. **All are ratified** (D10-D17). v1 scope is called
out where a decision defers part of itself to appliance phase 2.

| ID  | Decision                    | Decided                                               | Status  |
| --- | --------------------------- | ----------------------------------------------------- | ------- |
| D10 | Window model                | tumbling, event-time, epoch-aligned; single-res v1    | decided |
| D11 | Watermark policy            | per-stream heuristic + idle floor; bounded by max_lag | decided |
| D12 | Late/early + accumulation   | v1 one on-time firing, late=drop-and-count            | decided |
| D13 | Stage-2 layout              | store seam; Vortex default; (name, res, window) key   | decided |
| D14 | Query/serving protocol      | DataFusion `TableProvider`; OPL/OTTL in, OTAP out     | decided |
| D15 | Forwarding granularity      | push OTAP; aggregated default; drop-oldest on outage  | decided |
| D16 | Store formats               | Vortex live store; Parquet archival sibling           | decided |
| D17 | Temporality + reaggregation | mixed in; store cumulative; output configurable       | decided |

### D10. Window model

**Decided:** fixed-width tumbling windows over event time, epoch-aligned so
buckets are mergeable across appliances and with the central platform; window
size configurable per signal. v1 ships a **single resolution**; the stage-2 key
reserves a `resolution` component (D13) so rollup/downsampling can add coarser
levels later without a migration. Sliding/session windows are out of scope.

### D11. Watermark policy

**Decided:** a **per-stream** heuristic watermark
(`max observed event_time - allowed_lateness`), with a per-bucket idle floor of
`processing_time - max_lag` so idle streams still close, `allowed_lateness <=
max_lag`, and the future bound supplied by `max_skew`. Per-stream (not
per-bucket) avoids the fastest stream prematurely closing a slower stream's
window; state is bounded by the processor's cardinality limit. The ingest
queue's admission window is the safety envelope; the watermark is the
fine-grained completeness estimate within it.

### D12. Late arrival, early arrival, and accumulation

**Decided:** **v1 = a single on-time firing per window; late points are
drop-and-count** (`late_points` telemetry); far-future points are rejected at L1
by `max_skew`. Reset/gap/overlap detection per the OTel data model is in v1 (it
is correctness, not a refinement; see D17). **Phase 2** adds restatement via L3
correction records, early/late firings, and the discarding / accumulating /
accumulating-and-retracting accumulation modes (default accumulating,
last-writer-wins at L3). Bounded by `max_lag` throughout.

### D13. Stage-2 store layout

**Decided:** aggregated windows are written as immutable columnar files behind a
**store seam** (writer + DataFusion `TableProvider`), keyed `(metric_name,
resolution, window_index)`. The default backend is **Vortex** (D16). Late
restatements are written as new files for the same key and resolved at read time
by the accumulation policy (phase 2). Retention is a fixed time/size budget with
drop-oldest (D15). This revises the earlier Arrow-IPC-in-`quiver` idea: `quiver`
stays the L1 *queue*; L3 is a *queryable store* of columnar files, a different
access pattern (random-access by key, not FIFO subscribe/ack).

### D14. Query and serving protocol

**Decided:** a **DataFusion `TableProvider` over the stage-2 store** answers
queries expressed in the query engine's language (OPL / OTTL) and returns OTAP --
no vendor query protocol. The concrete, replaceable contract is a **local HTTP
query endpoint** (OPL/OTTL + time range in, Arrow/OTAP out) supporting range and
instant queries; Grafana and other clients are adapters over that endpoint, not
separate server protocols. Pull-based retrieval is the interesting case for
metrics; spans and logs are served as sample queries over their
temporally-aggregated data. v1 targets metrics; spans/logs aggregation is a
later decision that reuses the same window+key model.

### D15. Forwarding granularity

**Decided:** push **OTAP** (no Prometheus remote-write or other vendor egress);
forward the aggregated stream by default with optional raw passthrough; forward
as a durable-stage subscriber (stage-1 raw, stage-2 aggregated) using its
at-least-once progress, so delivery resumes on reconnect. Under a sustained
outage the stage sheds **oldest-first** at its retention budget rather than
blocking ingest (the same drop-oldest QoS as local retention; lossless mode
remains available per ingest-queue D6).

### D16. Store formats: Vortex live, Parquet archival

**Decided:** the live, query-served stage-2 store is **Vortex** -- a
Linux-Foundation-governed, Apache-2.0, Arrow-native columnar format, file-format
stable since 0.36.0, with ~100-200x faster random access than Parquet (the
Grafana per-series lookup pattern). **Parquet** is retained as the cold,
widely-readable archival/interop sibling (the existing `parquet_exporter` to an
`object_store` backend). Both are *format + library*, not a server or database:
this keeps the minimum-dependency, vendor-neutral, Arrow-native posture while
letting the engine *rent* rather than *own* the store. ClickHouse and other
external databases were considered and rejected as required dependencies for
this reason.

### D17. Temporality and reaggregation

**Decided:** L2/L3 **accept mixed input temporality** (delta and cumulative;
temporality is non-manifesting per ingest-queue D1) and **store cumulative** as
the canonical form -- the OTel data model targets a timeseries that "does not
support delta counters," and names storing-cumulative-with-reset-insertion *a
special case of reaggregation for cumulative series.* **Output temporality is a
configurable per-consumer choice** (cumulative as-stored, or delta by
differencing) at L4/L5. The aggregator follows the data model's
[Resets and Gaps](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#resets-and-gaps)
and Overlap rules exactly (`start < time` true reset; `start == time`
unknown-start reset; uncovered range = gap, never zero-filled; overlap =
single-writer violation, drop + observe). Delta-to-cumulative conversion is
stateful and *requires a single-writer destination*, which L1's shuffle-by-name
provides for free. **Metric attribute reduction** (dropping attributes and
re-aggregating across the now-equivalent series) is a designed-for, deferred
capability done at stage-2 on an additive basis then projected to the chosen
output temporality, reset-aware -- hence temporality-agnostic.

## Implementation phases

The appliance builds on the ingest queue (L1, whose own phases 0-4 are in
[`ingest-queue-design.md`](./ingest-queue-design.md)). Appliance phases:

- **A0 -- event-time windowing (L2 core).** Extend `temporal_reaggregation` to
  event-time tumbling windows (epoch-aligned), per-stream watermark + idle
  floor, single on-time firing, mixed-temporality intake, cumulative store form
  with OTel reset/gap/overlap handling (D10-D12, D17). Emits complete windowed
  batches. Depends on L1 admission/identity (ingest Phase 0) and benefits from
  the shuffle-by-name (ingest Phase 1).
- **A1 -- stage-2 store (L3 core).** The store seam (writer + `TableProvider`)
  with the Vortex backend, keyed `(metric_name, resolution, window_index)`;
  fixed-window drop-oldest retention (D13, D15, D16). Can be prototyped in
  parallel against a fixed windowed-batch schema, then integrated with A0.
- **A2 -- query/serving (L4 core).** DataFusion `TableProvider` over the store
  plus the HTTP query endpoint (OPL/OTTL in, Arrow/OTAP out), range and instant
  queries (D14). Depends on A1.
- **A3 -- forwarding (L5).** Store-and-forward OTAP exporter as a durable-stage
  subscriber; aggregated default, raw passthrough; drop-oldest under outage
  (D15). Parquet archival sibling reuses `parquet_exporter` (D16). Depends on L1
  (raw) and/or A1 (aggregated).
- **A4 -- appliance phase 2.** Late restatement + L3 correction files, early/late
  firings and accumulation modes (D12), rollup/downsampling resolutions (D10),
  metric attribute reduction (D17), Grafana datasource adapter, spans/logs sample
  queries (D14).

## Status and next steps

**Where this stands:**

- L1 (the ingest queue) is designed in `ingest-queue-design.md` (D1-D9
  ratified). Nothing in this document is implemented yet; decisions D10-D17 are
  ratified.
- L2's seed exists -- the `temporal_reaggregation` processor -- but is
  processing-time, not event-time; L2 (phase A0) is the extension described here.
- `quiver` (L1 substrate), the `parquet_exporter` (object-store Parquet, L5
  archival), the OTAP exporters (L5), and the query engine with its OPL/OTTL
  languages exist. The Vortex-backed stage-2 store (L3) and the `TableProvider`
  query path over it (L4) are the new assembly.

**To resume, start at phase A0:**

1. Confirm the OTAP columns and view accessors for event time
   (`time_unix_nano`, `start_time_unix_nano`) and instrument type feeding the
   windowing, via `pdata-views` (`OtapMetricsView`).
2. Extend `temporal_reaggregation` with the event-time, watermark-driven
   windowing mode emitting complete cumulative windows.
3. In parallel, prototype the A1 store seam + Vortex writer against the windowed
   batch schema.
4. File tracking issues per phase.

**Related docs:** [`ingest-queue-design.md`](./ingest-queue-design.md),
[`design-principles.md`](./design-principles.md), the `quiver` crate README and
ARCHITECTURE, and the `temporal_reaggregation` processor README.

## Glossary

- **Event time**: the origin timestamp of a measurement (`time_unix_nano`, with
  `start_time_unix_nano` for cumulative/delta), stamped by the producer.
- **Arrival time / processing time**: when the ingest queue admitted a point,
  and when L2 aggregates it; both differ from event time by delivery latency.
- **Tumbling window**: a non-overlapping, fixed-width, epoch-aligned window over
  event time; the unit of aggregation.
- **Watermark**: an estimate of event-time completeness for a stream; when it
  passes a window's end, the window may fire.
- **Trigger**: the rule that decides when a window emits (on-time, early, late).
- **Allowed lateness**: how far past a window's end late data is still
  incorporated; bounded by the ingest queue's `max_lag`.
- **Accumulation mode**: how successive firings of a window relate (discarding,
  accumulating, accumulating-and-retracting).
- **Restatement / correction record**: a re-emitted window result for late data
  (appliance phase 2), stored as a new stage-2 file resolved at query time.
- **Stage-1 quiver / stage-2 store**: the raw durable ingest buffer (L1, a
  `quiver` queue) and the aggregated durable timeseries store (L3, Vortex files
  behind a store seam).
- **Vortex**: a Linux-Foundation-governed, Apache-2.0, Arrow-native columnar
  file format (stable file format since 0.36.0) used as the L3 live store; fast
  random access and zero-copy to Arrow.
- **Store seam**: the L3 abstraction (writer + DataFusion `TableProvider`) that
  makes the stage-2 backend a configuration choice (Vortex default).
- **Resolution**: the window granularity of a stored aggregate; constant in v1,
  the key component reserved for later rollup/downsampling.
- **Reset / gap / overlap**: OTel data-model conditions on a stream -- a counter
  restart (`start < time` true reset, or `start == time` unknown-start reset),
  an uncovered (undefined) time range, and a single-writer violation,
  respectively.
- **Single-writer principle**: each series has one logical writer; L1's
  shuffle-by-name enforces it per series, enabling local delta-to-cumulative
  conversion.
- **Attribute reduction**: cardinality reduction by dropping attributes and
  re-aggregating across the now-equivalent series at stage-2 (designed-for,
  deferred), done on an additive basis and projected to the chosen temporality.
- **Pull-based retrieval**: querying stored data on demand (as opposed to
  push/export); OpenTelemetry specifies no pull protocol, so L4 uses the query
  engine for this.
- **OPL / OTTL**: the query engine's query languages (OpenTelemetry Processing
  Language; OpenTelemetry Transformation Language); a query compiles to the
  engine's intermediate representation and executes over OTAP, returning OTAP.
