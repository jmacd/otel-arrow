# Durable Metrics Appliance

This document sketches a durable, disconnection-tolerant metrics appliance built
on the otap-dataflow engine. It layers event-time aggregation, a queryable
timeseries store, and a Grafana-facing query interface on top of the
vertically-integrated ingest queue, so a single node can ingest, aggregate,
store, serve, and forward metrics -- continuing to operate (and answer local
queries) while disconnected from a central observability platform.

> Status: early design, not yet implemented. The ingest queue (L1) is designed
> separately in [`ingest-queue-design.md`](./ingest-queue-design.md) with
> ratified decisions D1-D9. This document covers the layers above it (L2-L5);
> its decisions (D10 and up) are proposed, not ratified. The deep section here
> is L2 (time, windowing, and watermarks); L3-L5 are outlined and will be
> developed next.

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

This is deliberately *not* a new time-series database project. It is a thin,
vertically-integrated assembly of primitives the engine already has: `quiver`
for durability, the `temporal_reaggregation` processor for metric aggregation,
DataFusion for query, and the existing exporters for forwarding.

## Layered architecture

```text
  OTAP/OTLP                                                       central
  in (gRPC)                                                       platform
     |                                                               ^
     v                                                               |
 +--------+   +-----------+   +-------------+   +---------+   +-------------+
 |  L1    |   |    L2     |   |     L3      |   |   L4    |   |     L5      |
 | Ingest |-->| Event-    |-->| Aggregated  |-->| Query + |   | Forwarding  |
 | queue  |   | time      |   | TS store    |   | serving |   | (store-and- |
 | (shuf- |   | windowing |   | (stage-2    |   | (Data-  |   |  forward)   |
 |  fle)  |   | + water-  |   |  quiver,    |   | Fusion +|   |             |
 |        |   | marks     |   |  Arrow IPC) |   | Grafana)|   |             |
 +--------+   +-----------+   +------+------+   +----+----+   +------+------+
 stage-1                            |                |               |
 quiver                             +----------------+---------------+
                                    |   (also) Parquet export -> object store
                                    v   for interop / archival (sibling path)
```

| Layer               | Role                                                    | Substrate / status                               |
| ------------------- | ------------------------------------------------------- | ------------------------------------------------ |
| L1 Ingest queue     | Admit, shuffle by name, durable raw OTAP buffer         | ingest-queue-design.md; stage-1 `quiver`         |
| L2 Aggregation      | Event-time windowing + watermarks over shuffled streams | `temporal_reaggregation` (processing-time today) |
| L3 Aggregated store | Complete windowed batches, keyed (name, bucket)         | stage-2 `quiver` (Arrow IPC); new usage          |
| L4 Query + serving  | Timeseries query + Grafana                              | DataFusion + serving layer (greenfield)          |
| L5 Forwarding       | Egress to a central observability platform              | OTAP / OTLP exporters (exist)                    |

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

The watermark is derived heuristically per bucket -- for example
`max(observed event_time) - allowed_lateness`, optionally floored by
`processing_time - max_lag` so an idle stream's windows still close. Its safety
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

The default is a single on-time firing per window, which is the simplest correct
behavior and matches the current processor's one-emit-per-period model.

### Late arrival, early arrival, and misaligned clocks

These are the real-world conditions the design must name explicitly:

- **Late arrival** (`event_time < watermark` when admitted): the point belongs
  to an already-closed window. Policy options (D12): drop-and-count
  (`late_points` telemetry), or **restate** -- re-open the window, re-aggregate,
  and emit a correction to L3. The bounded late horizon never exceeds L1's
  `max_lag`, so late state is bounded.
- **Early / future-dated arrival** (`event_time > now + skew`): rejected at L1
  by `max_skew`; L2 never sees a point beyond the skew bound, so a single fast
  clock cannot corrupt far-future windows.
- **Misaligned clocks across producers**: epoch-aligned windows mean each
  producer's points land in the correct absolute-time bucket regardless of
  delivery order; per-producer clock offset shows up as ordinary lateness or
  skew and is handled by the two mechanisms above, not as a special case.
- **Clock regressions / cumulative resets**: for cumulative series a decrease in
  the cumulative value (or a change in `start_time_unix_nano`) signals a reset;
  the aggregator must detect it and start a new accumulation rather than emit a
  negative delta. This is inherited from the metrics data model and the existing
  processor.

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

These modes interact with OTel **temporality**. Recall that temporality is
non-manifesting for identity (per ingest-queue D1), but it is central to
aggregation:

- **Delta** sums/histograms aggregate by summing within the window; accumulating
  mode naturally restates the window total.
- **Cumulative** sums/histograms aggregate by taking the latest cumulative value
  in the window relative to the series start, with reset detection.
- **Gauges** take last (or min/max/last per configuration); **summaries** are
  passed through or dropped as the existing processor does.

The aggregation function for a stream is chosen by its **instrument type**,
which L1's per-core type registry already records (and which D7's primary
descriptor disambiguates when a name carries conflicting types). L2 thus reads
the same type identity L1 established, rather than re-deriving it.

### Output to L3

A fired window produces a **complete aggregated batch** -- the rows for one
`(metric_name, window)` across its streams -- handed to L3 keyed by
`(metric_name, window_index)`. On-time firings produce the primary record; late
firings or restatements produce correction records that L3 resolves at read time
(see L3). Output is OTAP/Arrow throughout, so there is no representation change
between L2 and L3.

## L3: Aggregated timeseries store (stage-2 quiver)

A second `quiver` stage stores the windowed, aggregated metrics durably and in a
form built for query:

- **Arrow IPC, keyed by `(metric_name, time_bucket)`**: complete window outputs
  from L2 are written as Arrow IPC segments. The representation is identical to
  the data path, so there is no Arrow-to-anything conversion.
- **Late arrival as correction segments**: a restated window is appended as a new
  segment for the same `(name, bucket)` key rather than rewriting the prior one.
  Queries resolve the live value by the configured accumulation policy
  (last-writer-wins by default, or retraction-aware). This is the same
  idempotent-identity dedup the ingest queue already describes, applied to
  aggregated rows.
- **Queryable**: stage-2 segments are the substrate for L4. Querying persisted
  segments with DataFusion is already on the `quiver` roadmap.
- **Retention**: bounded by `quiver`'s disk budget and time/size retention; the
  aggregated form is far smaller than raw ingest, so a long local window is
  cheap.

This is why Arrow IPC in `quiver`, not Parquet, is the **live** store -- see
"Why not Parquet for the live store" below.

## L4: Query and serving

The appliance answers timeseries queries locally, so dashboards keep working
while disconnected:

- **Query engine**: DataFusion over the stage-2 segments, with the windowing key
  `(metric_name, time_bucket)` enabling efficient range scans for a name.
- **Grafana-facing interface**: served through a vendor-neutral protocol. The
  preferred option (D14) is the Prometheus remote-read / HTTP query API, because
  Grafana's Prometheus datasource is ubiquitous and the protocol is an open CNCF
  standard, consistent with the project's vendor-neutral posture. Arrow
  FlightSQL is a possible Arrow-native alternative, and a native datasource
  plugin is the most work.
- **Disconnected operation**: because L1-L3 have no central dependency, L4
  serves from local storage regardless of upstream connectivity.

## L5: Forwarding

The appliance forwards data to a central observability platform using the
engine's existing exporters (OTAP, OTLP), with store-and-forward semantics:

- **What is forwarded** (D15): the aggregated stream by default (smaller,
  already windowed), with raw passthrough available when the central platform
  wants full-fidelity data.
- **Store-and-forward**: forwarding reads from a durable stage (stage-1 for raw,
  stage-2 for aggregated), so an outage simply delays delivery; at-least-once
  and progress tracking resume on reconnect with no data loss.
- **No new dependency**: forwarding is an exporter on the balanced topic, the
  same mechanism the pipeline already uses.

## Why not Parquet for the live store

Parquet remains a first-class **export/interop** format (the existing
`parquet_exporter` already writes Arrow to an `object_store` backend, local or
`s3://`), but it is a poor fit for the *live, query-served, late-mutable* store,
for three reasons:

- **Mutation cost**: Parquet row groups are immutable and write-once; late
  arrival (restatement) would mean rewriting files, a large write amplification.
  Stage-2 quiver appends a correction segment instead.
- **Query shape**: Parquet is tuned for large analytical scans, not the
  recent-range, per-series lookups a Grafana dashboard issues.
- **Conversion**: keeping the live store in Arrow IPC avoids an Arrow-to-Parquet
  encode on the hot path.

So the roles split (and both are configurable, per the engine's modularity
posture):

- **Arrow IPC in stage-2 quiver (L3)**: the live, mutable, query-served store.
- **Parquet via the exporter (sibling of L2/L3)**: cold, widely-readable,
  archival and interop output -- hand-off to a lakehouse or the central
  platform, partitioned by date bucket and metric name.

## Decisions

These gate the detail above. Each is stated with a proposed recommendation and
is **open** (none ratified). L2 decisions (D10-D12) are developed first, per the
drafting order; L3-L5 decisions (D13-D16) are placeholders to be expanded.

| ID  | Decision                  | Recommendation (proposed)                            | Status |
| --- | ------------------------- | ---------------------------------------------------- | ------ |
| D10 | Window model              | tumbling, event-time, epoch-aligned, per-signal size | open   |
| D11 | Watermark policy          | heuristic; bounded by L1 max_lag                     | open   |
| D12 | Late/early + accumulation | allowed_lateness; restate via correction; modes      | open   |
| D13 | Stage-2 layout            | Arrow IPC, (name, bucket) key, correction segments   | open   |
| D14 | Query/serving protocol    | Prometheus remote-read (neutral); FlightSQL alt      | open   |
| D15 | Forwarding granularity    | forward aggregated; store-and-forward                | open   |
| D16 | Parquet role              | interop/archival export only                         | open   |

### D10. Window model

**Question:** what window shape and alignment does L2 use? **Recommendation:**
fixed-width tumbling windows over event time, epoch-aligned so buckets are
mergeable across appliances and with the central platform; window size
configurable per signal. Sliding/session windows are out of scope for v1.

### D11. Watermark policy

**Question:** how is the per-stream watermark derived, and how does it relate to
admission? **Recommendation:** a heuristic watermark
(`max observed event_time - allowed_lateness`, floored by `processing_time -
max_lag` for idle streams), with `allowed_lateness <= max_lag` and the future
bound supplied by `max_skew`. The ingest queue's admission window is the safety
envelope; the watermark is the fine-grained completeness estimate within it.

### D12. Late arrival, early arrival, and accumulation

**Question:** what happens to data outside the on-time window, and how do
re-firings relate? **Recommendation:** bound late handling by `max_lag`; default
to restatement via correction records at L3 (configurable to drop-and-count);
support discarding / accumulating / accumulating-and-retracting accumulation
modes, defaulting to accumulating (last-writer-wins at L3). Reject far-future
points at L1 via `max_skew`; detect cumulative resets per the metrics data
model.

### D13. Stage-2 store layout

**Question:** how are aggregated rows laid out and corrected? **Recommendation
(placeholder):** Arrow IPC segments keyed `(metric_name, window_index)`; late
restatements appended as correction segments and resolved at read time by the
accumulation policy; retention via the `quiver` disk budget.

### D14. Query and serving protocol

**Question:** how does Grafana talk to the appliance? **Recommendation
(placeholder):** a vendor-neutral Prometheus remote-read / HTTP query API over
DataFusion, with Arrow FlightSQL as an Arrow-native alternative.

### D15. Forwarding granularity

**Question:** forward raw, aggregated, or both? **Recommendation
(placeholder):** aggregated by default with optional raw passthrough; reads from
a durable stage for store-and-forward; at-least-once on reconnect.

### D16. Parquet role

**Question:** where does Parquet fit? **Recommendation (placeholder):** an
interop/archival export sibling to L2/L3 (cold, widely-readable), never the live
query store, which is Arrow IPC in stage-2 quiver.

## Status and next steps

**Where this stands:**

- L1 (the ingest queue) is designed in `ingest-queue-design.md` (D1-D9
  ratified). Nothing in this document is implemented yet.
- L2's seed exists -- the `temporal_reaggregation` processor -- but is
  processing-time, not event-time; L2 is the extension described here.
- `quiver` (L1/L3 substrate), the `parquet_exporter` (object-store Parquet), and
  the OTAP/OTLP exporters (L5) exist; the L4 serving layer is greenfield.

**To resume:**

1. Refine and ratify the L2 decisions (D10-D12): window model, watermark policy,
   late/accumulation semantics.
2. Confirm the OTAP columns and view accessors for event time
   (`time_unix_nano`, `start_time_unix_nano`) feeding the windowing, via
   `pdata-views`.
3. Expand L3-L5 (D13-D16) into full decisions.
4. Prototype: extend `temporal_reaggregation` with an event-time, watermark-
   driven windowing mode emitting complete windows to a stage-2 `quiver`.

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
- **Restatement / correction segment**: a re-emitted window result for late data,
  stored as a new stage-2 segment resolved at query time.
- **Stage-1 / stage-2 quiver**: the raw durable ingest buffer (L1) and the
  aggregated durable timeseries store (L3).
