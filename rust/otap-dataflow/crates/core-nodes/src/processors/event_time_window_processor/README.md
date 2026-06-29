# Event-time window processor (L2, proof of concept)

> ## :warning: THIS PROCESSOR DUPLICATES CODE ON PURPOSE
>
> **This is a throwaway proof of concept.** It deliberately re-implements
> identity extraction, metric-point aggregation, and OTLP output assembly that
> already exist (in more complete, OTAP-native form) in the
> [`temporal_reaggregation_processor`](../temporal_reaggregation_processor/) and
> its private `identity` / `builder` modules. None of that logic is reused here,
> because those modules are private and this PoC's purpose is to exercise the new
> **event-time** windowing core
> ([`otap_df_pdata::otap::windowing`](../../../../pdata/src/otap/windowing.rs))
> end to end inside a real processor node, not to ship production code.
>
> When the event-time path graduates, this crate should be **deleted** and its
> behavior folded into the temporal reaggregation processor (which keys per-stream
> state and flushes on a processing-time period today). Do not build on this PoC.

## What it does

Implements the metrics-appliance Layer 2 (event-time windowing) described in
[`docs/metrics-appliance-design.md`](../../../../../docs/metrics-appliance-design.md)
(decisions D10-D12). For every incoming OTAP metrics batch it:

1. Reads each NUMBER data point (sum and gauge only) through the metrics view.
2. Computes the point's series identity (resource attributes, scope name, metric
   name, unit, instrument kind, point attributes) and its event time.
3. Routes the point to its epoch-aligned tumbling window via
   `WindowedAggregators::admit`, folding it into a per-(series, window) aggregate:
   delta-sum for sums, last-value for gauges. Late points (event time below the
   stream watermark) are dropped and counted (D12).
4. After ingesting the batch, drains every window the watermark has closed
   (`drain_complete`) and emits the completed aggregates as a new metrics batch.

The watermark policy is `max(max_event - allowed_lateness, processing_time -
max_lag)` (D11); a window fires once the watermark reaches `window_end +
allowed_lateness`.

## Load reporting (shuffle owner)

The shuffle tags each batch with the partition it was dispatched to, so the
windower keeps an independent aggregator per partition tag and a partition's
**active series count is exactly that aggregator's stream count**. When a
`LoadReportSender` is wired in (via `set_load_report_sender`), each telemetry
collection reports per-partition load -- active series plus the interval ingest
count -- through the load feedback loop in
[`durable-dispatch-topic-design.md`](../../../../../docs/durable-dispatch-topic-design.md).
A `PlacementScheduler` merges those reports and rebalances partitions across
owners, so the windower acts as a real aggregating owner closing the loop. As a
PoC the active-series gauge counts every series seen (streams are not aged out
once their windows drain), and inbound reassignments are not yet handled.

## PoC limitations (not production behavior)

- **NUMBER points only.** Histogram, exponential histogram, and summary points
  are skipped and counted (`points_skipped`).
- **No D17 reset / gap / cumulative handling.** Sums are treated as deltas and
  added; no start-time tracking, reset detection, or cumulative-to-delta
  conversion.
- **OTLP output, not OTAP columnar.** Fired windows are re-encoded as OTLP proto
  bytes for simplicity. The real layer emits OTAP record batches.
- **Drain is batch-driven, not timer-driven.** Windows close only when a batch
  arrives (its processing-time `now` advances the idle floor). A production node
  would also drain on a timer so idle streams flush.
- **Array / kvlist attribute values collapse to empty** in the identity key.
- **Output carries a default context**, losing the input batch's partition tag.

## Configuration

| Field              | Default | Meaning                                  |
| ------------------ | ------- | ---------------------------------------- |
| `window_size`      | `60s`   | Tumbling window width over event time.   |
| `allowed_lateness` | `30s`   | Watermark trail and on-time fire offset. |
| `max_lag`          | `1h`    | Idle floor: watermark >= `now - max_lag`.|

Windows are epoch-aligned. `allowed_lateness` is both how far the watermark
trails the maximum observed event time and the offset past a window's end at
which it fires.

URN: `urn:otel:processor:event_time_window`. Telemetry metric set:
`processor.event_time_window`.
