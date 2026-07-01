# Integrated logs-and-traces reservoir sampling

This module implements the sampler specified in
[`docs/integrated-logs-traces-reservoir.md`](../../../../docs/integrated-logs-traces-reservoir.md).
It samples logs and spans together under fixed budgets and emits ordinary
OpenTelemetry logs that carry their own sampling weights as adjusted-count
attributes.

## Module map

- [`callsite`](callsite.rs) — the stable identities the sampler keys on. The
  span-start key is derived from the Tokio `tracing` callsite `Identifier`
  (`callsite_identity`), the canonical per-site identity, which is process-local
  and is never serialized. A name-based hash (`span_start_identity`) is retained
  for any future cross-process or persisted use. The log callsite identity is an
  FNV-1a hash of target, name, file, and line.
- [`bottom_floor`](bottom_floor.rs) — the statistical core. `BottomFloor` is the
  self-calibrating weighted bottom-k sampler with the Horvitz-Thompson inclusion
  correction and the rarest-seen floor. `observe` is a first-level arrival and
  `observe_weighted` is a second-level representative that stands for a carried
  local count, so a group total sums the values rather than counting entries.
  `UniformReservoir` is the degenerate single-weight case used per span.
  `BottomFloor::observe` returns the displaced (terminally rejected or evicted)
  payload, and `WindowOutput` exposes the dropped boundary record, so a caller
  can conserve records the bottom-k would otherwise discard.
- [`buffer`](buffer.rs) — `LocalSampleBuffer`, the per-worker integrated buffer.
  It reserves the criterion-one sample and routes every span-selected record that
  the reservoir does not keep, including evicted records and the boundary, to an
  overflow, so span-selected records are never lost. `flush` yields the
  Horvitz-Thompson weighted sample and the raw span stream.
- [`span_start`](span_start.rs) — `SpanStartSampler`, a `Sampler` that selects
  root spans from a wait-free `ArcSwap` threshold table, `build_span_start_table`,
  the target-rate feedback that rebuilds the table each window with equal coverage
  or surprisal-value weighting, and `SpanStartValues`, the surprisal accumulator
  shared by the processor and the aggregator.
- [`heavy_hitter`](heavy_hitter.rs) — `HeavyHitterTable`, criterion one's
  second-level feedback: the global inverse-frequency weights `g_c = 1 / N_c`, a
  scalar floor, and the global threshold `tau_g`, published wait-free through an
  `ArcSwap` with a local-only fail-safe.
- [`record`](record.rs) — the retained-record machinery shared by the processor
  and the aggregator: `FlushedRecord`, the reference-counted `Retained`, and the
  `Annotated` merge by allocation identity, so both stages produce identical
  output.
- [`processor`](processor.rs) — `IntegratedSampler`, the windowed processor that
  maintains all three estimators over the one emitted stream and flushes the
  retained set annotated with three adjusted counts.
- [`aggregator`](aggregator.rs) — `WindowAggregator`, the all-CPU second level.
  It recurses Bottom-Floor over the per-worker representatives weighted by their
  carried local counts, reassembles spans across workers by `span_id`, and at each
  window republishes the span-start and heavy-hitter tables while emitting one
  process-global annotated stream.
- [`encode`](encode.rs) — stamps a flushed `LogRecord` with the three
  adjusted-count attributes, including meaningful exact zeros.

The per-worker installation and flush live one level up in
[`local_buffer`](../local_buffer.rs): a `thread_local` `LocalSampleBuffer`, the
`route` the tracing layer's `emit` calls, and `flush_to`, which the pipeline
controller drives on its periodic window. `flush_to` hands the raw flush to the
process-global aggregator in [`aggregation`](../aggregation.rs) when one is
running and otherwise applies **local-only** annotations. See
[`docs/integrated-sampler-engine-mechanism.md`](../../../../../docs/integrated-sampler-engine-mechanism.md).

## The three populations

Each estimator is a separate population with its own adjusted-count attribute. A
record carries all three; a value of exactly zero means the record is present in
the output but is not a member of that population.

| Attribute | Population | Estimator |
| --- | --- | --- |
| `otel.logs.adjusted_count` | Global-independent logs | criterion one |
| `otel.traces.adjusted_count` | Spans | span-start sampler |
| `otel.span_logs.adjusted_count` | In-span logs | criterion two |

## Hot path and processor split

The tracer hot path runs the span-start sampler, a single wait-free `ArcSwap`
read, and emits each log exactly once, plus span START and END events. It never
touches reservoir state. The `IntegratedSampler` processor consumes that one
stream and decides which populations a record belongs to. The two are joined by
the span-start threshold table, which travels back from the processor to the
sampler through the `ArcSwap`.

```text
tracer (SpanStartSampler) --emit once--> IntegratedSampler --flush--> OTLP logs
        ^                                                   |
        +------------- ArcSwap span-start table ------------+
```

## Wiring

`IntegratedSampler::with_sampler` builds a processor and a matching
`SpanStartSampler` that share one threshold table. Install the sampler on the
tracing layer with `StructuredLoggingLayer::with_sampler`, and drive the
processor from the emitted telemetry stream, calling `flush` on the window
timer. The processor is single-threaded and is intended to run on a dedicated
internal-telemetry thread.

The processor is generic over the emitted payload, so it can be unit-tested with
synthetic records. The live adapter constructs an `IncomingRecord` per emitted
`LogRecord` by computing the log callsite identity from the record's callsite
metadata and the span phase from how the record was produced (START, END, or an
ordinary in-span or out-of-span log).

## Status and follow-ups

The sampling library and its statistical core are validated per-crate. The engine
mechanism that drives them is built across all three subsystems: Subsystem 1
(data-plane span context), Subsystem 2 (the per-worker `LocalSampleBuffer`, its
install, and the controller flush), and Subsystem 3 (the all-CPU
`WindowAggregator` and its two feedback tables). A worker flush prefers the
process-global aggregator and falls back to the **local-only** self-contained
sample when none is running. The aggregator is activated through its lifecycle
guard by the observability pipeline configuration. See
[`docs/integrated-sampler-engine-mechanism.md`](../../../../../docs/integrated-sampler-engine-mechanism.md)
for the mechanism and the resolved decisions.

Follow-ups: the worker heavy-hitter binding gate that reads `g_c` on the hot
path, wiring the `SpanStartSampler` onto the live tracer, smoothing the global
counts `N_c` across windows at the aggregator, the cross-process two-level log
feedback over the OTLP response path, the optional SDK-wide span-log second
stage, and a configuration surface.
