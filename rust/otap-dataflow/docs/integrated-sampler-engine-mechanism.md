# Integrated Sampler: Engine Mechanism

## Status

Implemented. Subsystems 1 and 2 are built and validated in the
`otap-df-telemetry` and `otap-df-engine` crates; Subsystem 3, the all-CPU
aggregator and its two feedback tables, is built and validated in
`otap-df-telemetry` as a library capability that the observability pipeline
configuration activates. This document is mechanism-focused: it specifies how the
OTAP-Dataflow engine consumes itself as the telemetry SDK for logs and spans,
wiring the integrated sampler into one pipeline runtime. It deliberately does not
re-derive the sampling probabilities or define a configuration surface.

### Implementation status

- **Subsystem 1 (span context in the data plane): done.** `OtapPdata`'s
  `Context` carries an optional `SpanContext`. The tracing layer reads a `tokio`
  task-local ambient span context (`scope_span_context` /
  `ambient_span_context` in `crates/telemetry/src/tracing_init.rs`) that the
  engine sets around a node's per-message processing, so node logs and child
  spans link to the data's trace across `await` points. Processors get the swap
  automatically in the run loop (`crates/engine/src/processor.rs`); receivers set
  the context on outbound data with `OtapPdata::with_span_context`
  (`crates/otap/src/pdata.rs`). The automatic exporter swap was intentionally
  skipped; exporters may call `scope_span_context` directly.
- **Subsystem 2 (per-worker integrated buffer): done, in local-only mode.**
  `LocalSampleBuffer`
  (`crates/telemetry/src/self_tracing/sampling/buffer.rs`) reserves the
  criterion-one sample and conserves every span-selected record through an
  overflow, including the bottom-k boundary record.
  `crates/telemetry/src/self_tracing/local_buffer.rs` installs one buffer per
  worker thread and routes the layer's `emit` through it; the pipeline
  controller flushes it on an always-armed periodic `select!` branch
  (`crates/engine/src/pipeline_ctrl.rs`, `RuntimeCtrlMsgManager::run`), and
  `run_forever` installs it for the worker's lifetime. In this phase the flush
  annotates each record with **local-only** adjusted counts and forwards to the
  reporter, so each worker's sample stands alone as an unbiased local sample.
- **Subsystem 3 (all-CPU aggregation + feedback): built as a library
  capability.** `WindowAggregator`
  (`crates/telemetry/src/self_tracing/sampling/aggregator.rs`) combines the
  per-worker flushes into one process-global sample and republishes the
  span-start and heavy-hitter tables; `HeavyHitterTable`
  (`sampling/heavy_hitter.rs`) is criterion one's second-level feedback.
  `crates/telemetry/src/self_tracing/aggregation.rs` carries each worker's raw
  `BufferFlush` over a channel to a single finalizer thread that owns the
  aggregator, and `flush_to` (`self_tracing/local_buffer.rs`) prefers that
  aggregator and falls back to the local-only path when none is running. The
  finalizer is started and stopped through its lifecycle guard,
  `start_sample_aggregator`, by the observability pipeline configuration rather
  than unconditionally, so the default remains the Subsystem 2 local-only
  behavior until it is configured. The previously-open decisions are resolved
  below: the in-memory worker-to-aggregator transfer, the standalone
  process-global finalizer, the worker-to-aggregator window alignment, and the
  back-pressure
  policy.

## Purpose and scope

The algorithm and its statistics are already specified and implemented as a
library:

- [`integrated-logs-traces-reservoir.md`](integrated-logs-traces-reservoir.md)
  is the algorithm.
- [`span-start-value-sampling.md`](span-start-value-sampling.md) is the
  span-start value math.
- The library lives in the `otap-df-telemetry` crate under
  [`self_tracing::sampling`](../crates/telemetry/src/self_tracing/sampling/README.md):
  `BottomFloor`, `UniformReservoir`, `SpanStartSampler`,
  `build_span_start_table`, and `annotate_log_record`.

This phase is the **mechanism** that drives that library from real engine
telemetry. It has three subsystems:

1. Span context carried through the data plane, with swap-in/swap-out around
   node execution, so node telemetry is causally linked to the data it
   processes.
2. A thread-local integrated emit/sample/buffer per engine worker.
3. An all-CPU windowed sampling processor that aggregates the per-worker output,
   emits the annotated stream, and publishes the two feedback tables.

In scope: the intra-process mechanism within one runtime. Out of scope, deferred
to the production sampler: the cross-process client/server heavy-hitter exchange
over OTAP/OTLP, the probability math, and the configuration surface.

## The two-level structure, reframed

The earlier two-level-logs prototype exchanged a global heavy-hitter table
between a client and a server over OTAP/OTLP. That is what a production,
multi-process sampler does, and it remains the production form.

This phase keeps the two-level structure but collapses it inside one process:
the two levels are **per-worker-thread** and **all-CPU**. Each engine worker
keeps a thread-local log sample; a single processor aggregates the per-worker
samples into a process-global picture over time windows and feeds two tables
back. Because every worker and the processor share process memory, the feedback
travels over `ArcSwap`, not a wire codec. The cross-process exchange is the same
shape one level up and is left to the production sampler.

## Architecture overview

```text
  side 1: per-worker integrated buffers          side 2: all-CPU processor
  +---------------------------------+
  | ordinary logs --> criterion-one |  period-end sample (HT-weighted)
  |                   local reservoir|------------------------\
  |                   (reserved)     |                         \
  | span-selected, reservoir-rejected|  eager raw span-logs     \
  |  + START/END    ----------------- }-----------------------> ITR --> WINDOWED
  +---------------------------------+                                  PROCESSOR
                 ^                                                          |
                 |  ArcSwap registry: heavy-hitter table + span-start table |
                 \----------------------------------------------------------/
                                                       --> emit annotated logs
```

Side 1 produces, side 2 aggregates and feeds back. The reporter-to-ITR path
(the implemented `InternalTelemetryReceiver`) carries side 1's output into the
internal telemetry pipeline where the processor node lives.

## Subsystem 1: span context in the data plane

A span surfaces causality through the data, not only through the synchronous
call stack. A receiver opens a span for an inbound request; the processors and
exporters that later handle that data must emit their logs and child spans under
that span, even though they run in different tasks and on different threads.

### Mechanism

- `OtapPdata`'s `Context` (`crates/otap/src/pdata.rs`, the `Context` struct)
  gains a field `span_context: Option<SpanContext>`. The context is already
  `Clone` and is already cloned into output `OtapPdata` by processors, so the
  span context rides with the data for free.
- A swap-in/swap-out guard sets the thread-local current span context to the
  data's `span_context` for the duration of a node's processing call, and
  restores the previous value on drop. It reuses the existing
  `CURRENT_SPAN_CONTEXT` thread-local and `current_span_context()` accessor in
  `crates/telemetry/src/tracing_init.rs`.
- **Processors**: the engine wraps the `processor.process(msg, ...)` call in the
  run loop (`crates/engine/src/processor.rs`, around the `Message::PData` arm)
  so the swap is automatic. For a `Message::PData(data)` the guard reads
  `data.context().span_context`.
- **Exporters**: the engine wraps the export of each inbound message in the
  exporter run loop (`crates/engine/src/exporter.rs`) the same way.
- **Receivers**: a receiver originates spans, so it uses the swap API
  explicitly. It opens a span for an inbound unit of work, enters it while
  building the outbound `OtapPdata`, and stamps that `OtapPdata.context` with
  the span context so downstream nodes inherit it.

### Semantics

A node that opens a child span while a data span context is swapped in links the
child to that context as its parent, exactly as the existing span layer does
from the entered-span stack. A processor that fans one input into several
outputs clones the context into each, so each output carries the same span
context. This subsystem is independently useful: even before the sampler runs,
it gives causally-linked node telemetry.

## Subsystem 2: the thread-local integrated buffer

Each engine worker owns one integrated buffer. A log record exists in it once,
whether it is logically part of the global sample, the span sample, both, or
neither.

### The local span-start decision

On a new span the worker runs the span-start sampler: it reads the span-start
threshold from the `ArcSwap` table, keyed by the span-start callsite, and
compares it against the trace-id randomness, both available in context. The
result sets the `locally_sampled` bit on the span context. This is already
implemented by `evaluate` and `SpanStartSampler`; nothing in the decision is
new. A span is **span-selected** when `locally_sampled` is set.

### Reserve the provisional, flush the terminal

The criterion-one independent sampler is a bottom-k reservoir over every log. In
bottom-k, a kept record is **provisional**: a later record with a smaller key
can evict it. A rejected record is **terminal**: once its key is past the cut it
can never re-enter, because the threshold only tightens as the window fills.

The buffer exploits this asymmetry:

| Record state | Span-selected | Action |
| --- | --- | --- |
| Kept by criterion one (provisional) | either | reserve until period end |
| Terminally rejected (on arrival or by eviction) | yes | move to the overflow |
| Terminally rejected | no | drop |
| START / END of a selected span | yes (by definition) | via the overflow when not kept |

The single load-bearing detail: when a previously-kept record is **evicted** it
has just become terminal, so if it is span-selected it must be moved to the
overflow **at eviction time**, not dropped. One more record is terminal at the
window boundary: bottom-k keeps `k + 1` entries and drops the largest to define
the threshold, so `finalize` returns that boundary record and the buffer
conserves it in the overflow when it is span-selected. At the window boundary the
buffer flushes both streams together: the reserved criterion-one sample with its
Horvitz-Thompson weights, and the raw overflow.

The result is that each record is flushed exactly once. A record can still
belong to two populations: a span-selected record that criterion one kept is
flushed once with the sample and serves both criterion one and, at the global
level, criterion two.

A future extension may add a third reserve for rare records that neither sample
selected, kept as reference events. It is noted here and not built.

### Flush transport, as implemented

Both streams leave the worker on the existing reporter path
(`ObservedEventReporter` in `crates/telemetry/src/event.rs`) into the
`InternalTelemetryReceiver`. In this local-only phase, `flush_to`
(`crates/telemetry/src/self_tracing/local_buffer.rs`) annotates each record with
the three adjusted counts computed **from this worker alone** and sends it: a
criterion-one representative carries its local `nhat` as
`otel.logs.adjusted_count`; a span-selected record additionally carries the
span's `otel.traces.adjusted_count` and `otel.span_logs.adjusted_count = 1`,
because there is no per-span reservoir at the worker level.

This local-only annotation is what Subsystem 3 changes. The next section explains
why, and the decision it turns on.

## Subsystem 3: the all-CPU windowed aggregator

The aggregator is the second side of the SDK: it turns the per-worker flushes
into one process-global sample, emits the final annotated stream, and publishes
the two feedback tables. It is not started; this section specifies it for a fresh
start, and names the one decision that shapes it.

### The decision: how a worker's flush reaches the aggregator

Subsystem 2's `flush_to` currently annotates records with local-only counts and
sends them on the reporter path into the `InternalTelemetryReceiver`, which
serializes each to OTLP bytes. That path is lossy for aggregation: the
process-local fields the aggregator needs are **not on the wire**. The serialized
`LogRecord` keeps `trace_id`, `span_id`, and `flags`, but **not** `start_callsite`
or `locally_sampled`, and the local `nhat` survives only as an attribute value
already dressed up as a final `otel.logs.adjusted_count`.

The aggregator needs, per flushed record, the role (criterion-one representative
versus raw span record versus boundary), the local `nhat`, the `span_id`, and the
`start_callsite`. Two ways to deliver them:

- **In-memory transfer, the chosen path.** Redirect the worker flush from the
  reporter into a process-global aggregator, handing over the `BufferFlush`
  (`SampledRecord` with `adjusted_count`, `span_selected`, and the full
  `LogRecord`, plus the raw span `LogRecord`s) with all fields intact and no
  serialization. The aggregator, not the worker, annotates and emits the final
  stream to the reporter. This matches the design's all-CPU-in-shared-memory
  intent and is the reason the feedback also rides shared memory rather than a
  wire codec.
- **Serialized transfer, rejected.** Keep flushing to the ITR and have
  the aggregator be a pipeline node reading the OTLP stream. Simpler wiring, but
  it cannot rebuild `start_callsite`, so the span-start value table cannot be
  formed globally and the aggregator would fall back to equal coverage. Recover
  the missing fields as extra attributes only if the in-memory path proves
  impractical.

The chosen path replaces Subsystem 2's local-only `flush_to` destination: a
worker flushes into the shared aggregator instead of the reporter.

### The aggregator structure

A process-global structure owned by a single finalizer thread, into which every
worker pushes its flush over a non-blocking channel and from which the finalizer
drains. It runs on one thread rather than behind a shared lock because it shares
a record between the two criteria within a window by reference count, which is a
single-thread construct. Per window it maintains the three estimators, reusing
the library:

- **Criterion one, second level.** Aggregate the per-worker criterion-one
  representatives, weighting each by its carried local `nhat`, into one all-CPU
  `BottomFloor`. Output: the process-global log sample and the **heavy-hitter
  table** fed back to the workers.
- **Criterion two.** Per-`span_id` `UniformReservoir`s over the union of the raw
  overflow records and the span-selected records in the per-worker samples.
  Keying by `span_id` reassembles spans whose records were produced on different
  workers.
- **Span-start value table.** The surprisal accumulator over in-span records,
  keyed by `start_callsite`, plus the span-start counter over boundaries, exactly
  `IntegratedSampler` today, feeding the next window's span-start threshold table.

At the window boundary it annotates every retained record with the three adjusted
counts via `annotate_log_record`, emits to the reporter, and republishes both
tables into the `ArcSwap` registry.

`IntegratedSampler`
(`crates/telemetry/src/self_tracing/sampling/processor.rs`) already implements
these three estimators over one in-memory stream. The work is to feed it from the
per-worker flushes, change criterion one from a single-level sample to the
second-level aggregation of representatives, and drive its window and feedback
from the seams below.

## Window mechanism

The per-worker flush is implemented on the pipeline controller's own periodic
`select!` branch, re-armed every `control_plane_metrics_flush_interval`
(`crates/engine/src/pipeline_ctrl.rs`), plus a final flush on loop exit. Each
worker flushes its own thread-local buffer, so the window is per worker.

The aggregator finalizes on its own timer as a standalone process-global
structure rather than as a pipeline node, for the reasons resolved under [Open
mechanism decisions](#open-mechanism-decisions). Its window is set equal to the
per-worker flush interval, `control_plane_metrics_flush_interval`, and its
finalizer runs an independent timer at that period, so the phase between a
worker's flush and the finalizer is arbitrary but fixed for a run. A per-worker
flush is atomic, so the aggregator never sees a split window, only whole
per-worker windows, and the arbitrary phase averages out over windows rather
than splitting any worker's contribution. The engine's periodic, coalesced
`CollectTelemetry` tick (`crates/control-channel/src/core.rs`) remains available
as an alternative finalize signal if folding the finalizer into the existing
cadence later proves preferable.

## Feedback transport: a process-global ArcSwap registry

The two tables are published by the aggregator and read by every worker and by
the aggregator itself. A process-global registry holds the current immutable
table for each, behind an `ArcSwap`: readers load wait-free on the hot path, and
the aggregator stores a freshly built table once per window. No wire codec is
needed because all participants share process memory, which is also why the
in-memory worker-to-aggregator transfer above is the natural choice. An absent
table degrades to the fail-safe already designed: the span-start table to sample
everything, the heavy-hitter table to local-only criterion one, which is exactly
the current Subsystem 2 behavior before any aggregator runs.

## Open mechanism decisions

Resolved during implementation:

- **Buffer anchoring: thread-local per core.** Each pipeline is a core-pinned
  thread-per-core runtime (`spawn_local` on a `LocalSet`), so the worker's tasks
  and its controller share one OS thread and a `thread_local` buffer is coherent.
- **Worker flush trigger: the controller's periodic `select!` branch.**
- **On-the-wire role marker: not needed under the in-memory transfer.** The
  `BufferFlush` carries role, local `nhat`, and full context in memory.
- **Exporter span-context swap: skipped**, per decision; exporters may call
  `scope_span_context` explicitly.

Resolved for Subsystem 3:

- **Aggregator form: a standalone process-global structure, not a pipeline
  node.** The aggregator sits upstream of the reporter, since it produces the
  annotated log events the reporter carries into the Internal Telemetry Receiver,
  so modeling it as a data-plane node would invert the flow. It also spans every
  core-pinned pipeline runtime in the process, whereas a node lives inside one
  pipeline and would bind process-global aggregation to that pipeline's lifecycle.
  It is a library capability started and stopped through a lifecycle guard,
  `start_sample_aggregator`, which the observability pipeline configuration
  activates rather than the controller starting it unconditionally; until then
  the workers keep the Subsystem 2 local-only behavior.
- **Window alignment: aggregator window equal to the worker flush interval on an
  independent timer.** A per-worker flush is atomic, so each push delivers one
  complete per-worker window and the aggregator never observes a split window.
  The finalizer runs its own timer at the same period as the worker flush rather
  than on a shared clock, so the phase between a worker's flush and the finalizer
  is arbitrary but fixed for a run. No cross-worker barrier is needed: a late
  worker's window lands in the next aggregator window and the estimators stay
  unbiased in expectation, so the arbitrary phase averages out over windows and
  trades a small, self-correcting wobble for freedom from coordination.
- **Back-pressure: never block a worker, sample rather than drop, keep the
  reporter lossy.** The worker-to-aggregator hop is a non-blocking channel send,
  so a worker never blocks on it; the aggregator runs on a single finalizer
  thread because it shares records between the two criteria by reference count
  within a window, which rules out a lock-shared instance. The aggregator's
  per-window budgets bound its memory and reduce over-budget records by sampling
  rather than losing them, the mechanism's intended reduction. Because a
  core-pinned worker must never block on telemetry, a full inbound channel drops
  a whole per-worker flush and counts it rather than stalling the worker. The
  aggregator-to-reporter hop keeps the reporter's existing non-blocking,
  lossy-under-back-pressure send policy, since the output is already a bounded
  sample travelling the same reporter-to-Internal-Telemetry-Receiver path
  Subsystem 2 used.

## Sequencing

1. **Subsystem 1: done.** Span context in `OtapPdata` plus the task-local
   swap-in/swap-out; independently useful for causal node telemetry.
2. **Subsystem 2: done, local-only.** The per-worker integrated buffer, its
   install, and the controller flush branch, replacing the direct-to-channel log
   path for worker threads.
3. **Subsystem 3: built as a library capability.** The all-CPU aggregator and the
   two `ArcSwap` feedback tables turn the per-worker flushes into the annotated
   stream, over a channel to a single finalizer thread, with the local-only path
   as the fallback until the observability pipeline configuration activates the
   aggregator. Follow-ups: the worker heavy-hitter binding gate, the live tracer
   span-start sampler, and the cross-process two-level log feedback.

## References

- [`integrated-logs-traces-reservoir.md`](integrated-logs-traces-reservoir.md),
  the algorithm and the three populations.
- [`span-start-value-sampling.md`](span-start-value-sampling.md), the span-start
  value math.
- [`self_tracing_architecture.md`](self_tracing_architecture.md), the internal
  telemetry pipeline and the Internal Telemetry Receiver.
- [`self_tracing_spans_design.md`](self_tracing_spans_design.md), the span model
  and the `SpanContext`.
