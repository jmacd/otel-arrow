# BKCR — DFE Integration Notes (working draft, not committed)

Companion to `design.md`. Captures the integration surface for wiring
the BKCR sampler into the OTAP Dataflow Engine's internal-telemetry
path. This doc is a starting point; many details below are open.

## Scope of integration

BKCR is a *library* (`crates/telemetry/src/sampler/`). Integration
work consists of:

1. Adding a sampling configuration knob in the internal-logs config.
2. Instantiating one `Bkcr` per pipeline thread, behind the existing
   tracing subscriber layer.
3. Carrying per-event sampling weight from sampler to
   `internal_telemetry_receiver` so that downstream OTLP records
   reflect the inverse-probability weights.
4. Batching: sending one flushed period of records through the
   existing `ObservedEvent` channel as a single batch event, so the
   receiver assembles one OTLP batch per period per thread instead
   of one per log line.

## Configuration

**No Cargo feature flag.** The behaviour is selected by configuration
only. Add a boolean (and supporting knobs) to `LogsConfig` in
`crates/config/src/settings/telemetry/logs.rs`. Sketch:

```rust
pub struct LogsConfig {
    pub level: LogLevel,
    pub providers: LoggingProviders,
    pub tap: InternalLogTapConfig,
    pub sampler: InternalLogSamplerConfig, // NEW
}

pub struct InternalLogSamplerConfig {
    /// Enable BKCR sampling for internal logs. When false, every
    /// log event is forwarded individually (today's behaviour).
    pub enabled: bool,                  // default false
    /// Sample-size cap `T` per period per thread.
    pub target: usize,                  // default e.g. 128
    /// Novelty-reserve cap `R` per period per thread.
    pub reserve_capacity: usize,        // default = target
    /// Period boundary trigger.  Open question (see below).
    pub period: SamplerPeriod,
}
```

Validation should reject `enabled && target == 0`.

## Hook point

`crates/telemetry/src/tracing_init.rs` :: `StructuredLoggingLayer::on_event`
is the per-event entry into the internal-telemetry path. It currently
does, unconditionally:

```rust
let record = LogRecord::new(event, context);
if let Some(reporter) = &self.reporter {
    reporter.log(LogEvent { time, record });
}
```

After integration, when the sampler is enabled this layer instead
routes through a per-thread `Bkcr`:

```rust
let admission = sampler.admit(callsite_id_of(event));
match admission {
    Admission::Skip => return,                            // no format cost
    Admission::Admit(_) | Admission::Reserve => {
        let record = LogRecord::new(event, context);
        sampler.insert(callsite_id_of(event), admission, LogEvent { time, record });
    }
}
```

Key design points for the hook:

- **Per-thread instances.** `Bkcr` is `!Sync` by design. One instance
  per pipeline thread, stored in a `thread_local!` or in the
  thread-owned subscriber state. The `StructuredLoggingLayer` is
  shared across threads via `Dispatch`, so the per-thread state must
  be reached either by a thread-local or via a per-thread layer
  installed at `with_subscriber` time.
- **Callsite identity.** `tracing::Event` carries a stable
  `event.metadata().callsite()` (`tracing::callsite::Identifier`).
  Use it (or its `&'static Metadata` pointer) as the `C` parameter
  to `Bkcr<C, _>`. Avoid hashing string content.
- **Defer formatting on `Skip`.** Today `LogRecord::new` is invoked
  unconditionally and visits all fields. The whole economic case for
  the sampler is that this work is skipped for `Admission::Skip`.
  This may require splitting `LogRecord::new` so callsite id and
  level are extracted cheaply before any field formatting.

## Payload type

`Bkcr<C, P>` is generic in payload. Concrete choice:
**`P = LogEvent`** (the already-existing struct with `time` +
`LogRecord`).

Pros: minimal new types; flush emits a `Vec<(C, LogEvent, f64)>` that
maps trivially onto the batch event.
Cons: pays formatting cost on `Reserve` too, not just `Admit`. This
is fine — reserve admissions are rare by design.

## Weight propagation

`LogRecord` currently has no field for the Horvitz–Thompson weight.
Add one:

```rust
pub struct LogRecord {
    // ...existing fields...
    pub sampling_weight: Option<f64>, // None when sampler disabled
}
```

`None` ⇒ no sampling, treat as weight 1 downstream. `Some(0.0)` ⇒
reserve novelty record (do not include in counts). `Some(w >= 1.0)`
⇒ statistical sample.

OTLP export must translate this to an OTLP log attribute on the
emitted record so external consumers can correctly aggregate. Suggested
attribute name: `otel.sampling.weight` (open).

## Batch transport

Today: `ObservedEvent::Log(LogEvent)` is one channel send per event.
The internal-telemetry receiver assembles its OTLP batch by polling
the channel and grouping by time/size.

Plan: introduce a new variant carrying a whole period's worth of
records from one thread's sampler:

```rust
pub enum ObservedEvent {
    Engine(EngineEvent),
    Log(LogEvent),
    LogBatch(Vec<LogEvent>),  // NEW
}
```

At sampler-flush time, the layer calls `bkcr.flush_into(...)` and
sends **one** `ObservedEvent::LogBatch` containing the full vector.
`internal_telemetry_receiver` (in
`crates/core-nodes/src/receivers/internal_telemetry_receiver/`)
flattens `LogBatch` into the OTLP scope-logs in a single shot,
preserving per-record sampling weight as the attribute above.

Open: should `LogBatch` also pass through the existing
`InternalLogTap`? Probably yes (tap is observational), tap can ignore
weights or surface them.

## Period boundary trigger

The sampler needs a `flush_into(...)` call at period boundaries. Who
drives that? Options (not yet decided):

1. **Wall-clock timer per thread.** A small background tick (e.g.,
   1 s) that flushes every active sampler. Adds a sync primitive
   between the tracing layer and the timer task.
2. **Event-count trigger.** Flush every N admissions or every N
   arrivals. Simple, but bursty workloads will produce bursty
   batches; quiet threads may never flush.
3. **Hybrid.** Flush whenever (a) `T` events have been admitted, or
   (b) a per-thread timer fires, whichever first. Most operationally
   reasonable; what the production deployment will likely want.

The receiver-side batching window should align with — or be a
multiple of — the sampler period to avoid tearing per-thread
batches across receiver batches.

## ProviderMode interaction

`ProviderMode::ITS` and `::ConsoleAsync` both build the
`InternalAsync` provider, which wires the `ObservedEventReporter`
into `StructuredLoggingLayer`. The sampler should be active in **both**
async modes when `LogsConfig.sampler.enabled` is set. `ConsoleDirect`
and `Noop` are unaffected: direct-console output bypasses the
reporter entirely, and noop discards.

## Things deliberately out of scope (for this integration milestone)

- Cross-thread sample merging. Each thread is independent; the
  receiver concatenates. Downstream estimators sum weighted records
  across threads, which is correct because BKCR's HT estimator is
  additive across independent samplers.
- Backpressure between sampler and channel. The flush is one send;
  if the channel is `SendPolicy::Drop`, the whole period's batch is
  lost as a single unit. Acceptable for v1.
- Dynamic reconfiguration of `T` and `R` at runtime. Restart-only
  for v1.
- Adaptive period sizing.

## Open questions

1. **Callsite id type.** `Identifier` (cheap, opaque) vs `&'static
   Metadata` (richer, also cheap). Pick one; commit.
2. **Period driver.** Timer-only, count-only, or hybrid (above).
3. **OTLP attribute name** for sampling weight.
4. **Reserve cap default.** `R = T` matches design.md but doubles
   worst-case throughput; revisit after first integration tests.
5. **Tap interaction.** Does `InternalLogTap` see sampled records,
   unsampled records, or both?
6. **What happens to `EngineEvent`?** BKCR samples logs only.
   `ObservedEvent::Engine(_)` continues to be sent unbatched. Confirm
   the receiver still handles the mixed stream correctly.
7. **Shutdown flush.** On clean shutdown the layer must call
   `flush_into` one last time per thread to avoid losing the
   in-flight period.

## Suggested first PR shape

Small enough to review, big enough to be useful:

1. Add `InternalLogSamplerConfig` to `LogsConfig` (no behaviour
   change yet; off by default).
2. Add `LogBatch(Vec<LogEvent>)` variant and teach
   `internal_telemetry_receiver` to flatten it.
3. Add `sampling_weight: Option<f64>` to `LogRecord` and wire it
   through to the OTLP exporter as an attribute.

Subsequent PRs introduce the actual sampler hook in
`StructuredLoggingLayer`, the per-thread sampler instances, and the
period driver.
