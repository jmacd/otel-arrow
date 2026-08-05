# Metrics SDK — Phase A Complete (April 8, 2026)

## What We Did

Replaced the OTel SDK metrics path (`MetricsDispatcher` → OTel SDK
`SdkMeterProvider` → `opentelemetry-prometheus`) with an OTAP-native
internal metrics pipeline that produces Arrow record batches directly.
The migration is invisible to users — same metric names, same
Prometheus `/metrics` output, same YAML config.

## Architecture

```
#[metric_set] structs (unchanged)
       │
       ▼
TelemetryRegistryHandle::visit_metrics_and_reset_with_entity()
       │
       ▼
MetricsTap::collect_and_dispatch()
       │
       ├── bridge::descriptor_to_schema_with_views()  ← init-time, cached
       │       builds PrecomputedMetricSchema from MetricsDescriptor
       │       applies ViewConfig renaming (scope_name + instrument_name → name + description)
       │
       ├── bridge::expand_snapshot()  ← runtime
       │       maps MetricValue variants to i64 values
       │       Mmsc expands to 4 sub-metrics (min, max, sum, count)
       │
       ├── CounterDataPointsBuilder::build_int_values_i64()  ← runtime
       │       builds NumberDataPoints Arrow RecordBatch
       │
       ├──► PrometheusExporter::ingest_with_modes()  ← always
       │       CumulativeAccumulator (delta→cumulative via Arrow compute)
       │       format_openmetrics() on scrape
       │
       └──► ITS channel (flume)  ← when internal pipeline configured
               OtapMetricsPayload → InternalTelemetryReceiver
```

### Key Insight: 2 of 3 Tables Are Precomputed

For each metric set, OTAP encoding produces 3 Arrow tables:

1. **Metrics table** — one row per metric (name, type, unit, temporality,
   resource.id, scope.id). Fully determined by schema → precomputed at
   init time.
2. **Attributes table** — dimension key/value pairs per data point.
   Fully determined by schema → precomputed at init time.
3. **NumberDataPoints table** — counter values + timestamps. Built at
   runtime from `MetricValue` snapshots.

The precomputed tables are cloned (Arc-backed, zero-copy) across
collection ticks. Only the data points table is rebuilt each cycle.

### Dual-Path Export (Like Logs)

Mirrors the existing internal logs architecture:

- **No internal pipeline configured**: MetricsTap runs on an admin
  thread, feeds PrometheusExporter directly. Prometheus scrapes read
  from `CumulativeAccumulator` via `RwLock`.
- **Internal pipeline configured**: MetricsTap additionally sends
  `OtapMetricsPayload` through a flume channel to the
  `InternalTelemetryReceiver`, which injects them into the engine
  pipeline. PrometheusExporter still receives a copy via MetricsTap.

### Views

The existing `engine.telemetry.metrics.views` YAML config is supported:

```yaml
views:
  - selector:
      scope_name: "engine.metrics"
      instrument_name: "memory.rss"
    stream:
      name: "process_memory_usage"
      description: "Total physical memory used."
```

Views are applied at init time in `descriptor_to_schema_with_views()`:
the `find_matching_view()` function checks each field against
configured selectors (with `None` = wildcard), and substitutes the
stream name/description into the precomputed metrics table row. For
Mmsc instruments, the renamed base name propagates to all 4
sub-metrics (e.g., `process_duration.min`, `.max`, `.sum`, `.count`).

Zero runtime cost — views are baked into the Arrow schema.

### Accumulation Modes

Each data point has an `AccumulationMode` determined by its instrument
type:

| Instrument | Temporality | Mode |
|---|---|---|
| Counter | Delta | Add |
| Counter | Cumulative | Replace |
| UpDownCounter | any | Replace |
| Gauge | — | Replace |
| Mmsc.min | — | Replace |
| Mmsc.max | — | Replace |
| Mmsc.sum | — | Add |
| Mmsc.count | — | Add |

The `CumulativeAccumulator::ingest_with_modes()` method handles mixed
Add/Replace within a single batch using per-element mode arrays.

## Commits (4)

1. **`499e3cef` — Foundation**: 8 `self_metrics/` modules ported from
   `jmacd/metrics_sdk_2` branch. 27 tests.
2. **`b47f9972` — Views**: `descriptor_to_schema_with_views()`,
   `find_matching_view()`, MetricsTap accepts `Vec<ViewConfig>`. 3 tests.
3. **`8e6d9b4b` — Wiring**: `InternalTelemetrySystem` creates
   `MetricsTap` instead of `MetricsDispatcher` + OTel SDK. Controller
   uses `metrics_tap.run()`. `shutdown()` replaces `shutdown_otel()`.
   `InternalTelemetrySettings` gains `metrics_receiver` field.
4. **`59ff1abf` — Integration tests**: Mixed instruments (Counter +
   Gauge + Mmsc) with views, and ITS channel dual-path test. 2 tests.

## Files Changed

```
15 files changed, 2890 insertions(+), 39 deletions(-)

New modules (crates/telemetry/src/self_metrics/):
  mod.rs           —  19 lines  module declarations
  precomputed.rs   — 349 lines  PrecomputedMetricSchema, CounterDataPointsBuilder
  bridge.rs        — 643 lines  descriptor_to_schema, views, expand_snapshot, scope/resource attrs
  accumulator.rs   — 340 lines  CumulativeAccumulator, MetricIdentity, ingest_with_modes
  assembly.rs      —  72 lines  assemble_metrics_payload (3 tables → OtapArrowRecords)
  collector.rs     — 129 lines  MetricsEncoder (snapshot → OTAP)
  openmetrics.rs   — 408 lines  format_openmetrics (Arrow → Prometheus text)
  prometheus.rs    — 199 lines  PrometheusExporter (RwLock<CumulativeAccumulator> + axum handler)
  metrics_tap.rs   — 627 lines  MetricsTap (dispatch loop, caching, dual-path export)

Modified:
  Cargo.toml       — +arrow dependency
  error.rs         — +MetricEncoding variant
  lib.rs           — MetricsTap replaces MetricsDispatcher + OTel SDK fields
  metrics.rs       — +visit_metrics_and_reset_with_entity()
  registry.rs      — +visit_metrics_and_reset_with_entity() public API
  controller/lib.rs — metrics_tap.run() replaces dispatcher.run_dispatch_loop()
```

## Test Coverage (32 tests)

- `precomputed`: basic counter, two dimensions, data points builder
- `bridge`: descriptor→schema, data points build, Mmsc expansion,
  snapshot zeros, resource attrs, all-int check, view rename counter,
  view rename Mmsc, view wildcard scope
- `accumulator`: first delta, multiple deltas, multiple identities,
  precomputed tables in snapshot
- `assembly`: assemble roundtrip
- `collector`: encode nonzero, encode all-zeros → None
- `openmetrics`: basic counter, accumulated values, empty, two dimensions
- `prometheus`: empty → EOF, ingest+format, accumulate across deltas
- `metrics_tap`: empty → no payloads, data → payload, Prometheus
  accumulates deltas, **integration: mixed instruments with views**,
  **integration: ITS channel receives payloads**

## What Remains

### Phase B — Schema YAML Definitions (incremental)

One metric_set at a time, add YAML schemas with v1-compatible fan-out
rules. `cargo xtask migrate-metric-set` auto-generates v1 schema from
existing structs. Codegen produces level-aware instrument enums. Source
branch: `jmacd/metrics_view_1` for codegen infrastructure.

### Phase C — Exponential Histogram

Vendor `rust-expohisto`, add histogram recording mode. Mmsc instruments
upgrade to expohisto at normal/detailed metric levels via schema-driven
level selection. OTAP encoding uses
`ExponentialHistogramDataPointsRecordBatchBuilder`.

### Cleanup

- OTel SDK dependencies (`opentelemetry`, `opentelemetry_sdk`,
  `opentelemetry-prometheus`, `opentelemetry-otlp`) remain in
  `Cargo.toml` — the `otel_sdk` module is still compiled. Full
  removal after verifying no downstream code references these types.
- `MetricsDispatcher` (`crates/telemetry/src/metrics/dispatcher.rs`)
  is still compiled but no longer instantiated. Can be removed once
  the migration is validated in production.
- The OTTL transform processor mentioned by the user operates
  downstream in the pipeline on OTAP batches — it is unaffected by
  this change since MetricsTap produces the same OTAP payload format.
