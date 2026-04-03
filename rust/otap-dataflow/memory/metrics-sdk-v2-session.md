# Metrics SDK v2 — Session Summary

## Starting Point

This work began from the report in
[`./memory/metrics-sdk-codegen-complete.md`](./memory/metrics-sdk-codegen-complete.md),
which documented a previous effort on the `jmacd/metric_sdk` branch:
a codegen-driven OTAP-native metrics SDK (13 commits, ~4,270 lines,
31 tests). That branch proved the architecture — precomputed Arrow
schemas, cumulative accumulation, OpenMetrics exposition — but relied
on YAML codegen for metric set definitions.

This session's goal: **repeat the effort without codegen**, bridging
from existing `#[metric_set]` / `#[attribute_set]` macros, and replace
the OTel SDK metrics path entirely.

## What Was Built

**7 commits on `jmacd/metrics_sdk_2`, 18 files changed, ~3,040 lines added, 27 tests.**

### Branch: `jmacd/metrics_sdk_2`

| # | Commit | Description |
|---|--------|-------------|
| 1 | `a52a034a` | Port runtime lib + descriptor-to-schema bridge |
| 2 | `eacd9515` | MetricsTap with Prometheus + OTAP pipeline export |
| 3 | `aabead1f` | Replace MetricsDispatcher + OTel SDK with MetricsTap |
| 4 | `47bf196e` | Support Gauge, UpDownCounter, and Mmsc types |
| 5 | `89fad9db` | Wire ITS mode: MetricsTap → channel → ITR → pipeline |
| 6 | `9ac9d8f1` | Update internal-telemetry.yaml with pipeline docs |
| 7 | `54dbb074` | Console exporter: implement metrics formatting |

## Architecture

```
#[metric_set] structs (unchanged — 240 Counters, 27 Gauges, 26 ObserveCounters,
                        12 Mmsc, 9 ObserveUpDownCounters, 1 UpDownCounter)
     │
     │  existing snapshot/report/accumulate path (unchanged)
     ▼
TelemetryRegistry (existing)
     │
     │  visit_metrics_and_reset_with_entity() — new method
     ▼
MetricsTap (replaces MetricsDispatcher + OTel SDK)
     │
     │  Caching (mirroring the logging partial-OTLP pattern):
     │    Resource: single RecordBatch, built once (like resource_bytes)
     │    Scope: HashMap<EntityKey, RecordBatch> (like ScopeToBytesMap)
     │    Schema: HashMap<&str, CachedDescriptor> per descriptor name
     │
     │  Per tick: expand_snapshot() → delta NumberDataPoints batch
     │    Mmsc fields expand to 4 data points (min/max/sum/count)
     │    Per-data-point AccumulationMode: Add (delta counters) vs Replace (gauges)
     │
     ├─→ PrometheusExporter (ALWAYS — like LogTap)
     │     CumulativeAccumulator with per-point Add/Replace semantics
     │     GET /metrics → format_openmetrics (counter vs gauge TYPE)
     │
     ├─→ [Admin mode — default]: admin /metrics + /status from registry
     │
     └─→ [ITS mode — opt-in]:
           flume channel → ITR event loop → OtapPdata → pipeline
           Each payload: resource.id=0, scope.id=0 + child batches
           Batch processor handles ID reindexing on merge
           Console exporter: hierarchical metrics formatting
           OTLP exporters: OTAP→OTLP conversion (existing)
```

## Files Added/Modified

### New: `crates/telemetry/src/self_metrics/`

| File | Lines | Purpose |
|------|-------|---------|
| `mod.rs` | 19 | Module wiring |
| `precomputed.rs` | 367 | PrecomputedMetricSchema, CounterDataPointsBuilder |
| `accumulator.rs` | 364 | CumulativeAccumulator with Add/Replace modes |
| `assembly.rs` | 77 | Assemble 3 OTAP tables into OtapArrowRecords |
| `openmetrics.rs` | 440 | Arrow batches → OpenMetrics text (counter+gauge) |
| `prometheus.rs` | 204 | PrometheusExporter with axum /metrics |
| `collector.rs` | 143 | MetricsEncoder for snapshot→OTAP encoding |
| `bridge.rs` | 450 | MetricsDescriptor → PrecomputedMetricSchema, Mmsc expansion |
| `metrics_tap.rs` | 448 | MetricsTap: collection, caching, dual export |

### Modified

| File | What changed |
|------|-------------|
| `crates/telemetry/src/lib.rs` | Replace MetricsDispatcher+OTel SDK with MetricsTap, Prometheus server, ITS channel |
| `crates/telemetry/src/metrics.rs` | Add `visit_metrics_and_reset_with_entity()` |
| `crates/telemetry/src/registry.rs` | Expose entity key in visitor callback |
| `crates/telemetry/src/error.rs` | Add MetricEncoding variant |
| `crates/telemetry/Cargo.toml` | Add `arrow` dependency |
| `crates/controller/src/lib.rs` | Spawn metrics_tap.run() instead of dispatcher |
| `crates/core-nodes/.../internal_telemetry_receiver/mod.rs` | Read metrics channel, send_metrics_payload() |
| `crates/core-nodes/.../console_exporter/mod.rs` | Implement metrics formatting (299 lines) |
| `configs/internal-telemetry.yaml` | Document OTAP metrics pipeline flow |

## Key Design Decisions

1. **No codegen**: Bridge from existing `MetricsDescriptor` (name, unit,
   instrument, temporality) carried by `#[metric_set]` — no YAML schemas
   or MiniJinja templates needed.

2. **`Some(0)` for resource/scope IDs**: Each payload has exactly one
   resource and one scope. The batch processor's reindex logic handles
   ID remapping when merging. Precedent: syslog receiver does the same.

3. **Mmsc expansion to 4 sub-metrics**: `name.min` (gauge), `name.max`
   (gauge), `name.sum` (counter), `name.count` (counter) — matches the
   admin endpoint's existing Prometheus formatting.

4. **Per-data-point AccumulationMode**: Delta counters use `Add`
   (pointwise accumulation), gauges and cumulative counters use `Replace`
   (latest value). Enables mixed-type metric sets in a single batch.

5. **Admin vs ITS**: Two configuration modes mirroring logging's
   ConsoleAsync vs ITS. Admin mode (default) needs no internal pipeline.
   ITS mode sends OTAP payloads through the pipeline for OTLP export.

## What's Next

- **Remove OTel SDK metrics dependencies**: `opentelemetry-prometheus`,
  `prometheus` crate, `opentelemetry-stdout` metrics features. The code
  paths are bypassed but deps remain in Cargo.toml.
- **Test end-to-end**: Run with `configs/internal-telemetry.yaml` and
  verify metrics appear in console output and on `/metrics`.
- **Double value support**: Mmsc min/max/sum currently truncated to i64.
  Add `double_value` column support for full f64 precision.
- **Scope name on metrics**: Set scope name/version from entity descriptor
  (currently empty scope).
- **Weaver integration**: Eventually replace `#[metric_set]` macros with
  schema-driven codegen from Weaver semantic conventions.
