# Internal Metrics SDK

Status: **Phase 1 implemented** on branch `jmacd/metrics_sdk_3`.

## Overview

The Internal Metrics SDK replaces the OTel SDK metrics path with an
OTAP-native pipeline that produces Arrow record batches directly from
`#[metric_set]` structs. It mirrors the architecture already
established for internal logs via the `internal_telemetry_receiver`,
providing a `MetricsTap` component analogous to `LogTap`.

The design proceeds in three phases:

1. **Seamless OTel SDK replacement** — swap the runtime path without
   changing any metric definitions or user-visible output.
2. **Schema-driven codegen with metric levels** — introduce YAML
   metric schemas and compile-time level selection, migrating one
   metric set at a time.
3. **Exponential histogram support** — upgrade Mmsc instruments to
   proper histograms with configurable resolution.

## Instrument Model

The SDK retains the three OTel instrument archetypes (counter,
updowncounter, gauge) but clarifies the observation style:

| Rust Type | Archetype | Observation | Reset Behavior |
|---|---|---|---|
| `Counter<T>` | counter | delta (`add`) | zeroed after collection |
| `UpDownCounter<T>` | updowncounter | delta (`add`/`sub`) | zeroed after collection |
| `ObserveCounter<T>` | counter | cumulative (`observe`) | preserved |
| `ObserveUpDownCounter<T>` | updowncounter | cumulative (`observe`) | preserved |
| `Gauge<T>` | gauge | instantaneous (`set`) | preserved |
| `Mmsc` | histogram | delta (`record`) | zeroed after collection |

Delta instruments accumulate within a collection interval and reset to
zero after snapshot. Observe/gauge instruments report the latest value
and are not reset. This distinction is enforced by the type system —
delta instruments have no `set()` method, and observe instruments have
no `add()` method.

## Phase 1: Seamless OTel SDK Replacement

### Goal

Replace `MetricsDispatcher` → OTel SDK `SdkMeterProvider` →
`opentelemetry-prometheus` with an OTAP-native path. Users see
identical Prometheus `/metrics` output. No `#[metric_set]` structs
change.

### Architecture

```text
#[metric_set] structs (unchanged)
        │
        ▼
  TelemetryRegistryHandle
  ::visit_metrics_and_reset_with_entity()
        │
        ▼
  MetricsTap::collect_and_dispatch()
        │
        ├── descriptor_to_schema_with_views()     [init-time, cached]
        │     builds PrecomputedMetricSchema
        │     applies view renames
        │
        ├── expand_number_snapshot()               [runtime]
        │     MetricValue → NumberValue (Int|Double)
        │
        ├── expand_histogram_snapshot()            [runtime]
        │     MetricValue::Mmsc → MmscSnapshot
        │
        ├──► PrometheusExporter                    [always]
        │     CumulativeAccumulator (NDP delta→cumulative)
        │     MmscSnapshot histogram accumulation
        │     format_openmetrics() on scrape
        │
        └──► ITS channel (flume)                   [when configured]
              OtapMetricsPayload → InternalTelemetryReceiver
```

### Precomputed Tables

For each metric set, OTAP encoding produces multiple Arrow tables.
Two are fully determined by the schema and precomputed at init time:

- **Metrics table** — one row per metric (name, type, unit,
  temporality, resource.id, scope.id).
- **Attributes table** — dimension key/value pairs per data point
  (empty in Phase 1 since current metric sets have no dimensions).

Only the data point tables are built at runtime:

- **NumberDataPoints** — for counters, updowncounters, and gauges.
  Supports both `int_value` and `double_value` columns.
- **HistogramDataPoints** — for Mmsc instruments. Encoded as
  explicit-bucket histograms with empty boundaries, carrying
  min/max/sum/count.

Precomputed batches are cloned via Arc (zero-copy) across collection
cycles.

### Dual-Path Export

MetricsTap supports two deployment modes, mirroring internal logs:

- **No internal pipeline** (`ConsoleAsync` or standalone): MetricsTap
  runs on an admin thread, feeds the `PrometheusExporter` directly.
  Prometheus scrapes read from the cumulative accumulator via
  `RwLock`. This is the default behavior.

- **Internal pipeline configured** (`ITS` mode): MetricsTap
  additionally sends `OtapMetricsPayload` through a flume channel to
  the `InternalTelemetryReceiver`, which injects metrics into the
  engine pipeline for export via configured exporters. The
  `PrometheusExporter` still receives a copy — builtin Prometheus
  always works regardless of ITS configuration.

### View Support

Phase 1 preserves the existing `engine.telemetry.metrics.views` YAML
configuration, limited to scoped metric renames:

```yaml
engine:
  telemetry:
    metrics:
      views:
        - selector:
            scope_name: "engine.metrics"
            instrument_name: "memory.rss"
          stream:
            name: "process_memory_usage"
            description: "Total physical memory used."
```

Views are applied at init time when building the precomputed metrics
table. The `find_matching_view()` function checks each field against
configured selectors (with `None` = wildcard match), and substitutes
the stream name and/or description. For Mmsc instruments, the renamed
base propagates to the histogram metric row.

This is the only views capability preserved from the OTel SDK. Phase
2 replaces this mechanism entirely with compile-time metric level
selection.

### Accumulation

Each data point has an accumulation mode determined by its instrument
type and temporality:

| Instrument | Temporality | Mode |
|---|---|---|
| `Counter` | Delta | Add |
| `Counter` | Cumulative | Replace |
| `UpDownCounter` | any | Replace |
| `Gauge` | — | Replace |

The `CumulativeAccumulator` converts delta snapshots to cumulative
state using per-element Arrow compute operations, supporting both
`int_value` and `double_value` columns with null-aware arithmetic.

Mmsc instruments are accumulated separately by the
`PrometheusExporter`: sum and count are added, min and max track
extremes across deltas.

## Phase 2: Schema-Driven Codegen with Metric Levels

### Goal

Introduce YAML metric schemas for each `#[metric_set]` struct,
enabling compile-time control over instrumentation detail through
three metric levels: Basic, Normal, and Detailed. Migrate one metric
set at a time with backward-compatible output.

### Metric Levels Replace Views

Instead of the traditional OTel Views configuration (which offers
arbitrary runtime stream transformations), this SDK offers three
hard-coded levels configured at build time through the metric schema.
For each metric in a schema, the YAML definition specifies what
happens at each level:

- **Attribute dimensions**: which dimensions are active (e.g., Basic
  gets no signal dimension, Normal adds `signal_type`, Detailed adds
  further breakdown).
- **Aggregation**: which recording mode to use (e.g., Basic gets Mmsc
  for durations, Normal gets Histogram<8>, Detailed gets
  Histogram<16>).
- **Metric presence**: some metrics may only exist at Normal or
  Detailed levels.

This is a deliberate deviation from the OTel SDK specification, which
treats views as a runtime configuration concern. We make it a
compile-time concern because:

1. The set of internal metrics is fully known at build time.
2. Three levels cover the practical dimensionality/cost trade-offs.
3. Compile-time selection eliminates runtime branching in the hot path.

### Schema Format

Schemas use OTel Semantic Conventions YAML syntax with `x-otap`
extensions:

```yaml
groups:
  - id: metric.node.consumer
    type: metric
    metric_name: node.consumer.items
    instrument: counter
    unit: "{item}"
    brief: "Items consumed by this node."
    stability: experimental
    attributes:
      - ref: outcome
        requirement_level: required
      - ref: signal_type
        requirement_level:
          conditionally_required: "when metric_level >= normal"
    x-otap-levels:
      basic:
        dimensions: [outcome]
      normal:
        dimensions: [outcome, signal_type]
      detailed:
        dimensions: [outcome, signal_type]
```

### Codegen

`cargo xtask gen-metrics` reads schema YAML files and generates:

- **Level-aware instrument enums**: e.g., `NodeConsumerItemsCounter`
  with `Basic([Counter<u64>; 3])` and
  `Normal([[Counter<u64>; 3]; 3])` variants.
- **Metric set structs**: with `MetricSetHandler` impl
  (snapshot/clear/needs_flush).
- **OTAP encoders**: precomputed schema construction with the
  correct dimension attributes per level.
- **Documentation**: `telemetry.md` generated from schema.

### Schema Versioning and Migration

Each metric set schema declares versions. The v1 schema captures the
exact current behavior (metric names derived from the existing
`replace('_', '.')` convention, no dimensions). The v2 schema
declares the intended new instrumentation (e.g., `consumer.requests`
with `{outcome, signal_type}` dimensions instead of
`consumed_success` / `consumed_failure` / `consumed_log_records` as
separate metrics).

The OTel schema file syntax supports metric renaming rules that
translate between versions. The codegen tool generates fan-out logic
so that a single internal measurement can produce output in both v1
and v2 formats simultaneously.

Users select the active schema version per metric set (by scope name)
through program settings. This enables gradual migration: upgrade one
metric set to v2 output while the rest remain on v1, with no
disruption to downstream consumers.

### Migration Workflow

For each `#[metric_set]` struct:

1. Run `cargo xtask migrate-metric-set` — auto-analyzes the existing
   struct, detects NxM dimension grids in field names, generates v1
   schema YAML with exact backward-compatible output rules.
2. Review and edit the generated YAML. Define the v2 schema with
   desired dimensions and level-aware behavior.
3. Run `cargo xtask gen-metrics` to produce generated code.
4. Replace the old struct with the generated one. Update call sites.
5. Verify Prometheus output is identical under v1 mode.

### `cargo xtask gen-metrics --check`

CI runs `gen-metrics --check` to verify generated code is fresh.
This prevents schema/code drift.

## Phase 3: Exponential Histogram Support

### Goal

Introduce exponential histograms as a recording mode, replacing Mmsc
for duration metrics at higher detail levels.

### Dependencies

Vendor or depend on
[rust-expohisto](https://github.com/jmacd/rust-expohisto) for the
exponential histogram data structure.

### Design

In the schema YAML, Mmsc instruments can specify level-dependent
aggregation upgrades:

```yaml
x-otap-levels:
  basic:
    aggregation: mmsc
  normal:
    aggregation: histogram
    histogram_size: 8
  detailed:
    aggregation: histogram
    histogram_size: 16
```

The codegen tool produces a level-aware enum:

```rust
pub enum NodeConsumerDuration {
    Basic(Mmsc),
    Normal(Box<ExponentialHistogram<8>>),
    Detailed(Box<ExponentialHistogram<16>>),
}
```

At Normal and Detailed levels, individual measurements are recorded
into the exponential histogram. OTAP encoding uses
`ExponentialHistogramDataPointsRecordBatchBuilder` from the pdata
crate.

At Basic level, Mmsc continues to use explicit-bucket histogram
encoding with empty boundaries (matching Phase 1 behavior).

## Addendum: Relationship to the OTel Metrics SDK

This design is intended to be a fully compliant OTel Metrics SDK,
albeit one that is OTAP-first and uses compile-time bindings. It
contains all the necessary concepts and component parts:

- **Instruments**: counter, updowncounter, gauge with both
  synchronous (delta add) and asynchronous (cumulative observe)
  variants.
- **Meter / MeterProvider**: the `#[metric_set]` struct serves as the
  Meter (scoped instrument container), and the
  `TelemetryRegistryHandle` serves as the MeterProvider (instrument
  registry with entity/scope correlation).
- **MetricReader**: `MetricsTap` is the periodic reader, with
  `PrometheusExporter` as a pull-based reader and the ITS channel as
  a push-based reader.
- **MetricProducer**: the `visit_metrics_and_reset_with_entity()`
  callback is the production interface.
- **Views**: Phase 1 supports scoped renames via configuration. Phase
  2 replaces this with compile-time level selection, which is a
  restricted but more efficient form of the same concept (attribute
  filtering, aggregation selection, metric renaming).
- **Aggregation**: Sum (counters), LastValue (gauges), and
  ExplicitBucketHistogram/ExponentialHistogram (Mmsc/Phase 3).
- **Temporality**: delta for collection, cumulative for export (via
  `CumulativeAccumulator`).
- **Resource and Scope**: encoded as OTAP Arrow child batches
  (`ResourceAttrs`, `ScopeAttrs`) with precomputed resource and
  per-entity scope attributes.

The key differences from a standard OTel SDK are:

1. **OTAP-first encoding**: metrics are encoded directly into Arrow
   record batches rather than going through an intermediate OTel SDK
   data model.
2. **Compile-time level selection**: three levels instead of arbitrary
   runtime Views.
3. **Schema-driven codegen**: instrument definitions come from YAML
   rather than imperative API calls.
4. **Thread-per-core architecture**: metric sets are per-entity
   structs collected via message passing, not shared-state
   synchronized instruments.

These are implementation choices, not semantic departures. The output
is standard OTAP/OTLP metrics data that any OTel-compatible backend
can consume.
