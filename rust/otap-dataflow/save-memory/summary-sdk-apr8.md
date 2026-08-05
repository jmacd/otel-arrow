# OTAP-Native Internal Metrics SDK — April 8 Report

## Summary

This changeset implements a codegen-driven, OTAP-native internal metrics SDK
for the OTAP dataflow pipeline. It replaces the existing OTel SDK bridge path
(`MetricsDispatcher` → OTel SDK → Prometheus) with a direct path: schema-driven
Rust code generation → precomputed OTAP Arrow tables → pipeline injection via
the Internal Telemetry System (ITS).

**12 commits, 18 new/modified source files, ~2,700 lines of new code** (excluding
the vendored `otel-expohisto` crate).

## Motivation

The pipeline's internal metrics previously bridged through the OTel SDK via
`MetricsDispatcher`, which:

- Added heavy dependency weight (`opentelemetry-prometheus`, `prometheus` crate)
- Limited aggregation to what the OTel SDK supports (no exponential histograms,
  no gauge histograms, no per-level dimension control)
- Diverged from the ITS architecture already established for internal logs
- Exported MMSC (min/max/sum/count) as synthetic OTel histograms with no
  bucket boundaries — losing distribution information

The new SDK follows the same ITS pattern as logs: a dedicated engine thread
periodically collects metrics, encodes them as OTAP Arrow record batches, and
injects them into the pipeline through the `InternalTelemetryReceiver`.

## Architecture

```
  metrics_schema.yaml (YAML-first authoring)
       │
       │  cargo xtask generate-metrics
       │  (schema parser → IR → Rust code generation)
       ▼
  self_metrics/generated.rs (checked in)
  ├── NodeConsumer / NodeProducer enums
  │   Basic:    [Counter<u64>; 1] + [Mmsc; 1]
  │   Normal:   [Counter<u64>; 3] + [Histogram<8>; 3]
  │   Detailed: [Counter<u64>; 9] + [Histogram<16>; 9]
  ├── Dimension-indexed API: add_items(value, outcome, signal_type)
  ├── snapshot_into / clear / needs_flush
  └── precomputed_schema(level) → PrecomputedMetricSchema
       │
       │  Engine thread: periodic CollectTelemetry tick
       │  DynMetricSetCollector → snapshot → encode → assemble
       ▼
  OTAP Arrow encoding
  ├── PrecomputedMetricSchema (metrics table + scope attrs, built once)
  ├── NumberDataPointsBuilder (NDP for counters, built per tick)
  ├── edp_encoder (EDP for histograms, built per tick)
  └── assemble_metrics_payload() → OtapArrowRecords::Metrics
       │
       │  InternalTelemetryReceiver.CollectTelemetry
       │  effect_handler.send_message(pdata)
       ▼
  Pipeline (same path as ITS logs)
       │
       └──→ Console exporter (tree-formatted metrics output)
```

## Instrument Model

### Three Archetypes

| Archetype | Semantic | Default Mode |
|-----------|----------|-------------|
| **Counter** | Monotonic accumulation | Counting |
| **Gauge** | Instantaneous value | LastValue |

(UpDownCounter deferred to a future changeset.)

### Recording Modes

| Mode | Counter | Gauge | Implementation |
|------|:-------:|:-----:|----------------|
| **Counting** | ✓ (default) | ✗ | `Counter<u64>` / `Counter<f64>` |
| **Histogram** | ✓ | ✓ | Basic: `Mmsc`, Normal: `Histogram<8>`, Detailed: `Histogram<16>` |
| **LastValue** | ✗ | ✓ (default) | `u64` / `f64` raw values |

The codegen validates archetype × recording mode compatibility at schema
parse time (e.g., "you cannot count a gauge" is a compile-time error).

### Level-Dependent Histogram Implementation

When `recording_mode: histogram`, the aggregator type varies by
`MetricLevel`:

| Level | Type | Memory | Resolution |
|-------|------|--------|------------|
| Basic | `Mmsc` | 32 bytes | min/max/sum/count only |
| Normal | `Histogram<8>` | ~64 bytes | 8 u64 words, ~5% relative error |
| Detailed | `Histogram<16>` | ~128 bytes | 16 u64 words, ~2% relative error |

`Histogram<N>` is the `otel-expohisto` exponential histogram — an
allocation-free, table-lookup-based implementation with sub-byte bucket
counters and automatic scale adjustment.

## Scope-Based Dimension Encoding

**Key architectural decision**: dimension attributes are encoded as OTAP
**scope attributes**, not per-data-point attributes. This means:

1. **Shared dimensions per metric set**: all metrics in a set share the
   same dimension attributes. Each unique dimension combination = one scope.

2. **Precomputed scope table**: at init time, enumerate all dimension
   combinations for the active level. The scope attributes table is fully
   precomputable as a dictionary-encoded Arrow RecordBatch.

3. **No per-data-point attributes**: the `NumberDpAttrs` table is not
   needed. All attributes live in `ScopeAttrs`.

4. **One metric row per metric × scope**: the UnivariateMetrics table has
   `num_metrics × num_scopes` rows, each referencing a scope ID. Dictionary
   encoding means metric names/descriptions/units are stored once.

### Level Progression (pilot schema: 2 metrics)

| Level | Dimensions | Scopes | Metric Rows | NDP/EDP Rows |
|-------|-----------|--------|-------------|-------------|
| Basic | `[]` | 1 | 2 | 2 |
| Normal | `[outcome]` | 3 | 6 | 6 |
| Detailed | `[outcome, signal_type]` | 9 | 18 | 18 |

### Example: Normal level

```
ScopeAttrs (precomputed, 3 rows)
┌───────────┬───────────────┬───────────────┐
│ parent_id │ attribute_key │ attribute_str │
├───────────┼───────────────┼───────────────┤
│         0 │ outcome       │ success       │
│         1 │ outcome       │ failure       │
│         2 │ outcome       │ refused       │
└───────────┴───────────────┴───────────────┘

UnivariateMetrics (precomputed, 6 rows = 2 metrics × 3 scopes)
┌────┬──────────┬─────────────────────┬────────┐
│ id │ scope.id │ name                │ unit   │
├────┼──────────┼─────────────────────┼────────┤
│  0 │        0 │ node.consumer.items │ {item} │
│  1 │        1 │ node.consumer.items │ {item} │
│  2 │        2 │ node.consumer.items │ {item} │
│  3 │        0 │ node.consumer.dur   │ ns     │
│  4 │        1 │ node.consumer.dur   │ ns     │
│  5 │        2 │ node.consumer.dur   │ ns     │
└────┴──────────┴─────────────────────┴────────┘

NumberDataPoints (runtime, 3 rows for items counter)
┌───────────┬───────────┐
│ parent_id │ int_value │
├───────────┼───────────┤
│         0 │        42 │  ← items[success]
│         1 │         3 │  ← items[failure]
│         2 │         0 │  ← items[refused]
└───────────┴───────────┘

ExpHistogramDataPoints (runtime, 3 rows for duration histogram)
┌───────────┬───────┬─────────┬───────┬─────┬─────┬────────────┐
│ parent_id │ count │ sum     │ scale │ min │ max │ pos.counts │
├───────────┼───────┼─────────┼───────┼─────┼─────┼────────────┤
│         3 │   100 │ 5432.1  │     5 │ 1.2 │ 98  │ [1,5,12,…] │
│         4 │    12 │  843.0  │     4 │ 3.1 │ 200 │ [2,3,7]    │
│         5 │     0 │    0.0  │     0 │ 0.0 │ 0.0 │            │
└───────────┴───────┴─────────┴───────┴─────┴─────┴────────────┘
```

The metrics and scope tables are built **once at init** using
`MetricsRecordBatchBuilder` with dictionary-encoded columns. They are
Arc-cloned per collection tick (cheap reference-counted column buffers).
Only the NDP/EDP tables are rebuilt per tick.

## Schema & Codegen Pipeline

### YAML Schema Format

```yaml
attributes:
  - id: outcome
    type: string
    values: [success, failure, refused]

metric_sets:
  - id: node_consumer
    x-otap:
      levels:
        basic:    { dimensions: [] }
        normal:   { dimensions: [outcome] }
        detailed: { dimensions: [outcome, signal_type] }
    metrics:
      - name: node.consumer.items
        instrument: counter
        recording_mode: counting
      - name: node.consumer.duration
        instrument: counter
        recording_mode: histogram
```

Dimensions are declared at the **metric set level** (not per-metric)
because they map to OTAP scopes shared by all metrics in the set.

### Codegen Pipeline

`cargo xtask generate-metrics`:

1. Discovers `metrics_schema.yaml` files under `crates/`
2. Parses YAML → `MetricSchema` (serde)
3. Resolves attribute references → `ResolvedMetricSet` with computed
   `ResolvedLevelLayout` (active dimensions, num_scopes)
4. Validates archetype × recording mode compatibility
5. Generates Rust source → `self_metrics/generated.rs`
6. `--check` mode for CI freshness validation

### Generated Code

For each metric set, the codegen produces:

- **Level-aware enum** with `Basic`/`Normal`/`Detailed` variants, each
  containing appropriately-sized arrays:
  ```rust
  pub enum NodeConsumer {
      Basic    { items: [Counter<u64>; 1], duration: [Mmsc; 1] },
      Normal   { items: [Counter<u64>; 3], duration: [Histogram<8>; 3] },
      Detailed { items: [Counter<u64>; 9], duration: [Histogram<16>; 9] },
  }
  ```

- **Constructor**: `new(level: MetricLevel) → Self`

- **Dimension-indexed API**: `add_items(value, outcome, signal_type)` uses
  row-major index arithmetic (`outcome * SIGNAL_TYPE_CARDINALITY + signal_type`)
  and ignores unused dimensions at lower levels.

- **Recording mode dispatch**: `record_duration(value, outcome, signal_type)`
  calls `Mmsc::record()` at Basic, `Histogram::update()` at Normal/Detailed.

- **Snapshot/clear/needs_flush**: for the collection lifecycle.

- **`precomputed_schema(level)`**: returns a `PrecomputedMetricSchema` with
  dictionary-encoded Arrow RecordBatches built from const metadata arrays.

## EDP Encoder

The `edp_encoder` module encodes histograms into OTAP
`ExponentialHistogramDataPoints` record batches:

- **`encode_histograms<N>()`**: walks `HistogramView::positive().iter()` in
  a single pass, collects bucket counts as `List(UInt64)`, computes
  `zero_count = count - Σbuckets`, maps to EDP columns.

- **`encode_mmsc()`**: encodes `MmscSnapshot` as degenerate EDP with
  `scale=0`, no bucket data, `sum/count/min/max` populated. This gives
  uniform EDP encoding across all histogram levels.

Both use `ExponentialHistogramDataPointsRecordBatchBuilder` from `pdata`
with `BucketsRecordBatchBuilder` for the positive/negative bucket structs.

## ITS Integration

The `CollectTelemetry` handler in `InternalTelemetryReceiver` now:

1. Locks the `otap_metrics_collectors` mutex
2. Calls `collect(start_time_ns, time_ns)` on each registered
   `DynMetricSetCollector`
3. Assembles non-empty `EncodedMetrics` into `OtapArrowRecords::Metrics`
   via `assemble_metrics_payload()`
4. Sends into the pipeline via `effect_handler.send_message()`

`DynMetricSetCollector` is a trait-object-safe wrapper over the generic
`MetricSetCollector<M>`, enabling heterogeneous metric set types in a
single `Vec<Box<dyn DynMetricSetCollector>>`.

The collectors vec lives on `InternalTelemetrySettings` behind
`Arc<Mutex<...>>`, shared between the code that registers metric sets
(at pipeline setup) and the ITR that collects them (on each tick).

## Console Exporter

The console exporter's `export_metrics` method now handles both payload
formats using the `MetricsView` trait hierarchy:

- **OTLP protobuf bytes**: `RawMetricsData` (zero-copy view)
- **OTAP Arrow records**: `OtapMetricsView`

Generic `print_metrics_data<M: MetricsView>()` formats output as
hierarchical tree lines matching the logs style:

```
                  RESOURCE   v1.Resource
│ SCOPE    v1.InstrumentationScope [outcome=success]
│ ├─ SUM node.consumer.items ({item})
│ │    42
│ └─ EXPHISTO node.consumer.duration (ns)
│      n=100 sum=5432.100 min=1.200 max=98.700 s=5 z=3 +[off=-2,n=[1,5,12,...]]
```

Supports Sum, Gauge, ExponentialHistogram data types with inline
`[key=value]` attribute display.

## otel-expohisto Integration

The `otel-expohisto` crate (from `github.com/jmacd/rust-expohisto`) has
been vendored into `crates/expohisto/` with workspace adaptations:

- `build.rs` removed; lookup tables checked in as `src/generated/`
  (following the xtask pattern — no build-time code generation)
- Workspace edition/rust-version
- Added as workspace dependency with `scale-8` feature (256 log buckets,
  2KB lookup table)

Key properties: allocation-free, `Copy`/`Send`/`Sync`, sub-byte bucket
counters (B1 width = 1024 one-bit buckets in 16 u64 words), automatic
scale adjustment, `no_std` compatible, zero `unsafe`.

## Files

### Runtime Library (`crates/telemetry/src/self_metrics/`)

| File | Lines | Purpose |
|------|------:|---------|
| `mod.rs` | 31 | Module root, doc comments |
| `dimension.rs` | 112 | `Dimension` trait + `Outcome`/`SignalType` impls |
| `precomputed.rs` | 309 | `PrecomputedMetricSchema::build()`, `NumberDataPointsBuilder`, `MetricInfo`, `DimensionInfo` |
| `collector.rs` | 166 | `CollectableMetrics` trait, `MetricsEncoder`, `MetricSetCollector`, `DynMetricSetCollector` |
| `edp_encoder.rs` | 188 | `encode_histograms<N>()`, `encode_mmsc()` → EDP RecordBatch |
| `assembly.rs` | 41 | `assemble_metrics_payload()` → `OtapArrowRecords::Metrics` |
| `generated.rs` | 396 | Generated: `NodeConsumer`, `NodeProducer` enums + APIs |

### Codegen (`xtask/src/generate_metrics/`)

| File | Lines | Purpose |
|------|------:|---------|
| `ir.rs` | 207 | IR types: `Archetype`, `RecordingMode`, `MetricSetDef`, `ResolvedLevelLayout` |
| `schema.rs` | 270 | YAML parser, attribute resolution, archetype×mode validation |
| `mod.rs` | 648 | Code generator, `cargo xtask generate-metrics` entry point |

### Modified Files

| File | Change |
|------|--------|
| `crates/telemetry/src/lib.rs` | Added `self_metrics` module, `otap_metrics_collectors` field on `InternalTelemetrySettings` |
| `crates/telemetry/Cargo.toml` | Added `arrow` and `otel-expohisto` deps |
| `crates/core-nodes/.../internal_telemetry_receiver/mod.rs` | `CollectTelemetry` handler wired to iterate collectors and inject metrics |
| `crates/core-nodes/.../console_exporter/mod.rs` | Full metrics formatting for both OTLP and OTAP payloads |
| `xtask/Cargo.toml` | Added `serde_yaml` dep |
| `xtask/src/main.rs` | Added `generate-metrics` subcommand |
| `Cargo.toml` | Added `otel-expohisto` workspace dep, excluded expohisto sub-crates |

## What's Next

### Immediate (Phase 5c, 7a)

- **Engine thread wiring**: register metric sets with the telemetry
  registry at pipeline setup, connecting concrete `NodeConsumer`/
  `NodeProducer` instances to the collectors vec.
- **Pilot integration test**: create a `NodeConsumer` at Normal level,
  record values, collect, verify the assembled OTAP payload.

### Short-term (Phase 6)

- **Cumulative accumulator**: Arrow-native delta→cumulative for Prometheus
- **OpenMetrics formatter**: walk OTAP batches → OpenMetrics text
- **Prometheus HTTP endpoint**: replace `prometheus_exporter_provider.rs`

### Medium-term (Phase 7)

- **Migrate all 30+ `#[metric_set]` structs** to YAML schemas
- **Remove OTel SDK metrics path**: drop `MetricsDispatcher`,
  `prometheus`/`opentelemetry-prometheus` crate dependencies
- **Views**: rename/description overrides already supported in schema;
  attribute dimension control is the MetricLevel mechanism itself

### Longer-term

- **Multivariate OTAP encoding**: collapse `num_metrics × num_scopes`
  rows to `num_scopes` rows (one per scope, multiple metric values)
- **Gauge histograms**: await `otel-expohisto` pos/neg bucket extension
- **UpDownCounter, splitcount, sample modes**: deferred from initial scope
- **Weaver integration**: schema validation against SemConv registry
