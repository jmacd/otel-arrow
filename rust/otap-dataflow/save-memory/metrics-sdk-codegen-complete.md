# Metrics SDK Codegen — Final Report

## Summary

This branch implements a codegen-driven OTAP-native metrics SDK for the
otap-dataflow pipeline. Starting from a rough 8-phase plan
(`metrics-plan-v1.md`) that covered the entire OTel SDK replacement, we
scoped down to **counters only** and built a complete vertical slice: schema
definition → code generation → runtime instrumentation → Arrow encoding →
cumulative accumulation → Prometheus-compatible HTTP export.

**13 commits, 23 new files, ~4,270 lines, 31 tests.**

## Architecture

```
  self_metrics.yaml (SemConv YAML + x-otap-levels)
       │
       │  cargo xtask generate-metrics
       │  (MiniJinja template rendering)
       ▼
  generated.rs
  ├── NodeConsumerItems enum (Basic=[Counter;3], Normal=[Counter;9])
  ├── NodeProducerItems enum (same shape)
  ├── add(value, outcome, signal_type) — dimension-indexed
  ├── snapshot() → Vec<u64>
  └── precomputed_schema(level) → PrecomputedMetricSchema
       │
       │  The precomputed schema holds two Arrow RecordBatches
       │  (metrics table + attributes table) built once at init.
       │  Only the NumberDataPoints table is built at runtime.
       │
       ▼
  Collection tick (read-and-reset, delta semantics)
       │
       │  CounterDataPointsBuilder: flat u64 array → Arrow RecordBatch
       │  Minimal 5-column batch: id, parent_id, start_time, time, int_value
       │
       ▼
  CumulativeAccumulator
  ├── BTreeMap<MetricIdentity, RecordBatch>
  │   identity = (schema_key, EntityKey)
  ├── Arrow-native: column-wise add via arrow::compute::kernels::numeric::add
  └── snapshot() → Vec<CumulativeEntry>
       │
       │  Cheap clone (Arc-backed column buffers)
       │
       ▼
  PrometheusExporter (Arc<RwLock<CumulativeAccumulator>>)
       │
       │  GET /metrics → format_openmetrics → OpenMetrics text
       ▼
  HTTP response
```

## Key Design Decisions

### Precomputed-first encoding

For each metric set, OTAP encoding produces 3 tables. Two are fully
determined by the schema:

1. **Metrics table** — metric identity (name, type, unit, temporality,
   monotonic). One row per counter. Built once.
2. **Attributes table** — dimension attributes per data point. One row per
   (point, attr_key) pair. Built once.
3. **NumberDataPoints table** — counter values. Built every collection tick.

Resource and scope columns are omitted from the precomputed batch — they
are contextual and assembled at the receiver/export boundary, mirroring
the ITS logs pattern where scope arrives via EntityKey and resource via
configuration.

### Identity-aware accumulation

The `CumulativeAccumulator` is keyed by `MetricIdentity(schema_key,
EntityKey)`. Multiple pipeline nodes can report the same schema with
different scope attributes. Pointwise addition happens between matching
identities using Arrow compute kernels — no row-by-row map lookups.

### Protometheus exporter (not a pipeline node)

The Prometheus exporter sits alongside the ITS like LogTap — it receives
structured `(identity, snapshot)` pairs directly from the collection path
with full schema information intact. It does NOT sit downstream as a
pipeline node that would need to reassemble schema from OTAP wire format.

### Delta SDK, cumulative on scrape

Counter structs are always delta (read-and-reset on collection tick).
The `CumulativeAccumulator` handles delta→cumulative conversion via
Arrow column-wise addition. On `/metrics` scrape, it clones the current
cumulative batch (cheap ref-counted buffers) and formats as OpenMetrics
text.

### Crate placement

All metrics SDK code lives in `crates/telemetry/src/self_metrics/`,
mirroring how `self_tracing/` handles logs. This avoids circular
dependencies and gives direct access to `EntityKey` from the telemetry
registry.

## Files

### Schema & Codegen

| File | Purpose |
|------|---------|
| `crates/telemetry/self_metrics.yaml` | Pilot schema: consumer/producer counters with outcome × signal_type dimensions |
| `templates/metrics/counter_set.rs.j2` | MiniJinja template for level-aware counter enums + precomputed schema |
| `xtask/src/generate_metrics/mod.rs` | Codegen orchestration: parse YAML → build context → render template |
| `xtask/src/generate_metrics/schema.rs` | YAML parser for SemConv + x-otap-levels extensions |

### Runtime Library (`crates/telemetry/src/self_metrics/`)

| File | Purpose |
|------|---------|
| `generated.rs` | Generated: `NodeConsumerItems`, `NodeProducerItems`, `precomputed_schema()` |
| `dimension.rs` | `Dimension` trait, `Outcome` enum, `SignalType` impl |
| `precomputed.rs` | `PrecomputedMetricSchema`, `CounterDataPointsBuilder` |
| `assembly.rs` | `assemble_metrics_payload()` — combine 3 tables into `OtapArrowRecords` |
| `collector.rs` | `MetricsEncoder` — snapshot → encode, skip all-zeros |
| `collectable.rs` | `CollectableMetrics` trait, `MetricSetCollector` |
| `accumulator.rs` | `CumulativeAccumulator`, `MetricIdentity`, `CumulativeEntry` |
| `openmetrics.rs` | `format_openmetrics()` — OTAP Arrow → OpenMetrics text |
| `prometheus.rs` | `PrometheusExporter` — `Arc<RwLock<CumulativeAccumulator>>` + axum `/metrics` |

### Example

| File | Purpose |
|------|---------|
| `crates/telemetry/examples/prometheus_demo.rs` | 4 simulated nodes, live `/metrics` scraping |

## Commits

| # | Hash | Description |
|---|------|-------------|
| 1 | `b9e79135` | Phases 1–3: codegen, pilot schema, 8 integration tests |
| 2 | `13bf6b1d` | Phase 4: ITS collection path, MetricsEncoder, CollectableMetrics |
| 3 | `908828dd` | Phase 5a: Arrow-native cumulative accumulator |
| 4 | `4c296dc7` | Phase 5b: OpenMetrics exposition formatter |
| 5 | `b4934bcb` | Phase 5c: PrometheusExporter HTTP endpoint |
| 6 | `90916179` | End-to-end prometheus_demo example |
| 7 | `632918ef` | Replace hardcoded constants with proto/type imports |
| 8 | `b30f2f0b` | Omit deprecated resource/scope fields |
| 9 | `38540bcb` | Remove resource/scope from precomputed batches |
| 10 | `2afd9158` | Minimal NDP batch without unused columns |
| 11 | `10c03979` | Use `&mut [u64]` slice for snapshot_into |
| 12 | `3ac7fafc` | Identity-aware cumulative accumulator |
| 13 | `2c9d18ca` | Merge metrics-sdk into crates/telemetry/src/self_metrics/ |

## What's Next

### Immediate (wiring)

- **Wire `PrometheusExporter` into `InternalTelemetryReceiver`** — replace
  the placeholder `collect_otap_metrics` with real collection + ingestion.
  The exporter sits alongside the ITR like LogTap, receiving
  `(MetricIdentity, RecordBatch)` pairs on each telemetry tick.
- **Start HTTP server for `/metrics`** — either reuse the admin/health
  endpoint or spin up a dedicated listener.

### Short-term (scope & resource encoding)

- **Scope encoding** — resolve EntityKey → scope attributes via the
  telemetry registry, encode as OTAP scope columns. The scope identity
  comes from `#[attribute_set]` macros today; eventually from Weaver
  schemas.
- **Resource encoding** — extract from CLI/config/environment, encode as
  OTAP resource columns. Replace the current "custom" field workaround
  with codegen'd resource formation.
- **Schema registration** — each node registers its metric schema +
  entity at pipeline setup, creating `MetricIdentity` entries in the
  accumulator.

### Medium-term (migration)

- **Migrate existing metric sets** — each of the 32+ `#[metric_set]`
  structs gets a YAML schema and generated replacement.
- **Remove OTel SDK metrics path** — drop `MetricsDispatcher`,
  `prometheus` crate, `opentelemetry-prometheus` once all sets are
  migrated.
- **Full Weaver integration** — use Weaver's resolution pipeline for
  attribute validation and SemConv registry generation.

### Longer-term (instrument types)

- **Histogram, gauge, updowncounter support** — extend the template and
  runtime types.
- **Multivariate batches** — multiple metrics sharing a timestamp in a
  single OTAP batch (one column per metric).
- **Weaver-generated entity attribute sets** — replace `#[attribute_set]`
  macros with schema-driven codegen for scope attributes.
