# OTAP Metrics SDK — Project Retrospective

## Where We Started

The original `metrics-plan-v1.md` was an ambitious 8-phase plan to replace the
entire OTel SDK metrics stack with a codegen-driven OTAP-native instrumentation
system. It covered counters, histograms, gauges, updowncounters, MMSC
aggregation, Histogram\<N\> instruments, Weaver SemConv integration, per-crate
migration of 32+ metric sets, OTel SDK removal, and Weaver CI validation. It
was, as noted, "rougher than the author realized" — effectively proposing a
radical new OTel SDK project.

## What We Scoped

Through the planning conversation, we narrowed dramatically:

- **Counters only** — no histograms, gauges, or updowncounters
- **Univariate OTAP encoding** — multivariate is a future phase
- **New crate isolation** — all new code in `crates/metrics-sdk/` and
  `xtask/src/generate_metrics/`, minimal touches to existing crates
- **Protometheus exporter** — instead of routing through the existing
  `temporal_reaggregation_processor` (which would double the stateful maps), we
  designed a single-component Arrow-native accumulator that is both the
  cumulative store and the Prometheus endpoint

The key insight that emerged during planning: for each metric set, OTAP
encoding produces 3 tables, and **2 of the 3 are completely determined by the
schema** — the metrics table and attributes table can be precomputed at init
time as const Arrow RecordBatches. Only the NumberDataPoints table is built at
runtime. This dramatically simplifies the hot path.

## What We Built

**6 commits, 26 files, ~3,760 lines, 33 tests.**

### Phase 1: Runtime Library (`crates/metrics-sdk/`)

The foundation types that generated code targets:

- **`Dimension` trait** — bounded enum types with compile-time cardinality,
  attribute key/value mapping, and index functions. Implemented for `Outcome`
  (success/failure/refused) and `SignalType` (traces/metrics/logs).
- **`PrecomputedMetricSchema`** — builds the metrics and attributes Arrow
  RecordBatches once at init time using the existing
  `MetricsRecordBatchBuilder` and `AttributesRecordBatchBuilder` from
  `otap-df-pdata`. The schema is fully determined by the counter definitions
  and dimension cardinalities.
- **`CounterDataPointsBuilder`** — builds the NumberDataPoints RecordBatch at
  runtime from a flat array of u64 counter values. Precomputes parent_ids so
  the hot path is just: fill in values → `finish()`.
- **`assemble_metrics_payload`** — combines the 3 tables into
  `OtapArrowRecords::Metrics(...)`.

### Phase 2: Schema & Codegen

- **Schema YAML** — `crates/telemetry/self_metrics.yaml` defines the
  consumer/producer counters using OTel SemConv YAML format with
  `x-otap-levels` extensions for level-aware dimensioning.
- **Schema parser** — `xtask/src/generate_metrics/schema.rs` parses the YAML,
  resolves attribute groups, and produces a code-generation-ready IR.
- **MiniJinja template** — `templates/metrics/counter_set.rs.j2` generates
  level-aware counter enums, dimension-indexed `add()` methods, `snapshot()`,
  `clear()`, and the `precomputed_schema()` function.
- **`cargo xtask generate-metrics`** — orchestrates parsing + rendering, writes
  checked-in `.rs` files.

**Plan deviation**: The plan called for using the Weaver Forge `TemplateEngine`
API directly. In practice, the Forge API is designed for CLI workflows with
complex config/file-loader setup. We used `minijinja` directly (the same engine
Weaver uses internally) for template rendering, while keeping the schema format
Weaver-compatible. The `weaver_*` crate dependencies were not added to xtask —
just `minijinja` and `serde_yaml`. This is a pragmatic simplification that can
evolve toward full Weaver Forge integration.

### Phase 3: Pilot

The generated code for `NodeConsumerItems` and `NodeProducerItems`:

- **Basic level**: `[Counter<u64>; 3]` — 3 data points indexed by outcome
- **Normal level**: `[Counter<u64>; 9]` — 9 data points indexed by
  `outcome.index() * 3 + signal_type.index()`
- **Detailed**: reuses the Normal variant (same dimensions, deduped)

The `new(MetricLevel)` constructor selects the right variant. The
`add(value, outcome, signal_type)` method uses match + index arithmetic —
zero-overhead for the Basic variant (signal_type is ignored). 8 integration
tests cover the full lifecycle.

### Phase 4: ITS Wiring

- **`MetricsEncoder`** — holds a `PrecomputedMetricSchema`, encodes counter
  snapshots into `OtapArrowRecords`, skips all-zero snapshots.
- **`CollectableMetrics` trait** — bridge between generated structs and the
  collection path (`snapshot_into` / `clear`).
- **`MetricSetCollector`** — pairs counters with encoder, provides atomic
  snapshot-encode-clear.
- **`InternalTelemetrySettings`** gained `otap_metrics_collectors` field.
- **`InternalTelemetryReceiver::CollectTelemetry`** handler wired to call
  `collect_otap_metrics` (placeholder for per-node counter struct connections).

### Phase 5: Protometheus Exporter

The plan's most important architectural decision — single-component accumulator
\+ exporter:

- **`CumulativeAccumulator`** — Arrow-native delta→cumulative. Stores one
  NumberDataPoints RecordBatch. On each delta:
  `arrow::compute::kernels::numeric::add` on the `int_value` column, update
  `time_unix_nano`. No row-by-row map lookups — vectorized column-wise
  addition.
- **`format_openmetrics`** — walks the OTAP Arrow batch structure (metrics rows
  → data points via `parent_id` → attributes via `dp id`), handles
  dictionary-encoded columns from the adaptive Arrow builders, emits standard
  OpenMetrics text.
- **`PrometheusExporter`** — `Arc<RwLock<CumulativeAccumulator>>` with
  `ingest_delta()` and an axum `GET /metrics` handler. On scrape: `read()`
  lock, clone batch (cheap ref-counted), format, respond.

### End-to-End Demo

`examples/prometheus_demo.rs` — 4 simulated pipeline nodes, each with
consumer/producer counters at Normal level. Every second: simulate random
traffic → snapshot → encode delta → ingest → serve. Verified live with
`curl localhost:9464/metrics` showing cumulative counters with proper
`{outcome="success",signal_type="logs"}` labels growing over time.

## What Changed vs. the Plan

| Plan Said | What Happened | Why |
|-----------|---------------|-----|
| Use Weaver Forge `TemplateEngine` API | Used `minijinja` directly | Forge API requires complex `WeaverConfig`/`FileLoader` setup; same engine underneath |
| Add `weaver_*` deps to xtask | Added `minijinja` + `serde_yaml` to xtask | Simpler, schema format remains Weaver-compatible |
| 3 separate template files | 1 template (`counter_set.rs.j2`) | All generation logic fits naturally in one template |
| `Outcome` impl on `RequestOutcome` from engine crate | New `Outcome` enum in metrics-sdk | Avoids coupling to engine crate's internal type |
| `PrecomputedMetricSchema` in `crates/pdata/` | In `crates/metrics-sdk/` | Keeps everything in the new crate |
| Plan listed `precomputed_schema.rs.j2` and `encoder.rs.j2` as separate templates | Combined into `counter_set.rs.j2` | The precomputed_schema function and encoder logic are small enough for one template |

## Open Items for Future Work

1. **Wire counter structs into the ITS receiver** — `collect_otap_metrics` is a
   placeholder. Each migrated metric set needs its counter struct registered and
   paired with a `MetricSetCollector`.
2. **Migrate existing 32+ metric sets** — each `#[metric_set]` struct gets a
   YAML schema and generated replacement.
3. **Remove OTel SDK metrics path** — once all metric sets are migrated, drop
   `MetricsDispatcher`, `prometheus` crate, `opentelemetry-prometheus`.
4. **Full Weaver integration** — use Weaver's resolution pipeline for attribute
   validation, generate SemConv registry YAML for CI.
5. **SemConv registry as submodule** — share standard attribute definitions
   instead of inline YAML.
6. **Resource/scope injection** — currently hardcoded; needs to come from
   pipeline identity at runtime.
7. **Histogram/gauge/updowncounter support** — extend the template and runtime
   types.
8. **Multivariate batches** — the plan's "multivariate OTAP batch" idea was
   deferred; current encoding is univariate (one metric per OTAP row, multiple
   data points per metric).
