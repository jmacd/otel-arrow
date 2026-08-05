# OTAP-Native Internal Metrics SDK — Implementation Plan

## Problem Statement

The otap-dataflow pipeline's internal metrics currently bridge through the
OTel SDK via `MetricsDispatcher`. This adds dependency weight, limits
aggregation flexibility (no exponential histograms, no gauge histograms,
no per-level dimension control), and diverges from the ITS architecture
already established for internal logs.

## Vision

Build a codegen-driven, OTAP-native internal metrics SDK that:

1. Mirrors the ITS logs pattern: dedicated engine thread, periodic
   collection, OTAP encoding, pipeline injection via
   `InternalTelemetryReceiver`.
2. Uses a YAML-first authoring model: metric schemas drive codegen that
   produces Rust metric set structs, recording mode implementations,
   dimension-indexed APIs, and OTAP encoders.
3. Introduces a refined instrument model with three archetypes (Counter,
   UpDownCounter, Gauge) and five recording modes (counting, histogram,
   sample, lastvalue, splitcount).
4. Goes off-spec for Views: views are baked into the YAML schema at build
   time; the only runtime knob is `MetricLevel = Basic | Normal | Detailed`.
5. Replaces `Mmsc` with exponential histograms (`otel-expohisto` crate)
   at Normal/Detailed levels, keeping MMSC as a cheap Basic-level
   histogram implementation.
6. Encodes metrics as multivariate OTAP Arrow batches keyed by EntityKey.

## Encoding Obstacles & Workarounds

Several recording modes produce data that has no first-class representation
in the OTel/OTAP metric type system (`MetricType` = Empty | Gauge | Sum |
Histogram | ExponentialHistogram | Summary). These require workarounds:

### 1. Gauge Histogram (no OTel type)

OTel defines no GaugeHistogram metric type. When recording_mode=histogram
is used with a Gauge archetype (distribution of gauge values over an
interval), there is no natural OTAP encoding.

**Workaround**: Encode as `MetricType::ExponentialHistogram` with
`aggregation_temporality = DELTA` and `is_monotonic = false`. Use a
metric metadata attribute (e.g., `otel.metric.archetype = "gauge"`) or
a naming convention to signal gauge-histogram semantics. This allows
the data to flow through the OTAP pipeline and be exported. Downstream
systems that understand the marker can interpret correctly; others see
a valid exponential histogram.

**Future**: Propose GaugeHistogram as a metric type to the OTel spec.

### 2. UpDownCounter & Gauge Histogram (pos/neg values)

The `otel-expohisto` crate currently supports positive values only.
The plan is to **extend `otel-expohisto` with pos/neg bucket support**
as an option, matching the OTel ExponentialHistogram data model which
defines both `positive` and `negative` bucket ranges.

**For Counter histogram**: Only positive increments are possible, so
these use a **positive-only `Histogram<N>`** (current `otel-expohisto`).
Encoded as standard EDP with only positive buckets populated.

**For UpDownCounter histogram** (when not using splitcount mode):
Use a **pos/neg-capable `Histogram<N>`** that populates both the
positive and negative bucket ranges of a single EDP row. This gives
a natural encoding as one `MetricType::ExponentialHistogram` data
point per timeseries with both bucket ranges active.

**For Gauge histogram**: Gauge values can be positive or negative.
Use a **pos/neg-capable `Histogram<N>`** with both bucket ranges.
Encoded as a single EDP row.

The pos/neg histogram variant will be a compile-time option in
`otel-expohisto` (e.g., `Histogram<N, Signed>` vs
`Histogram<N, Unsigned>`), roughly doubling memory for the signed
case. Since counter histograms (the common case) remain unsigned,
memory impact is limited to UDC/gauge histograms which are rarer.

### 3. MMSC at Basic level

MMSC (min/max/sum/count) is a degenerate histogram. Current code
encodes it as a synthetic OTel Histogram with no bucket boundaries.

**Encoding**: Use `MetricType::ExponentialHistogram` with scale=0
and zero bucket counts. The EDP format natively carries sum, count,
min, max — exactly the MMSC fields. This gives uniform encoding
across histogram levels (all EDP, just different resolutions).

### 4. Sample mode output

A sampled value (one random measurement per interval) has no
specific OTel metric point type.

**Encoding**: For Counter/UpDownCounter sample: encode as a
`MetricType::Gauge` data point with the sampled value (it's a
point-in-time snapshot of a single measurement, not a sum).
For Gauge sample: same, `MetricType::Gauge`.

### 5. Splitcount output

Two separate counters (pos increments, neg increments) for
UpDownCounter.

**Encoding**: Two `MetricType::Sum` data points per timeseries,
differentiated by `direction = "pos"` / `direction = "neg"`.
Each is monotonic within its direction. The original signed
sum can be reconstructed as `pos.sum - neg.sum`.

## Scope-Based Dimension Encoding

**Key architectural decision** (per OTAP/OTel-Arrow architect):

Dimension attributes (outcome, signal_type, etc.) are encoded as **scope
attributes** rather than per-data-point attributes. This means:

1. **Shared dimensions per metric set**: All metrics in a metric_set share
   the same dimension attributes. Dimensions are declared at the set level,
   not per-metric. Each unique combination of dimension values becomes a
   separate scope ID.

2. **Precomputed scope table**: At init time, enumerate all dimension
   combinations for the active MetricLevel. For Basic with [outcome]:
   3 scopes. For Normal with [outcome, signal_type]: 9 scopes. The scope
   attributes table is fully precomputable.

3. **No per-data-point attributes**: The NumberDpAttrs table is not needed.
   All attributes live in ScopeAttrs. This is cheaper at encoding time and
   avoids redundant attribute storage.

4. **One metric row per metric × scope**: The UnivariateMetrics table has
   one row per (metric, scope) pair. With 2 metrics × 9 scopes = 18 rows.
   Each metric row references a scope ID. Each has exactly 1 NDP row.

5. **Multivariate efficiency**: Within a scope, multiple metrics share the
   same timestamp and scope attributes. This is the natural grouping for
   OTAP multivariate encoding when it arrives.

### Example: Normal level (outcome × signal_type = 9 scopes)

```
ScopeAttrs (precomputed, 18 rows = 9 scopes × 2 attrs each)
┌───────────┬───────────────┬───────────────┐
│ parent_id │ attribute_key │ attribute_str │
├───────────┼───────────────┼───────────────┤
│         0 │ outcome       │ success       │
│         0 │ signal_type   │ logs          │
│         1 │ outcome       │ success       │
│         1 │ signal_type   │ metrics       │
│       ... │ ...           │ ...           │
│         8 │ outcome       │ refused       │
│         8 │ signal_type   │ traces        │
└───────────┴───────────────┴───────────────┘

UnivariateMetrics (precomputed, 18 rows = 2 metrics × 9 scopes)
┌────┬─────────────┬─────────────────────┬──────────────┐
│ id │ metric_type │ name                │ scope.id     │
├────┼─────────────┼─────────────────────┼──────────────┤
│  0 │ Sum         │ node.consumer.items │            0 │
│  1 │ Sum         │ node.consumer.items │            1 │
│ ...│ ...         │ ...                 │          ... │
│  9 │ ExpHisto    │ node.consumer.dur   │            0 │
│ 10 │ ExpHisto    │ node.consumer.dur   │            1 │
│ ...│ ...         │ ...                 │          ... │
└────┴─────────────┴─────────────────────┴──────────────┘

NumberDataPoints (runtime, 18 rows — 1 per metric row)
┌───────────┬───────────┐
│ parent_id │ int_value │
├───────────┼───────────┤
│         0 │        42 │  ← items[success,logs]
│         1 │        17 │  ← items[success,metrics]
│       ... │       ... │
└───────────┴───────────┘
```

### Schema YAML impact

Dimensions move from per-metric to per-metric-set:

```yaml
metric_sets:
  - id: node_consumer
    x-otap:
      levels:
        basic:
          dimensions: [outcome]
        normal:
          dimensions: [outcome, signal_type]
        detailed:
          dimensions: [outcome, signal_type]
    metrics:
      - name: node.consumer.items
        instrument: counter
        ...
      - name: node.consumer.duration
        instrument: counter
        recording_mode: histogram
        ...
```

## Design

### Instrument Model

Three archetypes — "things you count, things you total, things you average":

| Archetype | Semantic | Default Mode | Interface Styles |
|-----------|----------|-------------|-----------------|
| **Counter** | Monotonic accumulation | Counting | Delta (`add`) or Cumulative (`observe`) |
| **UpDownCounter** | Signed accumulation | Counting | Delta (`add`/`sub`) or Cumulative (`observe`) |
| **Gauge** | Instantaneous value | LastValue | `set` only |

### Recording Modes

Each instrument has a default recording mode; the schema can override it.

| Recording Mode | Counter | UpDownCounter | Gauge | Meaning |
|---------------|:-------:|:-------------:|:-----:|---------|
| **Counting** | ✓ (default) | ✓ (default) | ✗ | Temporal sum aggregation |
| **Histogram** | ✓ | ✓ (pos/neg) | ✓ | Full distribution. Level-dependent impl: MMSC (Basic), ExpoHisto (Normal/Detailed) |
| **Sample** | ✓ | ✓ | ✓ | Reservoir-sample 1 random value per interval |
| **LastValue** | ✗ | ✗ | ✓ (default) | Keep latest value + timestamp |
| **Splitcount** | ✗ | ✓ | ✗ | Two separate counters for positive and negative increments |

### Interface Styles (orthogonal to recording mode)

- **Delta/Increment**: Caller reports changes. `counter.add(5)`,
  `updown.add(-3)`. The SDK accumulates deltas.
- **Cumulative/Total**: Caller reports current observed value.
  `counter.observe(total_bytes)`. The SDK computes deltas internally.
  Maps to the existing `ObserveCounter`/`ObserveUpDownCounter` pattern.
- **Gauges** use `set()` only — they're inherently instantaneous.

### Histogram Implementation by Level

When `recording_mode: histogram`:

| Level | Implementation | Memory | Resolution |
|-------|---------------|--------|------------|
| Basic | MMSC (min/max/sum/count) | 32 bytes | Summary only |
| Normal | `Histogram<8>` (ExpoHisto, 8 words) | ~64 bytes | ~5% relative error |
| Detailed | `Histogram<16>` (ExpoHisto, 16 words) | ~128 bytes | ~2% relative error |

For UpDownCounter histogram: two instances (one positive, one negative).
For Gauge histogram: distribution of gauge values observed during interval.

### Views (Off-Spec)

Views are baked into the YAML metric schema at build time. No runtime View
registration. The schema defines per-level behavior:

```yaml
groups:
  - id: metric.node.consumer.items
    type: metric
    metric_name: node.consumer.items
    instrument: counter           # archetype
    unit: "{item}"
    brief: "Items consumed."
    attributes:
      - ref: outcome
      - ref: signal_type
    x-otap:
      interface: delta            # delta or cumulative
      recording_mode: counting    # default for counter, explicit here
      levels:
        basic:
          dimensions: [outcome]
        normal:
          dimensions: [outcome, signal_type]
        detailed:
          dimensions: [outcome, signal_type]
      view:                       # optional OTel-style view overrides
        name: "consumer.items"    # rename
        # attribute_keys: ...     # additional filtering beyond dimensions
```

View features supported at build time:
- **Rename** metrics (name override)
- **Description** override
- **Attribute dimension control** (which dims active at each level)
- **Aggregation/recording mode** selection per level
- **Drop** (disable metric entirely)

Runtime choice: `MetricLevel::Basic | Normal | Detailed` — selects the
pre-built variant.

### Architecture

```
  metric_schema.yaml (per-crate, YAML-first authoring)
       │
       │  cargo xtask generate-metrics
       │  (schema parser → MiniJinja templates → Rust)
       ▼
  self_metrics/generated.rs (per-crate)
  ├── Level-aware metric set enums
  │   (Basic variant: [Counter; 3], Normal variant: [[Counter; 3]; 3])
  ├── Recording mode aggregators
  │   (Histogram<N>, MMSC, SampleReservoir, SplitCount, ...)
  ├── Dimension-indexed API: add(value, outcome, signal_type)
  ├── snapshot_into(&mut [u8]) + clear()
  └── precomputed_schema(level) → PrecomputedMetricSchema
       │
       │  Engine thread: periodic CollectTelemetry tick
       │  MetricsReporter::report() → snapshot → encode
       ▼
  OTAP Arrow encoding
  ├── PrecomputedMetricSchema (metrics + attrs tables, built once)
  ├── DataPointsBuilder (NDP / HDP / EDP tables, built per tick)
  └── assemble → OtapPdata::Metrics(...)
       │
       │  InternalTelemetryReceiver
       ▼
  Pipeline injection (same path as ITS logs)
       │
       ├──→ OTAP export (standard pipeline path)
       └──→ Prometheus /metrics (CumulativeAccumulator + OpenMetrics)
```

### Crate Placement

All runtime library code lives in `crates/telemetry/src/self_metrics/`,
parallel to the existing `self_tracing/` module. This avoids circular
dependencies and gives direct access to `EntityKey`, `MetricLevel`,
`Interests`, and the telemetry registry.

Codegen lives in `xtask/src/generate_metrics/`.
Templates live in `templates/metrics/`.

## Phased Implementation

### Phase 1: Foundation Types

Hand-written runtime types that generated code will target.

**1a: Recording mode aggregator types**

New module `crates/telemetry/src/self_metrics/aggregators.rs`:

- `MmscAggregator` — reuse existing `Mmsc` (min/max/sum/count)
- `SampleReservoir` — reservoir sampling of 1 value (random replacement)
- `SplitCount<T>` — two counters {positive, negative} for UpDownCounter

And add `otel-expohisto` as a workspace dependency for `Histogram<N>`.
The crate will be extended upstream with pos/neg bucket support
(`Histogram<N, Signed>` vs `Histogram<N, Unsigned>`) for UDC and
gauge histogram use cases. Counter histograms use the existing
positive-only variant.

**1b: Dimension trait**

`crates/telemetry/src/self_metrics/dimension.rs`:

```rust
pub trait Dimension: Copy + 'static {
    const CARDINALITY: usize;
    const ATTRIBUTE_KEY: &'static str;
    const ATTRIBUTE_VALUES: &'static [&'static str];
    fn index(self) -> usize;
}
```

Implement for `Outcome` (success/failure/refused) and `SignalType`
(logs/metrics/traces) — defined locally to avoid coupling.

**1c: Precomputed OTAP schema types**

`crates/telemetry/src/self_metrics/precomputed.rs`:

- `PrecomputedMetricSchema` — holds precomputed metrics table and
  attributes table as Arrow RecordBatches, plus layout metadata
  (points per metric, total points).
- `DataPointsBuilder` — builds NDP (NumberDataPoints), HDP
  (HistogramDataPoints), or EDP (ExponentialHistogramDataPoints)
  RecordBatch from snapshot values. Precomputes parent_ids.

**1d: Collection + encoding bridge**

`crates/telemetry/src/self_metrics/collector.rs`:

- `CollectableMetrics` trait — snapshot_into / clear
- `MetricsEncoder` — pairs precomputed schema with snapshot → OtapPdata
- `MetricSetCollector` — atomic snapshot-encode-clear cycle

**1e: OTAP assembly**

`crates/telemetry/src/self_metrics/assembly.rs`:

- `assemble_metrics_payload()` — combine 3 tables (metrics, data points,
  attributes) into `OtapArrowRecords::Metrics(...)`.

### Phase 2: Schema Format & Parser

**2a: YAML schema format design**

Design the SemConv-compatible YAML format with `x-otap` extensions:

- Archetype (`instrument: counter | updowncounter | gauge`)
- Interface style (`interface: delta | cumulative`)
- Recording mode (`recording_mode: counting | histogram | sample |
  lastvalue | splitcount`)
- Per-level configuration (`levels.basic/normal/detailed`):
  - `dimensions: [attr_list]`
  - `histogram_size: N` (for expohisto word count)
  - `recording_mode_override: ...` (e.g., histogram at detailed only)
- View overrides (`view.name`, `view.description`)
- Shared attribute group definitions (`x-otap-attributes`)

**2b: Schema parser**

`xtask/src/generate_metrics/schema.rs`:

- Parse YAML into an intermediate representation (IR)
- Validate archetype × recording mode compatibility
- Resolve attribute references
- Compute per-level data point counts, array sizes, boxing decisions

**2c: Codegen IR types**

`xtask/src/generate_metrics/ir.rs`:

- `MetricSetDef`, `MetricDef`, `DimensionDef`, `LevelConfig`
- `RecordingMode`, `Archetype`, `InterfaceStyle` enums
- Computed fields: variant array sizes, box thresholds (>64 bytes → Box)

### Phase 3: Code Generation

**3a: MiniJinja templates**

`templates/metrics/metric_set.rs.j2`:

Generate per-metric-set:
- Level-aware enum with variants (Basic/Normal/Detailed)
- Per-variant field storage (arrays sized by dimension cardinality × recording mode)
- `new(level: MetricLevel)` constructor
- Dimension-indexed `add`/`record`/`set` API methods
- `snapshot_into(&mut [AggregatorSnapshot])` + `clear()`
- `precomputed_schema(level)` → builds PrecomputedMetricSchema

`templates/metrics/dimension.rs.j2`:

Generate per-attribute:
- `Dimension` trait impl for each attribute enum

**3b: xtask generate-metrics command**

`xtask/src/generate_metrics/mod.rs`:

- Discover `**/metrics_schema.yaml` files in workspace
- Parse each schema → IR
- Render templates → `self_metrics/generated.rs` per crate
- `--check` mode for CI freshness validation
- Add `minijinja` + `serde_yaml` deps to xtask

Wire into `xtask/src/main.rs` as a new subcommand.

### Phase 4: OTAP Encoding

**4a: Number data point encoding** (counting, lastvalue, sample, splitcount)

Generated encoder fills `NumberDataPointsBuilder` from counter/gauge
snapshot arrays. Encoding rules per recording mode:

- **Counting** → `MetricType::Sum`, standard NDP with int/double value
- **LastValue** → `MetricType::Gauge`, NDP with latest value
- **Sample** → `MetricType::Gauge`, NDP with sampled value
- **Splitcount** → Two `MetricType::Sum` NDP rows per timeseries,
  one for positive increments (monotonic), one for negative (monotonic),
  differentiated by `direction` attribute (see Encoding Obstacles §5)

**4b: Histogram data point encoding** (histogram recording mode)

All histogram levels encode as `MetricType::ExponentialHistogram` for
uniformity (see Encoding Obstacles §3):

- **Basic (MMSC)** → EDP with scale=0, zero bucket counts, sum/count/
  min/max populated. Degenerate but schema-compatible.
- **Normal/Detailed (ExpoHisto)** → Full EDP: map `Histogram<N>::view()`
  to OTAP EDP columns (scale, zero_count, positive buckets offset+counts,
  sum, count, min, max).
- **Counter histogram** → Uses positive-only `Histogram<N>`. Standard
  EDP with only positive buckets.
- **UpDownCounter histogram** (non-split) → Uses pos/neg-capable
  `Histogram<N>`. Single EDP row with both positive and negative
  bucket ranges populated.
- **Gauge histogram** → Uses pos/neg-capable `Histogram<N>`. Single
  EDP row with `aggregation_temporality = DELTA`,
  `is_monotonic = false`, plus archetype metadata attribute (see
  Encoding Obstacles §1).

**4c: Precomputed schema construction**

Generated `precomputed_schema()` function builds:
1. Metrics table — one row per metric (name, type, unit, temporality,
   monotonic). Uses view name override if configured. For splitcount
   and UDC histogram modes, two metric rows per logical metric.
2. Attributes table — one row per (data_point, attribute_key) pair for
   dimension attributes. Includes synthetic `direction` attribute for
   splitcount mode, and `otel.metric.archetype` for gauge histogram.

The metrics table is precomputed at init, const after. Note that
splitcount is the only mode that produces two metric rows per logical
metric (pos/neg counters). UDC and gauge histograms encode as single
EDP rows with both pos/neg bucket ranges.

### Phase 5: ITS Integration

**5a: InternalTelemetrySettings extension**

Add `otap_metrics_collectors: Vec<Box<dyn MetricSetCollector>>` to
`InternalTelemetrySettings` (parallel to `logs_receiver`).

**5b: InternalTelemetryReceiver metrics collection**

Extend the `CollectTelemetry` handler in `InternalTelemetryReceiver`
to iterate `otap_metrics_collectors`, call `collect()` on each, and
inject the resulting `OtapPdata::Metrics(...)` into the pipeline via
`effect_handler.send_message()`.

**5c: Engine thread wiring**

The engine thread already sends `CollectTelemetry` events on telemetry
timer ticks. Wire the new metric set structs:
1. Each pipeline node's metric set is registered with the telemetry
   registry alongside its entity.
2. On `CollectTelemetry`, the receiver snapshots all registered metric
   sets, encodes, and injects.

### Phase 6: Prometheus Export

**6a: Cumulative accumulator**

`crates/telemetry/src/self_metrics/accumulator.rs`:

Arrow-native delta → cumulative conversion. Keyed by
`MetricIdentity(schema_key, EntityKey)`. Column-wise addition using
`arrow::compute::kernels::numeric::add`. Handles NDP, HDP, and EDP.

**6b: OpenMetrics formatter**

`crates/telemetry/src/self_metrics/openmetrics.rs`:

Walk OTAP Arrow batch structure → emit OpenMetrics text. Handle:
- Counters → `_total` suffix, monotonic
- Gauges → gauge type
- Histograms → `_bucket`, `_count`, `_sum` with boundaries
- Exponential histograms → native histograms or explicit bucket conversion

**6c: Prometheus HTTP endpoint**

`crates/telemetry/src/self_metrics/prometheus.rs`:

`PrometheusExporter` — `Arc<RwLock<CumulativeAccumulator>>` + axum
`GET /metrics` handler. Clone batch on scrape (cheap ref-counted),
format, respond. Replaces current
`prometheus_exporter_provider.rs` + `opentelemetry-prometheus` dep.

### Phase 7: Pilot & Migration

**7a: Pilot schema — consumer/producer metrics**

Write `crates/telemetry/metrics_schema.yaml` for:
- `node.consumer.items` (counter, counting, outcome × signal_type dims)
- `node.producer.items` (counter, counting, outcome × signal_type dims)
- `node.consumer.duration` (counter, histogram, outcome dim)
- `node.producer.duration` (counter, histogram, outcome dim)

Generate code, write integration tests.

**7b: Migrate existing metric sets**

Each `#[metric_set]` struct gets a YAML schema and generated
replacement. Independent migration tasks per metric set group:
- Channel sender/receiver metrics
- Pipeline metrics
- Control plane metrics
- Engine metrics
- Completion emission metrics
- Component-specific metrics (parquet exporter, debug processor, etc.)

**7c: Remove OTel SDK metrics path**

Once all sets migrated:
- Remove `MetricsDispatcher`
- Remove `prometheus` + `opentelemetry-prometheus` crate deps
- Remove `prometheus_exporter_provider.rs`
- Remove `MetricsCollector` (old aggregation path)
- Update `CONTRIBUTING.md` with new metrics guidance

## Dependency Graph

```
Phase 1 (Foundation Types)
    │
    ├──► Phase 2 (Schema Format & Parser)
    │         │
    │         └──► Phase 3 (Code Generation)
    │                   │
    │                   └──► Phase 7a (Pilot Schema)
    │                             │
    │                             └──► Phase 7b (Migration)
    │                                       │
    │                                       └──► Phase 7c (Remove OTel SDK)
    │
    └──► Phase 4 (OTAP Encoding)
              │
              └──► Phase 5 (ITS Integration)
                        │
                        └──► Phase 6 (Prometheus Export)
```

Phases 1, 2 can start in parallel (1 provides types, 2 provides schema;
3 needs both). Phase 4 can start once Phase 1 is done. Phase 5 needs
both 3 and 4. Phase 6 needs 5. Phase 7 needs 3+5+6.

## New Files & Directories

```
crates/telemetry/src/self_metrics/       # Runtime library (new module)
  mod.rs
  aggregators.rs                         # MmscAgg, SampleReservoir, SplitCount
  dimension.rs                           # Dimension trait + impls
  precomputed.rs                         # PrecomputedMetricSchema, DataPointsBuilder
  collector.rs                           # CollectableMetrics, MetricsEncoder
  assembly.rs                            # OtapPdata assembly
  accumulator.rs                         # CumulativeAccumulator
  openmetrics.rs                         # OpenMetrics formatter
  prometheus.rs                          # PrometheusExporter
  generated.rs                           # Codegen output (checked in)

crates/telemetry/metrics_schema.yaml     # Pilot metric schema

templates/metrics/                       # MiniJinja templates
  metric_set.rs.j2
  dimension.rs.j2

xtask/src/generate_metrics/              # Codegen infrastructure
  mod.rs
  schema.rs                              # YAML parser → IR
  ir.rs                                  # Codegen IR types
```

## Key Dependencies

- `otel-expohisto` — exponential histogram (new workspace dependency)
- `minijinja` — template rendering in xtask (new xtask dependency)
- `serde_yaml` — schema parsing in xtask (likely already present)
- Existing: `arrow`, `otap-df-pdata`, `otap-df-config`

## Open Design Questions

1. **ExpoHisto merge in accumulator**: The CumulativeAccumulator needs
   to merge ExpoHisto snapshots. Column-wise addition isn't sufficient
   for histograms — this may require deserializing EDP columns, merging
   via the `otel-expohisto` merge API, and re-encoding. Alternatively,
   the accumulator could hold `Histogram<N>` instances directly rather
   than Arrow batches for histogram-type metrics.

2. **Gauge histogram temporality**: Since OTel has no gauge histogram
   spec, we choose: delta (distribution of values seen this interval)
   or cumulative (running distribution)? Delta seems more useful for
   internal metrics — it answers "what did the gauge look like recently?"
   rather than "what has it looked like since startup?"

3. **Gauge histogram metadata marker**: Use an attribute
   (`otel.metric.archetype = "gauge"`), a naming convention
   (`*.gauge_histogram`), or a custom field in the OTAP metrics table?
   The attribute approach is most portable.

4. **Sample mode reservoir size**: Reservoir of 1 value is simplest.
   Should we support configurable reservoir sizes (k random samples)?
   Start with 1, make extensible.

5. **Generated file naming**: `generated.rs` (single file per crate) or
   `generated/{metric_set_name}.rs` (file per set)?

6. **Multivariate wire format**: Use `UNIVARIATE_METRICS` (current, fully
   implemented) or `MULTIVARIATE_METRICS` (defined but schema is empty)?
   Univariate is safer initially; multivariate is the aspiration.
