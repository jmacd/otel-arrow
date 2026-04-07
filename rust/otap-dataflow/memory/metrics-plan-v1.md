# Metrics Instrumentation Codegen & Migration Plan

## Problem Statement

The otap-dataflow metrics instrumentation has several interconnected problems:

1. **No per-signal breakdown**: Producer/consumer metrics report aggregate
   counts across all signal types. Users want logs/metrics/traces separately.

2. **Field duplication anti-pattern**: PR #2504 (Geneva exporter) and the
   durable buffer processor duplicate every metric field with `log_`/`trace_`
   prefixes instead of using a single metric with a `signal_type` attribute.
   This produces N×M fields instead of 1 metric with OTel attributes.

3. **Outcome counters should be attributes**: Producer/consumer metrics have 3
   separate counters (`consumed_success`/`consumed_failure`/`consumed_refused`)
   that should be 1 OTel metric with an `outcome` attribute (3 timeseries).

4. **No histogram instrument**: MMSC (min/max/sum/count) is the only duration
   aggregator. The OTel SDK export path can't support pre-aggregated histograms
   anyway (no observe/record-histogram API exists). Histogram support is blocked
   until we replace the OTel SDK dispatcher with direct OTAP encoding.

5. **MetricLevel should control dimensionality and aggregation**: A Basic user
   might want a single aggregate counter; Normal wants per-signal breakdown;
   Detailed wants richer dimensions and histograms. No mechanism exists for
   level-dependent attribute dimensions or aggregation selection.

6. **Manual documentation drift**: Telemetry.md files are manually maintained
   and drift from code. Issue #2507 calls for auto-generated documentation.

7. **OTel SDK dependency must be replaced**: The telemetry README and docs
   describe evolution toward an Internal Telemetry System (ITS) where the OTAP
   engine itself consumes internal telemetry streams. The current
   `MetricsDispatcher` bridges to the OTel SDK, but the target architecture has
   the engine encoding metrics directly into OTAP format and routing through its
   own export infrastructure — just as it already does for logs via the
   `internal_telemetry_receiver`.

## Current Architecture (Summary)

- **Instruments**: `Counter<u64|f64>`, `UpDownCounter`, `ObserveCounter`,
  `ObserveUpDownCounter`, `Gauge<u64|f64>`, `Mmsc` — all in
  `crates/telemetry/src/instrument.rs`. No `Histogram`.
- **Metric sets**: Declared with `#[metric_set(name = "...")]` proc macro,
  generating `MetricSetHandler` impls with `snapshot_values()`/`clear_values()`.
  Each field is a single physical instrument — no dimension/attribute support
  at the field level.
- **Attribute sets**: Declared with `#[attribute_set(name = "...")]`, attached
  at the entity level. All metrics in a set share the same entity attributes.
  No per-field attribute dimensions.
- **MetricLevel**: `None|Basic|Normal|Detailed` in `crates/config/src/policy.rs`.
  Controls which *entire metric sets* are enabled via `Interests` bitflags.
  Does not control per-field aggregation or dimensionality.
- **Dispatcher → OTel SDK**: `MetricsDispatcher` converts `MetricValue::{U64,
  F64, Mmsc}` to OTel counters/gauges/histograms. MMSC is exported as a
  histogram with empty boundaries. Cumulative counters exported as gauges to
  avoid double-counting. This entire path is the replacement target.
- **ITS for logs**: The `internal_telemetry_receiver` already produces
  `OtapPdata` objects for logs by encoding OTLP log bytes. The same pattern
  will be applied to metrics, but using direct OTAP Arrow encoding.
- **OTAP metrics encoding**: `crates/pdata/` has full Arrow record batch
  builders for metrics (`MetricsRecordBatchBuilder`,
  `NumberDataPointsRecordBatchBuilder`, `HistogramDataPointsRecordBatchBuilder`,
  etc.) and the `temporal_reaggregation_processor` does cumulative aggregation.

## Design Vision

### Codegen-First Approach

Rather than hand-evolving the existing instrument types and then building
codegen later, we build the codegen system first and use it to generate the new
instrumentation. The schema drives everything:

```text
  metrics.yaml (OTel SemConv YAML, extended)
      │
      ├──► cargo xtask gen-metrics
      │        │
      │        ├──► Rust instrument enums + metric set structs
      │        │    (dimensioned, level-aware, boxed/unboxed)
      │        │
      │        ├──► OTAP Arrow record batch encoders
      │        │    (direct measurement → OTAP, bypassing OTel SDK)
      │        │
      │        ├──► telemetry.md documentation
      │        │
      │        └──► SemConv registry YAML (for Weaver validation)
      │
      └──► call sites use generated APIs
```

### Schema Format: OTel SemConv YAML (Extended)

Use the standard OTel semantic conventions YAML format natively compatible
with Weaver, extended with custom fields for our level-aware and dimensioned
instrument needs:

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
    # Extension: level-aware behavior
    x-otap-levels:
      basic:
        dimensions: [outcome]           # 0D for signal, 1D for outcome
      normal:
        dimensions: [outcome, signal_type]  # 2D
      detailed:
        dimensions: [outcome, signal_type]

  - id: metric.node.consumer.duration
    type: metric
    metric_name: node.consumer.duration
    instrument: histogram
    unit: ns
    brief: "Time to consume items."
    stability: experimental
    # Extension: level-aware aggregation
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

### Generated Rust Code

For the above schema, codegen produces:

**Level-aware instrument enums** (boxed for large, unboxed for small):
```rust
// Generated: level-aware counter with outcome × signal dimensions
pub enum NodeConsumerItemsCounter {
    // Basic: 1D by outcome only (3 counters)
    Basic([Counter<u64>; 3]),
    // Normal/Detailed: 2D by outcome × signal (9 counters)
    Normal([[Counter<u64>; 3]; 3]),
}

impl NodeConsumerItemsCounter {
    pub fn new(level: MetricLevel) -> Self { ... }
    pub fn add(&mut self, value: u64, outcome: Outcome, signal: SignalType) {
        match self {
            Self::Basic(arr) => arr[outcome.index()].add(value),
            Self::Normal(arr) => arr[outcome.index()][signal.index()].add(value),
        }
    }
}

// Generated: level-aware duration with aggregation selection
pub enum NodeConsumerDuration {
    Basic(Mmsc),
    Normal(Box<Histogram<8>>),
    Detailed(Box<Histogram<16>>),
}
```

**OTAP Arrow encoder** (replaces OTel SDK dispatcher for this metric):
```rust
// Generated: encodes NodeConsumerMetrics into OTAP Arrow record batches
impl NodeConsumerMetrics {
    pub fn encode_otap(
        &self,
        metrics_builder: &mut MetricsRecordBatchBuilder,
        ndp_builder: &mut NumberDataPointsRecordBatchBuilder,
        hdp_builder: &mut HistogramDataPointsRecordBatchBuilder,
        attrs_builder: &mut AttributesRecordBatchBuilder,
        entity_attrs: &dyn AttributeSetHandler,
        timestamp: u64,
    ) -> Result<(), Error> {
        // Encode items counter with dimension attributes
        // Encode duration with appropriate datapoint type
    }
}
```

### Instrument-by-Instrument OTel SDK Replacement

The migration replaces the OTel SDK dependency gradually:

1. For each metric set, write a `metrics.yaml` schema and generate code.
2. The generated code includes both the old `MetricSetHandler` impl (for
   backward compatibility during migration) and the new OTAP encoder.
3. A feature flag or runtime config switches between the OTel SDK dispatcher
   path and the new direct-OTAP path per metric set.
4. Once all metric sets are migrated, the `MetricsDispatcher` and OTel SDK
   dependency are removed.

## Phased Implementation

### Phase 1: Foundation Types

**Goal**: Create the core library types that generated code will use. These
live in the telemetry crate and are hand-written (not generated).

**Scope**:
- `Dimension` trait for bounded enum types:
  ```rust
  pub trait Dimension: Copy {
      const CARDINALITY: usize;
      const ATTRIBUTE_KEY: &'static str;
      const ATTRIBUTE_VALUES: &'static [&'static str];
      fn index(self) -> usize;
  }
  ```
- `Histogram<const N: usize>` instrument type (const-generic, N boundary
  values, N+1 bucket counts, min/max/sum/count).
- `MetricValue::Histogram(HistogramSnapshot)` variant.
- `DimensionedInstrument<T, D>` generic wrapper (holds `[T; D::CARDINALITY]`,
  provides indexed access).
- Impl `Dimension` for `SignalType` and `RequestOutcome`.
- Snapshot/clear/merge support for all new types.
- Unit tests.

**Does NOT include**: Level-aware enums (those are generated), OTAP encoding
(generated), macro extensions (superseded by codegen).

**Files**:
- `crates/telemetry/src/instrument.rs` — `Histogram<N>`
- `crates/telemetry/src/dimension.rs` — new: `Dimension` trait,
  `DimensionedInstrument`
- `crates/telemetry/src/metrics.rs` — `MetricValue::Histogram` variant
- `crates/config/src/lib.rs` — `impl Dimension for SignalType`
- `crates/engine/src/channel_metrics.rs` — `impl Dimension for RequestOutcome`

### Phase 2: Schema Format & Parser

**Goal**: Define the schema YAML format and build a parser in the xtask crate.

**Scope**:
- Define the `metrics.yaml` schema format based on OTel SemConv YAML with
  `x-otap-levels` extensions (as shown above).
- Define shared attribute groups (outcome, signal_type) as reusable refs.
- Build a schema parser in `xtask/` that reads and validates the YAML.
- Design the internal representation (IR) that the code generators consume:
  metric sets, fields, dimensions, level configurations, aggregation choices.
- Write a prototype `metrics.yaml` for the producer/consumer metrics as the
  first test case.
- Validate the schema format against real use cases: Geneva exporter, durable
  buffer, channel metrics, control plane metrics.

**Files**:
- `xtask/src/gen_metrics/` — new module
- `xtask/src/gen_metrics/schema.rs` — YAML parser, IR types
- `crates/engine/metrics.yaml` — first schema file (prototype)

### Phase 3: Rust Instrument Code Generator

**Goal**: Generate Rust metric instrumentation code from the schema.

**Scope**:
- Generate level-aware instrument enums (e.g., `NodeConsumerItemsCounter`,
  `NodeConsumerDuration`) with appropriate boxing:
  - Unboxed: single `Counter`, `Mmsc`, small arrays (`[Counter; 3]`)
  - Boxed: `Histogram<N>`, larger arrays
- Generate metric set structs with `MetricSetHandler` impl
  (snapshot_values, clear_values, needs_flush).
- Generate dimension-aware snapshot logic that produces per-slot values with
  attribute key/value pairs.
- Generate constructors that take `MetricLevel` and select the right enum
  variant.
- Generate the increment/record API methods with dimension parameters.
- Output to `<crate>/src/metrics_gen.rs` (or similar).
- `cargo xtask gen-metrics --check` for CI freshness validation.

**Files**:
- `xtask/src/gen_metrics/rust_gen.rs` — Rust code generator
- `xtask/src/gen_metrics/mod.rs` — orchestration
- Per-crate `src/metrics_gen.rs` — generated output

### Phase 4: OTAP Arrow Encoder Generator

**Goal**: Generate code that encodes metric snapshots directly into OTAP Arrow
record batches, bypassing the OTel SDK.

**Scope**:
- For each metric in the schema, generate an encoder function that:
  - Creates the root metrics record (name, unit, type, temporality, monotonic)
  - Creates datapoint records (NumberDataPoint for counters/gauges,
    HistogramDataPoint for histograms, NumberDataPoint with
    min/max/sum/count for MMSC)
  - Creates attribute records (entity attributes + dimension attributes)
  - Uses the existing `pdata` crate builders (`MetricsRecordBatchBuilder`,
    `NumberDataPointsRecordBatchBuilder`, `HistogramDataPointsRecordBatchBuilder`,
    `AttributesRecordBatchBuilder`)
- Generate a per-metric-set `encode_otap()` method that produces an
  `OtapPdata::Metrics(...)`.
- This becomes the replacement for the `MetricsDispatcher` → OTel SDK path.
- The `internal_telemetry_receiver` (or a new metrics-specific counterpart)
  calls the generated encoders.

**Files**:
- `xtask/src/gen_metrics/otap_gen.rs` — OTAP encoder generator
- Per-crate `src/metrics_otap_gen.rs` — generated OTAP encoder output

### Phase 5: Documentation Generator

**Goal**: Auto-generate `telemetry.md` documentation from the schema.

**Scope**:
- For each crate's `metrics.yaml`, generate a `telemetry.md` that documents:
  - Each metric: name, type, unit, description, stability
  - Dimensions/attributes: key, values, which levels they're active at
  - Aggregation by level: MMSC vs Histogram<8> vs Histogram<16>
  - Entity attributes inherited from the metric set
- Replace hand-written telemetry.md files.
- `cargo xtask gen-metrics` regenerates docs alongside code.
- Weaver SemConv YAML output for CI validation (future).

**Files**:
- `xtask/src/gen_metrics/doc_gen.rs` — documentation generator
- Per-crate `telemetry.md` — generated output

### Phase 6: Migrate Core Metrics (Schema-First)

**Goal**: Write schemas for existing metrics and migrate call sites to use
generated code, replacing the OTel SDK path instrument by instrument.

**Sub-tasks** (each is an independent PR):

**6a. Producer/Consumer metrics** (`crates/engine/`):
- Write schema for `node.consumer` and `node.producer` metric sets.
- Dimensions: `outcome` (basic+), `signal_type` (normal+).
- Generate code, update `pipeline_ctrl.rs` call sites.
- Wire generated OTAP encoder into ITS metrics path.

**6b. Channel metrics** (`crates/engine/`):
- Write schema for `channel.sender` and `channel.receiver`.
- Evaluate signal_type dimension need.

**6c. Durable buffer processor** (`crates/core-nodes/`):
- Write schema replacing `consumed_log_records`/`consumed_metric_points`/
  `consumed_spans` with `DimensionedCounter<SignalType>`.

**6d. Geneva exporter** (`crates/contrib-nodes/`):
- Write schema replacing the `log_*`/`trace_*` field duplication from PR #2504.

**6e. Control plane metrics** (`crates/engine/`):
- Write schema. Duration metrics become level-aware (MMSC at basic,
  histogram at normal+, once histogram support is available via OTAP path).

**6f. Completion emission metrics** (`crates/engine/`):
- Write schema. Evaluate dimension opportunities.

**6g. Other component metrics**: Perf exporter, Azure Monitor exporter,
resource validator, syslog receiver, etc.

### Phase 7: Remove OTel SDK Dispatcher

**Goal**: Once all metric sets are migrated to generated OTAP encoders,
remove the `MetricsDispatcher` and OTel SDK metrics dependency.

**Scope**:
- Verify all metric sets have generated OTAP encoders.
- Remove `MetricsDispatcher` (`crates/telemetry/src/metrics/dispatcher.rs`).
- Remove OTel SDK meter provider setup for metrics
  (`crates/telemetry/src/otel_sdk/meter_provider.rs`).
- Keep OTel SDK for traces if still needed, or remove entirely.
- Update `InternalTelemetrySystem` to route metrics through the engine's
  internal pipeline (like logs already do).
- Update documentation.

### Phase 8: Weaver Integration

**Goal**: Connect the schema to OTel tooling ecosystem.

**Scope**:
- Generate OTel SemConv registry files from `metrics.yaml` schemas.
- Integrate Weaver for CI validation (registry compliance checks).
- Live checks during tests: verify expected metrics are actually produced.
- Generate HTML/Markdown documentation from registry via Weaver.

## Dependency Graph

```text
Phase 1 (Foundation Types)
    │
    └──► Phase 2 (Schema Format & Parser)
              │
              ├──► Phase 3 (Rust Codegen)
              │        │
              │        └──► Phase 6 (Migrate Metrics) ─► Phase 7 (Remove OTel SDK)
              │
              ├──► Phase 4 (OTAP Encoder Codegen)
              │        │
              │        └──► Phase 6 (Migrate Metrics) ─► Phase 7 (Remove OTel SDK)
              │
              └──► Phase 5 (Doc Codegen)
                       │
                       └──► Phase 6 (Migrate Metrics)

Phase 8 (Weaver) can proceed in parallel once Phase 2 is done.
```

Phase 3, 4, and 5 are independent code generators that can be built in
parallel once the schema parser (Phase 2) exists.

Phase 6 sub-tasks are independent of each other but each requires both
Phase 3 (Rust codegen) and Phase 4 (OTAP codegen) to be functional.

Phase 7 requires all of Phase 6 to be complete.

Phase 8 can start as soon as Phase 2 is done (schema format is stable).

## Open Design Questions

1. **SemConv YAML extensions**: The `x-otap-levels` extension needs to be
   designed carefully so that the base YAML remains valid SemConv that Weaver
   can parse, while the extensions carry our level-aware behavior. Need to
   verify Weaver's extension mechanism supports this cleanly.

2. **OTAP encoding granularity**: Should each metric set have its own OTAP
   encoder that produces a complete `OtapPdata`, or should there be a
   shared encoder that batches multiple metric sets into one OTAP payload?
   The temporal reaggregation processor suggests the latter is fine (it can
   handle multiple metrics in one batch).

3. **Backward compatibility during migration**: During the transition, some
   metrics will be on the old OTel SDK path and some on the new OTAP path.
   The generated code should support both paths. A runtime flag or config
   option per metric set, or a global "use ITS for metrics" switch?

4. **Snapshot format evolution**: The current `MetricSetSnapshot` carries
   `Vec<MetricValue>` with no attribute information per value. Dimensioned
   metrics need per-slot attributes in snapshots. This changes the snapshot
   contract. Should we evolve `MetricSetSnapshot` or introduce a new type
   for the OTAP path?

5. **Boxing threshold**: Suggested: box anything > 64 bytes (one cache line).
   `[Counter; 3]` (24 bytes) unboxed, `Histogram<8>` (~200 bytes) boxed.

6. **Interests interaction**: With level-aware instruments that self-select
   their dimensionality, do we still need the coarse `Interests` bitflags
   for consumer/producer metrics? Or do the instruments themselves handle
   level sensitivity, and `Interests` remains only for enabling/disabling
   entire metric categories?
