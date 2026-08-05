# Metrics SDK v4 — Schema-Versioned Codegen with Migration Support

## Problem Statement

The otap-dataflow pipeline has 30+ `#[metric_set]` structs with
instrumentation patterns that encode dimensions into metric names
rather than attributes. The canonical example:

```rust
// Current: 9 separate counters, signal type baked into field name
#[metric_set(name = "exporter.pdata")]
struct ExporterPDataMetrics {
    pub metrics_consumed: Counter<u64>,
    pub metrics_exported: Counter<u64>,
    pub metrics_failed:   Counter<u64>,
    pub logs_consumed:    Counter<u64>,
    pub logs_exported:    Counter<u64>,
    pub logs_failed:      Counter<u64>,
    pub traces_consumed:  Counter<u64>,
    pub traces_exported:  Counter<u64>,
    pub traces_failed:    Counter<u64>,
}
```

The preferred form is a single metric with dimension attributes:

```
exporter.requests{outcome=consumed|exported|failed, signal=logs|metrics|traces}
```

This yields the same 9 timeseries but with a clean API
(`add_requests(count, outcome, signal)`), level-aware dimensionality
(0/1/2 active dimensions depending on basic/normal/detailed), and
uniform encoding.

Previous iterations (v1–v3) built a working PoC for the new SDK with
codegen, level-aware enums, precomputed OTAP encoding, and Prometheus
export. **This v4 iteration focuses on the migration path**: how to
transition from old metric names to new ones without breaking existing
consumers.

## Core Insight: Schema Files as Both API Source and Output Versioner

A single schema file serves three roles:

1. **API definition** — determines the Rust instrumentation API
   (method names, parameter types, dimension enums)
2. **Output versioning** — defines multiple named output versions
   (v1 = legacy fan-out names, v2 = dimension-attributed names)
3. **Translation rules** — encodes the mapping between versions using
   OTel schema-compatible transformation syntax

The compile-time choice selects which output version the codegen
produces. The instrumentation API is always the clean,
dimension-indexed form regardless of output version.

## Schema Format

### Canonical Metric Definition

```yaml
# crates/otap/exporter_metrics_schema.yaml
schema_url: "https://opentelemetry.io/schemas/otap/exporter/2.0"

attributes:
  - id: outcome
    type: string
    brief: "Result of the export operation."
    values: [consumed, exported, failed]

  - id: signal
    type: string
    brief: "Telemetry signal type."
    values: [logs, metrics, traces]

metric_sets:
  - id: exporter_pdata
    scope_name: "exporter.pdata"

    # Level-dependent dimensionality (the "views" mechanism)
    x-otap:
      levels:
        basic:    { dimensions: [] }                # 1 timeseries
        normal:   { dimensions: [outcome] }         # 3 timeseries
        detailed: { dimensions: [outcome, signal] } # 9 timeseries

    metrics:
      - name: exporter.requests
        instrument: counter
        unit: "{msg}"
        brief: "Number of pdata messages at each stage."
        recording_mode: counting
        attributes:
          - ref: outcome
          - ref: signal
```

### Output Versions

The schema file defines named output versions that control how the
OTAP encoder maps the canonical metric to wire-format names:

```yaml
    # Continued from metric_sets[0]
    output_versions:
      # v2: clean dimensional output (the new way)
      v2:
        # Default — uses canonical metric names with dimension attributes.
        # exporter.requests{outcome=consumed, signal=logs} → 1 metric, N attrs

      # v1: legacy fan-out output (backward compat)
      v1:
        fan_out:
          # Each dimension value combination becomes a separate metric name.
          # The template references dimension values.
          - metric: exporter.requests
            name_template: "{signal}_{outcome}"
            # Produces: logs_consumed, logs_exported, logs_failed,
            #           metrics_consumed, traces_consumed, ...
            # Dimension attributes are NOT emitted (they're in the name).
            strip_dimensions: [outcome, signal]
```

### OTel Schema Translation Syntax

For interop with the broader OTel ecosystem, we also support
OTel-schema-file-compatible translation declarations. These are
useful for generating processor-based translators (out of scope for
this iteration, but the schema supports it):

```yaml
# Separate file or section: schema_translations.yaml
versions:
  2.0.0:
    metrics:
      changes:
        # Fan-in: multiple old names → single new name with attributes
        - merge_metrics:
            target: exporter.requests
            sources:
              - name: logs_consumed
                attributes: { outcome: consumed, signal: logs }
              - name: logs_exported
                attributes: { outcome: exported, signal: logs }
              - name: logs_failed
                attributes: { outcome: failed, signal: logs }
              - name: metrics_consumed
                attributes: { outcome: consumed, signal: metrics }
              # ... etc for all 9 combinations
```

This `merge_metrics` extension to the OTel schema syntax is the
inverse of `fan_out`. Together they enable bidirectional translation
between metric versions — useful for future processor-based migration
(dual-reporting, query-time translation).

## Generated Code: Output Version Variants

The codegen produces code that always uses the clean API internally
but can encode to either output format at compile time.

### Compile-Time Version Selection

```rust
// In the schema YAML or in build config:
// selected_output_version: "v2"    ← compile-time choice

// Generated enum — always has the same API shape
pub enum ExporterPdata {
    // Level variants (unchanged from v3)
    Basic {
        requests: [Counter<u64>; 1],  // 0 dimensions → 1 slot
    },
    Normal {
        requests: [Counter<u64>; 3],  // 1 dimension (outcome) → 3 slots
    },
    Detailed {
        requests: [Counter<u64>; 9],  // 2 dimensions → 9 slots
    },
}

impl ExporterPdata {
    // API is always dimension-indexed, regardless of output version
    pub fn add_requests(&mut self, value: u64, outcome: Outcome, signal: Signal) {
        // index arithmetic: outcome * SIGNAL_CARDINALITY + signal
        // lower levels ignore unused dimensions (aggregate into fewer slots)
    }
}
```

### Output Version Affects Encoding Only

```rust
impl ExporterPdata {
    // v2 output: 1 metric "exporter.requests" with scope attrs
    // v1 output: 9 metrics "logs_consumed", "logs_exported", etc. — no attrs
    pub fn precomputed_schema(level: MetricLevel) -> PrecomputedMetricSchema {
        #[cfg(feature = "output-v2")]
        { /* single metric name, scope attrs for dimensions */ }

        #[cfg(feature = "output-v1")]
        { /* fan-out: one metric row per (name_template × dimension combo) */ }
    }
}
```

The feature flag (`output-v1` / `output-v2`) or a const generic
controls which precomputed schema is built. The hot-path recording
code is identical — only the init-time schema construction and the
snapshot-to-OTAP encoding differ.

### Alternative: Runtime Version Selection

For dual-reporting during migration, the version could be a runtime
enum rather than a compile-time feature. This adds ~10% encoding
overhead (branch per metric row) but allows a single binary to serve
both old and new consumers:

```rust
pub enum OutputVersion { V1, V2 }

impl ExporterPdata {
    pub fn precomputed_schema(level: MetricLevel, version: OutputVersion)
        -> PrecomputedMetricSchema
    { ... }
}
```

## Shared Instrumentation Libraries

### The Exporter Metrics Pattern

The Geneva exporter's metrics struct demonstrates a pattern that
should be reusable. Every exporter needs:

- Consumed/exported/failed counts × signal type
- Encode/upload/failure durations × signal type
- Error counts

This is a **shared metric set library** that component crates import:

```yaml
# crates/shared-metrics/exporter_metrics_schema.yaml
metric_sets:
  - id: exporter_pdata
    scope_name: "exporter.pdata"
    x-otap:
      levels:
        basic:    { dimensions: [] }
        normal:   { dimensions: [outcome] }
        detailed: { dimensions: [outcome, signal] }
    metrics:
      - name: exporter.requests
        instrument: counter
        unit: "{msg}"
        attributes: [outcome, signal]

      - name: exporter.duration
        instrument: counter
        recording_mode: histogram
        unit: ms
        attributes: [outcome, signal]
```

The Geneva exporter then *imports* this shared set and adds its own
component-specific metrics:

```yaml
# crates/contrib-nodes/geneva_metrics_schema.yaml
imports:
  - from: shared-metrics/exporter_metrics_schema.yaml
    metric_set: exporter_pdata

metric_sets:
  - id: geneva_specific
    scope_name: "otap.exporter.geneva"
    metrics:
      - name: geneva.empty_payloads_skipped
        instrument: counter
        unit: "{msg}"
      - name: geneva.conversion_errors
        instrument: counter
        unit: "{error}"
```

### Migration from Component-Specific to Shared

When migrating the Geneva exporter from its current 21-field struct to
the shared library:

1. The shared schema defines `exporter.requests{outcome, signal}` and
   `exporter.duration{outcome, signal}`
2. The Geneva exporter's schema defines `output_versions.v1` with
   `fan_out` rules that reproduce the old metric names
   (`log_batches_uploaded`, `trace_upload_success_duration`, etc.)
3. Initially deploy with `output-v1` for backward compatibility
4. Consumers migrate to the new names
5. Switch to `output-v2` (or dual-report during transition)
6. Remove `v1` output version from schema

## Migration Path: Concrete Steps

### Phase 1: Schema + Codegen Infrastructure

Build the schema parser extensions and codegen template changes:

- **1a**: Extend YAML schema format with `output_versions` and
  `fan_out` / `strip_dimensions` syntax
- **1b**: Extend codegen IR with `OutputVersionDef`,
  `FanOutRule`, `NameTemplate`
- **1c**: Extend MiniJinja templates to generate version-aware
  `precomputed_schema()` functions
- **1d**: Add `--output-version <name>` flag to
  `cargo xtask generate-metrics`

### Phase 2: Pilot — ExporterPDataMetrics Migration

Migrate the exemplar `ExporterPDataMetrics` (9 counters → 1
dimensioned counter):

- **2a**: Write `exporter_metrics_schema.yaml` with both v1 and v2
  output versions
- **2b**: Generate code, verify v1 output produces the same 9 metric
  names as the current struct
- **2c**: Replace `ExporterPDataMetrics` struct and call sites with
  the generated `ExporterPdata` enum
- **2d**: Verify Prometheus `/metrics` output is unchanged (v1)
- **2e**: Switch to v2, verify new dimensional output

### Phase 3: Shared Exporter Metrics Library

- **3a**: Create `crates/shared-metrics/` with the exporter schema
- **3b**: Generate the shared `ExporterPdata` type
- **3c**: Migrate Geneva exporter to use the shared type
- **3d**: Migrate other exporters (perf, parquet, topic, console)
- **3e**: Add v1 fan-out rules for each exporter's legacy names

### Phase 4: Consumer/Producer Migration

Migrate the core `ConsumedMetrics` / `ProducedMetrics`:

- **4a**: These already have the right semantic shape
  (`consumed.success` / `consumed.failure` / `consumed.refused` →
  `consumer.requests{outcome}`)
- **4b**: Add signal dimension at detailed level
- **4c**: Define v1 output that reproduces current names
- **4d**: Switch to v2

### Phase 5: Remaining Metric Sets

Migrate all 30+ metric sets, grouped by crate:

- Engine metrics (pipeline, control plane, engine, process duration)
- Channel metrics (sender, receiver)
- Core-nodes processors (batch, debug, filter, transform, etc.)
- Contrib-nodes (Azure Monitor, Geneva)
- Validation metrics

### Phase 6: OTel SDK Removal

Once all metric sets are migrated:

- Remove `MetricsDispatcher`
- Remove `opentelemetry-prometheus`, `prometheus` crate dependencies
- Remove `prometheus_exporter_provider.rs`
- Remove `MetricsCollector` (old aggregation path)
- Update `CONTRIBUTING.md`

## Output Version Encoding Details

### v2 (Dimensional) Encoding

Standard OTAP encoding with scope-based dimensions (as in v3 PoC):

```
UnivariateMetrics (1 row per level)
┌────┬──────────────────┬──────────┐
│ id │ name             │ scope.id │
├────┼──────────────────┼──────────┤
│  0 │ exporter.requests│        0 │  ← outcome=consumed, signal=logs
│  1 │ exporter.requests│        1 │  ← outcome=consumed, signal=metrics
│... │ ...              │      ... │
│  8 │ exporter.requests│        8 │  ← outcome=failed, signal=traces
└────┴──────────────────┴──────────┘

ScopeAttrs (precomputed, 18 rows = 9 scopes × 2 attrs)
┌───────────┬───────────────┬───────────────┐
│ parent_id │ attribute_key │ attribute_str │
├───────────┼───────────────┼───────────────┤
│         0 │ outcome       │ consumed      │
│         0 │ signal        │ logs          │
│       ... │ ...           │ ...           │
└───────────┴───────────────┴───────────────┘
```

### v1 (Fan-Out) Encoding

Each dimension combination becomes a distinct metric name. No scope
attributes needed for the fanned-out dimensions:

```
UnivariateMetrics (9 rows — one per fan-out name)
┌────┬──────────────────┬──────────┐
│ id │ name             │ scope.id │
├────┼──────────────────┼──────────┤
│  0 │ logs_consumed    │        0 │
│  1 │ logs_exported    │        0 │
│  2 │ logs_failed      │        0 │
│  3 │ metrics_consumed │        0 │
│  4 │ metrics_exported │        0 │
│... │ ...              │      ... │
│  8 │ traces_failed    │        0 │
└────┴──────────────────┴──────────┘
```

All metrics share a single scope (no dimension attributes). The
metric identity is in the name. Same 9 NDP rows, same values — just
different precomputed schema.

### Mixed-Level Fan-Out

At lower levels, fewer dimensions are active:

- **Normal** (1 dim = outcome): v1 fan-out produces 3 metrics
  (`consumed`, `exported`, `failed`), each summing across signals
- **Basic** (0 dims): v1 fan-out produces 1 metric (the aggregate),
  named per the first template token or a fallback name

The codegen computes the fan-out table per (level × version).

## Dual-Reporting

For seamless migration, a binary can produce both v1 and v2 output
simultaneously. Two approaches:

### Compile-Time Dual (Two Precomputed Schemas)

```rust
impl ExporterPdata {
    pub fn precomputed_schemas(level: MetricLevel)
        -> (PrecomputedMetricSchema, PrecomputedMetricSchema)
    {
        (Self::precomputed_schema_v1(level),
         Self::precomputed_schema_v2(level))
    }
}
```

Each collection tick encodes the snapshot twice (once per schema) and
injects both into the pipeline. Old consumers see old names; new
consumers see new names. CPU cost is ~2× encoding but encoding is
cheap (precomputed schema, only NDP values differ in layout).

### Pipeline-Processor Translation (Future)

A processor that reads v2 OTAP batches and produces v1 output (or
vice versa) using the translation rules from `schema_translations`.
This runs in the pipeline, not at the SDK level. Out of scope for
this iteration but the schema format is designed to support it.

## Relationship to OTel Schema Files

The OTel Telemetry Schema specification defines a file format for
describing how telemetry changes across schema versions:

```yaml
# Standard OTel schema file
file_format: "1.1.0"
schema_url: "https://example.com/schema/1.5.0"
versions:
  1.5.0:
    metrics:
      changes:
        - rename_metrics:
            old.metric.name: new.metric.name
```

Our `output_versions` and `fan_out` / `merge_metrics` syntax is a
**superset** of the OTel schema file's `rename_metrics`. The standard
OTel schema supports 1:1 renaming; our extension supports N:1
fan-in (`merge_metrics`) and 1:N fan-out (`fan_out`). This enables
the dimension↔name transformations that are the heart of the
migration.

We design the schema extensions to be convertible to/from OTel
schema files where possible, enabling future interop with Weaver's
schema resolution pipeline.

## Weaver Integration Path

Weaver's semconv registry model uses a directory tree of YAML
definition files where attribute references (`ref: outcome`) resolve
globally across the registry. The `weaver registry generate` command
compiles the full registry, resolves all refs, and renders templates.
Weaver supports registry dependencies via a definition manifest with
`schema_url` and optional `registry_path` (local dir, archive, or
Git repo).

Our shared metric set import mechanism follows Weaver's lead:

1. **Now**: Our schema files live alongside crates. Shared schemas
   go in a registry directory (`registry/metrics/`). Component
   schemas use `ref` to reference shared attribute definitions,
   exactly as Weaver semconv groups do. The xtask codegen resolves
   refs by loading the full registry before rendering templates.

2. **Next**: Validate our schemas against Weaver's SemConv registry
   (attribute definitions, metric naming conventions) by running
   `weaver registry check` as a CI step.

3. **Later**: Use Weaver's template engine (also Jinja2/MiniJinja)
   directly, contributing our `x-otap` extensions as a Weaver
   plugin. Our templates are already MiniJinja, so the migration
   is mechanical.

4. **Eventually**: Upstream shared instrumentation schemas to the
   OTel SemConv registry (e.g., `collector.exporter.*` metrics).

## Resolved Design Decisions

### 1. Version Selection: Feature Flags + Runtime Enum

Feature flags control which output versions are **available** in the
binary. A runtime `OutputVersion` enum selects the active version(s).
This supports dual-reporting from a single binary:

```rust
// Cargo.toml feature flags control code availability
[features]
output-v1 = []  # include v1 fan-out encoding
output-v2 = []  # include v2 dimensional encoding
default = ["output-v1", "output-v2"]  # both available

// Runtime selection (per metric set)
pub enum OutputVersion { V1, V2 }

impl ExporterPdata {
    pub fn precomputed_schema(level: MetricLevel, version: OutputVersion)
        -> PrecomputedMetricSchema
    {
        match version {
            #[cfg(feature = "output-v1")]
            OutputVersion::V1 => Self::precomputed_schema_v1(level),
            #[cfg(feature = "output-v2")]
            OutputVersion::V2 => Self::precomputed_schema_v2(level),
        }
    }
}
```

Dead code from disabled features is compiled out. Dual-reporting
just calls `precomputed_schema()` twice with different versions and
injects both into the pipeline.

### 2. Fan-Out Name Template: Simple Interpolation

`{signal}_{outcome}` — simple `{dimension_name}` substitution.
Covers all known naming patterns in the codebase. No template
language needed.

### 3. Scope Name: Stable, Versioned

Scope name remains the canonical name (`exporter.pdata`) across
all output versions. Scopes carry a `schema_url` with a semver
version, declared in the schema file:

```yaml
metric_sets:
  - id: exporter_pdata
    scope_name: "exporter.pdata"
    schema_url: "https://opentelemetry.io/schemas/otap/exporter/2.0"
    # scope_name is stable; schema_url carries the version
```

### 4. Shared Schema Import: Weaver-Style Registry

Follow Weaver's registry model: a `registry/` directory tree with
attribute and metric group definitions. Schema files use `ref` for
cross-file attribute references. Component schemas reference shared
metric sets via `extends` (Weaver semconv syntax). The xtask codegen
loads the full registry tree before resolving and rendering.

```yaml
# registry/metrics/exporter.yaml — shared definition
groups:
  - id: metric.exporter.requests
    type: metric
    metric_name: exporter.requests
    instrument: counter
    unit: "{msg}"
    attributes:
      - ref: outcome
      - ref: signal

# crates/contrib-nodes/geneva_metrics_schema.yaml — component
groups:
  - id: metric.geneva.specific
    type: metric
    extends: metric.exporter.requests   # inherits attrs + x-otap
    # ... plus component-specific metrics
```

### 5. Automatic V1 Name Derivation: Migration Tool

**This is the key enabler for seamless one-at-a-time migration.**

The existing `#[metric_set]` proc macro derives metric names from
Rust field names via `ident.to_string().replace('_', '.')` (or
explicit `#[metric(name = "...")]`). A new xtask command
auto-generates the initial schema file from the existing struct:

```
cargo xtask migrate-metric-set \
    --struct ExporterPDataMetrics \
    --source crates/otap/src/metrics.rs
```

This tool:

1. **Parses** the existing `#[metric_set]` struct using `syn`
2. **Extracts** field names, explicit metric names, units, instrument
   types, and doc comments
3. **Analyzes** naming patterns to detect implicit dimensions:
   - Fields sharing a common suffix with different prefixes
     (e.g., `logs_consumed`, `metrics_consumed`, `traces_consumed`)
     → suggests a `signal` dimension with values
     `[logs, metrics, traces]` and an `outcome` dimension with
     value `consumed`
   - Fields sharing a common prefix with different suffixes
     (e.g., `consumed_success`, `consumed_failure`,
     `consumed_refused`)
     → suggests an `outcome` dimension with values
     `[success, failure, refused]`
4. **Generates** a schema YAML with:
   - A canonical metric name (the common root after dimension
     extraction)
   - Dimension attributes with their detected values
   - `output_versions.v1` with `fan_out` rules that reproduce the
     exact old metric names (field-name-derived via
     `replace('_', '.')`)
   - `output_versions.v2` with the clean dimensional output
   - `x-otap.levels` with a default progression (basic=no dims,
     normal=first dim, detailed=all dims)
5. **Writes** the schema file alongside the crate

The generated schema file is a starting point — a human reviews and
adjusts (e.g., refining dimension names, adjusting level
assignments, adding descriptions). But the v1 fan-out rules are
computed to exactly match the old output, ensuring the migration
is invisible to metric consumers.

#### Example: Auto-Migration of ExporterPDataMetrics

Input struct (in `crates/otap/src/metrics.rs`):

```rust
#[metric_set(name = "exporter.pdata")]
struct ExporterPDataMetrics {
    #[metric(unit = "{msg}")] pub metrics_consumed: Counter<u64>,
    #[metric(unit = "{msg}")] pub metrics_exported: Counter<u64>,
    #[metric(unit = "{msg}")] pub metrics_failed:   Counter<u64>,
    #[metric(unit = "{msg}")] pub logs_consumed:     Counter<u64>,
    #[metric(unit = "{msg}")] pub logs_exported:     Counter<u64>,
    #[metric(unit = "{msg}")] pub logs_failed:       Counter<u64>,
    #[metric(unit = "{msg}")] pub traces_consumed:   Counter<u64>,
    #[metric(unit = "{msg}")] pub traces_exported:   Counter<u64>,
    #[metric(unit = "{msg}")] pub traces_failed:     Counter<u64>,
}
```

Detected pattern: 3×3 grid of `{signal}_{outcome}` where
`signal ∈ [metrics, logs, traces]` and
`outcome ∈ [consumed, exported, failed]`.

Generated schema:

```yaml
attributes:
  - id: signal
    type: string
    values: [metrics, logs, traces]
  - id: outcome
    type: string
    values: [consumed, exported, failed]

metric_sets:
  - id: exporter_pdata
    scope_name: "exporter.pdata"
    schema_url: "https://opentelemetry.io/schemas/otap/exporter/2.0"
    x-otap:
      levels:
        basic:    { dimensions: [] }
        normal:   { dimensions: [outcome] }
        detailed: { dimensions: [outcome, signal] }
    metrics:
      - name: exporter.requests
        instrument: counter
        unit: "{msg}"
        brief: "Number of pdata messages at each stage."
        attributes: [outcome, signal]
    output_versions:
      v1:
        fan_out:
          - metric: exporter.requests
            name_template: "{signal}.{outcome}"
            # Produces: metrics.consumed, metrics.exported, ..., traces.failed
            # (dots because the proc macro does replace('_', '.'))
            strip_dimensions: [outcome, signal]
      v2: {}
```

The v1 fan-out names exactly match what the old proc macro would
produce (`metrics_consumed` → `metrics.consumed` via
`replace('_', '.')`). Prometheus further mangles to underscores,
so users see the same `exporter_pdata_metrics_consumed` they
always did.

### 6. Output Version Granularity: Per Metric Set

Different metric sets in the same binary can use different output
versions. This enables incremental migration: migrate one set to
v2 while others remain v1.

```rust
// Pipeline setup: each metric set gets its own version
let exporter_metrics = ExporterPdata::new(MetricLevel::Normal);
let consumer_metrics = NodeConsumer::new(MetricLevel::Detailed);

// Registration specifies the output version per collector
registry.register(exporter_metrics, OutputVersion::V2);  // migrated
registry.register(consumer_metrics, OutputVersion::V1);   // not yet
```

## Documentation Generation (Issue #2507)

Today, metric documentation lives in three places that drift apart:

1. **Rust doc comments** on `#[metric_set]` fields — extracted by the
   proc macro into `MetricsField.brief` at compile time
2. **Telemetry.md files** — hand-maintained markdown tables with
   richer descriptions (e.g., "Aligned with OTel semantic convention
   `process.cpu.utilization`"), source file references, and
   contextual notes
3. **OTel metric description** — the `description` string in the
   OTAP/OTLP wire encoding, currently just the `brief` from (1)

The schema YAML becomes the **single source of truth** for all three.
Each metric definition carries both `brief` (short, one-line) and
`note` (extended, multi-line) fields:

```yaml
metrics:
  - name: exporter.requests
    instrument: counter
    unit: "{msg}"
    brief: "Number of pdata messages at each stage."
    note: |
      Tracks messages through the consume → export → ack/fail
      lifecycle. At detailed level, broken down by signal type
      (logs, metrics, traces) and outcome (consumed, exported,
      failed). Replaces the legacy per-signal counter fields.
    attributes: [outcome, signal]
```

The codegen pipeline produces three outputs from this schema:

1. **Generated Rust code** — `brief` becomes the doc comment on the
   generated method and populates `MetricsField.brief` (which flows
   into the OTel metric `description` on the wire)
2. **Generated Telemetry.md** — a MiniJinja template renders the
   full metric table with `brief`, `note`, unit, instrument type,
   attributes, and source file. Replaces hand-maintained docs.
3. **SemConv YAML** — optionally emits a Weaver-compatible registry
   file for upstream validation or contribution

### Migration Tool: Doc Comment Extraction

The `cargo xtask migrate-metric-set` tool (Phase 0) also:

- Extracts Rust doc comments from existing fields → `brief`
- Cross-references the corresponding Telemetry.md file (if found)
  to pull in richer descriptions → `note`
- Marks any Telemetry.md entries that have NO corresponding
  `#[metric_set]` field (documentation for metrics that don't
  exist, or vice versa) — surfacing the drift that #2507 aims
  to eliminate

### CI Freshness Check

`cargo xtask generate-metrics --check` already validates generated
Rust code freshness. Extend it to also check Telemetry.md freshness:
if the schema changes and docs aren't regenerated, CI fails. This
closes the drift loop permanently.

## Updated Migration Path

The auto-migration tool changes the phasing:

### Phase 0: Migration Tooling (NEW)

- **0a**: Build `cargo xtask migrate-metric-set` — parses existing
  `#[metric_set]` structs via `syn`, detects dimension patterns,
  extracts doc comments as `brief`, cross-references Telemetry.md
  for richer `note` text, generates schema YAML with v1 fan-out
  rules that reproduce old metric names exactly
- **0b**: Build Telemetry.md template — MiniJinja template that
  renders schema YAML into the standard Telemetry.md table format,
  replacing hand-maintained docs (closes #2507)
- **0c**: Test on ExporterPDataMetrics, ConsumedMetrics, and
  ProducedMetrics to validate pattern detection and doc extraction
- **0d**: Run on all 30+ metric sets to generate initial schema
  files and Telemetry.md files (human review pass follows)

### Phase 1–6: Same as before

Phase 0 front-loads the schema generation, making subsequent
phases (codegen infra, pilot, shared libs, migration, SDK removal)
faster because the schemas already exist.
