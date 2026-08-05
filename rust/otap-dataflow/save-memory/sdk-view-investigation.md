# SDK View Investigation — Session Report

## Objective

Investigate how to make OTel SDK metric Views available in the OTAP dataflow
pipeline for internal self-observability metrics, replacing the OTel SDK's
view mechanism as part of the broader SDK replacement effort.

## Background

The OTel SDK provides a
[Views](https://opentelemetry.io/docs/specs/otel/metrics/sdk/#view) mechanism
to customize metric streams: filtering attributes, renaming metrics, changing
aggregation, and dropping instruments. The
[declarative configuration v1.0](https://opentelemetry.io/docs/specs/otel/configuration/#declarative-configuration)
defines the YAML schema for `meter_provider.views` with `ViewSelector`
(instrument_name, instrument_type, unit, meter_name/version/schema_url) and
`ViewStream` (name, description, aggregation, attribute_keys with
include/exclude).

This codebase is replacing the OTel SDK for internal metrics with an
OTAP-native pipeline (as already done for logs). The question: where and
how should metric views be implemented?

## What We Found in the Codebase

### Existing view config types

`crates/config/src/pipeline/telemetry/metrics/views.rs` already has:

```rust
pub struct ViewConfig {
    pub selector: MetricSelector,
    pub stream: MetricStream,
}

pub struct MetricSelector {
    pub instrument_name: Option<String>,
    pub scope_name: Option<String>,
}

pub struct MetricStream {
    pub name: Option<String>,
    pub description: Option<String>,
    // attribute_keys is NOT here yet
}
```

These are used in pipeline YAML (e.g., `configs/fake-debug-noop-telemetry.yaml`)
under `engine.telemetry.metrics.views` and wired into the OTel SDK via
`crates/telemetry/src/otel_sdk/meter_provider/views_provider.rs`, which
translates them into SDK `View` functions for name/description overrides.

The selector uses `scope_name` (not the OTel spec's `meter_name`) and
currently supports only exact matching (no wildcards). The stream only
supports `name` and `description` — no `attribute_keys`, no `aggregation`.

### Internal metrics encoding paths

**Current path (OTel SDK)**:
1. `#[metric_set]` proc macro generates `MetricSetHandler` with
   `snapshot_values() → Vec<MetricValue>` (flat array, one value per field)
2. `MetricsDescriptor` provides field metadata (name, unit, instrument kind)
3. `AttributeSetHandler` provides entity-level attributes (shared across
   all fields in a set)
4. `MetricsDispatcher::dispatch_metrics()` calls `visit_metrics_and_reset()`
   and fans out each field to an OTel SDK counter/gauge call with entity
   attributes

**Codegen path (from `memory/metrics-sdk-codegen-complete.md`)**:
1. Schema-driven codegen produces level-aware counter enums
   (e.g., `Basic=[Counter; 3]`, `Normal=[Counter; 9]`)
2. Dimensions baked into the array layout at init time
   (e.g., `outcome(3) × signal_type(3) = 9` data points)
3. `PrecomputedMetricSchema` builds metrics + attributes Arrow tables once
4. `CounterDataPointsBuilder` fills values at runtime
5. Precomputed schema is fully determined by the metric_set type + MetricLevel

## Approaches Explored

### Approach 1: Pipeline Processor (built, then reconsidered)

We built a `metric_sdkview_processor` in
`crates/core-nodes/src/processors/metric_sdkview_processor/` with:

- **`config.rs`**: `ExtendedViewConfig` extending `MetricSelector` with
  `attribute_keys` (include/exclude lists) and `aggregation` (drop support).
  Reuses `MetricSelector`/`MetricStream` from the config crate.
- **`matcher.rs`**: `ViewMatcher` with precompiled selectors and glob
  pattern matching (`*`, `?`) for `instrument_name`.
- **`mod.rs`**: Full processor implementation — factory registration,
  `local::Processor<OtapPdata>` impl, OTAP Arrow batch inspection via
  `OtapMetricsView`, attribute deletion via `apply_attribute_transform` /
  `DeleteTransform`, metric name/description column rewriting.
- **`metrics.rs`**: Self-instrumentation counters.

**26 tests, `cargo xtask quick-check` clean.**

This approach operates on already-encoded OTAP Arrow batches:
- Read metric name/scope from root table via view API
- Match against configured views (first match wins)
- Drop: discard the entire batch
- Attribute filter: build a `DeleteTransform` from the include/exclude spec,
  apply to all data-point attribute tables
- Name/description: rewrite dictionary-encoded columns in the root table

### Why the processor approach is suboptimal

The key problem: **attribute filtering on encoded Arrow batches does work
twice** — encode the attributes into Arrow, then scan and delete them. For
internal metrics this is wasteful. The right place to filter is at the
point of OTAP assembly, where you can simply not emit unwanted attributes.

### Approach 2: ITR-level view application (reconsidered further)

Applying views at the Internal Telemetry Receiver (ITR) during OTAP
assembly. This is cheaper for `drop`, `name`/`description`, and
entity-level `attribute_keys` filtering.

However, **this is not trivial for dimension-level attributes** in the
codegen path:

- The codegen counter arrays have their dimensions baked into the layout
  at init time (e.g., 9 counters for `outcome × signal_type`)
- The `PrecomputedMetricSchema` builds Arrow tables to match
- Removing a dimension attribute (e.g., `signal_type`) requires
  **summing across that dimension** — collapsing 9 data points to 3
- This is a structural change to the snapshot, not just "skip an attribute"

### Approach 3: Init-time dimension selection (the natural fit)

For codegen metric_sets, `attribute_keys` filtering is equivalent to what
`MetricLevel` already does. `MetricLevel::Basic` with
`dimensions: [outcome]` produces the 3-counter variant; `Normal` with
`dimensions: [outcome, signal_type]` produces the 9-counter variant.

A view with `attribute_keys.included: [outcome]` is the external
configuration surface for the same init-time decision. The view config
needs to be known **at metric_set construction time** to influence the
dimension layout.

For current non-codegen `#[metric_set]` structs, there are no per-field
dimension attributes — only entity-level attributes. Filtering entity
attributes during OTAP assembly is straightforward: skip unwanted keys
from the `AttributeSetHandler`.

## Key Conclusions

1. **Views should not be a processor.** Operating on already-encoded Arrow
   batches is doing unnecessary work for internal metrics.

2. **Views config belongs in `engine.telemetry.metrics.views`** — it's
   already there, just needs `attribute_keys` added to `MetricStream`.

3. **For codegen metrics, `attribute_keys` = dimension selection at init
   time.** This unifies with `MetricLevel`: both control which dimensions
   are active. The view is the external config surface; the level/variant
   selection is the internal mechanism.

4. **For non-codegen metrics, entity-attribute filtering at OTAP assembly
   time is cheap** — just skip keys from `AttributeSetHandler` that don't
   pass the include/exclude filter.

5. **`drop` and `name`/`description` are easy at assembly time** — skip
   the metric_set (drop), or use the override string (rename). No Arrow
   manipulation needed.

6. **The single-writer rule concern is resolved by init-time filtering.**
   If attributes are removed before aggregation (at init time for codegen)
   or before encoding (at assembly time for non-codegen), no duplicate
   identity points are created. No downstream aggregation is needed.

## Reusable artifacts from the processor prototype

The processor code in `crates/core-nodes/src/processors/metric_sdkview_processor/`
should be removed, but these pieces are reusable:

| Artifact | Destination | Purpose |
|----------|-------------|---------|
| `IncludeExclude` type | `crates/config/` | Shared config type for attribute key filtering |
| `ViewMatcher` + glob matching | `crates/config/` or `crates/telemetry/` | Compile and match view selectors at init time |
| `ExtendedViewConfig::from_base()` | Utility | Convert base `ViewConfig` to extended form |
| `Aggregation::Drop` variant | `crates/config/` | Drop support in view stream config |

## Next Steps

1. **Extend `MetricStream`** in `crates/config/src/pipeline/telemetry/metrics/views.rs`
   with `attribute_keys: Option<IncludeExclude>` and `drop: Option<bool>`.

2. **Move `ViewMatcher`** to the telemetry or config crate so the ITR can
   use it at init time.

3. **For non-codegen metric_sets**: At OTAP assembly time in the ITR, match
   views and filter entity-level attributes, skip dropped sets, apply
   name/description overrides.

4. **For codegen metric_sets**: Pass the matched view's `attribute_keys` to
   the metric_set constructor alongside `MetricLevel`. The codegen template
   selects the appropriate dimension variant based on both the level and the
   view's retained attributes.

5. **Update `views_provider.rs`**: Translate the new `attribute_keys` to the
   OTel SDK's `AttributeFilter` (for the transition period while both paths
   coexist).

6. **Remove the processor**: Delete
   `crates/core-nodes/src/processors/metric_sdkview_processor/` and its
   registration in `processors/mod.rs`.
