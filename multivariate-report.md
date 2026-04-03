# Multivariate Metric Handling in OTAP

This report describes how the OTAP (OpenTelemetry Arrow Protocol) codebase
represents multivariate metrics — the case where a single logical observation
captures multiple related measurements that share the same entity, attributes,
and timestamp.

## Background

Traditional OpenTelemetry metrics are **univariate**: each metric instrument
is independent, and every time series (unique attribute combination) produces
its own stream of data points. When several counters or gauges always share the
same attribute tuple — for example, `send.count`, `send.error_full`, and
`send.error_closed` for a channel sender — the univariate model duplicates
attribute storage and loses the explicit grouping.

OTAP addresses this with a **multivariate metric set** concept at the SDK
layer, while the wire format currently remains univariate (with a multivariate
wire format defined but not yet populated).

## Two layers, two representations

### 1. Internal Telemetry SDK — native multivariate model

The SDK's core abstraction is the **metric set**, declared with a proc macro:

```rust
#[metric_set(name = "channel.sender")]
#[derive(Debug, Default, Clone)]
pub struct ChannelSenderMetrics {
    /// Count of messages successfully sent to the channel.
    #[metric(name = "send.count", unit = "{message}")]
    pub send_count: Counter<u64>,
    /// Count of send failures due to a full channel.
    #[metric(name = "send.error_full", unit = "{1}")]
    pub send_error_full: Counter<u64>,
    /// Count of send failures due to a closed channel.
    #[metric(name = "send.error_closed", unit = "{1}")]
    pub send_error_closed: Counter<u64>,
}
```

*(from `crates/engine/src/channel_metrics.rs`)*

The `#[metric_set]` macro generates a `MetricSetHandler` implementation that
provides:

- `descriptor()` — a static `MetricsDescriptor` with ordered field metadata
  (name, unit, instrument kind, brief description)
- `snapshot_values()` — returns a dense `Vec<MetricValue>` of all field values
  in declaration order
- `clear_values()` — resets all fields to zero
- `needs_flush()` — fast check for any non-zero field

*(macro implementation in `crates/telemetry-macros/src/lib.rs`)*

#### The "one row per multivariate observation" model

A single `MetricSetSnapshot` is the multivariate row:

```rust
pub struct MetricSetSnapshot {
    key: MetricSetKey,           // identifies the metric set instance
    metrics: Vec<MetricValue>,   // one value per instrument, in descriptor order
}
```

*(from `crates/telemetry/src/metrics.rs`)*

Each snapshot captures **all instruments for one entity + attribute-tuple at one
point in time**. The `MetricSetKey` links to an `EntityKey` that holds the
shared attributes. Multiple metric sets can share the same entity key — for
example, `PipelineMetrics` and `TokioRuntimeMetrics` both register against
the same pipeline entity.

#### Supported instrument types

The `MetricValue` enum carries the observation:

| Variant | Backing type | Instruments |
|---------|-------------|-------------|
| `U64(u64)` | Unsigned integer | Counter, UpDownCounter, Gauge |
| `F64(f64)` | Float | Counter, UpDownCounter, Gauge |
| `Mmsc(MmscSnapshot)` | Min/Max/Sum/Count | Mmsc (synthetic histogram) |

*(from `crates/telemetry/src/metrics.rs` and `crates/telemetry/src/descriptor.rs`)*

#### Collection and aggregation flow

1. **Hot path**: Per-core metric sets are mutated without atomics or contention.
   Each core owns its `MetricSet<M>` instance.
2. **Snapshot**: A `MetricsReporter` calls `snapshot()`, producing a
   `MetricSetSnapshot`, sends it over an MPSC channel, and clears the local
   values.
3. **Aggregation**: A `MetricsCollector` accumulates snapshots into a
   `MetricSetRegistry` (`SlotMap<MetricSetKey, MetricsEntry>`). Merge semantics
   depend on instrument type:
   - Delta counters: add
   - Cumulative sums: replace
   - Gauges: replace (last value wins)
   - MMSC: merge (min of mins, max of maxes, sum of sums, count of counts)
4. **Export**: The `MetricsDispatcher` iterates each registered metric set and
   fans out the multivariate snapshot into individual OTel instrument calls,
   attaching the same attribute set to each.

*(from `crates/telemetry/src/reporter.rs`, `crates/telemetry/src/collector.rs`,
and `crates/telemetry/src/metrics/dispatcher.rs`)*

#### Concrete examples of metric sets in the codebase

| Metric set struct | Name | Location |
|-------------------|------|----------|
| `ChannelSenderMetrics` | `channel.sender` | `crates/engine/src/channel_metrics.rs` |
| `ChannelReceiverMetrics` | `channel.receiver` | `crates/engine/src/channel_metrics.rs` |
| `ConsumedMetrics` | `node.consumer` | `crates/engine/src/channel_metrics.rs` |
| `ProducedMetrics` | `node.producer` | `crates/engine/src/channel_metrics.rs` |
| `PipelineMetrics` | `otelcol.pipeline` | `crates/engine/src/pipeline_metrics.rs` |
| `TokioRuntimeMetrics` | `otelcol.tokio.runtime` | `crates/engine/src/pipeline_metrics.rs` |

### 2. OTAP wire format — star-schema of Arrow tables (univariate)

On the wire, OTAP encodes metrics as a **star schema** of Arrow record batches.
The key tables for numeric metrics are:

#### Root table: `UnivariateMetrics`

One row per metric definition.

| Column | Type | Required |
|--------|------|----------|
| `id` | UInt16 | yes |
| `metric_type` | UInt8 | yes |
| `name` | Dictionary(U8, Utf8) | yes |
| `aggregation_temporality` | Dictionary(U8, Int32) | no |
| `description` | Dictionary(U8, Utf8) | no |
| `is_monotonic` | Boolean | no |
| `unit` | Dictionary(U8, Utf8) | no |
| `schema_url` | Dictionary(U8, Utf8) | no |
| `resource` | Struct | no |
| `scope` | Struct | no |

#### Child table: `NumberDataPoints`

One row per observation (one data point for one time series).

| Column | Type | Required |
|--------|------|----------|
| `parent_id` | UInt16 | yes |
| `start_time_unix_nano` | Timestamp(ns) | no |
| `time_unix_nano` | Timestamp(ns) | no |
| `int_value` | Int64 | no |
| `double_value` | Float64 | no |
| `id` | UInt32 | no |
| `flags` | UInt32 | no |

The `parent_id` column is a foreign key into the root table's `id`, linking
each data point to its metric definition.

#### Attribute tables

Attributes are stored in separate child batches (not as map columns):

| Column | Type |
|--------|------|
| `parent_id` | UInt16/UInt32 |
| `attribute_key` | Dictionary-encoded |
| `attribute_str` | Dictionary-encoded |
| `attribute_int` | Dictionary-encoded |
| `attribute_double` | Float64 |
| `attribute_bool` | Boolean |
| `attribute_bytes` | Dictionary-encoded |

*(schemas from `crates/pdata/src/schema/payloads.rs`)*

#### Example: one counter with three time series

Consider a counter `send.count` with three attribute combinations
(`endpoint=A`, `endpoint=B`, `endpoint=C`):

```text
UnivariateMetrics (root)
┌────┬─────────────┬──────────────┬──────┐
│ id │ metric_type │ name         │ unit │
├────┼─────────────┼──────────────┼──────┤
│  1 │ Sum         │ "send.count" │ {msg}│
└────┴─────────────┴──────────────┴──────┘

NumberDataPoints (child)
┌───────────┬──────────────────┬───────────┐
│ parent_id │ time_unix_nano   │ int_value │
├───────────┼──────────────────┼───────────┤
│         1 │ 1712100000000000 │        42 │  ← endpoint=A
│         1 │ 1712100000000000 │        17 │  ← endpoint=B
│         1 │ 1712100000000000 │         5 │  ← endpoint=C
└───────────┴──────────────────┴───────────┘

MetricAttrs (child of data points)
┌───────────┬───────────────┬───────────────┐
│ parent_id │ attribute_key │ attribute_str │
├───────────┼───────────────┼───────────────┤
│         0 │ "endpoint"    │ "A"           │
│         1 │ "endpoint"    │ "B"           │
│         2 │ "endpoint"    │ "C"           │
└───────────┴───────────────┴───────────────┘
```

Each attribute combination is a separate data-point row. The metric definition
is stored once in the root table.

### 3. The multivariate wire format gap

The OTAP proto defines both payload types:

```text
UNIVARIATE_METRICS  = 10   // currently used
MULTIVARIATE_METRICS = 25   // defined but schema is empty
```

*(from `proto/opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`)*

In the Rust code, `MultivariateMetrics` maps to `Schema::EMPTY`, and the
transport optimizer contains an explicit TODO:

```rust
// TODO handle multi variate metrics
```

*(from `crates/pdata/src/otap/transform/transport_optimize.rs`)*

The implementation gaps document confirms this:

> "OTLP and OTAP lack first-class multivariate metric sets — limits protocol
> efficiency; some semantics may be lossy"

*(from `docs/telemetry/implementation-gaps.md`)*

Both root types (`UnivariateMetrics` and `MultivariateMetrics`) share the same
child payload types (data-point tables, attribute tables). The code selects
the root type dynamically:

```rust
fn root_payload_type() -> ArrowPayloadType {
    // prefers MultivariateMetrics if present, falls back to UnivariateMetrics
}
```

*(from `crates/pdata/src/otap.rs`)*

## The bridge: SDK multivariate → wire univariate

The `MetricsDispatcher` performs the translation. For each flush cycle it:

1. Calls `visit_metrics_and_reset()` on the registry, which yields one
   multivariate metric set at a time: `(descriptor, attributes, MetricsIterator)`
2. Converts the shared attributes into OTel `KeyValue` pairs once
3. Iterates each `(MetricsField, MetricValue)` pair and records it as an
   independent OTel instrument call with those same attributes

So a metric set with 3 counters sharing attributes becomes 3 separate OTel
counter `.add()` calls, each with identical attribute vectors. The multivariate
grouping is lost at the wire boundary.

```rust
fn dispatch_metrics(&self) -> Result<(), Error> {
    self.metrics_handler
        .visit_metrics_and_reset(|descriptor, attributes, metrics_iter| {
            let meter = global::meter(descriptor.name);
            let otel_attributes = Self::to_opentelemetry_attributes(attributes);
            for (field, value) in metrics_iter {
                self.add_opentelemetry_metric(field, value, &otel_attributes, &meter);
            }
        });
    Ok(())
}
```

*(from `crates/telemetry/src/metrics/dispatcher.rs`)*

## Summary table

| Layer | Representation | "One row" meaning |
|-------|---------------|-------------------|
| **SDK** | `MetricSetSnapshot { key, Vec<MetricValue> }` | One snapshot = all instruments for one entity + attributes + timestamp |
| **OTAP wire (current)** | Star schema: root metric def + child data points | One row per data point per metric (univariate) |
| **OTAP wire (future)** | `MultivariateMetrics` payload type | Intended to carry grouped metrics — schema not yet defined |
| **Export to OTel** | Fan-out per instrument | Each instrument recorded independently with shared attributes |

## Design principles and future direction

From the telemetry documentation (`docs/telemetry/README.md`):

> The internal telemetry model and SDK natively support **multivariate metric
> sets**. This enables: efficient sharing of attribute tuples, coherent modeling
> of related measurements, reduced duplication explosion compared to naive
> univariate metrics. Multivariate metrics are treated as a fundamental modeling
> capability rather than a post-processing optimization.

The project intends to:

1. Contribute multivariate support to OpenTelemetry protocols
2. Use the internal SDK as a proving ground for the concept
3. Eventually auto-generate metric instrumentation APIs from a semantic
   convention registry via Weaver
4. Support metric-level detail control (`basic` / `normal` / `detailed`) that
   affects both attribute cardinality and aggregator type (MMSC → Histogram)

The `METRICS_SDK.md` design note describes the aspiration: a schema-driven
system where a single counter like "consumed requests" can be configured to
have zero dimensions at `basic` level (one counter in memory), one dimension
(`outcome`) at `normal` level (3 counters exported as 1 OTel metric with 3
attribute values), and two dimensions (`outcome` × `signal`) at `detailed`
level — all generated from a declarative schema.
