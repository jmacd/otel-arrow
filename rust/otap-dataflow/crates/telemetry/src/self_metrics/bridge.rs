//! Bridge from existing `MetricsDescriptor` / `MetricValue` types to
//! OTAP-native `PrecomputedMetricSchema`.
//!
//! This module converts the metadata carried by `#[metric_set]` structs
//! into precomputed Arrow batches suitable for OTAP encoding, without
//! requiring codegen. Each `MetricsField` maps to one data point with
//! zero per-point attributes (current metric sets have no dimensions).
//!
//! Resource and scope are represented as `id = Some(0)` with
//! corresponding `ResourceAttrs` / `ScopeAttrs` child batches. The
//! pipeline batch processor handles ID reindexing when merging payloads.

use arrow::array::RecordBatch;
use arrow::error::ArrowError;

use otap_df_pdata::encode::record::attributes::AttributesRecordBatchBuilder;
use otap_df_pdata::encode::record::metrics::MetricsRecordBatchBuilder;
use otap_df_pdata::otlp::metrics::MetricType;
use otap_df_pdata::proto::opentelemetry::metrics::v1::AggregationTemporality;

use crate::attributes::{AttributeSetHandler, AttributeValue};
use crate::descriptor::{
    Instrument, MetricValueType, MetricsDescriptor, MetricsField, Temporality,
};
use crate::metrics::MetricValue;
use crate::self_metrics::precomputed::PrecomputedMetricSchema;

use otap_df_config::pipeline::telemetry::metrics::views::ViewConfig;

/// Whether a data point value should be added to the cumulative
/// state or replace it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulationMode {
    /// Pointwise add (delta counters, Mmsc sum/count).
    Add,
    /// Replace with latest value (gauges, cumulative counters, Mmsc min/max).
    Replace,
}

/// Result of building a precomputed schema from a descriptor.
pub struct DescriptorSchema {
    /// The precomputed OTAP schema.
    pub schema: PrecomputedMetricSchema,
    /// Per-data-point accumulation mode (same length as total_points).
    pub accumulation_modes: Vec<AccumulationMode>,
}

/// Build a [`PrecomputedMetricSchema`] from an existing `MetricsDescriptor`.
///
/// Equivalent to `descriptor_to_schema_with_views(desc, &[])`.
pub fn descriptor_to_schema(desc: &MetricsDescriptor) -> Result<DescriptorSchema, ArrowError> {
    descriptor_to_schema_with_views(desc, &[])
}

/// Build a [`PrecomputedMetricSchema`] from an existing `MetricsDescriptor`,
/// applying view transformations.
///
/// Each non-Mmsc field becomes one metric row and one data point.
/// Each Mmsc field expands to 4 metrics: `_min` (gauge), `_max` (gauge),
/// `_sum` (counter), `_count` (counter), matching the admin endpoint.
///
/// Views are applied at init time: if a view selector matches
/// `(descriptor.name, field.name)`, the stream's name and/or description
/// replace the original values in the precomputed metrics table.
///
/// Resource and scope IDs are set to `Some(0)` — the caller must
/// provide matching `ResourceAttrs` and `ScopeAttrs` child batches.
pub fn descriptor_to_schema_with_views(
    desc: &MetricsDescriptor,
    views: &[ViewConfig],
) -> Result<DescriptorSchema, ArrowError> {
    let mut metrics_builder = MetricsRecordBatchBuilder::new();
    let mut parent_ids: Vec<u16> = Vec::new();
    let mut accumulation_modes: Vec<AccumulationMode> = Vec::new();
    let mut metric_id: u16 = 0;

    for field in desc.metrics.iter() {
        // Find matching view for this (scope, instrument) pair.
        let view = find_matching_view(views, desc.name, field.name);
        let effective_name = view
            .and_then(|v| v.stream.name.as_deref())
            .unwrap_or(field.name);
        let effective_description = view
            .and_then(|v| v.stream.description.as_deref())
            .unwrap_or(field.brief);

        match field.instrument {
            Instrument::Mmsc => {
                // Expand Mmsc into 4 separate metrics.
                let sub_metrics = [
                    (
                        format!("{effective_name}.min"),
                        MetricType::Gauge,
                        None,
                        None,
                        AccumulationMode::Replace,
                    ),
                    (
                        format!("{effective_name}.max"),
                        MetricType::Gauge,
                        None,
                        None,
                        AccumulationMode::Replace,
                    ),
                    (
                        format!("{effective_name}.sum"),
                        MetricType::Sum,
                        Some(AggregationTemporality::Cumulative as i32),
                        Some(true),
                        AccumulationMode::Add,
                    ),
                    (
                        format!("{effective_name}.count"),
                        MetricType::Sum,
                        Some(AggregationTemporality::Cumulative as i32),
                        Some(true),
                        AccumulationMode::Add,
                    ),
                ];

                for (name, metric_type, temporality, monotonic, mode) in &sub_metrics {
                    metrics_builder.append_id(metric_id);
                    metrics_builder.append_metric_type(*metric_type as u8);
                    metrics_builder.append_name(name.as_bytes());
                    metrics_builder.append_description(effective_description.as_bytes());
                    metrics_builder.append_unit(field.unit.as_bytes());
                    metrics_builder.append_aggregation_temporality(*temporality);
                    metrics_builder.append_is_monotonic(*monotonic);
                    metrics_builder.resource.append_id(Some(0));
                    metrics_builder.scope.append_id(Some(0));
                    metrics_builder
                        .scope
                        .append_name(Some(desc.name.as_bytes()));

                    parent_ids.push(metric_id);
                    accumulation_modes.push(*mode);
                    metric_id += 1;
                }
            }
            _ => {
                metrics_builder.append_id(metric_id);
                metrics_builder.append_metric_type(instrument_to_metric_type(field) as u8);
                metrics_builder.append_name(effective_name.as_bytes());
                metrics_builder.append_description(effective_description.as_bytes());
                metrics_builder.append_unit(field.unit.as_bytes());
                metrics_builder
                    .append_aggregation_temporality(field_aggregation_temporality(field));
                metrics_builder.append_is_monotonic(field_is_monotonic(field));
                metrics_builder.resource.append_id(Some(0));
                metrics_builder.scope.append_id(Some(0));
                metrics_builder
                    .scope
                    .append_name(Some(desc.name.as_bytes()));

                parent_ids.push(metric_id);
                accumulation_modes.push(field_accumulation_mode(field));
                metric_id += 1;
            }
        }
    }

    let total_points = parent_ids.len();
    let metrics_batch = metrics_builder.finish()?;

    let mut attrs_builder = AttributesRecordBatchBuilder::<u32>::new();
    let attrs_batch = attrs_builder.finish()?;

    Ok(DescriptorSchema {
        schema: PrecomputedMetricSchema::from_parts(
            metrics_batch,
            attrs_batch,
            total_points,
            parent_ids,
        ),
        accumulation_modes,
    })
}

/// Build a `ResourceAttrs` child batch from key-value pairs.
///
/// All rows have `parent_id = 0` (single resource).
pub fn build_resource_attrs(attrs: &[(&str, &str)]) -> Result<RecordBatch, ArrowError> {
    let mut builder = AttributesRecordBatchBuilder::<u16>::new();

    for (key, value) in attrs {
        builder.append_parent_id(&0u16);
        builder.append_key(key.as_bytes());
        builder.any_values_builder.append_str(value.as_bytes());
    }

    builder.finish()
}

/// Build a `ScopeAttrs` child batch from an `AttributeSetHandler`.
///
/// All rows have `parent_id = 0` (single scope). The scope name is
/// taken from the descriptor name.
pub fn build_scope_attrs_from_entity(
    attrs: &dyn AttributeSetHandler,
) -> Result<RecordBatch, ArrowError> {
    let mut builder = AttributesRecordBatchBuilder::<u16>::new();

    for (key, value) in attrs.iter_attributes() {
        builder.append_parent_id(&0u16);
        builder.append_key(key.as_bytes());
        match value {
            AttributeValue::String(s) => {
                builder.any_values_builder.append_str(s.as_bytes());
            }
            AttributeValue::Int(v) => {
                builder.any_values_builder.append_int(*v);
            }
            AttributeValue::UInt(v) => {
                builder.any_values_builder.append_int(*v as i64);
            }
            AttributeValue::Double(v) => {
                builder.any_values_builder.append_double(*v);
            }
            AttributeValue::Boolean(v) => {
                builder.any_values_builder.append_bool(*v);
            }
            AttributeValue::Map(_) => {
                builder
                    .any_values_builder
                    .append_str(value.to_string_value().as_bytes());
            }
        }
    }

    builder.finish()
}

/// Extract values from a `MetricValue` slice, expanding Mmsc fields.
///
/// Mmsc values expand to 4 data points (min, max, sum, count).
/// All values are mapped to `i64` for the NumberDataPoints `int_value`
/// column.
#[must_use]
pub fn expand_snapshot(desc: &MetricsDescriptor, values: &[MetricValue]) -> Vec<i64> {
    let mut result = Vec::new();
    for (field, value) in desc.metrics.iter().zip(values.iter()) {
        match (field.instrument, value) {
            (Instrument::Mmsc, MetricValue::Mmsc(s)) => {
                result.push(s.min as i64);
                result.push(s.max as i64);
                result.push(s.sum as i64);
                result.push(s.count as i64);
            }
            (Instrument::Mmsc, _) => {
                result.extend_from_slice(&[0, 0, 0, 0]);
            }
            (_, MetricValue::U64(n)) => result.push(*n as i64),
            (_, MetricValue::F64(n)) => result.push(*n as i64),
            (_, MetricValue::Mmsc(s)) => result.push(s.sum as i64),
        }
    }
    result
}

/// Returns true if all values in the snapshot are zero.
#[must_use]
pub fn snapshot_all_zeros(values: &[MetricValue]) -> bool {
    values.iter().all(|v| v.is_zero())
}

/// Whether the descriptor's fields are all integer-typed.
#[must_use]
pub fn descriptor_is_all_int(desc: &MetricsDescriptor) -> bool {
    desc.metrics
        .iter()
        .all(|f| f.value_type == MetricValueType::U64)
}

/// Find a matching view for a given scope name and instrument name.
///
/// Returns the first view whose selector matches. A selector field
/// that is `None` matches any value (wildcard).
fn find_matching_view<'a>(
    views: &'a [ViewConfig],
    scope_name: &str,
    instrument_name: &str,
) -> Option<&'a ViewConfig> {
    views.iter().find(|v| {
        let scope_ok = v
            .selector
            .scope_name
            .as_ref()
            .is_none_or(|s| s == scope_name);
        let name_ok = v
            .selector
            .instrument_name
            .as_ref()
            .is_none_or(|n| n == instrument_name);
        scope_ok && name_ok
    })
}

fn instrument_to_metric_type(field: &MetricsField) -> MetricType {
    match field.instrument {
        Instrument::Counter | Instrument::UpDownCounter => MetricType::Sum,
        Instrument::Gauge => MetricType::Gauge,
        Instrument::Histogram | Instrument::Mmsc => MetricType::Histogram,
    }
}

fn field_aggregation_temporality(field: &MetricsField) -> Option<i32> {
    match field.instrument {
        Instrument::Counter | Instrument::UpDownCounter => {
            let temp = match field.temporality {
                Some(Temporality::Cumulative) => AggregationTemporality::Cumulative,
                Some(Temporality::Delta) | None => AggregationTemporality::Delta,
            };
            Some(temp as i32)
        }
        Instrument::Gauge | Instrument::Histogram | Instrument::Mmsc => None,
    }
}

fn field_is_monotonic(field: &MetricsField) -> Option<bool> {
    match field.instrument {
        Instrument::Counter => Some(true),
        Instrument::UpDownCounter => Some(false),
        _ => None,
    }
}

fn field_accumulation_mode(field: &MetricsField) -> AccumulationMode {
    match field.instrument {
        Instrument::Counter if field.temporality == Some(Temporality::Delta) => {
            AccumulationMode::Add
        }
        Instrument::Gauge => AccumulationMode::Replace,
        _ => AccumulationMode::Replace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::{MetricsDescriptor, MetricsField};

    fn test_descriptor() -> &'static MetricsDescriptor {
        static DESC: MetricsDescriptor = MetricsDescriptor {
            name: "test.metrics",
            metrics: &[
                MetricsField {
                    name: "items.received",
                    unit: "{item}",
                    brief: "Number of items received",
                    instrument: Instrument::Counter,
                    temporality: Some(Temporality::Delta),
                    value_type: MetricValueType::U64,
                },
                MetricsField {
                    name: "items.sent",
                    unit: "{item}",
                    brief: "Number of items sent",
                    instrument: Instrument::Counter,
                    temporality: Some(Temporality::Delta),
                    value_type: MetricValueType::U64,
                },
                MetricsField {
                    name: "queue.size",
                    unit: "{item}",
                    brief: "Current queue depth",
                    instrument: Instrument::Gauge,
                    temporality: None,
                    value_type: MetricValueType::U64,
                },
            ],
        };
        &DESC
    }

    #[test]
    fn descriptor_to_schema_basic() {
        let desc = test_descriptor();
        let ds = descriptor_to_schema(desc).expect("should build schema");

        assert_eq!(ds.schema.total_points(), 3);
        assert_eq!(ds.schema.metrics_batch().num_rows(), 3);
        assert_eq!(ds.schema.attrs_batch().num_rows(), 0);
        assert_eq!(ds.accumulation_modes[0], AccumulationMode::Add);
        assert_eq!(ds.accumulation_modes[1], AccumulationMode::Add);
        assert_eq!(ds.accumulation_modes[2], AccumulationMode::Replace);
    }

    #[test]
    fn descriptor_to_schema_data_points_build() {
        let desc = test_descriptor();
        let ds = descriptor_to_schema(desc).expect("should build schema");
        let builder = ds.schema.data_points_builder();

        let values = vec![
            MetricValue::U64(100),
            MetricValue::U64(95),
            MetricValue::U64(5),
        ];
        let int_values = expand_snapshot(desc, &values);

        let batch = builder
            .build_int_values_i64(1_000_000_000, 2_000_000_000, &int_values)
            .expect("should build data points");

        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn expand_snapshot_with_mmsc() {
        static DESC: MetricsDescriptor = MetricsDescriptor {
            name: "test.mixed",
            metrics: &[
                MetricsField {
                    name: "items",
                    unit: "{item}",
                    brief: "Items",
                    instrument: Instrument::Counter,
                    temporality: Some(Temporality::Delta),
                    value_type: MetricValueType::U64,
                },
                MetricsField {
                    name: "latency",
                    unit: "ms",
                    brief: "Latency",
                    instrument: Instrument::Mmsc,
                    temporality: Some(Temporality::Delta),
                    value_type: MetricValueType::F64,
                },
            ],
        };

        let values = vec![
            MetricValue::U64(42),
            MetricValue::Mmsc(crate::instrument::MmscSnapshot {
                min: 1.0,
                max: 10.0,
                sum: 55.0,
                count: 10,
            }),
        ];
        let expanded = expand_snapshot(&DESC, &values);
        assert_eq!(expanded, vec![42, 1, 10, 55, 10]);
    }

    #[test]
    fn mmsc_schema_expansion() {
        static DESC: MetricsDescriptor = MetricsDescriptor {
            name: "test.mmsc",
            metrics: &[MetricsField {
                name: "latency",
                unit: "ms",
                brief: "Latency",
                instrument: Instrument::Mmsc,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::F64,
            }],
        };

        let ds = descriptor_to_schema(&DESC).expect("should build");
        assert_eq!(ds.schema.metrics_batch().num_rows(), 4);
        assert_eq!(ds.schema.total_points(), 4);
        assert_eq!(ds.accumulation_modes[0], AccumulationMode::Replace);
        assert_eq!(ds.accumulation_modes[1], AccumulationMode::Replace);
        assert_eq!(ds.accumulation_modes[2], AccumulationMode::Add);
        assert_eq!(ds.accumulation_modes[3], AccumulationMode::Add);
    }

    #[test]
    fn snapshot_all_zeros_detection() {
        let zeros = vec![MetricValue::U64(0), MetricValue::U64(0)];
        assert!(snapshot_all_zeros(&zeros));

        let nonzero = vec![MetricValue::U64(0), MetricValue::U64(1)];
        assert!(!snapshot_all_zeros(&nonzero));
    }

    #[test]
    fn build_resource_attrs_basic() {
        let attrs = [
            ("service.name", "otap-dataflow"),
            ("service.version", "0.1"),
        ];
        let batch = build_resource_attrs(&attrs).expect("should build");
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn descriptor_is_all_int_check() {
        let desc = test_descriptor();
        assert!(descriptor_is_all_int(desc));
    }

    #[test]
    fn view_renames_metric() {
        use otap_df_config::pipeline::telemetry::metrics::views::{
            MetricSelector, MetricStream, ViewConfig,
        };

        let desc = test_descriptor();
        let views = vec![ViewConfig {
            selector: MetricSelector {
                scope_name: Some("test.metrics".to_string()),
                instrument_name: Some("items.received".to_string()),
            },
            stream: MetricStream {
                name: Some("http.requests.total".to_string()),
                description: Some("Total HTTP requests".to_string()),
            },
        }];

        let ds = descriptor_to_schema_with_views(desc, &views).expect("should build");

        // 3 metrics, same as without views.
        assert_eq!(ds.schema.metrics_batch().num_rows(), 3);

        // Verify the first metric was renamed via OpenMetrics output.
        let mut acc = crate::self_metrics::accumulator::CumulativeAccumulator::new();
        acc.register_schema("test", ds.schema.clone());
        let dp = ds
            .schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[10, 5, 3])
            .unwrap();
        acc.ingest_delta(
            crate::self_metrics::accumulator::MetricIdentity {
                schema_key: "test",
                entity_key: crate::registry::EntityKey::default(),
            },
            &dp,
        )
        .unwrap();
        let text = crate::self_metrics::openmetrics::format_openmetrics(&acc.snapshot());

        // Renamed metric.
        assert!(text.contains("http_requests_total_total 10"));
        assert!(text.contains("Total HTTP requests"));
        // Unrenamed metrics still present.
        assert!(text.contains("items_sent_total 5"));
        assert!(text.contains("queue_size 3"));
    }

    #[test]
    fn view_renames_mmsc_metric() {
        use otap_df_config::pipeline::telemetry::metrics::views::{
            MetricSelector, MetricStream, ViewConfig,
        };

        static DESC: MetricsDescriptor = MetricsDescriptor {
            name: "test.mmsc",
            metrics: &[MetricsField {
                name: "latency",
                unit: "ms",
                brief: "Latency",
                instrument: Instrument::Mmsc,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::F64,
            }],
        };

        let views = vec![ViewConfig {
            selector: MetricSelector {
                scope_name: None,
                instrument_name: Some("latency".to_string()),
            },
            stream: MetricStream {
                name: Some("process_duration".to_string()),
                description: Some("Processing duration".to_string()),
            },
        }];

        let ds = descriptor_to_schema_with_views(&DESC, &views).expect("should build");
        // Mmsc expands to 4 sub-metrics with renamed base.
        assert_eq!(ds.schema.metrics_batch().num_rows(), 4);

        let mut acc = crate::self_metrics::accumulator::CumulativeAccumulator::new();
        acc.register_schema("test", ds.schema.clone());
        let dp = ds
            .schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[1, 10, 55, 10])
            .unwrap();
        acc.ingest_delta(
            crate::self_metrics::accumulator::MetricIdentity {
                schema_key: "test",
                entity_key: crate::registry::EntityKey::default(),
            },
            &dp,
        )
        .unwrap();
        let text = crate::self_metrics::openmetrics::format_openmetrics(&acc.snapshot());

        assert!(text.contains("process_duration_min"));
        assert!(text.contains("process_duration_max"));
        assert!(text.contains("process_duration_sum"));
        assert!(text.contains("process_duration_count"));
        assert!(text.contains("Processing duration"));
        // Original name should NOT appear.
        assert!(!text.contains("latency_min"));
    }

    #[test]
    fn view_wildcard_scope_matches_any() {
        use otap_df_config::pipeline::telemetry::metrics::views::{
            MetricSelector, MetricStream, ViewConfig,
        };

        let desc = test_descriptor();
        let views = vec![ViewConfig {
            selector: MetricSelector {
                scope_name: None, // wildcard
                instrument_name: Some("queue.size".to_string()),
            },
            stream: MetricStream {
                name: Some("buffer.depth".to_string()),
                description: None, // keep original
            },
        }];

        let ds = descriptor_to_schema_with_views(desc, &views).expect("should build");

        let mut acc = crate::self_metrics::accumulator::CumulativeAccumulator::new();
        acc.register_schema("test", ds.schema.clone());
        let dp = ds
            .schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[10, 5, 3])
            .unwrap();
        acc.ingest_delta(
            crate::self_metrics::accumulator::MetricIdentity {
                schema_key: "test",
                entity_key: crate::registry::EntityKey::default(),
            },
            &dp,
        )
        .unwrap();
        let text = crate::self_metrics::openmetrics::format_openmetrics(&acc.snapshot());

        assert!(text.contains("buffer_depth"));
        // Original description preserved.
        assert!(text.contains("Current queue depth"));
        assert!(!text.contains("queue_size"));
    }
}
