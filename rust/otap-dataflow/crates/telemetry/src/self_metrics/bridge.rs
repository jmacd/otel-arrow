// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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

/// Build a [`PrecomputedMetricSchema`] from an existing `MetricsDescriptor`.
///
/// Each field in the descriptor becomes one metric row and one data
/// point. Resource and scope IDs are set to `Some(0)` — the caller
/// must provide matching `ResourceAttrs` and `ScopeAttrs` child batches.
pub fn descriptor_to_schema(
    desc: &MetricsDescriptor,
) -> Result<PrecomputedMetricSchema, ArrowError> {
    let mut metrics_builder = MetricsRecordBatchBuilder::new();
    let mut parent_ids: Vec<u16> = Vec::with_capacity(desc.metrics.len());

    for (idx, field) in desc.metrics.iter().enumerate() {
        let metric_id = idx as u16;

        metrics_builder.append_id(metric_id);
        metrics_builder.append_metric_type(instrument_to_metric_type(field) as u8);
        metrics_builder.append_name(field.name.as_bytes());
        metrics_builder.append_description(field.brief.as_bytes());
        metrics_builder.append_unit(field.unit.as_bytes());
        metrics_builder.append_aggregation_temporality(field_aggregation_temporality(field));
        metrics_builder.append_is_monotonic(field_is_monotonic(field));

        // Resource and scope: one shared resource (id=0) and one
        // shared scope (id=0) for the entire metric set.
        metrics_builder.resource.append_id(Some(0));
        metrics_builder.scope.append_id(Some(0));

        // One data point per metric field, no per-point attributes.
        parent_ids.push(metric_id);
    }

    let total_points = parent_ids.len();
    let metrics_batch = metrics_builder.finish()?;

    // Empty attributes batch (no per-data-point dimensions).
    let mut attrs_builder = AttributesRecordBatchBuilder::<u32>::new();
    let attrs_batch = attrs_builder.finish()?;

    Ok(PrecomputedMetricSchema::from_parts(
        metrics_batch,
        attrs_batch,
        total_points,
        parent_ids,
    ))
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
                // Map attributes are stringified for now (matching
                // the existing MetricsDispatcher behavior).
                builder
                    .any_values_builder
                    .append_str(value.to_string_value().as_bytes());
            }
        }
    }

    builder.finish()
}

/// Extract integer counter values from a `MetricValue` slice.
///
/// Each value is mapped to `i64` for the NumberDataPoints `int_value`
/// column. F64 values are truncated. Mmsc snapshots use the sum.
#[must_use]
pub fn snapshot_to_int_values(values: &[MetricValue]) -> Vec<i64> {
    values
        .iter()
        .map(|v| match v {
            MetricValue::U64(n) => *n as i64,
            MetricValue::F64(n) => *n as i64,
            MetricValue::Mmsc(s) => s.sum as i64,
        })
        .collect()
}

/// Extract double values from a `MetricValue` slice.
///
/// Each value is mapped to `f64` for the NumberDataPoints `double_value`
/// column.
#[must_use]
pub fn snapshot_to_double_values(values: &[MetricValue]) -> Vec<f64> {
    values
        .iter()
        .map(|v| match v {
            MetricValue::U64(n) => *n as f64,
            MetricValue::F64(n) => *n,
            MetricValue::Mmsc(s) => s.sum,
        })
        .collect()
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

// --- internal helpers ---

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
        let schema = descriptor_to_schema(desc).expect("should build schema");

        // 3 metrics → 3 data points, 0 attributes
        assert_eq!(schema.total_points(), 3);
        assert_eq!(schema.metrics_batch().num_rows(), 3);
        assert_eq!(schema.attrs_batch().num_rows(), 0);
    }

    #[test]
    fn descriptor_to_schema_data_points_build() {
        let desc = test_descriptor();
        let schema = descriptor_to_schema(desc).expect("should build schema");
        let builder = schema.data_points_builder();

        let values = vec![
            MetricValue::U64(100),
            MetricValue::U64(95),
            MetricValue::U64(5),
        ];
        let int_values = snapshot_to_int_values(&values);

        let batch = builder
            .build_int_values_i64(1_000_000_000, 2_000_000_000, &int_values)
            .expect("should build data points");

        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn snapshot_to_int_values_mixed() {
        let values = vec![
            MetricValue::U64(42),
            MetricValue::F64(1.5),
            MetricValue::Mmsc(crate::instrument::MmscSnapshot {
                min: 1.0,
                max: 10.0,
                sum: 55.0,
                count: 10,
            }),
        ];
        let ints = snapshot_to_int_values(&values);
        assert_eq!(ints, vec![42, 1, 55]);
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
}
