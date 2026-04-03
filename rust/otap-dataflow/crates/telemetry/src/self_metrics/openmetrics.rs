//! OpenMetrics exposition format formatter.
//!
//! Reads OTAP Arrow metric batches (from a [`CumulativeEntry`]) and
//! produces OpenMetrics text format output suitable for Prometheus scraping.

use std::fmt::Write;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{Int64Type, UInt8Type, UInt16Type, UInt32Type};

use crate::self_metrics::accumulator::CumulativeEntry;

/// Format cumulative entries as OpenMetrics text.
///
/// Walks all entries, correlates data points by parent_id, and
/// resolves dimension attributes to produce standard OpenMetrics
/// exposition format. Entries from the same schema but different
/// entities produce separate metric lines.
#[must_use]
pub fn format_openmetrics(entries: &[CumulativeEntry]) -> String {
    let mut out = String::with_capacity(4096);

    if entries.is_empty() {
        write_eof(&mut out);
        return out;
    }

    for entry in entries {
        format_entry(&mut out, entry);
    }

    write_eof(&mut out);
    out
}

/// Format a single cumulative entry.
fn format_entry(out: &mut String, entry: &CumulativeEntry) {
    let metrics = &entry.metrics_batch;
    let data_points = &entry.data_points_batch;
    let attrs = &entry.attrs_batch;

    let num_metrics = metrics.num_rows();
    if num_metrics == 0 {
        return;
    }

    // Extract metric-level columns
    let metric_ids = get_u16_column(metrics, "id");
    let metric_names = get_string_column(metrics, "name");
    let metric_units = get_string_column(metrics, "unit");
    let metric_descs = get_string_column(metrics, "description");

    // Extract data point columns
    let dp_parent_ids = get_u16_column(data_points, "parent_id");
    let dp_int_values = get_optional_i64_column(data_points, "int_value");
    let dp_ids = get_u32_column(data_points, "id");

    // Extract attribute columns
    let attr_parent_ids = get_u32_column(attrs, "parent_id");
    let attr_keys = get_string_column(attrs, "key");
    let attr_str_values = get_string_column(attrs, "str");

    // For each metric, emit TYPE/UNIT/HELP + data points
    for m_row in 0..num_metrics {
        let metric_id = metric_ids[m_row];
        let name = &metric_names[m_row];
        let unit = &metric_units[m_row];
        let desc = &metric_descs[m_row];

        // OpenMetrics metric name: replace dots with underscores
        let om_name = name.replace('.', "_");

        // HELP line
        if !desc.is_empty() {
            let _ = writeln!(out, "# HELP {om_name} {desc}");
        }

        // TYPE line (counters only for now)
        let _ = writeln!(out, "# TYPE {om_name} counter");

        // UNIT line
        if !unit.is_empty() {
            let _ = writeln!(out, "# UNIT {om_name} {unit}");
        }

        // Find data points belonging to this metric
        for dp_row in 0..data_points.num_rows() {
            if dp_parent_ids[dp_row] != metric_id {
                continue;
            }

            let dp_id = dp_ids[dp_row];

            // Get the int counter value
            let value = if let Some(ref int_vals) = dp_int_values {
                if !int_vals.is_null(dp_row) {
                    format!("{}", int_vals.value(dp_row))
                } else {
                    "0".to_string()
                }
            } else {
                "0".to_string()
            };

            // Collect attributes for this data point
            let mut labels = Vec::new();
            for attr_row in 0..attrs.num_rows() {
                if attr_parent_ids[attr_row] == dp_id {
                    let key = &attr_keys[attr_row];
                    let val = &attr_str_values[attr_row];
                    labels.push(format!("{key}=\"{val}\""));
                }
            }

            // Emit the metric line
            if labels.is_empty() {
                let _ = writeln!(out, "{om_name}_total {value}");
            } else {
                let label_str = labels.join(",");
                let _ = writeln!(out, "{om_name}_total{{{label_str}}} {value}");
            }
        }

        // Blank line between metric families
        let _ = writeln!(out);
    }
}

fn write_eof(out: &mut String) {
    let _ = writeln!(out, "# EOF");
}

/// Get a UInt16 column by name, handling dictionary encoding.
fn get_u16_column(batch: &RecordBatch, name: &str) -> Vec<u16> {
    let idx = batch.schema().index_of(name).expect("column not found");
    let col = batch.column(idx);

    // Handle possible dictionary encoding from AdaptiveArrayBuilder
    if let Ok(casted) = arrow::compute::cast(col, &arrow::datatypes::DataType::UInt16) {
        return casted
            .as_primitive::<UInt16Type>()
            .iter()
            .map(|v| v.unwrap_or(0))
            .collect();
    }

    col.as_primitive::<UInt16Type>()
        .iter()
        .map(|v| v.unwrap_or(0))
        .collect()
}

/// Get a UInt32 column by name, handling dictionary encoding.
fn get_u32_column(batch: &RecordBatch, name: &str) -> Vec<u32> {
    let idx = batch.schema().index_of(name).expect("column not found");
    let col = batch.column(idx);

    if let Ok(casted) = arrow::compute::cast(col, &arrow::datatypes::DataType::UInt32) {
        return casted
            .as_primitive::<UInt32Type>()
            .iter()
            .map(|v| v.unwrap_or(0))
            .collect();
    }

    col.as_primitive::<UInt32Type>()
        .iter()
        .map(|v| v.unwrap_or(0))
        .collect()
}

/// Get a string column (possibly dictionary-encoded) by name.
fn get_string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let idx = match batch.schema().index_of(name) {
        Ok(i) => i,
        Err(_) => return vec![String::new(); batch.num_rows()],
    };
    let col = batch.column(idx);
    let len = col.len();

    // Try StringArray
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return (0..len)
            .map(|i| {
                if arr.is_null(i) {
                    String::new()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect();
    }

    // Try StringViewArray
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        return (0..len)
            .map(|i| {
                if arr.is_null(i) {
                    String::new()
                } else {
                    arr.value(i).to_string()
                }
            })
            .collect();
    }

    // Try Dictionary<UInt8, Utf8>
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<UInt8Type>>()
    {
        if let Some(values) = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            return (0..len)
                .map(|i| {
                    if dict.is_null(i) {
                        String::new()
                    } else {
                        let key = dict.keys().value(i) as usize;
                        values.value(key).to_string()
                    }
                })
                .collect();
        }
    }

    // Try Dictionary<UInt16, Utf8>
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<UInt16Type>>()
    {
        if let Some(values) = dict
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            return (0..len)
                .map(|i| {
                    if dict.is_null(i) {
                        String::new()
                    } else {
                        let key = dict.keys().value(i) as usize;
                        values.value(key).to_string()
                    }
                })
                .collect();
        }
    }

    // Fallback
    vec![String::new(); len]
}

/// Get an optional Int64 column by name.
fn get_optional_i64_column(batch: &RecordBatch, name: &str) -> Option<arrow::array::Int64Array> {
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
    Some(col.as_primitive::<Int64Type>().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::EntityKey;
    use crate::self_metrics::accumulator::{CumulativeAccumulator, MetricIdentity};
    use crate::self_metrics::precomputed::{CounterMetricDef, PrecomputedMetricSchema};

    const TEST_SCHEMA: &str = "test.consumer";

    fn test_schema() -> PrecomputedMetricSchema {
        PrecomputedMetricSchema::new(&[CounterMetricDef {
            name: "node.consumer.items",
            unit: "{item}",
            description: "Items consumed by this node.",
            num_points: 3,
            point_attributes: &[
                ("outcome", "success"),
                ("outcome", "failure"),
                ("outcome", "refused"),
            ],
            attrs_per_point: 1,
        }])
        .unwrap()
    }

    fn test_id() -> MetricIdentity {
        MetricIdentity {
            schema_key: TEST_SCHEMA,
            entity_key: EntityKey::default(),
        }
    }

    #[test]
    fn format_basic_counter() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(TEST_SCHEMA, schema.clone());

        let delta = schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[100, 5, 2])
            .unwrap();
        acc.ingest_delta(test_id(), &delta).unwrap();

        let text = format_openmetrics(&acc.snapshot());

        assert!(text.contains("# HELP node_consumer_items Items consumed by this node."));
        assert!(text.contains("# TYPE node_consumer_items counter"));
        assert!(text.contains("# UNIT node_consumer_items {item}"));
        assert!(text.contains("node_consumer_items_total{outcome=\"success\"} 100"));
        assert!(text.contains("node_consumer_items_total{outcome=\"failure\"} 5"));
        assert!(text.contains("node_consumer_items_total{outcome=\"refused\"} 2"));
        assert!(text.contains("# EOF"));
    }

    #[test]
    fn format_accumulated_values() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(TEST_SCHEMA, schema.clone());

        acc.ingest_delta(
            test_id(),
            &schema
                .data_points_builder()
                .build_int_values(1_000_000_000, 2_000_000_000, &[10, 5, 2])
                .unwrap(),
        )
        .unwrap();
        acc.ingest_delta(
            test_id(),
            &schema
                .data_points_builder()
                .build_int_values(1_000_000_000, 3_000_000_000, &[3, 1, 0])
                .unwrap(),
        )
        .unwrap();

        let text = format_openmetrics(&acc.snapshot());

        assert!(text.contains("node_consumer_items_total{outcome=\"success\"} 13"));
        assert!(text.contains("node_consumer_items_total{outcome=\"failure\"} 6"));
        assert!(text.contains("node_consumer_items_total{outcome=\"refused\"} 2"));
    }

    #[test]
    fn format_empty_snapshot() {
        let text = format_openmetrics(&[]);
        assert_eq!(text.trim(), "# EOF");
    }

    #[test]
    fn format_two_dimension_counter() {
        let schema_key = "test.2d";
        let schema = PrecomputedMetricSchema::new(&[CounterMetricDef {
            name: "node.consumer.items",
            unit: "{item}",
            description: "Items consumed.",
            num_points: 6,
            point_attributes: &[
                ("outcome", "success"),
                ("signal", "logs"),
                ("outcome", "success"),
                ("signal", "traces"),
                ("outcome", "failure"),
                ("signal", "logs"),
                ("outcome", "failure"),
                ("signal", "traces"),
                ("outcome", "refused"),
                ("signal", "logs"),
                ("outcome", "refused"),
                ("signal", "traces"),
            ],
            attrs_per_point: 2,
        }])
        .unwrap();

        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(schema_key, schema.clone());
        let id = MetricIdentity {
            schema_key,
            entity_key: EntityKey::default(),
        };

        acc.ingest_delta(
            id,
            &schema
                .data_points_builder()
                .build_int_values(1_000_000_000, 2_000_000_000, &[10, 20, 3, 4, 1, 0])
                .unwrap(),
        )
        .unwrap();

        let text = format_openmetrics(&acc.snapshot());

        assert!(text.contains("node_consumer_items_total{outcome=\"success\",signal=\"logs\"} 10"));
        assert!(
            text.contains("node_consumer_items_total{outcome=\"success\",signal=\"traces\"} 20")
        );
        assert!(text.contains("node_consumer_items_total{outcome=\"failure\",signal=\"logs\"} 3"));
    }
}
