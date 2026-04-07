// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Precomputed OTAP schema types for the internal metrics SDK.
//!
//! For each metric set at a given level, OTAP encoding produces:
//!
//! 1. **Metrics table** — `num_metrics × num_scopes` rows. Each row has a
//!    scope.id referencing the scope that carries its dimension attributes.
//!    Metric name/description/unit are dictionary-encoded (stored once).
//!    **Precomputed at init time.**
//! 2. **ScopeAttrs table** — `num_scopes × num_dimensions` rows. Each row
//!    is one (scope_id, attribute_key, attribute_value) triple.
//!    **Precomputed at init time.**
//! 3. **NumberDataPoints table** — `num_metrics × num_scopes` rows. One
//!    data point per metric row. **Built at runtime per collection tick.**
//!
//! The metrics and scope tables are built once and cloned by Arc reference
//! on each tick. Only the NDP table is rebuilt (timestamps + values).

use arrow::array::{Int64Builder, UInt16Builder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use otap_df_pdata::encode::record::attributes::AttributesRecordBatchBuilder;
use otap_df_pdata::encode::record::metrics::MetricsRecordBatchBuilder;
use otap_df_pdata::otlp::metrics::MetricType;
use std::sync::Arc;

/// Metadata for one metric in a metric set (used to build precomputed tables).
#[derive(Debug, Clone)]
pub struct MetricInfo {
    /// Metric name (e.g., "node.consumer.items").
    pub name: &'static str,
    /// Metric description.
    pub description: &'static str,
    /// Unit string.
    pub unit: &'static str,
    /// OTAP metric type (Sum, Gauge, ExponentialHistogram, ...).
    pub metric_type: MetricType,
    /// Aggregation temporality (1=Delta, 2=Cumulative). None for Gauge.
    pub aggregation_temporality: Option<i32>,
    /// Whether the metric is monotonic (only for Sum).
    pub is_monotonic: Option<bool>,
}

/// Metadata for one dimension in a metric set.
#[derive(Debug, Clone)]
pub struct DimensionInfo {
    /// Attribute key (e.g., "outcome").
    pub key: &'static str,
    /// Enumerated values in index order.
    pub values: &'static [&'static str],
}

/// Holds the precomputed tables for a single metric set at a specific level.
///
/// The metrics table and scope attrs table are fully determined by the
/// schema and the active MetricLevel. They are constructed once at startup.
/// Only the NDP table needs to be built per collection tick.
#[derive(Clone, Debug)]
pub struct PrecomputedMetricSchema {
    /// The metrics table: `num_metrics × num_scopes` rows.
    /// Dictionary-encoded columns for name, description, unit, etc.
    pub metrics_batch: RecordBatch,
    /// The scope attributes table: `num_scopes × num_dimensions` rows.
    /// Empty if no dimensions are active at this level.
    pub scope_attrs_batch: Option<RecordBatch>,
    /// Number of scopes (unique dimension combinations) at this level.
    pub num_scopes: usize,
    /// Number of metrics in the set.
    pub num_metrics: usize,
    /// Total rows in metrics table = `num_metrics × num_scopes`.
    /// Also the number of NDP rows per tick.
    pub total_rows: usize,
    /// Precomputed parent_ids for the NDP table.
    /// `parent_ids[i]` = the metric row index that NDP row i belongs to.
    pub parent_ids: Vec<u16>,
}

impl PrecomputedMetricSchema {
    /// Build precomputed tables from metric and dimension metadata.
    ///
    /// The `active_dimensions` slice selects which dimensions (by index
    /// into `dimensions`) are active at this level. The number of scopes
    /// is the product of active dimension cardinalities.
    pub fn build(
        metrics: &[MetricInfo],
        dimensions: &[DimensionInfo],
        active_dimensions: &[usize],
    ) -> Result<Self, ArrowError> {
        // Compute scope count.
        let num_scopes = if active_dimensions.is_empty() {
            1
        } else {
            active_dimensions
                .iter()
                .map(|&i| dimensions[i].values.len())
                .product()
        };
        let num_metrics = metrics.len();
        let total_rows = num_metrics * num_scopes;

        // ── Build metrics table ─────────────────────────────────────
        // num_metrics × num_scopes rows, ordered as:
        // metric0[scope0], metric0[scope1], ..., metric1[scope0], ...
        let mut mb = MetricsRecordBatchBuilder::new();
        let mut row_id: u16 = 0;
        for metric in metrics {
            for scope_id in 0..num_scopes {
                mb.append_id(row_id);
                mb.scope.append_id(Some(scope_id as u16));
                mb.append_metric_type(metric.metric_type as u8);
                mb.append_name(metric.name.as_bytes());
                mb.append_description(metric.description.as_bytes());
                mb.append_unit(metric.unit.as_bytes());
                mb.append_aggregation_temporality(metric.aggregation_temporality);
                mb.append_is_monotonic(metric.is_monotonic);
                row_id += 1;
            }
        }
        let metrics_batch = mb.finish()?;

        // ── Build scope attrs table ─────────────────────────────────
        // num_scopes × num_active_dims rows.
        let scope_attrs_batch = if active_dimensions.is_empty() {
            None
        } else {
            let mut ab = AttributesRecordBatchBuilder::<u16>::new();
            for scope_id in 0..num_scopes {
                // Decompose scope_id into per-dimension indices.
                let mut remaining = scope_id;
                // Walk dimensions in reverse to extract indices (row-major).
                let mut dim_indices = vec![0usize; active_dimensions.len()];
                for i in (0..active_dimensions.len()).rev() {
                    let card = dimensions[active_dimensions[i]].values.len();
                    dim_indices[i] = remaining % card;
                    remaining /= card;
                }
                // Emit one row per dimension.
                for (pos, &dim_idx) in active_dimensions.iter().enumerate() {
                    let dim = &dimensions[dim_idx];
                    let value = dim.values[dim_indices[pos]];
                    ab.append_parent_id(&(scope_id as u16));
                    ab.append_key(dim.key.as_bytes());
                    ab.any_values_builder.append_str(value.as_bytes());
                }
            }
            Some(ab.finish()?)
        };

        // ── Build parent_ids for NDP ────────────────────────────────
        // One NDP row per metric table row.
        let parent_ids: Vec<u16> = (0..total_rows as u16).collect();

        Ok(Self {
            metrics_batch,
            scope_attrs_batch,
            num_scopes,
            num_metrics,
            total_rows,
            parent_ids,
        })
    }
}

/// Builds a NumberDataPoints RecordBatch from a flat snapshot of values.
///
/// The parent_ids and row count are precomputed from the schema.
/// The hot path is: set timestamps + fill values → `finish()`.
pub struct NumberDataPointsBuilder {
    schema: Arc<Schema>,
    parent_ids: Vec<u16>,
    id_builder: UInt32Builder,
    parent_id_builder: UInt16Builder,
    start_time_builder: Int64Builder,
    time_builder: Int64Builder,
    int_value_builder: Int64Builder,
}

impl NumberDataPointsBuilder {
    /// Create from a precomputed schema.
    #[must_use]
    pub fn new(precomputed: &PrecomputedMetricSchema) -> Self {
        let n = precomputed.total_rows;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("start_time_unix_nano", DataType::Int64, true),
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("int_value", DataType::Int64, true),
        ]));

        Self {
            schema,
            parent_ids: precomputed.parent_ids.clone(),
            id_builder: UInt32Builder::with_capacity(n),
            parent_id_builder: UInt16Builder::with_capacity(n),
            start_time_builder: Int64Builder::with_capacity(n),
            time_builder: Int64Builder::with_capacity(n),
            int_value_builder: Int64Builder::with_capacity(n),
        }
    }

    /// Fill data point values from a flat snapshot.
    ///
    /// `values` layout: for each metric, `num_scopes` values in scope order.
    /// Total length = `num_metrics × num_scopes`.
    pub fn set_int_values(&mut self, start_time_ns: i64, time_ns: i64, values: &[u64]) {
        debug_assert_eq!(values.len(), self.parent_ids.len());

        for (i, (&parent_id, &value)) in
            self.parent_ids.iter().zip(values.iter()).enumerate()
        {
            self.id_builder.append_value(i as u32);
            self.parent_id_builder.append_value(parent_id);
            self.start_time_builder.append_value(start_time_ns);
            self.time_builder.append_value(time_ns);
            self.int_value_builder.append_value(value as i64);
        }
    }

    /// Build the NumberDataPoints RecordBatch.
    pub fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(self.id_builder.finish()),
            Arc::new(self.parent_id_builder.finish()),
            Arc::new(self.start_time_builder.finish()),
            Arc::new(self.time_builder.finish()),
            Arc::new(self.int_value_builder.finish()),
        ];
        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, UInt16Array, UInt32Array};

    fn make_empty_metrics_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::UInt16,
            false,
        )]));
        RecordBatch::new_empty(schema)
    }

    #[test]
    fn ndp_builder_basic_level() {
        // Basic: 2 metrics × 1 scope = 2 rows
        let precomputed = PrecomputedMetricSchema {
            metrics_batch: make_empty_metrics_batch(),
            scope_attrs_batch: None,
            num_scopes: 1,
            num_metrics: 2,
            total_rows: 2,
            parent_ids: vec![0, 1],
        };

        let mut builder = NumberDataPointsBuilder::new(&precomputed);
        builder.set_int_values(100, 200, &[42, 17]);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let parent_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(parent_ids.values(), &[0, 1]);

        let values = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[42, 17]);
    }

    #[test]
    fn ndp_builder_normal_level() {
        // Normal: 2 metrics × 3 scopes = 6 rows
        // Layout: metric0[scope0,scope1,scope2], metric1[scope0,scope1,scope2]
        let precomputed = PrecomputedMetricSchema {
            metrics_batch: make_empty_metrics_batch(),
            scope_attrs_batch: None,
            num_scopes: 3,
            num_metrics: 2,
            total_rows: 6,
            parent_ids: vec![0, 1, 2, 3, 4, 5],
        };

        let mut builder = NumberDataPointsBuilder::new(&precomputed);
        builder.set_int_values(0, 1000, &[1, 2, 3, 4, 5, 6]);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 6);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[0, 1, 2, 3, 4, 5]);
    }
}
