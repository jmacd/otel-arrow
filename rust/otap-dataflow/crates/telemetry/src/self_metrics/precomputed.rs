// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Precomputed OTAP schema types for the internal metrics SDK.
//!
//! For each metric set, OTAP encoding produces 3 tables:
//!
//! 1. **Metrics table** — one row per metric (name, type, unit, temporality,
//!    monotonic). Fully determined by the schema; precomputed at init time.
//! 2. **Attributes table** — one row per (data_point, attribute_key) pair
//!    for dimension attributes. Fully determined by the schema; precomputed.
//! 3. **DataPoints table** — one row per data point. Built at runtime from
//!    counter/gauge/histogram snapshots during each collection tick.
//!
//! This module provides types for holding the precomputed tables and for
//! building the runtime data points table.

use arrow::array::{Int64Builder, UInt16Builder, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Holds the precomputed metrics and attributes record batches for a
/// single metric set at a specific [`MetricLevel`].
///
/// The metrics and attributes tables are fully determined by the schema
/// and the active MetricLevel, so they can be constructed once at startup.
/// Only the data points table needs to be built per collection tick.
///
/// [`MetricLevel`]: otap_df_config::policy::MetricLevel
#[derive(Clone, Debug)]
pub struct PrecomputedMetricSchema {
    /// The metrics table (one row per metric). Static after init.
    pub metrics_batch: RecordBatch,
    /// The attributes table (dimension attrs per data point). Static after init.
    pub attrs_batch: Option<RecordBatch>,
    /// Number of data points per metric (product of active dimension cardinalities).
    pub points_per_metric: Vec<usize>,
    /// Total number of data points across all metrics.
    pub total_points: usize,
    /// Precomputed parent_ids for the data points table: which metric
    /// (by row index in the metrics table) each data point belongs to.
    pub parent_ids: Vec<u16>,
}

/// Builds a NumberDataPoints RecordBatch from a flat snapshot of counter/gauge values.
///
/// The parent_ids and point count are precomputed from the schema.
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
        let n = precomputed.total_points;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("parent_id", DataType::UInt16, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Int64,
                true,
            ),
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

    /// Fill data point values from a flat counter snapshot.
    ///
    /// `values` must have length == total_points from the schema.
    /// Each entry corresponds to one data point in schema order.
    pub fn set_int_values(
        &mut self,
        start_time_ns: i64,
        time_ns: i64,
        values: &[u64],
    ) {
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
    fn ndp_builder_basic() {
        let precomputed = PrecomputedMetricSchema {
            metrics_batch: make_empty_metrics_batch(),
            attrs_batch: None,
            points_per_metric: vec![3],
            total_points: 3,
            parent_ids: vec![0, 0, 0],
        };

        let mut builder = NumberDataPointsBuilder::new(&precomputed);
        builder.set_int_values(100, 200, &[10, 20, 30]);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[0, 1, 2]);

        let parent_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(parent_ids.values(), &[0, 0, 0]);

        let values = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[10, 20, 30]);
    }

    #[test]
    fn ndp_builder_multi_metric() {
        let precomputed = PrecomputedMetricSchema {
            metrics_batch: make_empty_metrics_batch(),
            attrs_batch: None,
            points_per_metric: vec![2, 3],
            total_points: 5,
            parent_ids: vec![0, 0, 1, 1, 1],
        };

        let mut builder = NumberDataPointsBuilder::new(&precomputed);
        builder.set_int_values(0, 1000, &[1, 2, 3, 4, 5]);

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 5);

        let parent_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(parent_ids.values(), &[0, 0, 1, 1, 1]);
    }
}
