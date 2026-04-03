//! Cumulative accumulator for delta OTAP metric batches.
//!
//! Holds a persistent cumulative state as Arrow RecordBatches.
//! Accepts delta NumberDataPoints batches and adds values column-wise
//! using Arrow compute kernels. The metrics and attributes tables are
//! precomputed and immutable — only the data points accumulate.

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::kernels::numeric::add;
use arrow::error::ArrowError;
use std::sync::Arc;

use crate::precomputed::PrecomputedMetricSchema;

/// Column indices in the NumberDataPoints RecordBatch.
/// These match the schema produced by `NumberDataPointsRecordBatchBuilder`.
mod ndp_cols {
    /// id: UInt32
    #[allow(dead_code)]
    pub const ID: usize = 0;
    /// parent_id: UInt16
    #[allow(dead_code)]
    pub const PARENT_ID: usize = 1;
    /// start_time_unix_nano: Timestamp(Nanosecond)
    #[allow(dead_code)]
    pub const START_TIME: usize = 2;
    /// time_unix_nano: Timestamp(Nanosecond)
    pub const TIME: usize = 3;
    /// int_value: Int64
    pub const INT_VALUE: usize = 4;
    /// double_value: Float64
    pub const DOUBLE_VALUE: usize = 5;
    /// flags: UInt32
    #[allow(dead_code)]
    pub const FLAGS: usize = 6;
}

/// Cumulative accumulator for delta OTAP metric batches.
///
/// Stores the running cumulative state as a NumberDataPoints RecordBatch.
/// On each delta ingestion, the int_value (or double_value) columns are
/// added element-wise using Arrow compute kernels. The time_unix_nano
/// column is updated to the latest delta timestamp.
///
/// The metrics table and attributes table are precomputed from the schema
/// and never change — they are simply cloned on scrape.
pub struct CumulativeAccumulator {
    /// The precomputed schema (metrics + attributes tables).
    schema: PrecomputedMetricSchema,
    /// The running cumulative NumberDataPoints batch, or None before
    /// the first delta arrives.
    cumulative_dp: Option<RecordBatch>,
}

impl CumulativeAccumulator {
    /// Create a new accumulator for the given precomputed schema.
    #[must_use]
    pub fn new(schema: PrecomputedMetricSchema) -> Self {
        Self {
            schema,
            cumulative_dp: None,
        }
    }

    /// Ingest a delta NumberDataPoints batch.
    ///
    /// Adds the delta's int_value and double_value columns to the
    /// cumulative state. Updates time_unix_nano from the delta.
    ///
    /// The delta batch must have the same schema and row count as
    /// the precomputed schema's total_points.
    pub fn ingest_delta(&mut self, delta_dp: &RecordBatch) -> Result<(), ArrowError> {
        match &self.cumulative_dp {
            None => {
                // First delta becomes the initial cumulative state
                self.cumulative_dp = Some(delta_dp.clone());
            }
            Some(cumulative) => {
                let mut cols: Vec<ArrayRef> = cumulative.columns().to_vec();

                // Add int_value columns element-wise
                let cum_int = cumulative.column(ndp_cols::INT_VALUE);
                let delta_int = delta_dp.column(ndp_cols::INT_VALUE);
                cols[ndp_cols::INT_VALUE] = add_nullable_numeric(cum_int, delta_int)?;

                // Add double_value columns element-wise
                let cum_dbl = cumulative.column(ndp_cols::DOUBLE_VALUE);
                let delta_dbl = delta_dp.column(ndp_cols::DOUBLE_VALUE);
                cols[ndp_cols::DOUBLE_VALUE] = add_nullable_numeric(cum_dbl, delta_dbl)?;

                // Update time_unix_nano from delta (latest timestamp wins)
                cols[ndp_cols::TIME] = Arc::clone(delta_dp.column(ndp_cols::TIME));

                self.cumulative_dp =
                    Some(RecordBatch::try_new(cumulative.schema(), cols)?);
            }
        }
        Ok(())
    }

    /// Get a snapshot of the current cumulative state.
    ///
    /// Returns clones of the metrics batch, attributes batch, and
    /// cumulative data points batch. Arrow cloning is cheap (ref-counted
    /// column buffers).
    ///
    /// Returns `None` if no delta has been ingested yet.
    #[must_use]
    pub fn snapshot(&self) -> Option<CumulativeSnapshot> {
        let dp = self.cumulative_dp.as_ref()?;
        Some(CumulativeSnapshot {
            metrics_batch: self.schema.metrics_batch().clone(),
            attrs_batch: self.schema.attrs_batch().clone(),
            data_points_batch: dp.clone(),
        })
    }
}

/// A point-in-time snapshot of cumulative metric state.
///
/// All three batches are cheaply cloned (Arc-backed column buffers).
pub struct CumulativeSnapshot {
    /// The metrics table (one row per metric).
    pub metrics_batch: RecordBatch,
    /// The dimension attributes table.
    pub attrs_batch: RecordBatch,
    /// The cumulative NumberDataPoints table.
    pub data_points_batch: RecordBatch,
}

/// Add two nullable numeric arrays element-wise.
///
/// Handles the case where both arrays may have nulls (e.g., int_value is
/// null when double_value is used, and vice versa). Null + Null = Null,
/// Null + X = X, X + Null = X.
fn add_nullable_numeric(
    cumulative: &ArrayRef,
    delta: &ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    // If both are entirely null, return as-is
    if cumulative.null_count() == cumulative.len() && delta.null_count() == delta.len() {
        return Ok(Arc::clone(cumulative));
    }

    // If cumulative is entirely null, the delta is the new state
    if cumulative.null_count() == cumulative.len() {
        return Ok(Arc::clone(delta));
    }

    // If delta is entirely null, cumulative is unchanged
    if delta.null_count() == delta.len() {
        return Ok(Arc::clone(cumulative));
    }

    // Both have some non-null values — use Arrow add
    add(cumulative, delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int64Array};
    use crate::precomputed::{CounterMetricDef, PrecomputedMetricSchema};

    fn test_schema() -> PrecomputedMetricSchema {
        PrecomputedMetricSchema::new(
            &[CounterMetricDef {
                name: "test.counter",
                unit: "{item}",
                description: "test",
                num_points: 3,
                point_attributes: &[
                    ("outcome", "success"),
                    ("outcome", "failure"),
                    ("outcome", "refused"),
                ],
                attrs_per_point: 1,
            }],
            "test",
        )
        .unwrap()
    }

    fn build_delta(schema: &PrecomputedMetricSchema, values: &[u64]) -> RecordBatch {
        schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, values)
            .unwrap()
    }

    #[test]
    fn first_delta_becomes_cumulative() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new(schema.clone());

        assert!(acc.snapshot().is_none());

        let delta = build_delta(&schema, &[10, 5, 2]);
        acc.ingest_delta(&delta).unwrap();

        let snap = acc.snapshot().expect("should have snapshot");
        let int_col = snap
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 10);
        assert_eq!(int_col.value(1), 5);
        assert_eq!(int_col.value(2), 2);
    }

    #[test]
    fn accumulates_multiple_deltas() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new(schema.clone());

        acc.ingest_delta(&build_delta(&schema, &[10, 5, 2]))
            .unwrap();
        acc.ingest_delta(&build_delta(&schema, &[3, 1, 0]))
            .unwrap();
        acc.ingest_delta(&build_delta(&schema, &[7, 0, 1]))
            .unwrap();

        let snap = acc.snapshot().unwrap();
        let int_col = snap
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 20); // 10+3+7
        assert_eq!(int_col.value(1), 6); // 5+1+0
        assert_eq!(int_col.value(2), 3); // 2+0+1
    }

    #[test]
    fn snapshot_is_cheap_clone() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new(schema.clone());

        acc.ingest_delta(&build_delta(&schema, &[10, 5, 2]))
            .unwrap();

        let snap1 = acc.snapshot().unwrap();
        let snap2 = acc.snapshot().unwrap();

        // Both snapshots should have the same values
        let col1 = snap1
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col2 = snap2
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col1.values(), col2.values());
    }

    #[test]
    fn snapshot_includes_precomputed_tables() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new(schema.clone());

        acc.ingest_delta(&build_delta(&schema, &[1, 2, 3]))
            .unwrap();

        let snap = acc.snapshot().unwrap();
        // Metrics table: 1 metric
        assert_eq!(snap.metrics_batch.num_rows(), 1);
        // Attrs table: 3 points × 1 attr = 3
        assert_eq!(snap.attrs_batch.num_rows(), 3);
        // Data points: 3
        assert_eq!(snap.data_points_batch.num_rows(), 3);
    }

    #[test]
    fn double_value_column_stays_null_for_int_counters() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new(schema.clone());

        acc.ingest_delta(&build_delta(&schema, &[10, 5, 2]))
            .unwrap();
        acc.ingest_delta(&build_delta(&schema, &[3, 1, 0]))
            .unwrap();

        let snap = acc.snapshot().unwrap();
        let dbl_col = snap
            .data_points_batch
            .column(ndp_cols::DOUBLE_VALUE)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // All double values should be null for int counters
        assert_eq!(dbl_col.null_count(), 3);
    }
}
