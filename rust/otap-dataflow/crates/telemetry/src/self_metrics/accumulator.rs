//! Cumulative accumulator for delta OTAP metric batches.
//!
//! Maintains a `BTreeMap<MetricIdentity, RecordBatch>` of cumulative
//! state, keyed by `(schema_key, entity_key)`. Each entry is a
//! per-identity cumulative NumberDataPoints batch. Delta arrivals are
//! added pointwise using Arrow compute kernels, matched by identity.

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::kernels::numeric::add;
use arrow::error::ArrowError;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::registry::EntityKey;

use crate::self_metrics::precomputed::PrecomputedMetricSchema;

/// Column indices in the NumberDataPoints RecordBatch.
/// Matches the output of `NumberDataPointsRecordBatchBuilder::finish()`.
mod ndp_cols {
    /// time_unix_nano: Timestamp(Nanosecond)
    pub const TIME: usize = 3;
    /// int_value: Int64 (nullable)
    pub const INT_VALUE: usize = 4;
    /// double_value: Float64 (nullable)
    pub const DOUBLE_VALUE: usize = 5;
}

/// Identifies a metric stream: which schema and which entity (node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetricIdentity {
    /// Schema name from the YAML definition (e.g., `"pipeline.consumer"`).
    pub schema_key: &'static str,
    /// The node's entity key, carrying scope attributes.
    pub entity_key: EntityKey,
}

/// Cumulative accumulator for delta OTAP metric batches.
///
/// Stores per-identity cumulative state as NumberDataPoints RecordBatches.
/// On each delta ingestion, the int_value column is added pointwise
/// using Arrow compute kernels, matched by `MetricIdentity`.
pub struct CumulativeAccumulator {
    /// Per-identity cumulative data points.
    state: BTreeMap<MetricIdentity, RecordBatch>,
    /// Registered schemas keyed by schema_key name.
    schemas: BTreeMap<&'static str, PrecomputedMetricSchema>,
}

impl CumulativeAccumulator {
    /// Create a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: BTreeMap::new(),
            schemas: BTreeMap::new(),
        }
    }

    /// Register a schema. Must be called before ingesting deltas for
    /// that schema_key.
    pub fn register_schema(&mut self, schema_key: &'static str, schema: PrecomputedMetricSchema) {
        let _ = self.schemas.insert(schema_key, schema);
    }

    /// Returns true if a schema with the given key has been registered.
    #[must_use]
    pub fn has_schema(&self, schema_key: &str) -> bool {
        self.schemas.contains_key(schema_key)
    }

    /// Ingest a delta NumberDataPoints batch for a specific identity.
    pub fn ingest_delta(
        &mut self,
        identity: MetricIdentity,
        delta_dp: &RecordBatch,
    ) -> Result<(), ArrowError> {
        match self.state.get(&identity) {
            None => {
                let _ = self.state.insert(identity, delta_dp.clone());
            }
            Some(cumulative) => {
                let mut cols: Vec<ArrayRef> = cumulative.columns().to_vec();

                // Accumulate int_value column.
                cols[ndp_cols::INT_VALUE] = add(
                    cumulative.column(ndp_cols::INT_VALUE),
                    delta_dp.column(ndp_cols::INT_VALUE),
                )?;

                // Accumulate double_value column.
                cols[ndp_cols::DOUBLE_VALUE] = add(
                    cumulative.column(ndp_cols::DOUBLE_VALUE),
                    delta_dp.column(ndp_cols::DOUBLE_VALUE),
                )?;

                cols[ndp_cols::TIME] = Arc::clone(delta_dp.column(ndp_cols::TIME));

                let _ = self
                    .state
                    .insert(identity, RecordBatch::try_new(cumulative.schema(), cols)?);
            }
        }
        Ok(())
    }

    /// Ingest a batch with per-data-point accumulation modes.
    ///
    /// Each element in `modes` specifies whether the corresponding data
    /// point should be accumulated (Add) or replaced (Replace).
    pub fn ingest_with_modes(
        &mut self,
        identity: MetricIdentity,
        delta_dp: &RecordBatch,
        modes: &[crate::self_metrics::bridge::AccumulationMode],
    ) -> Result<(), ArrowError> {
        match self.state.get(&identity) {
            None => {
                let _ = self.state.insert(identity, delta_dp.clone());
            }
            Some(cumulative) => {
                let mut cols: Vec<ArrayRef> = cumulative.columns().to_vec();
                let n = cumulative.num_rows();

                // Accumulate int_value with modes.
                let cum_int = cumulative.column(ndp_cols::INT_VALUE);
                let delta_int = delta_dp.column(ndp_cols::INT_VALUE);
                cols[ndp_cols::INT_VALUE] =
                    accumulate_with_modes_i64(cum_int, delta_int, modes, n);

                // Accumulate double_value with modes.
                let cum_dbl = cumulative.column(ndp_cols::DOUBLE_VALUE);
                let delta_dbl = delta_dp.column(ndp_cols::DOUBLE_VALUE);
                cols[ndp_cols::DOUBLE_VALUE] =
                    accumulate_with_modes_f64(cum_dbl, delta_dbl, modes, n);

                cols[ndp_cols::TIME] = Arc::clone(delta_dp.column(ndp_cols::TIME));

                let _ = self
                    .state
                    .insert(identity, RecordBatch::try_new(cumulative.schema(), cols)?);
            }
        }
        Ok(())
    }

    /// Get a snapshot of the current cumulative state.
    #[must_use]
    pub fn snapshot(&self) -> Vec<CumulativeEntry> {
        self.state
            .iter()
            .filter_map(|(identity, dp_batch)| {
                let schema = self.schemas.get(identity.schema_key)?;
                Some(CumulativeEntry {
                    identity: *identity,
                    metrics_batch: schema.metrics_batch().clone(),
                    attrs_batch: schema.attrs_batch().clone(),
                    data_points_batch: dp_batch.clone(),
                })
            })
            .collect()
    }
}

/// Accumulate i64 values with per-element modes. Handles nullable arrays.
fn accumulate_with_modes_i64(
    cum: &ArrayRef,
    delta: &ArrayRef,
    modes: &[crate::self_metrics::bridge::AccumulationMode],
    n: usize,
) -> ArrayRef {
    use crate::self_metrics::bridge::AccumulationMode;
    let cum = cum
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("int_value column");
    let delta = delta
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("int_value column");

    let merged: arrow::array::Int64Array = (0..n)
        .map(|i| {
            if cum.is_null(i) && delta.is_null(i) {
                None
            } else {
                let mode = modes.get(i).copied().unwrap_or(AccumulationMode::Add);
                let c = if cum.is_null(i) { 0 } else { cum.value(i) };
                let d = if delta.is_null(i) { 0 } else { delta.value(i) };
                Some(match mode {
                    AccumulationMode::Add => c + d,
                    AccumulationMode::Replace => d,
                })
            }
        })
        .collect();
    Arc::new(merged)
}

/// Accumulate f64 values with per-element modes. Handles nullable arrays.
fn accumulate_with_modes_f64(
    cum: &ArrayRef,
    delta: &ArrayRef,
    modes: &[crate::self_metrics::bridge::AccumulationMode],
    n: usize,
) -> ArrayRef {
    use crate::self_metrics::bridge::AccumulationMode;
    let cum = cum
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("double_value column");
    let delta = delta
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("double_value column");

    let merged: arrow::array::Float64Array = (0..n)
        .map(|i| {
            if cum.is_null(i) && delta.is_null(i) {
                None
            } else {
                let mode = modes.get(i).copied().unwrap_or(AccumulationMode::Add);
                let c = if cum.is_null(i) { 0.0 } else { cum.value(i) };
                let d = if delta.is_null(i) { 0.0 } else { delta.value(i) };
                Some(match mode {
                    AccumulationMode::Add => c + d,
                    AccumulationMode::Replace => d,
                })
            }
        })
        .collect();
    Arc::new(merged)
}

impl Default for CumulativeAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// A single entry in a cumulative snapshot.
pub struct CumulativeEntry {
    /// The identity of this metric stream.
    pub identity: MetricIdentity,
    /// The metrics table (one row per metric in this schema).
    pub metrics_batch: RecordBatch,
    /// The dimension attributes table for this schema.
    pub attrs_batch: RecordBatch,
    /// The cumulative NumberDataPoints table for this identity.
    pub data_points_batch: RecordBatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_metrics::precomputed::{CounterMetricDef, PrecomputedMetricSchema};
    use arrow::array::{Array, Int64Array};

    const SCHEMA_A: &str = "test.consumer";
    const SCHEMA_B: &str = "test.producer";

    fn test_schema() -> PrecomputedMetricSchema {
        PrecomputedMetricSchema::new(&[CounterMetricDef {
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
        }])
        .unwrap()
    }

    fn build_delta(schema: &PrecomputedMetricSchema, values: &[u64]) -> RecordBatch {
        schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, values)
            .unwrap()
    }

    fn identity_a() -> MetricIdentity {
        MetricIdentity {
            schema_key: SCHEMA_A,
            entity_key: EntityKey::default(),
        }
    }

    #[test]
    fn first_delta_becomes_cumulative() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(SCHEMA_A, schema.clone());

        assert!(acc.snapshot().is_empty());

        acc.ingest_delta(identity_a(), &build_delta(&schema, &[10, 5, 2]))
            .unwrap();

        let entries = acc.snapshot();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].identity, identity_a());

        let int_col = entries[0]
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
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(SCHEMA_A, schema.clone());
        let id = identity_a();

        acc.ingest_delta(id, &build_delta(&schema, &[10, 5, 2]))
            .unwrap();
        acc.ingest_delta(id, &build_delta(&schema, &[3, 1, 0]))
            .unwrap();
        acc.ingest_delta(id, &build_delta(&schema, &[7, 0, 1]))
            .unwrap();

        let entries = acc.snapshot();
        let int_col = entries[0]
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 20);
        assert_eq!(int_col.value(1), 6);
        assert_eq!(int_col.value(2), 3);
    }

    #[test]
    fn multiple_identities_independent() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(SCHEMA_A, schema.clone());
        acc.register_schema(SCHEMA_B, schema.clone());

        let id_a = MetricIdentity {
            schema_key: SCHEMA_A,
            entity_key: EntityKey::default(),
        };
        let id_b = MetricIdentity {
            schema_key: SCHEMA_B,
            entity_key: EntityKey::default(),
        };

        acc.ingest_delta(id_a, &build_delta(&schema, &[100, 50, 20]))
            .unwrap();
        acc.ingest_delta(id_b, &build_delta(&schema, &[1, 2, 3]))
            .unwrap();

        let entries = acc.snapshot();
        assert_eq!(entries.len(), 2);

        let a_vals: Vec<i64> = entries
            .iter()
            .find(|e| e.identity == id_a)
            .unwrap()
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(a_vals, vec![100, 50, 20]);

        let b_vals: Vec<i64> = entries
            .iter()
            .find(|e| e.identity == id_b)
            .unwrap()
            .data_points_batch
            .column(ndp_cols::INT_VALUE)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(b_vals, vec![1, 2, 3]);
    }

    #[test]
    fn snapshot_includes_precomputed_tables() {
        let schema = test_schema();
        let mut acc = CumulativeAccumulator::new();
        acc.register_schema(SCHEMA_A, schema.clone());

        acc.ingest_delta(identity_a(), &build_delta(&schema, &[1, 2, 3]))
            .unwrap();

        let entries = acc.snapshot();
        assert_eq!(entries[0].metrics_batch.num_rows(), 1);
        assert_eq!(entries[0].attrs_batch.num_rows(), 3);
        assert_eq!(entries[0].data_points_batch.num_rows(), 3);
    }
}
