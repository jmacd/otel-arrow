//! Precomputed metric schema and runtime data point builder.
//!
//! For each metric set, the OTAP encoding produces 3 tables:
//! 1. **Metrics table** — one row per metric (name, type, unit, etc.).
//!    Precomputed at init time.
//! 2. **Attributes table** — dimension attributes per data point.
//!    Precomputed at init time.
//! 3. **NumberDataPoints table** — counter values. Built at runtime.
//!
//! This module provides types for building and holding these tables.

use arrow::array::{RecordBatch, TimestampNanosecondArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use std::sync::Arc;

use otap_df_pdata::encode::record::attributes::AttributesRecordBatchBuilder;
use otap_df_pdata::encode::record::metrics::MetricsRecordBatchBuilder;
use otap_df_pdata::otlp::metrics::MetricType;
use otap_df_pdata::proto::opentelemetry::metrics::v1::AggregationTemporality;

/// Description of a single counter metric for precomputation.
pub struct CounterMetricDef {
    /// OTel metric name (e.g., `"node.consumer.items"`).
    pub name: &'static str,
    /// Metric unit (e.g., `"{item}"`).
    pub unit: &'static str,
    /// Metric description.
    pub description: &'static str,
    /// Number of data points this metric produces (product of active
    /// dimension cardinalities).
    pub num_points: usize,
    /// Number of attributes per data point.
    pub attrs_per_point: usize,
    /// Attribute key/value pairs for each data point, in order.
    /// Each entry is `(key, value)`. The length must equal
    /// `num_points * attrs_per_point`.
    pub point_attributes: &'static [(&'static str, &'static str)],
}

/// Holds precomputed metrics and attributes record batches for a metric set.
#[derive(Clone)]
pub struct PrecomputedMetricSchema {
    /// The metrics table (one row per counter).
    metrics_batch: RecordBatch,
    /// The data-point attributes table. Parent IDs are u32 matching
    /// NumberDataPoints IDs.
    attrs_batch: RecordBatch,
    /// Total number of data points across all metrics.
    total_points: usize,
    /// Precomputed parent_id for each data point.
    parent_ids: Vec<u16>,
}

impl PrecomputedMetricSchema {
    /// Build the precomputed schema from a list of counter metric definitions.
    pub fn new(metrics: &[CounterMetricDef]) -> Result<Self, ArrowError> {
        let mut metrics_builder = MetricsRecordBatchBuilder::new();
        let mut attrs_builder = AttributesRecordBatchBuilder::<u32>::new();
        let mut parent_ids: Vec<u16> = Vec::new();
        let mut dp_id: u32 = 0;

        for (metric_idx, def) in metrics.iter().enumerate() {
            let metric_id = metric_idx as u16;

            // Metrics table row: metric identity only, no resource/scope.
            metrics_builder.append_id(metric_id);
            metrics_builder.append_metric_type(MetricType::Sum as u8);
            metrics_builder.append_name(def.name.as_bytes());
            metrics_builder.append_description(def.description.as_bytes());
            metrics_builder.append_unit(def.unit.as_bytes());
            metrics_builder
                .append_aggregation_temporality(Some(AggregationTemporality::Delta as i32));
            metrics_builder.append_is_monotonic(Some(true));

            // Resource and scope: append only a placeholder id so the
            // StructArray builders produce valid (non-empty) arrays.
            // All other resource/scope fields are contextual and
            // assembled at the receiver/export boundary.
            metrics_builder.resource.append_id(Some(0));
            metrics_builder.scope.append_id(Some(0));

            // Data points and their dimension attributes
            for point_idx in 0..def.num_points {
                parent_ids.push(metric_id);

                // Build attribute rows for this data point
                let attr_start = point_idx * def.attrs_per_point;
                for attr_offset in 0..def.attrs_per_point {
                    let (key, value) = def.point_attributes[attr_start + attr_offset];
                    attrs_builder.append_parent_id(&dp_id);
                    attrs_builder.append_key(key.as_bytes());
                    attrs_builder
                        .any_values_builder
                        .append_str(value.as_bytes());
                }

                dp_id += 1;
            }
        }

        let total_points = parent_ids.len();
        let metrics_batch = metrics_builder.finish()?;
        let attrs_batch = attrs_builder.finish()?;

        Ok(Self {
            metrics_batch,
            attrs_batch,
            total_points,
            parent_ids,
        })
    }

    /// The precomputed metrics record batch.
    #[must_use]
    pub fn metrics_batch(&self) -> &RecordBatch {
        &self.metrics_batch
    }

    /// The precomputed attributes record batch.
    #[must_use]
    pub fn attrs_batch(&self) -> &RecordBatch {
        &self.attrs_batch
    }

    /// Total number of data points across all metrics.
    #[must_use]
    pub fn total_points(&self) -> usize {
        self.total_points
    }

    /// Create a `PrecomputedMetricSchema` from pre-built parts.
    ///
    /// Used by the bridge module which builds the metrics and attrs
    /// batches directly from `MetricsDescriptor` rather than from
    /// `CounterMetricDef`.
    pub fn from_parts(
        metrics_batch: RecordBatch,
        attrs_batch: RecordBatch,
        total_points: usize,
        parent_ids: Vec<u16>,
    ) -> Self {
        Self {
            metrics_batch,
            attrs_batch,
            total_points,
            parent_ids,
        }
    }

    /// Create a new data points builder for this schema.
    #[must_use]
    pub fn data_points_builder(&self) -> CounterDataPointsBuilder {
        CounterDataPointsBuilder::new(self)
    }
}

/// Builds the NumberDataPoints record batch at runtime from counter
/// snapshots.
///
/// The builder is initialized from a [`PrecomputedMetricSchema`] and
/// reused across collection ticks. Call [`set_int_values`] or
/// [`set_double_values`] to fill in the counter values, then [`finish`]
/// to produce the record batch.
pub struct CounterDataPointsBuilder {
    parent_ids: Vec<u16>,
    total_points: usize,
}

impl CounterDataPointsBuilder {
    fn new(schema: &PrecomputedMetricSchema) -> Self {
        Self {
            parent_ids: schema.parent_ids.clone(),
            total_points: schema.total_points,
        }
    }

    /// Build a NumberDataPoints record batch from integer counter values.
    ///
    /// Produces a minimal batch with only the columns needed for int
    /// counters: id, parent_id, start_time_unix_nano, time_unix_nano,
    /// and int_value. The double_value and flags columns are omitted
    /// since they are unused for integer counters.
    pub fn build_int_values(
        &self,
        start_time_unix_nano: i64,
        time_unix_nano: i64,
        values: &[u64],
    ) -> Result<RecordBatch, ArrowError> {
        assert_eq!(
            values.len(),
            self.total_points,
            "values length must match total_points"
        );

        let n = self.total_points;
        let ids: Vec<u32> = (0..n as u32).collect();
        let start_times: Vec<i64> = vec![start_time_unix_nano; n];
        let times: Vec<i64> = vec![time_unix_nano; n];
        let int_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("parent_id", DataType::UInt16, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("int_value", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(ids)),
                Arc::new(UInt16Array::from(self.parent_ids.clone())),
                Arc::new(TimestampNanosecondArray::from(start_times)),
                Arc::new(TimestampNanosecondArray::from(times)),
                Arc::new(arrow::array::Int64Array::from(int_values)),
            ],
        )
    }

    /// Build a NumberDataPoints record batch from pre-converted i64 values.
    ///
    /// Same as [`build_int_values`] but accepts `&[i64]` directly,
    /// avoiding the u64→i64 conversion when the caller has already
    /// performed it (e.g., from the bridge module).
    pub fn build_int_values_i64(
        &self,
        start_time_unix_nano: i64,
        time_unix_nano: i64,
        values: &[i64],
    ) -> Result<RecordBatch, ArrowError> {
        assert_eq!(
            values.len(),
            self.total_points,
            "values length must match total_points"
        );

        let n = self.total_points;
        let ids: Vec<u32> = (0..n as u32).collect();
        let start_times: Vec<i64> = vec![start_time_unix_nano; n];
        let times: Vec<i64> = vec![time_unix_nano; n];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("parent_id", DataType::UInt16, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("int_value", DataType::Int64, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(ids)),
                Arc::new(UInt16Array::from(self.parent_ids.clone())),
                Arc::new(TimestampNanosecondArray::from(start_times)),
                Arc::new(TimestampNanosecondArray::from(times)),
                Arc::new(arrow::array::Int64Array::from(values.to_vec())),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precomputed_schema_basic_counter() {
        let metrics = [CounterMetricDef {
            name: "test.counter",
            unit: "{item}",
            description: "A test counter",
            num_points: 3,
            point_attributes: &[
                ("outcome", "success"),
                ("outcome", "failure"),
                ("outcome", "refused"),
            ],
            attrs_per_point: 1,
        }];

        let schema = PrecomputedMetricSchema::new(&metrics).expect("should build schema");

        assert_eq!(schema.total_points(), 3);
        assert_eq!(schema.metrics_batch().num_rows(), 1);
        assert_eq!(schema.attrs_batch().num_rows(), 3);
    }

    #[test]
    fn precomputed_schema_two_dimensions() {
        // 1 metric with outcome(3) × signal_type(3) = 9 points, 2 attrs each = 18 attr rows
        let mut attrs = Vec::new();
        let outcomes = ["success", "failure", "refused"];
        let signals = ["traces", "metrics", "logs"];
        for outcome in &outcomes {
            for signal in &signals {
                attrs.push(("outcome", *outcome));
                attrs.push(("signal_type", *signal));
            }
        }
        let attrs_static: Vec<(&'static str, &'static str)> = attrs.into_iter().collect();

        let metrics = [CounterMetricDef {
            name: "node.consumer.items",
            unit: "{item}",
            description: "Items consumed",
            num_points: 9,
            point_attributes: attrs_static.leak(),
            attrs_per_point: 2,
        }];

        let schema = PrecomputedMetricSchema::new(&metrics).expect("should build schema");

        assert_eq!(schema.total_points(), 9);
        assert_eq!(schema.metrics_batch().num_rows(), 1);
        // 9 points × 2 attrs = 18 attribute rows
        assert_eq!(schema.attrs_batch().num_rows(), 18);
    }

    #[test]
    fn data_points_builder_produces_correct_batch() {
        let metrics = [CounterMetricDef {
            name: "test.counter",
            unit: "{item}",
            description: "A test counter",
            num_points: 3,
            point_attributes: &[
                ("outcome", "success"),
                ("outcome", "failure"),
                ("outcome", "refused"),
            ],
            attrs_per_point: 1,
        }];

        let schema = PrecomputedMetricSchema::new(&metrics).expect("should build schema");
        let builder = schema.data_points_builder();

        let start_time = 1_000_000_000i64;
        let time = 2_000_000_000i64;
        let values = [100u64, 5, 2];

        let batch = builder
            .build_int_values(start_time, time, &values)
            .expect("should build data points");

        assert_eq!(batch.num_rows(), 3);
    }
}
