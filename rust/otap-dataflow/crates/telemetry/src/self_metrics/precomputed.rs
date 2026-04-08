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
use otap_df_pdata::encode::record::metrics::{
    HistogramDataPointsRecordBatchBuilder, MetricsRecordBatchBuilder,
};
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
    /// The metrics table (one row per metric).
    metrics_batch: RecordBatch,
    /// The data-point attributes table. Parent IDs are u32 matching
    /// NumberDataPoints IDs.
    attrs_batch: RecordBatch,
    /// Total number of NumberDataPoints across all non-Mmsc metrics.
    total_number_points: usize,
    /// Precomputed parent_id for each NumberDataPoint.
    number_parent_ids: Vec<u16>,
    /// Total number of HistogramDataPoints (one per Mmsc metric).
    total_histogram_points: usize,
    /// Precomputed parent_id for each HistogramDataPoint.
    histogram_parent_ids: Vec<u16>,
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
            total_number_points: total_points,
            number_parent_ids: parent_ids,
            total_histogram_points: 0,
            histogram_parent_ids: Vec::new(),
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

    /// Total number of NumberDataPoints across all non-Mmsc metrics.
    #[must_use]
    pub fn total_number_points(&self) -> usize {
        self.total_number_points
    }

    /// Total number of HistogramDataPoints (Mmsc metrics).
    #[must_use]
    pub fn total_histogram_points(&self) -> usize {
        self.total_histogram_points
    }

    /// Create a `PrecomputedMetricSchema` from pre-built parts.
    #[must_use]
    pub fn from_parts(
        metrics_batch: RecordBatch,
        attrs_batch: RecordBatch,
        total_number_points: usize,
        number_parent_ids: Vec<u16>,
        total_histogram_points: usize,
        histogram_parent_ids: Vec<u16>,
    ) -> Self {
        Self {
            metrics_batch,
            attrs_batch,
            total_number_points,
            number_parent_ids,
            total_histogram_points,
            histogram_parent_ids,
        }
    }

    /// Create a new NumberDataPoints builder for this schema.
    #[must_use]
    pub fn data_points_builder(&self) -> CounterDataPointsBuilder {
        CounterDataPointsBuilder::new(self)
    }

    /// Create a new HistogramDataPoints builder for Mmsc fields.
    #[must_use]
    pub fn histogram_data_points_builder(&self) -> MmscHistogramBuilder {
        MmscHistogramBuilder::new(self)
    }
}

/// Builds the NumberDataPoints record batch at runtime from counter
/// snapshots.
pub struct CounterDataPointsBuilder {
    parent_ids: Vec<u16>,
    total_points: usize,
}

impl CounterDataPointsBuilder {
    fn new(schema: &PrecomputedMetricSchema) -> Self {
        Self {
            parent_ids: schema.number_parent_ids.clone(),
            total_points: schema.total_number_points,
        }
    }

    /// Build a NumberDataPoints record batch from integer counter values.
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

/// Builds HistogramDataPoints record batch for Mmsc snapshots using
/// the pdata `HistogramDataPointsRecordBatchBuilder`.
pub struct MmscHistogramBuilder {
    parent_ids: Vec<u16>,
    total_points: usize,
}

impl MmscHistogramBuilder {
    fn new(schema: &PrecomputedMetricSchema) -> Self {
        Self {
            parent_ids: schema.histogram_parent_ids.clone(),
            total_points: schema.total_histogram_points,
        }
    }

    /// Build a HistogramDataPoints record batch from Mmsc snapshots.
    ///
    /// Each `MmscSnapshot` becomes one histogram data point with empty
    /// explicit bounds and one bucket (the total count), carrying
    /// min/max/sum/count.
    pub fn build(
        &self,
        start_time_unix_nano: i64,
        time_unix_nano: i64,
        snapshots: &[crate::instrument::MmscSnapshot],
    ) -> Result<RecordBatch, ArrowError> {
        assert_eq!(
            snapshots.len(),
            self.total_points,
            "snapshots length must match total_histogram_points"
        );

        let mut builder = HistogramDataPointsRecordBatchBuilder::new();

        for (i, snap) in snapshots.iter().enumerate() {
            builder.append_id(i as u32);
            builder.append_parent_id(self.parent_ids[i]);
            builder.append_start_time_unix_nano(start_time_unix_nano);
            builder.append_time_unix_nano(time_unix_nano);
            builder.append_count(snap.count);
            builder.append_sum(Some(snap.sum));
            builder.append_min(Some(snap.min));
            builder.append_max(Some(snap.max));
            // Empty explicit bounds → one bucket with the full count.
            builder.append_explicit_bounds(std::iter::empty());
            builder.append_bucket_counts(std::iter::once(snap.count));
            builder.append_flags(0);
        }

        builder.finish()
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

        assert_eq!(schema.total_number_points(), 3);
        assert_eq!(schema.metrics_batch().num_rows(), 1);
        assert_eq!(schema.attrs_batch().num_rows(), 3);
    }

    #[test]
    fn precomputed_schema_two_dimensions() {
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

        assert_eq!(schema.total_number_points(), 9);
        assert_eq!(schema.metrics_batch().num_rows(), 1);
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
