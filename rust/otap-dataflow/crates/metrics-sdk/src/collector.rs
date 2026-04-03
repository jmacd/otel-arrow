//! Metrics collection: snapshot counters and encode to OTAP Arrow.
//!
//! The [`MetricsEncoder`] holds the precomputed schema for a metric set
//! and encodes counter snapshots into `OtapArrowRecords` ready for
//! pipeline injection.

use arrow::error::ArrowError;

use crate::assembly::assemble_metrics_payload;
use crate::precomputed::PrecomputedMetricSchema;
use otap_df_pdata::OtapArrowRecords;

/// Encodes counter metric snapshots into OTAP Arrow record batches.
///
/// Holds the precomputed schema (metrics table + attributes table) and
/// produces complete `OtapArrowRecords` from counter value snapshots.
pub struct MetricsEncoder {
    schema: PrecomputedMetricSchema,
    start_time_unix_nano: i64,
}

impl MetricsEncoder {
    /// Create a new encoder with the given precomputed schema.
    ///
    /// `start_time_unix_nano` is the collection interval start time,
    /// typically set once at pipeline startup.
    #[must_use]
    pub fn new(schema: PrecomputedMetricSchema, start_time_unix_nano: i64) -> Self {
        Self {
            schema,
            start_time_unix_nano,
        }
    }

    /// Update the collection interval start time.
    pub fn set_start_time(&mut self, start_time_unix_nano: i64) {
        self.start_time_unix_nano = start_time_unix_nano;
    }

    /// Number of data points this encoder expects.
    #[must_use]
    pub fn total_points(&self) -> usize {
        self.schema.total_points()
    }

    /// Encode a snapshot of integer counter values into OTAP Arrow records.
    ///
    /// `values` must have length matching `schema.total_points()`.
    /// `time_unix_nano` is the current collection timestamp.
    ///
    /// Returns `None` if all values are zero (nothing to report).
    pub fn encode_int_counters(
        &self,
        time_unix_nano: i64,
        values: &[u64],
    ) -> Result<Option<OtapArrowRecords>, EncodeError> {
        // Skip if all values are zero
        if values.iter().all(|&v| v == 0) {
            return Ok(None);
        }

        let builder = self.schema.data_points_builder();
        let dp_batch = builder
            .build_int_values(self.start_time_unix_nano, time_unix_nano, values)
            .map_err(EncodeError::Arrow)?;

        let records = assemble_metrics_payload(
            self.schema.metrics_batch(),
            self.schema.attrs_batch(),
            dp_batch,
        )
        .map_err(EncodeError::Pdata)?;

        Ok(Some(records))
    }
}

/// Errors that can occur during metrics encoding.
#[derive(Debug)]
pub enum EncodeError {
    /// Arrow record batch building failed.
    Arrow(ArrowError),
    /// OTAP payload assembly failed.
    Pdata(otap_df_pdata::error::Error),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow(e) => write!(f, "Arrow encoding error: {e}"),
            Self::Pdata(e) => write!(f, "OTAP assembly error: {e}"),
        }
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(e) => Some(e),
            Self::Pdata(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precomputed::{CounterMetricDef, PrecomputedMetricSchema};

    fn test_schema() -> PrecomputedMetricSchema {
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
        PrecomputedMetricSchema::new(&metrics, "test").unwrap()
    }

    #[test]
    fn encode_nonzero_values() {
        let encoder = MetricsEncoder::new(test_schema(), 1_000_000_000);
        let result = encoder
            .encode_int_counters(2_000_000_000, &[100, 5, 2])
            .expect("should encode");
        assert!(result.is_some());
    }

    #[test]
    fn encode_all_zeros_returns_none() {
        let encoder = MetricsEncoder::new(test_schema(), 1_000_000_000);
        let result = encoder
            .encode_int_counters(2_000_000_000, &[0, 0, 0])
            .expect("should encode");
        assert!(result.is_none());
    }
}
