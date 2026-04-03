//! Trait for metric sets that can be collected into OTAP Arrow batches.
//!
//! This trait bridges the generated counter structs and the ITS
//! collection path. Each generated metric set implements this trait
//! to provide its counter values as a flat array for encoding.

use crate::self_metrics::collector::{EncodeError, MetricsEncoder};
use otap_df_pdata::OtapArrowRecords;

/// A collection of counters that can be snapshotted and encoded.
///
/// Generated counter structs implement this trait. The ITS collection
/// path uses it to snapshot and encode metrics without knowing the
/// specific counter struct types.
pub trait CollectableMetrics: Send + 'static {
    /// Snapshot all counter values into the provided buffer.
    ///
    /// `buf` is a pre-sized slice with exactly the right number of
    /// elements for this metric set (matching the precomputed schema's
    /// total_points). Returns `false` if the metric set is disabled.
    fn snapshot_into(&self, buf: &mut [u64]) -> bool;

    /// Clear all counters after a successful collection.
    fn clear(&mut self);
}

/// Combines a set of collectable counters with their encoder.
///
/// Holds the counter struct (behind a closure that snapshots + clears)
/// and the precomputed encoder. Used by the ITS receiver to collect
/// metrics on each telemetry tick.
pub struct MetricSetCollector {
    encoder: MetricsEncoder,
    snapshot_buf: Vec<u64>,
}

impl MetricSetCollector {
    /// Create a new collector for a metric set.
    #[must_use]
    pub fn new(encoder: MetricsEncoder) -> Self {
        let total_points = encoder.total_points();
        Self {
            encoder,
            snapshot_buf: vec![0u64; total_points],
        }
    }

    /// Collect metrics from the given counter set and encode to OTAP.
    ///
    /// Returns `None` if:
    /// - The metric set is disabled
    /// - All counter values are zero
    pub fn collect(
        &mut self,
        metrics: &mut dyn CollectableMetrics,
        time_unix_nano: i64,
    ) -> Result<Option<OtapArrowRecords>, EncodeError> {
        self.snapshot_buf.fill(0);
        if !metrics.snapshot_into(&mut self.snapshot_buf) {
            return Ok(None);
        }

        let result = self
            .encoder
            .encode_int_counters(time_unix_nano, &self.snapshot_buf)?;

        if result.is_some() {
            metrics.clear();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_metrics::precomputed::{CounterMetricDef, PrecomputedMetricSchema};

    struct TestMetrics {
        values: [u64; 3],
        enabled: bool,
    }

    impl CollectableMetrics for TestMetrics {
        fn snapshot_into(&self, buf: &mut [u64]) -> bool {
            if !self.enabled {
                return false;
            }
            buf[..3].copy_from_slice(&self.values);
            true
        }

        fn clear(&mut self) {
            self.values = [0; 3];
        }
    }

    fn test_encoder() -> MetricsEncoder {
        let schema = PrecomputedMetricSchema::new(&[CounterMetricDef {
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
        .unwrap();
        MetricsEncoder::new(schema, 1_000_000_000)
    }

    #[test]
    fn collect_nonzero_clears_metrics() {
        let mut collector = MetricSetCollector::new(test_encoder());
        let mut metrics = TestMetrics {
            values: [10, 5, 2],
            enabled: true,
        };

        let result = collector.collect(&mut metrics, 2_000_000_000).unwrap();
        assert!(result.is_some());
        // Should have been cleared after successful collection
        assert_eq!(metrics.values, [0, 0, 0]);
    }

    #[test]
    fn collect_all_zeros_does_not_clear() {
        let mut collector = MetricSetCollector::new(test_encoder());
        let mut metrics = TestMetrics {
            values: [0, 0, 0],
            enabled: true,
        };

        let result = collector.collect(&mut metrics, 2_000_000_000).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn collect_disabled_returns_none() {
        let mut collector = MetricSetCollector::new(test_encoder());
        let mut metrics = TestMetrics {
            values: [10, 5, 2],
            enabled: false,
        };

        let result = collector.collect(&mut metrics, 2_000_000_000).unwrap();
        assert!(result.is_none());
        // Values should NOT have been cleared
        assert_eq!(metrics.values, [10, 5, 2]);
    }
}
