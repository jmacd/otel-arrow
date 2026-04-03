//! Self-metrics for the telemetry system.
//!
//! Generated from `self_metrics.yaml` by `cargo xtask generate-metrics`.

pub mod generated;

#[cfg(test)]
mod tests {
    use super::generated::*;
    use otap_df_config::SignalType;
    use otap_df_config::policy::MetricLevel;
    use otap_df_metrics_sdk::assembly::assemble_metrics_payload;
    use otap_df_metrics_sdk::dimension::{Dimension, Outcome};

    #[test]
    fn basic_level_counter_lifecycle() {
        let mut consumer = NodeConsumerItems::new(MetricLevel::Basic);

        // At basic level, signal_type is ignored — only outcome matters
        consumer.add(10, Outcome::Success, SignalType::Logs);
        consumer.add(5, Outcome::Success, SignalType::Traces);
        consumer.add(3, Outcome::Failure, SignalType::Logs);
        consumer.add(1, Outcome::Refused, SignalType::Metrics);

        let snapshot = consumer.snapshot().expect("should produce snapshot");
        // Basic: 3 data points [success, failure, refused]
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot[Outcome::Success.index()], 15); // 10 + 5
        assert_eq!(snapshot[Outcome::Failure.index()], 3);
        assert_eq!(snapshot[Outcome::Refused.index()], 1);

        consumer.clear();
        let snapshot = consumer.snapshot().expect("should produce snapshot");
        assert!(snapshot.iter().all(|&v| v == 0));
    }

    #[test]
    fn normal_level_counter_lifecycle() {
        let mut consumer = NodeConsumerItems::new(MetricLevel::Normal);

        consumer.add(10, Outcome::Success, SignalType::Logs);
        consumer.add(5, Outcome::Success, SignalType::Traces);
        consumer.add(3, Outcome::Failure, SignalType::Metrics);
        consumer.add(1, Outcome::Refused, SignalType::Logs);

        let snapshot = consumer.snapshot().expect("should produce snapshot");
        // Normal: 9 data points [outcome × signal_type]
        // Index = outcome.index() * 3 + signal_type.index()
        assert_eq!(snapshot.len(), 9);

        // Success × Traces = 0*3 + 0 = 0
        assert_eq!(snapshot[0], 5);
        // Success × Logs = 0*3 + 2 = 2
        assert_eq!(snapshot[2], 10);
        // Failure × Metrics = 1*3 + 1 = 4
        assert_eq!(snapshot[4], 3);
        // Refused × Logs = 2*3 + 2 = 8
        assert_eq!(snapshot[8], 1);
    }

    #[test]
    fn detailed_uses_normal_variant() {
        let mut consumer = NodeConsumerItems::new(MetricLevel::Detailed);
        consumer.add(7, Outcome::Success, SignalType::Metrics);

        let snapshot = consumer.snapshot().expect("should produce snapshot");
        // Detailed has same shape as Normal (9 points)
        assert_eq!(snapshot.len(), 9);
        // Success × Metrics = 0*3 + 1 = 1
        assert_eq!(snapshot[1], 7);
    }

    #[test]
    fn none_level_produces_no_snapshot() {
        let consumer = NodeConsumerItems::new(MetricLevel::None);
        assert!(consumer.snapshot().is_none());
    }

    #[test]
    fn precomputed_schema_basic() {
        let schema = precomputed_schema(MetricLevel::Basic).expect("should build schema");
        // Basic: 2 metrics × 3 points each = 6 total points
        assert_eq!(schema.total_points(), 6);
        assert_eq!(schema.metrics_batch().num_rows(), 2);
        // 6 points × 1 attr each = 6 attribute rows
        assert_eq!(schema.attrs_batch().num_rows(), 6);
    }

    #[test]
    fn precomputed_schema_normal() {
        let schema = precomputed_schema(MetricLevel::Normal).expect("should build schema");
        // Normal: 2 metrics × 9 points each = 18 total points
        assert_eq!(schema.total_points(), 18);
        assert_eq!(schema.metrics_batch().num_rows(), 2);
        // 18 points × 2 attrs each = 36 attribute rows
        assert_eq!(schema.attrs_batch().num_rows(), 36);
    }

    #[test]
    fn precomputed_schema_none_returns_none() {
        assert!(precomputed_schema(MetricLevel::None).is_none());
    }

    #[test]
    fn end_to_end_encode_to_otap() {
        let mut consumer = NodeConsumerItems::new(MetricLevel::Normal);
        let mut producer = NodeProducerItems::new(MetricLevel::Normal);

        // Simulate some traffic
        consumer.add(100, Outcome::Success, SignalType::Logs);
        consumer.add(50, Outcome::Success, SignalType::Traces);
        consumer.add(5, Outcome::Failure, SignalType::Logs);
        producer.add(95, Outcome::Success, SignalType::Logs);
        producer.add(50, Outcome::Success, SignalType::Traces);
        producer.add(2, Outcome::Refused, SignalType::Metrics);

        let schema = precomputed_schema(MetricLevel::Normal).expect("should build schema");
        let builder = schema.data_points_builder();

        // Build the combined snapshot: consumer points then producer points
        let consumer_snap = consumer.snapshot().unwrap();
        let producer_snap = producer.snapshot().unwrap();
        let mut all_values = consumer_snap;
        all_values.extend(producer_snap);

        let dp_batch = builder
            .build_int_values(1_000_000_000, 2_000_000_000, &all_values)
            .expect("should build data points");

        assert_eq!(dp_batch.num_rows(), 18); // 9 + 9

        // Assemble into OTAP payload
        let records =
            assemble_metrics_payload(schema.metrics_batch(), schema.attrs_batch(), dp_batch)
                .expect("should assemble");

        match &records {
            otap_df_pdata::OtapArrowRecords::Metrics(_) => {}
            _ => panic!("expected Metrics variant"),
        }
    }
}
