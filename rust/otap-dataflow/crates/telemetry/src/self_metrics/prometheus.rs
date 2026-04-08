//! Protometheus exporter: cumulative accumulator + HTTP `/metrics` endpoint.
//!
//! Replaces the OTel SDK → `opentelemetry-prometheus` export chain
//! with a single Arrow-native component.

use arrow::array::RecordBatch;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::self_metrics::accumulator::{CumulativeAccumulator, MetricIdentity};
use crate::self_metrics::openmetrics::format_openmetrics;
use crate::self_metrics::precomputed::PrecomputedMetricSchema;

/// Shared state for the protometheus exporter.
///
/// Thread-safe: the accumulator is behind a `RwLock` so that delta
/// ingestion (write) and Prometheus scrapes (read) can proceed with
/// minimal contention.
#[derive(Clone)]
pub struct PrometheusExporter {
    state: Arc<RwLock<CumulativeAccumulator>>,
}

impl PrometheusExporter {
    /// Create a new empty exporter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CumulativeAccumulator::new())),
        }
    }

    /// Register a schema for a given schema key.
    pub fn register_schema(&self, schema_key: &'static str, schema: PrecomputedMetricSchema) {
        self.state.write().register_schema(schema_key, schema);
    }

    /// Register a schema only if the given schema_key hasn't been
    /// registered yet.
    pub fn register_schema_if_needed(
        &self,
        schema_key: &'static str,
        schema: PrecomputedMetricSchema,
    ) {
        let mut state = self.state.write();
        if !state.has_schema(schema_key) {
            state.register_schema(schema_key, schema);
        }
    }

    /// Ingest a delta NumberDataPoints batch for a specific identity.
    pub fn ingest_delta(
        &self,
        identity: MetricIdentity,
        delta_dp: &RecordBatch,
    ) -> Result<(), arrow::error::ArrowError> {
        self.state.write().ingest_delta(identity, delta_dp)
    }

    /// Ingest a batch with per-data-point accumulation modes.
    pub fn ingest_with_modes(
        &self,
        identity: MetricIdentity,
        delta_dp: &RecordBatch,
        modes: &[crate::self_metrics::bridge::AccumulationMode],
    ) -> Result<(), arrow::error::ArrowError> {
        self.state
            .write()
            .ingest_with_modes(identity, delta_dp, modes)
    }

    /// Format the current cumulative state as OpenMetrics text.
    #[must_use]
    pub fn format_metrics(&self) -> String {
        let entries = self.state.read().snapshot();
        if entries.is_empty() {
            "# EOF\n".to_string()
        } else {
            format_openmetrics(&entries)
        }
    }

    /// Build an axum Router that serves `/metrics`.
    pub fn router(self) -> axum::Router {
        axum::Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(self)
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

async fn metrics_handler(State(exporter): State<PrometheusExporter>) -> impl IntoResponse {
    let body = exporter.format_metrics();
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::EntityKey;
    use crate::self_metrics::precomputed::CounterMetricDef;

    const TEST_SCHEMA: &str = "test.consumer";

    fn test_schema() -> PrecomputedMetricSchema {
        PrecomputedMetricSchema::new(&[CounterMetricDef {
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
    fn empty_exporter_returns_eof() {
        let exporter = PrometheusExporter::new();
        let text = exporter.format_metrics();
        assert_eq!(text.trim(), "# EOF");
    }

    #[test]
    fn ingest_and_format() {
        let schema = test_schema();
        let exporter = PrometheusExporter::new();
        exporter.register_schema(TEST_SCHEMA, schema.clone());

        let delta = schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[100, 5, 2])
            .unwrap();
        exporter.ingest_delta(test_id(), &delta).unwrap();

        let text = exporter.format_metrics();
        assert!(text.contains("test_counter_total{outcome=\"success\"} 100"));
        assert!(text.contains("test_counter_total{outcome=\"failure\"} 5"));
        assert!(text.contains("test_counter_total{outcome=\"refused\"} 2"));
    }

    #[test]
    fn accumulates_across_deltas() {
        let schema = test_schema();
        let exporter = PrometheusExporter::new();
        exporter.register_schema(TEST_SCHEMA, schema.clone());

        exporter
            .ingest_delta(
                test_id(),
                &schema
                    .data_points_builder()
                    .build_int_values(1_000_000_000, 2_000_000_000, &[10, 5, 2])
                    .unwrap(),
            )
            .unwrap();
        exporter
            .ingest_delta(
                test_id(),
                &schema
                    .data_points_builder()
                    .build_int_values(1_000_000_000, 3_000_000_000, &[3, 1, 0])
                    .unwrap(),
            )
            .unwrap();

        let text = exporter.format_metrics();
        assert!(text.contains("test_counter_total{outcome=\"success\"} 13"));
        assert!(text.contains("test_counter_total{outcome=\"failure\"} 6"));
        assert!(text.contains("test_counter_total{outcome=\"refused\"} 2"));
    }
}
