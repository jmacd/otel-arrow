//! Protometheus exporter: cumulative accumulator + HTTP `/metrics` endpoint.
//!
//! This module provides a self-contained Prometheus-compatible exporter
//! that receives delta OTAP metric batches, accumulates them into
//! cumulative state, and serves OpenMetrics text on HTTP scrapes.
//!
//! It replaces the OTel SDK → `opentelemetry-prometheus` export chain
//! with a single Arrow-native component.

use std::sync::Arc;

use arrow::array::RecordBatch;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use parking_lot::RwLock;

use crate::accumulator::{CumulativeAccumulator, CumulativeSnapshot};
use crate::openmetrics::format_openmetrics;
use crate::precomputed::PrecomputedMetricSchema;

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
    /// Create a new exporter with the given precomputed schema.
    #[must_use]
    pub fn new(schema: PrecomputedMetricSchema) -> Self {
        Self {
            state: Arc::new(RwLock::new(CumulativeAccumulator::new(schema))),
        }
    }

    /// Ingest a delta NumberDataPoints batch.
    ///
    /// This is called from the ITS collection path on each telemetry tick.
    pub fn ingest_delta(&self, delta_dp: &RecordBatch) -> Result<(), arrow::error::ArrowError> {
        self.state.write().ingest_delta(delta_dp)
    }

    /// Get a snapshot of the current cumulative state.
    ///
    /// Returns `None` if no deltas have been ingested yet.
    #[must_use]
    pub fn snapshot(&self) -> Option<CumulativeSnapshot> {
        self.state.read().snapshot()
    }

    /// Format the current cumulative state as OpenMetrics text.
    #[must_use]
    pub fn format_metrics(&self) -> String {
        match self.snapshot() {
            Some(snap) => format_openmetrics(&snap),
            None => "# EOF\n".to_string(),
        }
    }

    /// Build an axum Router that serves `/metrics` (or a custom path).
    pub fn router(self) -> axum::Router {
        axum::Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(self)
    }
}

/// Axum handler for GET /metrics.
///
/// Clones the current cumulative batch (cheap — ref-counted buffers),
/// formats as OpenMetrics text, and returns the response.
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
    use crate::precomputed::CounterMetricDef;

    fn test_exporter() -> PrometheusExporter {
        let schema = PrecomputedMetricSchema::new(
            &[CounterMetricDef {
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
            }],
            "test",
        )
        .unwrap();
        PrometheusExporter::new(schema)
    }

    #[test]
    fn empty_exporter_returns_eof() {
        let exporter = test_exporter();
        let text = exporter.format_metrics();
        assert_eq!(text.trim(), "# EOF");
    }

    #[test]
    fn ingest_and_format() {
        let exporter = test_exporter();
        let schema = PrecomputedMetricSchema::new(
            &[CounterMetricDef {
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
            }],
            "test",
        )
        .unwrap();

        let delta = schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[100, 5, 2])
            .unwrap();
        exporter.ingest_delta(&delta).unwrap();

        let text = exporter.format_metrics();
        assert!(text.contains("test_counter_total{outcome=\"success\"} 100"));
        assert!(text.contains("test_counter_total{outcome=\"failure\"} 5"));
        assert!(text.contains("test_counter_total{outcome=\"refused\"} 2"));
    }

    #[test]
    fn accumulates_across_deltas() {
        let exporter = test_exporter();
        let schema = PrecomputedMetricSchema::new(
            &[CounterMetricDef {
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
            }],
            "test",
        )
        .unwrap();

        exporter
            .ingest_delta(
                &schema
                    .data_points_builder()
                    .build_int_values(1_000_000_000, 2_000_000_000, &[10, 5, 2])
                    .unwrap(),
            )
            .unwrap();
        exporter
            .ingest_delta(
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

    #[test]
    fn clone_shares_state() {
        let exporter = test_exporter();
        let clone = exporter.clone();
        let schema = PrecomputedMetricSchema::new(
            &[CounterMetricDef {
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
            }],
            "test",
        )
        .unwrap();

        let delta = schema
            .data_points_builder()
            .build_int_values(1_000_000_000, 2_000_000_000, &[7, 3, 1])
            .unwrap();
        exporter.ingest_delta(&delta).unwrap();

        // Clone should see the same data
        let text = clone.format_metrics();
        assert!(text.contains("test_counter_total{outcome=\"success\"} 7"));
    }
}
