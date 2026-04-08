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
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

use crate::instrument::MmscSnapshot;
use crate::self_metrics::accumulator::{CumulativeAccumulator, MetricIdentity};
use crate::self_metrics::openmetrics::format_openmetrics;
use crate::self_metrics::precomputed::PrecomputedMetricSchema;

/// Cumulative state for a single Mmsc histogram metric.
struct HistogramState {
    /// Metric name from the precomputed metrics table.
    name: String,
    /// Metric description.
    description: String,
    /// Metric unit.
    unit: String,
    /// Cumulative snapshot (sum and count accumulate, min/max replace).
    cumulative: MmscSnapshot,
}

/// Shared state for the protometheus exporter.
///
/// Thread-safe: the state is behind a `RwLock` so that delta
/// ingestion (write) and Prometheus scrapes (read) can proceed with
/// minimal contention.
#[derive(Clone)]
pub struct PrometheusExporter {
    state: Arc<RwLock<ExporterState>>,
}

struct ExporterState {
    /// Accumulator for NumberDataPoints (counters, gauges).
    number_acc: CumulativeAccumulator,
    /// Cumulative histogram state, keyed by (identity, metric_index).
    histograms: BTreeMap<(MetricIdentity, usize), HistogramState>,
}

impl PrometheusExporter {
    /// Create a new empty exporter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ExporterState {
                number_acc: CumulativeAccumulator::new(),
                histograms: BTreeMap::new(),
            })),
        }
    }

    /// Register a schema for a given schema key.
    pub fn register_schema(&self, schema_key: &'static str, schema: PrecomputedMetricSchema) {
        self.state
            .write()
            .number_acc
            .register_schema(schema_key, schema);
    }

    /// Register a schema only if the given schema_key hasn't been
    /// registered yet.
    pub fn register_schema_if_needed(
        &self,
        schema_key: &'static str,
        schema: PrecomputedMetricSchema,
    ) {
        let mut state = self.state.write();
        if !state.number_acc.has_schema(schema_key) {
            state.number_acc.register_schema(schema_key, schema);
        }
    }

    /// Ingest a delta NumberDataPoints batch for a specific identity.
    pub fn ingest_delta(
        &self,
        identity: MetricIdentity,
        delta_dp: &RecordBatch,
    ) -> Result<(), arrow::error::ArrowError> {
        self.state.write().number_acc.ingest_delta(identity, delta_dp)
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
            .number_acc
            .ingest_with_modes(identity, delta_dp, modes)
    }

    /// Ingest Mmsc histogram snapshots for a specific identity.
    ///
    /// `names`, `descriptions`, and `units` are parallel slices with
    /// the metric name/description/unit for each histogram field.
    /// `snapshots` contains the delta Mmsc values.
    /// Sum and count are accumulated; min and max are replaced.
    pub fn ingest_histograms(
        &self,
        identity: MetricIdentity,
        names: &[String],
        descriptions: &[String],
        units: &[String],
        snapshots: &[MmscSnapshot],
    ) {
        let mut state = self.state.write();
        for (i, snap) in snapshots.iter().enumerate() {
            let key = (identity, i);
            let entry = state.histograms.entry(key).or_insert_with(|| {
                HistogramState {
                    name: names[i].clone(),
                    description: descriptions[i].clone(),
                    unit: units[i].clone(),
                    cumulative: MmscSnapshot {
                        min: f64::MAX,
                        max: f64::MIN,
                        sum: 0.0,
                        count: 0,
                    },
                }
            });
            // Accumulate: sum and count add, min/max track extremes.
            entry.cumulative.sum += snap.sum;
            entry.cumulative.count += snap.count;
            if snap.count > 0 {
                if snap.min < entry.cumulative.min {
                    entry.cumulative.min = snap.min;
                }
                if snap.max > entry.cumulative.max {
                    entry.cumulative.max = snap.max;
                }
            }
        }
    }

    /// Format the current cumulative state as OpenMetrics text.
    #[must_use]
    pub fn format_metrics(&self) -> String {
        let state = self.state.read();
        let ndp_entries = state.number_acc.snapshot();

        let has_ndp = !ndp_entries.is_empty();
        let has_hdp = !state.histograms.is_empty();

        if !has_ndp && !has_hdp {
            return "# EOF\n".to_string();
        }

        let mut out = if has_ndp {
            format_openmetrics_without_eof(&ndp_entries)
        } else {
            String::with_capacity(1024)
        };

        // Append histogram metrics.
        if has_hdp {
            format_histograms(&mut out, &state.histograms);
        }

        let _ = writeln!(out, "# EOF");
        out
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

/// Format NDP entries without the trailing `# EOF` line.
fn format_openmetrics_without_eof(
    entries: &[crate::self_metrics::accumulator::CumulativeEntry],
) -> String {
    // Re-use the existing formatter but strip the EOF.
    let text = format_openmetrics(entries);
    text.strip_suffix("# EOF\n")
        .unwrap_or(&text)
        .to_string()
}

/// Append histogram metrics in Prometheus exposition format.
fn format_histograms(
    out: &mut String,
    histograms: &BTreeMap<(MetricIdentity, usize), HistogramState>,
) {
    // Group by metric name to emit TYPE/HELP once per metric family.
    let mut seen_names = std::collections::HashSet::new();

    for state in histograms.values() {
        let om_name = state.name.replace('.', "_");

        if seen_names.insert(om_name.clone()) {
            if !state.description.is_empty() {
                let _ = writeln!(out, "# HELP {om_name} {}", state.description);
            }
            let _ = writeln!(out, "# TYPE {om_name} histogram");
            if !state.unit.is_empty() {
                let _ = writeln!(out, "# UNIT {om_name} {}", state.unit);
            }
        }

        let s = &state.cumulative;
        let _ = writeln!(out, "{om_name}_bucket{{le=\"+Inf\"}} {}", s.count);
        let _ = writeln!(out, "{om_name}_count {}", s.count);
        let _ = writeln!(out, "{om_name}_sum {}", s.sum);
        if s.count > 0 {
            let _ = writeln!(out, "{om_name}_min {}", s.min);
            let _ = writeln!(out, "{om_name}_max {}", s.max);
        }
        let _ = writeln!(out);
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

    #[test]
    fn histogram_accumulates_and_formats() {
        let exporter = PrometheusExporter::new();
        let id = test_id();

        // First delta.
        exporter.ingest_histograms(
            id,
            &["latency".to_string()],
            &["Processing latency".to_string()],
            &["ns".to_string()],
            &[MmscSnapshot {
                min: 100.0,
                max: 500.0,
                sum: 1200.0,
                count: 5,
            }],
        );

        // Second delta.
        exporter.ingest_histograms(
            id,
            &["latency".to_string()],
            &["Processing latency".to_string()],
            &["ns".to_string()],
            &[MmscSnapshot {
                min: 50.0,
                max: 800.0,
                sum: 3000.0,
                count: 10,
            }],
        );

        let text = exporter.format_metrics();
        assert!(text.contains("# TYPE latency histogram"), "got:\n{text}");
        assert!(text.contains("# HELP latency Processing latency"), "got:\n{text}");
        // Cumulative: count=15, sum=4200, min=50, max=800
        assert!(text.contains("latency_count 15"), "got:\n{text}");
        assert!(text.contains("latency_sum 4200"), "got:\n{text}");
        assert!(text.contains("latency_min 50"), "got:\n{text}");
        assert!(text.contains("latency_max 800"), "got:\n{text}");
        assert!(text.contains("latency_bucket{le=\"+Inf\"} 15"), "got:\n{text}");
        assert!(text.contains("# EOF"), "got:\n{text}");
    }
}
