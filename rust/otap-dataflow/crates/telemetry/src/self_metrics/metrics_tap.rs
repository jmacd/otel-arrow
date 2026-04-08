// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MetricsTap: OTAP-native metrics dispatch with Prometheus + pipeline export.
//!
//! Replaces `MetricsDispatcher` (which bridged to OTel SDK). On each
//! collection tick it visits all registered metric sets, builds
//! delta OTAP Arrow payloads, and:
//!
//! 1. **Always**: feeds the `PrometheusExporter` cumulative accumulator
//! 2. **ITS mode**: returns assembled `OtapPdata` payloads for pipeline injection

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::otap::Metrics;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use otap_df_config::pipeline::telemetry::metrics::views::ViewConfig;

use crate::error::Error;
use crate::metrics::MetricValue;
use crate::registry::{EntityKey, TelemetryRegistryHandle};
use crate::self_metrics::accumulator::MetricIdentity;
use crate::self_metrics::bridge;
use crate::self_metrics::precomputed::PrecomputedMetricSchema;
use crate::self_metrics::prometheus::PrometheusExporter;

/// Cached precomputed schema for a descriptor (keyed by descriptor name).
struct CachedDescriptor {
    schema: PrecomputedMetricSchema,
    accumulation_modes: Vec<bridge::AccumulationMode>,
    histogram_field_indices: Vec<usize>,
    number_field_indices: Vec<usize>,
    /// Metric names for histogram fields (used for Prometheus exposition).
    histogram_names: Vec<String>,
    /// Metric descriptions for histogram fields.
    histogram_descriptions: Vec<String>,
    /// Metric units for histogram fields.
    histogram_units: Vec<String>,
}

/// An assembled OTAP metrics payload ready for pipeline injection.
#[derive(Clone)]
pub struct OtapMetricsPayload {
    /// The OTAP Arrow records containing the metrics payload.
    pub records: OtapArrowRecords,
}

/// MetricsTap collects metrics and exports them via Prometheus and/or
/// pipeline injection.
pub struct MetricsTap {
    registry: TelemetryRegistryHandle,
    reporting_interval: std::time::Duration,
    prometheus: PrometheusExporter,
    /// Precomputed resource attributes batch (built once, zerocopy reuse).
    resource_attrs: RecordBatch,
    /// Per-descriptor precomputed schemas (metrics table + accumulation modes).
    descriptor_cache: parking_lot::Mutex<HashMap<&'static str, CachedDescriptor>>,
    /// Per-entity scope attributes batches (like ScopeToBytesMap for logs).
    scope_cache: parking_lot::Mutex<HashMap<EntityKey, RecordBatch>>,
    /// Optional channel for ITS pipeline injection.
    metrics_sender: Option<flume::Sender<OtapMetricsPayload>>,
    /// Pipeline startup time for delta start_time_unix_nano.
    start_time_nanos: i64,
    /// View configurations for metric renaming.
    views: Vec<ViewConfig>,
}

impl MetricsTap {
    /// Create a new MetricsTap.
    pub fn new(
        registry: TelemetryRegistryHandle,
        reporting_interval: std::time::Duration,
        resource_attrs: &[(&str, &str)],
        views: Vec<ViewConfig>,
    ) -> Result<Self, Error> {
        let resource_attrs_batch = bridge::build_resource_attrs(resource_attrs)
            .map_err(|e| Error::MetricEncoding(e.to_string()))?;

        let start_time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        Ok(Self {
            registry,
            reporting_interval,
            prometheus: PrometheusExporter::new(),
            resource_attrs: resource_attrs_batch,
            descriptor_cache: parking_lot::Mutex::new(HashMap::new()),
            scope_cache: parking_lot::Mutex::new(HashMap::new()),
            metrics_sender: None,
            start_time_nanos,
            views,
        })
    }

    /// Create a metrics channel for ITS pipeline injection.
    pub fn create_metrics_channel(
        &mut self,
        capacity: usize,
    ) -> flume::Receiver<OtapMetricsPayload> {
        let (sender, receiver) = flume::bounded(capacity);
        self.metrics_sender = Some(sender);
        receiver
    }

    /// Get a reference to the Prometheus exporter for serving /metrics.
    pub fn prometheus_exporter(&self) -> &PrometheusExporter {
        &self.prometheus
    }

    /// Run the collection loop: periodically visit metrics and dispatch.
    pub async fn run(self: Arc<Self>, cancellation_token: CancellationToken) -> Result<(), Error> {
        let mut ticker = interval(self.reporting_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return Ok(())
                }
                _ = ticker.tick() => {
                    let _ = self.collect_and_dispatch()?;
                }
            }
        }
    }

    /// Perform one collection cycle.
    pub fn collect_and_dispatch(&self) -> Result<Vec<OtapMetricsPayload>, Error> {
        let time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let mut payloads = Vec::new();

        self.registry.visit_metrics_and_reset_with_entity(
            |entity_key, descriptor, attrs, metrics_iter| {
                let values: Vec<MetricValue> = metrics_iter.map(|(_, v)| v).collect();

                if bridge::snapshot_all_zeros(&values) {
                    return;
                }

                // Get or create cached descriptor schema.
                let desc_key = descriptor.name;
                let mut desc_cache = self.descriptor_cache.lock();
                if !desc_cache.contains_key(desc_key) {
                    match bridge::descriptor_to_schema_with_views(descriptor, &self.views) {
                        Ok(ds) => {
                            // Extract histogram metric names/descriptions from descriptor
                            // with view overrides applied.
                            let mut hist_names = Vec::new();
                            let mut hist_descriptions = Vec::new();
                            let mut hist_units = Vec::new();
                            for &idx in &ds.histogram_field_indices {
                                let field = &descriptor.metrics[idx];
                                let view = bridge::find_matching_view(
                                    &self.views,
                                    descriptor.name,
                                    field.name,
                                );
                                hist_names.push(
                                    view.and_then(|v| v.stream.name.clone())
                                        .unwrap_or_else(|| field.name.to_string()),
                                );
                                hist_descriptions.push(
                                    view.and_then(|v| v.stream.description.clone())
                                        .unwrap_or_else(|| field.brief.to_string()),
                                );
                                hist_units.push(field.unit.to_string());
                            }

                            let _ = desc_cache.insert(
                                desc_key,
                                CachedDescriptor {
                                    schema: ds.schema,
                                    accumulation_modes: ds.accumulation_modes,
                                    histogram_field_indices: ds.histogram_field_indices,
                                    number_field_indices: ds.number_field_indices,
                                    histogram_names: hist_names,
                                    histogram_descriptions: hist_descriptions,
                                    histogram_units: hist_units,
                                },
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                descriptor = descriptor.name,
                                "Failed to build precomputed schema"
                            );
                            return;
                        }
                    }
                }
                let cached_desc = match desc_cache.get(desc_key) {
                    Some(c) => c,
                    None => return,
                };

                // Get or create cached scope attrs for this entity.
                let mut scope_cache = self.scope_cache.lock();
                let scope_attrs = scope_cache
                    .entry(entity_key)
                    .or_insert_with(|| {
                        bridge::build_scope_attrs_from_entity(attrs).unwrap_or_else(|e| {
                            tracing::warn!(
                                error = %e,
                                descriptor = descriptor.name,
                                "Failed to build scope attrs"
                            );
                            RecordBatch::new_empty(arrow::datatypes::Schema::empty().into())
                        })
                    })
                    .clone();

                // Build delta NumberDataPoints for non-Mmsc fields.
                let ndp_batch = if !cached_desc.number_field_indices.is_empty() {
                    let int_values = bridge::expand_number_snapshot(
                        &values,
                        &cached_desc.number_field_indices,
                    );
                    match cached_desc
                        .schema
                        .data_points_builder()
                        .build_int_values_i64(self.start_time_nanos, time_nanos, &int_values)
                    {
                        Ok(batch) => Some(batch),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                descriptor = descriptor.name,
                                "Failed to build number data points batch"
                            );
                            return;
                        }
                    }
                } else {
                    None
                };

                // Build delta HistogramDataPoints for Mmsc fields.
                let hdp_batch = if !cached_desc.histogram_field_indices.is_empty() {
                    let histograms = bridge::expand_histogram_snapshot(
                        &values,
                        &cached_desc.histogram_field_indices,
                    );
                    match cached_desc
                        .schema
                        .histogram_data_points_builder()
                        .build(self.start_time_nanos, time_nanos, &histograms)
                    {
                        Ok(batch) => Some(batch),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                descriptor = descriptor.name,
                                "Failed to build histogram data points batch"
                            );
                            return;
                        }
                    }
                } else {
                    None
                };

                // Feed Prometheus accumulator with per-point modes.
                let identity = MetricIdentity {
                    schema_key: descriptor.name,
                    entity_key,
                };

                self.prometheus
                    .register_schema_if_needed(descriptor.name, cached_desc.schema.clone());

                if let Some(ref ndp) = ndp_batch {
                    if let Err(e) = self.prometheus.ingest_with_modes(
                        identity,
                        ndp,
                        &cached_desc.accumulation_modes,
                    ) {
                        tracing::warn!(
                            error = %e,
                            descriptor = descriptor.name,
                            "Failed to ingest into Prometheus accumulator"
                        );
                    }
                }

                // Feed Prometheus histogram accumulator with Mmsc snapshots.
                if !cached_desc.histogram_field_indices.is_empty() {
                    let histograms = bridge::expand_histogram_snapshot(
                        &values,
                        &cached_desc.histogram_field_indices,
                    );
                    self.prometheus.ingest_histograms(
                        identity,
                        &cached_desc.histogram_names,
                        &cached_desc.histogram_descriptions,
                        &cached_desc.histogram_units,
                        &histograms,
                    );
                }

                // Assemble complete OTAP payload.
                match self.assemble_payload(cached_desc, &scope_attrs, ndp_batch, hdp_batch) {
                    Ok(payload) => payloads.push(payload),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            descriptor = descriptor.name,
                            "Failed to assemble OTAP metrics payload"
                        );
                    }
                }
            },
        );

        // Send payloads through the ITS channel if configured.
        if let Some(ref sender) = self.metrics_sender {
            for payload in &payloads {
                if sender
                    .try_send(OtapMetricsPayload {
                        records: payload.records.clone(),
                    })
                    .is_err()
                {
                    tracing::debug!("ITS metrics channel full, dropping payload");
                }
            }
        }

        Ok(payloads)
    }

    fn assemble_payload(
        &self,
        cached_desc: &CachedDescriptor,
        scope_attrs: &RecordBatch,
        ndp_batch: Option<RecordBatch>,
        hdp_batch: Option<RecordBatch>,
    ) -> Result<OtapMetricsPayload, Error> {
        let mut records = OtapArrowRecords::Metrics(Metrics::default());

        if cached_desc.schema.metrics_batch().num_rows() > 0 {
            records
                .set(
                    ArrowPayloadType::UnivariateMetrics,
                    cached_desc.schema.metrics_batch().clone(),
                )
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        if let Some(ndp) = ndp_batch {
            if ndp.num_rows() > 0 {
                records
                    .set(ArrowPayloadType::NumberDataPoints, ndp)
                    .map_err(|e| Error::MetricEncoding(e.to_string()))?;
            }
        }

        if let Some(hdp) = hdp_batch {
            if hdp.num_rows() > 0 {
                records
                    .set(ArrowPayloadType::HistogramDataPoints, hdp)
                    .map_err(|e| Error::MetricEncoding(e.to_string()))?;
            }
        }

        if self.resource_attrs.num_rows() > 0 {
            records
                .set(ArrowPayloadType::ResourceAttrs, self.resource_attrs.clone())
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        if scope_attrs.num_rows() > 0 {
            records
                .set(ArrowPayloadType::ScopeAttrs, scope_attrs.clone())
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        Ok(OtapMetricsPayload { records })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor::{
        Instrument, MetricValueType, MetricsDescriptor, MetricsField, Temporality,
    };
    use crate::metrics::MetricSetHandler;
    use crate::registry::TelemetryRegistryHandle;
    use otap_df_pdata::otap::OtapBatchStore;

    #[derive(Debug, Default, Clone)]
    struct TestMetrics {
        items_in: u64,
        items_out: u64,
    }

    static TEST_DESCRIPTOR: MetricsDescriptor = MetricsDescriptor {
        name: "test.metrics",
        metrics: &[
            MetricsField {
                name: "items.in",
                unit: "{item}",
                brief: "Items received",
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::U64,
            },
            MetricsField {
                name: "items.out",
                unit: "{item}",
                brief: "Items sent",
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::U64,
            },
        ],
    };

    impl MetricSetHandler for TestMetrics {
        fn descriptor(&self) -> &'static MetricsDescriptor {
            &TEST_DESCRIPTOR
        }

        fn snapshot_values(&self) -> Vec<MetricValue> {
            vec![
                MetricValue::U64(self.items_in),
                MetricValue::U64(self.items_out),
            ]
        }

        fn clear_values(&mut self) {
            self.items_in = 0;
            self.items_out = 0;
        }

        fn needs_flush(&self) -> bool {
            self.items_in > 0 || self.items_out > 0
        }
    }

    fn make_test_setup() -> (TelemetryRegistryHandle, MetricsTap) {
        let registry = TelemetryRegistryHandle::new();
        let tap = MetricsTap::new(
            registry.clone(),
            std::time::Duration::from_secs(1),
            &[("service.name", "test")],
            Vec::new(),
        )
        .expect("should create tap");
        (registry, tap)
    }

    #[test]
    fn collect_empty_returns_no_payloads() {
        let (_registry, tap) = make_test_setup();
        let payloads = tap.collect_and_dispatch().expect("should collect");
        assert!(payloads.is_empty());
    }

    #[test]
    fn collect_with_data_returns_payload() {
        let (registry, tap) = make_test_setup();

        let mut metrics_set =
            registry.register_metric_set::<TestMetrics>(crate::testing::EmptyAttributes());
        metrics_set.items_in = 100;
        metrics_set.items_out = 95;

        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());

        let payloads = tap.collect_and_dispatch().expect("should collect");
        assert_eq!(payloads.len(), 1);

        match &payloads[0].records {
            OtapArrowRecords::Metrics(_) => {}
            _ => panic!("expected Metrics variant"),
        }
    }

    #[test]
    fn prometheus_accumulates_deltas() {
        let (registry, tap) = make_test_setup();

        let mut metrics_set =
            registry.register_metric_set::<TestMetrics>(crate::testing::EmptyAttributes());

        // First collection.
        metrics_set.items_in = 10;
        metrics_set.items_out = 8;
        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());
        let _ = tap.collect_and_dispatch().expect("should collect");

        // Second collection.
        metrics_set.items_in = 5;
        metrics_set.items_out = 4;
        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());
        let _ = tap.collect_and_dispatch().expect("should collect");

        // Prometheus should have cumulative values.
        let text = tap.prometheus_exporter().format_metrics();
        assert!(text.contains("items_in_total"));
        assert!(text.contains("items_out_total"));
        assert!(text.contains("15"));
        assert!(text.contains("12"));
    }

    // --- Integration tests ---

    /// Mixed instrument types: Counter + Gauge + Mmsc.
    #[derive(Debug, Default, Clone)]
    struct MixedMetrics {
        requests: u64,
        queue_depth: u64,
        latency: crate::instrument::Mmsc,
    }

    static MIXED_DESCRIPTOR: MetricsDescriptor = MetricsDescriptor {
        name: "test.mixed",
        metrics: &[
            MetricsField {
                name: "requests",
                unit: "{request}",
                brief: "Request count",
                instrument: Instrument::Counter,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::U64,
            },
            MetricsField {
                name: "queue.depth",
                unit: "{item}",
                brief: "Current queue depth",
                instrument: Instrument::Gauge,
                temporality: None,
                value_type: MetricValueType::U64,
            },
            MetricsField {
                name: "latency",
                unit: "ns",
                brief: "Processing latency",
                instrument: Instrument::Mmsc,
                temporality: Some(Temporality::Delta),
                value_type: MetricValueType::F64,
            },
        ],
    };

    impl MetricSetHandler for MixedMetrics {
        fn descriptor(&self) -> &'static MetricsDescriptor {
            &MIXED_DESCRIPTOR
        }

        fn snapshot_values(&self) -> Vec<MetricValue> {
            vec![
                MetricValue::U64(self.requests),
                MetricValue::U64(self.queue_depth),
                MetricValue::Mmsc(self.latency.get()),
            ]
        }

        fn clear_values(&mut self) {
            self.requests = 0;
            self.queue_depth = 0;
            self.latency.reset();
        }

        fn needs_flush(&self) -> bool {
            self.requests > 0 || self.queue_depth > 0 || self.latency.get().count > 0
        }
    }

    #[test]
    fn integration_mixed_instruments_with_views() {
        use otap_df_config::pipeline::telemetry::metrics::views::{
            MetricSelector, MetricStream, ViewConfig,
        };

        let views = vec![
            ViewConfig {
                selector: MetricSelector {
                    scope_name: Some("test.mixed".to_string()),
                    instrument_name: Some("requests".to_string()),
                },
                stream: MetricStream {
                    name: Some("http.requests".to_string()),
                    description: Some("HTTP request count".to_string()),
                },
            },
            ViewConfig {
                selector: MetricSelector {
                    scope_name: None,
                    instrument_name: Some("latency".to_string()),
                },
                stream: MetricStream {
                    name: Some("process.duration".to_string()),
                    description: Some("Processing duration".to_string()),
                },
            },
        ];

        let registry = TelemetryRegistryHandle::new();
        let tap = MetricsTap::new(
            registry.clone(),
            std::time::Duration::from_secs(1),
            &[("service.name", "integration-test")],
            views,
        )
        .expect("should create tap");

        let mut metrics_set =
            registry.register_metric_set::<MixedMetrics>(crate::testing::EmptyAttributes());

        // First collection cycle.
        metrics_set.requests = 100;
        metrics_set.queue_depth = 5;
        metrics_set.latency.record(1000.0);
        metrics_set.latency.record(5000.0);
        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());
        let payloads = tap.collect_and_dispatch().expect("should collect");
        assert_eq!(payloads.len(), 1);

        // Second collection cycle (delta accumulation).
        metrics_set.requests = 50;
        metrics_set.queue_depth = 3;
        metrics_set.latency.record(2000.0);
        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());
        let _ = tap.collect_and_dispatch().expect("should collect");

        let text = tap.prometheus_exporter().format_metrics();

        // Counter: renamed via view, cumulative (100+50=150).
        assert!(
            text.contains("http_requests_total 150"),
            "expected renamed cumulative counter, got:\n{text}"
        );
        assert!(text.contains("HTTP request count"));

        // Gauge: not renamed, replaced (latest value = 3).
        assert!(
            text.contains("queue_depth 3"),
            "expected gauge with latest value, got:\n{text}"
        );

        // Mmsc: histogram data points are in the OTAP payload AND
        // in the Prometheus output as histogram type.
        assert!(
            text.contains("# TYPE process_duration histogram"),
            "expected histogram type in Prometheus output, got:\n{text}"
        );
        assert!(
            text.contains("process_duration_count"),
            "expected histogram count, got:\n{text}"
        );
        assert!(
            text.contains("process_duration_sum"),
            "expected histogram sum, got:\n{text}"
        );
        assert!(text.contains("Processing duration"));

        for payload in &payloads {
            match &payload.records {
                OtapArrowRecords::Metrics(m) => {
                    assert!(
                        m.get(ArrowPayloadType::HistogramDataPoints).is_some(),
                        "expected HistogramDataPoints in OTAP payload"
                    );
                    assert!(
                        m.get(ArrowPayloadType::NumberDataPoints).is_some(),
                        "expected NumberDataPoints in OTAP payload"
                    );
                }
                _ => panic!("expected Metrics variant"),
            }
        }
    }

    #[test]
    fn integration_its_channel_receives_payloads() {
        let registry = TelemetryRegistryHandle::new();
        let mut tap = MetricsTap::new(
            registry.clone(),
            std::time::Duration::from_secs(1),
            &[("service.name", "its-test")],
            Vec::new(),
        )
        .expect("should create tap");

        let metrics_rx = tap.create_metrics_channel(10);

        let mut metrics_set =
            registry.register_metric_set::<TestMetrics>(crate::testing::EmptyAttributes());
        metrics_set.items_in = 42;
        metrics_set.items_out = 40;
        let snapshot = metrics_set.snapshot();
        registry.accumulate_metric_set_snapshot(snapshot.key(), snapshot.get_metrics());

        let _ = tap.collect_and_dispatch().expect("should collect");

        // ITS channel should have received the payload.
        let payload = metrics_rx.try_recv().expect("should receive payload");
        match &payload.records {
            OtapArrowRecords::Metrics(_) => {}
            _ => panic!("expected Metrics variant on ITS channel"),
        }

        // Prometheus should also have the data.
        let text = tap.prometheus_exporter().format_metrics();
        assert!(text.contains("items_in_total 42"));
        assert!(text.contains("items_out_total 40"));
    }
}
