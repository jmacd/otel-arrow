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
//!
//! Each metric set produces a self-contained OTAP payload with
//! `resource.id=0`, `scope.id=0`, plus precomputed ResourceAttrs and
//! ScopeAttrs child batches. The pipeline batch processor handles ID
//! reindexing when merging payloads from different metric sets.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::otap::Metrics;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use crate::attributes::AttributeSetHandler;
use crate::descriptor::MetricsDescriptor;
use crate::error::Error;
use crate::metrics::MetricValue;
use crate::registry::TelemetryRegistryHandle;
use crate::self_metrics::accumulator::MetricIdentity;
use crate::self_metrics::bridge;
use crate::self_metrics::precomputed::PrecomputedMetricSchema;
use crate::self_metrics::prometheus::PrometheusExporter;

/// Cached precomputed schema for a metric set.
struct CachedSchema {
    schema: PrecomputedMetricSchema,
    scope_attrs: RecordBatch,
    accumulation_modes: Vec<bridge::AccumulationMode>,
}

/// An assembled OTAP metrics payload ready for pipeline injection.
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
    /// Precomputed resource attributes batch (shared across all payloads).
    resource_attrs: RecordBatch,
    /// Cached per-descriptor precomputed schemas, keyed by descriptor name.
    schema_cache: parking_lot::Mutex<HashMap<&'static str, CachedSchema>>,
    /// Pipeline startup time for delta start_time_unix_nano.
    start_time_nanos: i64,
}

impl MetricsTap {
    /// Create a new MetricsTap.
    ///
    /// `resource_attrs` are key-value pairs describing the pipeline
    /// resource (e.g., `service.name`). These are encoded once and
    /// reused across all OTAP payloads.
    pub fn new(
        registry: TelemetryRegistryHandle,
        reporting_interval: std::time::Duration,
        resource_attrs: &[(&str, &str)],
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
            schema_cache: parking_lot::Mutex::new(HashMap::new()),
            start_time_nanos,
        })
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

    /// Perform one collection cycle: visit all metric sets, build
    /// deltas, feed the Prometheus accumulator, and return OTAP payloads
    /// for pipeline injection (ITS mode).
    pub fn collect_and_dispatch(&self) -> Result<Vec<OtapMetricsPayload>, Error> {
        let time_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        let mut payloads = Vec::new();

        self.registry.visit_metrics_and_reset_with_entity(
            |entity_key, descriptor, attrs, metrics_iter| {
                // Collect all values from the iterator.
                let values: Vec<MetricValue> = metrics_iter.map(|(_, v)| v).collect();

                if bridge::snapshot_all_zeros(&values) {
                    return;
                }

                // Get or create cached schema for this descriptor.
                let desc_key = descriptor.name;

                let mut cache = self.schema_cache.lock();
                if !cache.contains_key(desc_key) {
                    match self.build_cached_schema(descriptor, attrs) {
                        Ok(cached) => {
                            let _ = cache.insert(desc_key, cached);
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                descriptor = descriptor.name,
                                "Failed to build precomputed schema for metric set"
                            );
                            return;
                        }
                    }
                }

                let cached = match cache.get(desc_key) {
                    Some(c) => c,
                    None => return,
                };

                // Build delta NumberDataPoints with Mmsc expansion.
                let int_values = bridge::expand_snapshot(descriptor, &values);
                let dp_batch = match cached.schema.data_points_builder().build_int_values_i64(
                    self.start_time_nanos,
                    time_nanos,
                    &int_values,
                ) {
                    Ok(batch) => batch,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            descriptor = descriptor.name,
                            "Failed to build data points batch"
                        );
                        return;
                    }
                };

                // Feed Prometheus accumulator with per-point modes.
                let identity = MetricIdentity {
                    schema_key: descriptor.name,
                    entity_key,
                };

                // Register schema if first time for this identity's schema_key.
                self.prometheus
                    .register_schema_if_needed(descriptor.name, cached.schema.clone());

                if let Err(e) = self.prometheus.ingest_with_modes(
                    identity,
                    &dp_batch,
                    &cached.accumulation_modes,
                ) {
                    tracing::warn!(
                        error = %e,
                        descriptor = descriptor.name,
                        "Failed to ingest into Prometheus accumulator"
                    );
                }

                // Assemble complete OTAP payload for pipeline injection.
                match self.assemble_payload(cached, dp_batch) {
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

        Ok(payloads)
    }

    fn build_cached_schema(
        &self,
        descriptor: &'static MetricsDescriptor,
        attrs: &dyn AttributeSetHandler,
    ) -> Result<CachedSchema, Error> {
        let desc_schema = bridge::descriptor_to_schema(descriptor)
            .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        let scope_attrs = bridge::build_scope_attrs_from_entity(attrs)
            .map_err(|e| Error::MetricEncoding(e.to_string()))?;

        Ok(CachedSchema {
            schema: desc_schema.schema,
            scope_attrs,
            accumulation_modes: desc_schema.accumulation_modes,
        })
    }

    fn assemble_payload(
        &self,
        cached: &CachedSchema,
        dp_batch: RecordBatch,
    ) -> Result<OtapMetricsPayload, Error> {
        let mut records = OtapArrowRecords::Metrics(Metrics::default());

        if cached.schema.metrics_batch().num_rows() > 0 {
            records
                .set(
                    ArrowPayloadType::UnivariateMetrics,
                    cached.schema.metrics_batch().clone(),
                )
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        if dp_batch.num_rows() > 0 {
            records
                .set(ArrowPayloadType::NumberDataPoints, dp_batch)
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        if self.resource_attrs.num_rows() > 0 {
            records
                .set(ArrowPayloadType::ResourceAttrs, self.resource_attrs.clone())
                .map_err(|e| Error::MetricEncoding(e.to_string()))?;
        }

        if cached.scope_attrs.num_rows() > 0 {
            records
                .set(ArrowPayloadType::ScopeAttrs, cached.scope_attrs.clone())
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

    // A minimal test metric set.
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

        // Register a metric set and add some values.
        let mut metrics_set =
            registry.register_metric_set::<TestMetrics>(crate::testing::EmptyAttributes());
        metrics_set.items_in = 100;
        metrics_set.items_out = 95;

        // Report the snapshot.
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
        // Values should be accumulated (10+5=15, 8+4=12).
        assert!(text.contains("15"));
        assert!(text.contains("12"));
    }
}
