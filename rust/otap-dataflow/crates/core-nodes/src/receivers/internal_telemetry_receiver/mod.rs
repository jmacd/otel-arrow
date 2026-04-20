// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Internal telemetry receiver.
//!
//! This receiver consumes internal logs from the logging channel and emits
//! the logs as OTLP ExportLogsRequest messages into the pipeline. When
//! metrics ITS is enabled, it also collects internal metrics and emits
//! OTAP Arrow metric payloads.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ReceiverFactory;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::receiver as local;
use otap_df_engine::node::NodeId;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_otap::OTAP_RECEIVER_FACTORIES;
use otap_df_otap::pdata::{Context, OtapPdata};
use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::OtlpProtoBytes;
use otap_df_pdata::otap::Metrics;
use otap_df_pdata::otlp::ProtoBuffer;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otap_df_telemetry::descriptor::MetricsDescriptor;
use otap_df_telemetry::event::{LogEvent, ObservedEvent};
use otap_df_telemetry::metrics::MetricSetSnapshot;
use otap_df_telemetry::registry::EntityKey;
use otap_df_telemetry::self_metrics::bridge;
use otap_df_telemetry::self_metrics::precomputed::PrecomputedMetricSchema;
use otap_df_telemetry::self_tracing::{ScopeToBytesMap, encode_export_logs_request};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The URN for the internal telemetry receiver.
pub use otap_df_telemetry::INTERNAL_TELEMETRY_RECEIVER_URN;

/// Configuration for the internal telemetry receiver.
#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {}

/// A receiver that consumes internal logs from the logging channel and emits OTLP logs.
pub struct InternalTelemetryReceiver {
    #[allow(dead_code)]
    config: Config,
    /// Internal telemetry settings obtained from the pipeline context during construction.
    /// Contains the logs receiver channel, pre-encoded resource bytes, and registry handle.
    internal_telemetry: otap_df_telemetry::InternalTelemetrySettings,
}

/// Declares the internal telemetry receiver as a local receiver factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static INTERNAL_TELEMETRY_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: INTERNAL_TELEMETRY_RECEIVER_URN,
    create: |mut pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig| {
        // Get internal telemetry settings from the pipeline context
        let internal_telemetry = pipeline.take_internal_telemetry().ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: "InternalTelemetryReceiver requires internal telemetry settings in pipeline context".to_owned(),
            }
        })?;

        Ok(ReceiverWrapper::local(
            InternalTelemetryReceiver::new_with_telemetry(
                InternalTelemetryReceiver::parse_config(&node_config.config)?,
                internal_telemetry,
            ),
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

impl InternalTelemetryReceiver {
    /// Create a new receiver with the given configuration and internal telemetry settings.
    #[must_use]
    pub const fn new_with_telemetry(
        config: Config,
        internal_telemetry: otap_df_telemetry::InternalTelemetrySettings,
    ) -> Self {
        Self {
            config,
            internal_telemetry,
        }
    }

    /// Parse configuration from a JSON value.
    pub fn parse_config(config: &Value) -> Result<Config, otap_df_config::error::Error> {
        serde_json::from_value(config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })
    }
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for InternalTelemetryReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl_msg_recv: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let internal = self.internal_telemetry.clone();
        let logs_receiver = internal.logs_receiver;
        let resource_bytes = internal.resource_bytes;
        let log_tap = internal.log_tap;
        let mut scope_cache = ScopeToBytesMap::new(internal.registry);

        // Metrics OTAP encoding state.
        let mut descriptor_cache: HashMap<DescriptorKey, CachedDescriptorSchema> = HashMap::new();
        let mut scope_attrs_cache: HashMap<EntityKey, RecordBatch> = HashMap::new();
        let resource_attrs = Self::build_resource_attrs(&self.internal_telemetry);
        let mut prev_collect_nanos = current_time_nanos();

        // Start periodic telemetry collection
        let _ = effect_handler
            .start_periodic_telemetry(std::time::Duration::from_secs(1))
            .await?;

        loop {
            tokio::select! {
                biased;

                // Handle control messages with priority
                ctrl_msg = ctrl_msg_recv.recv() => {
                    match ctrl_msg {
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            while let Ok(event) = logs_receiver.try_recv() {
                                if let ObservedEvent::Log(log_event) = event {
                                    if let Some(log_tap) = log_tap.as_ref() {
                                        log_tap.record(log_event.clone());
                                    }
                                    Self::send_log_event(&effect_handler, log_event, &resource_bytes, &mut scope_cache).await?;
                                }
                            }
                            effect_handler.notify_receiver_drained().await?;
                            return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            // Drain any remaining logs from channel before shutdown
                            while let Ok(event) = logs_receiver.try_recv() {
                                if let ObservedEvent::Log(log_event) = event {
                                    if let Some(log_tap) = log_tap.as_ref() {
                                        log_tap.record(log_event.clone());
                                    }
                                    Self::send_log_event(&effect_handler, log_event, &resource_bytes, &mut scope_cache).await?;
                                }
                            }
                            return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(NodeControlMsg::CollectTelemetry { .. }) => {
                            Self::collect_metrics(
                                &self.internal_telemetry,
                                &effect_handler,
                                &mut descriptor_cache,
                                &mut scope_attrs_cache,
                                &resource_attrs,
                                prev_collect_nanos,
                            ).await?;
                            prev_collect_nanos = current_time_nanos();
                        }
                        Err(e) => {
                            return Err(Error::ChannelRecvError(e));
                        }
                        _ => {
                             // Ignore other control messages
                        }
                    }
                }

                // Receive logs from the channel
                result = logs_receiver.recv_async() => {
                    match result {
                        Ok(ObservedEvent::Log(log_event)) => {
                            if let Some(log_tap) = log_tap.as_ref() {
                                log_tap.record(log_event.clone());
                            }
                            Self::send_log_event(&effect_handler, log_event, &resource_bytes, &mut scope_cache).await?;
                        }
                        Ok(ObservedEvent::Engine(_)) => {
                            // Engine events are not yet processed
                        }
                        Err(_) => {
                            // Channel closed, exit gracefully
                            return Ok(TerminalState::default());
                        }
                    }
                }
            }
        }
    }
}

impl InternalTelemetryReceiver {
    /// Send a log event as OTLP logs with scope attributes from entity context.
    async fn send_log_event(
        effect_handler: &local::EffectHandler<OtapPdata>,
        log_event: LogEvent,
        resource_bytes: &Bytes,
        scope_cache: &mut ScopeToBytesMap,
    ) -> Result<(), Error> {
        let mut buf = ProtoBuffer::with_capacity(512);

        encode_export_logs_request(&mut buf, &log_event, resource_bytes, scope_cache);

        let pdata = OtapPdata::new(
            Context::default(),
            OtlpProtoBytes::ExportLogsRequest(buf.into_bytes()).into(),
        );
        effect_handler.send_message(pdata).await?;
        Ok(())
    }

    /// Build OTAP ResourceAttrs once at startup from config.
    fn build_resource_attrs(
        settings: &otap_df_telemetry::InternalTelemetrySettings,
    ) -> RecordBatch {
        let attrs: Vec<(&str, &str)> = settings
            .resource_config
            .iter()
            .filter_map(|(k, v)| {
                if let otap_df_config::pipeline::telemetry::AttributeValue::String(s) = v {
                    Some((k.as_str(), s.as_str()))
                } else {
                    None
                }
            })
            .collect();
        bridge::build_resource_attrs(&attrs).unwrap_or_else(|_| {
            RecordBatch::new_empty(arrow::datatypes::Schema::empty().into())
        })
    }

    /// Collect metrics from registry, build OTAP payloads, feed MetricsTap.
    async fn collect_metrics(
        settings: &otap_df_telemetry::InternalTelemetrySettings,
        effect_handler: &local::EffectHandler<OtapPdata>,
        descriptor_cache: &mut HashMap<DescriptorKey, CachedDescriptorSchema>,
        scope_attrs_cache: &mut HashMap<EntityKey, RecordBatch>,
        resource_attrs: &RecordBatch,
        start_time_nanos: i64,
    ) -> Result<(), Error> {
        let time_nanos = current_time_nanos();
        let mut payloads: Vec<OtapPdata> = Vec::new();

        settings.registry.visit_metrics_and_reset_with_entity(
            |descriptor, entity_key, attrs, iter| {
                // Snapshot values before the iterator is consumed.
                let values: Vec<_> = iter.map(|(_, v)| v).collect();

                // Feed MetricsTap for Prometheus.
                if let Some(tap) = &settings.metrics_tap {
                    tap.lock().feed_values(descriptor, attrs, &values);
                }

                // Get or create cached descriptor schema.
                let desc_key = DescriptorKey(descriptor as *const MetricsDescriptor);
                if !descriptor_cache.contains_key(&desc_key) {
                    match bridge::descriptor_to_schema_with_views(descriptor, &settings.views) {
                        Ok(ds) => {
                            let _ = descriptor_cache.insert(desc_key, CachedDescriptorSchema {
                                schema: ds.schema,
                                number_field_indices: ds.number_field_indices,
                                histogram_field_indices: ds.histogram_field_indices,
                            });
                        }
                        Err(_) => return,
                    }
                }
                let cached = match descriptor_cache.get(&desc_key) {
                    Some(c) => c,
                    None => return,
                };

                // Get or create cached scope attrs.
                let scope_attrs = scope_attrs_cache
                    .entry(entity_key)
                    .or_insert_with(|| {
                        bridge::build_scope_attrs_from_entity(attrs)
                            .unwrap_or_else(|_| RecordBatch::new_empty(
                                arrow::datatypes::Schema::empty().into(),
                            ))
                    })
                    .clone();

                // Build NumberDataPoints for scalar fields.
                let ndp = if !cached.number_field_indices.is_empty() {
                    let typed = bridge::expand_number_snapshot(
                        &values,
                        &cached.number_field_indices,
                    );
                    cached.schema.data_points_builder()
                        .build(start_time_nanos, time_nanos, &typed)
                        .ok()
                } else {
                    None
                };

                // Build HistogramDataPoints for Mmsc fields.
                let hdp = if !cached.histogram_field_indices.is_empty() {
                    let histograms = bridge::expand_histogram_snapshot(
                        &values,
                        &cached.histogram_field_indices,
                    );
                    cached.schema.histogram_data_points_builder()
                        .build(start_time_nanos, time_nanos, &histograms)
                        .ok()
                } else {
                    None
                };

                // Assemble OTAP payload.
                if let Ok(payload) = assemble_metrics_payload(
                    cached,
                    &scope_attrs,
                    resource_attrs,
                    ndp,
                    hdp,
                ) {
                    payloads.push(OtapPdata::new(Context::default(), payload.into()));
                }
            },
        );

        // Send all payloads through the pipeline.
        for pdata in payloads {
            effect_handler.send_message(pdata).await?;
        }

        Ok(())
    }
}

/// Stable cache key using descriptor pointer identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DescriptorKey(*const MetricsDescriptor);

// SAFETY: MetricsDescriptor is &'static and immutable.
#[allow(unsafe_code)]
unsafe impl Send for DescriptorKey {}
#[allow(unsafe_code)]
unsafe impl Sync for DescriptorKey {}

/// Cached precomputed schema for a descriptor.
struct CachedDescriptorSchema {
    schema: PrecomputedMetricSchema,
    number_field_indices: Vec<usize>,
    histogram_field_indices: Vec<usize>,
}

/// Get current time as nanoseconds since UNIX epoch.
fn current_time_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Assemble a complete OTAP metrics payload from precomputed and runtime batches.
fn assemble_metrics_payload(
    cached: &CachedDescriptorSchema,
    scope_attrs: &RecordBatch,
    resource_attrs: &RecordBatch,
    ndp: Option<RecordBatch>,
    hdp: Option<RecordBatch>,
) -> Result<OtapArrowRecords, Error> {
    let mut records = OtapArrowRecords::Metrics(Metrics::default());

    if cached.schema.metrics_batch().num_rows() > 0 {
        records
            .set(
                ArrowPayloadType::UnivariateMetrics,
                cached.schema.metrics_batch().clone(),
            )
            .map_err(|e| Error::InternalError { message: e.to_string() })?;
    }

    if let Some(ndp) = ndp {
        if ndp.num_rows() > 0 {
            records
                .set(ArrowPayloadType::NumberDataPoints, ndp)
                .map_err(|e| Error::InternalError { message: e.to_string() })?;
        }
    }

    if let Some(hdp) = hdp {
        if hdp.num_rows() > 0 {
            records
                .set(ArrowPayloadType::HistogramDataPoints, hdp)
                .map_err(|e| Error::InternalError { message: e.to_string() })?;
        }
    }

    if resource_attrs.num_rows() > 0 {
        records
            .set(ArrowPayloadType::ResourceAttrs, resource_attrs.clone())
            .map_err(|e| Error::InternalError { message: e.to_string() })?;
    }

    if scope_attrs.num_rows() > 0 {
        records
            .set(ArrowPayloadType::ScopeAttrs, scope_attrs.clone())
            .map_err(|e| Error::InternalError { message: e.to_string() })?;
    }

    Ok(records)
}
