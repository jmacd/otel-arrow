// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Internal telemetry receiver.
//!
//! This receiver consumes internal logs from the logging channel and emits
//! the logs as OTLP ExportLogsRequest messages into the pipeline.

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
use otap_df_pdata::OtlpProtoBytes;
use otap_df_pdata::otlp::ProtoBuffer;
use otap_df_telemetry::event::{LogEvent, ObservedEvent};
use otap_df_telemetry::metrics::MetricSetSnapshot;
use otap_df_telemetry::self_tracing::{ScopeToBytesMap, encode_export_logs_request};
use otap_df_telemetry::{IntegratedSampleAggregator, SampleAggregatorConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// The URN for the internal telemetry receiver.
pub use otap_df_telemetry::INTERNAL_TELEMETRY_RECEIVER_URN;

/// Configuration for the internal telemetry receiver.
#[derive(Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Enable the integrated all-CPU sample aggregator. When set, this receiver
    /// owns the process-global aggregator: it ingests every worker's per-window
    /// sample flush and, on each telemetry-collection tick, emits one aggregated
    /// global sample as OTLP instead of the workers emitting local-only samples.
    #[serde(default)]
    integrated_sampling: bool,
}

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
             receiver_config: &ReceiverConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
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

        // When integrated sampling is enabled this receiver owns the
        // process-global aggregator: it ingests per-worker sample flushes and
        // emits the aggregated global sample on each `CollectTelemetry` tick.
        let mut sampler = self
            .config
            .integrated_sampling
            .then(|| IntegratedSampleAggregator::new(SampleAggregatorConfig::default()));

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
                            Self::drain_and_finalize_sampler(
                                sampler.as_mut(),
                                &effect_handler,
                                &resource_bytes,
                                &mut scope_cache,
                                log_tap.as_ref(),
                            )
                            .await?;
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
                            Self::drain_and_finalize_sampler(
                                sampler.as_mut(),
                                &effect_handler,
                                &resource_bytes,
                                &mut scope_cache,
                                log_tap.as_ref(),
                            )
                            .await?;
                            return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(NodeControlMsg::CollectTelemetry { .. }) => {
                            // The telemetry-collection tick is the aggregator's
                            // window boundary: finalize and emit the global
                            // sample, republishing the feedback tables.
                            if let Some(sampler) = sampler.as_mut() {
                                Self::emit_log_events(
                                    &effect_handler,
                                    sampler.finalize(),
                                    &resource_bytes,
                                    &mut scope_cache,
                                    log_tap.as_ref(),
                                )
                                .await?;
                            }
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

                // Ingest per-worker sample flushes when integrated sampling is
                // enabled. Records accumulate in the aggregator until the next
                // `CollectTelemetry` tick closes the window.
                pending = async {
                    // Armed only when the sampler is present, so the handle is set.
                    sampler.as_ref().expect("sampler present when armed").recv().await
                }, if sampler.is_some() => {
                    match pending {
                        Some(pending) => {
                            if let Some(sampler) = sampler.as_mut() {
                                sampler.ingest(pending);
                            }
                        }
                        None => {
                            // The sample channel closed; drop the handle so this
                            // branch disarms and workers revert to local-only.
                            sampler = None;
                        }
                    }
                }
            }
        }
    }
}

impl InternalTelemetryReceiver {
    /// Emit a batch of aggregated log events as OTLP, recording each in the log
    /// tap when one is present.
    async fn emit_log_events(
        effect_handler: &local::EffectHandler<OtapPdata>,
        events: Vec<LogEvent>,
        resource_bytes: &Bytes,
        scope_cache: &mut ScopeToBytesMap,
        log_tap: Option<&otap_df_telemetry::log_tap::InternalLogTapHandle>,
    ) -> Result<(), Error> {
        for event in events {
            if let Some(log_tap) = log_tap {
                log_tap.record(event.clone());
            }
            Self::send_log_event(effect_handler, event, resource_bytes, scope_cache).await?;
        }
        Ok(())
    }

    /// Drain any buffered sample flushes into the aggregator and emit its final
    /// window, for the drain and shutdown paths.
    async fn drain_and_finalize_sampler(
        sampler: Option<&mut IntegratedSampleAggregator>,
        effect_handler: &local::EffectHandler<OtapPdata>,
        resource_bytes: &Bytes,
        scope_cache: &mut ScopeToBytesMap,
        log_tap: Option<&otap_df_telemetry::log_tap::InternalLogTapHandle>,
    ) -> Result<(), Error> {
        if let Some(sampler) = sampler {
            while let Some(pending) = sampler.try_recv() {
                sampler.ingest(pending);
            }
            Self::emit_log_events(
                effect_handler,
                sampler.finalize(),
                resource_bytes,
                scope_cache,
                log_tap,
            )
            .await?;
        }
        Ok(())
    }

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
}
