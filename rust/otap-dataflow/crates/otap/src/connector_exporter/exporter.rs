// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Connector exporter implementation.
//!
//! This exporter sends OTLP bytes through a flume channel to an external consumer.
//! It handles conversion from OTAP Arrow format to OTLP bytes when necessary.

use crate::OTAP_EXPORTER_FACTORIES;
use crate::connector_exporter::channel::{
    ConnectorRequest, ConnectorResponse, ConnectorSender, RequestId, RequestIdGenerator,
};
use crate::connector_exporter::config::Config;
use crate::metrics::ExporterPDataMetrics;
use crate::pdata::{Context, OtapPdata};
use async_trait::async_trait;
use bytes::Bytes;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::{Error, ExporterErrorKind, format_error_sources};
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::otlp::logs::LogsProtoBytesEncoder;
use otap_df_pdata::otlp::metrics::MetricsProtoBytesEncoder;
use otap_df_pdata::otlp::traces::TracesProtoBytesEncoder;
use otap_df_pdata::otlp::{ProtoBuffer, ProtoBytesEncoder};
use otap_df_pdata::{OtapPayload, OtlpProtoBytes};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::otel_info;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The URN for the connector exporter.
pub const CONNECTOR_EXPORTER_URN: &str = "urn:otel:connector:exporter";

/// Connector exporter that sends OTLP bytes to an external consumer via channels.
pub struct ConnectorExporter {
    /// The channel sender for communicating with the consumer.
    sender: ConnectorSender,
    /// Exporter configuration.
    config: Config,
    /// Metrics for tracking pdata processing.
    pdata_metrics: MetricSet<ExporterPDataMetrics>,
}

/// Declares the connector exporter as a local exporter factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static CONNECTOR_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: CONNECTOR_EXPORTER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig| {
        // Note: The actual ConnectorSender must be provided externally since
        // the channel endpoints need to be shared with the consumer.
        // This factory creates a placeholder that will fail if used directly.
        // Use ConnectorExporter::new() directly when you have the channel.
        Err(otap_df_config::error::Error::InvalidUserConfig {
            error: "ConnectorExporter requires external channel setup. \
                    Use ConnectorExporter::new() with a pre-created channel."
                .to_string(),
        })
    },
};

impl ConnectorExporter {
    /// Create a new connector exporter with the given channel sender.
    ///
    /// # Arguments
    ///
    /// * `pipeline_ctx` - Pipeline context for metrics registration.
    /// * `sender` - The sender half of the connector channel.
    /// * `config` - Exporter configuration.
    pub fn new(pipeline_ctx: PipelineContext, sender: ConnectorSender, config: Config) -> Self {
        let pdata_metrics = pipeline_ctx.register_metrics::<ExporterPDataMetrics>();
        Self {
            sender,
            config,
            pdata_metrics,
        }
    }

    /// Create an ExporterWrapper for use in a pipeline.
    pub fn into_wrapper(
        self,
        node: NodeId,
        node_config: Arc<NodeUserConfig>,
        exporter_config: &ExporterConfig,
    ) -> ExporterWrapper<OtapPdata> {
        ExporterWrapper::local(self, node, node_config, exporter_config)
    }
}

/// Saved context for in-flight requests, used for ACK/NACK routing.
struct InFlightRequest {
    /// Original context for routing ACK/NACK.
    context: Context,
    /// Saved payload for potential retry.
    saved_payload: OtapPayload,
    /// Signal type for metrics.
    signal_type: SignalType,
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ConnectorExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        otel_info!(
            "Exporter.Start",
            max_in_flight = self.config.max_in_flight,
            message = "Starting Connector Exporter"
        );

        let exporter_id = effect_handler.exporter_id();
        let timer_cancel_handle = effect_handler
            .start_periodic_telemetry(Duration::from_secs(1))
            .await?;

        // Request ID generator for unique request correlation.
        let id_generator = RequestIdGenerator::new();

        // Track in-flight requests for ACK/NACK routing.
        let mut in_flight: HashMap<RequestId, InFlightRequest> = HashMap::new();

        // Reusable encoders and buffers for OTAP -> OTLP conversion.
        let mut logs_encoder = LogsProtoBytesEncoder::new();
        let mut metrics_encoder = MetricsProtoBytesEncoder::new();
        let mut traces_encoder = TracesProtoBytesEncoder::new();
        let mut proto_buffer = ProtoBuffer::with_capacity(8 * 1024);

        // Pending message when we're at capacity and need to wait for responses.
        let mut pending_msg: Option<Message<OtapPdata>> = None;

        loop {
            // If at capacity with a pending message, only wait for responses.
            if in_flight.len() >= self.config.max_in_flight && pending_msg.is_some() {
                if let Some(response) = self.sender.recv_response_async().await {
                    self.handle_response(response, &mut in_flight, &effect_handler)
                        .await?;
                }
                continue;
            }

            // Use select! to wait on either a pipeline message or a consumer response.
            // This ensures we can process responses promptly while also accepting new work.
            let msg = if let Some(msg) = pending_msg.take() {
                msg
            } else if in_flight.is_empty() {
                // No in-flight requests, just wait for the next message.
                msg_chan.recv().await?
            } else {
                // Have in-flight requests, select between message and response.
                tokio::select! {
                    biased;

                    // Prioritize responses to free up capacity.
                    response = self.sender.recv_response_async() => {
                        if let Some(response) = response {
                            self.handle_response(response, &mut in_flight, &effect_handler)
                                .await?;
                        }
                        continue;
                    }

                    msg = msg_chan.recv() => {
                        msg?
                    }
                }
            };

            match msg {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    // Drain remaining responses before shutdown.
                    while !in_flight.is_empty() {
                        if let Some(response) = self.sender.recv_response_async().await {
                            self.handle_response(response, &mut in_flight, &effect_handler)
                                .await?;
                        } else {
                            // Consumer disconnected.
                            break;
                        }
                    }
                    _ = timer_cancel_handle.cancel().await;
                    return Ok(TerminalState::new(deadline, [self.pdata_metrics]));
                }

                Message::Control(NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                }) => {
                    _ = metrics_reporter.report(&mut self.pdata_metrics);
                }

                Message::Control(NodeControlMsg::TimerTick { .. })
                | Message::Control(NodeControlMsg::Config { .. }) => {
                    // Ignored.
                }

                Message::PData(pdata) => {
                    // If at capacity, park the message and wait for responses.
                    if in_flight.len() >= self.config.max_in_flight {
                        pending_msg = Some(Message::PData(pdata));
                        continue;
                    }

                    let signal_type = pdata.signal_type();
                    let (context, payload) = pdata.into_parts();
                    self.pdata_metrics.inc_consumed(signal_type);

                    // Convert to OTLP bytes if necessary.
                    let (otlp_bytes, saved_payload) = match convert_to_otlp_bytes(
                        payload,
                        signal_type,
                        &exporter_id,
                        &mut proto_buffer,
                        &mut logs_encoder,
                        &mut metrics_encoder,
                        &mut traces_encoder,
                        context.may_return_payload(),
                    ) {
                        Ok(result) => result,
                        Err((error, saved_payload)) => {
                            self.pdata_metrics.inc_failed(signal_type);
                            // Send NACK for encoding failure.
                            let nack_pdata = OtapPdata::new(context, saved_payload);
                            effect_handler
                                .notify_nack(NackMsg::new(&error.to_string(), nack_pdata))
                                .await?;
                            continue;
                        }
                    };

                    // Generate request ID and send to consumer.
                    let request_id = id_generator.next();
                    let request = ConnectorRequest::new(request_id, signal_type, otlp_bytes);

                    if let Err(failed_request) = self.sender.send_async(request).await {
                        // Consumer disconnected.
                        self.pdata_metrics.inc_failed(signal_type);
                        let nack_pdata = OtapPdata::new(context, saved_payload);
                        effect_handler
                            .notify_nack(NackMsg::new("consumer disconnected", nack_pdata))
                            .await?;

                        return Err(Error::ExporterError {
                            exporter: exporter_id,
                            kind: ExporterErrorKind::Transport,
                            error: "Consumer disconnected".to_string(),
                            source_detail: String::new(),
                        });
                    }

                    // Track the in-flight request.
                    in_flight.insert(
                        request_id,
                        InFlightRequest {
                            context,
                            saved_payload,
                            signal_type,
                        },
                    );
                }

                _ => {
                    // Ignore other message types.
                }
            }
        }
    }
}

impl ConnectorExporter {
    async fn handle_response(
        &mut self,
        response: ConnectorResponse,
        in_flight: &mut HashMap<RequestId, InFlightRequest>,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Some(request_info) = in_flight.remove(&response.id) else {
            // Unknown request ID - could log a warning here.
            return Ok(());
        };

        let pdata = OtapPdata::new(request_info.context, request_info.saved_payload);

        if response.is_success() {
            self.pdata_metrics.inc_exported(request_info.signal_type);
            effect_handler.notify_ack(AckMsg::new(pdata)).await?;
        } else {
            self.pdata_metrics.inc_failed(request_info.signal_type);
            let error_msg = response
                .error_message
                .as_deref()
                .unwrap_or("consumer reported failure");
            effect_handler
                .notify_nack(NackMsg::new(error_msg, pdata))
                .await?;
        }

        Ok(())
    }
}

/// Convert an OTAP payload to OTLP bytes.
///
/// Returns the OTLP bytes and a saved payload for potential ACK/NACK routing.
#[allow(clippy::too_many_arguments)]
fn convert_to_otlp_bytes(
    payload: OtapPayload,
    signal_type: SignalType,
    exporter_id: &NodeId,
    proto_buffer: &mut ProtoBuffer,
    logs_encoder: &mut LogsProtoBytesEncoder,
    metrics_encoder: &mut MetricsProtoBytesEncoder,
    traces_encoder: &mut TracesProtoBytesEncoder,
    may_return_payload: bool,
) -> Result<(Bytes, OtapPayload), (Error, OtapPayload)> {
    match payload {
        // Already OTLP bytes - pass through.
        OtapPayload::OtlpBytes(otlp_bytes) => {
            let bytes = match &otlp_bytes {
                OtlpProtoBytes::ExportLogsRequest(b) => b.clone(),
                OtlpProtoBytes::ExportMetricsRequest(b) => b.clone(),
                OtlpProtoBytes::ExportTracesRequest(b) => b.clone(),
            };

            // Preserve payload for ACK/NACK if requested.
            let saved_payload = if may_return_payload {
                OtapPayload::OtlpBytes(otlp_bytes)
            } else {
                OtapPayload::empty(signal_type)
            };

            Ok((bytes, saved_payload))
        }

        // Convert OTAP Arrow to OTLP bytes.
        OtapPayload::OtapArrowRecords(mut otap_batch) => {
            proto_buffer.clear();

            let encode_result = match signal_type {
                SignalType::Logs => logs_encoder.encode(&mut otap_batch, proto_buffer),
                SignalType::Metrics => metrics_encoder.encode(&mut otap_batch, proto_buffer),
                SignalType::Traces => traces_encoder.encode(&mut otap_batch, proto_buffer),
            };

            if let Err(e) = encode_result {
                // Preserve the original payload for the NACK.
                let saved_payload = if may_return_payload {
                    OtapPayload::OtapArrowRecords(otap_batch)
                } else {
                    let _drop = otap_batch.take_payload();
                    OtapPayload::OtapArrowRecords(otap_batch)
                };

                return Err((
                    Error::ExporterError {
                        exporter: exporter_id.clone(),
                        kind: ExporterErrorKind::Other,
                        error: format!("OTAP to OTLP encoding failed: {e}"),
                        source_detail: format_error_sources(&e),
                    },
                    saved_payload,
                ));
            }

            let (bytes, next_capacity) = proto_buffer.take_into_bytes();
            proto_buffer.ensure_capacity(next_capacity);

            // Prepare saved payload for ACK/NACK.
            let saved_payload = if may_return_payload {
                OtapPayload::OtapArrowRecords(otap_batch)
            } else {
                let _drop = otap_batch.take_payload();
                OtapPayload::OtapArrowRecords(otap_batch)
            };

            Ok((bytes, saved_payload))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector_exporter::channel::create_connector_channel;
    use crate::testing::create_test_pdata;
    use otap_df_config::node::NodeUserConfig;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::control::{Controllable, pipeline_ctrl_msg_channel};
    use otap_df_engine::exporter::ExporterWrapper;
    use otap_df_engine::local::message::{LocalReceiver, LocalSender};
    use otap_df_engine::message::{Receiver, Sender};
    use otap_df_engine::node::NodeWithPDataReceiver;
    use otap_df_engine::testing::{create_not_send_channel, test_node};
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use otap_df_telemetry::reporter::MetricsReporter;
    use std::time::Instant;

    #[test]
    fn test_connector_exporter_basic_flow() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let local = tokio::task::LocalSet::new();
            local
                .run_until(async {
                    // Create channel pair.
                    let (sender, consumer) = create_connector_channel(10, 10);

                    // Set up pipeline context.
                    let telemetry_registry = TelemetryRegistryHandle::new();
                    let controller_ctx = ControllerContext::new(telemetry_registry);
                    let pipeline_ctx = controller_ctx.pipeline_context_with(
                        "test".into(),
                        "pipeline".into(),
                        0,
                        0,
                    );

                    let config = Config::default();
                    let exporter = ConnectorExporter::new(pipeline_ctx, sender, config);

                    let node_config =
                        Arc::new(NodeUserConfig::new_exporter_config(CONNECTOR_EXPORTER_URN));
                    let exporter_config = ExporterConfig::new("test_connector");
                    let node_id = test_node(exporter_config.name.clone());

                    let mut wrapper = exporter.into_wrapper(node_id.clone(), node_config, &exporter_config);

                    // Set up channels.
                    let (pdata_tx, pdata_rx) = create_not_send_channel::<OtapPdata>(10);
                    let pdata_tx = Sender::Local(LocalSender::mpsc(pdata_tx));
                    let pdata_rx = Receiver::Local(LocalReceiver::mpsc(pdata_rx));
                    let (pipeline_ctrl_tx, _pipeline_ctrl_rx) = pipeline_ctrl_msg_channel(10);

                    let control_sender = wrapper.control_sender();
                    wrapper
                        .set_pdata_receiver(node_id, pdata_rx)
                        .expect("set receiver");

                    let (_metrics_rx, metrics_reporter) =
                        MetricsReporter::create_new_and_receiver(1);

                    // Spawn consumer thread.
                    let consumer_handle = std::thread::spawn(move || {
                        // Receive and respond to one request.
                        if let Some(request) = consumer.recv_timeout(Duration::from_secs(5)) {
                            assert_eq!(request.signal_type, SignalType::Logs);
                            consumer.respond_success(request.id).unwrap();
                        }
                    });

                    // Spawn exporter.
                    let exporter_handle = tokio::task::spawn_local(async move {
                        wrapper.start(pipeline_ctrl_tx, metrics_reporter).await
                    });

                    // Send test data.
                    let pdata = create_test_pdata();
                    pdata_tx.send(pdata).await.expect("send pdata");

                    // Give time for processing.
                    tokio::time::sleep(Duration::from_millis(100)).await;

                    // Shutdown.
                    control_sender
                        .send(NodeControlMsg::Shutdown {
                            deadline: Instant::now() + Duration::from_secs(1),
                            reason: "test complete".into(),
                        })
                        .await
                        .expect("send shutdown");

                    let result = exporter_handle.await.expect("exporter join");
                    assert!(result.is_ok());

                    consumer_handle.join().expect("consumer join");
                })
                .await;
        });
    }
}
