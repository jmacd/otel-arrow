// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT receiver backed by the `mqtt_ingress` capability.

#[cfg(feature = "sparkplug-death-codec")]
pub mod sparkplug_death_codec;

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = MQTT_RECEIVER_URN,
    target = "otel.receiver.mqtt",
);

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use linkme::distributed_slice;
use otel_arrow_dfe_channel::error::SendError;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension;
use otel_arrow_dfe_engine::ReceiverFactory;
use otel_arrow_dfe_engine::capability::mqtt::MqttMessage;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_ingress::MqttIngress as MqttIngressCap;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind, TypedError};
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_ingress::MqttIngress as LocalMqttIngress;
use otel_arrow_dfe_engine::local::receiver as local;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::OtlpProtoBytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use prost::Message;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

/// URN identifying this receiver in pipeline configuration.
pub const MQTT_RECEIVER_URN: &str = "urn:otel:receiver:mqtt";

const MQTT_TOPIC_ATTRIBUTE: &str = "mqtt.topic";

/// Configuration for `receiver:mqtt`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// MQTT topic filter consumed through the bound capability.
    pub topic_filter: String,
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.topic_filter.trim().is_empty() {
            return Err(ConfigError::InvalidUserConfig {
                error: "mqtt receiver topic_filter must not be empty".to_owned(),
            });
        }
        Ok(())
    }
}

/// MQTT receiver that wraps inbound publishes as OTLP log records.
pub struct MqttReceiver {
    config: Config,
    mqtt_ingress: Box<dyn LocalMqttIngress>,
}

/// Declares the MQTT receiver as a local receiver factory.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static MQTT_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: MQTT_RECEIVER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config = MqttReceiver::parse_config(&node_config.config)?;
        let mqtt_ingress = capabilities
            .require_local::<MqttIngressCap>()
            .map_err(|error| ConfigError::InvalidUserConfig {
                error: error.to_string(),
            })?;

        Ok(ReceiverWrapper::local(
            MqttReceiver {
                config,
                mqtt_ingress,
            },
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |config| MqttReceiver::parse_config(config).map(|_| ()),
};

impl MqttReceiver {
    fn parse_config(config: &Value) -> Result<Config, ConfigError> {
        let config: Config = serde_json::from_value(config.clone()).map_err(|error| {
            ConfigError::InvalidUserConfig {
                error: error.to_string(),
            }
        })?;
        config.validate()?;
        Ok(config)
    }
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for MqttReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl_msg_recv: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let MqttReceiver {
            config,
            mqtt_ingress,
        } = *self;

        let mut stream = mqtt_ingress
            .subscribe(&config.topic_filter)
            .await
            .map_err(|error| Error::ReceiverError {
                receiver: effect_handler.receiver_id(),
                kind: ReceiverErrorKind::Connect,
                error: format!("failed to subscribe MQTT ingress: {error}"),
                source_detail: String::new(),
            })?;

        let mut pending_pdata: Option<OtapPdata> = None;

        loop {
            tokio::select! {
                biased;

                ctrl_msg = ctrl_msg_recv.recv() => {
                    match ctrl_msg {
                        Ok(NodeControlMsg::CollectTelemetry { .. }) => {}
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            effect_handler.notify_receiver_drained().await?;
                            return Ok(TerminalState::new::<[otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            return Ok(TerminalState::new::<[otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot; 0]>(deadline, []));
                        }
                        Ok(_) => {}
                        Err(error) => return Err(Error::ChannelRecvError(error)),
                    }
                }

                _ = std::future::ready(()), if pending_pdata.is_some() => {
                    let pdata = pending_pdata.take().expect("pending pdata branch requires pdata");
                    match effect_handler.try_send_message_with_source_node(pdata) {
                        Ok(()) => {}
                        Err(TypedError::ChannelSendError(SendError::Full(pdata))) => {
                            pending_pdata = Some(pdata);
                            tokio::task::yield_now().await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }

                mqtt_message = stream.next(), if pending_pdata.is_none() => {
                    let Some(mqtt_message) = mqtt_message else {
                        return Err(Error::ReceiverError {
                            receiver: effect_handler.receiver_id(),
                            kind: ReceiverErrorKind::Transport,
                            error: "mqtt ingress stream ended unexpectedly".to_owned(),
                            source_detail: String::new(),
                        });
                    };

                    pending_pdata = Some(mqtt_message_to_pdata(mqtt_message));
                }
            }
        }
    }
}

fn mqtt_message_to_pdata(message: MqttMessage) -> OtapPdata {
    let request = mqtt_message_to_export_logs_request(&message);
    let payload = OtlpProtoBytes::ExportLogsRequest(Bytes::from(request.encode_to_vec()));
    OtapPdata::new(Context::default(), payload.into())
}

fn mqtt_message_to_export_logs_request(message: &MqttMessage) -> ExportLogsServiceRequest {
    let body = String::from_utf8_lossy(&message.payload).into_owned();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    body: Some(string_value(body)),
                    attributes: vec![KeyValue {
                        key: MQTT_TOPIC_ATTRIBUTE.to_owned(),
                        value: Some(string_value(message.topic.clone())),
                    }],
                    ..LogRecord::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn string_value(value: String) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value as AnyValueValue;

    /// Scenario: An MQTT publish arrives with a UTF-8 payload and topic.
    /// Guarantees: The receiver maps it to one OTLP log record whose body is the payload text and whose attributes preserve `mqtt.topic`.
    #[test]
    fn mqtt_message_maps_to_log_record_body_and_topic_attribute() {
        let request = mqtt_message_to_export_logs_request(&MqttMessage::new(
            "sensors/temperature",
            Bytes::from_static(b"21.5 C"),
        ));

        let log_record = &request.resource_logs[0].scope_logs[0].log_records[0];
        let body = log_record.body.as_ref().expect("body must be present");
        assert_eq!(
            body.value,
            Some(AnyValueValue::StringValue("21.5 C".to_owned()))
        );
        assert_eq!(log_record.attributes.len(), 1);
        assert_eq!(log_record.attributes[0].key, MQTT_TOPIC_ATTRIBUTE);
        assert_eq!(
            log_record.attributes[0]
                .value
                .as_ref()
                .and_then(|value| value.value.clone()),
            Some(AnyValueValue::StringValue("sensors/temperature".to_owned()))
        );
    }

    /// Scenario: An MQTT publish arrives with invalid UTF-8 bytes in the payload.
    /// Guarantees: The receiver uses lossy UTF-8 decoding instead of rejecting the message.
    #[test]
    fn mqtt_message_uses_lossy_utf8_for_invalid_payload_bytes() {
        let request = mqtt_message_to_export_logs_request(&MqttMessage::new(
            "devices/a",
            Bytes::from(vec![b'f', b'o', 0x80, b'o']),
        ));

        let body = request.resource_logs[0].scope_logs[0].log_records[0]
            .body
            .as_ref()
            .expect("body must be present");
        assert_eq!(
            body.value,
            Some(AnyValueValue::StringValue("fo\u{fffd}o".to_owned()))
        );
    }
}
