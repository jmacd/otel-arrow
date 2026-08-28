// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT exporter backed by the `mqtt_egress` capability.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = MQTT_EXPORTER_URN,
    target = "otel.exporter.mqtt",
);

use async_trait::async_trait;
use bytes::Bytes;
use linkme::distributed_slice;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ConsumerEffectHandlerExtension;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::capability::mqtt::MqttMessage;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_egress::MqttEgress as MqttEgressCap;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_egress::MqttEgress as LocalMqttEgress;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::LogsData;
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::Arc;

/// URN identifying this exporter in pipeline configuration.
pub const MQTT_EXPORTER_URN: &str = "urn:otel:exporter:mqtt";

/// Configuration for `exporter:mqtt`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Fixed destination topic for every published log record.
    pub topic: String,
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.topic.trim().is_empty() {
            return Err(ConfigError::InvalidUserConfig {
                error: "mqtt exporter topic must not be empty".to_owned(),
            });
        }
        Ok(())
    }
}

/// MQTT exporter.
pub struct MqttExporter {
    config: Config,
    mqtt_egress: Box<dyn LocalMqttEgress>,
}

/// Declares the MQTT exporter as a local exporter factory.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static MQTT_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: MQTT_EXPORTER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config = MqttExporter::parse_config(&node_config.config)?;
        let mqtt_egress = capabilities
            .require_local::<MqttEgressCap>()
            .map_err(|error| ConfigError::InvalidUserConfig {
                error: error.to_string(),
            })?;

        Ok(ExporterWrapper::local(
            MqttExporter {
                config,
                mqtt_egress,
            },
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |config| MqttExporter::parse_config(config).map(|_| ()),
};

impl MqttExporter {
    fn parse_config(config: &serde_json::Value) -> Result<Config, ConfigError> {
        let config: Config = serde_json::from_value(config.clone()).map_err(|error| {
            ConfigError::InvalidUserConfig {
                error: error.to_string(),
            }
        })?;
        config.validate()?;
        Ok(config)
    }

    fn collect_messages(&self, pdata: &OtapPdata) -> Result<Vec<MqttMessage>, String> {
        let otlp_bytes: OtlpProtoBytes = pdata
            .payload_ref()
            .clone()
            .try_into_with_default()
            .map_err(|error| format!("failed to convert payload to OTLP bytes: {error}"))?;

        match OtlpProtoMessage::try_from(otlp_bytes)
            .map_err(|error| format!("failed to decode OTLP logs payload: {error}"))?
        {
            OtlpProtoMessage::Logs(logs) => {
                Ok(collect_messages_from_logs_data(&logs, &self.config.topic))
            }
            other => Err(format!(
                "mqtt exporter expected OTLP logs but received {other:?}"
            )),
        }
    }

    fn unsupported_signal_reason(signal: SignalType) -> String {
        format!("mqtt exporter supports the logs signal only; received {signal:?}")
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for MqttExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let exporter = *self;
        loop {
            match inbox.recv().await? {
                Message::Control(NodeControlMsg::CollectTelemetry { .. }) => {}
                Message::Control(NodeControlMsg::Config { .. }) => {}
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    return Ok(TerminalState::new::<
                        [otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot; 0],
                    >(deadline, []));
                }
                Message::PData(pdata) => {
                    export_pdata(&exporter, pdata, &effect_handler).await?;
                }
                _ => {}
            }
        }
    }
}

async fn export_pdata(
    exporter: &MqttExporter,
    pdata: OtapPdata,
    effect_handler: &EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if pdata.signal_type() != SignalType::Logs {
        let reason = MqttExporter::unsupported_signal_reason(pdata.signal_type());
        otel_warn!(
            "mqtt.exporter.unsupported_signal",
            signal = format!("{:?}", pdata.signal_type()),
            message = reason.as_str()
        );
        effect_handler
            .notify_nack(NackMsg::new_permanent(reason, pdata))
            .await?;
        return Ok(());
    }

    let messages = match exporter.collect_messages(&pdata) {
        Ok(messages) => messages,
        Err(reason) => {
            effect_handler
                .notify_nack(NackMsg::new_permanent(reason, pdata))
                .await?;
            return Ok(());
        }
    };

    for message in messages {
        if let Err(error) = exporter.mqtt_egress.publish(message).await {
            let reason = format!("mqtt publish failed: {error}");
            effect_handler
                .notify_nack(NackMsg::new(reason, pdata))
                .await?;
            return Ok(());
        }
    }

    effect_handler.notify_ack(AckMsg::new(pdata)).await?;
    Ok(())
}

fn collect_messages_from_logs_data(logs: &LogsData, topic: &str) -> Vec<MqttMessage> {
    let mut messages = Vec::new();
    for resource_logs in &logs.resource_logs {
        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                let payload = record
                    .body
                    .as_ref()
                    .map(any_value_to_payload_string)
                    .unwrap_or_default();
                messages.push(MqttMessage::new(topic.to_owned(), Bytes::from(payload)));
            }
        }
    }
    messages
}

fn any_value_to_payload_string(value: &AnyValue) -> String {
    match value.value.as_ref() {
        None => String::new(),
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => lossy_utf8(value),
        Some(any_value::Value::ArrayValue(_)) | Some(any_value::Value::KvlistValue(_)) => {
            any_value_to_json(value).to_string()
        }
    }
}

fn any_value_to_json(value: &AnyValue) -> JsonValue {
    match value.value.as_ref() {
        None => JsonValue::Null,
        Some(any_value::Value::StringValue(value)) => JsonValue::String(value.clone()),
        Some(any_value::Value::BoolValue(value)) => JsonValue::Bool(*value),
        Some(any_value::Value::IntValue(value)) => JsonValue::Number((*value).into()),
        Some(any_value::Value::DoubleValue(value)) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Some(any_value::Value::BytesValue(value)) => JsonValue::String(lossy_utf8(value)),
        Some(any_value::Value::ArrayValue(value)) => {
            JsonValue::Array(value.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let mut map = JsonMap::new();
            for attribute in &value.values {
                let entry = attribute
                    .value
                    .as_ref()
                    .map(any_value_to_json)
                    .unwrap_or(JsonValue::Null);
                let _ = map.insert(attribute.key.clone(), entry);
            }
            JsonValue::Object(map)
        }
    }
}

fn lossy_utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs,
    };
    use prost::Message;

    /// Scenario: A log record body is already an OTLP string value.
    /// Guarantees: The exporter republishes that text byte-for-byte on the configured MQTT topic.
    #[test]
    fn string_log_body_maps_directly_to_mqtt_payload() {
        let bytes = build_logs_request(vec![AnyValue {
            value: Some(any_value::Value::StringValue("hello mqtt".to_owned())),
        }])
        .encode_to_vec();
        let logs = LogsData::decode(bytes.as_slice()).expect("logs data");

        let messages = collect_messages_from_logs_data(&logs, "egress/topic");

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].topic, "egress/topic");
        assert_eq!(messages[0].payload, Bytes::from_static(b"hello mqtt"));
    }

    /// Scenario: A log record body carries non-string OTLP values.
    /// Guarantees: The exporter falls back to stable string representations instead of panicking or dropping the record.
    #[test]
    fn non_string_log_bodies_use_best_effort_string_fallbacks() {
        let bytes = build_logs_request(vec![
            AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            },
            AnyValue {
                value: Some(any_value::Value::BytesValue(vec![b'a', 0x80, b'b'])),
            },
            AnyValue {
                value: Some(any_value::Value::KvlistValue(
                    vec![KeyValue {
                        key: "kind".to_owned(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::IntValue(7)),
                        }),
                    }]
                    .into(),
                )),
            },
        ])
        .encode_to_vec();
        let logs = LogsData::decode(bytes.as_slice()).expect("logs data");

        let messages = collect_messages_from_logs_data(&logs, "egress/topic");

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].payload, Bytes::from_static(b"true"));
        assert_eq!(messages[1].payload, Bytes::from("a\u{fffd}b"));
        assert_eq!(messages[2].payload, Bytes::from_static(br#"{"kind":7}"#));
    }

    fn build_logs_request(bodies: Vec<AnyValue>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: bodies
                        .into_iter()
                        .map(|body| LogRecord {
                            body: Some(body),
                            ..LogRecord::default()
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}
