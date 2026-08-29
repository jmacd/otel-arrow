// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pure conversion from a decoded Sparkplug NDEATH/DDEATH message into an
//! OTLP `ExportLogsServiceRequest`, per the mapping in
//! `rfcs/0003-mqtt-service-capabilities.md` ("Sparkplug as a pluggable byte
//! representation").
//!
//! # Scope
//!
//! This module is deliberately a pure function library, not a wired
//! pipeline component. `otel_arrow_dfe_sparkplug::SparkplugDecodeContext` is
//! computed today by `extension:mqtt_sparkplug` but has no carrier to reach
//! a receiver or processor yet: the engine's `MqttMessage` capability type
//! and `OtapPdata`/`Context` are both deliberately protocol-generic (see
//! `crates/engine/src/capability/mqtt/models/mqtt_message.rs` and
//! `rfcs/0004-pdata-context.md`), and extending either to carry
//! Sparkplug-specific state is out of scope for this codec. Wiring this
//! function into a running receiver/processor is tracked as follow-on work
//! (`sparkplug-e2e-datalogger-demo`).

use bytes::Bytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_sparkplug::{DeathOrigin, SparkplugDecodeContext, SparkplugMessageType};

/// Resource attribute carrying the Sparkplug group identifier.
pub const SPARKPLUG_GROUP_ID_ATTRIBUTE: &str = "sparkplug.group_id";
/// Resource attribute carrying the Sparkplug edge node identifier.
pub const SPARKPLUG_EDGE_NODE_ID_ATTRIBUTE: &str = "sparkplug.edge_node_id";
/// Resource attribute carrying the Sparkplug device identifier, present only
/// for device-scoped deaths (DDEATH).
pub const SPARKPLUG_DEVICE_ID_ATTRIBUTE: &str = "sparkplug.device_id";
/// Log attribute carrying the birth/death sequence number, when known.
pub const SPARKPLUG_BD_SEQ_ATTRIBUTE: &str = "sparkplug.bd_seq";
/// Log attribute carrying the best-effort death origin classification.
pub const SPARKPLUG_DEATH_ORIGIN_ATTRIBUTE: &str = "sparkplug.death_origin";
/// Log attribute carrying the MQTT topic the death was observed on.
pub const MQTT_TOPIC_ATTRIBUTE: &str = "mqtt.topic";
/// Log attribute carrying the MQTT QoS level, when known.
pub const MQTT_QOS_ATTRIBUTE: &str = "mqtt.qos";
/// Log attribute carrying the MQTT retain flag, when known.
pub const MQTT_RETAIN_ATTRIBUTE: &str = "mqtt.retain";

/// The OTLP log record `event_name` emitted for an NDEATH message.
pub const NODE_DEATH_EVENT_NAME: &str = "sparkplug.node.death";
/// The OTLP log record `event_name` emitted for a DDEATH message.
pub const DEVICE_DEATH_EVENT_NAME: &str = "sparkplug.device.death";

/// Errors returned when converting a Sparkplug decode context to an OTLP
/// death log.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DeathLogCodecError {
    /// The decode context did not describe an NDEATH or DDEATH message.
    #[error("message type {0:?} is not a Sparkplug death message")]
    NotADeathMessage(SparkplugMessageType),
}

/// MQTT envelope fields attached to the death log as attributes, all
/// optional because not every transport/extension surfaces them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MqttDeathEnvelope {
    /// The MQTT topic the death message was observed on.
    pub topic: String,
    /// The MQTT QoS level, when known.
    pub qos: Option<u8>,
    /// The MQTT retain flag, when known.
    pub retain: Option<bool>,
}

/// Converts a decoded Sparkplug NDEATH/DDEATH context into an OTLP
/// `ExportLogsServiceRequest` containing exactly one log record, per the
/// mapping rules in RFC 0003.
///
/// `observed_time_unix_nano` populates `LogRecord.observed_time_unix_nano`;
/// callers supply it rather than this function reading the clock, keeping
/// the conversion a pure function of its inputs.
pub fn death_log_request(
    context: &SparkplugDecodeContext,
    payload: &Bytes,
    envelope: &MqttDeathEnvelope,
    observed_time_unix_nano: u64,
) -> Result<ExportLogsServiceRequest, DeathLogCodecError> {
    let event_name = match context.message_type {
        SparkplugMessageType::NDeath => NODE_DEATH_EVENT_NAME,
        SparkplugMessageType::DDeath => DEVICE_DEATH_EVENT_NAME,
        other => return Err(DeathLogCodecError::NotADeathMessage(other)),
    };

    let mut resource_attributes = vec![
        string_attribute(SPARKPLUG_GROUP_ID_ATTRIBUTE, &context.group_id),
        string_attribute(SPARKPLUG_EDGE_NODE_ID_ATTRIBUTE, &context.edge_node_id),
    ];
    if let Some(device_id) = &context.device_id {
        resource_attributes.push(string_attribute(SPARKPLUG_DEVICE_ID_ATTRIBUTE, device_id));
    }

    let mut log_attributes = vec![
        string_attribute(
            SPARKPLUG_DEATH_ORIGIN_ATTRIBUTE,
            death_origin_str(context.death_origin),
        ),
        string_attribute(MQTT_TOPIC_ATTRIBUTE, &envelope.topic),
    ];
    if let Some(b_d_seq) = context.b_d_seq {
        log_attributes.push(int_attribute(SPARKPLUG_BD_SEQ_ATTRIBUTE, b_d_seq as i64));
    }
    if let Some(qos) = envelope.qos {
        log_attributes.push(int_attribute(MQTT_QOS_ATTRIBUTE, i64::from(qos)));
    }
    if let Some(retain) = envelope.retain {
        log_attributes.push(bool_attribute(MQTT_RETAIN_ATTRIBUTE, retain));
    }

    Ok(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    observed_time_unix_nano,
                    event_name: event_name.to_owned(),
                    body: Some(bytes_value(payload.clone())),
                    attributes: log_attributes,
                    ..LogRecord::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    })
}

fn death_origin_str(origin: DeathOrigin) -> &'static str {
    match origin {
        DeathOrigin::RouterWill => "router_will",
        DeathOrigin::ExplicitPublish => "explicit_publish",
        DeathOrigin::Unknown => "unknown",
    }
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

fn int_attribute(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn bool_attribute(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
        }),
    }
}

fn bytes_value(payload: Bytes) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BytesValue(payload.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_sparkplug::{ResolvedAlias, SignalType};

    fn node_death_context(
        b_d_seq: Option<u64>,
        death_origin: DeathOrigin,
    ) -> SparkplugDecodeContext {
        SparkplugDecodeContext {
            group_id: "plant1".to_owned(),
            edge_node_id: "edgeA".to_owned(),
            device_id: None,
            message_type: SparkplugMessageType::NDeath,
            signal: SignalType::Log,
            b_d_seq,
            resolved_aliases: Vec::<ResolvedAlias>::new(),
            death_origin,
        }
    }

    fn device_death_context(b_d_seq: Option<u64>) -> SparkplugDecodeContext {
        SparkplugDecodeContext {
            group_id: "plant1".to_owned(),
            edge_node_id: "edgeA".to_owned(),
            device_id: Some("pump-1".to_owned()),
            message_type: SparkplugMessageType::DDeath,
            signal: SignalType::Log,
            b_d_seq,
            resolved_aliases: Vec::<ResolvedAlias>::new(),
            death_origin: DeathOrigin::ExplicitPublish,
        }
    }

    fn only_log_record(request: &ExportLogsServiceRequest) -> &LogRecord {
        &request.resource_logs[0].scope_logs[0].log_records[0]
    }

    fn attribute_value<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a any_value::Value> {
        attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.value.as_ref())
    }

    /// Scenario: An NDEATH decode context with a known bdSeq is converted to an OTLP log.
    /// Guarantees: The log has event_name "sparkplug.node.death", resource attrs for
    /// group/edge but no device attr, and log attrs for bdSeq/death_origin/topic/qos/retain.
    #[test]
    fn ndeath_maps_to_node_death_event() {
        let context = node_death_context(Some(7), DeathOrigin::ExplicitPublish);
        let envelope = MqttDeathEnvelope {
            topic: "spBv1.0/plant1/NDEATH/edgeA".to_owned(),
            qos: Some(1),
            retain: Some(false),
        };
        let payload = Bytes::from_static(b"raw-ndeath-bytes");

        let request =
            death_log_request(&context, &payload, &envelope, 1_000).expect("NDEATH must convert");

        let resource = request.resource_logs[0]
            .resource
            .as_ref()
            .expect("resource");
        assert_eq!(
            attribute_value(&resource.attributes, SPARKPLUG_GROUP_ID_ATTRIBUTE),
            Some(&any_value::Value::StringValue("plant1".to_owned()))
        );
        assert_eq!(
            attribute_value(&resource.attributes, SPARKPLUG_EDGE_NODE_ID_ATTRIBUTE),
            Some(&any_value::Value::StringValue("edgeA".to_owned()))
        );
        assert_eq!(
            attribute_value(&resource.attributes, SPARKPLUG_DEVICE_ID_ATTRIBUTE),
            None,
            "node death must not carry a device_id resource attribute"
        );

        let log_record = only_log_record(&request);
        assert_eq!(log_record.event_name, NODE_DEATH_EVENT_NAME);
        assert_eq!(log_record.observed_time_unix_nano, 1_000);
        assert_eq!(
            log_record.body,
            Some(AnyValue {
                value: Some(any_value::Value::BytesValue(b"raw-ndeath-bytes".to_vec()))
            })
        );
        assert_eq!(
            attribute_value(&log_record.attributes, SPARKPLUG_BD_SEQ_ATTRIBUTE),
            Some(&any_value::Value::IntValue(7))
        );
        assert_eq!(
            attribute_value(&log_record.attributes, SPARKPLUG_DEATH_ORIGIN_ATTRIBUTE),
            Some(&any_value::Value::StringValue(
                "explicit_publish".to_owned()
            ))
        );
        assert_eq!(
            attribute_value(&log_record.attributes, MQTT_TOPIC_ATTRIBUTE),
            Some(&any_value::Value::StringValue(
                "spBv1.0/plant1/NDEATH/edgeA".to_owned()
            ))
        );
        assert_eq!(
            attribute_value(&log_record.attributes, MQTT_QOS_ATTRIBUTE),
            Some(&any_value::Value::IntValue(1))
        );
        assert_eq!(
            attribute_value(&log_record.attributes, MQTT_RETAIN_ATTRIBUTE),
            Some(&any_value::Value::BoolValue(false))
        );
    }

    /// Scenario: A cascaded DDEATH decode context (synthetic, empty payload, no known bdSeq)
    /// is converted to an OTLP log.
    /// Guarantees: The log has event_name "sparkplug.device.death", a device_id resource
    /// attribute, an empty-bytes body, and omits the bdSeq attribute when unknown.
    #[test]
    fn ddeath_maps_to_device_death_event_and_omits_unknown_bd_seq() {
        let context = device_death_context(None);
        let envelope = MqttDeathEnvelope {
            topic: "spBv1.0/plant1/DDEATH/edgeA/pump-1".to_owned(),
            qos: None,
            retain: None,
        };
        let payload = Bytes::new();

        let request =
            death_log_request(&context, &payload, &envelope, 2_000).expect("DDEATH must convert");

        let resource = request.resource_logs[0]
            .resource
            .as_ref()
            .expect("resource");
        assert_eq!(
            attribute_value(&resource.attributes, SPARKPLUG_DEVICE_ID_ATTRIBUTE),
            Some(&any_value::Value::StringValue("pump-1".to_owned()))
        );

        let log_record = only_log_record(&request);
        assert_eq!(log_record.event_name, DEVICE_DEATH_EVENT_NAME);
        assert_eq!(
            log_record.body,
            Some(AnyValue {
                value: Some(any_value::Value::BytesValue(Vec::new()))
            })
        );
        assert_eq!(
            attribute_value(&log_record.attributes, SPARKPLUG_BD_SEQ_ATTRIBUTE),
            None
        );
        assert_eq!(
            attribute_value(&log_record.attributes, MQTT_QOS_ATTRIBUTE),
            None
        );
        assert_eq!(
            attribute_value(&log_record.attributes, MQTT_RETAIN_ATTRIBUTE),
            None
        );
    }

    /// Scenario: A decode context for a non-death message type (e.g. NBIRTH) is passed to
    /// the death codec.
    /// Guarantees: The codec rejects it with `NotADeathMessage` instead of emitting a
    /// misleading death log.
    #[test]
    fn non_death_message_type_is_rejected() {
        let mut context = node_death_context(None, DeathOrigin::Unknown);
        context.message_type = SparkplugMessageType::NBirth;
        let envelope = MqttDeathEnvelope::default();

        let error = death_log_request(&context, &Bytes::new(), &envelope, 0)
            .expect_err("NBIRTH must not convert");

        assert_eq!(
            error,
            DeathLogCodecError::NotADeathMessage(SparkplugMessageType::NBirth)
        );
    }

    /// Scenario: Death origin is `router_will` (the router itself detected and delivered
    /// the Will for a disconnected edge node), not an explicit publish.
    /// Guarantees: The mapped log attribute reads "router_will", distinguishing this from
    /// an explicit NDEATH/DDEATH publish for downstream analysis.
    #[test]
    fn router_will_origin_is_distinguished_from_explicit_publish() {
        let context = node_death_context(Some(3), DeathOrigin::RouterWill);
        let envelope = MqttDeathEnvelope {
            topic: "spBv1.0/plant1/NDEATH/edgeA".to_owned(),
            qos: None,
            retain: None,
        };

        let request =
            death_log_request(&context, &Bytes::new(), &envelope, 0).expect("NDEATH must convert");
        let log_record = only_log_record(&request);

        assert_eq!(
            attribute_value(&log_record.attributes, SPARKPLUG_DEATH_ORIGIN_ATTRIBUTE),
            Some(&any_value::Value::StringValue("router_will".to_owned()))
        );
    }
}
