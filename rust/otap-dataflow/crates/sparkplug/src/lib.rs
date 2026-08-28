// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Standalone Sparkplug topic parsing, payload decoding, and session state.

mod context;
mod payload;
mod state;
mod topic;

/// Generated Sparkplug protobuf types.
pub mod pb {
    #![allow(missing_docs)]
    #![allow(clippy::must_use_candidate)]

    include!(concat!(env!("OUT_DIR"), "/sparkplug.rs"));
}

pub use context::{DeathOrigin, ResolvedAlias, SignalType, SparkplugDecodeContext};
pub use payload::{DecodeError, SparkplugPayload};
pub use state::{
    CascadedDeviceDeath, DeviceState, GroupState, Metric, MetricValue, SparkplugState,
    SparkplugStateError, Store, Timestamp, UnsupportedMetricValue, VisitOutcome,
    classify_decode_context,
};
pub use topic::{
    LifecycleMessageType, LifecycleTopic, SPARKPLUG_NAMESPACE, SparkplugMessageType,
    StateTopicProfile, Topic, TopicParseError,
};

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;

    fn must_ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error}"),
        }
    }

    fn must_some<T>(value: Option<T>, context: &str) -> T {
        match value {
            Some(item) => item,
            None => panic!("missing value: {context}"),
        }
    }

    fn named_metric(
        name: &str,
        alias: u64,
        timestamp: Timestamp,
        datatype: pb::DataType,
        value: pb::payload::metric::Value,
    ) -> pb::payload::Metric {
        pb::payload::Metric {
            name: Some(name.to_owned()),
            alias: Some(alias),
            timestamp: Some(timestamp),
            datatype: Some(datatype as u32),
            is_historical: None,
            is_transient: None,
            is_null: None,
            metadata: Some(pb::payload::MetaData {
                is_multi_part: None,
                content_type: None,
                size: None,
                seq: None,
                file_name: None,
                file_type: None,
                md5: None,
                description: Some(format!("description:{name}")),
            }),
            properties: None,
            value: Some(value),
        }
    }

    fn alias_metric(
        alias: u64,
        timestamp: Timestamp,
        datatype: pb::DataType,
        value: pb::payload::metric::Value,
    ) -> pb::payload::Metric {
        pb::payload::Metric {
            name: None,
            alias: Some(alias),
            timestamp: Some(timestamp),
            datatype: Some(datatype as u32),
            is_historical: None,
            is_transient: None,
            is_null: None,
            metadata: None,
            properties: None,
            value: Some(value),
        }
    }

    fn payload(
        metrics: Vec<pb::payload::Metric>,
        timestamp: Timestamp,
        seq: u64,
    ) -> SparkplugPayload {
        SparkplugPayload::from(pb::Payload {
            timestamp: Some(timestamp),
            metrics,
            seq: Some(seq),
            uuid: None,
            body: None,
        })
    }

    fn lifecycle_topic(topic: &str) -> LifecycleTopic {
        let parsed = must_ok(Topic::parse(topic));
        must_ok(
            parsed
                .as_lifecycle_topic()
                .map_err(SparkplugStateError::NotLifecycleOrData),
        )
    }

    /// Scenario: Parsing every Sparkplug topic form, including both STATE profiles.
    /// Guarantees: All supported message types round-trip through Topic::parse and Display.
    #[test]
    fn parses_and_round_trips_supported_topics() {
        let topics = [
            "spBv1.0/group/NBIRTH/edge",
            "spBv1.0/group/NDEATH/edge",
            "spBv1.0/group/DBIRTH/edge/device",
            "spBv1.0/group/DDEATH/edge/device",
            "spBv1.0/group/NDATA/edge",
            "spBv1.0/group/DDATA/edge/device",
            "spBv1.0/group/NCMD/edge",
            "spBv1.0/group/DCMD/edge/device",
            "STATE/host-22",
            "spBv1.0/STATE/host-30",
        ];

        for topic in topics {
            let parsed = must_ok(Topic::parse(topic));
            assert_eq!(parsed.to_string(), topic);
        }
    }

    /// Scenario: Parsing malformed Sparkplug topic strings.
    /// Guarantees: Topic::parse rejects invalid namespace, shape, empty segments, and unknown message types.
    #[test]
    fn rejects_malformed_topics() {
        assert!(matches!(
            Topic::parse("bad/group/NBIRTH/edge"),
            Err(TopicParseError::InvalidNamespace(_))
        ));
        assert!(matches!(
            Topic::parse("spBv1.0//NBIRTH/edge"),
            Err(TopicParseError::EmptySegment("group_id"))
        ));
        assert!(matches!(
            Topic::parse("spBv1.0/group/BOGUS/edge"),
            Err(TopicParseError::UnknownMessageType(_))
        ));
        assert!(matches!(
            Topic::parse("spBv1.0/group/DBIRTH/edge"),
            Err(TopicParseError::MissingDeviceId(
                SparkplugMessageType::DBirth
            ))
        ));
        assert!(matches!(
            Topic::parse("spBv1.0/group/NDATA/edge/device"),
            Err(TopicParseError::UnexpectedDeviceId(
                SparkplugMessageType::NData
            ))
        ));
        assert!(matches!(
            Topic::parse("spBv1.0/group/STATE/edge"),
            Err(TopicParseError::InvalidStateShape(_))
        ));
    }

    /// Scenario: Visiting a birth payload and then an alias-only data payload.
    /// Guarantees: Store::define and visit_lifecycle_or_data preserve the metric definition and update alias-only values.
    #[test]
    fn visit_birth_then_alias_only_data() {
        let mut state = SparkplugState::new();
        let birth = payload(
            vec![
                named_metric(
                    "temperature",
                    7,
                    10,
                    pb::DataType::Float,
                    pb::payload::metric::Value::FloatValue(21.5),
                ),
                named_metric(
                    "bdSeq",
                    8,
                    11,
                    pb::DataType::UInt64,
                    pb::payload::metric::Value::LongValue(33),
                ),
            ],
            1000,
            1,
        );

        let birth_topic = lifecycle_topic("spBv1.0/group/NBIRTH/edge");
        let visit = must_ok(state.visit_message(&birth_topic, &birth, 2000));
        assert!(visit.cascaded_device_deaths.is_empty());

        let store = &must_some(
            must_some(state.group_ref("group"), "group").edge_node_ref("edge"),
            "edge",
        )
        .store;
        assert_eq!(store.birth_time(), Some(1000));
        assert_eq!(store.last_time(), Some(2000));
        assert!(store.is_online());
        assert_eq!(store.b_d_seq(), Some(33));

        let metric = must_some(store.metric_by_alias(7), "temperature alias");
        assert_eq!(metric.name, "temperature");
        assert_eq!(metric.start_timestamp, 10);
        assert_eq!(metric.description, "description:temperature");
        assert_eq!(metric.value, Some(MetricValue::Float(21.5)));

        let data = payload(
            vec![alias_metric(
                7,
                20,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(22.0),
            )],
            1001,
            2,
        );

        let data_topic = lifecycle_topic("spBv1.0/group/NDATA/edge");
        _ = must_ok(state.visit_message(&data_topic, &data, 3000));

        let updated_store = &must_some(
            must_some(state.group_ref("group"), "group").edge_node_ref("edge"),
            "edge",
        )
        .store;
        let updated_metric = must_some(updated_store.metric_by_name("temperature"), "temperature");
        assert_eq!(updated_metric.start_timestamp, 10);
        assert_eq!(updated_metric.timestamp, 20);
        assert_eq!(updated_metric.value, Some(MetricValue::Float(22.0)));
        assert_eq!(updated_store.last_time(), Some(3000));
    }

    /// Scenario: Receiving alias-only data before any defining birth payload.
    /// Guarantees: The state machine returns a typed rebirth-needed error instead of panicking or dropping data.
    #[test]
    fn rejects_alias_without_prior_birth() {
        let mut state = SparkplugState::new();
        let data = payload(
            vec![alias_metric(
                99,
                12,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(1.0),
            )],
            1000,
            1,
        );

        let topic = lifecycle_topic("spBv1.0/group/NDATA/edge");
        let error = match state.visit_message(&topic, &data, 2000) {
            Ok(_) => panic!("expected rebirth-needed error"),
            Err(error) => error,
        };

        assert_eq!(error, SparkplugStateError::RebirthNeeded { alias: 99 });
    }

    /// Scenario: An edge node with multiple tracked devices publishes NDEATH.
    /// Guarantees: NDEATH marks the edge node and every tracked device offline and returns one cascade entry per device.
    #[test]
    fn ndeath_cascades_to_all_known_devices() {
        let mut state = SparkplugState::new();
        let device_birth = payload(
            vec![named_metric(
                "pressure",
                1,
                10,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(5.0),
            )],
            1000,
            1,
        );

        let first_topic = lifecycle_topic("spBv1.0/group/DBIRTH/edge/device-a");
        _ = must_ok(state.visit_message(&first_topic, &device_birth, 2000));

        let second_topic = lifecycle_topic("spBv1.0/group/DBIRTH/edge/device-b");
        _ = must_ok(state.visit_message(&second_topic, &device_birth, 2100));

        let ndeath = payload(Vec::new(), 1002, 2);
        let ndeath_topic = lifecycle_topic("spBv1.0/group/NDEATH/edge");
        let outcome = must_ok(state.visit_message(&ndeath_topic, &ndeath, 3000));

        assert_eq!(outcome.cascaded_device_deaths.len(), 2);
        assert!(
            outcome
                .cascaded_device_deaths
                .contains(&CascadedDeviceDeath {
                    group_id: "group".to_owned(),
                    edge_node_id: "edge".to_owned(),
                    device_id: "device-a".to_owned(),
                })
        );
        assert!(
            outcome
                .cascaded_device_deaths
                .contains(&CascadedDeviceDeath {
                    group_id: "group".to_owned(),
                    edge_node_id: "edge".to_owned(),
                    device_id: "device-b".to_owned(),
                })
        );

        let edge = must_some(
            must_some(state.group_ref("group"), "group").edge_node_ref("edge"),
            "edge",
        );
        assert!(!edge.store.is_online());
        assert!(
            !must_some(edge.device_ref("device-a"), "device-a")
                .store
                .is_online()
        );
        assert!(
            !must_some(edge.device_ref("device-b"), "device-b")
                .store
                .is_online()
        );
        assert_eq!(
            must_some(edge.device_ref("device-a"), "device-a")
                .store
                .last_time(),
            Some(3000)
        );
    }

    /// Scenario: Decoding protobuf payloads that use the supported primitive Sparkplug value kinds.
    /// Guarantees: SparkplugPayload::decode round-trips protobuf bytes and Store visits preserve each supported value type.
    #[test]
    fn decodes_supported_metric_value_kinds() {
        let original = pb::Payload {
            timestamp: Some(101),
            metrics: vec![
                named_metric(
                    "int",
                    1,
                    1,
                    pb::DataType::Int32,
                    pb::payload::metric::Value::IntValue(10),
                ),
                named_metric(
                    "long",
                    2,
                    2,
                    pb::DataType::UInt64,
                    pb::payload::metric::Value::LongValue(11),
                ),
                named_metric(
                    "float",
                    3,
                    3,
                    pb::DataType::Float,
                    pb::payload::metric::Value::FloatValue(1.5),
                ),
                named_metric(
                    "double",
                    4,
                    4,
                    pb::DataType::Double,
                    pb::payload::metric::Value::DoubleValue(2.5),
                ),
                named_metric(
                    "boolean",
                    5,
                    5,
                    pb::DataType::Boolean,
                    pb::payload::metric::Value::BooleanValue(true),
                ),
                named_metric(
                    "string",
                    6,
                    6,
                    pb::DataType::String,
                    pb::payload::metric::Value::StringValue("value".to_owned()),
                ),
                named_metric(
                    "bytes",
                    7,
                    7,
                    pb::DataType::Bytes,
                    pb::payload::metric::Value::BytesValue(vec![1, 2, 3]),
                ),
            ],
            seq: Some(7),
            uuid: None,
            body: None,
        };
        let bytes = original.encode_to_vec();
        let decoded = must_ok(SparkplugPayload::decode(&bytes));

        assert_eq!(decoded.as_inner(), &original);

        let mut store = Store::default();
        must_ok(store.visit_lifecycle_or_data(LifecycleMessageType::NBirth, &decoded, 999));

        assert_eq!(
            must_some(store.metric_by_name("int"), "int").value,
            Some(MetricValue::Int(10))
        );
        assert_eq!(
            must_some(store.metric_by_name("long"), "long").value,
            Some(MetricValue::Long(11))
        );
        assert_eq!(
            must_some(store.metric_by_name("float"), "float").value,
            Some(MetricValue::Float(1.5))
        );
        assert_eq!(
            must_some(store.metric_by_name("double"), "double").value,
            Some(MetricValue::Double(2.5))
        );
        assert_eq!(
            must_some(store.metric_by_name("boolean"), "boolean").value,
            Some(MetricValue::Boolean(true))
        );
        assert_eq!(
            must_some(store.metric_by_name("string"), "string").value,
            Some(MetricValue::String("value".to_owned()))
        );
        assert_eq!(
            must_some(store.metric_by_name("bytes"), "bytes").value,
            Some(MetricValue::Bytes(vec![1, 2, 3]))
        );
    }

    /// Scenario: Decoding a payload with a complex Sparkplug dataset value.
    /// Guarantees: Unsupported metric kinds are preserved as typed placeholders instead of panicking.
    #[test]
    fn decodes_unsupported_dataset_without_panicking() {
        let decoded = payload(
            vec![named_metric(
                "dataset",
                42,
                99,
                pb::DataType::DataSet,
                pb::payload::metric::Value::DatasetValue(pb::payload::DataSet {
                    num_of_columns: Some(1),
                    columns: vec!["value".to_owned()],
                    types: vec![pb::DataType::Float as u32],
                    rows: Vec::new(),
                }),
            )],
            1000,
            3,
        );

        let mut store = Store::default();
        must_ok(store.visit_lifecycle_or_data(LifecycleMessageType::NBirth, &decoded, 2000));

        assert_eq!(
            must_some(store.metric_by_name("dataset"), "dataset").value,
            Some(MetricValue::Unsupported(UnsupportedMetricValue::Dataset))
        );
    }

    /// Scenario: Building decode context for every Sparkplug lifecycle and data message type.
    /// Guarantees: NDEATH and DDEATH classify as logs, while birth and data messages classify as metrics.
    #[test]
    fn classifies_signal_type_by_message_type() {
        let mut state = SparkplugState::new();
        let birth = payload(
            vec![named_metric(
                "temperature",
                7,
                10,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(21.5),
            )],
            1000,
            1,
        );

        let node_birth_topic = lifecycle_topic("spBv1.0/group/NBIRTH/edge");
        _ = must_ok(state.visit_message(&node_birth_topic, &birth, 2000));

        let cases = [
            ("spBv1.0/group/NBIRTH/edge", SignalType::Metric),
            ("spBv1.0/group/DBIRTH/edge/device", SignalType::Metric),
            ("spBv1.0/group/NDATA/edge", SignalType::Metric),
            ("spBv1.0/group/DDATA/edge/device", SignalType::Metric),
            ("spBv1.0/group/NDEATH/edge", SignalType::Log),
            ("spBv1.0/group/DDEATH/edge/device", SignalType::Log),
        ];

        for (topic_text, expected_signal) in cases {
            let topic = lifecycle_topic(topic_text);
            let context = must_ok(state.classify_decode_context(
                &topic,
                &birth,
                DeathOrigin::ExplicitPublish,
            ));

            assert_eq!(context.signal, expected_signal);
        }
    }

    /// Scenario: Building decode context for an alias-only data payload after a defining birth.
    /// Guarantees: Decode context captures only the aliases referenced by the payload and resolves them to names.
    #[test]
    fn decode_context_resolves_only_referenced_aliases() {
        let mut state = SparkplugState::new();
        let birth = payload(
            vec![
                named_metric(
                    "temperature",
                    7,
                    10,
                    pb::DataType::Float,
                    pb::payload::metric::Value::FloatValue(21.5),
                ),
                named_metric(
                    "pressure",
                    8,
                    11,
                    pb::DataType::Float,
                    pb::payload::metric::Value::FloatValue(5.0),
                ),
                named_metric(
                    "bdSeq",
                    9,
                    12,
                    pb::DataType::UInt64,
                    pb::payload::metric::Value::LongValue(77),
                ),
            ],
            1000,
            1,
        );
        let birth_topic = lifecycle_topic("spBv1.0/group/NBIRTH/edge");
        _ = must_ok(state.visit_message(&birth_topic, &birth, 2000));

        let data = payload(
            vec![alias_metric(
                7,
                20,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(22.0),
            )],
            1001,
            2,
        );
        let data_topic = lifecycle_topic("spBv1.0/group/NDATA/edge");
        let context =
            must_ok(state.classify_decode_context(&data_topic, &data, DeathOrigin::Unknown));

        assert_eq!(context.resolved_aliases.len(), 1);
        assert_eq!(
            context.resolved_aliases[0],
            ResolvedAlias {
                alias: 7,
                name: "temperature".to_owned(),
            }
        );
        assert_eq!(context.b_d_seq, Some(77));
    }
}
