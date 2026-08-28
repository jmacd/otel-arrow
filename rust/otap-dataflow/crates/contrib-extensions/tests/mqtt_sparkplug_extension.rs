// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "mqtt-sparkplug-extension")]

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_contrib_extensions::mqtt_sparkplug::{
    MQTT_SPARKPLUG_EXTENSION, MQTT_SPARKPLUG_URN,
};
use otel_arrow_dfe_contrib_nodes::common::mqtt::backend::{
    MqttBackendConfig, RawMqttMessage, connect, publish,
};
use otel_arrow_dfe_engine::capability::ExtensionCapability;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_egress::MqttEgress as MqttEgressCap;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_ingress::MqttIngress as MqttIngressCap;
use otel_arrow_dfe_engine::capability::registry::CapabilityRegistry;
use otel_arrow_dfe_engine::config::ExtensionConfig;
use otel_arrow_dfe_engine::control::ExtensionControlMsg;
use otel_arrow_dfe_engine::extension::ExtensionBundle;
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_ingress::MqttIngress as _;
use otel_arrow_dfe_engine::testing::capability::resolve_bindings_for_test;
use otel_arrow_dfe_engine::testing::liveness::completes_within;
use otel_arrow_dfe_engine::testing::{setup_test_runtime, test_extension_ctx};
use otel_arrow_dfe_sparkplug::SparkplugPayload;
use otel_arrow_dfe_sparkplug::pb;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;

fn test_metrics_reporter() -> MetricsReporter {
    let (tx, _rx) = flume::bounded(1);
    MetricsReporter::new(tx)
}

fn reserve_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve local port");
    listener.local_addr().expect("reserved local addr").port()
}

fn build_bundle(port: u16) -> ExtensionBundle {
    let extension_id = "mqtt_sparkplug_test".into();
    let user_config = Arc::new(ExtensionUserConfig::new(
        MQTT_SPARKPLUG_URN.into(),
        serde_json::json!({
            "bind_host": "127.0.0.1",
            "bind_port": port,
            "host_id": "otap-datalogger-01",
            "state_profile": "sparkplug_3",
        }),
    ));
    let (ext_ctx, _registry) = test_extension_ctx();
    (MQTT_SPARKPLUG_EXTENSION.create)(
        &ext_ctx,
        extension_id,
        user_config,
        &ExtensionConfig::new("mqtt_sparkplug_test"),
    )
    .expect("create mqtt_sparkplug bundle")
}

fn resolve_capabilities(
    bundle: &ExtensionBundle,
) -> otel_arrow_dfe_engine::capability::registry::Capabilities {
    let mut registry = CapabilityRegistry::new();
    bundle
        .register_into(
            MQTT_SPARKPLUG_EXTENSION.capabilities.as_ref(),
            &mut registry,
        )
        .expect("register bundle capabilities");

    let bindings = HashMap::from([
        (MqttIngressCap::name().into(), "mqtt_sparkplug_test".into()),
        (MqttEgressCap::name().into(), "mqtt_sparkplug_test".into()),
    ]);
    let known_extensions = HashSet::from(["mqtt_sparkplug_test".into()]);

    resolve_bindings_for_test(&bindings, &registry, &known_extensions)
        .expect("resolve mqtt_sparkplug capability bindings")
}

fn named_metric(
    name: &str,
    alias: u64,
    timestamp: u64,
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
        metadata: None,
        properties: None,
        value: Some(value),
    }
}

fn alias_metric(
    alias: u64,
    timestamp: u64,
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

fn sparkplug_payload(metrics: Vec<pb::payload::Metric>, timestamp: u64, seq: u64) -> Bytes {
    Bytes::from(
        SparkplugPayload::from(pb::Payload {
            timestamp: Some(timestamp),
            metrics,
            seq: Some(seq),
            uuid: None,
            body: None,
        })
        .encode_to_vec(),
    )
}

/// Scenario: A connected Sparkplug device publishes NBIRTH followed by alias-only NDATA.
/// Guarantees: The resolved `mqtt_ingress` capability relays both raw publishes unchanged after the extension updates Sparkplug state.
#[test]
fn mqtt_sparkplug_ingress_relays_birth_then_alias_only_data() {
    let (runtime, local_set) = setup_test_runtime();
    runtime.block_on(local_set.run_until(async {
        let port = reserve_local_port();
        let mut bundle = build_bundle(port);
        let capabilities = resolve_capabilities(&bundle);
        let ingress = capabilities
            .require_local::<MqttIngressCap>()
            .expect("resolve local mqtt_ingress");

        let mut wrapper = bundle.take_local().expect("local mqtt_sparkplug wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let extension_task =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut stream = ingress
            .subscribe("spBv1.0/#")
            .await
            .expect("subscribe to mqtt ingress");

        let connection = connect(MqttBackendConfig {
            hostname: "127.0.0.1".to_string(),
            port,
            client_id: Some("sparkplugclient-birth".to_string()),
            subscribe_topic_filter: None,
            connect_timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("connect mqtt test client");
        let client = connection.client;
        let receiver = connection.receiver;
        let driver = connection.driver;
        let driver_task = tokio::task::spawn_local(async move { driver.drive().await });

        let birth_payload = sparkplug_payload(
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
        publish(
            &client,
            RawMqttMessage {
                topic: "spBv1.0/group/NBIRTH/edge".to_string(),
                payload: birth_payload.clone(),
            },
        )
        .await
        .expect("publish NBIRTH");

        let data_payload = sparkplug_payload(
            vec![alias_metric(
                7,
                20,
                pb::DataType::Float,
                pb::payload::metric::Value::FloatValue(22.0),
            )],
            1001,
            2,
        );
        publish(
            &client,
            RawMqttMessage {
                topic: "spBv1.0/group/NDATA/edge".to_string(),
                payload: data_payload.clone(),
            },
        )
        .await
        .expect("publish alias-only NDATA");

        let birth_message =
            completes_within(Duration::from_secs(5), "birth ingress item", stream.next())
                .await
                .expect("birth ingress stream item");
        assert_eq!(birth_message.topic, "spBv1.0/group/NBIRTH/edge");
        assert_eq!(birth_message.payload, birth_payload);

        let data_message =
            completes_within(Duration::from_secs(5), "data ingress item", stream.next())
                .await
                .expect("data ingress stream item");
        assert_eq!(data_message.topic, "spBv1.0/group/NDATA/edge");
        assert_eq!(data_message.payload, data_payload);

        drop(client);
        drop(receiver);
        control
            .send(ExtensionControlMsg::Shutdown {
                deadline: Instant::now(),
                reason: "test complete".into(),
            })
            .await
            .expect("send extension shutdown");
        assert!(extension_task.await.expect("extension task join").is_ok());
        let _ = completes_within(
            Duration::from_secs(5),
            "mqtt client driver shutdown",
            driver_task,
        )
        .await;
    }));
}

/// Scenario: An edge node with two previously birthed devices publishes NDEATH.
/// Guarantees: `mqtt_ingress` emits the raw NDEATH plus one synthetic empty-payload DDEATH message per tracked device.
#[test]
fn mqtt_sparkplug_ndeath_cascades_to_synthetic_device_deaths() {
    let (runtime, local_set) = setup_test_runtime();
    runtime.block_on(local_set.run_until(async {
        let port = reserve_local_port();
        let mut bundle = build_bundle(port);
        let capabilities = resolve_capabilities(&bundle);
        let ingress = capabilities
            .require_local::<MqttIngressCap>()
            .expect("resolve local mqtt_ingress");

        let mut wrapper = bundle.take_local().expect("local mqtt_sparkplug wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let extension_task =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let connection = connect(MqttBackendConfig {
            hostname: "127.0.0.1".to_string(),
            port,
            client_id: Some("sparkplug-death-client".to_string()),
            subscribe_topic_filter: None,
            connect_timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("connect mqtt test client");
        let client = connection.client;
        let receiver = connection.receiver;
        let driver = connection.driver;
        let driver_task = tokio::task::spawn_local(async move { driver.drive().await });

        let device_birth_payload = sparkplug_payload(
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
        publish(
            &client,
            RawMqttMessage {
                topic: "spBv1.0/group/DBIRTH/edge/device-a".to_string(),
                payload: device_birth_payload.clone(),
            },
        )
        .await
        .expect("publish device-a DBIRTH");
        publish(
            &client,
            RawMqttMessage {
                topic: "spBv1.0/group/DBIRTH/edge/device-b".to_string(),
                payload: device_birth_payload,
            },
        )
        .await
        .expect("publish device-b DBIRTH");

        let mut stream = ingress
            .subscribe("spBv1.0/#")
            .await
            .expect("subscribe to mqtt ingress");
        let ndeath_payload = sparkplug_payload(Vec::new(), 1002, 2);
        publish(
            &client,
            RawMqttMessage {
                topic: "spBv1.0/group/NDEATH/edge".to_string(),
                payload: ndeath_payload.clone(),
            },
        )
        .await
        .expect("publish NDEATH");

        let first = completes_within(
            Duration::from_secs(5),
            "first death ingress item",
            stream.next(),
        )
        .await
        .expect("first death ingress stream item");
        let second = completes_within(
            Duration::from_secs(5),
            "second death ingress item",
            stream.next(),
        )
        .await
        .expect("second death ingress stream item");
        let third = completes_within(
            Duration::from_secs(5),
            "third death ingress item",
            stream.next(),
        )
        .await
        .expect("third death ingress stream item");

        let received = [first, second, third];
        let topics: HashSet<&str> = received
            .iter()
            .map(|message| message.topic.as_str())
            .collect();
        assert!(topics.contains("spBv1.0/group/NDEATH/edge"));
        assert!(topics.contains("spBv1.0/group/DDEATH/edge/device-a"));
        assert!(topics.contains("spBv1.0/group/DDEATH/edge/device-b"));

        let payloads: HashMap<String, Bytes> = received
            .into_iter()
            .map(|message| (message.topic, message.payload))
            .collect();
        assert_eq!(
            payloads
                .get("spBv1.0/group/NDEATH/edge")
                .expect("NDEATH payload"),
            &ndeath_payload
        );
        assert!(
            payloads
                .get("spBv1.0/group/DDEATH/edge/device-a")
                .expect("device-a payload")
                .is_empty()
        );
        assert!(
            payloads
                .get("spBv1.0/group/DDEATH/edge/device-b")
                .expect("device-b payload")
                .is_empty()
        );

        drop(client);
        drop(receiver);
        control
            .send(ExtensionControlMsg::Shutdown {
                deadline: Instant::now(),
                reason: "test complete".into(),
            })
            .await
            .expect("send extension shutdown");
        assert!(extension_task.await.expect("extension task join").is_ok());
        let _ = completes_within(
            Duration::from_secs(5),
            "mqtt client driver shutdown",
            driver_task,
        )
        .await;
    }));
}

/// Scenario: A client subscribes to the configured Sparkplug STATE topic after the extension has already started.
/// Guarantees: The embedded listener replays the retained ONLINE STATE payload so late subscribers see current Primary Host liveness immediately.
#[test]
fn mqtt_sparkplug_state_subscription_receives_retained_online_payload() {
    let (runtime, local_set) = setup_test_runtime();
    runtime.block_on(local_set.run_until(async {
        let port = reserve_local_port();
        let mut bundle = build_bundle(port);

        let mut wrapper = bundle.take_local().expect("local mqtt_sparkplug wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let extension_task =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let connection = connect(MqttBackendConfig {
            hostname: "127.0.0.1".to_string(),
            port,
            client_id: Some("state-subscriber-client".to_string()),
            subscribe_topic_filter: Some("spBv1.0/STATE/otap-datalogger-01".to_string()),
            connect_timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("connect mqtt test client");
        let client = connection.client;
        let mut receiver = connection.receiver;
        let driver = connection.driver;
        let driver_task = tokio::task::spawn_local(async move { driver.drive().await });

        let message = completes_within(
            Duration::from_secs(5),
            "retained state message",
            receiver.recv(),
        )
        .await
        .expect("retained state publish");
        assert_eq!(message.topic, "spBv1.0/STATE/otap-datalogger-01");
        let payload: serde_json::Value = serde_json::from_slice(message.payload.as_ref())
            .expect("Sparkplug 3 STATE JSON payload");
        assert_eq!(payload["online"], serde_json::Value::Bool(true));
        assert!(payload["timestamp"].as_u64().is_some());

        drop(client);
        drop(receiver);
        control
            .send(ExtensionControlMsg::Shutdown {
                deadline: Instant::now(),
                reason: "test complete".into(),
            })
            .await
            .expect("send extension shutdown");
        assert!(extension_task.await.expect("extension task join").is_ok());
        let _ = completes_within(
            Duration::from_secs(5),
            "mqtt client driver shutdown",
            driver_task,
        )
        .await;
    }));
}
