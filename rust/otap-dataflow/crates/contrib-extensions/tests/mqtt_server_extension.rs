// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_contrib_extensions::mqtt_server::{MQTT_SERVER_EXTENSION, MQTT_SERVER_URN};
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
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_egress::MqttEgress as _;
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_ingress::MqttIngress as _;
use otel_arrow_dfe_engine::testing::capability::resolve_bindings_for_test;
use otel_arrow_dfe_engine::testing::liveness::completes_within;
use otel_arrow_dfe_engine::testing::{setup_test_runtime, test_extension_ctx};
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
    let extension_id = "mqtt_server_test".into();
    let user_config = Arc::new(ExtensionUserConfig::new(
        MQTT_SERVER_URN.into(),
        serde_json::json!({
            "bind_host": "127.0.0.1",
            "bind_port": port,
        }),
    ));
    let (ext_ctx, _registry) = test_extension_ctx();
    (MQTT_SERVER_EXTENSION.create)(
        &ext_ctx,
        extension_id,
        user_config,
        &ExtensionConfig::new("mqtt_server_test"),
    )
    .expect("create mqtt_server bundle")
}

fn resolve_capabilities(
    bundle: &ExtensionBundle,
) -> otel_arrow_dfe_engine::capability::registry::Capabilities {
    let mut registry = CapabilityRegistry::new();
    bundle
        .register_into(MQTT_SERVER_EXTENSION.capabilities.as_ref(), &mut registry)
        .expect("register bundle capabilities");

    let bindings = HashMap::from([
        (MqttIngressCap::name().into(), "mqtt_server_test".into()),
        (MqttEgressCap::name().into(), "mqtt_server_test".into()),
    ]);
    let known_extensions = HashSet::from(["mqtt_server_test".into()]);

    resolve_bindings_for_test(&bindings, &registry, &known_extensions)
        .expect("resolve mqtt_server capability bindings")
}

/// Scenario: The public `extension:mqtt_server` factory starts a listener and a real MQTT client publishes matching telemetry into it.
/// Guarantees: The resolved `mqtt_ingress` capability yields that topic and payload through the extension's live listener.
#[test]
fn mqtt_server_ingress_observes_real_client_publish() {
    let (runtime, local_set) = setup_test_runtime();
    runtime.block_on(local_set.run_until(async {
        let port = reserve_local_port();
        let mut bundle = build_bundle(port);
        let capabilities = resolve_capabilities(&bundle);
        let ingress = capabilities
            .require_local::<MqttIngressCap>()
            .expect("resolve local mqtt_ingress");

        let mut wrapper = bundle.take_local().expect("local mqtt_server wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let extension_task =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut stream = ingress
            .subscribe("devices/#")
            .await
            .expect("subscribe to mqtt ingress");

        let connection = connect(MqttBackendConfig {
            hostname: "127.0.0.1".to_string(),
            port,
            client_id: Some("ingress-test-client".to_string()),
            subscribe_topic_filter: None,
            connect_timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("connect mqtt test client");
        let client = connection.client;
        let receiver = connection.receiver;
        let driver = connection.driver;
        let driver_task = tokio::task::spawn_local(async move { driver.drive().await });

        publish(
            &client,
            RawMqttMessage {
                topic: "devices/temperature".to_string(),
                payload: Bytes::from_static(b"42"),
            },
        )
        .await
        .expect("publish from mqtt test client");

        let message = completes_within(Duration::from_secs(5), "mqtt ingress item", stream.next())
            .await
            .expect("mqtt ingress stream item");
        assert_eq!(message.topic, "devices/temperature");
        assert_eq!(message.payload.as_ref(), b"42");

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

/// Scenario: A real MQTT client subscribes to the public `extension:mqtt_server`, then a resolved `mqtt_egress` capability publishes a matching message.
/// Guarantees: The matching subscribed client receives the outbound publication through the live listener.
#[test]
fn mqtt_server_egress_reaches_real_client_subscription() {
    let (runtime, local_set) = setup_test_runtime();
    runtime.block_on(local_set.run_until(async {
        let port = reserve_local_port();
        let mut bundle = build_bundle(port);
        let capabilities = resolve_capabilities(&bundle);
        let egress = capabilities
            .require_local::<MqttEgressCap>()
            .expect("resolve local mqtt_egress");

        let mut wrapper = bundle.take_local().expect("local mqtt_server wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let extension_task =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let connection = connect(MqttBackendConfig {
            hostname: "127.0.0.1".to_string(),
            port,
            client_id: Some("egress-test-client".to_string()),
            subscribe_topic_filter: Some("commands/#".to_string()),
            connect_timeout: Some(Duration::from_secs(5)),
        })
        .await
        .expect("connect mqtt test client");
        let client = connection.client;
        let mut receiver = connection.receiver;
        let driver = connection.driver;
        let driver_task = tokio::task::spawn_local(async move { driver.drive().await });

        egress
            .publish(otel_arrow_dfe_engine::capability::mqtt::MqttMessage::new(
                "commands/device-1".to_string(),
                Bytes::from_static(b"reboot"),
            ))
            .await
            .expect("publish via mqtt_egress");

        let message = completes_within(
            Duration::from_secs(5),
            "mqtt client receive outbound publish",
            receiver.recv(),
        )
        .await
        .expect("mqtt client receive publish");
        assert_eq!(message.topic, "commands/device-1");
        assert_eq!(message.payload.as_ref(), b"reboot");

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
