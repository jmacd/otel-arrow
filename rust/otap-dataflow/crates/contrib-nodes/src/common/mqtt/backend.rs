// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Backend-neutral MQTT client wrapper built on `ms-mqtt-client`.
//!
//! This module owns the `ms_mqtt_client` types and exposes a narrow surface
//! that `extension:mqtt_client` (in `otel-arrow-dfe-contrib-extensions`)
//! adapts onto the engine's `MqttIngress`/`MqttEgress` capabilities. Keeping
//! `ms_mqtt_client` usage confined to this module means the extension crate
//! never has to know about completion tokens, packet identifiers, or the
//! plaintext-vs-TLS transport split.
//!
//! Scope, matching the current milestone (see
//! `rfcs/0003-mqtt-service-capabilities.md`): plaintext TCP transport only, a
//! single fixed subscription (no per-consumer topic-filter fan-out yet -- one
//! filter is registered at connect time and every subscriber receives every
//! matching message), and QoS 1 publish/subscribe (the crate does not yet
//! support QoS 2 end to end). Multiple ingress consumers with independent
//! topic filters, and a richer ack-boundary contract, are follow-on work.

use bytes::Bytes;
use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectResult, Connection, DisconnectedEvent, KeepAliveConfig,
    Receiver as MqttClientReceiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, PublishProperties, QoS, RetainOptions, SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use ms_mqtt_client::transport::{ConnectionTransportConfig, ConnectionTransportType};
use std::fmt;
use std::time::Duration;

/// Configuration needed to establish a plaintext MQTT connection.
#[derive(Debug, Clone)]
pub struct MqttBackendConfig {
    /// Broker hostname or IP address.
    pub hostname: String,
    /// Broker TCP port (1883 is the conventional plaintext MQTT port).
    pub port: u16,
    /// Client identifier. If `None`, the broker assigns one.
    pub client_id: Option<String>,
    /// Topic filter this connection subscribes to on connect. `None` means
    /// the connection is egress-only (no SUBSCRIBE is sent).
    pub subscribe_topic_filter: Option<String>,
    /// Connect timeout. `None` uses the underlying transport's default.
    pub connect_timeout: Option<Duration>,
}

/// An error establishing or operating an MQTT connection through this
/// backend.
#[derive(Debug)]
pub enum MqttBackendError {
    /// The configured topic name or filter failed MQTT syntax validation.
    InvalidTopic(String),
    /// The CONNECT handshake failed.
    ConnectFailed(String),
    /// The initial SUBSCRIBE (when a `subscribe_topic_filter` is configured)
    /// failed or was rejected by the broker.
    SubscribeFailed(String),
    /// A publish request was rejected by the client or the broker.
    PublishFailed(String),
}

impl fmt::Display for MqttBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttBackendError::InvalidTopic(msg) => write!(f, "invalid MQTT topic: {msg}"),
            MqttBackendError::ConnectFailed(msg) => write!(f, "MQTT connect failed: {msg}"),
            MqttBackendError::SubscribeFailed(msg) => write!(f, "MQTT subscribe failed: {msg}"),
            MqttBackendError::PublishFailed(msg) => write!(f, "MQTT publish failed: {msg}"),
        }
    }
}

impl std::error::Error for MqttBackendError {}

/// A minimal, envelope-free MQTT message: topic and payload bytes only.
///
/// Mirrors `otel_arrow_dfe_engine::capability::mqtt::MqttMessage`; this
/// module intentionally does not depend on the engine crate, so the two
/// types are converted at the extension boundary rather than shared.
#[derive(Debug, Clone)]
pub struct RawMqttMessage {
    /// The topic the message was published on (ingress) or should be
    /// published to (egress).
    pub topic: String,
    /// The message payload, with no assumed encoding.
    pub payload: Bytes,
}

/// Wraps the raw `ms_mqtt_client::client::Receiver`, translating each inbound
/// PUBLISH into a [`RawMqttMessage`] and auto-acknowledging it.
pub struct MqttMessageReceiver(MqttClientReceiver);

impl MqttMessageReceiver {
    /// Receives the next inbound PUBLISH, if any, auto-acknowledging it with
    /// default properties (this milestone does not expose manual
    /// accept/reject to callers).
    pub async fn recv(&mut self) -> Option<RawMqttMessage> {
        let (publish, manual_ack) = self.0.recv().await?;
        // Dropping `manual_ack` performs auto-acknowledgement (QoS 0: no-op;
        // QoS 1: default PUBACK). QoS 2 is not reachable because subscriptions
        // request a maximum of QoS 1.
        drop(manual_ack);
        Some(RawMqttMessage {
            topic: publish.topic_name.as_str().to_owned(),
            payload: publish.payload,
        })
    }
}

/// The MQTT protocol driver for a live connection.
///
/// This is a thin re-export of `ms_mqtt_client::client::Connection`: callers
/// must continuously poll [`MqttProtocolDriver::drive`] (typically in a
/// `tokio::select!` alongside inbound-receive and shutdown signals) for the
/// connection to make any progress at all.
pub struct MqttProtocolDriver(Connection);

/// Outcome of [`MqttProtocolDriver::drive`] running to completion.
#[derive(Debug)]
pub struct MqttConnectionEnded {
    /// The reason the connection ended.
    pub event: DisconnectedEvent,
}

impl MqttProtocolDriver {
    /// Drives MQTT I/O for this connection until it ends (error, broker
    /// disconnect, or an application-requested disconnect via a previously
    /// obtained disconnect handle).
    pub async fn drive(self) -> MqttConnectionEnded {
        let (_connect_handle, event) = self.0.run_until_disconnect().await;
        MqttConnectionEnded { event }
    }
}

/// The three independently-ownable pieces of a live MQTT connection, split so
/// a caller can drive all of them concurrently (typically via
/// `tokio::select!`, following the pattern used by
/// `ms_mqtt_client`'s own `examples/scenario_1_simple.rs`):
///
/// - `client`: outbound handle for [`publish`]; cheaply `Clone`.
/// - `receiver`: inbound PUBLISH stream, via [`MqttMessageReceiver::recv`].
/// - `driver`: the protocol/I-O loop, via [`MqttProtocolDriver::drive`].
pub struct MqttConnectionParts {
    /// Outbound handle for publishing. Cheaply `Clone`.
    pub client: Client,
    /// Inbound PUBLISH stream.
    pub receiver: MqttMessageReceiver,
    /// The protocol/I-O driver; must be polled continuously via
    /// [`MqttProtocolDriver::drive`].
    pub driver: MqttProtocolDriver,
}

/// Connects to `config.hostname:config.port` over plaintext TCP, and, if
/// `config.subscribe_topic_filter` is set, subscribes to it (QoS 1) before
/// returning.
pub async fn connect(config: MqttBackendConfig) -> Result<MqttConnectionParts, MqttBackendError> {
    let options = ClientOptions {
        client_id: config.client_id.clone(),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    // Clean start: this milestone does not yet implement session
    // persistence/resumption, so each connection starts fresh.
    let (connection, _connack, disconnect_handle) = match connect_handle
        .connect(
            ConnectionTransportConfig {
                transport_type: ConnectionTransportType::Tcp {
                    hostname: config.hostname.clone(),
                    port: config.port,
                },
                timeout: config.connect_timeout,
                proxy: None,
                tcp_nodelay: false,
            },
            true,
            KeepAliveConfig::Infinite,
            None,
            None,
            None,
            ConnectProperties::default(),
            None,
        )
        .await
    {
        ConnectResult::Success(connection, connack, disconnect_handle) => {
            (connection, connack, disconnect_handle)
        }
        ConnectResult::Failure(_handle, error) => {
            return Err(MqttBackendError::ConnectFailed(error.to_string()));
        }
    };
    // The disconnect handle is only needed to request an application-driven
    // disconnect; this milestone always disconnects by dropping the
    // connection/client, so the handle is not retained.
    drop(disconnect_handle);

    if let Some(filter) = &config.subscribe_topic_filter {
        let topic_filter =
            TopicFilter::new(filter).map_err(|e| MqttBackendError::InvalidTopic(e.to_string()))?;
        let ct = client
            .subscribe(
                topic_filter,
                QoS::AtLeastOnce,
                false,
                RetainOptions::default(),
                SubscribeProperties::default(),
            )
            .await
            .map_err(|e| MqttBackendError::SubscribeFailed(e.to_string()))?;
        let suback = ct
            .await
            .map_err(|e| MqttBackendError::SubscribeFailed(e.to_string()))?;
        suback
            .as_result()
            .map_err(|e| MqttBackendError::SubscribeFailed(e.to_string()))?;
    }

    Ok(MqttConnectionParts {
        client,
        receiver: MqttMessageReceiver(receiver),
        driver: MqttProtocolDriver(connection),
    })
}

/// Publishes `message` at QoS 1 and waits for the broker's PUBACK.
///
/// A standalone function (rather than a method on [`MqttConnection`]) because
/// publishing only needs a cloned [`Client`] handle, which callers typically
/// hold independently of the [`MqttConnection`] they are concurrently
/// driving.
pub async fn publish(client: &Client, message: RawMqttMessage) -> Result<(), MqttBackendError> {
    let topic_name = TopicName::new(&message.topic)
        .map_err(|e| MqttBackendError::InvalidTopic(e.to_string()))?;
    let ct = client
        .publish_qos1(
            topic_name,
            message.payload,
            false,
            PublishProperties::default(),
        )
        .await
        .map_err(|e| MqttBackendError::PublishFailed(e.to_string()))?;
    let puback = ct
        .await
        .map_err(|e| MqttBackendError::PublishFailed(e.to_string()))?;
    puback
        .as_result()
        .map_err(|e| MqttBackendError::PublishFailed(e.to_string()))?;
    Ok(())
}

/// Returns whether `topic` matches `topic_filter` under MQTT topic-filter
/// matching rules.
///
/// Used to narrow the extension's single broker-level subscription (set via
/// [`MqttBackendConfig::subscribe_topic_filter`]) down to what an individual
/// `MqttIngress::subscribe` caller asked for. This is client-side filtering
/// only: it never causes an additional SUBSCRIBE to be sent to the broker.
pub fn topic_matches(topic_filter: &str, topic: &str) -> Result<bool, MqttBackendError> {
    let filter = TopicFilter::new(topic_filter)
        .map_err(|e| MqttBackendError::InvalidTopic(e.to_string()))?;
    let name = TopicName::new(topic).map_err(|e| MqttBackendError::InvalidTopic(e.to_string()))?;
    Ok(filter.matches_topic_name(&name))
}
