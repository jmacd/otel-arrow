// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use ntex_bytes::Bytes as NtexBytes;
use ntex_mqtt::MqttServer as ProtocolServer;
use ntex_mqtt::MqttServiceConfig;
use ntex_mqtt::TopicFilter;
use ntex_mqtt::v5;
use ntex_net::Reactor as _;
use ntex_service::Pipeline;
use ntex_service::cfg::SharedCfg;
use ntex_service::{fn_factory_with_config, fn_service_st};
use otel_arrow_dfe_config::ExtensionId;
use otel_arrow_dfe_engine::capability::mqtt::MqttMessage;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_egress::MqttEgress as MqttEgressCap;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_ingress::{
    MqttIngress as MqttIngressCap, MqttMessageStream,
};
use otel_arrow_dfe_engine::capability::{CapabilityError, CapabilityErrorSource};
use otel_arrow_dfe_engine::control::ExtensionControlMsg;
use otel_arrow_dfe_engine::error::Error as EngineError;
use otel_arrow_dfe_engine::extension::EffectHandler;
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_egress::MqttEgress as LocalMqttEgress;
use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_ingress::MqttIngress as LocalMqttIngress;
use otel_arrow_dfe_engine::local::extension::{ControlChannel, Extension as LocalExtension};
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use super::config::Config;

/// Bounded fan-out buffer for inbound device publications delivered to
/// `MqttIngress` subscribers on the local core.
const INGRESS_BROADCAST_CAPACITY: usize = 1024;

#[derive(Debug, thiserror::Error)]
enum ServerPublishError {
    #[error("invalid MQTT topic name: {0}")]
    InvalidTopic(String),
    #[error("failed to read MQTT payload: {0}")]
    Payload(String),
    #[error("failed to forward MQTT publish to a connected client: {0}")]
    Forward(String),
}

#[derive(Debug, Clone)]
struct IngressMessage {
    topic: String,
    payload: Bytes,
}

impl TryFrom<ServerPublishError> for v5::PublishAck {
    type Error = ServerPublishError;

    fn try_from(value: ServerPublishError) -> Result<Self, Self::Error> {
        let reason = match value {
            ServerPublishError::InvalidTopic(_) => v5::codec::PublishAckReason::TopicNameInvalid,
            ServerPublishError::Payload(_) => v5::codec::PublishAckReason::PayloadFormatInvalid,
            ServerPublishError::Forward(_) => {
                v5::codec::PublishAckReason::ImplementationSpecificError
            }
        };
        Ok(v5::PublishAck::new(reason))
    }
}

/// Shared state for one connected MQTT client.
struct ConnectedClient {
    client_id: String,
    subscriptions: RefCell<Vec<String>>,
    sink: v5::MqttSink,
}

/// Per-connection session state stored inside `ntex-mqtt`.
#[derive(Clone)]
struct SessionState {
    connection_id: u64,
    client: Rc<ConnectedClient>,
}

/// Shared local extension state.
struct Inner {
    config: Config,
    ingress_cap_err: CapabilityErrorSource<MqttIngressCap>,
    egress_cap_err: CapabilityErrorSource<MqttEgressCap>,
    ingress_tx: broadcast::Sender<IngressMessage>,
    listener_addr: RefCell<Option<SocketAddr>>,
    next_connection_id: Cell<u64>,
    connections: RefCell<HashMap<u64, Rc<ConnectedClient>>>,
}

/// Minimal embedded MQTT server extension backed by `ntex-mqtt`.
#[derive(Clone)]
pub(crate) struct MqttServerExtension {
    inner: Rc<Inner>,
}

impl MqttServerExtension {
    pub(crate) fn new(extension_id: ExtensionId, config: Config) -> Self {
        let (ingress_tx, _rx) = broadcast::channel(INGRESS_BROADCAST_CAPACITY);
        Self {
            inner: Rc::new(Inner {
                config,
                ingress_cap_err: CapabilityErrorSource::new(extension_id.clone()),
                egress_cap_err: CapabilityErrorSource::new(extension_id),
                ingress_tx,
                listener_addr: RefCell::new(None),
                next_connection_id: Cell::new(1),
                connections: RefCell::new(HashMap::new()),
            }),
        }
    }

    fn next_connection_id(&self) -> u64 {
        let connection_id = self.inner.next_connection_id.get();
        self.inner
            .next_connection_id
            .set(connection_id.saturating_add(1));
        connection_id
    }

    fn mqtt_service_config(&self) -> SharedCfg {
        SharedCfg::new("mqtt_server")
            .service("mqtt_server")
            .add(MqttServiceConfig::new().set_max_qos(v5::QoS::AtLeastOnce))
            .build()
    }

    fn register_client(&self, client_id: String, sink: v5::MqttSink) -> SessionState {
        let connection_id = self.next_connection_id();
        let client = Rc::new(ConnectedClient {
            client_id,
            subscriptions: RefCell::new(Vec::new()),
            sink,
        });
        _ = self
            .inner
            .connections
            .borrow_mut()
            .insert(connection_id, client.clone());
        SessionState {
            connection_id,
            client,
        }
    }

    fn remove_client(&self, connection_id: u64) {
        if let Some(client) = self.inner.connections.borrow_mut().remove(&connection_id) {
            otel_debug!(
                "mqtt_server.client_removed",
                client_id = client.client_id.as_str(),
                connection_id = connection_id
            );
        }
    }

    fn close_all_clients(&self) {
        for client in self.inner.connections.borrow().values() {
            client.sink.close();
        }
    }

    fn validate_topic_name(&self, topic: &str) -> Result<(), ServerPublishError> {
        if topic.is_empty() || topic.contains(['+', '#']) {
            Err(ServerPublishError::InvalidTopic(topic.to_string()))
        } else {
            Ok(())
        }
    }

    fn topic_matches(&self, topic_filter: &str, topic: &str) -> Result<bool, ServerPublishError> {
        self.validate_topic_name(topic)?;
        let filter: TopicFilter = topic_filter.parse().map_err(|error| {
            ServerPublishError::InvalidTopic(format!("{topic_filter}: {error:?}"))
        })?;
        Ok(filter.matches_topic(topic))
    }

    fn prune_closed_clients(&self) {
        self.inner
            .connections
            .borrow_mut()
            .retain(|_, client| client.sink.is_open());
    }

    fn matching_client_sinks(&self, topic: &str) -> Vec<v5::MqttSink> {
        self.prune_closed_clients();
        let connections = self.inner.connections.borrow();
        connections
            .values()
            .filter_map(|client| {
                let is_match =
                    client.subscriptions.borrow().iter().any(|topic_filter| {
                        match self.topic_matches(topic_filter, topic) {
                            Ok(result) => result,
                            Err(error) => {
                                otel_warn!(
                                    "mqtt_server.subscription_match_failed",
                                    client_id = client.client_id.as_str(),
                                    error = %error
                                );
                                false
                            }
                        }
                    });

                if is_match {
                    Some(client.sink.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    async fn relay_to_matching_clients(
        &self,
        topic: &str,
        payload: Bytes,
    ) -> Result<(), ServerPublishError> {
        let sinks = self.matching_client_sinks(topic);
        for sink in sinks {
            _ = sink
                .publish(topic.to_string())
                .send_at_least_once(NtexBytes::from(payload.to_vec()))
                .await
                .map_err(|error| ServerPublishError::Forward(error.to_string()))?;
        }
        Ok(())
    }

    async fn handle_handshake(
        &self,
        handshake: v5::Handshake,
    ) -> Result<v5::HandshakeAck<SessionState>, ServerPublishError> {
        let client_id = handshake.packet().client_id.to_string();
        let session = self.register_client(client_id.clone(), handshake.sink());
        otel_debug!(
            "mqtt_server.client_connected",
            client_id = client_id.as_str(),
            connection_id = session.connection_id
        );
        Ok(handshake.ack(session))
    }

    async fn handle_inbound_publish(
        &self,
        session: &v5::Session<SessionState>,
        publish: v5::Publish,
    ) -> Result<v5::PublishAck, ServerPublishError> {
        let topic = publish.publish_topic().to_string();
        let payload = publish
            .read_all()
            .await
            .map_err(|error| ServerPublishError::Payload(error.to_string()))?;
        let payload = Bytes::copy_from_slice(payload.as_ref());

        let _ = self.inner.ingress_tx.send(IngressMessage {
            topic: topic.clone(),
            payload: payload.clone(),
        });

        self.relay_to_matching_clients(&topic, payload).await?;

        otel_debug!(
            "mqtt_server.publish_received",
            client_id = session.client.client_id.as_str(),
            connection_id = session.connection_id,
            topic = topic.as_str()
        );

        Ok(publish.ack())
    }

    async fn handle_protocol_message(
        &self,
        session: &v5::Session<SessionState>,
        message: v5::ProtocolMessage,
    ) -> Result<v5::ProtocolMessageAck, ServerPublishError> {
        match message {
            v5::ProtocolMessage::Auth(auth) => Ok(auth.ack(v5::codec::Auth::default())),
            v5::ProtocolMessage::Disconnect(disconnect) => {
                self.remove_client(session.connection_id);
                Ok(disconnect.ack())
            }
            v5::ProtocolMessage::Ping(ping) => Ok(ping.ack()),
            v5::ProtocolMessage::Subscribe(mut subscribe) => {
                let mut subscriptions = session.client.subscriptions.borrow_mut();
                for mut entry in subscribe.iter_mut() {
                    let topic_filter = entry.topic().to_string();
                    if !subscriptions
                        .iter()
                        .any(|existing| existing == &topic_filter)
                    {
                        subscriptions.push(topic_filter);
                    }
                    entry.confirm(v5::QoS::AtLeastOnce);
                }
                Ok(subscribe.ack())
            }
            v5::ProtocolMessage::Unsubscribe(unsubscribe) => {
                let removals: Vec<String> =
                    unsubscribe.iter().map(|topic| topic.to_string()).collect();
                session
                    .client
                    .subscriptions
                    .borrow_mut()
                    .retain(|topic_filter| !removals.iter().any(|removal| removal == topic_filter));
                Ok(unsubscribe.ack())
            }
            other => Ok(other.ack()),
        }
    }

    #[cfg(test)]
    pub(crate) fn bound_addr(&self) -> Option<SocketAddr> {
        *self.inner.listener_addr.borrow()
    }
}

#[async_trait(?Send)]
impl LocalMqttIngress for MqttServerExtension {
    async fn subscribe(&self, topic_filter: &str) -> Result<MqttMessageStream, CapabilityError> {
        let topic_filter = topic_filter.to_owned();
        let rx = self.inner.ingress_tx.subscribe();
        let cap_err = self.inner.ingress_cap_err.clone();
        let extension = self.clone();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let topic_filter = topic_filter.clone();
            let cap_err = cap_err.clone();
            let extension = extension.clone();
            async move {
                match item {
                    Ok(message) => match extension.topic_matches(&topic_filter, &message.topic) {
                        Ok(true) => Some(MqttMessage::new(message.topic, message.payload)),
                        Ok(false) => None,
                        Err(error) => {
                            otel_warn!(
                                "mqtt_server.ingress_topic_match_failed",
                                error = %cap_err.error(error.to_string())
                            );
                            None
                        }
                    },
                    Err(BroadcastStreamRecvError::Lagged(count)) => {
                        otel_warn!("mqtt_server.ingress_subscriber_lagged", count = count);
                        None
                    }
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[async_trait(?Send)]
impl LocalMqttEgress for MqttServerExtension {
    async fn publish(&self, message: MqttMessage) -> Result<(), CapabilityError> {
        self.validate_topic_name(&message.topic)
            .map_err(|error| self.inner.egress_cap_err.error(error.to_string()))?;

        // RFC 0003 ("Sparkplug as a pluggable byte representation") defines
        // origin-tagged loop prevention for `local_pipeline` and `local_service`
        // publications. This scaffold does not track that provenance yet; it
        // only fans local publishes out to matching connected MQTT clients and
        // does not route them back into local `mqtt_ingress`.
        self.relay_to_matching_clients(&message.topic, message.payload)
            .await
            .map_err(|error| self.inner.egress_cap_err.error(error.to_string()))
    }
}

#[async_trait(?Send)]
impl LocalExtension for MqttServerExtension {
    async fn start(
        self: Rc<Self>,
        mut ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        let listener = TcpListener::bind((
            self.inner.config.bind_host.as_str(),
            self.inner.config.bind_port,
        ))
        .await
        .map_err(|error| EngineError::InternalError {
            message: format!("MQTT listener bind failed: {error}"),
        })?;
        let listener_addr = listener
            .local_addr()
            .map_err(|error| EngineError::InternalError {
                message: format!("MQTT listener local_addr failed: {error}"),
            })?;
        *self.inner.listener_addr.borrow_mut() = Some(listener_addr);
        effect_handler.signal_ready();
        otel_debug!("mqtt_server.listener_bound", address = %listener_addr);

        let handshake_extension = self.clone();
        let publish_extension = self.clone();
        let protocol_extension = self.clone();
        let publish_factory = fn_factory_with_config(async move |_| {
            let publish_extension = publish_extension.clone();
            Ok::<_, ServerPublishError>(fn_service_st(async move |session, publish| {
                publish_extension
                    .handle_inbound_publish(session, publish)
                    .await
            }))
        });
        let protocol_factory = fn_factory_with_config(async move |_| {
            let protocol_extension = protocol_extension.clone();
            Ok::<_, ServerPublishError>(fn_service_st(async move |session, message| {
                protocol_extension
                    .handle_protocol_message(session, message)
                    .await
            }))
        });
        let server = ProtocolServer::new().v5(v5::MqttServer::new(publish_factory)
            .protocol(protocol_factory)
            .build(move |handshake| {
                let handshake_extension = handshake_extension.clone();
                async move { handshake_extension.handle_handshake(handshake).await }
            }));
        let pipeline = Pipeline::new(server);
        let pipeline_binding = pipeline.bind();
        let io_cfg = self.mqtt_service_config();
        let reactor = ntex_net::tokio::Reactor;

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, peer_addr) = match accepted {
                        Ok(parts) => parts,
                        Err(error) => {
                            otel_warn!("mqtt_server.accept_failed", error = %error);
                            continue;
                        }
                    };

                    let std_stream = stream.into_std().map_err(|error| EngineError::InternalError {
                        message: format!("MQTT TcpStream conversion failed: {error}"),
                    })?;
                    let io = reactor.from_tcp_stream(std_stream, io_cfg.clone()).map_err(|error| {
                        EngineError::InternalError {
                            message: format!("MQTT ntex Io creation failed: {error}"),
                        }
                    })?;

                    let pipeline_binding = pipeline_binding.clone();
                    let extension = self.clone();
                    _ = tokio::task::spawn_local(async move {
                        let result = pipeline_binding.call_static(io).await;
                        match result {
                            Ok(()) => {
                                otel_debug!("mqtt_server.connection_closed", peer = %peer_addr);
                            }
                            Err(error) => {
                                otel_warn!(
                                    "mqtt_server.connection_ended_with_error",
                                    peer = %peer_addr,
                                    error = ?error
                                );
                            }
                        }
                        extension.prune_closed_clients();
                    });
                }
                ctrl_msg = ctrl_chan.recv() => {
                    match ctrl_msg {
                        Ok(ExtensionControlMsg::Shutdown { .. }) => {
                            self.close_all_clients();
                            *self.inner.listener_addr.borrow_mut() = None;
                            return Ok(TerminalState::default());
                        }
                        Ok(ExtensionControlMsg::Config { .. }) => {
                            // Live reconfiguration is not implemented in this milestone.
                        }
                        Ok(ExtensionControlMsg::CollectTelemetry { .. }) => {
                            // No metric set is registered yet for this extension.
                        }
                        Err(_) => {
                            self.close_all_clients();
                            *self.inner.listener_addr.borrow_mut() = None;
                            return Ok(TerminalState::default());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;
    use otel_arrow_dfe_config::extension::ExtensionUserConfig;
    use otel_arrow_dfe_contrib_nodes::common::mqtt::backend::{
        MqttBackendConfig, RawMqttMessage, connect, publish,
    };
    use otel_arrow_dfe_engine::config::ExtensionConfig;
    use otel_arrow_dfe_engine::control::ExtensionControlMsg;
    use otel_arrow_dfe_engine::extension::ExtensionWrapper;
    use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_egress::MqttEgress as _;
    use otel_arrow_dfe_engine::local::capability::mqtt::mqtt_ingress::MqttIngress as _;
    use otel_arrow_dfe_engine::testing::liveness::completes_within;
    use otel_arrow_dfe_engine::testing::setup_test_runtime;
    use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    fn test_metrics_reporter() -> MetricsReporter {
        let (tx, _rx) = flume::bounded(1);
        MetricsReporter::new(tx)
    }

    async fn start_test_extension(
        config: Config,
    ) -> (
        MqttServerExtension,
        otel_arrow_dfe_engine::message::Sender<ExtensionControlMsg>,
        tokio::task::JoinHandle<Result<TerminalState, EngineError>>,
    ) {
        let extension_id: ExtensionId = "mqtt_server_test".into();
        let extension = MqttServerExtension::new(extension_id.clone(), config);
        let user_config = Arc::new(ExtensionUserConfig::new(
            crate::mqtt_server::MQTT_SERVER_URN.into(),
            serde_json::Value::Null,
        ));
        let mut bundle = ExtensionWrapper::builder(
            extension_id.clone(),
            user_config,
            &ExtensionConfig::new(extension_id),
        )
        .active()
        .local::<MqttServerExtension>(Rc::new(extension.clone()))
        .build()
        .expect("build local mqtt_server extension bundle");
        let wrapper = bundle.take_local().expect("local wrapper");
        let control = wrapper
            .extension_control_sender()
            .expect("extension control sender");
        let handle =
            tokio::task::spawn_local(async move { wrapper.start(test_metrics_reporter()).await });

        completes_within(Duration::from_secs(5), "mqtt_server listener bind", async {
            loop {
                if extension.bound_addr().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;

        (extension, control, handle)
    }

    /// Scenario: A real MQTT client connects to the embedded server and publishes on a topic observed by a local ingress subscriber.
    /// Guarantees: `MqttIngress::subscribe` yields the published topic and payload received through the live listener.
    #[test]
    fn ingress_subscriber_observes_client_publish() {
        let (rt, local_set) = setup_test_runtime();
        rt.block_on(local_set.run_until(async {
            let (extension, control, handle) = start_test_extension(Config {
                bind_host: "127.0.0.1".to_string(),
                bind_port: 0,
            })
            .await;

            let listener_addr = extension.bound_addr().expect("listener bound address");
            let mut stream = extension
                .subscribe("devices/#")
                .await
                .expect("subscribe to mqtt ingress");

            let connection = connect(MqttBackendConfig {
                hostname: "127.0.0.1".to_string(),
                port: listener_addr.port(),
                client_id: Some("ingress-test-client".to_string()),
                subscribe_topic_filter: None,
                connect_timeout: Some(Duration::from_secs(5)),
            })
            .await
            .expect("connect mqtt test client");
            let client = connection.client;
            let receiver = connection.receiver;
            let driver = connection.driver;
            let driver_handle = tokio::task::spawn_local(async move { driver.drive().await });

            publish(
                &client,
                RawMqttMessage {
                    topic: "devices/temperature".to_string(),
                    payload: Bytes::from_static(b"42"),
                },
            )
            .await
            .expect("publish from mqtt test client");

            let message =
                completes_within(Duration::from_secs(5), "mqtt ingress item", stream.next())
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
                .expect("send shutdown");
            assert!(handle.await.expect("extension task join").is_ok());
            let _ = completes_within(
                Duration::from_secs(5),
                "mqtt client driver shutdown",
                driver_handle,
            )
            .await;
        }));
    }

    /// Scenario: A real MQTT client subscribes through the embedded server and the local egress capability publishes a matching message.
    /// Guarantees: `MqttEgress::publish` forwards the message to connected clients whose subscribed topic filters match.
    #[test]
    fn egress_publish_reaches_matching_client_subscription() {
        let (rt, local_set) = setup_test_runtime();
        rt.block_on(local_set.run_until(async {
            let (extension, control, handle) = start_test_extension(Config {
                bind_host: "127.0.0.1".to_string(),
                bind_port: 0,
            })
            .await;

            let listener_addr = extension.bound_addr().expect("listener bound address");
            let connection = connect(MqttBackendConfig {
                hostname: "127.0.0.1".to_string(),
                port: listener_addr.port(),
                client_id: Some("egress-test-client".to_string()),
                subscribe_topic_filter: Some("commands/#".to_string()),
                connect_timeout: Some(Duration::from_secs(5)),
            })
            .await
            .expect("connect mqtt test client");
            let client = connection.client;
            let mut receiver = connection.receiver;
            let driver = connection.driver;
            let driver_handle = tokio::task::spawn_local(async move { driver.drive().await });

            extension
                .publish(MqttMessage::new(
                    "commands/device-1".to_string(),
                    Bytes::from_static(b"reboot"),
                ))
                .await
                .expect("publish via mqtt egress capability");

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
                .expect("send shutdown");
            assert!(handle.await.expect("extension task join").is_ok());
            let _ = completes_within(
                Duration::from_secs(5),
                "mqtt client driver shutdown",
                driver_handle,
            )
            .await;
        }));
    }
}
