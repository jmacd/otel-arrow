// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

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
use otel_arrow_dfe_sparkplug::pb;
use otel_arrow_dfe_sparkplug::{
    CascadedDeviceDeath, DeathOrigin, SparkplugDecodeContext, SparkplugMessageType,
    SparkplugPayload, SparkplugState, Timestamp, Topic,
};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use super::config::{Config, StateProfile};

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

#[derive(Debug, Clone)]
struct IngressMessage {
    topic: String,
    payload: Bytes,
    _decode_context: Option<SparkplugDecodeContext>,
}

enum InboundPublishAction {
    Forward {
        ingress: IngressMessage,
        cascaded_ingress: Vec<IngressMessage>,
    },
    AckOnly(v5::PublishAck),
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
    retained: RefCell<HashMap<String, Bytes>>,
    sparkplug_state: RefCell<SparkplugState>,
}

/// Minimal embedded MQTT Sparkplug extension backed by `ntex-mqtt`.
#[derive(Clone)]
pub(crate) struct MqttSparkplugExtension {
    inner: Rc<Inner>,
}

impl MqttSparkplugExtension {
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
                retained: RefCell::new(HashMap::new()),
                sparkplug_state: RefCell::new(SparkplugState::new()),
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
        SharedCfg::new("mqtt_sparkplug")
            .service("mqtt_sparkplug")
            .add(MqttServiceConfig::new().set_max_qos(v5::QoS::AtLeastOnce))
            .build()
    }

    fn state_topic(&self) -> String {
        Topic::State {
            profile: self.inner.config.state_profile.into(),
            host_id: self.inner.config.host_id.clone(),
        }
        .to_string()
    }

    fn state_payload(&self, online: bool, observed_at: Timestamp) -> Bytes {
        match self.inner.config.state_profile {
            StateProfile::Sparkplug22 => {
                if online {
                    Bytes::from_static(b"ONLINE")
                } else {
                    Bytes::from_static(b"OFFLINE")
                }
            }
            StateProfile::Sparkplug30 => Bytes::from(
                serde_json::json!({
                    "online": online,
                    "timestamp": observed_at,
                })
                .to_string(),
            ),
        }
    }

    fn seed_online_state_retained(&self) {
        let observed_at = current_timestamp_millis();
        let state_topic = self.state_topic();
        let payload = self.state_payload(true, observed_at);
        _ = self
            .inner
            .retained
            .borrow_mut()
            .insert(state_topic, payload);
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
                "mqtt_sparkplug.client_removed",
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
                                    "mqtt_sparkplug.subscription_match_failed",
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

    fn all_client_sinks(&self) -> Vec<v5::MqttSink> {
        self.prune_closed_clients();
        self.inner
            .connections
            .borrow()
            .values()
            .map(|client| client.sink.clone())
            .collect()
    }

    async fn publish_to_sinks(
        &self,
        sinks: Vec<v5::MqttSink>,
        topic: &str,
        payload: &Bytes,
        retain: bool,
    ) -> Result<(), ServerPublishError> {
        for sink in sinks {
            _ = sink
                .publish(topic.to_string())
                .retain(retain)
                .send_at_least_once(NtexBytes::from(payload.to_vec()))
                .await
                .map_err(|error| ServerPublishError::Forward(error.to_string()))?;
        }
        Ok(())
    }

    async fn relay_to_matching_clients(
        &self,
        topic: &str,
        payload: &Bytes,
    ) -> Result<(), ServerPublishError> {
        let sinks = self.matching_client_sinks(topic);
        self.publish_to_sinks(sinks, topic, payload, false).await
    }

    async fn deliver_to_all_clients(
        &self,
        topic: &str,
        payload: &Bytes,
        retain: bool,
    ) -> Result<(), ServerPublishError> {
        let sinks = self.all_client_sinks();
        self.publish_to_sinks(sinks, topic, payload, retain).await
    }

    fn retained_messages_for_filters(
        &self,
        topic_filters: &[String],
    ) -> Result<Vec<(String, Bytes)>, ServerPublishError> {
        let retained = self.inner.retained.borrow();
        let mut seen_topics = HashSet::new();
        let mut matched = Vec::new();

        for topic_filter in topic_filters {
            for (topic, payload) in &*retained {
                if self.topic_matches(topic_filter, topic)? && seen_topics.insert(topic.clone()) {
                    matched.push((topic.clone(), payload.clone()));
                }
            }
        }

        Ok(matched)
    }

    fn send_retained_matches_to_client(
        &self,
        client: Rc<ConnectedClient>,
        topic_filters: Vec<String>,
    ) {
        let retained_messages = match self.retained_messages_for_filters(&topic_filters) {
            Ok(messages) => messages,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.retained_match_failed",
                    client_id = client.client_id.as_str(),
                    error = %error
                );
                return;
            }
        };

        if retained_messages.is_empty() {
            return;
        }

        let extension = self.clone();
        _ = tokio::task::spawn_local(async move {
            for (topic, payload) in retained_messages {
                if let Err(error) = extension
                    .publish_to_sinks(vec![client.sink.clone()], &topic, &payload, true)
                    .await
                {
                    otel_warn!(
                        "mqtt_sparkplug.retained_delivery_failed",
                        client_id = client.client_id.as_str(),
                        topic = topic.as_str(),
                        error = %error
                    );
                    break;
                }
            }
        });
    }

    async fn publish_graceful_offline_state(&self) {
        let observed_at = current_timestamp_millis();
        let state_topic = self.state_topic();
        let payload = self.state_payload(false, observed_at);
        _ = self
            .inner
            .retained
            .borrow_mut()
            .insert(state_topic.clone(), payload.clone());

        // RFC 0003 notes that the standalone server path does not yet have a
        // true MQTT-level Will for the server's own Primary Host identity. As a
        // scoped substitute, graceful shutdown pushes the retained OFFLINE
        // state to every connected client before the listener closes.
        if let Err(error) = self
            .deliver_to_all_clients(&state_topic, &payload, true)
            .await
        {
            otel_warn!(
                "mqtt_sparkplug.graceful_offline_publish_failed",
                topic = state_topic.as_str(),
                error = %error
            );
        }
    }

    async fn handle_handshake(
        &self,
        handshake: v5::Handshake,
    ) -> Result<v5::HandshakeAck<SessionState>, ServerPublishError> {
        let client_id = handshake.packet().client_id.to_string();
        let session = self.register_client(client_id.clone(), handshake.sink());
        otel_debug!(
            "mqtt_sparkplug.client_connected",
            client_id = client_id.as_str(),
            connection_id = session.connection_id
        );
        Ok(handshake.ack(session))
    }

    fn process_inbound_publish(
        &self,
        topic: &str,
        payload: &Bytes,
        observed_at: Timestamp,
    ) -> InboundPublishAction {
        let parsed_topic = match Topic::parse(topic) {
            Ok(parsed) => parsed,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.non_sparkplug_publish_ignored",
                    topic = topic,
                    error = %error
                );
                return InboundPublishAction::AckOnly(v5::PublishAck::new(
                    v5::codec::PublishAckReason::Success,
                ));
            }
        };

        let lifecycle_topic = match parsed_topic.as_lifecycle_topic() {
            Ok(lifecycle_topic) => lifecycle_topic,
            Err(message_type) => {
                otel_warn!(
                    "mqtt_sparkplug.unexpected_message_type_ignored",
                    topic = topic,
                    message_type = %message_type
                );
                return InboundPublishAction::AckOnly(v5::PublishAck::new(
                    v5::codec::PublishAckReason::Success,
                ));
            }
        };

        let decoded_payload = match SparkplugPayload::decode(payload.as_ref()) {
            Ok(decoded) => decoded,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.payload_decode_failed",
                    topic = topic,
                    error = %error
                );
                return InboundPublishAction::AckOnly(v5::PublishAck::new(
                    v5::codec::PublishAckReason::PayloadFormatInvalid,
                ));
            }
        };

        let mut state = self.inner.sparkplug_state.borrow_mut();
        let visit_outcome =
            match state.visit_message(&lifecycle_topic, &decoded_payload, observed_at) {
                Ok(outcome) => outcome,
                Err(error) => {
                    otel_warn!(
                        "mqtt_sparkplug.state_visit_failed",
                        topic = topic,
                        error = %error
                    );
                    return InboundPublishAction::AckOnly(v5::PublishAck::new(
                        v5::codec::PublishAckReason::ImplementationSpecificError,
                    ));
                }
            };
        let decode_context = match state.classify_decode_context(
            &lifecycle_topic,
            &decoded_payload,
            DeathOrigin::ExplicitPublish,
        ) {
            Ok(context) => context,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.decode_context_failed",
                    topic = topic,
                    error = %error
                );
                return InboundPublishAction::AckOnly(v5::PublishAck::new(
                    v5::codec::PublishAckReason::ImplementationSpecificError,
                ));
            }
        };

        let ingress = IngressMessage {
            topic: topic.to_string(),
            payload: payload.clone(),
            _decode_context: Some(decode_context),
        };
        let cascaded_ingress = visit_outcome
            .cascaded_device_deaths
            .iter()
            .filter_map(|death| self.synthetic_cascaded_death_message(&state, death))
            .collect();

        InboundPublishAction::Forward {
            ingress,
            cascaded_ingress,
        }
    }

    fn synthetic_cascaded_death_message(
        &self,
        state: &SparkplugState,
        death: &CascadedDeviceDeath,
    ) -> Option<IngressMessage> {
        let topic = Topic::Message {
            group_id: death.group_id.clone(),
            message_type: SparkplugMessageType::DDeath,
            edge_node_id: death.edge_node_id.clone(),
            device_id: Some(death.device_id.clone()),
        }
        .to_string();

        let parsed_topic = match Topic::parse(&topic) {
            Ok(parsed_topic) => parsed_topic,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.synthetic_death_topic_failed",
                    topic = topic.as_str(),
                    error = %error
                );
                return None;
            }
        };
        let lifecycle_topic = match parsed_topic.as_lifecycle_topic() {
            Ok(lifecycle_topic) => lifecycle_topic,
            Err(message_type) => {
                otel_warn!(
                    "mqtt_sparkplug.synthetic_death_topic_failed",
                    topic = topic.as_str(),
                    message_type = %message_type
                );
                return None;
            }
        };

        let context = match state.classify_decode_context(
            &lifecycle_topic,
            &empty_sparkplug_payload(),
            DeathOrigin::ExplicitPublish,
        ) {
            Ok(context) => context,
            Err(error) => {
                otel_warn!(
                    "mqtt_sparkplug.synthetic_death_context_failed",
                    topic = topic.as_str(),
                    error = %error
                );
                return None;
            }
        };

        Some(IngressMessage {
            topic,
            payload: Bytes::new(),
            _decode_context: Some(context),
        })
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
        let observed_at = current_timestamp_millis();

        let action = self.process_inbound_publish(&topic, &payload, observed_at);
        match action {
            InboundPublishAction::Forward {
                ingress,
                cascaded_ingress,
            } => {
                let _ = self.inner.ingress_tx.send(ingress);
                for message in cascaded_ingress {
                    let _ = self.inner.ingress_tx.send(message);
                }

                if let Err(error) = self.relay_to_matching_clients(&topic, &payload).await {
                    otel_warn!(
                        "mqtt_sparkplug.remote_relay_failed",
                        client_id = session.client.client_id.as_str(),
                        connection_id = session.connection_id,
                        topic = topic.as_str(),
                        error = %error
                    );
                    return Ok(v5::PublishAck::new(
                        v5::codec::PublishAckReason::ImplementationSpecificError,
                    ));
                }

                otel_debug!(
                    "mqtt_sparkplug.publish_received",
                    client_id = session.client.client_id.as_str(),
                    connection_id = session.connection_id,
                    topic = topic.as_str()
                );

                Ok(publish.ack())
            }
            InboundPublishAction::AckOnly(ack) => Ok(ack),
        }
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
                let mut new_filters = Vec::new();
                {
                    let mut subscriptions = session.client.subscriptions.borrow_mut();
                    for mut entry in subscribe.iter_mut() {
                        let topic_filter = entry.topic().to_string();
                        if !subscriptions
                            .iter()
                            .any(|existing| existing == &topic_filter)
                        {
                            subscriptions.push(topic_filter.clone());
                            new_filters.push(topic_filter);
                        }
                        entry.confirm(v5::QoS::AtLeastOnce);
                    }
                }

                if !new_filters.is_empty() {
                    self.send_retained_matches_to_client(session.client.clone(), new_filters);
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

fn empty_sparkplug_payload() -> SparkplugPayload {
    SparkplugPayload::from(pb::Payload {
        timestamp: None,
        metrics: Vec::new(),
        seq: None,
        uuid: None,
        body: None,
    })
}

fn current_timestamp_millis() -> Timestamp {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let millis = duration.as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[async_trait(?Send)]
impl LocalMqttIngress for MqttSparkplugExtension {
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
                                "mqtt_sparkplug.ingress_topic_match_failed",
                                error = %cap_err.error(error.to_string())
                            );
                            None
                        }
                    },
                    Err(BroadcastStreamRecvError::Lagged(count)) => {
                        otel_warn!("mqtt_sparkplug.ingress_subscriber_lagged", count = count);
                        None
                    }
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[async_trait(?Send)]
impl LocalMqttEgress for MqttSparkplugExtension {
    async fn publish(&self, message: MqttMessage) -> Result<(), CapabilityError> {
        self.validate_topic_name(&message.topic)
            .map_err(|error| self.inner.egress_cap_err.error(error.to_string()))?;

        // RFC 0003 reserves a higher-priority local-service egress lane for
        // STATE and command traffic. This scaffold keeps the same direct
        // fan-out shape as `extension:mqtt_server`; the reserved lane is
        // follow-on work.
        self.relay_to_matching_clients(&message.topic, &message.payload)
            .await
            .map_err(|error| self.inner.egress_cap_err.error(error.to_string()))
    }
}

#[async_trait(?Send)]
impl LocalExtension for MqttSparkplugExtension {
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
        self.seed_online_state_retained();
        effect_handler.signal_ready();
        otel_debug!("mqtt_sparkplug.listener_bound", address = %listener_addr);

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
                            otel_warn!("mqtt_sparkplug.accept_failed", error = %error);
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
                                otel_debug!("mqtt_sparkplug.connection_closed", peer = %peer_addr);
                            }
                            Err(error) => {
                                otel_warn!(
                                    "mqtt_sparkplug.connection_ended_with_error",
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
                            self.publish_graceful_offline_state().await;
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
                            self.publish_graceful_offline_state().await;
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
