// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The MQTT Client extension: a single plaintext MQTT connection (via
//! `ms-mqtt-client`) exposed to data-path nodes through the `MqttIngress`
//! and `MqttEgress` capabilities.
//!
//! This is a `!Send`, thread-per-core extension: each core that hosts a
//! `receiver:mqtt` or MQTT-publishing exporter node gets its own connection.
//! There is deliberately no cross-core connection sharing or session
//! resumption in this milestone -- see
//! `rfcs/0003-mqtt-service-capabilities.md`.

use std::cell::RefCell;
use std::rc::Rc;

use async_trait::async_trait;
use futures::StreamExt;
use otel_arrow_dfe_config::ExtensionId;
use otel_arrow_dfe_contrib_nodes::common::mqtt::backend::{
    self, MqttBackendConfig, MqttConnectionParts, RawMqttMessage,
};
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
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use super::config::Config;

/// Bounded fan-out buffer for inbound messages delivered to `MqttIngress`
/// subscribers. Sized generously for a prototype pipeline; a slow subscriber
/// that falls this far behind loses the oldest unread messages (reported by
/// `tokio::sync::broadcast` as a lag, silently dropped by this extension --
/// see the `RecvError::Lagged` handling in [`MqttClientExtension::start`]).
/// A configurable size and back-pressure-aware fan-out are follow-on work.
const INGRESS_BROADCAST_CAPACITY: usize = 1024;

/// Shared state behind an `Rc`, so every clone of [`MqttClientExtension`]
/// (the background task plus every capability consumer on the same core)
/// observes the same connection.
struct Inner {
    config: Config,
    ingress_cap_err: CapabilityErrorSource<MqttIngressCap>,
    egress_cap_err: CapabilityErrorSource<MqttEgressCap>,
    /// Set once `start()` has connected. `None` before connection and after
    /// the connection has ended (e.g. broker disconnect); capability calls
    /// in that window fail with a `CapabilityError` rather than blocking.
    client: RefCell<Option<ms_mqtt_client::client::Client>>,
    /// Fan-out for inbound PUBLISH messages. Always constructed, even for an
    /// egress-only configuration (`subscribe_topic_filter: None`), in which
    /// case it simply never receives anything.
    ingress_tx: broadcast::Sender<RawMqttMessage>,
}

/// The MQTT Client extension. `!Send`; every clone on a core shares one
/// [`Inner`] via `Rc`.
#[derive(Clone)]
pub(crate) struct MqttClientExtension {
    inner: Rc<Inner>,
}

impl MqttClientExtension {
    /// Builds a new, not-yet-connected instance. The connection is
    /// established when the extension's active task (`start()`) runs.
    pub(crate) fn new(extension_id: ExtensionId, config: Config) -> Self {
        let (ingress_tx, _rx) = broadcast::channel(INGRESS_BROADCAST_CAPACITY);
        Self {
            inner: Rc::new(Inner {
                config,
                ingress_cap_err: CapabilityErrorSource::new(extension_id.clone()),
                egress_cap_err: CapabilityErrorSource::new(extension_id),
                client: RefCell::new(None),
                ingress_tx,
            }),
        }
    }

    /// Builds the `MqttBackendConfig` for the initial connection.
    fn backend_config(&self) -> MqttBackendConfig {
        MqttBackendConfig {
            hostname: self.inner.config.hostname.clone(),
            port: self.inner.config.port,
            client_id: self.inner.config.client_id.clone(),
            subscribe_topic_filter: self.inner.config.subscribe_topic_filter.clone(),
            connect_timeout: Some(self.inner.config.connect_timeout),
        }
    }
}

#[async_trait(?Send)]
impl LocalMqttIngress for MqttClientExtension {
    async fn subscribe(&self, topic_filter: &str) -> Result<MqttMessageStream, CapabilityError> {
        let topic_filter = topic_filter.to_owned();
        let rx = self.inner.ingress_tx.subscribe();
        let cap_err = self.inner.ingress_cap_err.clone();
        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            let topic_filter = topic_filter.clone();
            let cap_err = cap_err.clone();
            async move {
                match item {
                    Ok(msg) => match backend::topic_matches(&topic_filter, &msg.topic) {
                        Ok(true) => Some(MqttMessage::new(msg.topic, msg.payload)),
                        Ok(false) => None,
                        Err(error) => {
                            otel_warn!(
                                "mqtt_client.ingress_topic_match_failed",
                                error = %cap_err.error(error.to_string())
                            );
                            None
                        }
                    },
                    // A slow subscriber missed `count` messages; this
                    // milestone has no redelivery or backfill, so the gap is
                    // simply skipped.
                    Err(BroadcastStreamRecvError::Lagged(count)) => {
                        otel_warn!("mqtt_client.ingress_subscriber_lagged", count = count);
                        None
                    }
                }
            }
        });
        Ok(Box::pin(stream))
    }
}

#[async_trait(?Send)]
impl LocalMqttEgress for MqttClientExtension {
    async fn publish(&self, message: MqttMessage) -> Result<(), CapabilityError> {
        let client = self
            .inner
            .client
            .borrow()
            .clone()
            .ok_or_else(|| self.inner.egress_cap_err.error("not connected"))?;
        backend::publish(
            &client,
            RawMqttMessage {
                topic: message.topic,
                payload: message.payload,
            },
        )
        .await
        .map_err(|error| self.inner.egress_cap_err.error(error.to_string()))
    }
}

#[async_trait(?Send)]
impl LocalExtension for MqttClientExtension {
    async fn start(
        self: Rc<Self>,
        mut ctrl_chan: ControlChannel,
        effect_handler: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        let MqttConnectionParts {
            client,
            mut receiver,
            driver,
        } = backend::connect(self.backend_config())
            .await
            .map_err(|error| EngineError::InternalError {
                message: format!("MQTT connect failed: {error}"),
            })?;
        *self.inner.client.borrow_mut() = Some(client);
        otel_debug!("mqtt_client.connected");
        effect_handler.signal_ready();

        let ingress_tx = self.inner.ingress_tx.clone();
        let recv_loop = async {
            loop {
                match receiver.recv().await {
                    Some(msg) => {
                        // No subscribers is not an error: the connection may
                        // be egress-only, or ingress consumers may not have
                        // started yet.
                        let _ = ingress_tx.send(msg);
                    }
                    None => {
                        otel_debug!("mqtt_client.receiver_closed");
                        break;
                    }
                }
            }
        };
        tokio::pin!(recv_loop);
        let mut recv_closed = false;

        let driver_fut = driver.drive();
        tokio::pin!(driver_fut);

        loop {
            tokio::select! {
                ended = &mut driver_fut => {
                    *self.inner.client.borrow_mut() = None;
                    otel_warn!("mqtt_client.connection_ended", event = ?ended.event);
                    return Ok(TerminalState::default());
                }
                () = &mut recv_loop, if !recv_closed => {
                    // The inbound channel closed; the connection may still
                    // be driven by `driver_fut` (egress-only usage keeps
                    // working), so keep looping rather than returning. The
                    // `if !recv_closed` guard stops this arm from being
                    // polled again once its future has completed.
                    recv_closed = true;
                }
                ctrl_msg = ctrl_chan.recv() => {
                    match ctrl_msg {
                        Ok(ExtensionControlMsg::Shutdown { .. }) => {
                            *self.inner.client.borrow_mut() = None;
                            return Ok(TerminalState::default());
                        }
                        Ok(ExtensionControlMsg::Config { .. }) => {
                            // Live reconfiguration is not implemented in this
                            // milestone.
                        }
                        Ok(ExtensionControlMsg::CollectTelemetry { .. }) => {
                            // No metric set is registered yet for this
                            // extension; see the RFC's follow-on work list.
                        }
                        Err(_) => {
                            *self.inner.client.borrow_mut() = None;
                            return Ok(TerminalState::default());
                        }
                    }
                }
            }
        }
    }
}
