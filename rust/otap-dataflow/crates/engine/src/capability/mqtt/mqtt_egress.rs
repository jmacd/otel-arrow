// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The `MqttEgress` capability.
//!
//! Exposed by an MQTT connection-owning extension (`extension:mqtt_client`
//! today) and consumed by an MQTT-publishing exporter node. See
//! `rfcs/0003-mqtt-service-capabilities.md` for the full design; this trait is
//! the first, narrow slice of it: a single `publish` call with no per-message
//! QoS/retain/property control and no bounded in-flight reservation yet --
//! the extension applies its own configured default QoS to every publish.
//!
//! The `#[capability]` proc macro expands the trait into:
//!
//! - A `pub(crate) mod local` containing the `!Send` `MqttEgress` trait variant
//! - A `pub(crate) mod shared` containing the `Send + Sync` `MqttEgress` trait variant
//! - A `SharedAsLocalMqttEgress` adapter
//! - A zero-sized `pub struct MqttEgress` registration handle
//! - `local_entry::<E>` / `shared_entry::<E>` factory bridges
//! - A `KNOWN_CAPABILITIES` distributed-slice entry

use super::MqttMessage;
use crate::capability::error::CapabilityError;
use otel_arrow_dfe_engine_macros::capability;

/// Publishes outbound MQTT PUBLISH messages.
#[capability(
    name = "mqtt_egress",
    description = "Publishes outbound MQTT PUBLISH messages"
)]
pub trait MqttEgress {
    /// Publishes `message` and resolves once the extension's MQTT client has
    /// accepted it for delivery (the RFC's `admitted` ack-boundary level, not
    /// yet configurable).
    ///
    /// Returns a [`CapabilityError`] if the extension rejects the publish
    /// (for example, the topic is invalid, or a bounded outbound queue is
    /// full). This does not distinguish broker refusal from transient
    /// connection loss yet -- that classification work is follow-on, tracked
    /// alongside the fuller ack-boundary design in the RFC.
    async fn publish(&self, message: MqttMessage) -> Result<(), CapabilityError>;
}
