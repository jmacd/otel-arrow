// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! [`MqttMessage`]: the neutral inbound/outbound MQTT payload type shared by
//! [`super::super::mqtt_ingress`] and [`super::super::mqtt_egress`].

use bytes::Bytes;

/// A single MQTT PUBLISH payload, reduced to the fields a prototype
/// text-in/text-out pipeline needs.
///
/// # Deliberately not carried yet (see `rfcs/0003-mqtt-service-capabilities.md`)
///
/// There is no QoS, retain flag, MQTT5 properties, origin tag, or
/// acknowledgment-boundary metadata here. The engine has no pluggable byte
/// representation yet, so this type does not attempt to be a faithful,
/// round-trippable MQTT envelope -- it is the minimum needed to bridge MQTT
/// PUBLISH payloads into pdata (as log record bodies) and back out again.
/// Extending this type with the fuller envelope contract from
/// `docs/issue-drafts/mqtt-raw-envelope-contract.md` is expected follow-on
/// work, not a redesign.
#[derive(Debug, Clone)]
pub struct MqttMessage {
    /// The MQTT topic the message was published on (ingress) or should be
    /// published to (egress). Not validated against MQTT topic-name syntax
    /// here; validation is the extension's responsibility.
    pub topic: String,
    /// The raw PUBLISH payload bytes. Callers that need text treat this as
    /// UTF-8, lossily, rather than rejecting non-UTF-8 payloads -- consistent
    /// with the "no round-trip fidelity yet" scope of this capability.
    pub payload: Bytes,
}

impl MqttMessage {
    /// Builds a message from a topic and payload.
    #[must_use]
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
        }
    }
}
