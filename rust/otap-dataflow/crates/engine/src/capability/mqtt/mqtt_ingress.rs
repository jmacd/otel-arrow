// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The `MqttIngress` capability.
//!
//! Exposed by an MQTT connection-owning extension (`extension:mqtt_client`
//! today) and consumed by `receiver:mqtt` (and, later, an MQTT-aware codec
//! extension). See `rfcs/0003-mqtt-service-capabilities.md` for the full
//! design; this trait is the first, narrow slice of it: one bounded stream of
//! [`MqttMessage`]s matching a topic filter, with no origin tagging, no
//! acknowledgment-boundary negotiation, and no per-consumer byte/item
//! reservation yet.
//!
//! The `#[capability]` proc macro expands the trait into:
//!
//! - A `pub(crate) mod local` containing the `!Send` `MqttIngress` trait variant
//! - A `pub(crate) mod shared` containing the `Send + Sync` `MqttIngress` trait variant
//! - A `SharedAsLocalMqttIngress` adapter
//! - A zero-sized `pub struct MqttIngress` registration handle
//! - `local_entry::<E>` / `shared_entry::<E>` factory bridges
//! - A `KNOWN_CAPABILITIES` distributed-slice entry

use super::MqttMessage;
use crate::capability::error::CapabilityError;
use futures::Stream;
use otel_arrow_dfe_engine_macros::capability;
use std::pin::Pin;

/// A subscription to inbound MQTT PUBLISH messages matching a topic filter.
///
/// Boxed to hide the concrete stream type behind the capability boundary, the
/// same reasoning as
/// [`TokenStream`](crate::capability::auth::bearer_token_provider::TokenStream).
/// Omits `Send` because a subscription is always consumed on the core that
/// created it (thread-per-core); the `#[capability]` macro emits this
/// signature into both the `local` (`?Send`) and `shared` (`Send + Sync`)
/// trait variants unchanged.
///
/// There is no acknowledgment handle carried alongside each item yet: this
/// capability's first slice always operates at the weakest ack-boundary level
/// (`protocol` in the RFC's terms) -- the extension has already completed the
/// MQTT-level handshake for a message before it appears on this stream.
/// Stronger ack-boundary levels are follow-on work.
pub type MqttMessageStream = Pin<Box<dyn Stream<Item = MqttMessage> + 'static>>;

/// Provides a stream of inbound MQTT PUBLISH messages matching a topic
/// filter.
#[capability(
    name = "mqtt_ingress",
    description = "Provides a stream of inbound MQTT PUBLISH messages matching a topic filter"
)]
pub trait MqttIngress {
    /// Subscribes to messages published on topics matching `topic_filter`
    /// (MQTT topic-filter syntax, e.g. `"sensors/#"`).
    ///
    /// Returns a [`CapabilityError`] if the extension cannot register the
    /// filter (for example, the filter is syntactically invalid, or the
    /// extension has already reached a configured subscription limit).
    /// Successful registration does not guarantee the underlying MQTT
    /// SUBSCRIBE has completed against a remote broker; that is runtime
    /// health, reported separately, not a precondition for this call.
    async fn subscribe(&self, topic_filter: &str) -> Result<MqttMessageStream, CapabilityError>;
}
