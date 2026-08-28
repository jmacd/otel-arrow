// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use crate::topic::SparkplugMessageType;

/// How a downstream codec should classify a Sparkplug message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalType {
    /// The payload should materialize as metrics.
    Metric,
    /// The payload should materialize as logs.
    Log,
}

impl SignalType {
    pub(crate) const fn for_message_type(message_type: SparkplugMessageType) -> Self {
        match message_type {
            SparkplugMessageType::NBirth
            | SparkplugMessageType::DBirth
            | SparkplugMessageType::NData
            | SparkplugMessageType::DData => Self::Metric,
            SparkplugMessageType::NDeath | SparkplugMessageType::DDeath => Self::Log,
            SparkplugMessageType::NCmd
            | SparkplugMessageType::DCmd
            | SparkplugMessageType::State => Self::Log,
        }
    }
}

/// The best-effort origin for a death message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DeathOrigin {
    /// The embedded router emitted a registered MQTT Will.
    RouterWill,
    /// The peer explicitly published the death payload.
    ExplicitPublish,
    /// The transport could not distinguish a Will from an ordinary publish.
    #[default]
    Unknown,
}

/// One alias definition referenced by a single payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedAlias {
    /// The numeric Sparkplug alias used in the payload.
    pub alias: u64,
    /// The resolved metric name for the alias.
    pub name: String,
}

/// Immutable context carried with one Sparkplug payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparkplugDecodeContext {
    /// The Sparkplug group identifier.
    pub group_id: String,
    /// The Sparkplug edge node identifier.
    pub edge_node_id: String,
    /// The optional Sparkplug device identifier.
    pub device_id: Option<String>,
    /// The parsed Sparkplug message type.
    pub message_type: SparkplugMessageType,
    /// The downstream signal family implied by the message type.
    pub signal: SignalType,
    /// The current birth/death sequence number when known.
    pub b_d_seq: Option<u64>,
    /// Only the aliases referenced by this payload, resolved to names.
    pub resolved_aliases: Vec<ResolvedAlias>,
    /// The best-effort death origin for death messages.
    pub death_origin: DeathOrigin,
}
