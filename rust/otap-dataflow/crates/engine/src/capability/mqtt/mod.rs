// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT service capabilities.
//!
//! This is the first, intentionally narrow slice of the design in
//! `rfcs/0003-mqtt-service-capabilities.md`: a connection-owning extension
//! (`extension:mqtt_client` today, an `ntex-mqtt`-backed `extension:mqtt_server`
//! later) exposes [`MqttMessage`] streams/sinks to receiver and exporter
//! nodes through the [`mqtt_ingress`] and [`mqtt_egress`] capabilities.
//!
//! [`MqttMessage`] is deliberately minimal -- topic and payload bytes only.
//! There is no QoS, retain flag, MQTT v5 user property, origin tag (which
//! connection/subscription produced this message), or acknowledgment-boundary
//! metadata yet. The RFC's fuller raw-envelope contract (tracked in
//! `docs/issue-drafts/mqtt-raw-envelope-contract.md`) and any pluggable bytes
//! representation for round-trip fidelity are explicit follow-on work, not
//! attempted in this slice: the near-term goal is a working MQTT-in/MQTT-out
//! pipeline (text payload -> log record -> OTLP JSON, and the reverse), not
//! protocol-faithful bridging.

mod models;
pub mod mqtt_egress;
pub mod mqtt_ingress;

pub use models::MqttMessage;
