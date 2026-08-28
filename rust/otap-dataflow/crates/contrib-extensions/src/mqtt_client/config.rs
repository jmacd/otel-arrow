// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT Client extension.

use serde::Deserialize;
use std::time::Duration;

/// Default MQTT broker port.
const fn default_port() -> u16 {
    1883
}

/// Default connect timeout.
fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Configuration for `extension:mqtt_client`.
///
/// Scope, matching the current milestone (see
/// `rfcs/0003-mqtt-service-capabilities.md`): a single plaintext TCP
/// connection to one broker, with at most one broker-level subscription. TLS,
/// multiple brokers, and per-consumer topic filters registered as distinct
/// broker-level SUBSCRIBEs are follow-on work.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Broker hostname or IP address.
    pub hostname: String,
    /// Broker TCP port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// MQTT client identifier. If omitted, the broker assigns one.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Topic filter to subscribe to on connect (MQTT topic-filter syntax,
    /// e.g. `"sensors/#"`). If omitted, the connection is egress-only (no
    /// SUBSCRIBE is sent, and `MqttIngress::subscribe` always yields an
    /// empty stream).
    #[serde(default)]
    pub subscribe_topic_filter: Option<String>,
    /// Connect timeout. Accepts human-readable durations (e.g. `10s`).
    #[serde(with = "humantime_serde", default = "default_connect_timeout")]
    pub connect_timeout: Duration,
}

impl Config {
    /// Validates the configuration beyond what deserialization checks.
    pub fn validate(&self) -> Result<(), String> {
        if self.hostname.trim().is_empty() {
            return Err("`hostname` must not be empty".to_string());
        }
        if self.port == 0 {
            return Err("`port` must not be zero".to_string());
        }
        if self.connect_timeout.is_zero() {
            return Err("`connect_timeout` must be greater than zero".to_string());
        }
        Ok(())
    }
}
