// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT Server extension.

/// Default bind host for the embedded MQTT listener.
fn default_bind_host() -> String {
    "0.0.0.0".to_string()
}

/// Default plaintext MQTT port.
const fn default_bind_port() -> u16 {
    1883
}

/// Configuration for `extension:mqtt_server`.
///
/// Scope, matching the current milestone (see
/// `rfcs/0003-mqtt-service-capabilities.md`): one listener on one core,
/// MQTT v5 only, no retained messages, no Will handling, and no persistent
/// sessions across reconnects.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Host or IP address to bind.
    #[serde(default = "default_bind_host")]
    pub bind_host: String,
    /// TCP port to bind. `0` requests an ephemeral port, primarily for tests.
    #[serde(default = "default_bind_port")]
    pub bind_port: u16,
}

impl Config {
    /// Validates the configuration beyond what deserialization checks.
    pub fn validate(&self) -> Result<(), String> {
        if self.bind_host.trim().is_empty() {
            return Err("`bind_host` must not be empty".to_string());
        }
        Ok(())
    }
}
