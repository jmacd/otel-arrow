// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the MQTT Sparkplug extension.

use otel_arrow_dfe_sparkplug::StateTopicProfile;

/// Default bind host for the embedded MQTT listener.
fn default_bind_host() -> String {
    "0.0.0.0".to_string()
}

/// Default plaintext MQTT port.
const fn default_bind_port() -> u16 {
    1883
}

/// Default Sparkplug STATE topic profile for new deployments.
const fn default_state_profile() -> StateProfile {
    StateProfile::Sparkplug30
}

/// Configurable Sparkplug STATE topic profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum StateProfile {
    /// Sparkplug 2.2 `STATE/{host_id}` topics with `ONLINE`/`OFFLINE` payloads.
    #[serde(rename = "sparkplug_2_2")]
    Sparkplug22,
    /// Sparkplug 3.0 `spBv1.0/STATE/{host_id}` topics with timestamped JSON payloads.
    #[serde(rename = "sparkplug_3")]
    Sparkplug30,
}

impl From<StateProfile> for StateTopicProfile {
    fn from(value: StateProfile) -> Self {
        match value {
            StateProfile::Sparkplug22 => StateTopicProfile::Sparkplug22,
            StateProfile::Sparkplug30 => StateTopicProfile::Sparkplug30,
        }
    }
}

/// Configuration for `extension:mqtt_sparkplug`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Host or IP address to bind.
    #[serde(default = "default_bind_host")]
    pub bind_host: String,
    /// TCP port to bind. `0` requests an ephemeral port, primarily for tests.
    #[serde(default = "default_bind_port")]
    pub bind_port: u16,
    /// Primary Host identifier used in the Sparkplug STATE topic.
    pub host_id: String,
    /// Sparkplug STATE topic profile and payload shape.
    #[serde(default = "default_state_profile")]
    pub state_profile: StateProfile,
}

impl Config {
    /// Validates the configuration beyond what deserialization checks.
    pub fn validate(&self) -> Result<(), String> {
        if self.bind_host.trim().is_empty() {
            return Err("`bind_host` must not be empty".to_string());
        }
        if self.host_id.trim().is_empty() {
            return Err("`host_id` must not be empty".to_string());
        }
        Ok(())
    }
}
