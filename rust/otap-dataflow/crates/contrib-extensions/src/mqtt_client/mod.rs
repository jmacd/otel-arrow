// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT Client extension.
//!
//! A single plaintext MQTT connection (via `ms-mqtt-client`) exposed to
//! data-path nodes through the `MqttIngress` and `MqttEgress` capabilities.
//! See `rfcs/0003-mqtt-service-capabilities.md` for the design and the
//! milestone this implements (no round-trip fidelity, no pluggable bytes
//! representations yet; plaintext only, see the `mqtt-client-extension`
//! feature comment in `Cargo.toml` for the OpenSSL status of the underlying
//! `ms-mqtt-client` library).

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = MQTT_CLIENT_URN,
    target = "microsoft.extension.mqtt_client",
);

pub mod config;
mod extension;

use std::rc::Rc;

use linkme::distributed_slice;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_engine::ExtensionFactory;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_egress::MqttEgress;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_ingress::MqttIngress;
use otel_arrow_dfe_engine::config::ExtensionConfig;
use otel_arrow_dfe_engine::context::ExtensionContext;
use otel_arrow_dfe_engine::extension::{ExtensionBundle, ExtensionWrapper};
use otel_arrow_dfe_engine::extension_capabilities;
use otel_arrow_dfe_otap::OTAP_EXTENSION_FACTORIES;

use self::config::Config;
use self::extension::MqttClientExtension;

/// URN under which this extension is registered.
pub const MQTT_CLIENT_URN: &str = "urn:microsoft:extension:mqtt_client";

/// Deserializes and validates the extension's user configuration.
fn parse_config(config: &serde_json::Value) -> Result<Config, ConfigError> {
    let parsed: Config =
        serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })?;
    parsed
        .validate()
        .map_err(|error| ConfigError::InvalidUserConfig { error })?;
    Ok(parsed)
}

/// Static config validation hook for the factory.
fn validate_config(config: &serde_json::Value) -> Result<(), ConfigError> {
    parse_config(config).map(|_| ())
}

/// Builds an `MqttClientExtension` bundle.
fn create(
    _ext_ctx: &ExtensionContext,
    name: otel_arrow_dfe_config::ExtensionId,
    ext_config: std::sync::Arc<ExtensionUserConfig>,
    extension_config: &ExtensionConfig,
) -> Result<ExtensionBundle, ConfigError> {
    // Validate config now so a bad config fails fast at wiring time.
    let config = parse_config(&ext_config.config)?;
    let connect_timeout = config.connect_timeout;

    let extension = MqttClientExtension::new(name.clone(), config);

    ExtensionWrapper::builder(name, ext_config, extension_config)
        .active()
        .with_readiness_probe_timeout_override(connect_timeout)
        .local::<MqttClientExtension>(Rc::new(extension))
        .build()
        .map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Factory registration for the MQTT Client extension.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Extension)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static MQTT_CLIENT_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: MQTT_CLIENT_URN,
    description: "Active+Local extension exposing MqttIngress/MqttEgress via ms-mqtt-client",
    documentation_url: "",
    capabilities: Some(extension_capabilities!(
        local: MqttClientExtension => [MqttIngress, MqttEgress]
    )),
    create,
    validate_config,
};
