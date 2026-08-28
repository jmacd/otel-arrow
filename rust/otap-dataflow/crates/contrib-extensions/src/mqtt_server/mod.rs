// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT Server extension.
//!
//! A minimal embedded MQTT host service backed by `ntex-mqtt`, exposed to
//! data-path nodes through the `MqttIngress` and `MqttEgress` capabilities.
//! This scaffold intentionally stays within the RFC's first server milestone:
//! one core, one listener, MQTT v5 only, no TLS, no retained messages, and
//! no Will or session-persistence support yet.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = MQTT_SERVER_URN,
    target = "microsoft.extension.mqtt_server",
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
use self::extension::MqttServerExtension;

/// URN under which this extension is registered.
pub const MQTT_SERVER_URN: &str = "urn:microsoft:extension:mqtt_server";

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

/// Builds an `MqttServerExtension` bundle.
fn create(
    _ext_ctx: &ExtensionContext,
    name: otel_arrow_dfe_config::ExtensionId,
    ext_config: std::sync::Arc<ExtensionUserConfig>,
    extension_config: &ExtensionConfig,
) -> Result<ExtensionBundle, ConfigError> {
    let config = parse_config(&ext_config.config)?;
    let extension = MqttServerExtension::new(name.clone(), config);

    ExtensionWrapper::builder(name, ext_config, extension_config)
        .active()
        .local::<MqttServerExtension>(Rc::new(extension))
        .build()
        .map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Factory registration for the MQTT Server extension.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Extension)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static MQTT_SERVER_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: MQTT_SERVER_URN,
    description: "Active+Local extension exposing MqttIngress/MqttEgress via ntex-mqtt",
    documentation_url: "",
    capabilities: Some(extension_capabilities!(
        local: MqttServerExtension => [MqttIngress, MqttEgress]
    )),
    create,
    validate_config,
};
