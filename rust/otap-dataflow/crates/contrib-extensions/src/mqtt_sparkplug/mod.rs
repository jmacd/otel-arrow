// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! MQTT Sparkplug extension.
//!
//! A minimal self-hosted Sparkplug service backed by `ntex-mqtt` and the
//! engine-free `otel-arrow-dfe-sparkplug` crate. This scaffold intentionally
//! stays within RFC 0003's first standalone milestone: one core, one listener,
//! MQTT v5 only, no TLS, retained STATE only, graceful-offline substitution
//! for crash-safe Will behavior, and raw MQTT relay through the existing
//! `MqttIngress`/`MqttEgress` capability surface.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = MQTT_SPARKPLUG_URN,
    target = "microsoft.extension.mqtt_sparkplug",
);

pub mod config;
mod extension;

use std::rc::Rc;

use linkme::distributed_slice;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_engine::ExtensionFactory;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_egress::MqttEgress as MqttEgressCap;
use otel_arrow_dfe_engine::capability::mqtt::mqtt_ingress::MqttIngress as MqttIngressCap;
use otel_arrow_dfe_engine::config::ExtensionConfig;
use otel_arrow_dfe_engine::context::ExtensionContext;
use otel_arrow_dfe_engine::extension::{ExtensionBundle, ExtensionWrapper};
use otel_arrow_dfe_engine::extension_capabilities;
use otel_arrow_dfe_otap::OTAP_EXTENSION_FACTORIES;

use self::config::Config;
use self::extension::MqttSparkplugExtension;

/// URN under which this extension is registered.
pub const MQTT_SPARKPLUG_URN: &str = "urn:microsoft:extension:mqtt_sparkplug";

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

/// Builds an `MqttSparkplugExtension` bundle.
fn create(
    _ext_ctx: &ExtensionContext,
    name: otel_arrow_dfe_config::ExtensionId,
    ext_config: std::sync::Arc<ExtensionUserConfig>,
    extension_config: &ExtensionConfig,
) -> Result<ExtensionBundle, ConfigError> {
    let config = parse_config(&ext_config.config)?;
    let extension = MqttSparkplugExtension::new(name.clone(), config);

    ExtensionWrapper::builder(name, ext_config, extension_config)
        .active()
        .local::<MqttSparkplugExtension>(Rc::new(extension))
        .build()
        .map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Factory registration for the MQTT Sparkplug extension.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Extension)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static MQTT_SPARKPLUG_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: MQTT_SPARKPLUG_URN,
    description: "Active+Local extension exposing Sparkplug-aware MqttIngress/MqttEgress",
    documentation_url: "",
    capabilities: Some(extension_capabilities!(
        local: MqttSparkplugExtension => [MqttIngressCap, MqttEgressCap]
    )),
    create,
    validate_config,
};
