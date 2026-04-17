// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics level configurations.

pub mod readers;
pub mod views;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::pipeline::telemetry::metrics::views::ViewConfig;

/// Metrics provider mode — single global setting that determines how
/// internal metrics are collected and exported.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderMode {
    /// No metrics collection or export.
    None,
    /// Prometheus + console reporting without an internal pipeline.
    /// Metrics are collected by an admin-side periodic task.
    #[default]
    Admin,
    /// Prometheus + OTAP encoding routed through the internal telemetry
    /// pipeline (Internal Telemetry System).
    ITS,
}

/// Internal metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub struct MetricsConfig {
    /// The metrics provider mode.
    #[serde(default)]
    pub provider: ProviderMode,

    /// The list of metrics readers to configure (legacy OTel SDK readers).
    #[serde(default)]
    pub readers: Vec<readers::MetricsReaderConfig>,

    /// The metrics views configuration.
    #[serde(default)]
    pub views: Vec<ViewConfig>,
}

impl MetricsConfig {
    /// Returns `true` if there are any metric readers configured.
    #[must_use]
    pub const fn has_readers(&self) -> bool {
        !self.readers.is_empty()
    }

    /// Returns `true` if the provider mode uses the internal telemetry system.
    #[must_use]
    pub const fn uses_its_provider(&self) -> bool {
        matches!(self.provider, ProviderMode::ITS)
    }

    /// Validates the metrics configuration.
    pub fn validate(&self) -> Result<(), crate::error::Error> {
        let mut errors = Vec::new();
        for reader in &self.readers {
            if let Err(e) = reader.validate() {
                errors.push(e);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error::Error::InvalidConfiguration { errors })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_mode_default() {
        let config = MetricsConfig::default();
        assert_eq!(config.provider, ProviderMode::Admin);
    }

    #[test]
    fn test_provider_mode_deserialize() {
        for (yaml, expected) in [
            ("provider: none", ProviderMode::None),
            ("provider: admin", ProviderMode::Admin),
            ("provider: its", ProviderMode::ITS),
        ] {
            let config: MetricsConfig = serde_yaml::from_str(yaml).unwrap();
            assert_eq!(config.provider, expected, "yaml: {yaml}");
        }
    }

    #[test]
    fn test_provider_mode_default_when_omitted() {
        let config: MetricsConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(config.provider, ProviderMode::Admin);
    }

    #[test]
    fn test_uses_its_provider() {
        assert!(!MetricsConfig::default().uses_its_provider());
        let config = MetricsConfig {
            provider: ProviderMode::ITS,
            ..Default::default()
        };
        assert!(config.uses_its_provider());
    }

    #[test]
    fn test_metrics_config_deserialize() {
        let yaml_str = r#"
            readers:
              - periodic:
                  exporter:
                    type: console
                  interval: "10s"
            "#;

        let config: MetricsConfig = serde_yaml::from_str(yaml_str).unwrap();

        assert_eq!(config.readers.len(), 1);

        if let readers::MetricsReaderConfig::Periodic(periodic_config) = &config.readers[0] {
            if readers::periodic::MetricsPeriodicExporterType::Console
                != periodic_config.exporter.exporter_type
            {
                panic!("Expected console exporter");
            }
            assert_eq!(periodic_config.interval.as_secs(), 10);
        } else {
            panic!("Expected periodic reader");
        }
    }
}
