// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the event-time window (L2) processor.

use std::time::Duration;

use otap_df_config::TopicName;
use otap_df_config::error::Error as ConfigError;
use serde::{Deserialize, Serialize};

const DEFAULT_WINDOW_SECS: u64 = 60;
const DEFAULT_ALLOWED_LATENESS_SECS: u64 = 30;
const DEFAULT_MAX_LAG_SECS: u64 = 3600;

/// Configuration for the event-time window processor (metrics-appliance L2,
/// decisions D10-D12).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// The tumbling window width over event time. Windows are epoch-aligned.
    #[serde(with = "humantime_serde", default = "default_window_size")]
    pub window_size: Duration,

    /// How far the per-stream watermark trails the maximum observed event time,
    /// and the on-time firing offset past a window's end. Should not exceed
    /// `max_lag`.
    #[serde(with = "humantime_serde", default = "default_allowed_lateness")]
    pub allowed_lateness: Duration,

    /// The idle floor: the watermark is at least `processing_time - max_lag`, so
    /// idle streams' windows still close. Matches the ingest queue's `max_lag`.
    #[serde(with = "humantime_serde", default = "default_max_lag")]
    pub max_lag: Duration,

    /// The partition-dispatch topic this windower is an owner of. When set, the
    /// windower pulls the topic's load-report sender from the pipeline's topic set
    /// and reports its per-partition load to the placement scheduler, closing the
    /// load feedback loop (durable-dispatch I4). When unset, load reporting is off.
    #[serde(default)]
    pub report_load_to: Option<TopicName>,
}

impl Config {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.window_size.is_zero() {
            return Err(ConfigError::InvalidUserConfig {
                error: "window_size must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            window_size: default_window_size(),
            allowed_lateness: default_allowed_lateness(),
            max_lag: default_max_lag(),
            report_load_to: None,
        }
    }
}

fn default_window_size() -> Duration {
    Duration::from_secs(DEFAULT_WINDOW_SECS)
}

fn default_allowed_lateness() -> Duration {
    Duration::from_secs(DEFAULT_ALLOWED_LATENESS_SECS)
}

fn default_max_lag() -> Duration {
    Duration::from_secs(DEFAULT_MAX_LAG_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults() {
        let c = Config::default();
        assert_eq!(c.window_size, Duration::from_secs(60));
        assert_eq!(c.allowed_lateness, Duration::from_secs(30));
        assert_eq!(c.max_lag, Duration::from_secs(3600));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn deserialize_overrides() {
        let c: Config = serde_json::from_value(json!({
            "window_size": "10s",
            "allowed_lateness": "2s",
            "max_lag": "10m"
        }))
        .unwrap();
        assert_eq!(c.window_size, Duration::from_secs(10));
        assert_eq!(c.allowed_lateness, Duration::from_secs(2));
        assert_eq!(c.max_lag, Duration::from_secs(600));
    }

    #[test]
    fn rejects_zero_window() {
        let c = Config {
            window_size: Duration::ZERO,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
}
