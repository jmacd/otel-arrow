// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the metrics admission processor.

use std::time::Duration;

use otap_df_config::error::Error as ConfigError;
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_LAG_SECS: u64 = 3600;
const DEFAULT_MAX_SKEW_SECS: u64 = 300;

/// Configuration for the metrics admission processor.
///
/// The admission window `[now - max_lag, now + max_skew]` is the crude,
/// protective time bound applied before data reaches durable storage: it caps
/// backfill (too-old points) and rejects bad future clocks (too-future points).
/// Downstream layers (event-time windowing) do the fine-grained completeness
/// estimation *within* this envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Maximum admitted lag. Data points whose event time is older than
    /// `now - max_lag` are rejected as too old. This bounds how far back
    /// durable storage can be backfilled. Default: 1h.
    #[serde(with = "humantime_serde", default = "default_max_lag")]
    pub max_lag: Duration,

    /// Maximum admitted skew. Data points whose event time is further ahead than
    /// `now + max_skew` are rejected as too far in the future (a bad producer
    /// clock). Default: 5m.
    #[serde(with = "humantime_serde", default = "default_max_skew")]
    pub max_skew: Duration,

    /// Conflict policy. When `false` (the default), every distinct identity is
    /// admitted and a `name_conflict` warning is emitted once per conflicting
    /// name. When `true`, the data points of metrics whose type descriptor
    /// conflicts with the recorded primary are rejected instead.
    #[serde(default)]
    pub strict_conflicts: bool,
}

impl Config {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_lag.is_zero() {
            return Err(ConfigError::InvalidUserConfig {
                error: "max_lag must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_lag: default_max_lag(),
            max_skew: default_max_skew(),
            strict_conflicts: false,
        }
    }
}

fn default_max_lag() -> Duration {
    Duration::from_secs(DEFAULT_MAX_LAG_SECS)
}

fn default_max_skew() -> Duration {
    Duration::from_secs(DEFAULT_MAX_SKEW_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults() {
        let c = Config::default();
        assert_eq!(c.max_lag, Duration::from_secs(3600));
        assert_eq!(c.max_skew, Duration::from_secs(300));
        assert!(!c.strict_conflicts);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn deserialize_empty() {
        let c: Config = serde_json::from_value(json!({})).unwrap();
        assert_eq!(c.max_lag, Duration::from_secs(3600));
    }

    #[test]
    fn deserialize_overrides() {
        let c: Config = serde_json::from_value(json!({
            "max_lag": "10m",
            "max_skew": "30s",
            "strict_conflicts": true
        }))
        .unwrap();
        assert_eq!(c.max_lag, Duration::from_secs(600));
        assert_eq!(c.max_skew, Duration::from_secs(30));
        assert!(c.strict_conflicts);
    }

    #[test]
    fn rejects_zero_max_lag() {
        let c = Config {
            max_lag: Duration::ZERO,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
}
