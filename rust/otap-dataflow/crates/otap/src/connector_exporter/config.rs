// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the connector exporter.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default request channel capacity.
const DEFAULT_REQUEST_CAPACITY: usize = 64;

/// Default response channel capacity.
const DEFAULT_RESPONSE_CAPACITY: usize = 64;

/// Default maximum in-flight requests before backpressure.
const DEFAULT_MAX_IN_FLIGHT: usize = 32;

/// Configuration for the connector exporter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Capacity of the request channel (exporter -> consumer).
    #[serde(default = "default_request_capacity")]
    pub request_capacity: usize,

    /// Capacity of the response channel (consumer -> exporter).
    #[serde(default = "default_response_capacity")]
    pub response_capacity: usize,

    /// Maximum number of in-flight requests before the exporter
    /// applies backpressure by waiting for responses.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,

    /// Timeout for waiting on a response from the consumer.
    /// If not set, the exporter will wait indefinitely.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub response_timeout: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_capacity: DEFAULT_REQUEST_CAPACITY,
            response_capacity: DEFAULT_RESPONSE_CAPACITY,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            response_timeout: None,
        }
    }
}

impl Config {
    /// Create a new configuration with custom capacities.
    pub fn new(
        request_capacity: usize,
        response_capacity: usize,
        max_in_flight: usize,
    ) -> Self {
        Self {
            request_capacity,
            response_capacity,
            max_in_flight,
            response_timeout: None,
        }
    }

    /// Set the response timeout.
    #[must_use]
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = Some(timeout);
        self
    }
}

fn default_request_capacity() -> usize {
    DEFAULT_REQUEST_CAPACITY
}

fn default_response_capacity() -> usize {
    DEFAULT_RESPONSE_CAPACITY
}

fn default_max_in_flight() -> usize {
    DEFAULT_MAX_IN_FLIGHT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.request_capacity, 64);
        assert_eq!(config.response_capacity, 64);
        assert_eq!(config.max_in_flight, 32);
        assert!(config.response_timeout.is_none());
    }

    #[test]
    fn test_config_from_json() {
        let json = r#"{
            "request_capacity": 128,
            "response_capacity": 256,
            "max_in_flight": 64,
            "response_timeout": "5s"
        }"#;

        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.request_capacity, 128);
        assert_eq!(config.response_capacity, 256);
        assert_eq!(config.max_in_flight, 64);
        assert_eq!(config.response_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_config_defaults_in_json() {
        let json = r#"{}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.request_capacity, 64);
        assert_eq!(config.response_capacity, 64);
        assert_eq!(config.max_in_flight, 32);
    }
}
