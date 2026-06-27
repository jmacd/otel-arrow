// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the partition (split-by-key) processor.

use std::num::NonZeroUsize;

use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_pdata::otap::partition::PartitionKey;
use serde::{Deserialize, Serialize};

/// The key dimension a batch is split by (the per-signal partitioner,
/// ingest-queue D3). Each variant applies to exactly one signal; batches of any
/// other signal pass through unchanged.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionKeyConfig {
    /// Hash the metric name. Applies to metrics.
    MetricName,
    /// Slice the low 56 bits of the trace id. Applies to traces (and
    /// trace-correlated logs, whose support is deferred per durable-dispatch
    /// D26); standalone logs pass through.
    TraceId,
}

impl PartitionKeyConfig {
    /// The corresponding pdata partition key.
    pub(crate) fn pdata_key(self) -> PartitionKey {
        match self {
            PartitionKeyConfig::MetricName => PartitionKey::MetricName,
            PartitionKeyConfig::TraceId => PartitionKey::TraceId,
        }
    }

    /// The signal this key partitions; batches of any other signal are
    /// forwarded unchanged.
    pub(crate) fn target_signal(self) -> SignalType {
        match self {
            PartitionKeyConfig::MetricName => SignalType::Metrics,
            PartitionKeyConfig::TraceId => SignalType::Traces,
        }
    }
}

/// Configuration for the partition processor.
///
/// The processor splits each incoming batch of the key's target signal into
/// `num_partitions` sub-batches by [`key`](Config::key), tagging each with its
/// partition index for a downstream partition-dispatch hop (durable-dispatch
/// design, Layer A).
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// The key dimension to split by.
    pub key: PartitionKeyConfig,

    /// The number of partitions `N`. Must be a power of two so the partition
    /// index is the low `log2(N)` bits of the key function.
    pub num_partitions: NonZeroUsize,
}

impl Config {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.num_partitions.get().is_power_of_two() {
            return Err(ConfigError::InvalidUserConfig {
                error: format!(
                    "num_partitions must be a power of two, got {}",
                    self.num_partitions
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deserialize_metric_name() {
        let c: Config = serde_json::from_value(json!({
            "key": "metric_name",
            "num_partitions": 16
        }))
        .unwrap();
        assert_eq!(c.key, PartitionKeyConfig::MetricName);
        assert_eq!(c.key.target_signal(), SignalType::Metrics);
        assert_eq!(c.num_partitions.get(), 16);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn deserialize_trace_id() {
        let c: Config = serde_json::from_value(json!({
            "key": "trace_id",
            "num_partitions": 8
        }))
        .unwrap();
        assert_eq!(c.key, PartitionKeyConfig::TraceId);
        assert_eq!(c.key.target_signal(), SignalType::Traces);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_non_power_of_two() {
        let c: Config = serde_json::from_value(json!({
            "key": "metric_name",
            "num_partitions": 12
        }))
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_partitions() {
        // NonZeroUsize rejects 0 at deserialization time.
        let r: Result<Config, _> = serde_json::from_value(json!({
            "key": "metric_name",
            "num_partitions": 0
        }));
        assert!(r.is_err());
    }
}
