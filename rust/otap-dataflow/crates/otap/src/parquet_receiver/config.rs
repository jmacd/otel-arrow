// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration structures for the Parquet Receiver

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the Parquet Receiver
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Base URI to the parquet files directory (e.g., "/tmp/output_parquet_files")
    pub base_uri: String,
    
    /// Signal types to process (logs, traces, metrics)
    #[serde(default = "default_signal_types")]
    pub signal_types: Vec<SignalType>,
    
    /// How often to scan for new files
    #[serde(default = "default_polling_interval")]
    pub polling_interval: Duration,
    
    /// Optional processing options
    #[serde(default)]
    pub processing_options: ProcessingOptions,
}

/// Signal types supported by the parquet receiver
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SignalType {
    /// OpenTelemetry logs signal
    Logs,
    /// OpenTelemetry traces signal
    Traces, 
    /// OpenTelemetry metrics signal
    Metrics,
}

/// Processing options for the parquet receiver
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingOptions {
    /// Maximum batch size for reconstructed data
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    
    /// Minimum file age before processing (helps avoid processing incomplete files)
    #[serde(default)]
    pub min_file_age: Option<Duration>,
    
    /// Whether to validate that related files exist (for debugging)
    #[serde(default)]
    pub validate_relations: bool,
}

impl Default for ProcessingOptions {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            min_file_age: None,
            validate_relations: false,
        }
    }
}

fn default_signal_types() -> Vec<SignalType> {
    vec![SignalType::Logs]
}

fn default_polling_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_batch_size() -> usize {
    1000
}

impl Config {
    /// Get the base path as a PathBuf
    pub fn base_path(&self) -> PathBuf {
        PathBuf::from(&self.base_uri)
    }
    
    /// Check if logs processing is enabled
    pub fn processes_logs(&self) -> bool {
        self.signal_types.contains(&SignalType::Logs)
    }
    
    /// Check if traces processing is enabled
    pub fn processes_traces(&self) -> bool {
        self.signal_types.contains(&SignalType::Traces)
    }
    
    /// Check if metrics processing is enabled
    pub fn processes_metrics(&self) -> bool {
        self.signal_types.contains(&SignalType::Metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_config_deserialization() {
        let config_json = r#"
        {
            "base_uri": "/tmp/output_parquet_files",
            "signal_types": ["logs", "traces"],
            "polling_interval": "10s"
        }
        "#;

        let config: Config = serde_json::from_str(config_json).unwrap();
        assert_eq!(config.base_uri, "/tmp/output_parquet_files");
        assert_eq!(config.signal_types.len(), 2);
        assert!(config.processes_logs());
        assert!(config.processes_traces());
        assert!(!config.processes_metrics());
        assert_eq!(config.polling_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_default_config() {
        let config_json = r#"
        {
            "base_uri": "/tmp/test"
        }
        "#;

        let config: Config = serde_json::from_str(config_json).unwrap();
        assert_eq!(config.signal_types, vec![SignalType::Logs]);
        assert_eq!(config.polling_interval, Duration::from_secs(5));
        assert_eq!(config.processing_options.batch_size, 1000);
        assert!(!config.processing_options.validate_relations);
    }
}