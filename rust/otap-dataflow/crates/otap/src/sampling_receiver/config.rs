//! Configuration structures for the Sampling Receiver
//!
//! This module defines the configuration schema for the sampling receiver,
//! supporting flexible DataFusion queries, temporal processing, and performance tuning.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use url::Url;

/// Configuration for the Sampling Receiver
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Base URI for parquet files (can be local path or object store URI)
    pub base_uri: Url,
    
    /// Signal types to process
    pub signal_types: Vec<SignalType>,
    
    /// Temporal processing configuration
    pub temporal: TemporalConfig,
    
    /// The SQL query to execute for data processing
    pub query: String,
    
    /// Performance and optimization settings
    #[serde(default)]
    pub performance: PerformanceConfig,
    
    /// Sampling-specific configuration (optional)
    pub sampling: Option<SamplingConfig>,
}

/// Signal types supported by the sampling receiver
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SignalType {
    /// Log signals
    Logs,
    /// Trace signals (future support)
    Traces,
    /// Metric signals (future support)
    Metrics,
}

/// Temporal processing configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TemporalConfig {
    /// Window granularity for time-aligned processing
    /// Examples: "1m", "5m", "1h"
    #[serde(with = "humantime_serde")]
    pub window_granularity: Duration,
    
    /// Processing delay - how long to wait before processing a time window
    /// This provides a safety margin for late-arriving data
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    
    /// Maximum expected clock drift between systems
    #[serde(with = "humantime_serde")]
    pub max_clock_drift: Duration,
    
    /// Maximum expected duration for a single parquet file
    #[serde(with = "humantime_serde")]
    pub max_file_duration: Duration,
}

/// Performance and optimization configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    /// Batch size for processing operations
    pub batch_size: usize,
    
    /// Whether to enable Arrow compute optimizations
    pub enable_arrow_optimization: bool,
    
    /// Memory limit for DataFusion operations
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
    
    /// Number of concurrent files to process
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    
    /// Target partition count for DataFusion
    #[serde(default = "default_target_partitions")]
    pub target_partitions: usize,
}

/// Sampling-specific configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SamplingConfig {
    /// Sample size (K parameter for reservoir sampling)
    pub sample_size: usize,
    
    /// Whether to preserve and generate sampling.adjusted_count attributes
    #[serde(default = "default_preserve_weights")]
    pub preserve_weights: bool,
    
    /// Sampling strategy
    #[serde(default)]
    pub strategy: SamplingStrategy,
}

/// Available sampling strategies
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SamplingStrategy {
    /// Weighted reservoir sampling
    Weighted,
    /// Uniform sampling
    Uniform,
    /// Stratified sampling
    Stratified,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            enable_arrow_optimization: true,
            memory_limit: default_memory_limit(),
            max_concurrent_files: default_max_concurrent_files(),
            target_partitions: default_target_partitions(),
        }
    }
}

impl Default for SamplingStrategy {
    fn default() -> Self {
        Self::Weighted
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_uri: Url::parse("file:///tmp/parquet_files").unwrap(),
            signal_types: vec![SignalType::Logs],
            temporal: TemporalConfig {
                window_granularity: Duration::from_secs(60), // 1 minute
                processing_delay: Duration::from_secs(600),   // 10 minutes
                max_clock_drift: Duration::from_secs(60),     // 1 minute
                max_file_duration: Duration::from_secs(60),   // 1 minute
            },
            query: default_passthrough_query().to_string(),
            performance: PerformanceConfig::default(),
            sampling: None,
        }
    }
}

/// Default pass-through query that processes all log_attributes
pub fn default_passthrough_query() -> &'static str {
    r#"
    SELECT *
    FROM log_attributes
    WHERE timestamp_unix_nano >= {window_start_ns}
      AND timestamp_unix_nano < {window_end_ns}
    ORDER BY _part_id, parent_id, key
    "#
}

/// Default sampling query template with weighted reservoir sampling
pub fn default_sampling_query() -> &'static str {
    r#"
    WITH service_weights AS (
        SELECT 
            l.id, l._part_id,
            ra.str as service_name,
            COALESCE(CAST(la.str AS DOUBLE), 1.0) as input_weight
        FROM logs l
        JOIN resource_attributes ra ON ra.parent_id = l.id AND ra._part_id = l._part_id AND ra.key = 'service.name'
        LEFT JOIN log_attributes la ON la.parent_id = l.id AND la._part_id = l._part_id AND la.key = 'sampling.adjusted_count'
        WHERE l.timestamp_unix_nano >= {window_start_ns} AND l.timestamp_unix_nano < {window_end_ns}
    ),
    sampling_decisions AS (
        SELECT service_name, _part_id,
               weighted_reservoir_sample(STRUCT(id, input_weight), {sample_size}) AS sample_decisions
        FROM service_weights GROUP BY service_name, _part_id
    )
    SELECT la.*
    FROM log_attributes la
    JOIN (
        SELECT UNNEST(sample_decisions.sampled_ids) as sampled_id, _part_id
        FROM sampling_decisions
    ) sampled ON sampled.sampled_id = la.parent_id AND sampled._part_id = la._part_id
    ORDER BY la._part_id, la.parent_id, la.key
    "#
}

fn default_memory_limit() -> String {
    "1GB".to_string()
}

fn default_max_concurrent_files() -> usize {
    10
}

fn default_target_partitions() -> usize {
    8 // Default to 8 partitions
}

fn default_preserve_weights() -> bool {
    true
}

impl Config {
    /// Validate the configuration and return any errors
    pub fn validate(&self) -> Result<(), crate::sampling_receiver::error::SamplingReceiverError> {
        // Validate temporal configuration
        if self.temporal.window_granularity.is_zero() {
            return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                "window_granularity must be greater than zero"
            ));
        }

        if self.temporal.processing_delay < self.temporal.max_clock_drift {
            return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                "processing_delay should be greater than max_clock_drift"
            ));
        }

        // Validate query is not empty
        if self.query.trim().is_empty() {
            return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                "query cannot be empty"
            ));
        }

        // Validate performance configuration
        if self.performance.batch_size == 0 {
            return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                "batch_size must be greater than zero"
            ));
        }

        if self.performance.max_concurrent_files == 0 {
            return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                "max_concurrent_files must be greater than zero"
            ));
        }

        // Validate sampling configuration if present
        if let Some(ref sampling) = self.sampling {
            if sampling.sample_size == 0 {
                return Err(crate::sampling_receiver::error::SamplingReceiverError::config_error(
                    "sample_size must be greater than zero"
                ));
            }
        }

        Ok(())
    }

    /// Get the base path as a PathBuf (if it's a file:// URL)
    pub fn base_path(&self) -> Option<PathBuf> {
        if self.base_uri.scheme() == "file" {
            self.base_uri.to_file_path().ok()
        } else {
            None
        }
    }

    /// Check if this configuration uses sampling
    pub fn is_sampling_enabled(&self) -> bool {
        self.sampling.is_some()
    }

    /// Get the query with window parameters substituted
    pub fn get_query_with_window(&self, window_start_ns: i64, window_end_ns: i64) -> String {
        let mut query = self.query.clone();
        query = query.replace("{window_start_ns}", &window_start_ns.to_string());
        query = query.replace("{window_end_ns}", &window_end_ns.to_string());
        
        // If sampling is enabled, also substitute sample_size
        if let Some(ref sampling) = self.sampling {
            query = query.replace("{sample_size}", &sampling.sample_size.to_string());
        }
        
        query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Test invalid window granularity
        config.temporal.window_granularity = Duration::from_secs(0);
        assert!(config.validate().is_err());

        config.temporal.window_granularity = Duration::from_secs(60);
        assert!(config.validate().is_ok());

        // Test invalid processing delay
        config.temporal.processing_delay = Duration::from_secs(30);
        config.temporal.max_clock_drift = Duration::from_secs(60);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_query_substitution() {
        let config = Config::default();
        let query = config.get_query_with_window(1000000, 2000000);
        
        assert!(query.contains("1000000"));
        assert!(query.contains("2000000"));
        assert!(!query.contains("{window_start_ns}"));
        assert!(!query.contains("{window_end_ns}"));
    }

    #[test]
    fn test_sampling_config() {
        let mut config = Config::default();
        config.sampling = Some(SamplingConfig {
            sample_size: 100,
            preserve_weights: true,
            strategy: SamplingStrategy::Weighted,
        });
        
        assert!(config.is_sampling_enabled());
        assert!(config.validate().is_ok());
        
        // Test invalid sample size
        config.sampling.as_mut().unwrap().sample_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_base_path() {
        let mut config = Config::default();
        config.base_uri = Url::parse("file:///tmp/test").unwrap();
        assert!(config.base_path().is_some());
        
        config.base_uri = Url::parse("s3://bucket/prefix").unwrap();
        assert!(config.base_path().is_none());
    }
}