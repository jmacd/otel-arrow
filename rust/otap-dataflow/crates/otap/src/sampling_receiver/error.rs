//! Error types for the Sampling Receiver
//!
//! This module defines error types specific to sampling receiver operations
//! including DataFusion query errors, temporal processing errors, and Arrow
//! compute optimization errors.

use thiserror::Error;

/// Errors that can occur in sampling receiver operations
#[derive(Error, Debug)]
pub enum SamplingReceiverError {
    /// DataFusion query execution errors (placeholder for future implementation)
    #[error("DataFusion query error: {message}")]
    DataFusionError { 
        /// Error message
        message: String 
    },

    /// Arrow compute errors during optimization
    #[error("Arrow compute error: {source}")]
    ArrowError {
        #[from]
        /// Arrow error source
        source: arrow::error::ArrowError,
    },

    /// File system and object store errors
    #[error("Object store error: {source}")]
    ObjectStoreError {
        #[from]
        /// Object store error source
        source: object_store::Error,
    },

    /// Configuration validation errors
    #[error("Configuration error: {message}")]
    ConfigError { 
        /// Error message
        message: String 
    },

    /// Temporal window processing errors
    #[error("Temporal window error: {message}")]
    TemporalWindowError { 
        /// Error message
        message: String 
    },

    /// SQL query parsing or validation errors
    #[error("Query error: {message}")]
    QueryError { 
        /// Error message
        message: String 
    },

    /// Sampling operation errors
    #[error("Sampling error: {message}")]
    SamplingError { 
        /// Error message
        message: String 
    },

    /// File discovery errors
    #[error("File discovery error: {message}")]
    FileDiscoveryError { 
        /// Error message
        message: String 
    },

    /// Arrow schema compatibility errors
    #[error("Schema compatibility error: {message}")]
    SchemaError { 
        /// Error message
        message: String 
    },

    /// Memory management errors
    #[error("Memory management error: {message}")]
    MemoryError { 
        /// Error message
        message: String 
    },

    /// Generic I/O errors
    #[error("I/O error: {source}")]
    IoError {
        #[from]
        /// I/O error source
        source: std::io::Error,
    },

    /// Serialization/deserialization errors
    #[error("Serialization error: {source}")]
    SerializationError {
        #[from]
        /// Serialization error source
        source: serde_json::Error,
    },
}

/// Result type alias for sampling receiver operations
pub type Result<T> = std::result::Result<T, SamplingReceiverError>;

impl SamplingReceiverError {
    /// Create a configuration error with a message
    pub fn config_error(message: impl Into<String>) -> Self {
        Self::ConfigError {
            message: message.into(),
        }
    }

    /// Create a DataFusion error with a message  
    pub fn datafusion_error(message: impl Into<String>) -> Self {
        Self::DataFusionError {
            message: message.into(),
        }
    }

    /// Create a temporal window error with a message
    pub fn temporal_window_error(message: impl Into<String>) -> Self {
        Self::TemporalWindowError {
            message: message.into(),
        }
    }

    /// Create a query error with a message
    pub fn query_error(message: impl Into<String>) -> Self {
        Self::QueryError {
            message: message.into(),
        }
    }

    /// Create a sampling error with a message
    pub fn sampling_error(message: impl Into<String>) -> Self {
        Self::SamplingError {
            message: message.into(),
        }
    }

    /// Create a file discovery error with a message
    pub fn file_discovery_error(message: impl Into<String>) -> Self {
        Self::FileDiscoveryError {
            message: message.into(),
        }
    }

    /// Create a schema error with a message
    pub fn schema_error(message: impl Into<String>) -> Self {
        Self::SchemaError {
            message: message.into(),
        }
    }

    /// Create a memory error with a message
    pub fn memory_error(message: impl Into<String>) -> Self {
        Self::MemoryError {
            message: message.into(),
        }
    }
}

// Manual From implementation for DataFusionError
impl From<datafusion::error::DataFusionError> for SamplingReceiverError {
    fn from(error: datafusion::error::DataFusionError) -> Self {
        Self::DataFusionError {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let config_err = SamplingReceiverError::config_error("Invalid query");
        assert!(config_err.to_string().contains("Configuration error"));

        let temporal_err = SamplingReceiverError::temporal_window_error("Window too large");
        assert!(temporal_err.to_string().contains("Temporal window error"));
    }

    #[test]
    fn test_error_conversion() {
        let arrow_err = arrow::error::ArrowError::InvalidArgumentError("test".to_string());
        let sampling_err: SamplingReceiverError = arrow_err.into();
        assert!(matches!(sampling_err, SamplingReceiverError::ArrowError { .. }));
    }
}