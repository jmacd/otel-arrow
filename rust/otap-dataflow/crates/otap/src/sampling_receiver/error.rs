//! Error types for the Sampling Receiver
//!
//! This module defines error types specific to sampling receiver operations
//! including DataFusion query errors, temporal processing errors, and Arrow
//! compute optimization errors.

use thiserror::Error;

/// Errors that can occur in sampling receiver operations
#[derive(Error, Debug)]
pub enum SamplingReceiverError {
    /// DataFusion query execution errors
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

    /// Configuration errors  
    #[error("Configuration error: {message}")]
    ConfigError {
        /// Error message
        message: String
    },

    /// Temporal processing errors
    #[error("Temporal processing error: {message}")]
    TemporalError {
        /// Error message
        message: String
    },

    /// Query template errors
    #[error("Query template error: {message}")]
    QueryTemplateError {
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

    /// Memory management errors
    #[error("Memory management error: {message}")]
    MemoryError { 
        /// Error message
        message: String 
    },

    /// OTAP record reconstruction errors
    #[error("OTAP reconstruction error: {message}")]
    ReconstructionError { 
        /// Error message
        message: String 
    },
}

/// Result type for sampling receiver operations
pub type Result<T> = std::result::Result<T, SamplingReceiverError>;

impl SamplingReceiverError {
    /// Create a DataFusion error
    pub fn datafusion_error<S: Into<String>>(message: S) -> Self {
        Self::DataFusionError { message: message.into() }
    }

    /// Create a configuration error
    pub fn config_error<S: Into<String>>(message: S) -> Self {
        Self::ConfigError { message: message.into() }
    }

    /// Create a temporal processing error
    pub fn temporal_error<S: Into<String>>(message: S) -> Self {
        Self::TemporalError { message: message.into() }
    }

    /// Create a query template error
    pub fn query_template_error<S: Into<String>>(message: S) -> Self {
        Self::QueryTemplateError { message: message.into() }
    }

    /// Create a memory management error
    pub fn memory_error<S: Into<String>>(message: S) -> Self {
        Self::MemoryError { message: message.into() }
    }

    /// Create an OTAP reconstruction error
    pub fn reconstruction_error<S: Into<String>>(message: S) -> Self {
        Self::ReconstructionError { message: message.into() }
    }
}

// Conversion from DataFusion error
impl From<datafusion::error::DataFusionError> for SamplingReceiverError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Self::DataFusionError {
            message: err.to_string(),
        }
    }
}

// Conversion from UUID parsing error
impl From<uuid::Error> for SamplingReceiverError {
    fn from(err: uuid::Error) -> Self {
        Self::ConfigError {
            message: format!("UUID error: {}", err),
        }
    }
}

// Conversion from serde JSON error
impl From<serde_json::Error> for SamplingReceiverError {
    fn from(err: serde_json::Error) -> Self {
        Self::ConfigError {
            message: format!("JSON error: {}", err),
        }
    }
}