// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types for the Parquet Receiver

use thiserror::Error;

/// Errors that can occur in the parquet receiver
#[derive(Error, Debug)]
pub enum ParquetReceiverError {
    /// File system I/O error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    /// Arrow processing error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    
    /// Parquet file processing error
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    
    /// Configuration validation error
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// File discovery and scanning error
    #[error("File discovery error: {0}")]
    FileDiscovery(String),
    
    /// Schema reconstruction and validation error
    #[error("Schema reconstruction error: {0}")]
    SchemaReconstruction(String),
    
    /// Required parquet file is missing
    #[error("Missing required file: {0}")]
    MissingFile(String),
    
    /// Invalid partition directory or UUID
    #[error("Invalid partition: {0}")]
    InvalidPartition(String),
    
    /// OTAP data reconstruction error
    #[error("Reconstruction error: {0}")]
    Reconstruction(String),
}