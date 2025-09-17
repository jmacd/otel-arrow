// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct object store streaming reader for Parquet files
//!
//! This module provides the core DirectParquetStreamReader that reads Parquet files
//! directly from object store in timestamp order, bypassing DataFusion complexity.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::{local::LocalFileSystem, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::fs;

use crate::parquet_receiver::error::ParquetReceiverError;

/// Configuration for the direct stream reader
#[derive(Debug, Clone)]
pub struct DirectStreamConfig {
    /// Base directory containing parquet files
    pub base_directory: PathBuf,
    /// Batch size for reading records
    pub batch_size: usize,
}

impl Default for DirectStreamConfig {
    fn default() -> Self {
        Self {
            base_directory: PathBuf::from("output_parquet_files"),
            batch_size: 65536, // 2^16 for UInt16 ID space
        }
    }
}

/// Direct Parquet stream reader that bypasses DataFusion
pub struct DirectParquetStreamReader {
    /// Object store for reading files
    object_store: Arc<dyn ObjectStore>,
    /// Configuration
    config: DirectStreamConfig,
    /// Current position in the stream
    current_file_index: usize,
    /// Discovered parquet files in timestamp order
    files: Vec<PathBuf>,
}

impl DirectParquetStreamReader {
    /// Create a new direct parquet stream reader
    pub async fn new(
        config: DirectStreamConfig,
        table_name: &str,
        partition_id: &str,
    ) -> Result<Self, ParquetReceiverError> {
        // Use local filesystem object store for now
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(&config.base_directory)
                .map_err(|e| ParquetReceiverError::Config(format!("Failed to create object store: {}", e)))?
        );

        // Discover files for this table and partition
        let partition_dir = config.base_directory
            .join(table_name)
            .join(format!("_part_id={}", partition_id));

        let files = Self::discover_files(&partition_dir).await?;

        Ok(Self {
            object_store,
            config,
            current_file_index: 0,
            files,
        })
    }

    /// Discover parquet files in timestamp order
    async fn discover_files(partition_dir: &PathBuf) -> Result<Vec<PathBuf>, ParquetReceiverError> {
        if !partition_dir.exists() {
            return Ok(vec![]); // No files for this partition
        }

        let mut files = Vec::new();
        let mut entries = fs::read_dir(partition_dir).await
            .map_err(|e| ParquetReceiverError::Io(e))?;

        while let Some(entry) = entries.next_entry().await
            .map_err(|e| ParquetReceiverError::Io(e))? {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "parquet") {
                files.push(path);
            }
        }

        // Sort files by name (which contains timestamp) for consistent ordering
        files.sort();

        log::debug!("📁 Discovered {} parquet files in {}", files.len(), partition_dir.display());
        for file in &files {
            log::debug!("   📄 {}", file.display());
        }

        Ok(files)
    }

    /// Read next batch of records from the stream
    pub async fn read_next_batch(&mut self) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        while self.current_file_index < self.files.len() {
            let file_path = &self.files[self.current_file_index];
            
            log::debug!("📖 Reading from file: {}", file_path.display());
            
            // Read parquet file directly using arrow-rs
            let file = std::fs::File::open(file_path)
                .map_err(|e| ParquetReceiverError::Io(e))?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| ParquetReceiverError::Parquet(e))?;

            let reader = builder
                .with_batch_size(self.config.batch_size)
                .build()
                .map_err(|e| ParquetReceiverError::Parquet(e))?;

            // For now, read all batches from the file and concatenate
            let mut batches = Vec::new();
            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| ParquetReceiverError::Arrow(e))?;
                batches.push(batch);
            }

            if !batches.is_empty() {
                // Combine batches if multiple exist
                let combined_batch = if batches.len() == 1 {
                    batches.into_iter().next().unwrap()
                } else {
                    let schema = batches[0].schema();
                    arrow::compute::concat_batches(&schema, &batches)
                        .map_err(|e| ParquetReceiverError::Arrow(e))?
                };

                log::debug!("✅ Read {} records from {}", combined_batch.num_rows(), file_path.display());

                // Move to next file for next call
                self.current_file_index += 1;

                return Ok(Some(combined_batch));
            }

            // File was empty, move to next
            self.current_file_index += 1;
        }

        // No more files
        Ok(None)
    }

    /// Check if there are more files to read
    pub fn has_more_files(&self) -> bool {
        self.current_file_index < self.files.len()
    }

    /// Get the total number of files discovered
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_direct_stream_config_default() {
        let config = DirectStreamConfig::default();
        assert_eq!(config.batch_size, 65536);
    }

    #[tokio::test]
    async fn test_discover_files_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let partition_dir = temp_dir.path().join("logs").join("_part_id=test");
        
        let files = DirectParquetStreamReader::discover_files(&partition_dir).await.unwrap();
        assert!(files.is_empty());
    }
}