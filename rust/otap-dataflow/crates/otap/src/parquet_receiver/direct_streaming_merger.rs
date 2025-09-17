// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct streaming merger for parent-child parquet data
//!
//! This module implements the core streaming merge logic that coordinates
//! logs and log_attrs tables, buffering attributes and creating joined batches
//! with proper parent-child relationships.

use std::collections::{BTreeMap, HashMap};

use arrow::{
    array::{Array, RecordBatch, UInt32Array},
    compute,
};
use uuid::Uuid;

use crate::parquet_receiver::{
    direct_stream_reader::{DirectParquetStreamReader, DirectStreamConfig},
    error::ParquetReceiverError,
};

/// Streaming merger for coordinating parent-child parquet data
pub struct DirectStreamingMerger {
    /// Reader for logs (parent) table
    logs_reader: DirectParquetStreamReader,
    /// Reader for log_attrs (child) table
    log_attrs_reader: Option<DirectParquetStreamReader>,
    /// Reader for resource_attrs table
    resource_attrs_reader: Option<DirectParquetStreamReader>,
    /// Reader for scope_attrs table  
    scope_attrs_reader: Option<DirectParquetStreamReader>,
    /// Buffer for attribute records waiting to be joined
    attribute_buffer: BTreeMap<u32, Vec<RecordBatch>>,
    /// Partition ID being processed
    partition_id: String,
}

/// Result of a streaming merge operation
#[derive(Debug)]
pub struct StreamingMergeBatch {
    /// Primary log records
    pub logs_batch: RecordBatch,
    /// Maximum ID in the logs batch
    pub max_primary_id: u32,
    /// Attribute batches keyed by table name
    pub attribute_batches: HashMap<String, RecordBatch>,
    /// Partition ID
    pub partition_id: Uuid,
}

impl DirectStreamingMerger {
    /// Create a new streaming merger for a specific partition
    pub async fn new(
        config: DirectStreamConfig,
        partition_id: String,
    ) -> Result<Self, ParquetReceiverError> {
        // Create readers for each table (some may not exist)
        let logs_reader = DirectParquetStreamReader::new(
            config.clone(),
            "logs",
            &partition_id,
        ).await?;

        let log_attrs_reader = DirectParquetStreamReader::new(
            config.clone(),
            "log_attrs", 
            &partition_id,
        ).await.ok(); // Optional - may not exist

        let resource_attrs_reader = DirectParquetStreamReader::new(
            config.clone(),
            "resource_attrs",
            &partition_id,
        ).await.ok(); // Optional - may not exist

        let scope_attrs_reader = DirectParquetStreamReader::new(
            config.clone(),
            "scope_attrs",
            &partition_id,
        ).await.ok(); // Optional - may not exist

        log::debug!("🔧 Created streaming merger for partition {}", partition_id);
        log::debug!("   📊 Logs files: {}", logs_reader.file_count());
        log::debug!("   📊 Log attrs files: {}", log_attrs_reader.as_ref().map_or(0, |r| r.file_count()));
        log::debug!("   📊 Resource attrs files: {}", resource_attrs_reader.as_ref().map_or(0, |r| r.file_count()));
        log::debug!("   📊 Scope attrs files: {}", scope_attrs_reader.as_ref().map_or(0, |r| r.file_count()));

        Ok(Self {
            logs_reader,
            log_attrs_reader,
            resource_attrs_reader,
            scope_attrs_reader,
            attribute_buffer: BTreeMap::new(),
            partition_id,
        })
    }

    /// Process next streaming merge batch
    pub async fn read_next_merge_batch(&mut self) -> Result<Option<StreamingMergeBatch>, ParquetReceiverError> {
        // Step 1: Read next batch from logs (primary) table
        let logs_batch = match self.logs_reader.read_next_batch().await? {
            Some(batch) => batch,
            None => return Ok(None), // No more logs to process
        };

        let record_count = logs_batch.num_rows();
        log::debug!("📊 Read primary batch: {} logs", record_count);

        // Step 2: Determine max_id from logs batch
        let max_id = Self::extract_max_id(&logs_batch)?;
        log::debug!("🔍 Max primary ID: {}", max_id);

        // Step 3: Collect attribute records up to max_id
        let mut attribute_batches = HashMap::new();

        // Process log_attrs
        if let Some(ref mut reader) = self.log_attrs_reader {
            if let Some(attrs_batch) = Self::read_attributes_up_to_id_static(reader, max_id, "log_attrs").await? {
                let _ = attribute_batches.insert("log_attrs".to_string(), attrs_batch);
            }
        }

        // Process resource_attrs
        if let Some(ref mut reader) = self.resource_attrs_reader {
            if let Some(attrs_batch) = Self::read_attributes_up_to_id_static(reader, max_id, "resource_attrs").await? {
                let _ = attribute_batches.insert("resource_attrs".to_string(), attrs_batch);
            }
        }

        // Process scope_attrs
        if let Some(ref mut reader) = self.scope_attrs_reader {
            if let Some(attrs_batch) = Self::read_attributes_up_to_id_static(reader, max_id, "scope_attrs").await? {
                let _ = attribute_batches.insert("scope_attrs".to_string(), attrs_batch);
            }
        }

        let partition_uuid = Uuid::parse_str(&self.partition_id)
            .map_err(|e| ParquetReceiverError::Config(format!("Invalid partition UUID: {}", e)))?;

        Ok(Some(StreamingMergeBatch {
            logs_batch,
            max_primary_id: max_id,
            attribute_batches,
            partition_id: partition_uuid,
        }))
    }

    /// Read attribute records up to a specific parent_id threshold
    async fn read_attributes_up_to_id_static(
        reader: &mut DirectParquetStreamReader,
        max_parent_id: u32,
        table_name: &str,
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let mut collected_batches = Vec::new();
        let mut total_records = 0;

        // Read from attribute table until we have all records <= max_parent_id
        // Note: This is simplified - in production we'd need more sophisticated
        // buffering and putback mechanism for partially consumed batches
        while reader.has_more_files() {
            if let Some(batch) = reader.read_next_batch().await? {
                // Filter batch to only include records <= max_parent_id
                let filtered_batch = Self::filter_batch_by_parent_id(&batch, max_parent_id)?;
                
                if filtered_batch.num_rows() > 0 {
                    let batch_rows = filtered_batch.num_rows();
                    total_records += batch_rows;
                    collected_batches.push(filtered_batch);
                    
                    log::debug!("📊 Collected {} {} records (total: {})", 
                        batch_rows, table_name, total_records);
                } else {
                    // If we got an empty filtered batch, we may have gone beyond max_parent_id
                    // TODO: Implement putback mechanism here
                    break;
                }
            } else {
                break; // No more batches from this reader
            }
        }

        if collected_batches.is_empty() {
            return Ok(None);
        }

        // Combine all collected batches
        let combined_batch = if collected_batches.len() == 1 {
            collected_batches.into_iter().next().unwrap()
        } else {
            let schema = collected_batches[0].schema();
            compute::concat_batches(&schema, &collected_batches)
                .map_err(|e| ParquetReceiverError::Arrow(e))?
        };

        log::debug!("✅ Final {} batch: {} records", table_name, combined_batch.num_rows());
        Ok(Some(combined_batch))
    }

    /// Extract maximum ID from a primary (logs) batch
    fn extract_max_id(batch: &RecordBatch) -> Result<u32, ParquetReceiverError> {
        let id_column = batch.column_by_name("id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No 'id' column found in logs batch".to_string()
            ))?;

        let id_array = id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "ID column is not UInt32Array".to_string()
            ))?;

        let mut max_id = 0u32;
        for i in 0..id_array.len() {
            if !id_array.is_null(i) {
                let id = id_array.value(i);
                max_id = max_id.max(id);
            }
        }

        Ok(max_id)
    }

    /// Filter a batch to only include records where parent_id <= max_parent_id
    fn filter_batch_by_parent_id(
        batch: &RecordBatch, 
        max_parent_id: u32
    ) -> Result<RecordBatch, ParquetReceiverError> {
        let parent_id_column = batch.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No 'parent_id' column found in attribute batch".to_string()
            ))?;

        let parent_id_array = parent_id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        // Create boolean mask for filtering
        let mut mask_values = Vec::new();
        for i in 0..parent_id_array.len() {
            let include = if parent_id_array.is_null(i) {
                false // Exclude null parent_ids
            } else {
                parent_id_array.value(i) <= max_parent_id
            };
            mask_values.push(include);
        };

        let mask = arrow::array::BooleanArray::from(mask_values);

        // Filter the batch using the mask
        let filtered_batch = compute::filter_record_batch(batch, &mask)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;

        Ok(filtered_batch)
    }

    /// Check if there are more merge batches to process
    pub fn has_more_batches(&self) -> bool {
        self.logs_reader.has_more_files()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_max_id_empty() {
        // TODO: Add tests for max_id extraction
    }

    #[test] 
    fn test_filter_batch_by_parent_id() {
        // TODO: Add tests for filtering logic
    }
}