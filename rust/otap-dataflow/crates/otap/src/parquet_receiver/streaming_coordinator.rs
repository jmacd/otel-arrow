// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Streaming coordinator for multi-array parquet reconstruction
//!
//! This module implements the streaming approach for reading coordinated parquet files:
//! 1. Read primary table (logs) in batches up to 65536 records  
//! 2. Determine max_id from the primary batch
//! 3. Scan each child table (log_attrs, resource_attrs, scope_attrs) to find records <= max_id
//! 4. Construct OTAP batch with UInt16 ID space mapping
//!
//! The streaming approach ensures we process related records together while maintaining
//! memory efficiency and correct parent-child relationships.

use crate::parquet_receiver::{
    config::SignalType,
    error::ParquetReceiverError,
    id_mapping::IdMapper,
};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, UInt32Array};
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::context::SessionContext,
};
use otel_arrow_rust::otap::{Logs, OtapArrowRecords, OtapBatchStore};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// Configuration for streaming coordinator
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Base directory containing parquet files
    pub base_directory: PathBuf,
    /// Maximum number of primary records per batch (up to 2^16)
    pub primary_batch_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            base_directory: PathBuf::from("output_parquet_files"),
            primary_batch_size: 65536, // 2^16
        }
    }
}

/// Maintains reading position for each parquet stream
#[derive(Debug, Clone)]
struct StreamPosition {
    /// Current row offset in the stream
    row_offset: usize,
    /// Whether we've reached the end of this stream
    is_exhausted: bool,
}

/// Streaming coordinator that processes multiple related parquet streams
pub struct StreamingCoordinator {
    config: StreamingConfig,
    /// Position tracking for each stream
    stream_positions: HashMap<String, StreamPosition>,
    /// DataFusion session context
    session_ctx: SessionContext,
}

/// Result from a streaming batch read
#[derive(Debug)]
pub struct StreamingBatch {
    /// Primary records (logs)
    pub primary_batch: RecordBatch,
    /// Maximum ID found in primary batch - used for child collection
    pub max_primary_id: u32,
    /// Related child records keyed by table name
    pub child_batches: HashMap<String, RecordBatch>,
    /// Partition ID this batch belongs to
    pub partition_id: Uuid,
}

impl StreamingCoordinator {
    /// Create a new streaming coordinator
    pub fn new(config: StreamingConfig) -> Self {
        Self {
            config,
            stream_positions: HashMap::new(),
            session_ctx: SessionContext::new(),
        }
    }

    /// Process a partition using streaming approach
    /// Returns iterator-like interface for processing batches
    pub async fn process_partition(
        &mut self,
        partition_id: &Uuid,
        signal_type: &SignalType,
    ) -> Result<Vec<StreamingBatch>, ParquetReceiverError> {
        // Reset positions for new partition
        self.reset_positions();
        
        // Only support logs for now (as specified)
        if *signal_type != SignalType::Logs {
            return Err(ParquetReceiverError::Config(format!(
                "Signal type {:?} not supported in streaming coordinator", signal_type
            )));
        }

        // Set up table mappings for logs
        let table_mappings = vec![
            ("logs", true),           // Primary table
            ("log_attrs", false),     // Child table
            // TODO: Add resource_attrs and scope_attrs when available
        ];

        // Register tables for this partition
        self.register_partition_tables(partition_id, &table_mappings).await?;

        let mut batches = Vec::new();
        
        // Stream processing loop - implement your pseudocode here
        loop {
            // Step 1: Read primary batch up to 65536 records
            let primary_batch = self.read_primary_batch("logs").await?;
            
            if primary_batch.is_none() {
                break; // No more primary records
            }
            
            let primary_batch = primary_batch.unwrap();
            let record_count = primary_batch.num_rows();
            log::debug!("📊 Read primary batch: {} records", record_count);
            
            // Step 2: Determine max_id from primary batch
            let max_id = self.get_max_id_from_batch(&primary_batch)?;
            log::debug!("🔍 Max primary ID in batch: {}", max_id);
            
            // Step 3: Collect child records up to max_id
            let mut child_batches = HashMap::new();
            
            for (table_name, is_main) in &table_mappings {
                if !is_main {
                    let child_batch = self.read_child_records_up_to_id(table_name, max_id).await?;
                    if let Some(batch) = child_batch {
                        let child_count = batch.num_rows();
                        log::debug!("📊 Read {} child records from {}", child_count, table_name);
                        let _ = child_batches.insert(table_name.to_string(), batch);
                    }
                }
            }
            
            batches.push(StreamingBatch {
                primary_batch,
                max_primary_id: max_id,
                child_batches,
                partition_id: *partition_id,
            });
        }

        Ok(batches)
    }

    /// Convert streaming batch to OTAP records with proper UInt16 ID mapping
    pub fn batch_to_otap(&self, streaming_batch: StreamingBatch) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut logs = Logs::default();
        let mut id_mapper = IdMapper::new();
        
        // Transform primary batch: UInt32 ID -> UInt16 ID
        let transformed_primary = id_mapper.transform_primary_batch(&streaming_batch.primary_batch)?;
        logs.set(ArrowPayloadType::Logs, transformed_primary);
        
        // Transform child batches: UInt32 parent_id -> UInt16 parent_id
        for (table_name, child_batch) in streaming_batch.child_batches {
            let payload_type = self.table_name_to_payload_type(&table_name)?;
            let transformed_child = id_mapper.transform_child_batch(&child_batch)?;
            logs.set(payload_type, transformed_child);
        }

        log::debug!("🔄 ID mapping complete: {} IDs mapped", id_mapper.mapping_count());

        Ok(OtapArrowRecords::Logs(logs))
    }

    /// Reset stream positions for new partition
    fn reset_positions(&mut self) {
        self.stream_positions.clear();
    }

    /// Register DataFusion tables for a partition
    async fn register_partition_tables(
        &mut self,
        partition_id: &Uuid,
        table_mappings: &[(& str, bool)],
    ) -> Result<(), ParquetReceiverError> {
        for (table_name, _is_main) in table_mappings {
            let partition_dir = self.config.base_directory
                .join(table_name)
                .join(format!("_part_id={}", partition_id));

            if !partition_dir.exists() {
                log::debug!("⚠️ Partition directory missing: {} (skipping)", partition_dir.display());
                continue;
            }

            let table_url = ListingTableUrl::parse(&format!("file://{}", partition_dir.display()))
                .map_err(|e| ParquetReceiverError::Config(format!("Invalid table URL: {}", e)))?;

            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension("parquet");

            let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

            let config_with_schema = config
                .infer_schema(&self.session_ctx.state())
                .await
                .map_err(|e| ParquetReceiverError::DataFusion(e))?;

            let listing_table = ListingTable::try_new(config_with_schema)
                .map_err(|e| ParquetReceiverError::DataFusion(e))?;

            let _ = self.session_ctx.register_table(*table_name, Arc::new(listing_table));
        }

        Ok(())
    }

    /// Read next batch from primary table
    async fn read_primary_batch(&mut self, table_name: &str) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let position = self.stream_positions.get(table_name).cloned().unwrap_or(StreamPosition {
            row_offset: 0,
            is_exhausted: false,
        });

        if position.is_exhausted {
            return Ok(None);
        }

        // Build query with LIMIT and OFFSET
        let query = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            table_name, self.config.primary_batch_size, position.row_offset
        );

        let df = self.session_ctx.sql(&query).await
            .map_err(|e| ParquetReceiverError::DataFusion(e))?;

        let batches = df.collect().await
            .map_err(|e| ParquetReceiverError::DataFusion(e))?;

        // Update position
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let new_position = StreamPosition {
            row_offset: position.row_offset + total_rows,
            is_exhausted: total_rows == 0,
        };
        let _ = self.stream_positions.insert(table_name.to_string(), new_position);

        if batches.is_empty() {
            Ok(None)
        } else {
            // Combine multiple batches into one if needed
            if batches.len() == 1 {
                Ok(Some(batches.into_iter().next().unwrap()))
            } else {
                let schema = batches[0].schema();
                let combined = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| ParquetReceiverError::Arrow(e))?;
                Ok(Some(combined))
            }
        }
    }

    /// Read child records up to max_id
    async fn read_child_records_up_to_id(
        &mut self, 
        table_name: &str, 
        max_id: u32
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let position = self.stream_positions.get(table_name).cloned().unwrap_or(StreamPosition {
            row_offset: 0,
            is_exhausted: false,
        });

        if position.is_exhausted {
            return Ok(None);
        }

        // Query child records where parent_id <= max_id, starting from current offset
        // This assumes records are sorted by parent_id for efficient streaming
        let query = format!(
            "SELECT * FROM {} WHERE parent_id <= {} OFFSET {}",
            table_name, max_id, position.row_offset
        );

        let df = self.session_ctx.sql(&query).await
            .map_err(|e| ParquetReceiverError::DataFusion(e))?;

        let batches = df.collect().await
            .map_err(|e| ParquetReceiverError::DataFusion(e))?;

        // Update position (simplified - we'll need more sophisticated tracking)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let new_position = StreamPosition {
            row_offset: position.row_offset + total_rows,
            is_exhausted: total_rows == 0,
        };
        let _ = self.stream_positions.insert(table_name.to_string(), new_position);

        if batches.is_empty() {
            Ok(None)
        } else {
            if batches.len() == 1 {
                Ok(Some(batches.into_iter().next().unwrap()))
            } else {
                let schema = batches[0].schema();
                let combined = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| ParquetReceiverError::Arrow(e))?;
                Ok(Some(combined))
            }
        }
    }

    /// Extract maximum ID from a primary batch
    fn get_max_id_from_batch(&self, batch: &RecordBatch) -> Result<u32, ParquetReceiverError> {
        let id_column = batch.column(0); // Assuming 'id' is first column
        
        let id_array = id_column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "ID column is not UInt32Array".to_string()
            ))?;

        let max_id = id_array.iter()
            .flatten()
            .max()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No valid IDs found in batch".to_string()
            ))?;

        Ok(max_id)
    }

    /// Convert table name to ArrowPayloadType
    fn table_name_to_payload_type(&self, table_name: &str) -> Result<ArrowPayloadType, ParquetReceiverError> {
        let payload_type = match table_name {
            "logs" => ArrowPayloadType::Logs,
            "log_attrs" => ArrowPayloadType::LogAttrs,
            "resource_attrs" => ArrowPayloadType::ResourceAttrs,
            "scope_attrs" => ArrowPayloadType::ScopeAttrs,
            _ => return Err(ParquetReceiverError::Reconstruction(format!(
                "Unknown table name: {}", table_name
            ))),
        };
        Ok(payload_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_config_default() {
        let config = StreamingConfig::default();
        assert_eq!(config.primary_batch_size, 65536);
    }

    #[test] 
    fn test_coordinator_creation() {
        let config = StreamingConfig::default();
        let _coordinator = StreamingCoordinator::new(config);
    }
}