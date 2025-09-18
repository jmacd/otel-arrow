// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct object store streaming coordinator for multi-table parquet reconstruction

use crate::parquet_receiver::{
    config::{Config, SignalType},
    direct_streaming_merger::DirectStreamingMerger,
    direct_stream_reader::DirectStreamConfig,
    error::ParquetReceiverError,
    id_mapping::IdMapper,
};
use arrow::array::UInt32Array;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use otel_arrow_rust::otap::{Logs, OtapArrowRecords, OtapBatchStore};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;
use tokio::fs;

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
            primary_batch_size: 65536,
        }
    }
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

/// Streaming coordinator using direct object store access
pub struct StreamingCoordinator {
    config: StreamingConfig,
}

impl StreamingCoordinator {
    /// Create a new streaming coordinator
    pub fn new(config: StreamingConfig) -> Self {
        Self { config }
    }

    /// Process all partitions with object store (replaces the old DataFusion method)
    pub async fn process_partition_with_object_store(
        &mut self,
        signal_type: &SignalType,
    ) -> Result<Vec<StreamingBatch>, ParquetReceiverError> {
        log::error!("🚀 DIRECT STREAMING COORDINATOR CALLED! Signal type: {:?}", signal_type);
        log::error!("   Base directory: {}", self.config.base_directory.display());
        log::error!("   Batch size: {}", self.config.primary_batch_size);
        
        let result = self.process_all_partitions(signal_type).await?;
        log::error!("🎯 DIRECT STREAMING COORDINATOR RETURNING {} batches", result.len());
        Ok(result)
    }

    /// Convert streaming batch to OTAP records with proper UInt16 ID mapping using normalization
    pub fn batch_to_otap(&self, streaming_batch: StreamingBatch) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut logs = Logs::default();
        
        // Determine batch normalization parameters from the primary batch
        let (max_id, min_id) = Self::analyze_id_range(&streaming_batch.primary_batch)?;
        log::error!("🎯 Batch ID range: {} to {} (will normalize to 0-{})", min_id, max_id, max_id - min_id);
        
        // Set up ID mapper for batch normalization
        let mut id_mapper = IdMapper::new();
        id_mapper.set_batch_start(min_id);
        
        log::error!("🔎 OTAP BATCH CONSTRUCTION (Direct Streaming) - COMPREHENSIVE DIAGNOSTIC:");
        log::error!("   Primary batch: {} rows", streaming_batch.primary_batch.num_rows());
        log::error!("   Child batches: {} tables - {:?}", streaming_batch.child_batches.len(), streaming_batch.child_batches.keys().collect::<Vec<_>>());
        log::error!("   Batch normalization: subtracting {} from all IDs", min_id);
        
        // Log detailed analysis of what attributes we have
        let mut total_attribute_records = 0;
        for (table_name, child_batch) in &streaming_batch.child_batches {
            let rows = child_batch.num_rows();
            total_attribute_records += rows;
            log::error!("   {} table: {} attribute records", table_name, rows);
            
            // Log sample parent_ids from attributes BEFORE transformation
            if rows > 0 {
                if let Some(parent_id_col) = child_batch.column_by_name("parent_id") {
                    if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<UInt32Array>() {
                        let mut sample_parent_ids = Vec::new();
                        for i in 0..rows.min(10) {
                            if !parent_id_array.is_null(i) {
                                sample_parent_ids.push(parent_id_array.value(i));
                            }
                        }
                        log::error!("     Sample BEFORE parent_ids: {:?}", sample_parent_ids);
                    }
                }
            }
        }
        
        log::error!("   TOTAL ATTRIBUTE RECORDS: {}", total_attribute_records);
        if total_attribute_records == 0 {
            log::error!("🚨 CRITICAL: NO ATTRIBUTES FOUND! All {} log records will have empty attributes!", streaming_batch.primary_batch.num_rows());
        }
        
        // Transform primary batch: UInt32 ID -> UInt16 ID
        let transformed_primary = id_mapper.transform_primary_batch(&streaming_batch.primary_batch)?;
        log::error!("   ✅ Primary batch transformed");
        logs.set(ArrowPayloadType::Logs, transformed_primary);
        
        // Transform child batches: UInt32 parent_id -> UInt16 parent_id
        for (table_name, child_batch) in streaming_batch.child_batches {
            let payload_type = self.table_name_to_payload_type(&table_name)?;
            let transformed_child = id_mapper.transform_child_batch(&child_batch)?;
            
            log::error!("   ✅ {} transformed: {} rows", table_name, transformed_child.num_rows());
            
            // Log sample parent_ids AFTER transformation
            if transformed_child.num_rows() > 0 {
                if let Some(parent_id_col) = transformed_child.column_by_name("parent_id") {
                    if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<arrow::array::UInt16Array>() {
                        let mut sample_parent_ids = Vec::new();
                        for i in 0..transformed_child.num_rows().min(10) {
                            if !parent_id_array.is_null(i) {
                                sample_parent_ids.push(parent_id_array.value(i));
                            }
                        }
                        log::error!("     Sample AFTER parent_ids: {:?}", sample_parent_ids);
                    }
                }
            }
            
            logs.set(payload_type, transformed_child);
        }

        let otap_records = OtapArrowRecords::Logs(logs);
        
        log::error!("🔄 Direct streaming OTAP batch complete: normalized from {} (batch start: {})", max_id - min_id + 1, min_id);
        log::error!("   - Primary records: {} logs", otap_records.get(ArrowPayloadType::Logs).map_or(0, |b| b.num_rows()));
        if let Some(attrs_batch) = otap_records.get(ArrowPayloadType::LogAttrs) {
            log::error!("   - Attribute records: {} log_attrs", attrs_batch.num_rows());
        } else {
            log::error!("🚨 NO log_attrs batch in final OTAP records!");
        }

        Ok(otap_records)
    }

    /// Analyze ID range from a primary batch (max, min)  
    fn analyze_id_range(batch: &RecordBatch) -> Result<(u32, u32), ParquetReceiverError> {
        let id_column = batch.column_by_name("id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No 'id' column found in logs batch".to_string()
            ))?;

        let id_array = id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "ID column is not UInt32Array".to_string()
            ))?;

        let mut min_id = u32::MAX;
        let mut max_id = 0u32;
        
        for i in 0..id_array.len() {
            if !id_array.is_null(i) {
                let id = id_array.value(i);
                min_id = min_id.min(id);
                max_id = max_id.max(id);
            }
        }

        if min_id == u32::MAX {
            return Err(ParquetReceiverError::Reconstruction(
                "No valid IDs found in batch".to_string()
            ));
        }

        Ok((max_id, min_id))
    }

    /// Process all partitions discovered in the base directory
    async fn process_all_partitions(
        &mut self,
        signal_type: &SignalType,
    ) -> Result<Vec<StreamingBatch>, ParquetReceiverError> {
        // Discover all partition IDs
        let partitions = self.discover_partitions("logs").await?;

        log::info!("🔍 Discovered {} partitions for processing", partitions.len());
        for partition in &partitions {
            log::debug!("   Partition: {}", partition);
        }

        let mut all_batches = Vec::new();

        // Process each partition using direct streaming
        for partition_id in partitions {
            let partition_uuid = Uuid::parse_str(&partition_id)
                .map_err(|e| ParquetReceiverError::Config(format!("Invalid partition UUID: {}", e)))?;

            let mut partition_batches = self.process_partition(&partition_uuid, signal_type).await?;
            all_batches.append(&mut partition_batches);
        }

        log::info!("🎉 Processed {} total batches from all partitions", all_batches.len());
        Ok(all_batches)
    }

    /// Process a partition using direct object store streaming approach
    async fn process_partition(
        &mut self,
        partition_id: &Uuid,
        signal_type: &SignalType,
    ) -> Result<Vec<StreamingBatch>, ParquetReceiverError> {
        // Only support logs for now (as specified)
        if *signal_type != SignalType::Logs {
            return Err(ParquetReceiverError::Config(format!(
                "Signal type {:?} not supported in direct streaming coordinator", signal_type
            )));
        }

        let partition_str = partition_id.to_string();
        log::info!("🔄 Processing partition: {} (direct streaming)", partition_str);

        // Create direct stream config
        let stream_config = DirectStreamConfig {
            base_directory: self.config.base_directory.clone(),
            batch_size: self.config.primary_batch_size,
        };

        // Create direct streaming merger for this partition
        let mut merger = DirectStreamingMerger::new(stream_config, partition_str.clone()).await?;

        let mut batches = Vec::new();
        
        // Stream processing loop using direct object store access
        while merger.has_more_batches() {
            if let Some(merge_batch) = merger.read_next_merge_batch().await? {
                log::debug!("📊 Processed merge batch: {} logs, {} attribute tables", 
                    merge_batch.logs_batch.num_rows(), 
                    merge_batch.attribute_batches.len());

                // Convert to StreamingBatch format for compatibility
                let streaming_batch = StreamingBatch {
                    primary_batch: merge_batch.logs_batch,
                    max_primary_id: merge_batch.max_primary_id,
                    child_batches: merge_batch.attribute_batches,
                    partition_id: merge_batch.partition_id,
                };

                batches.push(streaming_batch);
            }
        }

        log::info!("✅ Completed direct streaming for partition: {} ({} batches)", partition_str, batches.len());
        Ok(batches)
    }

    /// Discover partition IDs by scanning the directory structure
    async fn discover_partitions(&self, table_name: &str) -> Result<Vec<String>, ParquetReceiverError> {
        let table_dir = self.config.base_directory.join(table_name);
        
        if !table_dir.exists() {
            log::warn!("⚠️ Table directory does not exist: {}", table_dir.display());
            return Ok(vec![]);
        }

        let mut partitions = Vec::new();
        let mut entries = fs::read_dir(&table_dir).await
            .map_err(|e| ParquetReceiverError::Io(e))?;

        while let Some(entry) = entries.next_entry().await
            .map_err(|e| ParquetReceiverError::Io(e))? {
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(partition_id) = dir_name.strip_prefix("_part_id=") {
                        partitions.push(partition_id.to_string());
                    }
                }
            }
        }

        partitions.sort(); // Consistent ordering
        log::debug!("📁 Discovered {} partitions in {}", partitions.len(), table_dir.display());

        Ok(partitions)
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
