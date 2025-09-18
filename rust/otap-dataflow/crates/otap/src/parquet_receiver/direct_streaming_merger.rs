// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct streaming merger for parent-child parquet data
//!
//! This module implements the core streaming merge logic that coordinates
//! logs and log_attrs tables, buffering attributes and creating joined batches
//! with proper parent-child relationships.

use std::collections::HashMap;

use arrow::{
    array::{Array, RecordBatch, UInt32Array},
    compute,
};
use uuid::Uuid;

use crate::parquet_receiver::{
    direct_stream_reader::{DirectParquetStreamReader, DirectStreamConfig},
    error::ParquetReceiverError,
};

/// Streaming merger for coordinating parent-child parquet data with cursor management
pub struct DirectStreamingMerger {
    /// Reader for logs (parent) table
    logs_reader: DirectParquetStreamReader,
    /// Reader for log_attrs (child) table
    log_attrs_reader: Option<DirectParquetStreamReader>,
    /// Reader for resource_attrs table
    resource_attrs_reader: Option<DirectParquetStreamReader>,
    /// Reader for scope_attrs table  
    scope_attrs_reader: Option<DirectParquetStreamReader>,
    /// Partition ID being processed
    partition_id: String,
    /// Buffered records from log_attrs reader (cursor state)
    log_attrs_buffer: Option<RecordBatch>,
    /// Current position in log_attrs buffer
    log_attrs_cursor: usize,
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
            partition_id,
            log_attrs_buffer: None,
            log_attrs_cursor: 0,
        })
    }

    /// Process next streaming merge batch
    pub async fn read_next_merge_batch(&mut self) -> Result<Option<StreamingMergeBatch>, ParquetReceiverError> {
        log::error!("🚀🚀🚀 DIRECT STREAMING MERGER CALLED!!! 🚀🚀🚀");
        
        // Step 1: Read next batch from logs (primary) table
        let logs_batch = match self.logs_reader.read_next_batch().await? {
            Some(batch) => batch,
            None => return Ok(None), // No more logs to process
        };

        let record_count = logs_batch.num_rows();
        log::error!("📊 Read primary batch: {} logs", record_count);

        // Step 2: Determine max_id from logs batch and log detailed ID analysis
        let (max_id, min_id) = Self::analyze_id_range(&logs_batch)?;
        log::debug!("🔍 Primary ID range: {} to {} (span: {})", min_id, max_id, max_id - min_id + 1);

        // Log first few and last few IDs for debugging
        Self::log_id_samples(&logs_batch, "logs")?;

        // Step 3: Collect attribute records for the NORMALIZED range (0 to batch_size-1)
        // Since each batch will be normalized to start at 0, we need attributes for parent_ids 0 to (batch_size-1)
        let normalized_min_id = 0;
        let normalized_max_id = max_id - min_id; // e.g., if logs are 100-199, normalized range is 0-99
        
        let mut attribute_batches = HashMap::new();
        let mut total_attrs_found = 0;

        log::debug!("🎯 Will collect attributes for normalized range: {} to {} (original range was {} to {})", 
            normalized_min_id, normalized_max_id, min_id, max_id);

                // Process log_attrs with cursor-based streaming  
        if self.log_attrs_reader.is_some() {
            log::error!("🔍 CURSOR-BASED STREAMING log_attrs for normalized parent_ids {} to {}", normalized_min_id, normalized_max_id);
            log::error!("   Log_attrs reader file count: {}", self.log_attrs_reader.as_ref().unwrap().file_count());
            log::error!("   Log_attrs reader has_more_files: {}", self.log_attrs_reader.as_ref().unwrap().has_more_files());
            
            if let Some(attrs_batch) = self.read_log_attrs_with_cursor_range(normalized_min_id, normalized_max_id).await? {
                let attrs_count = attrs_batch.num_rows();
                total_attrs_found += attrs_count;
                log::error!("✅ Found {} log_attrs for normalized parent_ids {} to {} (cursor-based)", attrs_count, normalized_min_id, normalized_max_id);
                
                // Log samples of the attributes found
                Self::log_attribute_samples(&attrs_batch, "log_attrs")?;
                let _ = attribute_batches.insert("log_attrs".to_string(), attrs_batch);
            } else {
                log::error!("🚨 NO log_attrs found for normalized parent_ids {} to {} using cursor-based streaming", normalized_min_id, normalized_max_id);
                log::error!("   Reader state after failed collection:");
                log::error!("   - has_more_files: {}", self.log_attrs_reader.as_ref().unwrap().has_more_files());
                log::error!("   - buffer state: {:?}", self.log_attrs_buffer.as_ref().map(|b| b.num_rows()));
                log::error!("   - cursor position: {}", self.log_attrs_cursor);
            }
        } else {
            log::error!("ℹ️ No log_attrs reader available");
        }", self.log_attrs_buffer.as_ref().map(|b| b.num_rows()));
                log::error!("   - cursor position: {}", self.log_attrs_cursor);
            }
        } else {
            log::debug!("No log_attrs reader available");
        }

        // Process resource_attrs
        if let Some(ref mut reader) = self.resource_attrs_reader {
            if let Some(attrs_batch) = Self::read_attributes_up_to_id_static(reader, max_id, "resource_attrs").await? {
                let attrs_count = attrs_batch.num_rows();
                total_attrs_found += attrs_count;
                log::debug!("✅ Found {} resource_attrs for parent_ids <= {}", attrs_count, max_id);
                let _ = attribute_batches.insert("resource_attrs".to_string(), attrs_batch);
            }
        }

        // Process scope_attrs
        if let Some(ref mut reader) = self.scope_attrs_reader {
            if let Some(attrs_batch) = Self::read_attributes_up_to_id_static(reader, max_id, "scope_attrs").await? {
                let attrs_count = attrs_batch.num_rows();
                total_attrs_found += attrs_count;
                log::debug!("✅ Found {} scope_attrs for parent_ids <= {}", attrs_count, max_id);
                let _ = attribute_batches.insert("scope_attrs".to_string(), attrs_batch);
            }
        }

        log::debug!("📊 BATCH SUMMARY: {} logs, {} total attributes across {} attribute tables", 
            record_count, total_attrs_found, attribute_batches.len());
        
        if total_attrs_found == 0 && record_count > 0 {
            log::warn!("🚨 ATTRIBUTE COORDINATION ISSUE: {} logs but 0 attributes found! This suggests ID misalignment.", record_count);
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

    /// Read log_attrs records for a specific parent_id range using cursor-based streaming
    async fn read_log_attrs_with_cursor_range(
        &mut self,
        min_parent_id: u32,
        max_parent_id: u32,
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let reader = match &mut self.log_attrs_reader {
            Some(r) => r,
            None => return Ok(None),
        };
        
        let mut collected_records = Vec::new();
        
        log::debug!("🔍 CURSOR-BASED READING log_attrs for parent_ids {} to {}", min_parent_id, max_parent_id);

        // Process buffered records first (from previous cursor position)
        if let Some(ref buffer) = self.log_attrs_buffer {
            log::debug!("📦 Processing buffered records from cursor position {}", self.log_attrs_cursor);
            
            // Extract records from buffer where min_parent_id <= parent_id <= max_parent_id
            let (matching_records, remaining_buffer, new_cursor) = 
                Self::extract_range_records_from_buffer(&buffer, min_parent_id, max_parent_id, self.log_attrs_cursor)?;
            
            if let Some(matching_batch) = matching_records {
                log::debug!("✅ Extracted {} records from buffer", matching_batch.num_rows());
                collected_records.push(matching_batch);
            }
            
            // Update cursor state
            self.log_attrs_buffer = remaining_buffer;
            self.log_attrs_cursor = new_cursor;
        }
        
        // Continue reading new files until we have enough attributes
        let mut files_read = 0;
        let max_files_per_batch = 20; // Safety limit to prevent infinite loops
        
        while reader.has_more_files() && files_read < max_files_per_batch {
            if let Some(batch) = reader.read_next_batch().await? {
                files_read += 1;
                log::debug!("📁 Read new log_attrs batch: {} rows (file #{})", batch.num_rows(), files_read);
                
                // Check if this batch contains records we need
                let (batch_min_parent_id, batch_max_parent_id) = Self::get_parent_id_range(&batch)?;
                log::debug!("🔍 Batch parent_id range: {} to {}", batch_min_parent_id, batch_max_parent_id);
                
                // Check if this batch has any overlap with our target range
                if batch_max_parent_id < min_parent_id {
                    // This batch is entirely before our target range - skip it
                    log::debug!("⏩ Skipping batch (ends at parent_id {}, before target {})", batch_max_parent_id, min_parent_id);
                    continue;
                }
                
                // Extract matching records from this batch for our specific range
                let (matching_records, remaining_buffer, new_cursor) = 
                    Self::extract_range_records_from_buffer(&batch, min_parent_id, max_parent_id, 0)?;
                
                if let Some(matching_batch) = matching_records {
                    log::debug!("✅ Extracted {} records from new batch (file #{})", matching_batch.num_rows(), files_read);
                    collected_records.push(matching_batch);
                }
                
                // Always continue to next file - don't buffer until we've read all files
                // This ensures we collect ALL attributes for the target range
                if batch_min_parent_id > max_parent_id {
                    // This batch starts beyond our target range - we can stop here
                    log::debug!("🔚 Stopping at file #{} (starts at parent_id {}, beyond target {})", files_read, batch_min_parent_id, max_parent_id);
                    if let Some(remaining) = remaining_buffer {
                        // Save remaining records for next iteration
                        log::debug!("💾 Buffering remaining {} records from file #{}", remaining.num_rows(), files_read);
                        self.log_attrs_buffer = Some(remaining);
                        self.log_attrs_cursor = 0; // Reset cursor for buffered batch
                    }
                    break;
                }
                
                // If there are remaining records but they're still within our range, 
                // we need to continue processing them
                if let Some(remaining) = remaining_buffer {
                    // These records are beyond our current range, buffer them
                    log::debug!("💾 Buffering remaining {} records for next iteration", remaining.num_rows());
                    self.log_attrs_buffer = Some(remaining);
                    self.log_attrs_cursor = 0; // Reset cursor for buffered batch
                    break;
                }
                
                // Clear buffer state as we consumed this entire file
                self.log_attrs_buffer = None;
                self.log_attrs_cursor = 0;
            } else {
                log::debug!("📄 No more log_attrs files available after reading {} files", files_read);
                break;
            }
        }
        
        if files_read >= max_files_per_batch {
            log::warn!("⚠️ Hit safety limit of {} files per batch", max_files_per_batch);
        }
        
        log::debug!("🎯 Cursor-based range collection complete: {} batches collected for parent_ids {} to {}", 
            collected_records.len(), min_parent_id, max_parent_id);
        
        if collected_records.is_empty() {
            return Ok(None);
        }
        
        // Combine all collected records
        let combined_batch = if collected_records.len() == 1 {
            collected_records.into_iter().next().unwrap()
        } else {
            let schema = collected_records[0].schema();
            compute::concat_batches(&schema, &collected_records)
                .map_err(|e| ParquetReceiverError::Arrow(e))?
        };
        
        log::debug!("✅ Final cursor-based range batch: {} records for log_attrs", combined_batch.num_rows());
        Ok(Some(combined_batch))
    }

    /// Read log_attrs records up to a specific parent_id threshold using cursor-based streaming
    async fn read_log_attrs_with_cursor(
        &mut self,
        max_parent_id: u32,
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let reader = match &mut self.log_attrs_reader {
            Some(r) => r,
            None => return Ok(None),
        };
        
        let mut collected_records = Vec::new();
        
        log::debug!("🔍 CURSOR-BASED READING log_attrs for parent_ids <= {}", max_parent_id);

        // Process buffered records first (from previous cursor position)
        if let Some(ref buffer) = self.log_attrs_buffer {
            log::debug!("📦 Processing buffered records from cursor position {}", self.log_attrs_cursor);
            
            // Extract records from buffer where parent_id <= max_parent_id
            let (matching_records, remaining_buffer, new_cursor) = 
                Self::extract_matching_records_from_buffer(&buffer, max_parent_id, self.log_attrs_cursor)?;
            
            if let Some(matching_batch) = matching_records {
                log::debug!("✅ Extracted {} records from buffer", matching_batch.num_rows());
                collected_records.push(matching_batch);
            }
            
            // Update cursor state
            self.log_attrs_buffer = remaining_buffer;
            self.log_attrs_cursor = new_cursor;
        }
        
        // Continue reading new files if needed
        while reader.has_more_files() {
            if let Some(batch) = reader.read_next_batch().await? {
                log::debug!("📁 Read new log_attrs batch: {} rows", batch.num_rows());
                
                // Check if this batch contains records we need
                let (min_parent_id, max_parent_in_batch) = Self::get_parent_id_range(&batch)?;
                log::debug!("🔍 Batch parent_id range: {} to {}", min_parent_id, max_parent_in_batch);
                
                if min_parent_id > max_parent_id {
                    // This batch starts beyond our target range - save as buffer for next iteration
                    log::debug!("💾 Buffering batch (starts at parent_id {}, beyond target {})", min_parent_id, max_parent_id);
                    self.log_attrs_buffer = Some(batch);
                    self.log_attrs_cursor = 0;
                    break;
                }
                
                // Extract matching records from this batch
                let (matching_records, remaining_buffer, new_cursor) = 
                    Self::extract_matching_records_from_buffer(&batch, max_parent_id, 0)?;
                
                if let Some(matching_batch) = matching_records {
                    log::debug!("✅ Extracted {} records from new batch", matching_batch.num_rows());
                    collected_records.push(matching_batch);
                }
                
                if let Some(remaining) = remaining_buffer {
                    // Save remaining records for next iteration
                    log::debug!("💾 Buffering remaining {} records", remaining.num_rows());
                    self.log_attrs_buffer = Some(remaining);
                    self.log_attrs_cursor = new_cursor;
                    break;
                } else {
                    // Entire batch was consumed, continue to next file
                    self.log_attrs_buffer = None;
                    self.log_attrs_cursor = 0;
                }
            } else {
                log::debug!("📄 No more log_attrs files available");
                break;
            }
        }
        
        log::debug!("🎯 Cursor-based collection complete: {} batches collected for parent_ids <= {}", 
            collected_records.len(), max_parent_id);
        
        if collected_records.is_empty() {
            return Ok(None);
        }
        
        // Combine all collected records
        let combined_batch = if collected_records.len() == 1 {
            collected_records.into_iter().next().unwrap()
        } else {
            let schema = collected_records[0].schema();
            compute::concat_batches(&schema, &collected_records)
                .map_err(|e| ParquetReceiverError::Arrow(e))?
        };
        
        log::debug!("✅ Final cursor-based batch: {} records for log_attrs", combined_batch.num_rows());
        Ok(Some(combined_batch))
    }
    
    /// Extract records from a batch where parent_id <= max_parent_id, starting from cursor position
    /// Returns (matching_records, remaining_buffer, new_cursor_position)
    fn extract_matching_records_from_buffer(
        batch: &RecordBatch,
        max_parent_id: u32,
        start_cursor: usize,
    ) -> Result<(Option<RecordBatch>, Option<RecordBatch>, usize), ParquetReceiverError> {
        let parent_id_column = batch.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No 'parent_id' column found in attribute batch".to_string()
            ))?;

        let parent_id_array = parent_id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        let mut matching_indices = Vec::new();
        let mut split_point = None;
        
        // Scan from cursor position
        for i in start_cursor..parent_id_array.len() {
            if !parent_id_array.is_null(i) {
                let parent_id = parent_id_array.value(i);
                if parent_id <= max_parent_id {
                    matching_indices.push(i);
                } else {
                    // Found first record beyond our range
                    split_point = Some(i);
                    break;
                }
            }
        }
        
        let matching_records = if matching_indices.is_empty() {
            None
        } else {
            // Create indices array for matching records
            let indices = UInt32Array::from(
                matching_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
            );
            let matching_batch = compute::take_record_batch(batch, &indices)
                .map_err(|e| ParquetReceiverError::Arrow(e))?;
            Some(matching_batch)
        };
        
        let remaining_buffer = if let Some(split_idx) = split_point {
            // Create slice from split point to end
            let remaining_batch = batch.slice(split_idx, batch.num_rows() - split_idx);
            Some(remaining_batch)
        } else {
            None
        };
        
        let new_cursor = split_point.unwrap_or(batch.num_rows());
        
        Ok((matching_records, remaining_buffer, new_cursor))
    }

    /// Extract records from buffer where min_parent_id <= parent_id <= max_parent_id
    fn extract_range_records_from_buffer(
        batch: &RecordBatch,
        min_parent_id: u32,
        max_parent_id: u32,
        start_cursor: usize,
    ) -> Result<(Option<RecordBatch>, Option<RecordBatch>, usize), ParquetReceiverError> {
        let parent_id_col = batch.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction("parent_id column not found".to_string()))?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction("parent_id column is not UInt32Array".to_string()))?;
        
        let mut matching_indices = Vec::new();
        let mut first_beyond_range = None;
        
        // Start from cursor position
        for i in start_cursor..parent_id_col.len() {
            let parent_id = parent_id_col.value(i);
            
            if parent_id < min_parent_id {
                // Skip records before our target range
                continue;
            }
            
            if parent_id > max_parent_id {
                // Found first record beyond our target range
                first_beyond_range = Some(i);
                break;
            }
            
            // This record is in our target range
            matching_indices.push(i);
        }
        
        log::debug!("🎯 Range extraction: found {} matching records from cursor {}, first_beyond_range: {:?}", 
            matching_indices.len(), start_cursor, first_beyond_range);
        
        // Build matching records batch
        let matching_batch = if matching_indices.is_empty() {
            None
        } else {
            let indices = UInt32Array::from(matching_indices.iter().map(|&i| i as u32).collect::<Vec<u32>>());
            let matching = compute::take_record_batch(batch, &indices)
                .map_err(|e| ParquetReceiverError::Arrow(e))?;
            Some(matching)
        };
        
        // Build remaining batch (records beyond our target range)
        let (remaining_batch, new_cursor) = if let Some(beyond_idx) = first_beyond_range {
            let remaining_indices: Vec<u32> = (beyond_idx..parent_id_col.len()).map(|i| i as u32).collect();
            
            if remaining_indices.is_empty() {
                (None, parent_id_col.len())
            } else {
                let indices = UInt32Array::from(remaining_indices);
                let remaining = compute::take_record_batch(batch, &indices)
                    .map_err(|e| ParquetReceiverError::Arrow(e))?;
                (Some(remaining), 0) // Reset cursor for new batch
            }
        } else {
            // No records beyond range, everything was consumed
            (None, parent_id_col.len())
        };
        
        Ok((matching_batch, remaining_batch, new_cursor))
    }

    /// Read attribute records up to a specific parent_id threshold using coordinated streaming
    async fn read_attributes_up_to_id_static(
        reader: &mut DirectParquetStreamReader,
        max_parent_id: u32,
        table_name: &str,
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        let mut collected_batches = Vec::new();
        let mut total_records = 0;
        let mut files_read = 0;

        log::debug!("🔍 READING {} attributes for parent_ids <= {}", table_name, max_parent_id);

        // Stream through files and collect records up to max_parent_id
        // Continue reading files as long as they contain relevant records
        while reader.has_more_files() {
            if let Some(batch) = reader.read_next_batch().await? {
                files_read += 1;
                let original_rows = batch.num_rows();
                log::debug!("📁 Read {} file #{} with {} rows", table_name, files_read, original_rows);

                // Log samples from the raw batch before filtering
                if original_rows > 0 {
                    Self::log_attribute_samples(&batch, &format!("{}_raw", table_name))?;
                }

                // Check if this batch contains any records we need
                let (min_parent_id, max_parent_in_batch) = Self::get_parent_id_range(&batch)?;
                log::debug!("🔍 File parent_id range: {} to {}", min_parent_id, max_parent_in_batch);

                if min_parent_id > max_parent_id {
                    // This file starts beyond our target range - we're done
                    // Put the batch back by not advancing the reader (this is tricky with current design)
                    log::debug!("⏹️ File starts at parent_id {}, beyond target {}. Stopping collection.", min_parent_id, max_parent_id);
                    
                    // For now, we can't put back the batch, but we know we're done
                    // TODO: Implement proper putback or buffering mechanism
                    break;
                }

                // Filter batch to only include records <= max_parent_id
                let filtered_batch = Self::filter_batch_by_parent_id(&batch, max_parent_id)?;
                
                let filtered_rows = filtered_batch.num_rows();
                log::debug!("🔽 After filtering parent_ids <= {}: {} → {} rows ({}% kept)", 
                    max_parent_id, original_rows, filtered_rows, 
                    if original_rows > 0 { (filtered_rows * 100) / original_rows } else { 0 });

                if filtered_rows > 0 {
                    total_records += filtered_rows;
                    collected_batches.push(filtered_batch);
                    
                    log::debug!("📊 Collected {} {} records (total: {})", 
                        filtered_rows, table_name, total_records);
                }

                // Check if we've collected all relevant records from this file
                if max_parent_in_batch > max_parent_id {
                    // This file contains records beyond our range, so we're done
                    log::debug!("✅ File contains parent_ids up to {}, beyond target {}. Collection complete.", max_parent_in_batch, max_parent_id);
                    break;
                }

                // If we get here, the entire file was ≤ max_parent_id, so continue to next file
            } else {
                log::debug!("📄 No more {} files available", table_name);
                break; // No more batches from this reader
            }
        }

        log::debug!("🎯 {} SUMMARY: Read {} files, collected {} total records for parent_ids <= {}", 
            table_name, files_read, total_records, max_parent_id);

        if collected_batches.is_empty() {
            log::warn!("⚠️ NO {} records found with parent_ids <= {}", table_name, max_parent_id);
            return Ok(None);
        }

        // Combine all collected batches
        let combined_batch = if collected_batches.len() == 1 {
            collected_batches.into_iter().next().unwrap()
        } else {
            log::debug!("🔗 Combining {} batches from {}", collected_batches.len(), table_name);
            let schema = collected_batches[0].schema();
            compute::concat_batches(&schema, &collected_batches)
                .map_err(|e| ParquetReceiverError::Arrow(e))?
        };

        log::debug!("✅ Final {} batch: {} records", table_name, combined_batch.num_rows());
        
        // Log final combined sample
        if combined_batch.num_rows() > 0 {
            Self::log_attribute_samples(&combined_batch, &format!("{}_final", table_name))?;
        }

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

    /// Get the min and max parent_id from an attribute batch
    fn get_parent_id_range(batch: &RecordBatch) -> Result<(u32, u32), ParquetReceiverError> {
        let parent_id_column = batch.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "No 'parent_id' column found in attribute batch".to_string()
            ))?;

        let parent_id_array = parent_id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        let mut min_parent_id = u32::MAX;
        let mut max_parent_id = 0u32;
        
        for i in 0..parent_id_array.len() {
            if !parent_id_array.is_null(i) {
                let parent_id = parent_id_array.value(i);
                min_parent_id = min_parent_id.min(parent_id);
                max_parent_id = max_parent_id.max(parent_id);
            }
        }

        if min_parent_id == u32::MAX {
            return Err(ParquetReceiverError::Reconstruction(
                "No valid parent_ids found in batch".to_string()
            ));
        }

        Ok((min_parent_id, max_parent_id))
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

    /// Analyze ID range from a primary batch (min, max)
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

    /// Log sample IDs from a batch for debugging
    fn log_id_samples(batch: &RecordBatch, table_name: &str) -> Result<(), ParquetReceiverError> {
        let id_column = batch.column_by_name("id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                format!("No 'id' column found in {} batch", table_name)
            ))?;

        let id_array = id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "ID column is not UInt32Array".to_string()
            ))?;

        let len = id_array.len();
        let sample_size = len.min(5);
        
        let mut first_ids = Vec::new();
        let mut last_ids = Vec::new();
        
        // Get first few IDs
        for i in 0..sample_size {
            if !id_array.is_null(i) {
                first_ids.push(id_array.value(i));
            }
        }
        
        // Get last few IDs if different from first
        if len > sample_size {
            for i in len.saturating_sub(sample_size)..len {
                if !id_array.is_null(i) {
                    last_ids.push(id_array.value(i));
                }
            }
        }

        log::debug!("🔍 {} ID samples - First {}: {:?}", table_name, first_ids.len(), first_ids);
        if !last_ids.is_empty() && last_ids != first_ids {
            log::debug!("🔍 {} ID samples - Last {}: {:?}", table_name, last_ids.len(), last_ids);
        }

        Ok(())
    }

    /// Log sample attributes for debugging
    fn log_attribute_samples(batch: &RecordBatch, table_name: &str) -> Result<(), ParquetReceiverError> {
        let parent_id_column = batch.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                format!("No 'parent_id' column found in {} batch", table_name)
            ))?;

        let parent_id_array = parent_id_column.as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        let len = parent_id_array.len();
        let sample_size = len.min(10);
        
        log::debug!("🔍 {} parent_id samples from {} rows:", table_name, len);
        
        for i in 0..sample_size {
            if !parent_id_array.is_null(i) {
                let parent_id = parent_id_array.value(i);
                
                // Try to get key and value for this attribute
                let key_info = if let Some(key_col) = batch.column_by_name("key") {
                    Self::extract_string_value(key_col, i).unwrap_or_else(|| "<no-key>".to_string())
                } else {
                    "<no-key-col>".to_string()
                };
                
                let value_info = if let Some(str_col) = batch.column_by_name("str") {
                    Self::extract_string_value(str_col, i).unwrap_or_else(|| "<no-str>".to_string())
                } else {
                    "<no-str-col>".to_string()
                };

                // log::debug!("   [{}] parent_id={} key={} value={}", i, parent_id, key_info, value_info);
            }
        }

        Ok(())
    }

    /// Helper function to extract string value from a column (handles Dictionary and regular Utf8)
    fn extract_string_value(column: &arrow::array::ArrayRef, index: usize) -> Option<String> {
        use arrow::array::{StringArray, DictionaryArray};
        use arrow::datatypes::{UInt8Type, UInt16Type};
        
        match column.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                column.as_any().downcast_ref::<StringArray>()
                    .and_then(|arr| {
                        if index < arr.len() && !arr.is_null(index) {
                            Some(arr.value(index).to_string())
                        } else {
                            None
                        }
                    })
            },
            arrow::datatypes::DataType::Dictionary(key_type, _) => {
                match key_type.as_ref() {
                    arrow::datatypes::DataType::UInt8 => {
                        column.as_any().downcast_ref::<DictionaryArray<UInt8Type>>()
                            .and_then(|dict| {
                                if index < dict.len() && !dict.is_null(index) {
                                    let key = dict.key(index)?;
                                    dict.values().as_any().downcast_ref::<StringArray>()
                                        .and_then(|str_arr| {
                                            let key_idx: usize = key.into();
                                            if key_idx < str_arr.len() {
                                                Some(str_arr.value(key_idx).to_string())
                                            } else {
                                                None
                                            }
                                        })
                                } else {
                                    None
                                }
                            })
                    },
                    arrow::datatypes::DataType::UInt16 => {
                        column.as_any().downcast_ref::<DictionaryArray<UInt16Type>>()
                            .and_then(|dict| {
                                if index < dict.len() && !dict.is_null(index) {
                                    let key = dict.key(index)?;
                                    dict.values().as_any().downcast_ref::<StringArray>()
                                        .and_then(|str_arr| {
                                            let key_idx: usize = key.into();
                                            if key_idx < str_arr.len() {
                                                Some(str_arr.value(key_idx).to_string())
                                            } else {
                                                None
                                            }
                                        })
                                } else {
                                    None
                                }
                            })
                    },
                    _ => None
                }
            },
            _ => None
        }
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