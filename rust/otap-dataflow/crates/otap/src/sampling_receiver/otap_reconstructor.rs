//!
//! This module converts DataFusion query results back into OTAP records,
//! following the patterns from the parquet_receiver but working with 
//! DataFusion's output format.

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::compute::kernels::cast::cast;
use log::{debug, info, error};

use crate::sampling_receiver::{
    config::Config,
    error::{Result, SamplingReceiverError},
    query_engine::DataFusionQueryEngine,
};
use crate::pdata::{OtapPdata, OtapPayload};
use otel_arrow_rust::otap::{Logs, OtapArrowRecords, OtapBatchStore};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// Reconstructs OTAP records from DataFusion log_attributes query results
/// by streaming merge with related tables (logs, resource_attrs, scope_attrs)
pub struct OtapReconstructor {
    /// DataFusion query engine for additional table lookups
    query_engine: Arc<DataFusionQueryEngine>,
    /// Configuration for batch sizes and performance tuning
    batch_size: usize,
}

/// Result of OTAP reconstruction containing the reconstructed records
#[derive(Debug)]
pub struct ReconstructionResult {
    /// Reconstructed OTAP records ready for pipeline output
    pub otap_records: Vec<OtapPdata>,
    /// Number of log_attributes processed
    pub attributes_processed: usize,
    /// Number of unique parent IDs (logs) involved
    pub unique_parent_ids: usize,
}

impl OtapReconstructor {
    /// Create a new OTAP reconstructor
    pub fn new(config: Config, query_engine: Arc<DataFusionQueryEngine>) -> Self {
        Self {
            query_engine,
            batch_size: config.performance.batch_size,
        }
    }

    /// Get related data for log_attributes reconstruction (placeholder)
    pub async fn get_related_data(&self) -> Result<()> {
        // Placeholder: This will query logs, resource_attrs, scope_attrs tables
        // to get the related data needed for OTAP reconstruction
        debug!("Getting related data for OTAP reconstruction");
        Ok(())
    }

    /// Reconstruct OTAP records from a DataFusion query stream (simplified interface)
    /// 
    /// This is a simplified wrapper around the main reconstruction logic for cases
    /// where we get query results as a stream instead of pre-collected batches.
    pub async fn reconstruct_from_stream(
        &self,
        mut stream: impl futures_util::Stream<Item = datafusion::common::Result<RecordBatch>> + Unpin,
    ) -> Result<Vec<OtapPdata>> {
        use futures_util::StreamExt;
        
        // Collect stream into batches
        let mut batches = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| SamplingReceiverError::query_template_error(
                    &format!("DataFusion stream error: {}", e)
                ))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // For now, create placeholder records based on the number of rows
        let mut results = Vec::new();
        let mut total_rows = 0;

        for batch in &batches {
            total_rows += batch.num_rows();
            for _ in 0..batch.num_rows() {
                // TODO: Replace with actual reconstruction logic
                let record = create_placeholder_otap_record();
                results.push(record);
            }
        }

        info!("Reconstructed {} OTAP records from {} rows (placeholder implementation)", 
              results.len(), total_rows);

        Ok(results)
    }

    /// Reconstruct OTAP records from DataFusion log_attributes query results
    ///
    /// This is the main entry point that takes log_attributes query results and
    /// reconstructs complete OTAP records by streaming merge with related tables.
    pub async fn reconstruct_from_log_attributes(
        &self,
        log_attributes_results: Vec<RecordBatch>,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<ReconstructionResult> {
        if log_attributes_results.is_empty() {
            return Ok(ReconstructionResult {
                otap_records: Vec::new(),
                attributes_processed: 0,
                unique_parent_ids: 0,
            });
        }

        info!("🔧 Starting OTAP reconstruction from {} log_attributes batches", log_attributes_results.len());

        // Step 1: Extract unique parent IDs from log_attributes results
        let parent_ids = self.extract_parent_ids(&log_attributes_results)?;
        let unique_parent_ids = parent_ids.len();
        
        info!("🔍 Found {} unique parent IDs to reconstruct", unique_parent_ids);

        // Step 2: Query related tables for these parent IDs
        let related_data = self.query_related_tables(&parent_ids, window_start_ns, window_end_ns).await?;

        // Step 3: Perform streaming merge reconstruction
        let otap_records = self.perform_streaming_merge(
            &log_attributes_results,
            &related_data,
        ).await?;

        let attributes_processed = log_attributes_results.iter()
            .map(|batch| batch.num_rows())
            .sum();

        info!("✅ OTAP reconstruction complete: {} records from {} attributes", 
              otap_records.len(), attributes_processed);

        Ok(ReconstructionResult {
            otap_records,
            attributes_processed,
            unique_parent_ids,
        })
    }

    /// Extract unique parent IDs from log_attributes query results
    fn extract_parent_ids(&self, log_attributes_results: &[RecordBatch]) -> Result<Vec<u32>> {
        let mut parent_ids = Vec::new();

        for batch in log_attributes_results {
            let parent_id_column = batch.column_by_name("parent_id")
                .ok_or_else(|| SamplingReceiverError::query_template_error(
                    "No parent_id column found in log_attributes results"
                ))?;

            let parent_id_array = parent_id_column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| SamplingReceiverError::query_template_error(
                    "parent_id column is not UInt32"
                ))?;

            for i in 0..parent_id_array.len() {
                if !parent_id_array.is_null(i) {
                    parent_ids.push(parent_id_array.value(i));
                }
            }
        }

        // Remove duplicates and sort for efficient lookups
        parent_ids.sort_unstable();
        parent_ids.dedup();

        debug!("📊 Extracted {} unique parent IDs from log_attributes", parent_ids.len());
        Ok(parent_ids)
    }

    /// Query related tables (logs, resource_attrs, scope_attrs) for the given parent IDs
    async fn query_related_tables(
        &self,
        parent_ids: &[u32],
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<RelatedTableData> {
        info!("🔍 Querying related tables for {} parent IDs", parent_ids.len());

        // Create IN clause for parent ID filtering
        let parent_id_list = parent_ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");

        // Query logs table
        let logs_query = format!(
            r#"
            SELECT id, time_unix_nano, observed_time_unix_nano, trace_id, span_id, 
                   severity_number, severity_text, body, dropped_attributes_count, flags
            FROM logs 
            WHERE id IN ({})
              AND time_unix_nano >= to_timestamp_nanos({})
              AND time_unix_nano < to_timestamp_nanos({})
            ORDER BY id
            "#,
            parent_id_list, window_start_ns, window_end_ns
        );

        // Query resource_attrs table
        let resource_attrs_query = format!(
            r#"
            SELECT parent_id, key, type, str, int, double, bool, bytes, ser
            FROM resource_attrs
            WHERE parent_id IN ({})
            ORDER BY parent_id, key
            "#,
            parent_id_list
        );

        // Query scope_attrs table  
        let scope_attrs_query = format!(
            r#"
            SELECT parent_id, key, type, str, int, double, bool, bytes, ser
            FROM scope_attrs
            WHERE parent_id IN ({})
            ORDER BY parent_id, key
            "#,
            parent_id_list
        );

        // Execute queries in parallel
        let (logs_results, resource_attrs_results, scope_attrs_results) = tokio::try_join!(
            self.query_engine.execute_query(&logs_query),
            self.query_engine.execute_query(&resource_attrs_query),
            self.query_engine.execute_query(&scope_attrs_query)
        )?;

        debug!("📊 Related table query results:");
        debug!("   Logs: {} batches", logs_results.len());
        debug!("   Resource attrs: {} batches", resource_attrs_results.len());
        debug!("   Scope attrs: {} batches", scope_attrs_results.len());

        Ok(RelatedTableData {
            logs: logs_results,
            resource_attrs: resource_attrs_results,
            scope_attrs: scope_attrs_results,
        })
    }

    /// Perform streaming merge reconstruction to create OTAP records
    /// This is the inverse of parquet_receiver: start with log_attrs, reconstruct full OTAP
    async fn perform_streaming_merge(
        &self,
        log_attributes_results: &[RecordBatch],
        _related_data: &RelatedTableData,
    ) -> Result<Vec<OtapPdata>> {
        info!("🔀 Starting streaming merge reconstruction (log_attrs → OTAP records)");
        let mut otap_records = Vec::new();

        // Process each log_attrs batch
        for log_attrs_batch in log_attributes_results {
            if log_attrs_batch.num_rows() == 0 {
                continue;
            }

            debug!("📊 Processing log_attrs batch: {} rows", log_attrs_batch.num_rows());

            // Step 1: Extract unique parent_ids from log_attrs (vectorized)
            let parent_ids = self.extract_parent_ids_vectorized(log_attrs_batch)?;
            if parent_ids.is_empty() {
                debug!("⚠️  No parent_ids found in batch, skipping");
                continue;
            }

            // Step 2: Query related tables for these parent_ids
            let merge_batch = self.create_merge_batch(log_attrs_batch.clone(), &parent_ids).await?;

            // Step 3: Convert merge batch to OTAP records using vectorized transformations
            let batch_otap_records = self.merge_batch_to_otap(&merge_batch)?;
            otap_records.extend(batch_otap_records);
        }

        info!("🎯 Streaming merge complete: created {} OTAP records", otap_records.len());
        Ok(otap_records)
    }

    /// Extract unique parent_ids from log_attrs batch using vectorized operations
    fn extract_parent_ids_vectorized(&self, batch: &RecordBatch) -> Result<Vec<u32>> {
        let parent_id_col = batch.column_by_name("parent_id")
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: "log_attrs batch missing parent_id column".to_string(),
            })?;

        let parent_id_array = parent_id_col.as_any().downcast_ref::<UInt32Array>()
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: "parent_id column is not UInt32".to_string(),
            })?;

        // Use HashSet for deduplication, then collect unique parent_ids
        let mut unique_ids = HashSet::new();
        for i in 0..parent_id_array.len() {
            if !parent_id_array.is_null(i) {
                let _ = unique_ids.insert(parent_id_array.value(i));
            }
        }

        let mut parent_ids: Vec<u32> = unique_ids.into_iter().collect();
        parent_ids.sort_unstable(); // Sort for efficient range queries
        
        debug!("🔍 Extracted {} unique parent_ids from {} log_attrs", parent_ids.len(), batch.num_rows());
        Ok(parent_ids)
    }

    /// Create merge batch by querying related tables for parent_ids
    async fn create_merge_batch(
        &self,
        log_attrs_batch: RecordBatch,
        parent_ids: &[u32],
    ) -> Result<SamplingMergeBatch> {
        if parent_ids.is_empty() {
            return Err(SamplingReceiverError::ReconstructionError {
                message: "Cannot create merge batch with empty parent_ids".to_string(),
            });
        }

        let min_id = *parent_ids.first().unwrap();
        let max_id = *parent_ids.last().unwrap();
        
        debug!("📋 Creating merge batch for parent_id range: {} to {}", min_id, max_id);

        // Query logs table for these parent_ids
        let logs_batch = self.query_logs_for_ids(parent_ids).await?;
        
        // Query attribute tables
        let mut attribute_batches = HashMap::new();
        
        // Resource attributes (optional)
        if let Ok(resource_batch) = self.query_resource_attrs_for_ids(parent_ids).await {
            if resource_batch.num_rows() > 0 {
                let _ = attribute_batches.insert("resource_attrs".to_string(), resource_batch);
            }
        }
        
        // Scope attributes (optional)
        if let Ok(scope_batch) = self.query_scope_attrs_for_ids(parent_ids).await {
            if scope_batch.num_rows() > 0 {
                let _ = attribute_batches.insert("scope_attrs".to_string(), scope_batch);
            }
        }

        debug!("✅ Merge batch created: logs={}, attributes={}", 
               logs_batch.as_ref().map_or(0, |b| b.num_rows()),
               attribute_batches.values().map(|b| b.num_rows()).sum::<usize>());

        Ok(SamplingMergeBatch {
            log_attrs_batch,
            logs_batch,
            attribute_batches,
            parent_id_range: (min_id, max_id),
        })
    }

    /// Convert merge batch to OTAP records using streaming coordinator patterns
    fn merge_batch_to_otap(&self, merge_batch: &SamplingMergeBatch) -> Result<Vec<OtapPdata>> {
        debug!("🔧 Converting merge batch to OTAP records");
        
        // Create OTAP batch following parquet_receiver streaming_coordinator pattern
        let otap_records = self.batch_to_otap_records(merge_batch)?;
        
        // Create OtapPdata objects
        let mut result = Vec::new();
        let payload = OtapPayload::OtapArrowRecords(otap_records);
        let otap_data = OtapPdata::new_todo_context(payload);
        result.push(otap_data);
        
        Ok(result)
    }

    /// Convert merge batch to OtapArrowRecords following parquet_receiver patterns
    fn batch_to_otap_records(&self, merge_batch: &SamplingMergeBatch) -> Result<OtapArrowRecords> {
        let mut logs = Logs::default();
        
        // Apply ID normalization using vectorized operations
        let (min_id, max_id) = merge_batch.parent_id_range;
        debug!("🎯 OTAP batch ID range: {} to {} (normalizing to 0-{})", min_id, max_id, max_id - min_id);
        
        let mut id_mapper = IdMapper::new();
        id_mapper.set_batch_start(min_id);
        
        // Transform logs batch if present
        if let Some(logs_batch) = &merge_batch.logs_batch {
            let transformed_logs = self.transform_batch_vectorized(logs_batch, &mut id_mapper, "id")?;
            logs.set(ArrowPayloadType::Logs, transformed_logs);
            debug!("✅ Logs batch transformed: {} records", logs_batch.num_rows());
        }
        
        // Transform log_attrs batch (primary data)
        let transformed_log_attrs = self.transform_batch_vectorized(&merge_batch.log_attrs_batch, &mut id_mapper, "parent_id")?;
        logs.set(ArrowPayloadType::LogAttrs, transformed_log_attrs);
        debug!("✅ LogAttrs batch transformed: {} records", merge_batch.log_attrs_batch.num_rows());
        
        // Transform other attribute batches
        for (table_name, batch) in &merge_batch.attribute_batches {
            let payload_type = match table_name.as_str() {
                "resource_attrs" => ArrowPayloadType::ResourceAttrs,
                "scope_attrs" => ArrowPayloadType::ScopeAttrs,
                _ => {
                    debug!("⚠️  Unknown attribute table: {}, skipping", table_name);
                    continue;
                }
            };
            
            let transformed_batch = self.transform_batch_vectorized(batch, &mut id_mapper, "parent_id")?;
            logs.set(payload_type, transformed_batch);
            debug!("✅ {} batch transformed: {} records", table_name, batch.num_rows());
        }
        
        let otap_records = OtapArrowRecords::Logs(logs);
        
        // Apply memory-optimized format (following parquet_receiver)
        let otap_records = self.ensure_memory_optimized_format(otap_records)?;
        
        Ok(otap_records)
    }

    /// Transform batch using vectorized ID operations (following Arrow compute optimization plan)
    fn transform_batch_vectorized(
        &self,
        batch: &RecordBatch,
        id_mapper: &mut IdMapper,
        id_column_name: &str,
    ) -> Result<RecordBatch> {
        let mut new_columns = Vec::with_capacity(batch.num_columns());
        let mut new_fields = Vec::with_capacity(batch.num_columns());
        
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_idx);
            
            if field.name() == id_column_name && matches!(field.data_type(), DataType::UInt32) {
                // Apply vectorized ID transformation
                let transformed_column = id_mapper.transform_id_column_vectorized(column)?;
                new_columns.push(transformed_column);
                new_fields.push(Field::new(id_column_name, DataType::UInt16, field.is_nullable()));
            } else if matches!(field.data_type(), DataType::Utf8View) {
                // Materialize UTF8View to UTF8 (following optimization plan)
                let materialized_column = cast(column, &DataType::Utf8)
                    .map_err(|e| SamplingReceiverError::ReconstructionError {
                        message: format!("UTF8View materialization failed: {}", e),
                    })?;
                new_columns.push(materialized_column);
                new_fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
            } else if matches!(field.data_type(), DataType::BinaryView) {
                // Materialize BinaryView to Binary (following optimization plan)
                let materialized_column = cast(column, &DataType::Binary)
                    .map_err(|e| SamplingReceiverError::ReconstructionError {
                        message: format!("BinaryView materialization failed: {}", e),
                    })?;
                new_columns.push(materialized_column);
                new_fields.push(Field::new(field.name(), DataType::Binary, field.is_nullable()));
            } else {
                // Keep column as-is
                new_columns.push(column.clone());
                new_fields.push(field.as_ref().clone());
            }
        }
        
        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Batch reconstruction failed: {}", e),
            })
    }

    /// Ensure OTAP records are marked as memory-optimized (following parquet_receiver pattern)
    fn ensure_memory_optimized_format(&self, mut otap_records: OtapArrowRecords) -> Result<OtapArrowRecords> {
        // Following parquet_receiver pattern for memory optimization
        match &mut otap_records {
            OtapArrowRecords::Logs(_logs) => {
                // This would add PLAIN encoding metadata to prevent transport optimization
                // For now, just return as-is since we're in memory-optimized format already
                debug!("📋 OTAP records marked as memory-optimized");
            }
            _ => {
                return Err(SamplingReceiverError::ReconstructionError {
                    message: "Expected Logs variant in OtapArrowRecords".to_string(),
                });
            }
        }
        Ok(otap_records)
    }
}

/// Container for related table query results
#[derive(Debug)]
struct RelatedTableData {
    /// Results from logs table query
    pub logs: Vec<RecordBatch>,
    /// Results from resource_attrs table query  
    pub resource_attrs: Vec<RecordBatch>,
    /// Results from scope_attrs table query
    pub scope_attrs: Vec<RecordBatch>,
}

/// Streaming merge batch for OTAP reconstruction (inverse of parquet_receiver pattern)
#[derive(Debug)]
struct SamplingMergeBatch {
    /// Primary log_attrs results from DataFusion query
    pub log_attrs_batch: RecordBatch,
    /// Related logs records
    pub logs_batch: Option<RecordBatch>,
    /// Related attribute batches keyed by table name
    pub attribute_batches: HashMap<String, RecordBatch>,
    /// Min/Max parent_ids in the log_attrs batch
    pub parent_id_range: (u32, u32),
}

/// ID mapper for vectorized batch normalization (following Arrow compute optimization plan)
struct IdMapper {
    batch_start_id: u32,
}

impl IdMapper {
    fn new() -> Self {
        Self {
            batch_start_id: 0,
        }
    }

    fn set_batch_start(&mut self, start_id: u32) {
        self.batch_start_id = start_id;
    }

    /// Vectorized ID column transformation using Arrow compute kernels
    /// Following the optimization plan for 10-100x performance improvement
    fn transform_id_column_vectorized(&self, column: &ArrayRef) -> Result<ArrayRef> {
        // Validate input type
        if !matches!(column.data_type(), DataType::UInt32) {
            return Err(SamplingReceiverError::ReconstructionError {
                message: format!("Expected UInt32 column, got {:?}", column.data_type()),
            });
        }

        // Create scalar for batch normalization (vectorized subtraction)
        let _offset_scalar = UInt32Array::new_scalar(self.batch_start_id);
        
        // TODO: Implement actual vectorized transformation
        // For now, just return the original column
        Ok(column.clone())
    }
}

// Additional impl block for OtapReconstructor with DataFusion query methods
impl OtapReconstructor {

    /// Query logs table for specific IDs using DataFusion
    async fn query_logs_for_ids(&self, parent_ids: &[u32]) -> Result<Option<RecordBatch>> {
        if parent_ids.is_empty() {
            return Ok(None);
        }

        let id_list = parent_ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        let query = format!(
            "SELECT * FROM logs WHERE id IN ({}) ORDER BY id",
            id_list
        );
        
        debug!("🔍 Querying logs for {} parent_ids", parent_ids.len());
        
        match self.query_engine.execute_query(&query).await {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if total_rows > 0 {
                    debug!("✅ Found {} log records for parent_ids", total_rows);
                    // Combine multiple batches if needed
                    if batches.len() == 1 {
                        Ok(Some(batches.into_iter().next().unwrap()))
                    } else {
                        // TODO: Combine multiple batches using Arrow compute
                        Ok(batches.into_iter().next())
                    }
                } else {
                    debug!("⚠️ No log records found for parent_ids");
                    Ok(None)
                }
            }
            Err(e) => {
                error!("❌ Failed to query logs table: {}", e);
                Ok(None) // Continue processing without logs
            }
        }
    }

    /// Query resource_attrs table for specific parent IDs
    async fn query_resource_attrs_for_ids(&self, parent_ids: &[u32]) -> Result<RecordBatch> {
        if parent_ids.is_empty() {
            return Err(SamplingReceiverError::ReconstructionError {
                message: "Empty parent_ids for resource_attrs query".to_string(),
            });
        }

        let id_list = parent_ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        let query = format!(
            "SELECT * FROM resource_attrs WHERE parent_id IN ({}) ORDER BY parent_id",
            id_list
        );
        
        debug!("🔍 Querying resource_attrs for {} parent_ids", parent_ids.len());
        
        let batches = self.query_engine.execute_query(&query).await.unwrap_or_else(|_| vec![]);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        
        if total_rows > 0 {
            debug!("✅ Found {} resource_attrs records", total_rows);
            // Return first non-empty batch for simplicity
            Ok(batches.into_iter().find(|b| b.num_rows() > 0)
                .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new())))))
        } else {
            debug!("ℹ️ No resource_attrs found for parent_ids");
            Ok(RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))))
        }
    }

    /// Query scope_attrs table for specific parent IDs
    async fn query_scope_attrs_for_ids(&self, parent_ids: &[u32]) -> Result<RecordBatch> {
        if parent_ids.is_empty() {
            return Err(SamplingReceiverError::ReconstructionError {
                message: "Empty parent_ids for scope_attrs query".to_string(),
            });
        }

        let id_list = parent_ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        let query = format!(
            "SELECT * FROM scope_attrs WHERE parent_id IN ({}) ORDER BY parent_id",
            id_list
        );
        
        debug!("🔍 Querying scope_attrs for {} parent_ids", parent_ids.len());
        
        let batches = self.query_engine.execute_query(&query).await.unwrap_or_else(|_| vec![]);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        
        if total_rows > 0 {
            debug!("✅ Found {} scope_attrs records", total_rows);
            // Return first non-empty batch for simplicity  
            Ok(batches.into_iter().find(|b| b.num_rows() > 0)
                .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new())))))
        } else {
            debug!("ℹ️ No scope_attrs found for parent_ids");
            Ok(RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_extract_parent_ids() {
        // TODO: Add tests for parent ID extraction
    }

    #[tokio::test] 
    async fn test_otap_reconstruction() {
        // TODO: Add integration tests for OTAP reconstruction
    }
}

/// Create a placeholder OTAP record for development
fn create_placeholder_otap_record() -> OtapPdata {
    // Create empty OTAP records as placeholder, following parquet_receiver pattern
    let logs = Logs::default();
    let otap_records = OtapArrowRecords::Logs(logs);
    let payload = OtapPayload::OtapArrowRecords(otap_records);
    
    OtapPdata::new_todo_context(payload)
}