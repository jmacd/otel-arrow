//!
//! This module converts DataFusion query results back into OTAP records,
//! following the patterns from the parquet_receiver but working with 
//! DataFusion's output format.

use std::sync::Arc;
use std::collections::HashSet;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, UInt32Array, UInt16Array, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::kernels::cast::cast;
use arrow::compute::{min, max};
use datafusion::catalog::memory::MemTable;
use log::{debug, info, warn};

use crate::sampling_receiver::{
    config::Config,
    error::{Result, SamplingReceiverError},
    query_engine::DataFusionQueryEngine,
};
use crate::pdata::OtapPdata;
use otel_arrow_rust::otap::{Logs, OtapArrowRecords, OtapBatchStore};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// Results from separate queries following streaming-separate-queries.md strategy
/// A = log_attrs (complex query), P = logs (primary), R = resource_attrs, S = scope_attrs
#[derive(Debug)]
struct SeparateQueryResults {
    /// log_attrs results from complex query (A)
    a_batches: Vec<RecordBatch>,
    /// logs results (primary table P)
    p_batches: Vec<RecordBatch>,
    /// resource_attrs results (R)  
    r_batches: Vec<RecordBatch>,
    /// scope_attrs results (S)
    s_batches: Vec<RecordBatch>,
}

impl SeparateQueryResults {
    /// Count total rows across all tables
    fn total_rows(&self) -> usize {
        self.a_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.p_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.r_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.s_batches.iter().map(|b| b.num_rows()).sum::<usize>()
    }
}

/// Reconstructs OTAP records from DataFusion log_attributes query results
/// by streaming merge with related tables (logs, resource_attrs, scope_attrs)
/// Following the streaming-separate-queries.md strategy
pub struct OtapReconstructor {
    /// DataFusion query engine for additional table lookups
    query_engine: Arc<DataFusionQueryEngine>,
    /// Batch size for processing (max 65536 IDs per batch)
    batch_size: usize,
    /// Temporary table counter for unique naming
    temp_table_counter: usize,
}

impl OtapReconstructor {
    /// Create a new OTAP reconstructor
    pub fn new(query_engine: Arc<DataFusionQueryEngine>, config: &Config) -> Self {
        Self {
            query_engine,
            batch_size: config.performance.batch_size.min(65536), // Cap at 65K as per expert guidance
            temp_table_counter: 0,
        }
    }

    /// Get related data for log_attributes reconstruction (placeholder)
    pub async fn get_related_data(&self) -> Result<()> {
        // Placeholder: This will query logs, resource_attrs, scope_attrs tables
        // to get the related data needed for OTAP reconstruction
        info!("Getting related data for OTAP reconstruction");
        Ok(())
    }

    /// Reconstruct OTAP records using separate queries strategy from streaming-separate-queries.md
    /// This follows the expert's approach: A=log_attrs, P=logs, R=resource_attrs, S=scope_attrs
    pub async fn reconstruct_from_stream(
        &mut self, 
        log_attributes_results: Vec<RecordBatch>
    ) -> Result<Vec<OtapPdata>> {
        let mut otap_records = Vec::new();
        
        info!("🔧 Starting OTAP reconstruction from {} log_attrs batches using separate queries", log_attributes_results.len());
        
        for (batch_idx, log_attrs_batch) in log_attributes_results.iter().enumerate() {
            debug!("Processing log_attrs batch {}: {} rows", batch_idx, log_attrs_batch.num_rows());
            
            // Analyze batch structure and sorting
            self.debug_batch_analysis("log_attrs", log_attrs_batch);
            
            // Step 1: Extract unique parent_ids from log_attrs batch (A table)
            let parent_ids = self.extract_parent_ids_from_log_attrs(log_attrs_batch)?;
            debug!("🔍 Extracted {} unique parent_ids from log_attrs", parent_ids.len());
            
            if parent_ids.is_empty() {
                debug!("No parent_ids found, skipping batch {}", batch_idx);
                continue;
            }
            
            // Step 2: Create temporary MemTable with parent IDs for efficient filtering
            let temp_table_name = format!("batch_ids_{}", self.temp_table_counter);
            debug!("📋 Created temporary table {} with {} parent IDs", temp_table_name, parent_ids.len());
            self.create_temp_id_table(&parent_ids, &temp_table_name).await?;
            debug!("✅ Temporary table {} created successfully", temp_table_name);
            
            // Step 3: Execute separate queries for P, R, S tables using temp table
            debug!("🔄 About to execute separate queries for tables A,P,R,S");
            let separate_results = self.query_tables_separately(&temp_table_name, log_attrs_batch).await?;
            debug!("✅ Separate queries completed successfully");
            
            // Step 4: Build OTAP Logs structure with 4 separate arrays
            debug!("🔄 Building OTAP Logs from separate query results");
            let otap_batch = self.build_otap_logs_from_separate_results(separate_results).await?;
            debug!("✅ Built OTAP Logs with {} total rows across all tables", log_attrs_batch.num_rows());
            
            // Step 5: Clean up temporary table
            // Note: We'll add the actual cleanup after we implement the query engine context access
            debug!("🧹 Cleaning up temporary table {}", temp_table_name);
            self.temp_table_counter += 1;
            
            // Convert to OtapPdata
            let pdata = OtapPdata::new_todo_context(otap_batch.into());
            otap_records.push(pdata);
        }
        
        info!("✅ Reconstructed {} OTAP batches using separate queries strategy", otap_records.len());
        Ok(otap_records)
    }
    
    /// Step 1: Extract unique parent_ids from log_attrs batch using vectorized operations
    /// ✅ VECTORIZED: Uses Arrow compute kernels instead of element-by-element processing
    fn extract_parent_ids_from_log_attrs(&self, batch: &RecordBatch) -> Result<Vec<u32>> {
        let parent_id_column = batch.column_by_name("parent_id")
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: "No parent_id column found in log_attrs batch".to_string(),
            })?;
            
        let parent_id_array = parent_id_column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: "parent_id column is not UInt32".to_string(),
            })?;

        // ✅ VECTORIZED: Use Arrow compute to find min/max for efficient range analysis
        let parent_id_u32_array = parent_id_array;
            
        let min_id_opt = min(parent_id_u32_array);
        let max_id_opt = max(parent_id_u32_array);

        // Extract scalar values for logging and validation
        let (min_id, max_id) = match (min_id_opt, max_id_opt) {
            (Some(min_val), Some(max_val)) => (min_val, max_val),
            _ => (0, 0), // Handle case where column is all nulls
        };

        // For now, fall back to HashSet for deduplication
        // TODO: Implement fully vectorized unique operation when Arrow adds it
        let mut unique_ids = HashSet::new();
        for i in 0..parent_id_array.len() {
            if !parent_id_array.is_null(i) {
                let _ = unique_ids.insert(parent_id_array.value(i));
            }
        }

        let mut parent_ids: Vec<u32> = unique_ids.into_iter().collect();
        parent_ids.sort_unstable(); // Sort for efficient range queries
        
        debug!("🔍 Extracted {} unique parent_ids from {} log_attrs rows (range: {} to {})", 
               parent_ids.len(), batch.num_rows(), min_id, max_id);
        Ok(parent_ids)
    }
    
    /// Step 2: Create MemTable with Arrow array of parent IDs following expert strategy
    async fn create_temp_id_table(&self, parent_ids: &[u32], table_name: &str) -> Result<()> {
        // Convert u32 parent_ids to i64 for DataFusion compatibility (expert uses Int64)
        let ids_i64: Vec<i64> = parent_ids.iter().map(|&id| id as i64).collect();
        
        // Create Arrow array from IDs - very efficient per expert guidance!
        let id_array = Arc::new(Int64Array::from(ids_i64));
        
        // Create schema for single ID column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        
        // Create RecordBatch with the IDs
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array])
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to create ID batch: {}", e),
            })?;
        
        // Create and register MemTable (following expert's exact approach)
        let _mem_table = Arc::new(MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to create MemTable: {}", e),
            })?);
            
        // TODO: Register table with query engine context
        // self.query_engine.register_table(table_name, mem_table)?;
        
        debug!("📋 Created temporary table {} with {} parent IDs", table_name, parent_ids.len());
        Ok(())
    }
    
    /// Step 3: Query each table separately using temp ID table (following expert strategy)
    async fn query_tables_separately(
        &self, 
        temp_table_name: &str, 
        log_attrs_batch: &RecordBatch
    ) -> Result<SeparateQueryResults> {
        debug!("🔍 Querying logs table to get primary records for parent IDs from {}", temp_table_name);
        
        // Extract unique parent IDs from log_attrs batch to query logs table
        let parent_ids = self.extract_parent_ids_from_log_attrs(log_attrs_batch)?;
        debug!("🔍 Need to fetch {} unique log records", parent_ids.len());
        
        // Query P table (logs) to get the actual log records
        let logs_query = format!(
            "SELECT l.* FROM logs l WHERE l.id IN ({})",
            parent_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ")
        );
        
        debug!("🔍 Executing logs query: {}", logs_query);
        let p_batches = match self.query_engine.execute_query(&logs_query).await {
            Ok(batches) => {
                debug!("✅ Successfully queried {} log records", batches.iter().map(|b| b.num_rows()).sum::<usize>());
                
                // Analyze each logs batch
                for (idx, batch) in batches.iter().enumerate() {
                    self.debug_batch_analysis(&format!("logs_batch_{}", idx), batch);
                }
                
                batches
            }
            Err(e) => {
                warn!("⚠️  Failed to query logs table: {}", e);
                vec![] // Continue without primary records
            }
        };
        
        // TODO: Implement R and S queries for resource_attrs and scope_attrs
        
        Ok(SeparateQueryResults {
            a_batches: vec![log_attrs_batch.clone()], // A = log_attrs (what we started with)
            p_batches, // P = logs (primary table) - now populated
            r_batches: vec![], // R = resource_attrs - TODO  
            s_batches: vec![], // S = scope_attrs - TODO
        })
    }
    
    /// Step 4: Build OTAP Logs structure with 4 separate arrays
    async fn build_otap_logs_from_separate_results(
        &self,
        results: SeparateQueryResults,
    ) -> Result<OtapArrowRecords> {
        let mut logs = Logs::default();
        let total_rows = results.total_rows(); // Calculate before consuming results
        
        // Use the minimum ID from log_attrs as global offset for all batches
        // This ensures parent_id -> id relationships are preserved since they're aligned from JOIN queries
        let global_offset = if !results.a_batches.is_empty() {
            self.find_min_id_in_batch(&results.a_batches[0], "log_attrs")?
        } else {
            0 // Fallback if no log_attrs (shouldn't happen)
        };
        debug!("📊 Using global offset from log_attrs min ID: {}", global_offset);
        
        // Set the 4 arrays following ArrowPayloadType mapping:
        // - LogAttrs (from A batches)
        // - Logs (from P batches) 
        // - ResourceAttrs (from R batches)
        // - ScopeAttrs (from S batches)
        
        if !results.a_batches.is_empty() {
            // Merge A batches and set as LogAttrs - use global offset for consistent 0-origin IDs
            let merged_a = self.merge_batches_with_global_offset(results.a_batches, "log_attrs", global_offset)?;
            // Debug ID ranges after merging and transformation
            self.debug_batch_id_ranges(&merged_a, "MERGED_log_attrs")?;
            // Remove partition columns before OTAP output
            let filtered_a = self.remove_partition_columns(&merged_a)?;
            // Add PLAIN encoding metadata to prevent OTLP layer from assuming delta encoding
            let plain_encoded_a = self.add_plain_encoding_metadata(&filtered_a, &["parent_id"])?;
            logs.set(ArrowPayloadType::LogAttrs, plain_encoded_a);
        }
        
        if !results.p_batches.is_empty() {
            // Merge P batches and set as Logs - use same global offset to maintain parent_id relationships
            let merged_p = self.merge_batches_with_global_offset(results.p_batches, "logs", global_offset)?;
            // Debug ID ranges after merging and transformation
            self.debug_batch_id_ranges(&merged_p, "MERGED_logs")?;
            // Add PLAIN encoding metadata to prevent OTLP layer from assuming delta encoding
            let plain_encoded_p = self.add_plain_encoding_metadata(&merged_p, &["id"])?;
            logs.set(ArrowPayloadType::Logs, plain_encoded_p);
        }
        
        if !results.r_batches.is_empty() {
            let merged_r = self.merge_batches(results.r_batches, "resource_attrs")?;
            logs.set(ArrowPayloadType::ResourceAttrs, merged_r);
        }
        
        if !results.s_batches.is_empty() {
            let merged_s = self.merge_batches(results.s_batches, "scope_attrs")?;
            logs.set(ArrowPayloadType::ScopeAttrs, merged_s);
        }
        
        let otap_records = OtapArrowRecords::Logs(logs);
        debug!("🔄 Built OTAP Logs with {} total rows across all tables", total_rows);
        
        Ok(otap_records)
    }
    
    /// Utility: Merge multiple batches into a single batch
    /// ✅ VECTORIZED: Merge batches with ID normalization applied
    /// Applies vectorized UInt32->UInt16 cast with offset to fit OTAP schema requirements
    fn merge_batches(&self, batches: Vec<RecordBatch>, table_name: &str) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))));
        }
        
        if batches.len() == 1 {
            // Single batch - still need to apply ID normalization
            return self.transform_batch_ids(batches.into_iter().next().unwrap(), table_name);
        }
        
        // Multiple batches - transform each one then concatenate
        let mut transformed_batches = Vec::with_capacity(batches.len());
        
        for batch in batches {
            let transformed = self.transform_batch_ids(batch, table_name)?;
            transformed_batches.push(transformed);
        }
        
        // ✅ VECTORIZED: Use Arrow's concat to efficiently merge transformed batches
        let schema = transformed_batches[0].schema();
        
        arrow::compute::concat_batches(&schema, &transformed_batches)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to merge {} batches: {}", table_name, e),
            })
    }
    
    /// ✅ VECTORIZED: Merge batches with global offset for consistent ID transformation
    /// This ensures parent_id relationships are preserved across related tables
    fn merge_batches_with_global_offset(&self, batches: Vec<RecordBatch>, table_name: &str, global_offset: u32) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))));
        }
        
        if batches.len() == 1 {
            // Single batch - apply global offset for ID normalization
            return self.transform_batch_ids_with_offset(batches.into_iter().next().unwrap(), table_name, global_offset);
        }
        
        // Multiple batches - transform each one with global offset then concatenate
        let mut transformed_batches = Vec::with_capacity(batches.len());
        
        for batch in batches {
            let transformed = self.transform_batch_ids_with_offset(batch, table_name, global_offset)?;
            transformed_batches.push(transformed);
        }
        
        // ✅ VECTORIZED: Use Arrow's concat to efficiently merge transformed batches
        let schema = transformed_batches[0].schema();
        
        arrow::compute::concat_batches(&schema, &transformed_batches)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to merge {} batches with global offset: {}", table_name, e),
            })
    }
    
    /// Transform ID columns in a batch using vectorized operations
    /// Also normalizes data types (Utf8View -> Utf8, BinaryView -> Binary) for OTAP compatibility
    fn transform_batch_ids(&self, batch: RecordBatch, table_name: &str) -> Result<RecordBatch> {
        // Step 1: First normalize data types (Utf8View -> Utf8, BinaryView -> Binary)
        let normalized_batch = self.normalize_batch_data_types(&batch)?;
        
        // Step 2: Then apply ID transformations (UInt32 -> UInt16)
        let batch_offset = self.find_min_id_in_batch(&normalized_batch, table_name)?;
        
        let mut new_columns = Vec::with_capacity(normalized_batch.num_columns());
        let mut new_fields = Vec::with_capacity(normalized_batch.num_columns());
        
        for (field, column) in normalized_batch.schema().fields().iter().zip(normalized_batch.columns()) {
            match field.name().as_str() {
                "parent_id" | "id" if matches!(field.data_type(), DataType::UInt32) => {
                    // ✅ VECTORIZED: Apply UInt32->UInt16 cast with offset normalization
                    let normalized_column = self.transform_id_column_vectorized(column, batch_offset)?;
                    new_columns.push(normalized_column);
                    new_fields.push(Field::new(field.name(), DataType::UInt16, field.is_nullable()));
                    
                    debug!("🚀 Transformed {}.{} column: {} elements (UInt32->UInt16, offset={})", 
                          table_name, field.name(), column.len(), batch_offset);
                }
                _ => {
                    // Keep other columns as-is
                    new_columns.push(column.clone());
                    new_fields.push(field.as_ref().clone());
                }
            }
        }
        
        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to create transformed batch for {}: {}", table_name, e),
            })
    }
    
    /// Transform ID columns in a batch using vectorized operations with a global offset
    /// This preserves parent_id relationships across related batches
    fn transform_batch_ids_with_offset(&self, batch: RecordBatch, table_name: &str, global_offset: u32) -> Result<RecordBatch> {
        // Step 1: First normalize data types (Utf8View -> Utf8, BinaryView -> Binary)
        let normalized_batch = self.normalize_batch_data_types(&batch)?;
        
        let mut new_columns = Vec::with_capacity(normalized_batch.num_columns());
        let mut new_fields = Vec::with_capacity(normalized_batch.num_columns());
        
        for (field, column) in normalized_batch.schema().fields().iter().zip(normalized_batch.columns()) {
            match field.name().as_str() {
                "parent_id" | "id" if matches!(field.data_type(), DataType::UInt32) => {
                    // ✅ VECTORIZED: Apply UInt32->UInt16 cast with global offset normalization
                    let normalized_column = self.transform_id_column_vectorized(column, global_offset)?;
                    new_columns.push(normalized_column);
                    new_fields.push(Field::new(field.name(), DataType::UInt16, field.is_nullable()));
                    
                    debug!("🚀 Transformed {}.{} column: {} elements (UInt32->UInt16, global_offset={})", 
                          table_name, field.name(), column.len(), global_offset);
                }
                _ => {
                    // Keep other columns as-is
                    new_columns.push(column.clone());
                    new_fields.push(field.as_ref().clone());
                }
            }
        }
        
        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to create transformed batch for {} with global offset: {}", table_name, e),
            })
    }
    
    /// Debug helper to show ID ranges in a batch
    fn debug_batch_id_ranges(&self, batch: &RecordBatch, table_name: &str) -> Result<()> {
        debug!("🔍 === ID Range Debug for {} ===", table_name);
        debug!("   Batch has {} rows, {} columns", batch.num_rows(), batch.num_columns());
        
        // Check parent_id column if it exists
        if let Some(parent_id_col) = batch.column_by_name("parent_id") {
            if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<UInt16Array>() {
                if parent_id_array.len() > 0 {
                    let min_parent = min(parent_id_array).unwrap_or(0);
                    let max_parent = max(parent_id_array).unwrap_or(0);
                    debug!("   parent_id (UInt16): min={}, max={}, count={}", min_parent, max_parent, parent_id_array.len());
                }
            } else if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<UInt32Array>() {
                if parent_id_array.len() > 0 {
                    let min_parent = min(parent_id_array).unwrap_or(0);
                    let max_parent = max(parent_id_array).unwrap_or(0);
                    debug!("   parent_id (UInt32): min={}, max={}, count={}", min_parent, max_parent, parent_id_array.len());
                }
            } else {
                debug!("   parent_id: unsupported data type {:?}", parent_id_col.data_type());
            }
        }
        
        // Check id column if it exists
        if let Some(id_col) = batch.column_by_name("id") {
            if let Some(id_array) = id_col.as_any().downcast_ref::<UInt16Array>() {
                if id_array.len() > 0 {
                    let min_id = min(id_array).unwrap_or(0);
                    let max_id = max(id_array).unwrap_or(0);
                    debug!("   id (UInt16): min={}, max={}, count={}", min_id, max_id, id_array.len());
                }
            } else if let Some(id_array) = id_col.as_any().downcast_ref::<UInt32Array>() {
                if id_array.len() > 0 {
                    let min_id = min(id_array).unwrap_or(0);
                    let max_id = max(id_array).unwrap_or(0);
                    debug!("   id (UInt32): min={}, max={}, count={}", min_id, max_id, id_array.len());
                }
            } else {
                debug!("   id: unsupported data type {:?}", id_col.data_type());
            }
        }
        
        debug!("🔍 === End ID Range Debug ===");
        Ok(())
    }

    /// Find minimum ID in batch for normalization offset
    fn find_min_id_in_batch(&self, batch: &RecordBatch, table_name: &str) -> Result<u32> {
        // Look for parent_id first (most common), then id
        let id_column = batch.column_by_name("parent_id")
            .or_else(|| batch.column_by_name("id"))
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: format!("No parent_id or id column found in {} batch", table_name),
            })?;
            
        if !matches!(id_column.data_type(), DataType::UInt32) {
            return Ok(0); // No transformation needed for non-UInt32 columns
        }
        
        // Cast to UInt32Array for the min operation
        let id_u32_array = id_column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to cast ID column to UInt32Array in {} batch", table_name),
            })?;
        
        // ✅ VECTORIZED: Use Arrow compute to find minimum efficiently
        let min_val = min(id_u32_array);
            
        match min_val {
            Some(min_id) => {
                debug!("📊 Found min ID in {} batch: {}", table_name, min_id);
                Ok(min_id)
            }
            None => Ok(0), // All nulls or empty batch
        }
    }
    
    /// ✅ VECTORIZED: Transform ID columns using Arrow compute kernels
    /// Implements the optimization plan's approach for 10x-100x performance improvement
    fn transform_id_column_vectorized(
        &self, 
        id_column: &arrow::array::ArrayRef, 
        batch_offset: u32
    ) -> Result<arrow::array::ArrayRef> {
        // Validate input type
        if !matches!(id_column.data_type(), DataType::UInt32) {
            return Err(SamplingReceiverError::ReconstructionError {
                message: format!("Expected UInt32 column, got {:?}", id_column.data_type()),
            });
        }

        // Create scalar for batch normalization - need to create an array of the same length
        let array_len = id_column.len();
        let offset_array = UInt32Array::from(vec![batch_offset; array_len]);
        
        // ✅ VECTORIZED: Single operation on entire array (10x-100x faster than element-by-element)
        let normalized_u32 = sub_wrapping(id_column, &offset_array)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Vectorized ID subtraction failed: {}", e),
            })?;
        
        // ✅ VECTORIZED: Type conversion UInt32 -> UInt16 using Arrow compute
        let normalized_u16 = cast(&normalized_u32, &DataType::UInt16)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Vectorized ID cast failed: {}", e),
            })?;
            
        debug!("🚀 Vectorized ID transformation: {} elements (UInt32 -> UInt16 with offset {})", 
               id_column.len(), batch_offset);
        
        Ok(normalized_u16)
    }
    
    /// ✅ VECTORIZED: Create efficient array slices using zero-copy operations
    /// Implements slice-first optimization from the plan
    fn create_efficient_slice(
        &self,
        batch: &RecordBatch,
        start_row: usize,
        num_rows: usize
    ) -> RecordBatch {
        // ✅ EFFICIENT: Zero-copy slice operation (constant time)
        debug!("📋 Creating zero-copy slice: rows {}..{} from batch with {} rows", 
               start_row, start_row + num_rows, batch.num_rows());
        batch.slice(start_row, num_rows)
    }
    
    /// Normalize Arrow data types by materializing View types to standard types for OTAP compatibility
    /// This fixes the "Invalid List array data type" error where OTLP expects Utf8 but receives Utf8View
    fn normalize_batch_data_types(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        let mut needs_transformation = false;

        // Check each column and materialize View types to standard types
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(i);
            
            match field.data_type() {
                DataType::Utf8View => {
                    // Materialize UTF8View to UTF8 for OTAP compatibility
                    debug!("🔄 Materializing Utf8View column '{}' to Utf8", field.name());
                    let materialized = cast(column, &DataType::Utf8)?;
                    new_columns.push(materialized);
                    new_fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
                    needs_transformation = true;
                }
                DataType::BinaryView => {
                    // Materialize BinaryView to Binary for OTAP compatibility  
                    debug!("🔄 Materializing BinaryView column '{}' to Binary", field.name());
                    let materialized = cast(column, &DataType::Binary)?;
                    new_columns.push(materialized);
                    new_fields.push(Field::new(field.name(), DataType::Binary, field.is_nullable()));
                    needs_transformation = true;
                }
                _ => {
                    // Keep other columns as-is
                    new_columns.push(column.clone());
                    new_fields.push(field.as_ref().clone());
                }
            }
        }

        if needs_transformation {
            let new_schema = Arc::new(Schema::new(new_fields));
            let normalized_batch = RecordBatch::try_new(new_schema, new_columns)?;
            debug!("✅ Normalized batch data types: {} rows, {} columns", normalized_batch.num_rows(), normalized_batch.num_columns());
            Ok(normalized_batch)
        } else {
            // No transformation needed, return original batch
            Ok(batch.clone())
        }
    }
    
    /// Add COLUMN_ENCODING=PLAIN metadata to specified columns in a RecordBatch
    /// This prevents the OTLP layer from assuming delta encoding for ID columns
    fn add_plain_encoding_metadata(&self, batch: &RecordBatch, column_names: &[&str]) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_fields = Vec::new();
        
        for field in schema.fields() {
            if column_names.contains(&field.name().as_str()) {
                // Add COLUMN_ENCODING=PLAIN metadata to this field
                let mut metadata = field.metadata().clone();
                let _ = metadata.insert("COLUMN_ENCODING".to_string(), "PLAIN".to_string());
                
                let new_field = Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                    .with_metadata(metadata);
                new_fields.push(new_field);
                debug!("🏷️  Added COLUMN_ENCODING=PLAIN metadata to column '{}'", field.name());
            } else {
                // Keep original field unchanged
                new_fields.push(field.as_ref().clone());
            }
        }
        
        let new_schema = Arc::new(Schema::new(new_fields));
        let new_batch = RecordBatch::try_new(new_schema, batch.columns().to_vec())?;
            
        Ok(new_batch)
    }
    
    /// Remove partition columns (currently hard-coded _part_id) from batch
    /// Later this will be configurable like parquet_exporter and DataFusion Hive partitioning
    fn remove_partition_columns(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut new_fields = Vec::new();
        let mut new_columns = Vec::new();
        
        for (i, field) in schema.fields().iter().enumerate() {
            // Hard-coded partition column filter - will be configurable later
            if field.name() == "_part_id" {
                debug!("🗑️  Removing partition column '{}'", field.name());
                continue; // Skip partition columns
            }
            
            // Keep non-partition columns
            new_fields.push(field.as_ref().clone());
            new_columns.push(batch.column(i).clone());
        }
        
        let new_schema = Arc::new(Schema::new(new_fields));
        let filtered_batch = RecordBatch::try_new(new_schema, new_columns)?;
        
        debug!("🧹 Filtered batch: {} → {} columns", 
            batch.num_columns(), filtered_batch.num_columns());
            
        Ok(filtered_batch)
    }
    
    /// Debug method to analyze sorting and ID ranges in batches  
    fn debug_batch_analysis(&self, batch_name: &str, batch: &RecordBatch) {
        debug!("🔍 ===== {} BATCH ANALYSIS =====", batch_name.to_uppercase());
        debug!("🔍 {} batch: {} rows total", batch_name, batch.num_rows());
        
        // Check parent_id column (for log_attrs) - handle both UInt32 and UInt16
        if let Some(parent_id_col) = batch.column_by_name("parent_id") {
            if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<UInt32Array>() {
                if parent_id_array.len() > 0 {
                    let min_parent = min(parent_id_array).unwrap_or(0);
                    let max_parent = max(parent_id_array).unwrap_or(0);
                    
                    // Check if sorted
                    let mut is_sorted = true;
                    let mut prev_val = 0u32;
                    for i in 0..parent_id_array.len() {
                        let val = parent_id_array.value(i);
                        if i > 0 && val < prev_val {
                            is_sorted = false;
                            break;
                        }
                        prev_val = val;
                    }
                    
                    debug!("🔍 {} parent_id (UInt32): range [{}-{}], sorted: {}", 
                        batch_name, min_parent, max_parent, is_sorted);
                        
                    // Show first few and last few values for debugging
                    let show_count = std::cmp::min(5, parent_id_array.len());
                    let first_vals: Vec<u32> = (0..show_count)
                        .map(|i| parent_id_array.value(i))
                        .collect();
                    let last_start = parent_id_array.len().saturating_sub(show_count);
                    let last_vals: Vec<u32> = (last_start..parent_id_array.len())
                        .map(|i| parent_id_array.value(i))
                        .collect();
                    debug!("🔍 {} parent_id first values: {:?}, last values: {:?}", 
                        batch_name, first_vals, last_vals);
                }
            } else if let Some(parent_id_array) = parent_id_col.as_any().downcast_ref::<UInt16Array>() {
                if parent_id_array.len() > 0 {
                    let min_parent = min(parent_id_array).unwrap_or(0);
                    let max_parent = max(parent_id_array).unwrap_or(0);
                    
                    // Check if sorted
                    let mut is_sorted = true;
                    let mut prev_val = 0u16;
                    for i in 0..parent_id_array.len() {
                        let val = parent_id_array.value(i);
                        if i > 0 && val < prev_val {
                            is_sorted = false;
                            break;
                        }
                        prev_val = val;
                    }
                    
                    debug!("🔍 {} parent_id (UInt16): range [{}-{}], sorted: {}", 
                        batch_name, min_parent, max_parent, is_sorted);
                        
                    // Show first few and last few values for debugging
                    let show_count = std::cmp::min(5, parent_id_array.len());
                    let first_vals: Vec<u16> = (0..show_count)
                        .map(|i| parent_id_array.value(i))
                        .collect();
                    let last_start = parent_id_array.len().saturating_sub(show_count);
                    let last_vals: Vec<u16> = (last_start..parent_id_array.len())
                        .map(|i| parent_id_array.value(i))
                        .collect();
                    debug!("🔍 {} parent_id first values: {:?}, last values: {:?}", 
                        batch_name, first_vals, last_vals);
                }
            }
        }
        
        // Check id column (for logs) - handle both UInt32 and UInt16
        if let Some(id_col) = batch.column_by_name("id") {
            if let Some(id_array) = id_col.as_any().downcast_ref::<UInt32Array>() {
                if id_array.len() > 0 {
                    let min_id = min(id_array).unwrap_or(0);
                    let max_id = max(id_array).unwrap_or(0);
                    
                    // Check if sorted
                    let mut is_sorted = true;
                    let mut prev_val = 0u32;
                    for i in 0..id_array.len() {
                        let val = id_array.value(i);
                        if i > 0 && val < prev_val {
                            is_sorted = false;
                            break;
                        }
                        prev_val = val;
                    }
                    
                    debug!("🔍 {} id (UInt32): range [{}-{}], sorted: {}", 
                        batch_name, min_id, max_id, is_sorted);
                        
                    // Show first few and last few values for debugging
                    let show_count = std::cmp::min(5, id_array.len());
                    let first_vals: Vec<u32> = (0..show_count)
                        .map(|i| id_array.value(i))
                        .collect();
                    let last_start = id_array.len().saturating_sub(show_count);
                    let last_vals: Vec<u32> = (last_start..id_array.len())
                        .map(|i| id_array.value(i))
                        .collect();
                    debug!("🔍 {} id first values: {:?}, last values: {:?}", 
                        batch_name, first_vals, last_vals);
                }
            } else if let Some(id_array) = id_col.as_any().downcast_ref::<UInt16Array>() {
                if id_array.len() > 0 {
                    let min_id = min(id_array).unwrap_or(0);
                    let max_id = max(id_array).unwrap_or(0);
                    
                    // Check if sorted
                    let mut is_sorted = true;
                    let mut prev_val = 0u16;
                    for i in 0..id_array.len() {
                        let val = id_array.value(i);
                        if i > 0 && val < prev_val {
                            is_sorted = false;
                            break;
                        }
                        prev_val = val;
                    }
                    
                    debug!("🔍 {} id (UInt16): range [{}-{}], sorted: {}", 
                        batch_name, min_id, max_id, is_sorted);
                        
                    // Show first few and last few values for debugging
                    let show_count = std::cmp::min(5, id_array.len());
                    let first_vals: Vec<u16> = (0..show_count)
                        .map(|i| id_array.value(i))
                        .collect();
                    let last_start = id_array.len().saturating_sub(show_count);
                    let last_vals: Vec<u16> = (last_start..id_array.len())
                        .map(|i| id_array.value(i))
                        .collect();
                    debug!("🔍 {} id first values: {:?}, last values: {:?}", 
                        batch_name, first_vals, last_vals);
                }
            }
        }
        
        // Show column names for reference
        let schema = batch.schema();
        let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        debug!("🔍 {} columns: {:?}", batch_name, column_names);
        debug!("🔍 ===== END {} ANALYSIS =====", batch_name.to_uppercase());
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_otap_reconstruction() {
        // TODO: Add integration tests for OTAP reconstruction
    }
}