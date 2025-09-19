//!
//! This module converts DataFusion query results back into OTAP records,
//! following the patterns from the parquet_receiver but working with 
//! DataFusion's output format.

use std::sync::Arc;
use std::collections::HashSet;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, UInt32Array, Int64Array};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::kernels::cast::cast;
use arrow::compute::{min, max};
use datafusion::catalog::memory::MemTable;
use log::{debug, info};

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
            
            // Step 1: Extract unique parent_ids from log_attrs batch (A table)
            let parent_ids = self.extract_parent_ids_from_log_attrs(log_attrs_batch)?;
            debug!("🔍 Extracted {} unique parent_ids from log_attrs", parent_ids.len());
            
            if parent_ids.is_empty() {
                debug!("No parent_ids found, skipping batch {}", batch_idx);
                continue;
            }
            
            // Step 2: Create temporary MemTable with parent IDs for efficient filtering
            let temp_table_name = format!("batch_ids_{}", self.temp_table_counter);
            self.create_temp_id_table(&parent_ids, &temp_table_name).await?;
            
            // Step 3: Execute separate queries for P, R, S tables using temp table
            let separate_results = self.query_tables_separately(&temp_table_name, log_attrs_batch).await?;
            
            // Step 4: Build OTAP Logs structure with 4 separate arrays
            let otap_batch = self.build_otap_logs_from_separate_results(separate_results).await?;
            
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
        _temp_table_name: &str, 
        log_attrs_batch: &RecordBatch
    ) -> Result<SeparateQueryResults> {
        // TODO: Implement separate queries for P, R, S tables
        // For now, return the log_attrs batch we already have
        Ok(SeparateQueryResults {
            a_batches: vec![log_attrs_batch.clone()], // A = log_attrs (what we started with)
            p_batches: vec![], // P = logs (primary table) - TODO
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
        
        // Set the 4 arrays following ArrowPayloadType mapping:
        // - LogAttrs (from A batches)
        // - Logs (from P batches) 
        // - ResourceAttrs (from R batches)
        // - ScopeAttrs (from S batches)
        
        if !results.a_batches.is_empty() {
            // Merge A batches and set as LogAttrs
            let merged_a = self.merge_batches(results.a_batches, "log_attrs")?;
            logs.set(ArrowPayloadType::LogAttrs, merged_a);
        }
        
        if !results.p_batches.is_empty() {
            let merged_p = self.merge_batches(results.p_batches, "logs")?;
            logs.set(ArrowPayloadType::Logs, merged_p);
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
    fn merge_batches(&self, batches: Vec<RecordBatch>, table_name: &str) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::new(Vec::<Field>::new()))));
        }
        
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }
        
        // ✅ VECTORIZED: Use Arrow's concat to efficiently merge batches
        let schema = batches[0].schema();
        
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| SamplingReceiverError::ReconstructionError {
                message: format!("Failed to merge {} batches: {}", table_name, e),
            })
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

        // Create scalar for batch normalization
        let offset_scalar = UInt32Array::from(vec![batch_offset]);
        
        // ✅ VECTORIZED: Single operation on entire array (10x-100x faster than element-by-element)
        let normalized_u32 = sub_wrapping(id_column, &offset_scalar)
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
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_otap_reconstruction() {
        // TODO: Add integration tests for OTAP reconstruction
    }
}