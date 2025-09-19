//! OTAP Record Reconstructor
//!
//! This module converts DataFusion query results back into OTAP records,
//! following the patterns from the parquet_receiver but working with 
//! DataFusion's output format.

use arrow::record_batch::RecordBatch;
use arrow::array::{Array, UInt32Array};
use datafusion::common::Result as DataFusionResult;
use log::{debug, info, error};
use std::sync::Arc;
use std::collections::HashSet;

use crate::sampling_receiver::{
    config::Config,
    error::{Result, SamplingReceiverError},
    query_engine::DataFusionQueryEngine,
};
use crate::pdata::{OtapPdata, OtapPayload, Context};
use otel_arrow_rust::otap::{Logs, OtapArrowRecords};
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
                .map_err(|e| crate::sampling_receiver::error::SamplingReceiverError::query_error(
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
                .ok_or_else(|| crate::sampling_receiver::error::SamplingReceiverError::query_error(
                    "No parent_id column found in log_attributes results"
                ))?;

            let parent_id_array = parent_id_column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| crate::sampling_receiver::error::SamplingReceiverError::query_error(
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
    async fn perform_streaming_merge(
        &self,
        log_attributes_results: &[RecordBatch],
        _related_data: &RelatedTableData,
    ) -> Result<Vec<OtapPdata>> {
        info!("🔀 Starting streaming merge reconstruction");

        // For now, return placeholder OTAP records
        // TODO: Implement actual streaming merge following parquet_receiver patterns
        let mut otap_records = Vec::new();

        // Count total attributes for sizing
        let total_attributes: usize = log_attributes_results.iter()
            .map(|batch| batch.num_rows())
            .sum();

        // Create placeholder OTAP records
        // In real implementation, this would do the streaming merge reconstruction
        for _i in 0..std::cmp::min(10, total_attributes) {
            let otap_record = create_placeholder_otap_record(); // Use the new function
            otap_records.push(otap_record);
        }

        info!("🎯 Streaming merge complete: created {} OTAP records", otap_records.len());
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