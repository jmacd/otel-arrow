// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! DataFusion-based query engine for reading and joining parquet files
//!
//! This module provides the core functionality for reading parquet files using
//! DataFusion and performing the multi-table joins needed to reconstruct OTAP data.

use crate::parquet_receiver::{
    config::SignalType, error::ParquetReceiverError, file_discovery::DiscoveredFile,
};
use arrow::record_batch::RecordBatch;
use datafusion::{
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::context::SessionContext,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

/// DataFusion-based query engine for parquet reconstruction
pub struct ParquetQueryEngine {}

/// Result of a query containing reconstructed data
#[derive(Debug)]
pub struct QueryResult {
    /// Main signal data
    pub main_records: Vec<RecordBatch>,
    /// Related attribute records by type
    pub related_records: HashMap<String, Vec<RecordBatch>>,
    /// Partition ID this data came from
    pub partition_id: Uuid,
    /// Signal type
    pub signal_type: SignalType,
}

impl ParquetQueryEngine {
    /// Create a new query engine instance
    pub fn new() -> Self {
        Self {}
    }

    /// Query and reconstruct data for a discovered file using ListingTable
    /// This method handles multiple files per partition automatically
    pub async fn query_file(
        &self,
        discovered_file: &DiscoveredFile,
    ) -> Result<QueryResult, ParquetReceiverError> {
        self.query_partition(
            &discovered_file.partition_id,
            &[discovered_file.signal_type.clone()],
        )
        .await
    }

    /// Query data for an entire partition using ListingTable
    /// This is the preferred method as it handles multiple files automatically
    pub async fn query_partition(
        &self,
        partition_id: &Uuid,
        signal_types: &[SignalType],
    ) -> Result<QueryResult, ParquetReceiverError> {
        // Create a new session context for this partition
        let ctx = SessionContext::new();

        // For now, use a hardcoded base directory - this should come from config in the future
        let base_directory = std::path::PathBuf::from(
            "/home/jmacd/src/otel/otel-arrow-tail-sampler/rust/otap-dataflow/output_parquet_files",
        );

        let mut main_records = Vec::new();
        let mut related_records = HashMap::new();
        let main_signal_type = signal_types[0].clone(); // Use first signal type as main

        // For logs, we need to query all 4 table types: logs, log_attrs, resource_attrs, scope_attrs
        if main_signal_type == SignalType::Logs {
            let table_mappings = [
                ("logs", true),           // Main table
                ("log_attrs", false),     // Related table  
                ("resource_attrs", false), // Related table
                ("scope_attrs", false),   // Related table
            ];

            for (table_type, is_main) in table_mappings {
                let signal_dir = base_directory.join(table_type);
                let partition_dir = signal_dir.join(format!("_part_id={}", partition_id));

                // Check if partition directory exists
                if !partition_dir.exists() {
                    log::debug!("Partition directory does not exist: {}", partition_dir.display());
                    continue; // Skip if partition doesn't exist for this table type
                }

                // Create ListingTable for this table type + partition combination
                let table_url = ListingTableUrl::parse(&format!("file://{}", partition_dir.display()))
                    .map_err(|e| ParquetReceiverError::Config(format!("Invalid table URL: {}", e)))?;

                let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension("parquet");

                let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

                // Infer schema and create table
                let config_with_schema = config
                    .infer_schema(&ctx.state())
                    .await
                    .map_err(|e| ParquetReceiverError::DataFusion(e))?;

                let listing_table = ListingTable::try_new(config_with_schema)
                    .map_err(|e| ParquetReceiverError::DataFusion(e))?;

                let table_name = table_type;
                let _ = ctx.register_table(table_name, Arc::new(listing_table))
                    .map_err(|e| ParquetReceiverError::DataFusion(e))?;

                // Query the table to get all records
                let df = ctx
                    .table(table_name)
                    .await
                    .map_err(|e| ParquetReceiverError::DataFusion(e))?;
                let records = df
                    .collect()
                    .await
                    .map_err(|e| ParquetReceiverError::DataFusion(e))?;

                log::debug!("Queried {} table: {} records in {} batches", table_type, 
                    records.iter().map(|r| r.num_rows()).sum::<usize>(), records.len());
                
                // Log schema information for each batch
                for (batch_idx, batch) in records.iter().enumerate() {
                    log::debug!("📋 Schema for {} batch {}: {} columns", table_type, batch_idx, batch.num_columns());
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        log::debug!("  Column {}: '{}' -> {:?}", col_idx, field.name(), field.data_type());
                    }
                }

                // Store records appropriately
                if is_main {
                    main_records = records;
                } else {
                    let _ = related_records.insert(table_type.to_string(), records);
                }
            }
        } else {
            // For traces/metrics (not implemented in hackathon scope)
            return Err(ParquetReceiverError::Config(format!(
                "Signal type {:?} not implemented in hackathon scope", main_signal_type
            )));
        }

        Ok(QueryResult {
            main_records,
            related_records,
            partition_id: *partition_id,
            signal_type: main_signal_type,
        })
    }

    /// Query multiple partitions for a time range (future enhancement)
    #[allow(dead_code)]
    pub async fn query_partitions(
        &self,
        partition_dirs: &[&Path],
        signal_type: &SignalType,
    ) -> Result<Vec<QueryResult>, ParquetReceiverError> {
        // TODO: Implement cross-partition querying for future enhancements
        // This would register multiple partition directories as a single logical table
        // and allow querying across time ranges
        let _ = (partition_dirs, signal_type);
        Err(ParquetReceiverError::Config(
            "Cross-partition querying not yet implemented".to_string(),
        ))
    }
}

impl Default for ParquetQueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Build SQL query for logs reconstruction with joins
fn build_logs_reconstruction_query(
    logs_table: &str,
    related_tables: &HashMap<String, String>,
) -> Result<String, ParquetReceiverError> {
    let mut query = format!(
        r#"
        SELECT 
            logs.id,
            logs.timestamp_unix_nano,
            logs.body,
            logs.severity_number,
            logs.severity_text,
            logs.flags,
            logs.trace_id,
            logs.span_id,
            logs.resource_id,
            logs.scope_id
        FROM {logs_table} AS logs"#
    );

    // Add LEFT JOINs for related tables if they exist
    if let Some(log_attrs_table) = related_tables.get("log_attrs") {
        query.push_str(&format!(
            r#"
        LEFT JOIN {log_attrs_table} AS log_attrs 
            ON logs.id = log_attrs.parent_id"#
        ));
    }

    if let Some(resource_attrs_table) = related_tables.get("resource_attrs") {
        query.push_str(&format!(
            r#"
        LEFT JOIN {resource_attrs_table} AS resource_attrs 
            ON logs.resource_id = resource_attrs.parent_id"#
        ));
    }

    if let Some(scope_attrs_table) = related_tables.get("scope_attrs") {
        query.push_str(&format!(
            r#"
        LEFT JOIN {scope_attrs_table} AS scope_attrs 
            ON logs.scope_id = scope_attrs.parent_id"#
        ));
    }

    query.push_str(" ORDER BY logs.id");

    Ok(query)
}

/// Convert signal type to main table name
fn signal_type_to_table_name(signal_type: &SignalType) -> String {
    match signal_type {
        SignalType::Logs => "logs".to_string(),
        SignalType::Traces => "spans".to_string(),
        SignalType::Metrics => "univariate_metrics".to_string(),
    }
}

/// Convert signal type to directory name
fn signal_type_to_directory(signal_type: &SignalType) -> String {
    match signal_type {
        SignalType::Logs => "logs".to_string(),
        SignalType::Traces => "traces".to_string(),
        SignalType::Metrics => "metrics".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_build_logs_reconstruction_query() {
        let logs_table = "logs_test";
        let mut related_tables = HashMap::new();
        let _ = related_tables.insert("log_attrs".to_string(), "log_attrs_test".to_string());
        let _ = related_tables.insert(
            "resource_attrs".to_string(),
            "resource_attrs_test".to_string(),
        );

        let query = build_logs_reconstruction_query(&logs_table, &related_tables).unwrap();

        assert!(query.contains("FROM logs_test AS logs"));
        assert!(query.contains("LEFT JOIN log_attrs_test AS log_attrs"));
        assert!(query.contains("LEFT JOIN resource_attrs_test AS resource_attrs"));
        assert!(query.contains("ORDER BY logs.id"));
    }

    #[test]
    fn test_signal_type_to_table_name() {
        assert_eq!(
            signal_type_to_table_name(&SignalType::Logs),
            "logs".to_string()
        );
        assert_eq!(
            signal_type_to_table_name(&SignalType::Traces),
            "spans".to_string()
        );
        assert_eq!(
            signal_type_to_table_name(&SignalType::Metrics),
            "univariate_metrics".to_string()
        );
    }

    #[tokio::test]
    async fn test_query_engine_creation() {
        let _engine = ParquetQueryEngine::new();
        // Just verify we can create the engine successfully
        // Real tests would require actual parquet files
        assert!(true);
    }
}
