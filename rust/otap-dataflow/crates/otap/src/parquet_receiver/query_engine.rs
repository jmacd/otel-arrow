// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! DataFusion-based query engine for reading and joining parquet files
//!
//! This module provides the core functionality for reading parquet files using
//! DataFusion and performing the multi-table joins needed to reconstruct OTAP data.

use crate::parquet_receiver::{
    config::SignalType,
    error::ParquetReceiverError,
    file_discovery::DiscoveredFile,
};
use arrow::record_batch::RecordBatch;
use datafusion::{
    execution::{context::SessionContext, options::ParquetReadOptions},
};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

/// DataFusion-based query engine for parquet reconstruction
pub struct ParquetQueryEngine {
    /// DataFusion session context
    ctx: SessionContext,
}

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
        let ctx = SessionContext::new();
        Self { ctx }
    }

    /// Query and reconstruct data for a discovered file
    pub async fn query_file(
        &self,
        discovered_file: &DiscoveredFile,
    ) -> Result<QueryResult, ParquetReceiverError> {
        // Create a new session context for this operation to avoid table name conflicts
        let ctx = SessionContext::new();

        // Register the main signal table
        let main_table_name = signal_type_to_table_name(&discovered_file.signal_type);
        ctx.register_parquet(
            &main_table_name,
            discovered_file.path.to_str().ok_or_else(|| {
                ParquetReceiverError::FileDiscovery(format!(
                    "Invalid file path: {:?}",
                    discovered_file.path
                ))
            })?,
            ParquetReadOptions::default(),
        )
        .await?;

        // Register related tables
        let mut related_table_info = HashMap::new();
        for related_file in &discovered_file.related_files {
            let table_name = format!("{}_{}", related_file.file_type, discovered_file.partition_id);
            ctx.register_parquet(
                &table_name,
                related_file.path.to_str().ok_or_else(|| {
                    ParquetReceiverError::FileDiscovery(format!(
                        "Invalid related file path: {:?}",
                        related_file.path
                    ))
                })?,
                ParquetReadOptions::default(),
            )
            .await?;
            let _ = related_table_info.insert(related_file.file_type.clone(), table_name);
        }

        // Execute the main query to get the signal data
        let main_df = ctx.table(&main_table_name).await?;
        let main_records = main_df.collect().await?;

        // Query related tables
        let mut related_records = HashMap::new();
        for (file_type, table_name) in related_table_info {
            let related_df = ctx.table(&table_name).await?;
            let records = related_df.collect().await?;
            let _ = related_records.insert(file_type, records);
        }

        Ok(QueryResult {
            main_records,
            related_records,
            partition_id: discovered_file.partition_id,
            signal_type: discovered_file.signal_type.clone(),
        })
    }

    /// Query and join related data for logs reconstruction
    /// This performs the multi-table join as described in the design document
    pub async fn query_logs_with_joins(
        &self,
        discovered_file: &DiscoveredFile,
    ) -> Result<QueryResult, ParquetReceiverError> {
        if discovered_file.signal_type != SignalType::Logs {
            return Err(ParquetReceiverError::Config(
                "query_logs_with_joins can only be used with logs signal type".to_string(),
            ));
        }

        // Create a new session context for this operation
        let ctx = SessionContext::new();

        // Register all tables for this partition
        let partition_suffix = discovered_file.partition_id.to_string().replace('-', "_");

        // Register main logs table
        let logs_table = format!("logs_{}", partition_suffix);
        ctx.register_parquet(
            &logs_table,
            discovered_file.path.to_str().ok_or_else(|| {
                ParquetReceiverError::FileDiscovery(format!(
                    "Invalid file path: {:?}",
                    discovered_file.path
                ))
            })?,
            ParquetReadOptions::default(),
        )
        .await?;

        // Register related tables
        let mut related_tables = HashMap::new();
        for related_file in &discovered_file.related_files {
            let table_name = format!("{}_{}", related_file.file_type, partition_suffix);
            ctx.register_parquet(
                &table_name,
                related_file.path.to_str().ok_or_else(|| {
                    ParquetReceiverError::FileDiscovery(format!(
                        "Invalid related file path: {:?}",
                        related_file.path
                    ))
                })?,
                ParquetReadOptions::default(),
            )
            .await?;
            let _ = related_tables.insert(related_file.file_type.clone(), table_name);
        }

        // Build the reconstruction query
        let query = build_logs_reconstruction_query(&logs_table, &related_tables)?;

        // Execute the query
        let df = ctx.sql(&query).await?;
        let main_records = df.collect().await?;

        // Also get the separate related records for further processing if needed
        let mut related_records = HashMap::new();
        for (file_type, table_name) in &related_tables {
            let related_df = ctx.table(table_name).await?;
            let records = related_df.collect().await?;
            let _ = related_records.insert(file_type.clone(), records);
        }

        Ok(QueryResult {
            main_records,
            related_records,
            partition_id: discovered_file.partition_id,
            signal_type: discovered_file.signal_type.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_build_logs_reconstruction_query() {
        let logs_table = "logs_test";
        let mut related_tables = HashMap::new();
        related_tables.insert("log_attrs".to_string(), "log_attrs_test".to_string());
        related_tables.insert(
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
        let engine = ParquetQueryEngine::new();
        // Just verify we can create the engine successfully
        // Real tests would require actual parquet files
        assert!(true);
    }
}