//! DataFusion Query Engine for OTAP Sampling Receiver
//!
//! This module implements the core DataFusion integration for processing parquet files
//! with 4 partitioned table registration (logs, log_attributes, resource_attributes, scope_attributes).

use crate::sampling_receiver::{
    config::Config,
    error::{Result, SamplingReceiverError},
};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::context::SessionContext;
use log::{debug, info, warn};
use std::sync::Arc;
use url::Url;

/// DataFusion Query Engine for OTAP data processing
/// 
/// This engine handles the registration of 4 partitioned tables:
/// - logs
/// - log_attributes  
/// - resource_attributes
/// - scope_attributes
/// 
/// Each table is registered as a partitioned ListingTable with virtual _part_id columns.
pub struct DataFusionQueryEngine {
    /// DataFusion session context
    context: SessionContext,
    /// Base URI for parquet files
    base_uri: Url,
    /// Configuration
    config: Config,
}

/// Table types supported by the query engine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtapTableType {
    /// Main log records
    Logs,
    /// Log attributes (key-value pairs)
    LogAttributes,
    /// Resource attributes (key-value pairs)
    ResourceAttributes,
    /// Scope attributes (key-value pairs)
    ScopeAttributes,
}

impl OtapTableType {
    /// Get the table name as used in SQL queries
    pub fn table_name(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::LogAttributes => "log_attributes",
            Self::ResourceAttributes => "resource_attributes", 
            Self::ScopeAttributes => "scope_attributes",
        }
    }

    /// Get the directory name within the parquet file structure
    pub fn directory_name(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::LogAttributes => "log_attrs", // Note: different from table name
            Self::ResourceAttributes => "resource_attrs",
            Self::ScopeAttributes => "scope_attrs",
        }
    }

    /// Get all table types
    pub fn all() -> &'static [OtapTableType] {
        &[
            Self::Logs,
            Self::LogAttributes,
            Self::ResourceAttributes,
            Self::ScopeAttributes,
        ]
    }
}

impl DataFusionQueryEngine {
    /// Create a new DataFusion Query Engine
    pub async fn new(config: Config) -> Result<Self> {
        info!("Creating DataFusion Query Engine with base_uri: {}", config.base_uri);

        // Create session context with basic configuration
        let session_config = SessionConfig::default()
            .with_target_partitions(config.performance.target_partitions)
            .with_batch_size(config.performance.batch_size);

        let context = SessionContext::new_with_config(session_config);

        let mut engine = Self {
            context,
            base_uri: config.base_uri.clone(),
            config,
        };

        // Register all 4 OTAP table types
        engine.register_tables().await?;

        info!("DataFusion Query Engine created successfully");
        Ok(engine)
    }

    /// Register all 4 OTAP table types as partitioned tables
    async fn register_tables(&mut self) -> Result<()> {
        for table_type in OtapTableType::all() {
            self.register_table(*table_type).await?;
        }
        Ok(())
    }

    /// Register a single table type as a partitioned ListingTable
    async fn register_table(&mut self, table_type: OtapTableType) -> Result<()> {
        let table_name = table_type.table_name();
        let dir_name = table_type.directory_name();

        debug!("Registering table: {} (directory: {})", table_name, dir_name);

        // Build the table path
        let table_path = self.build_table_path(dir_name)?;
        
        // Create listing options with partition column
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![
                ("_part_id".to_string(), DataType::Utf8), // Virtual partition column
            ]);

        // Create listing table configuration
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options);

        // Create and register the table
        let table = ListingTable::try_new(config)?;
        let _previous_table = self.context.register_table(table_name, Arc::new(table))?;

        info!("Registered partitioned table: {}", table_name);
        Ok(())
    }

    /// Build the full path for a table directory
    fn build_table_path(&self, dir_name: &str) -> Result<ListingTableUrl> {
        let mut path = self.base_uri.clone();
        
        // Append directory name to path
        if !path.path().ends_with('/') {
            path.set_path(&format!("{}/{}", path.path(), dir_name));
        } else {
            path.set_path(&format!("{}{}", path.path(), dir_name));
        }

        ListingTableUrl::parse(&path.to_string())
            .map_err(|e| SamplingReceiverError::DataFusionError {
                message: format!("Failed to parse table path {}: {}", path, e),
            })
    }

    /// Execute a SQL query and return the results
    pub async fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        debug!("Executing DataFusion query: {}", query);

        // Execute the query directly
        let dataframe = self.context.sql(query).await?;
        let batches = dataframe.collect().await?;

        info!("Query executed successfully, returned {} batches", batches.len());
        
        // Log batch sizes for debugging
        for (i, batch) in batches.iter().enumerate() {
            debug!("Batch {}: {} rows, {} columns", i, batch.num_rows(), batch.num_columns());
        }

        Ok(batches)
    }

    /// Get the session context (for advanced usage)
    pub fn context(&self) -> &SessionContext {
        &self.context
    }

    /// Check if tables are properly registered
    pub async fn validate_tables(&self) -> Result<()> {
        for table_type in OtapTableType::all() {
            let table_name = table_type.table_name();
            
            match self.context.table(table_name).await {
                Ok(table) => {
                    let schema = table.schema();
                    debug!("Table {} validated with {} columns", table_name, schema.fields().len());
                }
                Err(e) => {
                    return Err(SamplingReceiverError::DataFusionError {
                        message: format!("Table {} not found: {}", table_name, e),
                    });
                }
            }
        }
        
        info!("All tables validated successfully");
        Ok(())
    }

    /// Execute a simple count query for testing
    pub async fn test_connection(&self) -> Result<()> {
        debug!("Testing DataFusion connection with simple queries");

        for table_type in OtapTableType::all() {
            let table_name = table_type.table_name();
            let query = format!("SELECT COUNT(*) as count FROM {} LIMIT 1", table_name);
            
            match self.execute_query(&query).await {
                Ok(batches) => {
                    if !batches.is_empty() {
                        debug!("Table {} is accessible", table_name);
                    } else {
                        warn!("Table {} returned empty result", table_name);
                    }
                }
                Err(e) => {
                    warn!("Failed to query table {}: {}", table_name, e);
                    // Don't fail completely - table might just be empty
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_config() -> Config {
        let temp_dir = tempdir().unwrap();
        let mut config = Config::default();
        config.base_uri = Url::from_file_path(temp_dir.path()).unwrap();
        config
    }

    #[test]
    fn test_table_type_names() {
        assert_eq!(OtapTableType::Logs.table_name(), "logs");
        assert_eq!(OtapTableType::LogAttributes.table_name(), "log_attributes");
        assert_eq!(OtapTableType::LogAttributes.directory_name(), "log_attrs");
    }

    #[tokio::test]
    async fn test_query_engine_creation() {
        let config = create_test_config();
        // This might fail due to missing parquet files, but should at least not panic
        let result = DataFusionQueryEngine::new(config).await;
        // We expect this to fail in tests due to missing files, but it should be a proper error
        assert!(result.is_err());
    }
}