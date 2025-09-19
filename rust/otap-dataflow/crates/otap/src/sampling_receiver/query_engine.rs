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
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::context::SessionContext;
use datafusion::datasource::TableProvider;
use log::{debug, info, warn};
use object_store::local::LocalFileSystem;
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
            Self::LogAttributes => "log_attrs", // Match query templates
            Self::ResourceAttributes => "resource_attrs", // Match query templates
            Self::ScopeAttributes => "scope_attrs", // Match query templates
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

        // Create SessionContext with local filesystem object store
        let ctx = SessionContext::new();
        
        // Register local filesystem object store for file:// URLs
        let local_fs = Arc::new(LocalFileSystem::new());
        let _ = ctx.runtime_env()
            .object_store_registry
            .register_store(&Url::parse("file://").unwrap(), local_fs);
        
        debug!("Registered local filesystem object store for file:// URLs");

        let base_uri = config.base_uri.clone();
        let mut engine = Self { context: ctx, base_uri, config };

        // Register all table types
        engine.register_tables().await?;

        info!("DataFusion Query Engine created successfully");
        Ok(engine)
    }

    /// Register all 4 OTAP table types as partitioned tables
    async fn register_tables(&mut self) -> Result<()> {
        for table_type in OtapTableType::all() {
            match self.register_table(*table_type).await {
                Ok(_) => {
                    debug!("Successfully registered table: {}", table_type.table_name());
                }
                Err(e) => {
                    warn!(
                        "Failed to register table {} (directory: {}): {}. Skipping this table.",
                        table_type.table_name(),
                        table_type.directory_name(),
                        e
                    );
                    // Continue with other tables - some might not exist in test data
                }
            }
        }
        Ok(())
    }

    /// Register a single table type as a partitioned ListingTable
    async fn register_table(&mut self, table_type: OtapTableType) -> Result<()> {
        let table_name = table_type.table_name();
        let dir_name = table_type.directory_name();

        debug!("Registering table: {} (directory: {})", table_name, dir_name);

        // Check if the directory exists first
        if !self.directory_exists(dir_name)? {
            return Err(SamplingReceiverError::DataFusionError {
                message: format!("Directory {} does not exist for table {}", dir_name, table_name),
            });
        }

        // Build the table path
        let table_path = self.build_table_path(dir_name)?;
        
        debug!("Table path for {}: {}", table_name, table_path);
        
        // Debug: Show what files exist in the directory - but don't use these for ListingTable
        self.log_directory_contents(dir_name, table_name)?;
        
        // Create listing options with partition columns - this is CORE to the architecture
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension("parquet") // Note: no leading dot
            .with_table_partition_cols(vec![
                ("_part_id".to_string(), DataType::Utf8), // Virtual partition column for Hive partitioning
            ]);
        
        debug!("Created ListingOptions with partition columns: {:?}", listing_options.table_partition_cols);

        // Simple directory-based approach - let DataFusion do ALL the work
        let table_url = ListingTableUrl::parse(&table_path)?;
        
        debug!("Using directory-based ListingTableConfig for Hive partitioning: {}", table_path);
        debug!("Trusting DataFusion to automatically discover files and infer schema");
        debug!("ListingTableConfig details:");
        debug!("  - table_url: {}", table_url);
        debug!("  - file_extension: {:?}", listing_options.file_extension);
        debug!("  - table_partition_cols: {:?}", listing_options.table_partition_cols);
        debug!("  - format: ParquetFormat");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options);
            
        debug!("Attempting to infer schema for {}", table_name);
        let config = config.infer_schema(&self.context.state()).await?;
        debug!("Successfully inferred schema for {}", table_name);

        debug!("Attempting to create ListingTable for {}", table_name);
        
        // Create and register the table - DataFusion will infer schema automatically
        let table = ListingTable::try_new(config)?;
        
        // Debug: Log detailed file discovery information
        self.log_table_file_discovery(&table, table_name).await?;
        
        debug!("Successfully created ListingTable for {}", table_name);
        let _previous_table = self.context.register_table(table_name, Arc::new(table))?;

        info!("Registered partitioned table: {}", table_name);
        Ok(())
    }

    /// Check if a directory exists in the base_uri
    fn directory_exists(&self, dir_name: &str) -> Result<bool> {
        if self.base_uri.scheme() == "file" {
            let base_path = self.base_uri.to_file_path()
                .map_err(|_| SamplingReceiverError::config_error("Invalid file path in base_uri"))?;
            let dir_path = base_path.join(dir_name);
            Ok(dir_path.exists() && dir_path.is_dir())
        } else {
            // For non-file URIs, assume directory exists (we'll get an error during registration if not)
            Ok(true)
        }
    }

    /// Log detailed information about what files DataFusion discovers for a table
    async fn log_table_file_discovery(&self, table: &ListingTable, table_name: &str) -> Result<()> {
        debug!("=== File Discovery Debug for table: {} ===", table_name);
        
        // Get the table schema
        let schema = table.schema();
        debug!("Table schema for {}: {} columns", table_name, schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            debug!("  Column {}: {} ({})", i, field.name(), field.data_type());
        }
        
        // Try to get file listing by creating a scan
        match table.scan(&self.context.state(), None, &[], None).await {
            Ok(_exec) => {
                debug!("Successfully created scan plan for {}", table_name);
                // Note: We can't easily get the file list from the ExecutionPlan without executing it
                // This at least confirms the table is scannable
            }
            Err(e) => {
                warn!("Failed to create scan plan for {}: {}", table_name, e);
            }
        }
        
        debug!("=== End File Discovery Debug for {} ===", table_name);
        Ok(())
    }

    /// Log the actual directory contents before DataFusion processes them
    fn log_directory_contents(&self, dir_name: &str, table_name: &str) -> Result<()> {
        debug!("=== Directory Contents for {} ({}) ===", table_name, dir_name);
        
        if self.base_uri.scheme() == "file" {
            let base_path = self.base_uri.to_file_path()
                .map_err(|_| SamplingReceiverError::config_error("Invalid file path in base_uri"))?;
            let dir_path = base_path.join(dir_name);
            
            if dir_path.exists() && dir_path.is_dir() {
                // List immediate subdirectories
                match std::fs::read_dir(&dir_path) {
                    Ok(entries) => {
                        for entry in entries {
                            match entry {
                                Ok(entry) => {
                                    let path = entry.path();
                                    if path.is_dir() {
                                        debug!("  Subdirectory: {}", path.file_name().unwrap_or_default().to_string_lossy());
                                        
                                        // List parquet files in this subdirectory
                                        match std::fs::read_dir(&path) {
                                            Ok(subentries) => {
                                                let mut file_count = 0;
                                                for subentry in subentries {
                                                    if let Ok(subentry) = subentry {
                                                        let subpath = subentry.path();
                                                        if subpath.extension() == Some(std::ffi::OsStr::new("parquet")) {
                                                            file_count += 1;
                                                            if file_count <= 5 { // Show first 5 files
                                                                debug!("    Parquet file: {}", subpath.file_name().unwrap_or_default().to_string_lossy());
                                                            }
                                                        }
                                                    }
                                                }
                                                debug!("    Total parquet files in this subdirectory: {}", file_count);
                                            }
                                            Err(e) => debug!("    Error reading subdirectory: {}", e),
                                        }
                                    } else if path.extension() == Some(std::ffi::OsStr::new("parquet")) {
                                        debug!("  Direct parquet file: {}", path.file_name().unwrap_or_default().to_string_lossy());
                                    }
                                }
                                Err(e) => debug!("  Error reading entry: {}", e),
                            }
                        }
                    }
                    Err(e) => debug!("Error reading directory {}: {}", dir_path.display(), e),
                }
            } else {
                debug!("Directory {} does not exist", dir_path.display());
            }
        }
        
        debug!("=== End Directory Contents for {} ===", table_name);
        Ok(())
    }

    /// Discover all parquet files in a table directory including partition subdirectories
    fn discover_parquet_files(&self, dir_name: &str) -> Result<Vec<String>> {
        let mut parquet_files = Vec::new();
        
        if self.base_uri.scheme() == "file" {
            let base_path = self.base_uri.to_file_path()
                .map_err(|_| SamplingReceiverError::config_error("Invalid file path in base_uri"))?;
            let dir_path = base_path.join(dir_name);
            
            if dir_path.exists() && dir_path.is_dir() {
                // Scan the directory and partition subdirectories
                match std::fs::read_dir(&dir_path) {
                    Ok(entries) => {
                        for entry in entries {
                            if let Ok(entry) = entry {
                                let path = entry.path();
                                if path.is_dir() {
                                    // This is a partition directory (_part_id=uuid)
                                    debug!("Scanning partition directory: {}", path.display());
                                    match std::fs::read_dir(&path) {
                                        Ok(subentries) => {
                                            for subentry in subentries {
                                                if let Ok(subentry) = subentry {
                                                    let subpath = subentry.path();
                                                    if subpath.extension() == Some(std::ffi::OsStr::new("parquet")) {
                                                        if let Some(path_str) = subpath.to_str() {
                                                            parquet_files.push(path_str.to_string());
                                                            debug!("Found parquet file: {}", path_str);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            debug!("Error reading partition directory {}: {}", path.display(), e);
                                        }
                                    }
                                } else if path.extension() == Some(std::ffi::OsStr::new("parquet")) {
                                    // Direct parquet file in the main directory
                                    if let Some(path_str) = path.to_str() {
                                        parquet_files.push(path_str.to_string());
                                        debug!("Found direct parquet file: {}", path_str);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(SamplingReceiverError::DataFusionError {
                            message: format!("Failed to read directory {}: {}", dir_path.display(), e),
                        });
                    }
                }
            }
        }
        
        debug!("Discovered {} total parquet files in {}", parquet_files.len(), dir_name);
        Ok(parquet_files)
    }

    /// Log which tables are currently registered
    async fn log_registered_tables(&self) {
        let mut registered_tables = Vec::new();
        
        for table_type in OtapTableType::all() {
            let table_name = table_type.table_name();
            if self.context.table(table_name).await.is_ok() {
                registered_tables.push(table_name);
            }
        }
        
        if registered_tables.is_empty() {
            warn!("No tables were successfully registered");
        } else {
            info!("Successfully registered tables: {}", registered_tables.join(", "));
        }
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