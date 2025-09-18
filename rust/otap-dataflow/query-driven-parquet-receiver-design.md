# Windowed Query-Driven Parquet Receiver Design

**Design Date**: September 18, 2025  
**Architecture Phase**: Temporal Windowed Query Processing  
**Status**: 🎯 **DESIGN PROPOSAL**

## Executive Summary

This document proposes a **windowed, time-based query-driven architecture** for the Parquet Receiver that solves temporal coordination between exporter and receiver. Instead of immediate file processing, the system uses configurable **processing delays**, **time windows**, and **DataFusion queries** to ensure complete data coverage while handling clock drift and late-arriving data through controlled latency.

## 🕒 Temporal Processing Model

### **Core Timing Parameters**
```rust
pub struct TemporalConfig {
    /// Processing delay: Wait time before consuming files (handles late arrivals)
    processing_delay: Duration,  // e.g., 10 minutes
    
    /// Maximum clock drift between systems
    max_clock_drift: Duration,   // e.g., 1 minute
    
    /// Maximum elapsed time per file in parquet exporter
    max_file_duration: Duration, // e.g., 1 minute
    
    /// Time window granularity for processing (configurable)
    window_granularity: Duration, // e.g., 1 minute, 5 minutes, 1 hour
    
    /// Safety margin = processing_delay + max_clock_drift + max_file_duration
    safety_margin: Duration,     // e.g., 12 minutes total
}

impl TemporalConfig {
    /// Calculate safety margin from component timings
    pub fn calculate_safety_margin(&self) -> Duration {
        self.processing_delay + self.max_clock_drift + self.max_file_duration
    }
    
    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.window_granularity < Duration::from_secs(1) {
            return Err("Window granularity must be at least 1 second".to_string());
        }
        if self.processing_delay < self.max_clock_drift + self.max_file_duration {
            return Err("Processing delay should be larger than clock drift + file duration".to_string());
        }
        Ok(())
    }
}
```

### **Windowed Processing Flow**
```
File System Monitoring → Time Window Assembly → DataFusion Query → OTAP Output
        ↑                        ↑                     ↑              ↑
   Files old enough         All files covering     Time-filtered    Aggregated
   (> safety_margin)        current window         query            results
```

## 🎯 Time Window Processing Algorithm

### **Window-Based Data Processing**
```rust
impl WindowedQueryReceiver {
    /// Main processing loop: monitor filesystem and process time windows
    pub async fn run_processing_loop(&mut self) -> Result<(), ParquetReceiverError> {
        let mut current_window_start = self.find_earliest_data_time().await?;
        
        loop {
            // Step 1: Wait for window to become "safe" to process
            let window_end = current_window_start + self.config.window_granularity;
            let safe_processing_time = window_end + self.config.safety_margin;
            
            self.wait_until(safe_processing_time).await;
            
            // Step 2: Discover all parquet files covering this time window
            let covering_files = self.discover_files_for_window(
                current_window_start,
                window_end
            ).await?;
            
            if !covering_files.is_empty() {
                // Step 3: Execute windowed query with time range filter
                let window_results = self.execute_windowed_query(
                    current_window_start,
                    window_end,
                    covering_files
                ).await?;
                
                // Step 4: Output OTAP records for this time window
                for otap_batch in window_results {
                    self.output_channel.send(otap_batch).await?;
                }
            }
            
            // Step 5: Advance to next time window
            current_window_start = window_end;
            
            // Optional: Sleep until next expected files
            tokio::time::sleep(self.config.window_granularity).await;
        }
    }
}
```

## 🏗️ Detailed Windowed Architecture

### **1. File Discovery for Time Windows**

```rust
impl WindowedQueryReceiver {
    /// Discover all parquet files that contain data for the specified time window
    async fn discover_files_for_window(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>
    ) -> Result<Vec<WindowFile>, ParquetReceiverError> {
        let mut covering_files = Vec::new();
        
        // Scan all partition directories
        for partition_dir in self.discover_partition_directories().await? {
            let partition_files = self.find_files_covering_window(
                &partition_dir,
                window_start,
                window_end
            ).await?;
            
            covering_files.extend(partition_files);
        }
        
        Ok(covering_files)
    }
    
    /// Check if a parquet file contains data within the time window
    async fn find_files_covering_window(
        &self,
        partition_dir: &PartitionDirectory,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>
    ) -> Result<Vec<WindowFile>, ParquetReceiverError> {
        let mut files = Vec::new();
        
        // Check each parquet file in the partition
        for file_path in partition_dir.list_parquet_files().await? {
            // Only process files that are old enough (past safety margin)
            let file_age = self.get_file_age(&file_path).await?;
            if file_age < self.config.safety_margin {
                continue; // Skip files that might still be receiving late data
            }
            
            // Check if file's time range overlaps with our window
            let file_time_range = self.extract_file_time_range(&file_path).await?;
            
            if self.time_ranges_overlap(file_time_range, (window_start, window_end)) {
                files.push(WindowFile {
                    path: file_path,
                    partition_id: partition_dir.partition_id.clone(),
                    time_range: file_time_range,
                    table_type: self.determine_table_type(&file_path)?,
                });
            }
        }
        
        Ok(files)
    }
    
    /// Extract time range from parquet file metadata and filename
    async fn extract_file_time_range(&self, file_path: &Path) -> Result<(DateTime<Utc>, DateTime<Utc>), ParquetReceiverError> {
        // Primary: Use parquet column statistics for precise time range
        if let Some(parquet_range) = self.extract_from_parquet_metadata(file_path).await? {
            return Ok(parquet_range);
        }
        
        // Fallback: Extract timestamp from filename (written by exporter)
        if let Some(filename_range) = self.extract_from_filename(file_path)? {
            return Ok(filename_range);
        }
        
        Err(ParquetReceiverError::NoTimestampMetadata(file_path.to_path_buf()))
    }
    
    /// Extract time range from parquet file column statistics
    async fn extract_from_parquet_metadata(&self, file_path: &Path) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>, ParquetReceiverError> {
        let file = File::open(file_path).await?;
        let parquet_metadata = ParquetMetadata::parse_metadata(&file).await?;
        
        // Look for timestamp_unix_nano column statistics
        if let Some(timestamp_stats) = parquet_metadata.get_column_statistics("timestamp_unix_nano") {
            let min_ns = timestamp_stats.min_as_i64()?;
            let max_ns = timestamp_stats.max_as_i64()?;
            
            let min_time = DateTime::from_timestamp_nanos(min_ns);
            let max_time = DateTime::from_timestamp_nanos(max_ns);
            
            return Ok(Some((min_time, max_time)));
        }
        
        Ok(None)
    }
    
    /// Extract timestamp from filename pattern (set by parquet exporter)
    fn extract_from_filename(&self, file_path: &Path) -> Result<Option<(DateTime<Utc>, DateTime<Utc>)>, ParquetReceiverError> {
        let filename = file_path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ParquetReceiverError::InvalidFilename(file_path.to_path_buf()))?;
        
        // Expected filename pattern from exporter: part-{timestamp}-{uuid}.parquet
        // e.g., part-20250918T140000Z-a1b2c3d4.parquet
        if let Some(timestamp_str) = self.extract_timestamp_from_filename(filename) {
            let file_time = DateTime::parse_from_rfc3339(&timestamp_str)
                .map_err(|e| ParquetReceiverError::TimestampParse(e))?;
            
            // Assume file covers max_file_duration around this timestamp
            let start_time = file_time - self.config.max_file_duration / 2;
            let end_time = file_time + self.config.max_file_duration / 2;
            
            return Ok(Some((start_time.into(), end_time.into())));
        }
        
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct WindowFile {
    pub path: PathBuf,
    pub partition_id: String,
    pub time_range: (DateTime<Utc>, DateTime<Utc>),
    pub table_type: TableType, // logs, log_attrs, spans, etc.
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableType {
    Logs,
    LogAttrs,
    ResourceAttrs,
    ScopeAttrs,
    Spans,
    SpanAttrs,
    // ... other OTAP table types
}
```

### **2. Windowed Query Execution**

```rust
impl WindowedQueryReceiver {
    /// Execute DataFusion query with time window constraints
    async fn execute_windowed_query(
        &mut self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        covering_files: Vec<WindowFile>
    ) -> Result<Vec<OtapArrowRecords>, ParquetReceiverError> {
        
        // Step 1: Register files as temporary DataFusion tables
        let table_registry = self.register_window_tables(covering_files).await?;
        
        // Step 2: Build time-constrained SQL query
        let windowed_query = self.build_windowed_query(
            window_start,
            window_end,
            &table_registry
        )?;
        
        // Step 3: Execute query and get primary records grouped by partition
        let primary_results = self.datafusion_context
            .sql(&windowed_query)
            .await?
            .collect()
            .await?;
        
        // Step 4: Group results by partition and enrich with attributes
        let partitioned_results = self.group_and_enrich_results(
            primary_results,
            window_start,
            window_end,
            &table_registry
        ).await?;
        
        // Step 5: Convert to OTAP using existing reconstruction logic
        let mut otap_results = Vec::new();
        for partition_data in partitioned_results {
            let otap_records = self.streaming_coordinator
                .batch_to_otap(partition_data)
                .await?;
            otap_results.push(otap_records);
        }
        
        // Step 6: Clean up temporary tables
        self.cleanup_window_tables(&table_registry).await?;
        
        Ok(otap_results)
    }
    
    /// Build SQL query with time window constraints
    fn build_windowed_query(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        table_registry: &WindowTableRegistry
    ) -> Result<String, ParquetReceiverError> {
        
        let window_start_ns = window_start.timestamp_nanos_opt()
            .ok_or_else(|| ParquetReceiverError::InvalidTimestamp)?;
        let window_end_ns = window_end.timestamp_nanos_opt()
            .ok_or_else(|| ParquetReceiverError::InvalidTimestamp)?;
        
        // Generate UNION query across all primary tables in all partitions
        let mut union_parts = Vec::new();
        
        for (partition_id, tables) in &table_registry.primary_tables {
            if let Some(primary_table) = tables.get(&TableType::Logs) {
                union_parts.push(format!(
                    r#"
                    SELECT *, '{}' as _part_id, {} as _window_start, {} as _window_end
                    FROM {} 
                    WHERE timestamp_unix_nano >= {} 
                      AND timestamp_unix_nano < {}
                    "#,
                    partition_id,
                    window_start_ns,
                    window_end_ns,
                    primary_table.table_name,
                    window_start_ns,
                    window_end_ns
                ));
            }
        }
        
        if union_parts.is_empty() {
            return Err(ParquetReceiverError::NoDataFound);
        }
        
        // Combine with UNION ALL and order by partition and time
        let windowed_query = format!(
            r#"
            WITH windowed_data AS (
                {}
            )
            SELECT * FROM windowed_data 
            ORDER BY _part_id, timestamp_unix_nano
            "#,
            union_parts.join(" UNION ALL ")
        );
        
        Ok(windowed_query)
    }
}

#[derive(Debug)]
pub struct WindowTableRegistry {
    /// Map of partition_id -> table_type -> registered table info
    pub primary_tables: HashMap<String, HashMap<TableType, RegisteredTable>>,
    pub attribute_tables: HashMap<String, HashMap<TableType, RegisteredTable>>,
}

#[derive(Debug, Clone)]
pub struct RegisteredTable {
    pub table_name: String,     // Temporary DataFusion table name
    pub file_paths: Vec<PathBuf>,
    pub partition_id: String,
    pub table_type: TableType,
}
```

### **3. Time Window Configuration and Management**

```rust
#[derive(Debug, Clone)]
pub struct WindowedProcessingConfig {
    /// Core timing configuration
    pub temporal: TemporalConfig,
    
    /// Processing configuration
    pub processing: ProcessingConfig,
    
    /// Query configuration for windowed processing
    pub query: WindowQueryConfig,
}

#[derive(Debug, Clone)]
pub struct ProcessingConfig {
    /// How often to check for new windows to process
    pub check_interval: Duration,        // e.g., 30 seconds
    
    /// Maximum number of concurrent windows being processed
    pub max_concurrent_windows: usize,   // e.g., 3
    
    /// Batch size for DataFusion query execution
    pub query_batch_size: usize,         // e.g., 10000
    
    /// Whether to enable query result caching
    pub enable_caching: bool,
}

#[derive(Debug, Clone)]
pub struct WindowQueryConfig {
    /// Base SQL query template for primary records
    /// Must include placeholders for time range and partition grouping
    pub primary_query_template: String,
    
    /// Custom WHERE clauses to add to windowed queries
    pub additional_filters: Vec<String>,
    
    /// Whether to enable cross-partition aggregations
    pub enable_cross_partition_queries: bool,
    
    /// Query timeout for each window
    pub query_timeout: Duration,
}

impl Default for WindowQueryConfig {
    fn default() -> Self {
        Self {
            primary_query_template: r#"
                SELECT *, '{partition_id}' as _part_id 
                FROM {table_name} 
                WHERE timestamp_unix_nano >= {window_start_ns} 
                  AND timestamp_unix_nano < {window_end_ns}
            "#.to_string(),
            additional_filters: Vec::new(),
            enable_cross_partition_queries: false,
            query_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}
```

### **Current Architecture (File-Driven)**
```
File System Scan → Discover Partitions → Sequential File Consumption → OTAP Reconstruction
     ↑                    ↑                      ↑                         ↑
  Fixed order      Chronological     Streaming state         Per-partition
  processing       discovery         maintenance             processing
```

### **Proposed Architecture (Query-Driven)**
```
SQL Query → DataFusion Execution → Group by Partition → OTAP Reconstruction
    ↑              ↑                     ↑                    ↑
Flexible        Optimized           Maintain ID           Reuse existing
selection       cross-partition     correlation           logic
logic           planning
```

## 🎯 Core Design Principles

### **1. Separation of Concerns**
- **Query Engine**: Handles data selection, filtering, time ranges, complex conditions
- **Partition Grouping**: Ensures OTAP ID correlation within partition boundaries
- **OTAP Reconstruction**: Existing logic, now fed by query results instead of file streams

### **2. SQL-First Interface**
```sql
-- Example: Process logs from last 24 hours with high severity
SELECT * FROM logs 
WHERE timestamp_unix_nano >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
  AND severity_number >= 17
ORDER BY _part_id, timestamp_unix_nano;

-- Example: Process logs matching specific trace patterns
SELECT * FROM logs 
WHERE trace_id IS NOT NULL 
  AND body LIKE '%error%'
ORDER BY _part_id, id;
```

### **3. Partition-Aware Processing**
Query results must be grouped by `_part_id` to maintain OTAP identifier correlation:
- IDs are only unique within a partition
- Attribute tables must be joined within the same partition
- Cross-partition joins would break identifier semantics

## 🏗️ Detailed Architecture Design

### **Component Overview**

```rust
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│   SQL Query     │───▶│  DataFusion      │───▶│   Partition         │───▶│  OTAP           │
│   Interface     │    │  Query Engine    │    │   Grouper           │    │  Reconstructor   │
└─────────────────┘    └──────────────────┘    └─────────────────────┘    └──────────────────┘
        │                        │                         │                         │
        │                        │                         │                         │
    User-defined           Parquet file              Groups records           Existing logic
    selection logic        registration              by _part_id             (minimal changes)
```

### **1. SQL Query Interface**

```rust
pub struct QueryDrivenParquetReceiver {
    /// DataFusion session context for query execution
    session_context: SessionContext,
    
    /// Base directory containing partitioned parquet files
    base_directory: PathBuf,
    
    /// Primary table configuration (e.g., "logs", "spans", "metrics")
    primary_table: PrimaryTableConfig,
    
    /// Query specification for data selection
    query_spec: QuerySpecification,
    
    /// Existing OTAP reconstruction components
    streaming_coordinator: StreamingCoordinator,
}

#[derive(Debug, Clone)]
pub struct QuerySpecification {
    /// SQL query to select primary records
    /// Must include _part_id in SELECT and ORDER BY _part_id for proper grouping
    sql_query: String,
    
    /// Optional query parameters for dynamic queries
    parameters: HashMap<String, QueryParameter>,
    
    /// Batch size for query execution
    batch_size: usize,
}

#[derive(Debug, Clone)]
pub enum QueryParameter {
    Timestamp(i64),
    String(String),
    Integer(i64),
    Boolean(bool),
}
```

### **2. DataFusion Integration Layer**

```rust
impl QueryDrivenParquetReceiver {
    /// Initialize DataFusion context and register parquet tables
    pub async fn new(config: QueryDrivenConfig) -> Result<Self, ParquetReceiverError> {
        let session_context = SessionContext::new();
        
        // Register all parquet files as DataFusion tables
        self.register_parquet_tables(&session_context, &config.base_directory).await?;
        
        Ok(Self {
            session_context,
            base_directory: config.base_directory,
            primary_table: config.primary_table,
            query_spec: config.query_spec,
            streaming_coordinator: StreamingCoordinator::new(),
        })
    }
    
    /// Register all partition directories as DataFusion tables
    async fn register_parquet_tables(&self, ctx: &SessionContext, base_dir: &Path) -> Result<(), ParquetReceiverError> {
        // Discover all partition directories
        let partitions = self.discover_partitions(base_dir).await?;
        
        for partition in partitions {
            let partition_id = partition.part_id;
            
            // Register primary table (e.g., logs_<partition_id>)
            let primary_table_path = partition.path.join(&self.primary_table.name);
            ctx.register_parquet(
                &format!("{}_{}", self.primary_table.name, partition_id),
                &primary_table_path.to_string_lossy(),
                ParquetReadOptions::default(),
            ).await?;
            
            // Register attribute tables
            for attr_table in &self.primary_table.attribute_tables {
                let attr_table_path = partition.path.join(attr_table);
                if attr_table_path.exists() {
                    ctx.register_parquet(
                        &format!("{}_{}", attr_table, partition_id),
                        &attr_table_path.to_string_lossy(),
                        ParquetReadOptions::default(),
                    ).await?;
                }
            }
        }
        
        Ok(())
    }
}
```

### **3. Query Execution and Partition Grouping**

```rust
impl QueryDrivenParquetReceiver {
    /// Execute SQL query and group results by partition ID
    pub async fn execute_query(&self) -> Result<Vec<PartitionedRecords>, ParquetReceiverError> {
        // Step 1: Execute user-defined SQL query
        let query_result = self.session_context
            .sql(&self.query_spec.sql_query)
            .await?
            .collect()
            .await?;
        
        // Step 2: Group results by partition ID
        let partitioned_results = self.group_by_partition(query_result).await?;
        
        // Step 3: For each partition group, fetch associated attribute tables
        let mut enriched_partitions = Vec::new();
        for partition_records in partitioned_results {
            let enriched = self.enrich_with_attributes(partition_records).await?;
            enriched_partitions.push(enriched);
        }
        
        Ok(enriched_partitions)
    }
    
    /// Group query results by partition ID
    async fn group_by_partition(&self, batches: Vec<RecordBatch>) -> Result<Vec<PartitionGroup>, ParquetReceiverError> {
        let mut partition_groups: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        
        for batch in batches {
            // Extract partition ID from _part_id column
            let part_id_column = batch.column_by_name("_part_id")
                .ok_or_else(|| ParquetReceiverError::Reconstruction(
                    "Query results must include _part_id column".to_string()
                ))?;
            
            // Group records by partition ID
            let groups = self.split_batch_by_partition_id(batch, part_id_column)?;
            
            for (partition_id, partition_batch) in groups {
                partition_groups.entry(partition_id)
                    .or_insert_with(Vec::new)
                    .push(partition_batch);
            }
        }
        
        // Convert to structured partition groups
        let mut result = Vec::new();
        for (partition_id, batches) in partition_groups {
            result.push(PartitionGroup {
                partition_id,
                primary_batches: batches,
                attribute_batches: HashMap::new(), // Will be filled by enrich_with_attributes
            });
        }
        
        Ok(result)
    }
    
    /// Enrich partition group with corresponding attribute tables
    async fn enrich_with_attributes(&self, mut partition_group: PartitionGroup) -> Result<PartitionedRecords, ParquetReceiverError> {
        let partition_id = &partition_group.partition_id;
        
        // Extract ID ranges from primary records to query only relevant attributes
        let id_ranges = self.extract_id_ranges(&partition_group.primary_batches)?;
        
        // Query each attribute table for the relevant ID ranges
        for attr_table in &self.primary_table.attribute_tables {
            let attr_query = format!(
                "SELECT * FROM {}_{} WHERE parent_id BETWEEN {} AND {} ORDER BY parent_id",
                attr_table, partition_id, id_ranges.min, id_ranges.max
            );
            
            let attr_results = self.session_context
                .sql(&attr_query)
                .await?
                .collect()
                .await?;
            
            partition_group.attribute_batches.insert(attr_table.clone(), attr_results);
        }
        
        Ok(PartitionedRecords {
            partition_id: partition_group.partition_id,
            primary_batch: self.combine_batches(partition_group.primary_batches)?,
            child_batches: partition_group.attribute_batches,
        })
    }
}
```

### **4. Integration with Existing OTAP Reconstruction**

```rust
impl QueryDrivenParquetReceiver {
    /// Process query results through existing OTAP reconstruction pipeline
    pub async fn process_to_otap(&self, partitioned_records: Vec<PartitionedRecords>) -> Result<Vec<OtapArrowRecords>, ParquetReceiverError> {
        let mut otap_results = Vec::new();
        
        for partition_records in partitioned_records {
            // Convert to StreamingBatch format (existing interface)
            let streaming_batch = StreamingBatch {
                primary_batch: partition_records.primary_batch,
                child_batches: partition_records.child_batches,
            };
            
            // Use existing OTAP reconstruction logic
            let otap_records = self.streaming_coordinator
                .batch_to_otap(streaming_batch)
                .await?;
            
            otap_results.push(otap_records);
        }
        
        Ok(otap_results)
    }
}
```

## 🚀 Windowed Processing Examples

### **Example 1: Standard Windowed Processing**
```rust
let temporal_config = TemporalConfig {
    processing_delay: Duration::from_secs(600),    // 10 minutes
    max_clock_drift: Duration::from_secs(60),      // 1 minute  
    max_file_duration: Duration::from_secs(60),    // 1 minute
    window_granularity: Duration::from_secs(60),   // 1 minute windows
    safety_margin: Duration::from_secs(720),       // 12 minutes total
};

let windowed_config = WindowedProcessingConfig {
    temporal: temporal_config,
    processing: ProcessingConfig {
        check_interval: Duration::from_secs(30),
        max_concurrent_windows: 3,
        query_batch_size: 10000,
        enable_caching: true,
    },
    query: WindowQueryConfig::default(),
};

let mut receiver = WindowedQueryReceiver::new(windowed_config).await?;
receiver.run_processing_loop().await?; // Runs indefinitely, processing minute-by-minute
```

### **Example 2: Aggregated Windowed Query**
```rust
let aggregation_query = WindowQueryConfig {
    primary_query_template: r#"
        SELECT 
            *, 
            '{partition_id}' as _part_id,
            date_trunc('minute', to_timestamp_seconds(timestamp_unix_nano / 1000000000)) as minute_bucket,
            COUNT(*) OVER (PARTITION BY '{partition_id}', minute_bucket) as logs_per_minute
        FROM {table_name} 
        WHERE timestamp_unix_nano >= {window_start_ns} 
          AND timestamp_unix_nano < {window_end_ns}
          AND severity_number >= 17  -- Only errors and above
    "#.to_string(),
    additional_filters: vec![
        "trace_id IS NOT NULL".to_string(),
        "body LIKE '%error%'".to_string(),
    ],
    enable_cross_partition_queries: true,
    query_timeout: Duration::from_secs(180),
};
```

### **Example 3: Timeline Visualization**
```
Timeline: Windowed Processing with 12-minute Safety Margin

Current Time: 14:00:00
├─ 13:47:00-13:48:00 ✅ PROCESSING (safe, 13+ min old)
├─ 13:48:00-13:49:00 ⏳ WAITING (only 12 min old)  
├─ 13:49:00-13:50:00 ⏳ WAITING (only 11 min old)
└─ 13:50:00-14:00:00 ⏳ CURRENT/FUTURE

Files for 13:47:00-13:48:00 window:
├─ partition_A/logs/part-001.parquet    [13:46:30 - 13:47:45] ✅
├─ partition_A/log_attrs/part-001.parquet [13:46:30 - 13:47:45] ✅  
├─ partition_B/logs/part-002.parquet    [13:47:15 - 13:48:20] ✅
└─ partition_B/log_attrs/part-002.parquet [13:47:15 - 13:48:20] ✅

Query: SELECT * FROM logs WHERE timestamp BETWEEN 13:47:00 AND 13:48:00
Result: 847 log records + 3,240 attribute records → 2 OTAP batches (by partition)
```

### **Example 4: Late Arrival Handling**
```rust
// Configuration explains how late arrivals are handled
let config = TemporalConfig {
    processing_delay: Duration::from_secs(600),    // 10 min delay
    max_clock_drift: Duration::from_secs(60),      // 1 min max drift
    max_file_duration: Duration::from_secs(60),    // 1 min per file
    window_granularity: Duration::from_secs(60),   // 1 min windows
    safety_margin: Duration::from_secs(720),       // 12 min = 10+1+1
};

/* 
Scenario: Processing window 14:15:00-14:16:00 at 14:27:00

✅ ACCEPTED: Files written before 14:15:00 (12+ minutes old)
✅ ACCEPTED: Late data arriving up to 14:25:00 (within 10-minute delay)
❌ REJECTED: Any data arriving after 14:27:00 for this window
❌ REJECTED: Files still being written (< 12 minutes old)

This ensures:
- All expected data is captured (handles max 1-min clock drift)
- All files are complete (max 1-min write duration + 10-min delay)
- Processing is deterministic (same query always gets same result)
- System can operate continuously without manual intervention
*/
```

### **Example 1: Time-Based Query**
```rust
let query_config = QuerySpecification {
    sql_query: r#"
        SELECT *, _part_id 
        FROM logs 
        WHERE timestamp_unix_nano >= $start_time 
          AND timestamp_unix_nano < $end_time
        ORDER BY _part_id, timestamp_unix_nano
    "#.to_string(),
    parameters: [
        ("start_time".to_string(), QueryParameter::Timestamp(start_timestamp)),
        ("end_time".to_string(), QueryParameter::Timestamp(end_timestamp)),
    ].into_iter().collect(),
    batch_size: 10000,
};

let receiver = QueryDrivenParquetReceiver::new(QueryDrivenConfig {
    base_directory: "/path/to/parquet/files".into(),
    primary_table: PrimaryTableConfig::logs(),
    query_spec: query_config,
}).await?;

let otap_batches = receiver.execute_and_process().await?;
```

### **Example 2: Filtered Processing**
```rust
let query_config = QuerySpecification {
    sql_query: r#"
        SELECT *, _part_id 
        FROM logs 
        WHERE severity_number >= 17 
          AND trace_id IS NOT NULL
          AND body LIKE '%error%'
        ORDER BY _part_id, id
        LIMIT 100000
    "#.to_string(),
    parameters: HashMap::new(),
    batch_size: 5000,
};
```

### **Example 3: Cross-Partition Analytics**
```rust
let query_config = QuerySpecification {
    sql_query: r#"
        SELECT *, _part_id,
               COUNT(*) OVER (PARTITION BY _part_id) as partition_count
        FROM logs 
        WHERE timestamp_unix_nano >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        ORDER BY _part_id, timestamp_unix_nano
    "#.to_string(),
    parameters: HashMap::new(),
    batch_size: 20000,
};
```

## 📊 Benefits Analysis

### **✅ Advantages**

| **Aspect** | **File-Driven** | **Query-Driven** | **Benefit** |
|------------|------------------|-------------------|-------------|
| **Flexibility** | Fixed chronological order | Arbitrary SQL conditions | **Complex filtering, time ranges, sampling** |
| **Testing** | Full partition processing | Targeted data selection | **Test specific scenarios easily** |
| **Performance** | Stream all data | Process only needed data | **Reduced I/O for filtered queries** |
| **Integration** | File system dependent | SQL interface | **Easy external system integration** |
| **Analytics** | Limited capabilities | Full SQL analytics | **Cross-partition analysis, aggregations** |
| **Development** | Custom scanning logic | Standard SQL | **Familiar query interface** |

### **⚠️ Trade-offs**

| **Consideration** | **File-Driven** | **Query-Driven** | **Impact** |
|-------------------|------------------|-------------------|------------|
| **Memory Usage** | Streaming processing | Query result materialization | **Higher memory for large result sets** |
| **Latency** | Immediate streaming | Query planning + execution | **Higher latency for simple cases** |
| **Complexity** | Simple file scanning | DataFusion integration | **More complex setup and dependencies** |
| **Real-time** | Continuous file monitoring | Periodic query execution | **Less suitable for real-time streaming** |

## 🏁 Implementation Plan for Windowed Architecture

### **Phase 1: Temporal Windowing Foundation (3 weeks)**
- [ ] Implement `TemporalConfig` and time window calculation logic
- [ ] Build file age detection and safety margin validation
- [ ] Create time range extraction from parquet file metadata
- [ ] Implement window-based file discovery algorithm
- [ ] Add comprehensive temporal coordination tests

### **Phase 2: DataFusion Integration (2 weeks)**
- [ ] Implement dynamic table registration for discovered files
- [ ] Build windowed SQL query generation with time constraints
- [ ] Create partition-aware query execution and result grouping
- [ ] Add query result caching and performance optimization
- [ ] Implement query timeout and error handling

### **Phase 3: Windowed Processing Loop (2 weeks)**
- [ ] Build main processing loop with configurable intervals
- [ ] Implement concurrent window processing with limits
- [ ] Create window state tracking and progress monitoring
- [ ] Add metrics and observability for windowed operations
- [ ] Implement graceful shutdown and recovery mechanisms

### **Phase 4: OTAP Integration (1 week)**
- [ ] Adapt existing `StreamingCoordinator` for windowed batches
- [ ] Ensure seamless integration with current OTAP reconstruction
- [ ] Implement efficient batch combination from multiple files
- [ ] Test end-to-end windowed query → OTAP flow

### **Phase 5: Query Design and Templates (2 weeks)**
- [ ] Design and implement configurable SQL query templates
- [ ] Add support for parameterized queries with dynamic values
- [ ] Create query validation and optimization framework
- [ ] Implement query result transformation and post-processing
- [ ] Build query performance monitoring and analytics

### **Phase 6: Testing and Optimization (2 weeks)**
- [ ] Comprehensive unit tests for all windowing components
- [ ] Performance benchmarking vs file-driven and immediate query approaches
- [ ] Memory usage optimization for large time windows
- [ ] End-to-end integration tests with realistic data volumes
- [ ] Production readiness validation and documentation

**Total Implementation Time**: **12 weeks**

## 🎯 Success Criteria for Windowed Architecture

### **Functional Requirements**
- [ ] Process time windows deterministically with configurable delays and safety margins
- [ ] Handle clock drift and late arrivals gracefully within configured parameters
- [ ] Execute complex SQL queries across multiple partitions and files
- [ ] Maintain OTAP identifier correlation within partition boundaries
- [ ] Support continuous operation with automatic window advancement

### **Performance Requirements**  
- [ ] Windowed processing shows 5-10x performance improvement for filtered queries vs full processing
- [ ] Memory usage scales predictably with window size and number of concurrent windows
- [ ] Query execution time remains under configurable timeout limits
- [ ] File discovery overhead is negligible compared to query execution time
- [ ] System can sustain continuous operation without memory leaks or degradation

### **Reliability Requirements**
- [ ] System handles missing files and partial data gracefully
- [ ] Late arrivals beyond safety margin are logged but don't break processing
- [ ] Query failures in one window don't affect processing of other windows
- [ ] System can recover from restarts and continue from correct time position
- [ ] Comprehensive observability for monitoring and debugging temporal issues

### **Integration Requirements**
- [ ] Compatible with existing parquet file format and partition structure
- [ ] Seamless integration with current OTAP reconstruction pipeline
- [ ] External systems can monitor processing progress via metrics
- [ ] Configuration changes can be applied without full system restart
- [ ] Clear migration path from file-driven to windowed processing

---

## 🔮 Future Enhancements

### **Advanced Query Features**
- **Query Templates**: Pre-built queries for common use cases
- **Query Scheduling**: Cron-like scheduling for periodic processing  
- **Query Optimization**: Custom DataFusion optimizations for OTAP patterns
- **Query Result Streaming**: Stream large result sets instead of materializing

### **External Integrations**
- **REST API**: HTTP interface for external query submission
- **GraphQL**: More flexible query interface for complex data relationships  
- **Message Queue**: Async query processing with result publishing
- **Monitoring**: Integration with observability systems for query performance

---

## ✅ **Design Decisions Made**

1. **File Metadata**: ✅ Use parquet column statistics (timestamp_unix_nano min/max) as primary source, with filename timestamps as fallback. Avoid filesystem timestamps.

2. **Query Templates**: 🔄 Deferred to next phase - will design comprehensive query framework with configurable templates and parameterization.

3. **Window Size**: ✅ Fully configurable - from seconds to hours based on use case requirements. Validation ensures minimum 1-second windows.

4. **Concurrent Processing**: ✅ Not a priority - sequential processing keeps implementation simple and reliable.

5. **Recovery**: ✅ Not a priority - system can restart from any time point without complex state management.

---

**Design Status**: **📋 READY FOR IMPLEMENTATION**

**Next Steps**: Begin Phase 1 implementation with temporal windowing foundation and configurable window sizes. Query design will be addressed in Phase 5.