# Direct Object Store Streaming Architecture

**Date:** September 17, 2025  
**Repository:** otel-arrow-tail-sampler  
**Context:** Parquet Receiver Streaming Merge Implementation

## Overview

This document outlines the architecture for implementing a streaming merge of parent-child Parquet data using direct object store access, bypassing DataFusion's query engine complexity while maintaining ordering guarantees and memory efficiency.

## Problem Statement

We have two related data streams with mismatched batch sizes:

```
LOGS stream:     [ID: 0-99]    [ID: 100-199]    [ID: 200-299]    ...
LOG_ATTRS stream: [parent_id: 0-559]  [parent_id: 560-1119]  ...
```

**Challenges:**
- Different batch sizes cause temporal misalignment
- ID ranges don't align across flushes 
- DataFusion SQL joins add complexity and overhead
- Need streaming processing for memory efficiency

**Requirements:**
- Maintain parent-child relationships (logs ↔ log_attrs)
- Process data in timestamp order
- Memory-efficient streaming
- Support multiple object store backends

## Architecture Decision

**Chosen Approach:** Direct Object Store + Streaming Merge

**Rejected Approaches:**
- DataFusion SQL joins (too complex, overhead)
- Fix ID generation at source (long-term solution)
- Full materialization (memory inefficient)

## Core Components

### 1. DirectParquetStreamReader

Reads Parquet files directly from object store in timestamp order:

```rust
pub struct DirectParquetStreamReader {
    object_store: Arc<dyn ObjectStore>,
    file_paths: Vec<ObjectPath>,
    current_file_index: usize,
    current_reader: Option<ParquetRecordBatchReader<bytes::Bytes>>,
    batch_size: usize,
}

impl DirectParquetStreamReader {
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        base_path: &str,
        table_name: &str, 
        partition_id: &str,
        batch_size: usize,
    ) -> Result<Self> {
        let partition_prefix = format!("{}/{}/_part_id={}/", 
            base_path.trim_end_matches('/'), table_name, partition_id);
        
        // Direct object store listing
        let mut file_paths = Vec::new();
        let prefix = ObjectPath::from(partition_prefix);
        
        let mut list_stream = object_store.list(Some(&prefix)).await?;
        while let Some(meta) = list_stream.next().await {
            let meta = meta?;
            if meta.location.as_ref().ends_with(".parquet") {
                file_paths.push(meta.location);
            }
        }
        
        // Critical: Sort by path for consistent ordering
        // Parquet filenames contain timestamps, so this gives us temporal order
        file_paths.sort();
        
        log::debug!("📁 Found {} {} files for partition {}", 
                   file_paths.len(), table_name, partition_id);
        
        Ok(Self {
            object_store,
            file_paths,
            current_file_index: 0,
            current_reader: None,
            batch_size,
        })
    }
    
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Try to read from current file reader
            if let Some(reader) = &mut self.current_reader {
                match reader.next() {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        // Current file exhausted, move to next
                        self.current_reader = None;
                        self.current_file_index += 1;
                    }
                }
            }
            
            // Open next file if available
            if self.current_file_index >= self.file_paths.len() {
                return Ok(None); // No more files
            }
            
            let file_path = &self.file_paths[self.current_file_index];
            log::debug!("📖 Opening file: {}", file_path);
            
            // Read file directly from object store
            let file_data = self.object_store.get(file_path).await?
                .bytes().await?;
            
            // Create Parquet reader
            let builder = ParquetRecordBatchReaderBuilder::try_new(file_data)?
                .with_batch_size(self.batch_size);
            
            self.current_reader = Some(builder.build()?);
        }
    }
}
```

### 2. DirectStreamingMerger

Performs streaming merge of logs and log_attrs:

```rust
pub struct DirectStreamingMerger {
    logs_reader: DirectParquetStreamReader,
    attrs_reader: DirectParquetStreamReader,
    attrs_buffer: BTreeMap<u32, Vec<AttributeRecord>>,
    partition_id: String,
}

impl DirectStreamingMerger {
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        base_path: &str,
        partition_id: &str,
    ) -> Result<Self> {
        let logs_reader = DirectParquetStreamReader::new(
            object_store.clone(),
            base_path,
            "logs", 
            partition_id,
            1000, // batch_size
        ).await?;
        
        let attrs_reader = DirectParquetStreamReader::new(
            object_store.clone(),
            base_path,
            "log_attrs",
            partition_id, 
            1000,
        ).await?;
        
        Ok(Self {
            logs_reader,
            attrs_reader,
            attrs_buffer: BTreeMap::new(),
            partition_id: partition_id.to_string(),
        })
    }
    
    pub async fn next_joined_batch(&mut self) -> Result<Option<JoinedBatch>> {
        // Read next logs batch (defines the ID boundary)
        let logs_batch = match self.logs_reader.next_batch().await? {
            Some(batch) => batch,
            None => return Ok(None),
        };
        
        let log_ids = logs_batch.column_by_name("id")
            .ok_or("Missing id column")?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or("Invalid id column type")?;
        
        let max_log_id = log_ids.iter()
            .flatten()
            .max()
            .ok_or("No valid log IDs")?;
        
        // Buffer attributes up to max_log_id
        self.buffer_attributes_up_to(max_log_id).await?;
        
        // Join logs with their buffered attributes
        let joined_batch = self.create_joined_batch(logs_batch, max_log_id)?;
        
        Ok(Some(joined_batch))
    }
    
    async fn buffer_attributes_up_to(&mut self, max_id: u32) -> Result<()> {
        while let Some(attrs_batch) = self.attrs_reader.next_batch().await? {
            let parent_ids = attrs_batch.column_by_name("parent_id")
                .ok_or("Missing parent_id column")?
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or("Invalid parent_id column type")?;
            
            let mut consumed_all = true;
            
            for (row_idx, parent_id) in parent_ids.iter().enumerate() {
                if let Some(parent_id) = parent_id {
                    if parent_id <= max_id {
                        // Extract and buffer this attribute
                        let attr = self.extract_attribute_record(&attrs_batch, row_idx)?;
                        self.attrs_buffer
                            .entry(parent_id)
                            .or_default()
                            .push(attr);
                    } else {
                        // Future attribute - we'll process it next time
                        consumed_all = false;
                        // TODO: Implement putback mechanism if needed
                        break;
                    }
                }
            }
            
            if !consumed_all {
                break;
            }
        }
        Ok(())
    }
    
    fn create_joined_batch(&mut self, logs_batch: RecordBatch, max_id: u32) -> Result<JoinedBatch> {
        let log_ids = logs_batch.column_by_name("id")?
            .as_any()
            .downcast_ref::<UInt32Array>()?;
        
        let mut joined_records = Vec::new();
        
        for (row_idx, log_id) in log_ids.iter().enumerate() {
            if let Some(log_id) = log_id {
                // Extract log record
                let log_record = self.extract_log_record(&logs_batch, row_idx)?;
                
                // Get buffered attributes for this log
                let attributes = self.attrs_buffer.remove(&log_id).unwrap_or_default();
                
                joined_records.push(JoinedRecord {
                    log: log_record,
                    attributes,
                });
            }
        }
        
        Ok(JoinedBatch {
            records: joined_records,
            partition_id: self.partition_id.clone(),
            max_processed_id: max_id,
        })
    }
}
```

### 3. Integration with StreamingCoordinator

```rust
use object_store::{local::LocalFileSystem, ObjectStore};

impl StreamingCoordinator {
    pub async fn process_partition_direct(
        &self,
        partition_id: String,
    ) -> Result<Vec<RecordBatch>> {
        // Create object store directly - no DataFusion needed
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(&self.base_directory)?
        );
        
        let mut merger = DirectStreamingMerger::new(
            object_store,
            "", // base_path is handled by LocalFileSystem prefix
            &partition_id,
        ).await?;
        
        let mut result_batches = Vec::new();
        
        while let Some(joined_batch) = merger.next_joined_batch().await? {
            log::info!(
                "✅ Processed partition {} batch: {} logs, {} attributes",
                partition_id,
                joined_batch.log_count(),
                joined_batch.attr_count()
            );
            
            // Convert to OTAP format
            let otap_batch = self.convert_joined_to_otap(joined_batch)?;
            result_batches.push(otap_batch);
        }
        
        Ok(result_batches)
    }
}
```

## Key Design Principles

### 1. **Minimal Dependencies**
```rust
// Only essential dependencies:
use object_store::{ObjectStore, local::LocalFileSystem};
use parquet::arrow::{ParquetRecordBatchReaderBuilder, ArrowReader};
use arrow::record_batch::RecordBatch;
// No DataFusion, no SQL, no ListingTable complexity
```

### 2. **Consistent File Ordering**
```rust
// Leverage filename-based ordering for consistency
file_paths.sort(); // Parquet filenames contain timestamps

// This provides the same consistency guarantees as DataFusion's ListingTable
// but with direct control and no overhead
```

### 3. **Streaming Memory Usage**
```rust
// Process data in streaming fashion:
// - Read logs batch → determines ID boundary
// - Stream log_attrs up to boundary → buffer relevant ones
// - Join and emit → clear buffers
// - Repeat

// Memory usage: O(batch_size + relevant_attributes)
// Not: O(total_dataset_size)
```

### 4. **Object Store Flexibility**
```rust
// Support any object store backend
let store: Arc<dyn ObjectStore> = match config.storage_type {
    StorageType::Local => Arc::new(LocalFileSystem::new_with_prefix(path)?),
    StorageType::S3 => Arc::new(AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region)
        .build()?),
    StorageType::Azure => Arc::new(MicrosoftAzureBuilder::new()
        .with_account(&config.account)
        .with_container_name(&config.container)
        .build()?),
    StorageType::GCS => Arc::new(GoogleCloudStorageBuilder::new()
        .with_bucket_name(&config.bucket)
        .build()?),
};
```

### 5. **Native Arrow Processing**
```rust
// Direct RecordBatch operations - no schema conversion overhead
let ids = batch.column_by_name("id")?
    .as_any()
    .downcast_ref::<UInt32Array>()?;

let timestamps = batch.column_by_name("time_unix_nano")?
    .as_any()
    .downcast_ref::<TimestampNanosecondArray>()?;
```

## Architecture Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Object Store  │◄───┤ DirectParquet    │◄───┤ File Discovery │
│   (Local/S3/    │    │ StreamReader     │    │ (sorted paths)  │
│    Azure/GCS)   │    │                  │    └─────────────────┘
└─────────────────┘    └──────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Parquet       │◄───┤ DirectStreaming  │───►│ Attribute       │
│   Files         │    │ Merger           │    │ Buffer          │
│   (Timestamp    │    │                  │    │ (BTreeMap)      │
│    Ordered)     │    │                  │    └─────────────────┘
└─────────────────┘    └──────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐
│   Arrow         │◄───┤ OTAP Batch       │
│   RecordBatch   │    │ Constructor      │
│   (Joined)      │    │                  │
└─────────────────┘    └──────────────────┘
```

## Benefits Over DataFusion Approach

### Performance Benefits
- **Zero query planning overhead** - direct Arrow operations
- **Minimal memory footprint** - streaming with bounded buffers
- **No schema inference** - work with known Parquet schemas
- **Reduced allocations** - direct bytes-to-Arrow conversion

### Operational Benefits
- **Simpler debugging** - no query plans to analyze
- **Predictable performance** - no query optimizer decisions
- **Direct control** - exact batching and memory management
- **Flexible backends** - any object store implementation

### Maintenance Benefits
- **Fewer dependencies** - no DataFusion version coupling
- **Clearer code flow** - straightforward streaming logic
- **Easier testing** - mock object stores, deterministic behavior
- **Better error handling** - direct control over error propagation

## Implementation Considerations

### 1. **Putback Mechanism**
When buffering attributes, we may need to "putback" a partially consumed batch:

```rust
// TODO: Implement batch putback for efficiency
// Option 1: Buffer the remaining rows in memory
// Option 2: Re-seek to the correct position in file
// Option 3: Keep track of row offset and skip already processed rows
```

### 2. **Error Handling**
```rust
// Robust error handling for:
// - Object store connection failures
// - Corrupted Parquet files  
// - Schema mismatches
// - Out of memory conditions
```

### 3. **Metrics and Observability**
```rust
// Track key metrics:
// - Files processed per partition
// - Records joined per batch
// - Buffer usage statistics
// - Processing time per partition
```

### 4. **Configuration**
```rust
pub struct StreamingConfig {
    pub batch_size: usize,           // Records per batch
    pub max_buffer_size: usize,      // Max attributes buffered
    pub object_store_config: ObjectStoreConfig,
    pub processing_timeout: Duration,
}
```

## Future Enhancements

### 1. **Parallel Partition Processing**
```rust
// Process multiple partitions concurrently
let handles: Vec<_> = partition_ids.into_iter()
    .map(|partition_id| {
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            coordinator.process_partition_direct(partition_id).await
        })
    })
    .collect();
```

### 2. **Advanced Buffering Strategies**
```rust
// Implement more sophisticated buffering:
// - LRU cache for frequently accessed attributes
// - Bloom filters for attribute existence checks
// - Compressed attribute storage
```

### 3. **Schema Evolution Support**
```rust
// Handle schema changes across Parquet files:
// - Schema compatibility checking
// - Automatic field mapping
// - Default value handling for missing fields
```

## Conclusion

The direct object store streaming approach provides:

✅ **Simplicity** - No DataFusion complexity, direct control  
✅ **Performance** - Minimal overhead, streaming processing  
✅ **Flexibility** - Any object store backend, configurable batching  
✅ **Reliability** - Deterministic ordering, robust error handling  
✅ **Maintainability** - Clear code flow, easy debugging  

This architecture perfectly addresses the streaming parent-child merge problem while maintaining the ordering guarantees and memory efficiency required for production OpenTelemetry data processing.

The approach transforms a complex "streaming join with mismatched batch sizes" problem into a simple "streaming merge of ordered sequences" - much more tractable and efficient for the specific use case.