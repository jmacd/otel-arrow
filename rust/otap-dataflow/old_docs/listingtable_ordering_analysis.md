# DataFusion ListingTable Ordering Analysis

**Date:** September 17, 2025  
**Repository:** apache/datafusion  
**Branch:** main

## Overview

This document analyzes how Apache DataFusion's `ListingTable` handles ordering among the files it processes. The analysis reveals that `ListingTable` implements sophisticated ordering mechanisms that go beyond simple file listing, providing both consistency guarantees and query optimization opportunities.

## Key Findings

**Yes, `ListingTable` does ensure ordering**, but it's more nuanced than a simple table-level sort. The ordering mechanisms serve multiple purposes:

1. **Execution consistency** - Ensuring reproducible query plans
2. **Query optimization** - Leveraging known file-level sort orders
3. **Parallel processing** - Intelligent file grouping that preserves ordering
4. **Statistics utilization** - Using min/max statistics for optimal file arrangement

## Ordering Mechanisms

### 1. File-Level Sort Order Configuration

`ListingTable` can be configured with a `file_sort_order` through `ListingOptions`:

```rust
// From ListingOptions struct
pub file_sort_order: Vec<Vec<SortExpr>>
```

**Key characteristics:**
- Tells DataFusion that individual files are pre-sorted according to specific columns
- Supports multiple equivalent orderings (outer `Vec`)
- Each ordering is a lexicographic sequence (inner `Vec<SortExpr>`)
- Enables query optimizer to skip unnecessary sorts

**Configuration example:**
```rust
let file_sort_order = vec![vec![
    col("timestamp").sort(true, false)  // ascending, nulls last
]];

let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_file_sort_order(file_sort_order);
```

### 2. Consistent File Discovery

When scanning for files, `ListingTable` ensures consistency:

```rust
// From FileGroup::split_files method
// ObjectStore::list does not guarantee any consistent order and for some
// implementations such as LocalFileSystem, it may be inconsistent. Thus
// Sort files by path to ensure consistent plans when run more than once.
self.files.sort_by(|a, b| a.path().cmp(b.path()));
```

**Why this matters:**
- Object stores (S3, local filesystem) don't guarantee consistent file ordering
- Path-based sorting ensures reproducible query execution plans
- Critical for deterministic behavior in distributed environments

### 3. Intelligent File Grouping

The `FileGroupPartitioner` handles file distribution across partitions:

```rust
pub struct FileGroupPartitioner {
    target_partitions: usize,
    repartition_file_min_size: usize,
    preserve_order_within_groups: bool,  // Key flag for ordering
}
```

**Order-preserving strategies:**
- When `preserve_order_within_groups = true`, files cannot be mixed within groups
- Large files are split into ranges while preserving sequential access
- Files are distributed to maintain overall ordering across partitions

### 4. Statistics-Based File Organization

The most sophisticated ordering feature is `split_groups_by_statistics_with_target_partitions`:

```rust
pub fn split_groups_by_statistics_with_target_partitions(
    table_schema: &SchemaRef,
    file_groups: &[FileGroup],
    sort_order: &LexOrdering,
    target_partitions: usize,
) -> Result<Vec<FileGroup>>
```

**Algorithm:**
1. Extract min/max statistics from each file for sort columns
2. Sort files by their minimum values
3. Group files such that within each group:
   - Files are non-overlapping (file₁.max < file₂.min)
   - Sort order is maintained
4. Target the desired number of partitions while respecting ordering constraints

**Benefits:**
- Enables parallel processing while maintaining sort order
- Optimizes for both performance and correctness
- Leverages file-level statistics for intelligent placement

### 5. Physical Execution Planning

The `try_create_output_ordering` method bridges logical and physical planning:

```rust
fn try_create_output_ordering(&self) -> Result<Vec<LexOrdering>> {
    create_ordering(&self.table_schema, &self.options.file_sort_order)
}
```

**Integration with query optimizer:**
- Converts logical sort expressions to physical sort expressions
- Informs the execution engine about ordering guarantees
- Enables elimination of redundant sort operations in query plans

## Implementation Details

### Core Data Structures

```rust
#[derive(Debug, Clone)]
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    file_schema: SchemaRef,
    table_schema: SchemaRef,
    options: ListingOptions,
    // ... other fields
}

#[derive(Clone, Debug)]
pub struct ListingOptions {
    pub file_sort_order: Vec<Vec<SortExpr>>,
    pub target_partitions: usize,
    // ... other configuration
}
```

### File Processing Pipeline

1. **Discovery**: `list_files_for_scan` discovers files from configured paths
2. **Statistics**: Optionally collect file-level statistics
3. **Grouping**: `split_files` groups files for parallel processing
4. **Ordering**: Apply ordering constraints based on configuration
5. **Execution**: Create physical execution plan with ordering information

### Key Code Locations

| Component | File Path | Key Methods |
|-----------|-----------|-------------|
| ListingTable | `datafusion/core/src/datasource/listing/table.rs` | `try_create_output_ordering`, `scan_with_args` |
| File Grouping | `datafusion/datasource/src/file_groups.rs` | `split_files`, `repartition_preserving_order` |
| Statistics | `datafusion/datasource/src/file_scan_config.rs` | `split_groups_by_statistics_with_target_partitions` |
| Physical Expressions | `datafusion/physical-expr/src/physical_expr.rs` | `create_ordering` |

## Use Cases and Benefits

### 1. Time-Series Data
- Files often naturally ordered by timestamp
- ListingTable preserves temporal ordering for efficient range queries
- Enables time-based partition pruning

### 2. Partitioned Datasets
- Hive-style partitioning with inherent ordering (e.g., by date)
- Maintains partition order while enabling parallel access
- Optimizes queries with partition-column predicates

### 3. Pre-Sorted Files
- Parquet files with embedded sort metadata
- CSV files sorted by specific columns
- Leverages existing ordering to avoid unnecessary sorting

### 4. Query Optimization
- Window functions benefit from preserved ordering
- ORDER BY clauses can be eliminated when file ordering matches
- Merge joins can be used instead of hash joins

## Configuration Examples

### Basic File Sort Order
```rust
// Files are sorted by customer_id, then order_date
let file_sort_order = vec![vec![
    col("customer_id").sort(true, false),
    col("order_date").sort(true, false)
]];

let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_file_sort_order(file_sort_order);
```

### Multiple Equivalent Orderings
```rust
// Files could be sorted by either ordering
let file_sort_order = vec![
    vec![col("region").sort(true, false), col("sales").sort(false, false)],
    vec![col("region").sort(true, false), col("profit").sort(false, false)]
];

let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_file_sort_order(file_sort_order);
```

### Order-Preserving Partitioning
```rust
let config = ListingTableConfig::new(table_path)
    .with_listing_options(options)
    .with_schema(schema);

// The table will automatically preserve file ordering based on configuration
let table = ListingTable::try_new(config)?;
```

## Limitations and Considerations

### 1. File-Level Granularity
- Ordering guarantees apply at the file level, not record level within files
- Mixing records from different files requires careful consideration
- Best suited for scenarios where files are naturally pre-ordered

### 2. Statistics Dependency
- Advanced ordering features require file statistics
- Statistics collection can add overhead during table initialization
- Not all file formats support comprehensive statistics

### 3. Partition Count Constraints
- Order-preserving partitioning may limit parallelism
- Target partition count may not be achievable with ordering constraints
- Trade-off between parallelism and ordering guarantees

### 4. Object Store Limitations
- Underlying object stores don't guarantee file listing order
- Path-based sorting provides consistency but may not reflect desired data order
- External tools may be needed to ensure proper file organization

## Best Practices

### 1. File Organization
- Name files to reflect their logical ordering (e.g., with timestamps)
- Use partitioning schemes that align with query patterns
- Consider file size when planning parallel processing

### 2. Configuration
- Specify `file_sort_order` when files are pre-sorted
- Enable statistics collection for optimal file grouping
- Set appropriate `target_partitions` based on cluster resources

### 3. Query Patterns
- Design queries to leverage file-level ordering
- Use partition pruning predicates when possible
- Consider ordering requirements in ETL pipelines

## Conclusion

DataFusion's `ListingTable` provides sophisticated ordering mechanisms that serve multiple purposes:

- **Consistency**: Ensures reproducible query execution through path-based file sorting
- **Optimization**: Leverages file-level sort orders for query plan optimization  
- **Intelligence**: Uses statistics to group files optimally while preserving ordering
- **Flexibility**: Supports multiple equivalent orderings and various partitioning strategies

These capabilities make `ListingTable` particularly effective for time-series data, partitioned datasets, and scenarios where files have natural ordering that should be preserved and leveraged for query optimization.

The ordering is not about sorting all data globally, but rather about maintaining consistency, preserving known orders, and enabling intelligent query optimization based on file-level characteristics.