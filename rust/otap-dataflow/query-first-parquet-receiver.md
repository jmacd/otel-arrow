# Query-First Parquet Receiver Design

**Status**: 🎯 **DESIGN PROPOSAL**  
**Date**: September 18, 2025

## Problem Statement

The current parquet receiver is hard-coded with a "select * from logs" approach and uses streaming joins with the other 3 tables. This limits flexibility and makes advanced processing like sampling difficult to implement.

## Solution: Query-First with Temporal Windowing

Flip the receiver to be **query-driven first** where queries drive data selection, combined with **temporal windowing** that processes time-aligned windows of parquet files after they've aged sufficiently to ensure completeness.

### Temporal Processing Model

The system processes **time windows** (e.g., 1-minute intervals aligned to wall clock) after waiting for a **safety margin** to ensure all files for that window are complete:

- **File Aging**: Wait for files to be old enough (configurable delay) before processing
- **Window Processing**: Process minute-wide (or configurable) time windows aligned to wall clock  
- **DataFusion Queries**: Each window becomes a separate DataFusion query across all partitions
- **Temporal Filtering**: Query includes time boundaries to restrict data to the window

## Core Concept: 4 Partitioned Tables with Virtual Partition Columns

### DataFusion Partition Column Architecture

Instead of registering N×4 individual tables, we use **4 partitioned ListingTables** with DataFusion's built-in **partition columns**:

```rust
// ✅ 4 table registrations (one per OTAP table type) with partition columns
for table_type in ["logs", "log_attributes", "resource_attributes", "scope_attributes"] {
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_table_partition_cols(vec![
            ("_part_id".to_string(), DataType::Utf8),  // partition_A, partition_B, etc.
        ]);
    
    let table = ListingTable::try_new(
        config_for_table_type(table_type)
            .with_listing_options(listing_options)
    )?;
    
    ctx.register_table(table_type, Arc::new(table))?;
}
```

**Key Benefits:**
- **4 table registrations** instead of N×4 registrations
- **Virtual `_part_id` column** automatically populated by DataFusion
- **Automatic predicate pushdown** on partition columns
- **Logical separation** of OTAP table types maintained
- **Simple queries** with no complex UNION ALL needed

### Unified Schema Per Table Type

Each OTAP table type gets its own partitioned table:

```sql
-- logs table with partition column
logs: id, timestamp_unix_nano, severity_number, trace_id, _part_id

-- log_attributes table with partition column  
log_attributes: parent_id, key, str, bool, int, double, _part_id

-- resource_attributes table with partition column
resource_attributes: parent_id, key, str, bool, int, double, _part_id

-- scope_attributes table with partition column
scope_attributes: parent_id, key, str, bool, int, double, _part_id
```

## Essential Design Principles

1. **Temporal Windows**: Process time-aligned windows (e.g., 1-minute) after safety margin
2. **Query-First**: Every window starts with a SQL query against the OTAP tables within time boundaries
3. **Partition-Aware**: Queries are executed within partitions via ListingTable registration, results include `_part_id`
4. **Attributes-Primary**: `log_attributes` becomes the driving table output, not `logs`
5. **Flexible Sampling**: Supports everything from 100% pass-through to complex weighted sampling

## Temporal Window Processing

### File Aging and Window Assembly

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
```

### Window-Based Processing Flow

```
File System Monitoring → Time Window Assembly → DataFusion Query → OTAP Output
        ↑                        ↑                     ↑              ↑
   Files old enough         All files covering     Time-filtered    Aggregated
   (> safety_margin)        current window         query            results
```

## Query Types

### 1. Pass-Through Query (100% sampling)

With 4 partitioned tables, the query becomes much simpler:

```sql
-- ✅ Simple query against partitioned log_attributes table
-- ✅ DataFusion: Automatic predicate pushdown on _part_id partition column
-- ✅ DataFusion: Temporal filtering pushed to parquet readers
SELECT *
FROM log_attributes
WHERE timestamp_unix_nano >= {window_start_ns}
  AND timestamp_unix_nano < {window_end_ns}
ORDER BY _part_id, parent_id, key;
```

**DataFusion automatically:**
- Scans only relevant partition files based on predicates
- Includes `_part_id` virtual column for downstream processing
- Applies temporal filters directly to parquet files

### 2. Filtered Query with Temporal Boundaries

Filtering with 4 partitioned tables maintains logical separation:

```sql
-- ✅ Clean JOIN between partitioned tables
-- ✅ DataFusion: Automatic partition pruning on both tables
-- ✅ DataFusion: Predicate pushdown on virtual _part_id columns
SELECT la.*
FROM log_attributes la
JOIN logs l ON l.id = la.parent_id AND l._part_id = la._part_id
WHERE la.timestamp_unix_nano >= {window_start_ns}
  AND la.timestamp_unix_nano < {window_end_ns}
  AND l.severity_number >= 17  -- Error and above
  AND l.trace_id IS NOT NULL
ORDER BY la._part_id, la.parent_id, la.key;
```

**Benefits:**
- Natural JOINs between logical table types
- Automatic partition alignment via `_part_id`
- Clean predicate pushdown to relevant partitions

### 3. Weighted Sampling Query

This is from [sampler-implementation-plan.md](sampler-implementation-plan.md)

### 3. Weighted Sampling Query

Weighted sampling with 4 partitioned tables leverages natural JOINs:

```sql
-- ✅ Clean JOINs between partitioned OTAP tables
-- ✅ DataFusion: Automatic partition alignment and predicate pushdown
WITH service_weights AS (
    SELECT 
        l.id,
        l._part_id,
        ra.str as service_name,
        COALESCE(CAST(la.str AS DOUBLE), 1.0) as input_weight
    FROM logs l
    JOIN resource_attributes ra ON ra.parent_id = l.id 
                                 AND ra._part_id = l._part_id
                                 AND ra.key = 'service.name'
    LEFT JOIN log_attributes la ON la.parent_id = l.id 
                                 AND la._part_id = l._part_id
                                 AND la.key = 'sampling.weight'
    WHERE l.timestamp_unix_nano >= {window_start_ns}
      AND l.timestamp_unix_nano < {window_end_ns}
),
sampling_decisions AS (
    -- Apply weighted reservoir sampling UDAF
    -- ✅ DataFusion: UDAFs work seamlessly with partitioned tables
    SELECT
        service_name,
        _part_id,
        weighted_reservoir_sample(
            STRUCT(id, input_weight),
            100
        ) AS sample_decisions
    FROM service_weights
    GROUP BY service_name, _part_id
),
selected_ids AS (
    -- Explode sampling results
    SELECT
        sd._part_id,
        sd.service_name,
        decision.value.id as sampled_id,
        decision.value.adjusted_weight as adjusted_weight
    FROM sampling_decisions sd
    CROSS JOIN UNNEST(sd.sample_decisions) AS t(decision)
)
-- Return log_attributes for sampled records plus adjusted weight attributes
SELECT 
    la._part_id,
    la.parent_id,
    la.key,
    la.str,
    si.adjusted_weight
FROM log_attributes la
JOIN selected_ids si ON si._part_id = la._part_id AND si.sampled_id = la.parent_id

UNION ALL

-- Add the adjusted weight attributes
SELECT
    si._part_id,
    si.sampled_id as parent_id,
    'sampling.adjusted_count' as key,
    CAST(si.adjusted_weight AS VARCHAR) as str,
    si.adjusted_weight
FROM selected_ids si
ORDER BY _part_id, parent_id, key;
```
```

### 4. Cross-Partition Aggregation

With 4 partitioned tables, cross-partition queries span all partitions automatically:

```sql
-- ✅ DataFusion automatically scans all partitions within each table
-- ✅ No manual UNION ALL needed - partition columns handle this
-- ✅ Clean aggregation across logical table boundaries
SELECT 
    _part_id,
    COUNT(*) as attribute_count,
    COUNT(DISTINCT parent_id) as unique_logs,
    COUNT(DISTINCT key) as unique_keys
FROM log_attributes
WHERE timestamp_unix_nano >= {window_start_ns}
  AND timestamp_unix_nano < {window_end_ns}
  AND key = 'service.name'
GROUP BY _part_id
ORDER BY _part_id;
```

**DataFusion automatically:**
- Scans all partition files within the `log_attributes` table
- Groups results by the virtual `_part_id` column
- Applies predicate pushdown across all partitions

## Query Result Format

All queries must return records in this format:
```
{_part_id, parent_id, key, str, bool, int, double, ...}
```

This represents the selected `log_attributes` records, grouped by partition, with all necessary OTAP reconstruction information.

## Streaming Merge Logic

### Simplified Flow with 4 Partitioned Tables
1. **Execute Query**: Run query against partitioned `log_attributes` table
2. **Extract Results**: Query returns log_attributes with `_part_id` included
3. **Filter Other Tables**: Use partition-aware queries for related data:
   ```sql
   -- Get logs for sampled IDs, partition-aligned
   SELECT * FROM logs l
   JOIN selected_ids si ON l.id = si.sampled_id AND l._part_id = si._part_id;
   
   -- Get resource_attributes, partition-aligned
   SELECT * FROM resource_attributes ra
   JOIN selected_ids si ON ra.parent_id = si.sampled_id AND ra._part_id = si._part_id;
   ```
4. **Reconstruct OTAP**: Standard streaming logic with partition-aware results

### Key Advantages
- **Natural table boundaries**: Each OTAP type has its own partitioned table
- **Automatic partition alignment**: `_part_id` ensures correct data relationships
- **Clean query patterns**: JOINs between logical table types work naturally
- **Partition pruning**: DataFusion automatically scans only relevant partitions

## Alternative Architecture: Custom TableProvider with Virtual Partition Columns

## DataFusion Integration with Partition Columns

### Built-in DataFusion Features

#### 1. **Partition Columns (Virtual Columns)**
DataFusion's `ListingTable` natively supports virtual partition columns:

```rust
// ✅ DataFusion: Built-in partition column support
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    .with_table_partition_cols(vec![
        ("_part_id".to_string(), DataType::Utf8),
        ("table_type".to_string(), DataType::Utf8),  // logs, log_attributes, etc.
    ]);
```

#### 2. **Custom TableProvider with Virtual Columns**
You can create a single `TableProvider` that:
- Wraps multiple partitions of parquet files
- Automatically injects partition values as constant columns
- Handles temporal windowing at the provider level

### Simplified Architecture Design

```rust
// Single table provider for all OTAP data with partition columns
#[derive(Debug)]
pub struct OtapTemporalTableProvider {
    /// Time window being processed
    time_window: TimeRange,
    /// Map of partition_id -> table_type -> parquet files
    partition_files: HashMap<String, HashMap<String, Vec<String>>>,
    /// Combined schema with virtual columns
    schema: SchemaRef,
}

impl TableProvider for OtapTemporalTableProvider {
    fn schema(&self) -> SchemaRef {
        // Schema includes both data columns AND virtual columns:
        // - Original columns (id, timestamp_unix_nano, severity_number, etc.)
        // - _part_id: Utf8  (virtual partition column)
        // - _table_type: Utf8 (virtual: "logs", "log_attributes", etc.)
        self.schema.clone()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ✅ Single execution plan handles all partitions and table types
        // ✅ Virtual columns automatically populated during scan
        // ✅ Predicate pushdown works on both real and virtual columns
        Ok(Arc::new(OtapTemporalScanExec::new(
            self.time_window.clone(),
            self.partition_files.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }
}
```

### Drastically Simplified Query Structure

Instead of complex multi-CTE queries, you get simple, natural SQL:

```sql
-- ✅ SIMPLE: All data in one table with virtual partition columns
WITH sampled_data AS (
    SELECT 
        id,
        _part_id,
        _table_type,
        str as weight_str,
        weighted_reservoir_sample(
            STRUCT(id, COALESCE(CAST(str AS DOUBLE), 1.0)),
            100
        ) OVER (PARTITION BY _part_id, _table_type) as sample
    FROM otap_temporal  -- Single table!
    WHERE _table_type = 'log_attributes'
      AND key = 'sampling.weight'
      AND timestamp_unix_nano >= {window_start}
      AND timestamp_unix_nano < {window_end}
)
SELECT 
    la._part_id,
    la.parent_id,
    la.key,
    la.str,
    -- Add adjusted weights from sampling
    COALESCE(sample_weights.adjusted_weight, 1.0) as adjusted_weight
FROM otap_temporal la
CROSS JOIN UNNEST(sampled_data.sample) AS t(sample_item)
WHERE la._table_type IN ('log_attributes', 'logs', 'resource_attributes')
  AND la.parent_id = sample_item.value.id;
```

### Implementation Complexity Comparison

| Aspect | Current Multi-Table Approach | Custom TableProvider Approach |
|--------|------------------------------|--------------------------------|
| **Table Registration** | N partitions × 4 tables = 4N registrations | 1 registration total |
| **Query Complexity** | Complex multi-CTE with UNION ALL | Simple single-table queries |
| **Predicate Pushdown** | Manual partition filtering | Automatic via DataFusion |
| **Memory Usage** | N × schemas in memory | Single schema |
| **Type Safety** | Manual schema alignment | Automatic schema handling |
| **Code Maintenance** | Complex query generation logic | Simple TableProvider impl |

### Built-in DataFusion Features You'd Leverage

#### 1. **Partition Column Injection**
```rust
// ✅ DataFusion automatically injects partition values
impl ExecutionPlan for OtapTemporalScanExec {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // DataFusion handles adding virtual columns automatically
        let partition_values = vec![
            (Field::new("_part_id", DataType::Utf8, false), ScalarValue::Utf8(Some(partition_id))),
            (Field::new("_table_type", DataType::Utf8, false), ScalarValue::Utf8(Some(table_type))),
        ];
        
        // Physical expr adapter automatically populates virtual columns
        let adapter = physical_expr_adapter.with_partition_values(partition_values);
    }
}
```

#### 2. **Filter Pushdown on Virtual Columns**
```rust
// ✅ DataFusion: Filters on virtual columns handled automatically
fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    // DataFusion can push down filters like:
    // WHERE _part_id = 'partition_A' AND _table_type = 'logs'
    // Even though these are virtual columns!
}
```

#### 3. **Predicate Elimination**
```rust
// ✅ DataFusion: Automatically eliminates partitions that don't match filters
// If query has WHERE _part_id = 'partition_A', only that partition is scanned
```

### Migration Complexity: Much Lower

**Phase 1**: Implement `OtapTemporalTableProvider`
- Single TableProvider implementation
- Virtual column schema definition
- File discovery and organization logic

**Phase 2**: Replace query generation
- Remove complex multi-table registration
- Remove UNION ALL query generation  
- Simple single-table SQL queries

**Phase 3**: Optimize
- Leverage DataFusion's built-in predicate pushdown
- Use partition pruning automatically

### Performance Benefits

1. **Memory Efficiency**: Single schema vs N×4 schemas
2. **Query Planning**: Simpler plans, faster optimization
3. **Execution**: Better partition pruning and pushdown
4. **Maintenance**: Much less complex code to maintain

### Conclusion

**Yes, this approach would be significantly simpler!** DataFusion's partition column support is exactly designed for this use case. The complexity difference is dramatic:

- **Current approach**: 4N table registrations + complex query generation
- **TableProvider approach**: 1 table registration + simple SQL

This is a much more maintainable and performant architecture that leverages DataFusion's built-in capabilities instead of working around them.

---

## DataFusion Integration with Partition Columns

### 4 Partitioned Table Registration

**Key Concept**: Register 4 tables (one per OTAP type), each spanning all partitions via virtual partition columns.

**Implementation Process**:

1. **File Discovery**: Find all parquet files for the time window, organized by table type and partition
   ```rust
   // Organize files: table_type -> partition_id -> file_paths  
   let files_by_type: HashMap<String, HashMap<String, Vec<String>>> = discover_files(window);
   ```

2. **Per-Table Registration**: Register each OTAP table type with partition columns
   ```rust
   for (table_type, partition_files) in files_by_type {
       let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
           .with_table_partition_cols(vec![
               ("_part_id".to_string(), DataType::Utf8),
           ]);
       
       let config = ListingTableConfig::new(all_files_for_table_type)
           .with_listing_options(listing_options)
           .with_schema(schema_for_table_type(&table_type));
           
       ctx.register_table(&table_type, Arc::new(ListingTable::try_new(config)?));
   }
   ```

**Result**: 4 table registrations, each automatically handling all partitions via virtual `_part_id` column.

### Simplified Query Processing

**Clean Multi-Table Queries:**

```rust
// Natural JOINs between logical table types:
let query = "
    SELECT la.* 
    FROM log_attributes la
    JOIN logs l ON l.id = la.parent_id AND l._part_id = la._part_id
    WHERE la.timestamp_unix_nano >= ? AND la.timestamp_unix_nano < ?
";

// DataFusion automatically:
// - Scans relevant partitions in both tables
// - Aligns partitions via _part_id virtual column  
// - Applies predicate pushdown to parquet files
let results = ctx.sql(&query).await?.collect().await?;
```

## Implementation Plan

## Implementation Plan

### Phase 1: 4 Partitioned Table Registration
- Implement file discovery organized by table type and partition
- Create 4 table registrations with virtual `_part_id` columns
- Build schema definitions for each OTAP table type

### Phase 2: Query Simplification
- Replace N×4 table queries with 4 partitioned table queries
- Implement natural JOINs between OTAP table types
- Test temporal filtering and partition alignment

### Phase 3: UDAF Integration
- Register `weighted_reservoir_sample` UDAF with 4-table context
- Test cross-table sampling with partition alignment
- Verify end-to-end OTAP reconstruction

**Key Benefits:**
- **4 registrations** instead of N×4 registrations
- **Natural table semantics** maintained
- **Automatic partition handling** via DataFusion
- **Clean JOIN patterns** between OTAP types

## Migration Path

1. **Unified Schema**: Create single schema combining all OTAP table types
2. **Partition Columns**: Register table with `_part_id` and `_table_type` virtual columns  
3. **Query Replacement**: Replace complex multi-table queries with simple single-table queries
4. **Testing**: Verify temporal filtering and sampling functionality
5. **Deployment**: Gradual rollout with performance monitoring

## Success Criteria

- [ ] Single table registration handles all partitions and table types
- [ ] Pass-through queries produce identical results to current receiver
- [ ] Weighted sampling queries work with simplified structure
- [ ] Query performance is comparable or better than current approach
- [ ] All existing OTAP reconstruction logic continues to work unchanged

---

**Key Insight**: DataFusion's partition columns feature transforms this from a complex multi-table coordination problem into a simple, elegant single-table design. The architecture becomes dramatically simpler while gaining performance and maintainability benefits.
