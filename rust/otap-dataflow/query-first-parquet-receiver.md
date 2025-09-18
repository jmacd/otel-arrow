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

## Core Concept

### Current Architecture (Logs-Driven)

```
logs (primary) → stream join → log_attrs, resource_attrs, scope_attrs (secondary)
```

### New Architecture (Query-Driven + Temporal Windowing)

```
Time Window → DataFusion Query → log_attrs (primary) → filter → logs, resource_attrs, scope_attrs (secondary)
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

This is an example with no filter except the necessary temporal range

```sql
-- Default behavior: select all log attributes within time window
-- Temporal filtering applied at logs level, filters propagated to log_attributes
WITH windowed_logs AS (
    SELECT l.*, '{partition_id}' as _part_id
    FROM logs_{partition_id} l
    WHERE l.timestamp_unix_nano >= {window_start_ns} 
      AND l.timestamp_unix_nano < {window_end_ns}
)
SELECT la.*, wl._part_id
FROM windowed_logs wl
JOIN log_attributes_{partition_id} la ON wl.id = la.parent_id
ORDER BY _part_id, parent_id, key;
```

### 2. Filtered Query with Temporal Boundaries

This is an example of a query that only filters the data.


```sql
-- Apply severity and other filters at logs level within time window
WITH windowed_filtered_logs AS (
    SELECT l.*, '{partition_id}' as _part_id
    FROM logs_{partition_id} l  
    WHERE l.timestamp_unix_nano >= {window_start_ns}
      AND l.timestamp_unix_nano < {window_end_ns}
      AND l.severity_number >= 17  -- Error and above
      AND l.trace_id IS NOT NULL
)
SELECT la.*, wfl._part_id
FROM windowed_filtered_logs wfl
JOIN log_attributes_{partition_id} la ON wfl.id = la.parent_id
ORDER BY _part_id, parent_id, key;
```

### 3. Weighted Sampling Query

This is from [sampler-implementation-plan.md](sampler-implementation-plan.md)

```sql
-- Complex sampling with temporal filtering and adjusted counts
WITH windowed_logs_with_weights AS (
    SELECT 
        l.id,
        ra.str as service_name,
        -- Get existing adjusted_count if it exists, otherwise use 1.0 as default weight
        COALESCE(CAST(la.str AS DOUBLE), 1.0) as input_weight,
        '{partition_id}' as _part_id
    FROM logs_{partition_id} l
    -- TEMPORAL FILTERING: Only process records within the time window
    WHERE l.timestamp_unix_nano >= {window_start_ns} 
      AND l.timestamp_unix_nano < {window_end_ns}
      -- Additional filters can be applied here (severity, trace_id, etc.)
    JOIN resource_attributes_{partition_id} ra ON l.id = ra.parent_id AND ra.key = 'service.name'
),
sampling_decisions AS (
    -- Apply weighted reservoir sampling UDAF
    SELECT
        service_name,
        _part_id,
        weighted_reservoir_sample(
            STRUCT(
                id,
                input_weight
            ),
            100 -- The desired sample size 'k'
        ) AS sample_decisions
    FROM windowed_logs_with_weights
    GROUP BY service_name, _part_id
),
exploded_sampling_decisions AS (
    -- "Explode" the ListArray to get individual sampled records
    SELECT
        _part_id,
        service_name,
        decision.value.id as sampled_id,
        decision.value.adjusted_weight as adjusted_weight
    FROM sampling_decisions
    CROSS JOIN UNNEST(sample_decisions) AS t(decision)
),
original_log_attributes AS (
    -- Get original attributes for sampled records only
    SELECT la.*, esd._part_id
    FROM log_attributes_{partition_id} la
    JOIN exploded_sampling_decisions esd ON la.parent_id = esd.sampled_id
),
adjusted_count_attributes AS (
    -- Add new sampling.adjusted_count attributes
    SELECT 
        esd.sampled_id as parent_id,
        'sampling.adjusted_count' as key,
        esd.adjusted_weight::TEXT as str,
        esd._part_id,
        -- Add other required columns from log_attributes schema with defaults
    FROM exploded_sampling_decisions esd
)
-- Return the combined log_attributes (original + adjusted_count)
SELECT * FROM original_log_attributes
UNION ALL
SELECT * FROM adjusted_count_attributes
ORDER BY _part_id, parent_id, key;
```

### 4. Cross-Partition Aggregation with Temporal Windows

For cross-partition queries, we use the same base query pattern (pass-through, filtered, or sampling) but apply it to each partition's tables and combine with UNION ALL:

```sql
-- Example: Cross-partition filtered query
WITH all_partitions AS (
  -- Partition A
  SELECT la.*, 'partition_A' as _part_id
  FROM windowed_logs_A wl
  JOIN log_attributes_partition_A la ON wl.id = la.parent_id
  
  UNION ALL
  
  -- Partition B  
  SELECT la.*, 'partition_B' as _part_id
  FROM windowed_logs_B wl
  JOIN log_attributes_partition_B la ON wl.id = la.parent_id
  
  -- ... additional partitions
)
SELECT * FROM all_partitions ORDER BY _part_id, parent_id, key;

-- Where windowed_logs_X are temporal + filter CTEs:
-- WITH windowed_logs_A AS (
--   SELECT l.* FROM logs_partition_A l
--   WHERE l.timestamp_unix_nano >= {window_start_ns}
--     AND l.timestamp_unix_nano < {window_end_ns}
-- )
```

## Query Result Format

All queries must return records in this format:
```
{_part_id, parent_id, key, str, bool, int, double, ...}
```

This represents the selected `log_attributes` records, grouped by partition, with all necessary OTAP reconstruction information.

## Streaming Merge Logic

### New Flow
1. **Execute Query**: Run SQL against registered parquet tables
2. **Extract Parent IDs**: Get `{_part_id, parent_id}` tuples from query results
3. **Filter Other Tables**: Use existing streaming logic to fetch matching records:
   - `logs` WHERE `id IN (parent_ids)` per partition
   - `resource_attributes` WHERE `parent_id IN (parent_ids)` per partition  
   - `scope_attributes` WHERE `parent_id IN (parent_ids)` per partition
4. **Reconstruct OTAP**: Combine query results (log_attributes) with filtered tables

### Key Insight
The query result becomes the "primary" table that drives what gets included in the final OTAP batch. The streaming merge becomes a **filter operation** rather than a **discovery operation**.

## DataFusion Integration with Temporal Windowing

### Windowed Table Registration

**Key Concept**: For each time window, we discover all parquet files that overlap with the window and register them as partition-specific ListingTables in DataFusion.

**Table Naming Convention**: `{table_type}_{partition_id}` (e.g., `logs_partition_A`, `log_attributes_partition_B`)

**Process**:
1. **File Discovery**: Find all parquet files older than safety margin that overlap the time window
2. **Partition Grouping**: Group files by partition ID and table type (logs, log_attributes, resource_attributes, scope_attributes)
3. **ListingTable Creation**: Create DataFusion ListingTable for each partition/table combination
4. **Registration**: Register tables with names like `logs_partition_A`, `log_attributes_partition_A`

**Result**: DataFusion context has tables named by partition, ready for query execution with temporal predicate pushdown.

### Windowed Query Assembly

**Key Concept**: We take a user query pattern (pass-through, filtered, sampling) and apply it to each partition's registered tables, combining with temporal boundaries and UNION ALL.

**Query Assembly Process**:

1. **Template Substitution**: For each partition, substitute the registered table names and partition ID:
   - `{logs_table}` → `logs_partition_A`
   - `{log_attributes_table}` → `log_attributes_partition_A`  
   - `{partition_id}` → `'partition_A'`

2. **Temporal Injection**: Add time window boundaries to the WHERE clause:
   - `l.timestamp_unix_nano >= {window_start_ns}`
   - `l.timestamp_unix_nano < {window_end_ns}`

3. **Partition Combination**: Combine all partition queries with `UNION ALL`:
   ```sql
   WITH all_partitions AS (
     -- Partition A query
     SELECT la.*, 'partition_A' as _part_id FROM ...
     UNION ALL
     -- Partition B query  
     SELECT la.*, 'partition_B' as _part_id FROM ...
   )
   SELECT * FROM all_partitions ORDER BY _part_id, parent_id, key
   ```

**Result**: A single SQL query that processes all partitions within the time window, returning log_attributes with `_part_id` for downstream OTAP reconstruction.

## Implementation Plan

### Phase 1: Query Interface
- Define `QuerySpecification` struct with SQL query and parameters
- Implement DataFusion context initialization and table registration
- Build query execution pipeline that returns `log_attributes` results

### Phase 2: Streaming Integration  
- Modify existing streaming merge to accept `log_attributes` as primary input
- Implement filtering logic for `logs`, `resource_attributes`, `scope_attributes`
- Ensure partition-aware processing maintains ID correlation

### Phase 3: UDAF Integration
- Implement `weighted_reservoir_sample` UDAF (see `sampler-implementation-plan.md`)
- Register UDAF with DataFusion context
- Test end-to-end sampling workflow

## Benefits

1. **Flexibility**: Support any SQL query from simple pass-through to complex sampling
2. **Performance**: DataFusion optimizations handle predicate pushdown and join optimization  
3. **Maintainability**: Single query interface for all processing modes
4. **Extensibility**: Easy to add new UDAFs and query patterns
5. **Compatibility**: Existing OTAP reconstruction logic remains largely unchanged

## Migration Path

1. **Default Query**: Start with `SELECT * FROM log_attributes` (equivalent to current behavior)
2. **Gradual Enhancement**: Add query parameters and custom queries over time
3. **Sampling Integration**: Deploy weighted sampling as advanced feature
4. **Full Query Support**: Enable arbitrary SQL queries for advanced use cases

## Success Criteria

- [ ] Pass-through query produces identical results to current receiver
- [ ] Weighted sampling query works with sampler-implementation-plan.md
- [ ] Cross-partition aggregation queries execute successfully  
- [ ] Performance is comparable or better than current streaming approach
- [ ] All existing OTAP reconstruction logic continues to work

---

**Key Insight**: This design inverts the current architecture to make queries the driving force, while preserving all existing OTAP reconstruction capabilities. The result is a flexible, powerful system that can handle everything from simple pass-through to sophisticated sampling and analytics.
