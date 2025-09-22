# Implementation Plan: Weighted Reservoir Sampling for OTAP Data

## 1. Objective

This document outlines the implementation plan for creating a head-sampling solution for OTAP (OpenTelemetry Arrow Profile) data stored in Parquet files. The goal is to leverage the query-driven Parquet receiver, powered by DataFusion, to perform weighted reservoir sampling using a custom User-Defined Aggregate Function (UDAF).

The final output will be a stream of OTAP `RecordBatch`es representing a statistically sound sample of the original data, enriched with an "adjusted count" representing how many original events each sampled event represents in the population.

## 2. Background & Strategy

Our strategy is based on several documents:

[query-first-parquet-receiver.md](query-first-parquet-receiver.md)

1. [query-first-parquet-receiver.md](query-first-parquet-receiver.md): This establishes the architecture for processing time-windowed Parquet files using DataFusion and SQL.
2. [sampler-implementation-plan.md](sampler-implementation-plan.md) : This provides the detailed design for the `weighted_reservoir_sample` UDAF, which is the core of our sampling logic.
3. [arrow-compute-optimization-plan.md](arrow-compute-optimization-plan.md): This provides feedback on the original parquet_receiver approach in terms of low-level Arrow kernels.

A sample UDAF for simple reserver sampling is given (but not built/tested) showing the Datafusion we will use.

By treating our Parquet files as a queryable, star-schema-like database (as explored in the [OTel Arrow Delta Lake article](https://www.rakirahman.me/otel-arrow-delta-lake/)), we can use a powerful SQL-based approach to define and execute our sampling policy.

## 🎯 **Core Performance Strategy**

Our approach leverages DataFusion's query optimizer for maximum efficiency:

**Sampling Decision Phase** - Minimal data processing:
- **Time filtering**: DataFusion pushes down time predicates to minimize Parquet reads
- **Columnar efficiency**: Only read necessary columns (`id`, `service.name`, `sampling.adjusted_count`)
- **Lean joins**: Only `resource_attributes` + `log_attributes` - no heavy `logs` table data
- **Optimized execution**: DataFusion's physical planner handles join ordering and parallelization

```sql
-- This query is designed to be extremely fast due to DataFusion optimizations
WITH logs_with_existing_weights AS (
    SELECT
        l.id,  -- Only need the ID, not the full log record
        ra.str as service_name,
        COALESCE(CAST(la.str AS DOUBLE), 1.0) as input_weight
    FROM logs l  -- Time-filtered, minimal column reads
    JOIN resource_attributes ra ON l.id = ra.parent_id AND ra.key = 'service.name'  
    LEFT JOIN log_attributes la ON l.id = la.parent_id AND la.key = 'sampling.adjusted_count'
)
SELECT service_name, weighted_reservoir_sample(...) FROM logs_with_existing_weights GROUP BY service_name;
```

**Key Insight**: Sampling decisions require very little data compared to final reconstruction. DataFusion's columnar processing and predicate pushdown make this extremely efficient.

## ⚖️ **Performance Trade-offs: DataFusion vs Hybrid Approach**

**The Core Question**: Do we execute multiple DataFusion queries or use a hybrid approach?

### Option 1: Multiple DataFusion Queries
```sql
-- Query 1: Sampling + log_attributes reconstruction
WITH sampling_decisions AS (...), 
combined_log_attributes AS (...)
SELECT * FROM combined_log_attributes ORDER BY parent_id, key;

-- Query 2: Filtered logs  
WITH sampling_decisions AS (...)
SELECT l.* FROM logs l JOIN sampling_decisions sd ON l.id = sd.sampled_id;

-- Query 3: Filtered resource_attributes
-- Query 4: Filtered scope_attributes
```

**Concerns**:
- **Repeated work**: The `sampling_decisions` CTE is computed 4 times
- **I/O redundancy**: Same Parquet files read multiple times  
- **Memory**: Multiple query executions vs single execution plan

### Option 2: Hybrid Approach
```sql  
-- Single DataFusion query: Sampling + log_attributes
WITH sampling_decisions AS (...), 
combined_log_attributes AS (...)
SELECT sampled_ids, combined_log_attributes FROM ...;
```
```rust
// Manual filtering with existing optimized code
let filtered_logs = existing_receiver.filter_by_ids(sampled_ids);
let filtered_resource_attrs = existing_receiver.filter_by_parent_ids(sampled_ids);  
// etc.
```

**Trade-offs**:
- **✅ No repeated computation**: Sampling decisions computed once
- **✅ Proven filtering**: Reuse battle-tested Parquet filtering logic
- **❓ I/O patterns**: Still reading same files, but potentially better caching
- **❓ Complexity**: Mixed DataFusion + manual approach

**Unknown**: Can DataFusion optimize multiple queries that share CTEs? Does it cache intermediate results across queries in the same session?

## 3. Conceptual SQL Query

The heart of this implementation will be a SQL query executed by DataFusion for each time window. This query will join the necessary tables (e.g., `logs`, `log_attributes`, `resource_attributes`), calculate a weight, and apply the sampling UDAF.

```sql
-- Optimized query for sampling decisions with input weight calculation
-- The UDAF calculates input weights based on existing sampling.adjusted_count attributes

-- Step 1: Reconstruct a minimal view with fields needed for sampling and weight calculation
WITH logs_with_existing_weights AS (
    SELECT
        l.id,
        ra.str as service_name,
        -- Get existing adjusted_count if it exists, otherwise use 1.0 as default weight
        COALESCE(
            CAST(
                (SELECT la.str FROM log_attributes la 
                 WHERE la.parent_id = l.id AND la.key = 'sampling.adjusted_count'
                 LIMIT 1) AS DOUBLE
            ), 
            1.0
        ) as input_weight
    FROM
        logs l
    JOIN
        resource_attributes ra ON l.id = ra.parent_id AND ra.key = 'service.name'
)

-- Step 2: Apply the UDAF to get sampling decisions
SELECT
    service_name,
    -- The UDAF uses the input_weight for weighted sampling decisions
    weighted_reservoir_sample(
        STRUCT(
            id,
            input_weight
        ),
        100 -- The desired sample size 'k'
    ) AS sample_decisions
FROM
    logs_with_existing_weights
GROUP BY
    service_name;
```

## 4. Implementation Phases

### Phase 1: Implement the `WeightedReservoirSampleUDF`

- **Task**: Implement the `WeightedReservoirSampleUDF` struct and the `WeightedReservoirAccumulator` as detailed in `weighted_reservoir_sampling_design.md`.
- **Details**:
  - Implement the `update_batch`, `merge_batch`, and `evaluate` methods for the accumulator.
  - The core logic will use a `BinaryHeap` to manage the reservoir based on keys derived from input weights.
  - **Input Weight Calculation**: For each log record:
    - If no `sampling.adjusted_count` attribute exists, use weight = 1.0 (original unsampled data)
    - If `sampling.adjusted_count` attribute exists, parse its value as the input weight (previously sampled data)
    - This preserves the unbiased property when re-sampling already sampled data
  - The `evaluate` method must calculate the `adjusted_weight` (representativity factor) for each sampled item and return a `List<Struct<value, adjusted_weight>>`.
  - Note: The `adjusted_weight` represents how many original events this sample represents and will become the `sampling.adjusted_count` in OTAP.
- **Deliverable**: A fully implemented and unit-tested Rust module containing the UDAF.

### Phase 2: Standalone Integration Test

- **Task**: Create a standalone integration test to verify the UDAF works within DataFusion.
- **Details**:
  - In the test, create a `SessionContext`.
  - Register the `weighted_reservoir_sample` UDAF with the context.
  - Construct several in-memory `RecordBatch`es to simulate the `logs_with_service` table.
  - Execute the conceptual SQL query against the in-memory data.
  - Assert that the output has the correct schema and that the sampling results are plausible.
- **Deliverable**: A passing integration test that proves the UDAF is correctly registered and executed by DataFusion.

### Phase 3: Integrate UDAF into the Query-Driven Receiver

- **Task**: Modify the query-driven Parquet receiver to use the sampling UDAF.
- **Details**:
  - The receiver's configuration should be updated to accept a sampling query string.
  - The receiver's `SessionContext` must be initialized with the `weighted_reservoir_sample` UDAF.
  - The receiver will execute the provided sampling query against the Parquet files for each time window.
- **Deliverable**: The receiver can successfully execute the sampling query over a directory of Parquet files.

### Phase 4: Hybrid DataFusion + Manual Reconstruction

- **Task**: Use DataFusion for sampling and `log_attributes` reconstruction, then leverage existing Parquet receiver logic for other tables.
- **Approach**: Hybrid strategy that combines the best of both worlds:
  1. DataFusion handles the complex sampling + `log_attributes` table reconstruction
  2. Existing Parquet receiver logic filters other tables based on sampled IDs

## 🔄 **Major Architectural Shift: Query-Driven First**

**Key Insight**: The current receiver is organized around the `logs` table as primary, but our sampling approach makes `log_attributes` the primary output of the DataFusion query.

### Current Architecture (logs-driven):
```
logs table (primary) → join with log_attrs, resource_attrs, scope_attrs (secondary)
```

### New Architecture (query-driven):
```
DataFusion query → log_attrs table (primary) → join with logs, resource_attrs, scope_attrs
```

**This requires reorganizing the streaming merge loop**:
- **Primary table**: `log_attributes` (from DataFusion query output)  
- **Secondary tables**: `logs`, `resource_attributes`, `scope_attributes`
- **Join logic**: Based on `parent_id` ranges from the DataFusion result

### Universal Query-Driven Design

**Default behavior** (100% sampling):
```sql
-- This becomes our "pass-through" query - no actual sampling
SELECT 
    parent_id,
    'sampling.adjusted_count' as key,
    '1.0' as str,
    -- other log_attributes columns
FROM log_attributes
UNION ALL  
SELECT * FROM log_attributes  -- Original attributes
ORDER BY parent_id, key;
```

**Sampling behavior**:
```sql  
-- The weighted sampling query we've been designing
WITH sampling_decisions AS (...),
original_log_attributes AS (...),
adjusted_count_attributes AS (...)
SELECT * FROM original_log_attributes UNION ALL SELECT * FROM adjusted_count_attributes;
```

- **Implementation Details**:
  - **DataFusion Query** (handles the complex part):

    ```sql
    WITH sampling_decisions AS (
      -- Existing UDAF sampling query, exploded to individual rows
      -- Returns: sampled_id, service_name, adjusted_weight
    ),
    original_log_attributes AS (
      SELECT la.* FROM log_attributes la
      JOIN sampling_decisions sd ON la.parent_id = sd.sampled_id  
    ),
    adjusted_count_attributes AS (
      SELECT 
        sd.sampled_id as parent_id,
        'sampling.adjusted_count' as key,
        sd.adjusted_weight::TEXT as str,
        -- Include all other log_attributes columns with appropriate defaults
      FROM sampling_decisions sd
    )
    SELECT * FROM original_log_attributes
    UNION ALL
    SELECT * FROM adjusted_count_attributes
    ORDER BY parent_id, key;
    ```

  - **Manual Filtering** (reuses existing efficient code):
    - Extract the list of `sampled_id`s from the DataFusion result
    - Use existing Parquet receiver logic to filter:
      - `logs` table (WHERE id IN sampled_ids)
      - `resource_attributes` table (WHERE parent_id IN sampled_ids)  
      - `scope_attributes` table (WHERE parent_id IN sampled_ids)

- **Benefits**:
  - **Complexity**: Only `log_attributes` needs complex DataFusion logic
  - **Performance**: Leverages existing optimized filtering code
  - **Maintainability**: Reuses proven Parquet receiver logic
  - **Clarity**: Clear separation between sampling logic and mechanical filtering

- **Deliverable**: A function that executes the DataFusion query for `log_attributes` reconstruction and uses existing Parquet receiver methods to filter other tables based on sampled IDs.


## 5. Success Criteria

- The `weighted_reservoir_sample` UDAF is implemented, tested, and correctly performs weighted sampling on minimal input data.
- The query-driven Parquet receiver can execute a sampling decision query containing the UDAF on time-windowed Parquet data.
- The system correctly extracts sampling decisions from the UDAF's `List<Struct>` output.
- The final output preserves the original OTAP table structure (logs, log_attributes, resource_attributes, scope_attributes) with proper filtering based on sampling decisions.
- The `sampling.adjusted_count` attribute is correctly inserted into the `log_attributes` table, maintaining sort order by `parent_id`.
