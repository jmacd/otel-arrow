# Implementation Plan: Weighted Reservoir Sampling for OTAP Data

## 1. Objective

This document outlines the implementation plan for creating a head-sampling solution for OTAP (OpenTelemetry Arrow Profile) data stored in Parquet files. The goal is to leverage the query-driven Parquet receiver, powered by DataFusion, to perform weighted reservoir sampling using a custom User-Defined Aggregate Function (UDAF).

The final output will be a stream of OTAP `RecordBatch`es representing a statistically sound sample of the original data, enriched with an "adjusted count" representing how many original events each sampled event represents in the population.

## 2. Background & Strategy

Our strategy is based on two key documents:

1.  **`query-driven-parquet-receiver-design.md`**: This establishes the architecture for processing time-windowed Parquet files using DataFusion and SQL.
2.  **`weighted_reservoir_sampling_design.md`**: This provides the detailed design for the `weighted_reservoir_sample` UDAF, which is the core of our sampling logic.

By treating our Parquet files as a queryable, star-schema-like database (as explored in the [OTel Arrow Delta Lake article](https://www.rakirahman.me/otel-arrow-delta-lake/)), we can use a powerful SQL-based approach to define and execute our sampling policy.

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

### Phase 4: Process UDAF Output and Join with Original Data

- **Task**: Transform the UDAF output into sampling decisions and join back with the original OTAP tables.
- **Details**:
  - The UDAF query result will be a `RecordBatch` with columns: `service_name` and `sample` (a `ListArray` of structs).
  - Extract the sampling decisions: "explode" the `ListArray` to get individual rows with `{part_id, id, adjusted_weight}`.
  - Use either DataFusion or the existing Parquet receiver logic to join the sampling decisions with the original tables (`logs`, `log_attributes`, `resource_attributes`, `scope_attributes`).
  - For the `sampling.adjusted_count` attribute: insert new rows into the `log_attributes` table with `key='sampling.adjusted_count'` and `str=adjusted_weight.to_string()`, representing the estimated count of original events this sample represents, maintaining sort order by `parent_id`.
  - The final output preserves the original OTAP structure with filtered data plus the new sampling attribute.
- **Deliverable**: A function that takes UDAF results and produces complete OTAP `RecordBatch`es (logs, log_attributes, etc.) with sampling applied and `sampling.adjusted_count` properly interleaved.

## 5. Success Criteria

- The `weighted_reservoir_sample` UDAF is implemented, tested, and correctly performs weighted sampling on minimal input data.
- The query-driven Parquet receiver can execute a sampling decision query containing the UDAF on time-windowed Parquet data.
- The system correctly extracts sampling decisions from the UDAF's `List<Struct>` output.
- The final output preserves the original OTAP table structure (logs, log_attributes, resource_attributes, scope_attributes) with proper filtering based on sampling decisions.
- The `sampling.adjusted_count` attribute is correctly inserted into the `log_attributes` table, maintaining sort order by `parent_id`.
