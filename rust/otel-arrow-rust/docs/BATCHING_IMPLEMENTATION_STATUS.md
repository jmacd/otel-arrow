# Batching Implementation Status

**Status Document for MVP Logs Batching Implementation**  
Josh MacDonald, October 31, 2025

## Overview

This document describes the current state of the minimum viable product (MVP) implementation for logs batching in `src/otap/groups.rs`. The implementation follows the design outlined in `batching_design.md` and incorporates patterns from the research documents.

## What Has Been Implemented

### 1. Core Public API (Unchanged)

The public API defined in the original `groups.rs` remains intact:

```rust
pub enum RecordsGroup {
    Logs(Vec<[Option<RecordBatch>; Logs::COUNT]>),
    Metrics(Vec<[Option<RecordBatch>; Metrics::COUNT]>),
    Traces(Vec<[Option<RecordBatch>; Traces::COUNT]>),
}

impl RecordsGroup {
    pub fn separate_logs(records: Vec<OtapArrowRecords>) -> Result<Self>
    pub fn separate_metrics(records: Vec<OtapArrowRecords>) -> Result<Self>
    pub fn separate_traces(records: Vec<OtapArrowRecords>) -> Result<Self>
    pub fn rebatch(self, max_output_batch: Option<NonZeroU64>) -> Result<Self>
    pub fn into_otap_arrow_records(self) -> Vec<OtapArrowRecords>
    pub fn is_empty(&self) -> bool
    pub fn len(&self) -> usize
}
```

This API provides:
- **Separation by signal type**: `separate_logs/metrics/traces` methods filter mixed inputs
- **Rebatching**: `rebatch()` method takes batches and produces maximally-full output batches
- **Conversion**: `into_otap_arrow_records()` converts back to output format

### 2. Helper Functions

#### `select<const N: usize>()` - Transpose Iterator

```rust
fn select<const N: usize>(
    batches: &[[Option<RecordBatch>; N]],
    i: usize,
) -> impl Iterator<Item = &RecordBatch>
```

**Purpose**: Provides an iterator over the i-th payload type across all batches.  
**Usage**: Essential for processing specific tables (Logs, LogAttrs, ResourceAttrs, etc.) across multiple input batches.  
**Source**: Preserved from `groups_old.rs` - proven pattern.

#### `primary_table<const N: usize>()` - Find Main Table

```rust
fn primary_table<const N: usize>(
    batches: &[Option<RecordBatch>; N]
) -> Option<&RecordBatch>
```

**Purpose**: Returns the primary table for a batch array based on const generic size.  
**Logic**: Uses pattern matching on N to determine signal type (Logs, Metrics, or Traces).  
**Source**: Preserved from original implementation.

### 3. Main Batching Pipeline

#### `rebatch_logs_single_pass()`

The core batching function implements a five-phase algorithm:

```rust
fn rebatch_logs_single_pass(
    mut items: Vec<[Option<RecordBatch>; Logs::COUNT]>,
    max_batch_size: NonZeroU64,
) -> Result<Vec<[Option<RecordBatch>; Logs::COUNT]>>
```

**Phase 1: Schema Unification** (`unify_logs()`)
- Discovers all fields across all input batches for each payload type
- Creates a unified schema containing all fields from any batch
- Adds null-filled columns to batches missing optional fields
- Handles: Logs, LogAttrs, ResourceAttrs, ScopeAttrs tables

**Phase 2: Concatenation** (`concatenate_logs_batches()`)
- Uses `arrow::compute::concat_batches()` to merge all batches of each payload type
- Results in a single large batch per payload type
- All schemas are unified at this point, making concatenation safe

**Phase 3: Sorting and Deduplication** (`sort_and_deduplicate_logs()`)
- Extracts `Resource.ID` and `Scope.ID` from Logs table struct columns
- Sorts Logs by Resource.ID then Scope.ID using `lexsort_to_indices()`
- Uses `take_record_batch()` to apply sort order
- **Note**: Deduplication of Resource/Scope attributes not yet implemented in this MVP

**Phase 4: Splitting** (`split_logs_to_size()`)
- Calculates number of output batches needed based on target size
- Slices the Logs table into chunks of target size
- **Current limitation**: Attribute tables not yet properly sliced based on PARENT_ID ranges
- **TODO**: Implement proper attribute slicing to follow Logs table splits

**Phase 5: Reindexing** (`reindex_logs()`)
- Creates sequential ID values starting from 0 for each output batch
- **Current implementation**: Simplified - only reindexes Logs.ID column
- **TODO**: Complete implementation needs to:
  - Update LogAttrs.PARENT_ID to match new Logs.ID
  - Reindex Resource.ID and update ResourceAttrs.PARENT_ID
  - Reindex Scope.ID and update ScopeAttrs.PARENT_ID

### 4. Schema Unification Implementation

#### `unify_logs()`

```rust
fn unify_logs(batches: &mut [[Option<RecordBatch>; Logs::COUNT]]) -> Result<()>
```

**Approach**: Simplified from `groups_old.rs` for MVP:
- Collects all unique field names and their definitions across batches
- Builds a unified schema containing all fields
- Adds null-filled columns where fields are missing

**Differences from full implementation**:
- ✅ Handles optional columns (int, double, str value columns in attributes)
- ❌ Does NOT optimize dictionary key types (always keeps existing encoding)
- ❌ Does NOT handle struct field unification recursively
- ❌ Does NOT estimate cardinality for dictionary vs native decisions

**Rationale**: The simplified approach is sufficient for MVP and avoids complexity. Full dictionary optimization can be added later as documented in `BATCHING_COMPARISON.md`.

#### `add_missing_columns()`

```rust
fn add_missing_columns(
    batch: RecordBatch,
    target_schema: &Schema
) -> Result<RecordBatch>
```

**Purpose**: Ensures a batch has all columns from the target schema.  
**Implementation**: For each field in target schema:
- If present in batch: use existing column
- If missing: add null-filled array using `arrow::array::new_null_array()`

### 5. Sorting Implementation

#### `sort_and_deduplicate_logs()`

**Current sorting strategy**:
1. Extract Resource.ID from `Logs.Resource` struct column
2. Extract Scope.ID from `Logs.Scope` struct column  
3. Sort by Resource.ID (primary), then Scope.ID (secondary)
4. Both sort columns use `nulls_first: true` option

**Benefits of this sort order**:
- Groups logs by resource, improving compression
- Groups logs by scope within each resource
- Reduces fan-out in downstream aggregation pipelines
- Prepares for resource/scope deduplication (future work)

**Not yet implemented**:
- TraceID sorting
- Multi-dimensional sorting
- Configurable sort policies
- Sort order metadata tracking

### 6. Reindexing Implementation

#### `reindex_logs()`

**Current implementation** (simplified):
- Iterates through each output batch
- Calls `reindex_id_column()` on Logs.ID if present
- Creates sequential IDs starting from 0 within each batch

#### `reindex_id_column()`

```rust
fn reindex_id_column(
    batch: RecordBatch,
    column_name: &'static str,
    start_id: u16,
) -> Result<RecordBatch>
```

**Implementation**:
- Extracts column by name from schema
- Generates new sequential UInt16 array: `start_id..start_id + num_rows`
- Replaces the column in the batch
- Returns new RecordBatch with reindexed column

**Limitations**:
- Only handles simple ID columns, not struct-nested IDs (Resource.ID, Scope.ID)
- Does not update PARENT_ID columns in child tables
- Does not handle ID ranges across multiple batches

## What Is NOT Yet Implemented

### 1. Attribute Deduplication (High Priority)

**Missing functionality**:
- Resource attribute deduplication using row format comparison
- Scope attribute deduplication using row format comparison
- PARENT_ID translation maps for updating Logs table references

**Design approach** (from `ARROW_GROUPING_DEDUPLICATION.md`):
```rust
// Planned signature
fn deduplicate_resource_attrs(
    batches: Vec<RecordBatch>
) -> Result<(RecordBatch, Vec<HashMap<u16, u16>>)>

fn deduplicate_scope_attrs(
    batches: Vec<RecordBatch>
) -> Result<(RecordBatch, Vec<HashMap<u16, u16>>)>
```

**Required steps**:
1. Use `arrow_row::RowConverter` to convert attribute rows to comparable byte sequences
2. Use `HashMap<OwnedRow, u16>` to track unique attribute sets
3. Return deduplicated batch and per-input-batch translation maps
4. Update Resource.ID and Scope.ID in Logs table using translation maps

### 2. Complete Attribute Slicing

**Current issue**: When splitting the Logs table, attribute tables are not sliced.

**Required implementation**:
- Track which Resource IDs are present in each output Logs batch
- Extract corresponding ResourceAttrs rows based on PARENT_ID
- Track which Scope IDs are present in each output Logs batch
- Extract corresponding ScopeAttrs rows based on PARENT_ID
- Track which Log IDs are present (non-NULL Logs.ID)
- Extract corresponding LogAttrs rows based on PARENT_ID

**Algorithm**:
```rust
// For each output batch of N logs:
let resource_ids: HashSet<u16> = extract_unique_ids(logs_batch, "Resource.ID");
let resource_attrs = filter_by_parent_id(all_resource_attrs, &resource_ids);

let scope_ids: HashSet<u16> = extract_unique_ids(logs_batch, "Scope.ID");
let scope_attrs = filter_by_parent_id(all_scope_attrs, &scope_ids);

let log_ids: HashSet<u16> = extract_non_null_ids(logs_batch, "ID");
let log_attrs = filter_by_parent_id(all_log_attrs, &log_ids);
```

### 3. Complete Reindexing

**Missing reindexing operations**:

1. **Logs.Resource.ID** (nested in struct):
   - Extract Resource struct column
   - Reindex the ID field within the struct
   - Update ResourceAttrs.PARENT_ID to match

2. **Logs.Scope.ID** (nested in struct):
   - Extract Scope struct column
   - Reindex the ID field within the struct
   - Update ScopeAttrs.PARENT_ID to match

3. **Logs.ID** (optional):
   - Only present for logs with exclusive attributes
   - Must be nullable (NULL when no log attributes)
   - Update LogAttrs.PARENT_ID to match

4. **Cross-batch ID tracking**:
   - Maintain `starting_ids` across batches
   - Ensure each batch's IDs start where previous batch ended
   - Required for proper streaming/incremental batching

**Reference implementation**: See `reindex()` function in `groups_old.rs` lines 921-1011.

### 4. Advanced Features (Future Work)

**Not in MVP scope**:
- Dictionary type optimization (Dict8 vs Dict16 vs native)
- Configurable sort policies (TraceID, multi-dimensional)
- Sort order metadata tracking (`SORT_COLUMNS` schema metadata)
- Metrics batching (multi-level hierarchies, multiple primary tables)
- Traces batching (parent-child span relationships)
- Batch size splitting by criteria other than row count (distinct values, time buckets)

## Testing Status

**Current state**: No tests written yet.

**Required tests**:
1. **Basic batching**: Single input → single output with correct size
2. **Concatenation**: Multiple small inputs → single full output
3. **Splitting**: Single large input → multiple full outputs + residual
4. **Schema unification**: Mixed schemas → unified output
5. **Sorting**: Verify output is sorted by Resource.ID, Scope.ID
6. **ID integrity**: Verify IDs are sequential starting from 0

**Test data needed**:
- Sample Logs batches with varying sizes
- Batches with optional columns present/absent
- Batches with Resource and Scope attributes
- Batches with nullable Logs.ID (some logs with/without LogAttrs)

**Test utilities needed**:
- Helper to create synthetic Logs batches
- Helper to verify ID/PARENT_ID relationships
- Helper to verify sort order
- Helper to check schema consistency

## Known Limitations

### 1. Incomplete Split Logic

**Issue**: `split_logs_to_size()` only slices the Logs table, not the attribute tables.

**Impact**: 
- Output batches have Logs data but may be missing corresponding attributes
- Breaks the PARENT_ID → ID relationships
- Cannot be used in production until fixed

**Priority**: HIGH - must be fixed before any real usage

### 2. Simplified Reindexing

**Issue**: Only reindexes Logs.ID, not Resource.ID, Scope.ID, or PARENT_ID columns.

**Impact**:
- ID values may not be sequential
- PARENT_ID references may be invalid
- Cannot use batched data until relationships are restored

**Priority**: HIGH - must be fixed before any real usage

### 3. No Deduplication

**Issue**: Resource and Scope attributes are not deduplicated.

**Impact**:
- Duplicate attribute sets consume unnecessary space
- Reduces compression efficiency
- Increases downstream processing cost

**Priority**: MEDIUM - reduces efficiency but doesn't break functionality

### 4. No Dictionary Optimization

**Issue**: Dictionary key types are not optimized based on cardinality.

**Impact**:
- May use Dict16 when Dict8 would suffice
- May use dictionary encoding when native would be more efficient
- Slightly larger batch sizes

**Priority**: LOW - optimization, not correctness

## Compilation Status

✅ **Code compiles successfully** with `cargo check --lib`

All type errors resolved:
- Proper use of `error::BatchingSnafu` for Arrow operation errors
- Correct use of `error::ColumnNotFoundSnafu` for missing columns
- Proper use of `error::ColumnDataTypeMismatchSnafu` for type errors
- No unused imports or variables

## Next Steps (Recommended Order)

### Phase 1: Make It Work
1. **Implement attribute slicing in split** - Extract attribute rows based on PARENT_ID ranges
2. **Implement complete reindexing** - Handle all ID and PARENT_ID columns including nested structs
3. **Write basic tests** - Verify round-trip batching produces valid data
4. **Fix edge cases** - Empty batches, NULL handling, single-row batches

### Phase 2: Make It Correct
5. **Implement attribute deduplication** - Use row format for Resource/Scope attrs
6. **Write comprehensive tests** - Edge cases, large batches, schema variations
7. **Add validation** - Verify ID relationships are maintained
8. **Document assumptions** - NULL handling, sort order, size limits

### Phase 3: Make It Fast
9. **Benchmark current implementation** - Measure performance baseline
10. **Optimize hot paths** - Reduce allocations, improve cache locality
11. **Add dictionary optimization** - Cardinality-based key type selection
12. **Consider parallelization** - Independent batch processing

### Phase 4: Extend to Other Signals
13. **Implement Metrics batching** - Handle multiple primary tables, data point counting
14. **Implement Traces batching** - Handle parent-child relationships
15. **Unify common code** - Extract generic patterns for all signal types

## Code Quality Notes

**Good patterns followed**:
- Const generics for signal-agnostic helpers (`select`, `primary_table`)
- Clear phase separation in main pipeline
- Comprehensive error handling with proper contexts
- Preserved working patterns from original implementation

**Areas for improvement**:
- Add more inline documentation
- Extract magic numbers to named constants
- Add debug assertions for invariants
- Consider builder pattern for batching configuration

## References

- **Design document**: `batching_design.md`
- **Algorithm research**: `docs/ARROW_GROUPING_DEDUPLICATION.md`
- **Schema patterns**: `docs/SCHEMA_UNIFICATION_PATTERNS.md`
- **Original implementation**: `src/otap/groups_old.rs`
- **Comparison analysis**: `docs/BATCHING_COMPARISON.md`

## Summary

The MVP implementation provides a **solid foundation** for logs batching with:
- ✅ Clean public API
- ✅ Schema unification
- ✅ Concatenation and sorting
- ✅ Basic splitting and reindexing
- ✅ Compiles without errors

Critical work remaining:
- ❌ Complete attribute slicing
- ❌ Complete reindexing (all columns, nested structs)
- ❌ Attribute deduplication
- ❌ Comprehensive testing

The implementation follows the design document's goal of starting with a **minimum viable implementation** that can be incrementally enhanced. The architecture supports adding the missing features without major refactoring.
