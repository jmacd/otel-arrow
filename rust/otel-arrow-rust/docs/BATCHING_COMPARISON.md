# Batching Logic Comparison: Original vs. Designed Approach

Research summary comparing original `groups_old.rs` with new batching design
Josh MacDonald, Oct 31, 2025

## Overview

This document compares the original batching implementation in `groups_old.rs` with the new design approach documented in `batching_design.md` and associated research documents. The goal is to identify useful patterns and code from the original that should be preserved in the new implementation.

## High-Level Architecture Comparison

### Original Approach (`groups_old.rs`)

**Structure**:

```rust
pub enum RecordsGroup {
    Logs(Vec<[Option<RecordBatch>; Logs::COUNT]>),
    Metrics(Vec<[Option<RecordBatch>; Metrics::COUNT]>),
    Traces(Vec<[Option<RecordBatch>; Traces::COUNT]>),
}
```

**Operations**:

1. `split_by_type()` - Separate mixed input into 3 signal-specific groups
2. `split()` - Split oversized batches into smaller pieces
3. `concatenate()` - Merge undersized batches to target size
4. `into_otap_arrow_records()` - Convert back to output format

**Key insight**: Works with **sequences** of complete OTAP payloads, processing them through split/concatenate pipeline.

### New Design Approach

**Key principles** (from `batching_design.md`):

1. Focus on **logs first**, generalize later
2. User passes `Vec<[Option<RecordBatch>; N]>` for J≥1 payloads
3. Return K≥1 payloads where K-1 are full-sized
4. Ensure 0th input is fully consumed (no residual accumulation)
5. Support multiple sorting/deduplication policies
6. Use research findings:
   - Row format for deduplication (`ARROW_GROUPING_DEDUPLICATION.md`)
   - Schema unification patterns (`SCHEMA_UNIFICATION_PATTERNS.md`)
   - Proper sort order tracking (`SORT_ORDER_ENCODING.md`)

**Alignment**: ✅ Both work with arrays of `Option<RecordBatch>`, original shows this is viable

## What to Keep from Original

### 1. ✅ Generic Array-Based Design Pattern

**Original**:

```rust
fn generic_split<const N: usize>(
    batches: Vec<[Option<RecordBatch>; N]>,
    max_output_batch: NonZeroU64,
    allowed_payloads: &[ArrowPayloadType],
    primary_payload: ArrowPayloadType,
) -> Result<Vec<[Option<RecordBatch>; N]>>

fn generic_concatenate<const N: usize>(
    batches: Vec<[Option<RecordBatch>; N]>,
    allowed_payloads: &[ArrowPayloadType],
    max_output_batch: Option<NonZeroU64>,
) -> Result<Vec<[Option<RecordBatch>; N]>>
```

**Why keep**: 

- Const generics `<const N: usize>` allow single implementation for all signal types

- `allowed_payloads` parameter provides flexibility

- Clean separation between signal-specific and generic logic

**New design usage**: Adopt this pattern but enhance with:

- Explicit sorting policy parameters

- Deduplication options for shared tables (Resource/Scope)

- Better error handling with specific error types

### 2. ✅ Helper: `select()` - Transpose View

**Original** (lines 908-913):

```rust
/// This is basically a transpose view that lets us look at a sequence of the `i`-th table given a
/// sequence of `RecordBatch` arrays.
fn select<const N: usize>(
    batches: &[[Option<RecordBatch>; N]],
    i: usize,
) -> impl Iterator<Item = &RecordBatch> {
    batches.iter().flat_map(move |batches| batches[i].as_ref())
}
```

**Why keep**: 

- Elegant solution for accessing same payload type across multiple batches

- Zero-allocation iterator

- Used throughout for processing specific payload types

**New design usage**: Keep as-is, very useful utility

### 3. ✅ Helper: `primary_table()` - Find Main Table

**Original** (lines 209-220):

```rust
fn primary_table<const N: usize>(batches: &[Option<RecordBatch>; N]) -> Option<&RecordBatch> {
    match N {
        Logs::COUNT => batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]].as_ref(),
        Metrics::COUNT => batches[POSITION_LOOKUP[ArrowPayloadType::UnivariateMetrics as usize]].as_ref(),
        Traces::COUNT => batches[POSITION_LOOKUP[ArrowPayloadType::Spans as usize]].as_ref(),
        _ => unreachable!()
    }
}
```

**Why keep**: 

- Centralized logic for finding primary table

- Used for batch size measurement

**New design improvement**: Make signal-type-agnostic:

```rust
fn primary_table<const N: usize>(
    batches: &[Option<RecordBatch>; N],
    primary_payload: ArrowPayloadType,
) -> Option<&RecordBatch> {
    batches[POSITION_LOOKUP[primary_payload as usize]].as_ref()
}
```

### 4. ✅ Helper: `batch_length()` - Measure Batch Size

**Used implicitly via `batch_length(batches)` throughout**

**Why keep**: 

- Consistent batch size measurement

- Handles signal-specific counting (for metrics, counts data points not metrics)

**New design note**: From design doc, we measure size in primary table items (logs/spans/data points)

### 5. ✅ Schema Unification: `unify()` Function

**Original** (lines 1167-1296, documented in `SCHEMA_UNIFICATION_PATTERNS.md`):

```rust
fn unify<const N: usize>(batches: &mut [[Option<RecordBatch>; N]]) -> Result<()>
```

**Three-phase process**:

1. Discovery: Find all dictionary and struct fields
2. Analysis: Determine optimal dictionary key types
3. Application: Apply unified types and add missing fields

**Why keep**: 

- Handles complex schema variations (missing optional columns)

- Dictionary type optimization (Dict8 vs Dict16 vs native)

- Struct field unification (Resource/Scope)

- Well-tested, comprehensive solution

**New design usage**: Use as-is or simplify for MVP:

- Initial implementation: Skip dictionary optimization, always use Dict16

- Later enhancement: Restore full dictionary type selection

### 6. ✅ Reindexing Logic

**Original** (lines 923-1003):

```rust
fn reindex<const N: usize>(
    batches: &mut [[Option<RecordBatch>; N]],
    allowed_payloads: &[ArrowPayloadType],
) -> Result<()>
```

**Key features**:


- Recursively reindexes parent-child relationships

- Maintains `starting_ids` array to track next available ID per table

- Handles both `ID` and `PARENT_ID` columns

- Sorts by ID before reindexing (ensures input assumptions)

**Why keep**: 

- Correctly handles hierarchical PARENT_ID → ID relationships

- Accounts for nullable IDs (nulls sort first)

- Generic across signal types

**New design usage**: This is essential logic, keep core algorithm but:

- Add deduplication before reindexing (for Resource/Scope)

- Update PARENT_ID references in primary tables after deduplication

### 7. ✅ ID Column Abstraction: `IDColumn` and `IDSeqs`

**Original** (lines 1006-1144):

```rust
enum IDColumn<'rb> {
    U16(&'rb PrimitiveArray<UInt16Type>),
    U32(&'rb PrimitiveArray<UInt32Type>),
}

enum IDSeqs {
    RangeU16(Vec<Option<RangeInclusive<u16>>>),
    RangeU32(Vec<Option<RangeInclusive<u32>>>),
}
```

**Why keep**: 

- Abstracts U16 vs U32 ID handling

- `IDSeqs` represents ID ranges for split operations

- Handles nullable IDs correctly

- Generic implementations avoid code duplication

**New design usage**: Keep these abstractions, they're clean and well-designed

### 8. ⚠️ Sorting Logic - Partially Keep

**Original** (lines 762-822):

```rust
enum HowToSort {
    SortByParentIdAndId,
    SortById,
}

fn sort_record_batch(rb: RecordBatch, how: HowToSort) -> Result<RecordBatch>
```

**What to keep**:


- Detection of already-sorted data (avoids unnecessary work)

- Use of `sort_to_indices()` which uses row format for multi-column sorts

- Handling of nullable columns (nulls_first: true)

**What to change** (per new design):


- Make sorting policy configurable by user

- Support multiple sort dimensions: TraceID, Resource.ID, Scope.ID, etc.

- Add sort order metadata to output (per `SORT_ORDER_ENCODING.md`)

- Consider deduplication-friendly sorting (type → key → value → parent_id for attributes)

**New design enhancement**:

```rust
pub enum SortPolicy {
    ByTraceId,
    ByResourceId,
    ByScopeId,
    MultiDimension(Vec<&'static str>),  // Column names
    NoSort,
}

fn sort_with_policy(
    rb: RecordBatch,
    policy: &SortPolicy,
) -> Result<RecordBatch>
```

## What NOT to Keep from Original

### 1. ❌ Split Logic - Too Complex

**Original**: Lines 225-400+, with special handling for metrics

**Problems**:


- Complex metric-specific batch counting (lines 648-755)

- Splits without considering deduplication opportunities

- No sorting policy integration

- FIXME comment at line 615: "calculation is broken for logs & traces"

**New design replacement**:


- Concatenate all inputs first

- Deduplicate shared tables (Resource/Scope)

- Sort by chosen policy

- Split to target size

- Simpler, more predictable logic

### 2. ❌ `RecordsGroup` Enum - Too Rigid

**Original**:

```rust
pub enum RecordsGroup {
    Logs(Vec<[Option<RecordBatch>; Logs::COUNT]>),
    Metrics(Vec<[Option<RecordBatch>; Metrics::COUNT]>),
    Traces(Vec<[Option<RecordBatch>; Traces::COUNT]>),
}
```

**Problems**:


- Forces all inputs to be same signal type

- `split_by_type()` is extra step

- Difficult to extend

**New design**: Work directly with `Vec<[Option<RecordBatch>; N]>`, determine signal from first batch

### 3. ❌ No Deduplication

**Original**: No resource or scope attribute deduplication

**New design adds** (per `ARROW_GROUPING_DEDUPLICATION.md`):


- Row format for attribute comparison

- HashMap-based deduplication

- Translation maps for PARENT_ID rewriting

### 4. ❌ No Sort Order Metadata

**Original**: Sorts but doesn't track/preserve metadata

**New design adds** (per `SORT_ORDER_ENCODING.md`):


- Read/write `SORT_COLUMNS` schema metadata

- Track encoding metadata (delta, plain, quasidelta)

- Verify sort assumptions before operations

### 5. ❌ Generic Concatenate Uses `BatchCoalescer`

**Original** (lines 877-892):

```rust
let mut batcher = arrow::compute::BatchCoalescer::new(schema.clone(), num_rows);
for row in batches.iter_mut() {
    if let Some(rb) = row[i].take() {
        batcher.push_batch(rb.with_schema(schema.clone())?)?;
    }
}
```

**Problem**: `BatchCoalescer` is for incremental building, not bulk concatenation

**New design**: Use `arrow_select::concat::concat_batches()` directly (simpler, documented in research)

## Recommended New Implementation Structure

Based on analysis, here's the recommended structure:

```rust
// High-level API (keep similar to original)
pub struct BatchConfig {
    pub max_batch_size: NonZeroUsize,
    pub sort_policy: SortPolicy,
    pub deduplicate_resources: bool,
    pub deduplicate_scopes: bool,
}

pub fn batch_logs(
    inputs: Vec<[Option<RecordBatch>; Logs::COUNT]>,
    config: &BatchConfig,
) -> Result<Vec<[Option<RecordBatch>; Logs::COUNT]>> {
    // Phase 1: Unify schemas (keep from original)
    let mut inputs = inputs;
    unify_logs(&mut inputs)?;
  
    // Phase 2: Concatenate all inputs
    let combined = concatenate_logs(&mut inputs)?;
  
    // Phase 3: Deduplicate shared tables (NEW)
    let (combined, mappings) = deduplicate_logs(combined, config)?;
  
    // Phase 4: Sort by policy (ENHANCED from original)
    let sorted = sort_logs_with_policy(combined, &config.sort_policy)?;
  
    // Phase 5: Reindex (keep from original, adjusted for deduplication)
    let reindexed = reindex_logs(sorted, &mappings)?;
  
    // Phase 6: Split to target size (SIMPLER than original)
    let outputs = split_to_size(reindexed, config.max_batch_size)?;
  
    Ok(outputs)
}

// Keep these helpers from original
fn select<const N: usize>(...) -> impl Iterator<Item = &RecordBatch>
fn primary_table<const N: usize>(...) -> Option<&RecordBatch>
fn batch_length<const N: usize>(...) -> usize

// Keep/adapt from original
fn unify<const N: usize>(batches: &mut [[Option<RecordBatch>; N]]) -> Result<()>
fn reindex<const N: usize>(batches: &mut [[Option<RecordBatch>; N]], ...) -> Result<()>

// NEW functions using research
fn deduplicate_resource_attrs(batches: Vec<RecordBatch>) -> Result<(RecordBatch, Vec<HashMap<u16, u16>>)>
fn deduplicate_scope_attrs(batches: Vec<RecordBatch>) -> Result<(RecordBatch, Vec<HashMap<u16, u16>>)>

// ENHANCED from original
fn sort_with_policy(rb: RecordBatch, policy: &SortPolicy) -> Result<RecordBatch>
fn split_to_size<const N: usize>(batch: [Option<RecordBatch>; N], size: usize) -> Result<Vec<[Option<RecordBatch>; N]>>
```

## Implementation Priorities

### Phase 1: Core Batching (Logs Only)

**Keep from original**:


- ✅ Generic array handling `<const N: usize>`

- ✅ `select()` helper

- ✅ `unify()` function (simplified: skip dict optimization)

- ✅ `reindex()` logic with `IDColumn`/`IDSeqs` abstractions

- ✅ Basic sort detection (already sorted check)

**Add new**:


- ✅ Resource/Scope deduplication using row format

- ✅ Configurable sort policies

- ✅ Sort order metadata tracking

- ✅ Simple split logic (no complex metrics handling yet)

### Phase 2: Enhancement

**Restore from original**:


- Full dictionary type optimization in `unify()`

- Better performance tuning

**Add new**:


- Additional sort policies (multi-dimension)

- Compression-aware sorting (quasi-delta for attributes)

### Phase 3: Extend to Traces/Metrics

**Adapt from original**:


- Multi-level hierarchies (Traces/Metrics)

- Metrics data point counting logic

## Key Takeaways

### Good Patterns from Original

1. **Const generic design**: `<const N: usize>` with array of `Option<RecordBatch>`
2. **Schema unification**: Three-phase discovery-analysis-application
3. **Reindexing algorithm**: Recursive parent-child handling
4. **ID abstractions**: `IDColumn`/`IDSeqs` enum pattern
5. **Helper functions**: `select()`, `primary_table()`, `batch_length()`
6. **Nullable ID handling**: Nulls-first sorting assumption

### Original Limitations Addressed by New Design

1. **No deduplication**: Now using row format for Resource/Scope attrs
2. **Limited sorting**: Now configurable policies for different use cases
3. **No metadata**: Now tracking sort order and encoding
4. **Complex split logic**: Simplified by concatenate-first approach
5. **Rigid structure**: More flexible configuration

### Recommended Approach

1. Start with **simplified version** of original patterns
2. Add **new capabilities** from research (deduplication, sort policies)
3. Keep **well-tested abstractions** (`unify`, `reindex`, ID handling)
4. **Enhance incrementally**: Basic → dictionary optimization → multi-signal

The original code provides solid foundations for array handling, schema unification, and reindexing. The new design adds deduplication, configurable sorting, and metadata tracking on top of these foundations.
