# Resource Deduplication Algorithm Using Arrow Row Format

## Overview

This document describes an efficient algorithm for deduplicating resource attributes across multiple OTAP batches using Arrow's row format. The key insight is that we can identify unique resources by the **sequence of their (KEY, VALUE) pairs**, independent of the PARENT_ID they were originally associated with.

## Problem Statement

Given multiple input batches, each containing:

- A main data table (Logs/Spans/DataPoints) with PARENT_ID references
- Shared attribute tables (ResourceAttrs, ScopeAttrs) with (PARENT_ID, KEY, VALUE) tuples

We need to:

1. Identify which PARENT_IDs across batches have identical attribute sets
2. Assign each unique attribute set a new global ID
3. Build translation maps to rewrite PARENT_ID references in output batches

## Algorithm: Row Format with Partition-Based Deduplication

### Core Insight

By encoding attributes as rows with **PARENT_ID as the first field**, we can:

1. Sort rows efficiently (O(n log n) on byte comparisons)
2. Use Arrow's `partition()` to group consecutive rows by PARENT_ID
3. Extract KEY+VALUE bytes by skipping the fixed-size PARENT_ID encoding (3 bytes for UInt16)
4. Use the concatenated KEY+VALUE bytes as a deduplication key

### Step-by-Step Algorithm

#### Step 1: Convert to Row Format

```rust
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use std::collections::HashMap;
use std::ops::Range;

// CRITICAL: PARENT_ID must be the first field for O(1) skipping
let converter = RowConverter::new(vec![
    SortField::new(DataType::UInt16),      // PARENT_ID - MUST BE FIRST!
    SortField::new(DataType::Utf8),        // KEY
    SortField::new(DataType::Utf8),        // VALUE
    // Add more value type columns as needed
])?;

// Convert all attribute columns to row format
let rows = converter.convert_columns(&[
    parent_id_col,
    key_col, 
    value_col,
])?;
```

**Why PARENT_ID first?**

- UInt16 has a fixed encoding size: 1 byte (null sentinel) + 2 bytes (value) = 3 bytes
- Skipping to KEY+VALUE is just `&row_bytes[3..]` - O(1) operation
- No variable-length parsing required

#### Step 2: Sort Rows Directly

```rust
// Build index array for sorting
let mut sorted_indices: Vec<usize> = (0..rows.num_rows()).collect();

// Sort indices by row comparison (very efficient - direct byte comparison)
sorted_indices.sort_unstable_by(|&a, &b| {
    rows.row(a).cmp(&rows.row(b))
});
```

**Why sort rows?**

- `Row` implements `Ord` via byte-wise comparison
- Sorting rows is more efficient than sorting Arrow arrays and then converting
- Results in PARENT_ID, KEY, VALUE being naturally ordered
- Enables efficient partitioning in the next step

#### Step 3: Access Rows via Sorted Indices

```rust
// No need to reconstruct! Just iterate through sorted indices
// to access the original row buffer in sorted order.

// The original `rows` buffer remains available
// We access it via sorted_indices when needed
```

**Why this is efficient:**

- **No second row buffer allocation** - we reuse the original `rows`
- **Zero-copy access** - `rows.row(sorted_indices[i])` is just pointer arithmetic
- **Single encoding** - row bytes are computed once and reused for both sorting and deduplication
- We can access original columnar data via `sorted_indices` when needed

#### Step 4: Identify PARENT_ID Groups

```rust
// Instead of using partition(), identify groups by comparing PARENT_ID values
// Since rows are sorted by PARENT_ID first, identical values are consecutive

let parent_id_col = combined_batch.column(0)
    .as_primitive::<arrow_array::types::UInt16Type>();

let mut group_ranges = Vec::new();
let mut group_start = 0;

for i in 1..sorted_indices.len() {
    let prev_idx = sorted_indices[i - 1];
    let curr_idx = sorted_indices[i];
    
    // Check if PARENT_ID changed
    if parent_id_col.value(prev_idx) != parent_id_col.value(curr_idx) {
        group_ranges.push(group_start..i);
        group_start = i;
    }
}
// Don't forget the last group
group_ranges.push(group_start..sorted_indices.len());
```

**What this gives us:**

- Ranges into `sorted_indices` where PARENT_ID is constant
- Each range represents one resource (one set of attributes)
- No need to convert rows back to columnar format

#### Step 5: Build Deduplication Map

```rust
// Fixed encoding size for UInt16 PARENT_ID field
const PARENT_ID_ENCODED_LEN: usize = 3; // 1 byte sentinel + 2 bytes

// Metadata for each unique resource
struct ResourceInfo {
    unique_id: u16,           // New global resource ID
    origin_batch: usize,      // Which input batch this came from
    row_range: Range<usize>,  // Range in sorted_rows
}

let mut resource_dedup: HashMap<Vec<u8>, ResourceInfo> = HashMap::new();
let mut next_unique_id = 0u16;

// Pre-computed: which batch each original row came from
// batch_boundaries[i] = cumulative row count after batch i
let batch_boundaries: Vec<usize> = input_batches
    .iter()
    .scan(0, |acc, batch| {
        *acc += batch.num_rows();
        Some(*acc)
    })
    .collect();

for range in group_ranges {
    // Extract KEY+VALUE bytes for this PARENT_ID group
    // Access the ORIGINAL rows buffer using sorted indices
    let mut composite_key = Vec::new();
    
    for i in range.clone() {
        let original_row_idx = sorted_indices[i];
        let row = rows.row(original_row_idx);  // Access original buffer
        let row_bytes = row.data();            // Zero-copy: returns &[u8]
        
        // Skip PARENT_ID (first 3 bytes), extract KEY+VALUE encoding
        let kv_bytes = &row_bytes[PARENT_ID_ENCODED_LEN..];
        composite_key.extend_from_slice(kv_bytes);
    }
    
    // Determine origin batch via sorted_indices back-mapping
    let original_idx = sorted_indices[range.start];
    let origin_batch = batch_boundaries
        .iter()
        .position(|&boundary| original_idx < boundary)
        .unwrap_or(batch_boundaries.len() - 1);
    
    // Insert or retrieve unique ID for this resource
    resource_dedup.entry(composite_key).or_insert_with(|| {
        let id = next_unique_id;
        next_unique_id += 1;
        ResourceInfo {
            unique_id: id,
            origin_batch,
            row_range: range.clone(),
        }
    });
}
```

**Key points:**

- `composite_key` is the concatenation of all KEY+VALUE row encodings for one PARENT_ID
- Two resources with identical attribute sets will have identical `composite_key` bytes
- `ResourceInfo` tracks metadata needed for downstream processing
- The HashMap automatically deduplicates across batches
- **Single encoding**: Row bytes are computed once and accessed via `sorted_indices`
- **Zero-copy reads**: `rows.row(idx).data()` is just a pointer dereference

#### Step 6: Build Per-Batch Translation Maps

```rust
// For each input batch: old_parent_id -> new_unique_id
let mut batch_translations: Vec<HashMap<u16, u16>> = 
    vec![HashMap::new(); num_input_batches];

for (composite_key, info) in &resource_dedup {
    // Extract original PARENT_ID from the combined batch
    // using the original row index from sorted_indices
    let original_row_idx = sorted_indices[info.row_range.start];
    let parent_id_col = combined_batch
        .column(0)
        .as_primitive::<arrow_array::types::UInt16Type>();
    let old_parent_id = parent_id_col.value(original_row_idx);
    
    // Map old -> new for this batch
    batch_translations[info.origin_batch]
        .insert(old_parent_id, info.unique_id);
}
```

**Purpose:**

- Enables rewriting PARENT_ID references in main data tables
- Per-batch maps handle the case where different batches use the same PARENT_ID value
- Fast O(1) lookup during output batch construction

### Complete Pseudocode Summary

```rust
// === SETUP ===
let converter = RowConverter::new(vec![
    SortField::new(DataType::UInt16),  // PARENT_ID first!
    SortField::new(DataType::Utf8),    // KEY
    SortField::new(DataType::Utf8),    // VALUE
])?;

// === PHASE 1: CONVERT & SORT ===
let rows = converter.convert_columns(&[parent_id_col, key_col, value_col])?;

let mut sorted_indices: Vec<usize> = (0..rows.num_rows()).collect();
sorted_indices.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));

// === PHASE 2: IDENTIFY PARENT_ID GROUPS ===
// Since rows are sorted by PARENT_ID first, identical values are consecutive
let parent_id_col = combined_batch.column(0)
    .as_primitive::<arrow_array::types::UInt16Type>();

let mut group_ranges = Vec::new();
let mut group_start = 0;

for i in 1..sorted_indices.len() {
    let prev_idx = sorted_indices[i - 1];
    let curr_idx = sorted_indices[i];
    if parent_id_col.value(prev_idx) != parent_id_col.value(curr_idx) {
        group_ranges.push(group_start..i);
        group_start = i;
    }
}
group_ranges.push(group_start..sorted_indices.len());

// === PHASE 3: DEDUPLICATE ===
const PARENT_ID_ENCODED_LEN: usize = 3;

struct ResourceInfo {
    unique_id: u16,
    origin_batch: usize,
    row_range: Range<usize>,
}

let mut resource_dedup: HashMap<Vec<u8>, ResourceInfo> = HashMap::new();
let mut next_unique_id = 0u16;

for range in &group_ranges {
    let mut composite_key = Vec::new();
    for i in range.clone() {
        let original_row_idx = sorted_indices[i];
        let kv_bytes = &rows.row(original_row_idx).data()[PARENT_ID_ENCODED_LEN..];
        composite_key.extend_from_slice(kv_bytes);
    }
    
    let original_idx = sorted_indices[range.start];
    let origin_batch = determine_batch(original_idx, &batch_boundaries);
    
    resource_dedup.entry(composite_key).or_insert_with(|| {
        let id = next_unique_id;
        next_unique_id += 1;
        ResourceInfo { unique_id: id, origin_batch, row_range: range.clone() }
    });
}

// === PHASE 4: BUILD TRANSLATION MAPS ===
let mut batch_translations: Vec<HashMap<u16, u16>> = 
    vec![HashMap::new(); num_input_batches];

for (_, info) in &resource_dedup {
    let original_row_idx = sorted_indices[info.row_range.start];
    let old_parent_id = combined_batch.column(0)
        .as_primitive::<UInt16Type>()
        .value(original_row_idx);
    
    batch_translations[info.origin_batch]
        .insert(old_parent_id, info.unique_id);
}
```

## Intermediate State

After running this algorithm, we have the complete intermediate state needed for batching:

### 1. Deduplication Map

```rust
resource_dedup: HashMap<Vec<u8>, ResourceInfo>
```

- **Key**: Concatenated KEY+VALUE row encodings (the "fingerprint" of a resource)
- **Value**: `ResourceInfo` containing:
  - `unique_id`: New global resource ID
  - `origin_batch`: Which input batch this resource came from
  - `row_range`: Where to find the actual attribute rows in `sorted_rows`

### 2. Translation Maps

```rust
batch_translations: Vec<HashMap<u16, u16>>
```

- One HashMap per input batch
- Maps: `old_parent_id -> unique_id`
- Used to rewrite PARENT_ID references when constructing output batches

### 3. Sorted Indices and Original Data

```rust
sorted_indices: Vec<usize>
rows: Rows                    // Original row buffer
combined_batch: RecordBatch   // Original columnar data
```

- `sorted_indices` provides logical sort order without copying data
- Original `rows` buffer remains available for zero-copy access
- Original `combined_batch` provides columnar access when needed
- Can extract KEY, VALUE columns using `sorted_indices[row_range]`

## Usage in Higher-Level Batching Logic

The batching system can now:

### Select Resources for Output Batch

```rust
// Decide which resources go into this output batch
let selected_resources: Vec<u16> = /* selection logic */;

// Extract their attributes from original data via sorted_indices
for resource_id in selected_resources {
    let info = resource_dedup.values()
        .find(|info| info.unique_id == resource_id)
        .unwrap();
    
    // Get the actual attribute values from original batch
    for i in info.row_range.clone() {
        let original_row_idx = sorted_indices[i];
        // Access columns from combined_batch at original_row_idx
        let key = combined_batch.column(1).as_string::<i32>().value(original_row_idx);
        let value = combined_batch.column(2).as_string::<i32>().value(original_row_idx);
        // ...
    }
}
```

### Rewrite PARENT_IDs in Main Data

```rust
// For each log/span/datapoint in input batch i
for (batch_idx, log_batch) in input_log_batches.iter().enumerate() {
    let translation = &batch_translations[batch_idx];
    
    let old_parent_ids = log_batch.column("resource_id")
        .as_primitive::<UInt16Type>();
    
    let new_parent_ids: Vec<u16> = old_parent_ids
        .values()
        .iter()
        .map(|&old_id| translation[&old_id])
        .collect();
    
    // Build output with new PARENT_IDs
}
```

## Performance Characteristics

### Time Complexity

- **Row conversion**: O(n) where n = total attribute rows
- **Sorting indices**: O(n log n) - efficient byte comparison via row format
- **Group identification**: O(n) - single pass to find PARENT_ID boundaries
- **Deduplication**: O(n × k) where k = average attributes per resource
- **Overall**: O(n log n) dominated by sorting

### Space Complexity

- **Rows storage**: O(n) - single copy of row-encoded data
- **Sorted indices**: O(n) - just usize values (8 bytes each)
- **Deduplication map**: O(r × k) where r = unique resources
- **Translation maps**: O(p) where p = unique PARENT_IDs across all batches
- **No sorted row buffer needed** - saves O(n) space compared to naive approach

### Optimization Notes

1. **Fixed-size PARENT_ID field**: O(1) skip operation, no parsing
2. **Direct row sorting**: More efficient than sort-then-convert
3. **Single row encoding**: Bytes computed once, accessed via sorted indices
4. **Zero-copy slicing**: `rows.row(sorted_indices[i]).data()` is pointer arithmetic
5. **No sorted buffer**: Saves O(n) memory by not creating a second Rows buffer
6. **Access original columns**: Via `sorted_indices` when columnar access needed

## Advantages Over Alternative Approaches

### vs. HashMap per PARENT_ID

```rust
// Alternative: group first, then deduplicate
let mut groups: HashMap<u16, Vec<Row>> = HashMap::new();
// Problem: requires HashMap operations during collection
```

**Row-based approach is better:**

- Single sort operation (very fast)
- Partition finds groups via consecutive ranges (O(n))
- Natural ordering of attributes within each group

### vs. Separate KEY+VALUE Conversion

```rust
// Alternative: convert KEY+VALUE separately
let kv_converter = RowConverter::new(vec![key_field, value_field])?;
let kv_rows = kv_converter.convert_columns(&[key_col, value_col])?;
```

**Combined approach is better:**

- Single row conversion (not two)
- Can partition by PARENT_ID directly
- O(1) skip to extract KEY+VALUE portion

### vs. Array-Based Sorting

```rust
// Alternative: sort arrays, then convert to rows
let sorted_batch = lexsort(&[parent_id_col, key_col, value_col])?;
let rows = converter.convert_columns(sorted_batch.columns())?;
```

**Row-first approach is better:**

- Sorting rows is more efficient (direct byte comparison)
- No need to maintain sorted array representation
- Row format is already the comparison key

## Example: Complete Implementation

```rust
use arrow_array::{RecordBatch, UInt16Array, StringArray, ArrayRef};
use arrow_row::{RowConverter, SortField};
use arrow_ord::partition::partition;
use arrow_schema::{Schema, Field, DataType};
use std::collections::HashMap;
use std::sync::Arc;
use std::ops::Range;

struct ResourceInfo {
    unique_id: u16,
    origin_batch: usize,
    row_range: Range<usize>,
}

fn deduplicate_resources(
    input_batches: &[RecordBatch],
) -> Result<(
    HashMap<Vec<u8>, ResourceInfo>,
    Vec<HashMap<u16, u16>>,
), ArrowError> {
    // Concatenate all input batches
    let schema = input_batches[0].schema();
    let combined = concat_batches(&schema, input_batches)?;
    
    // Track batch boundaries
    let batch_boundaries: Vec<usize> = input_batches
        .iter()
        .scan(0, |acc, batch| {
            *acc += batch.num_rows();
            Some(*acc)
        })
        .collect();
    
    // Convert to row format (PARENT_ID first!)
    let converter = RowConverter::new(vec![
        SortField::new(DataType::UInt16),
        SortField::new(DataType::Utf8),
        SortField::new(DataType::Utf8),
    ])?;
    
    let rows = converter.convert_columns(combined.columns())?;
    
    // Sort indices by row comparison
    let mut sorted_indices: Vec<usize> = (0..rows.num_rows()).collect();
    sorted_indices.sort_unstable_by(|&a, &b| rows.row(a).cmp(&rows.row(b)));
    
    // Identify PARENT_ID groups in sorted order
    let parent_id_col = combined.column(0)
        .as_primitive::<arrow_array::types::UInt16Type>();
    
    let mut group_ranges = Vec::new();
    let mut group_start = 0;
    
    for i in 1..sorted_indices.len() {
        let prev_idx = sorted_indices[i - 1];
        let curr_idx = sorted_indices[i];
        if parent_id_col.value(prev_idx) != parent_id_col.value(curr_idx) {
            group_ranges.push(group_start..i);
            group_start = i;
        }
    }
    group_ranges.push(group_start..sorted_indices.len());
    
    // Deduplicate
    const PARENT_ID_ENCODED_LEN: usize = 3;
    let mut resource_dedup: HashMap<Vec<u8>, ResourceInfo> = HashMap::new();
    let mut next_unique_id = 0u16;
    
    for range in &group_ranges {
        let mut composite_key = Vec::new();
        // Access original rows buffer via sorted indices
        for i in range.clone() {
            let original_row_idx = sorted_indices[i];
            let kv_bytes = &rows.row(original_row_idx).data()[PARENT_ID_ENCODED_LEN..];
            composite_key.extend_from_slice(kv_bytes);
        }
        
        let original_idx = sorted_indices[range.start];
        let origin_batch = batch_boundaries
            .iter()
            .position(|&b| original_idx < b)
            .unwrap_or(batch_boundaries.len() - 1);
        
        resource_dedup.entry(composite_key).or_insert_with(|| {
            let id = next_unique_id;
            next_unique_id += 1;
            ResourceInfo {
                unique_id: id,
                origin_batch,
                row_range: range.clone(),
            }
        });
    }
    
    // Build translation maps
    let mut batch_translations: Vec<HashMap<u16, u16>> = 
        vec![HashMap::new(); input_batches.len()];
    
    for (_, info) in &resource_dedup {
        let original_row_idx = sorted_indices[info.row_range.start];
        let old_parent_id = parent_id_col.value(original_row_idx);
        
        batch_translations[info.origin_batch]
            .insert(old_parent_id, info.unique_id);
    }
    
    Ok((resource_dedup, batch_translations))
}
```

## Conclusion

This algorithm efficiently deduplicates resources across batches by:

1. Leveraging Arrow's row format for efficient multi-column comparison
2. Using fixed-size field encoding for O(1) key extraction
3. Sorting indices only - no data copying during sort
4. **Single row encoding** - bytes computed once and reused for sorting and deduplication
5. **Zero-copy access** - using sorted indices to access original buffers
6. Building complete intermediate state for downstream batching

The result is a scalable O(n log n) algorithm with minimal memory overhead (single row encoding) and clean separation between deduplication logic and batching logic.
