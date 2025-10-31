# Arrow-RS Tools for Hash-Based Grouping and Deduplication

Research summary for OTAP batching project
Josh MacDonald, Oct 31, 2025

## Overview

This document summarizes the Arrow-RS tools and techniques relevant to deduplicating and aggregating OTAP attribute tables across multiple RecordBatch inputs. Our goal is to identify PARENT_IDs with identical key-value sets and build efficient mappings for reindexing.

## Primary Tool: Row Format Conversion (`arrow-row`)

The `arrow-row` crate provides the core functionality for hash-based grouping and deduplication through the `RowConverter` API.

### Key Concepts

**RowConverter** converts column-oriented Arrow arrays into normalized, comparable row-oriented byte sequences. These byte sequences:

- Are optimized for `memcmp` comparison
- Can be efficiently hashed
- Support all Arrow data types including dictionaries
- Are bidirectionally convertible (columns ↔ rows)

### Location in Codebase

- Main implementation: `arrow-rs/arrow-row/src/lib.rs`
- Documentation includes detailed examples (lines 1-150)

### Core API

```rust
use arrow_row::{RowConverter, SortField, Rows, Row, OwnedRow};
use std::collections::{HashMap, HashSet};

// 1. Create a RowConverter matching your schema
let fields = vec![
    SortField::new(DataType::UInt16),           // PARENT_ID
    SortField::new(DataType::Dictionary(        // KEY (dictionary)
        Box::new(DataType::Int16),
        Box::new(DataType::Utf8)
    )),
    SortField::new(DataType::Int64),            // Optional<int>
    SortField::new(DataType::Float64),          // Option<f64>
    SortField::new(DataType::Dictionary(        // Optional<String>
        Box::new(DataType::Int16),
        Box::new(DataType::Utf8)
    )),
];
let converter = RowConverter::new(fields)?;

// 2. Convert columns to row format
let rows: Rows = converter.convert_columns(&columns)?;

// 3. Iterate over rows for deduplication
for row in rows.iter() {
    // Row implements Copy, cheap to pass around
    // Use row.owned() to get OwnedRow for HashMap keys
}

// 4. Convert back to columns
let arrays: Vec<ArrayRef> = converter.convert_rows(selected_rows)?;
```

### Important Characteristics

1. **Dictionary Flattening**: Dictionary arrays are automatically "hydrated" during conversion
   - `Dictionary<Int8, Utf8>` becomes comparable `Utf8` values in row format
   - Conversion back produces non-dictionary arrays
   - This is ideal for deduplication since we compare actual values

2. **Zero-Copy Iteration**: `Rows::iter()` provides lightweight `Row<'a>` references
   - `Row` is `Copy` and contains only a byte slice reference
   - Very efficient for scanning and comparison

3. **Owned Rows**: `OwnedRow` implements `Hash + Eq`
   - Can be used as HashMap/HashSet keys
   - Created via `row.owned()`
   - See example at lines 860-875 in `arrow-row/src/lib.rs`

## Deduplication Architecture for OTAP Attributes

### Recommended Approach

For your use case with multiple input batches and varying PARENT_ID cardinalities:

```rust
// Phase 1: Concatenate all input attribute batches
use arrow_select::concat::concat_batches;

let combined_batch = concat_batches(&schema, input_batches.iter())?;

// Phase 2: Convert to row format
let converter = RowConverter::new(sort_fields)?;
let rows = converter.convert_columns(combined_batch.columns())?;

// Phase 3: Group by PARENT_ID and deduplicate attributes
// Track which input batch each row came from
let mut batch_boundaries = vec![0];
for batch in &input_batches {
    batch_boundaries.push(batch_boundaries.last().unwrap() + batch.num_rows());
}

// Build deduplication map: (PARENT_ID, attribute_row) -> new_parent_id
struct AttributeKey {
    parent_id: u16,
    attr_row: OwnedRow,
}

let mut dedup_map: HashMap<AttributeKey, u16> = HashMap::new();
let mut next_output_id = 0u16;

for (global_idx, row) in rows.iter().enumerate() {
    let parent_id = get_parent_id_at(combined_batch, global_idx);
    let key = AttributeKey {
        parent_id,
        attr_row: row.owned(),
    };
    
    dedup_map.entry(key).or_insert_with(|| {
        let id = next_output_id;
        next_output_id += 1;
        id
    });
}

// Phase 4: Build per-batch mapping vectors
let num_batches = input_batches.len();
let mut mappings: Vec<Vec<u16>> = Vec::with_capacity(num_batches);

for batch_idx in 0..num_batches {
    let start = batch_boundaries[batch_idx];
    let end = batch_boundaries[batch_idx + 1];
    
    let mut batch_mapping = Vec::with_capacity(end - start);
    for global_idx in start..end {
        let row = unsafe { rows.row_unchecked(global_idx) };
        let parent_id = get_parent_id_at(combined_batch, global_idx);
        let key = AttributeKey { parent_id, attr_row: row.owned() };
        
        let new_id = dedup_map[&key];
        batch_mapping.push(new_id);
    }
    
    mappings.push(batch_mapping);
}
```

### Alternative: Group-First Approach

For better cache locality when PARENT_IDs have varying cardinalities:

```rust
// After combining batches, partition by PARENT_ID first
use arrow_ord::partition::partition;
use arrow_ord::sort::{lexsort, SortColumn};

// Sort by PARENT_ID
let sorted_indices = lexsort(&[
    SortColumn {
        values: parent_id_column.clone(),
        options: Some(SortOptions::default()),
    }
], None)?;

// Apply sort to get grouped data
let sorted_batch = take_record_batch(&combined_batch, &sorted_indices)?;

// Identify PARENT_ID boundaries
let partitions = partition(&[sorted_batch.column(parent_id_col_idx)])?;

// Process each PARENT_ID group separately
for range in partitions.ranges() {
    // All rows in range have same PARENT_ID
    // Extract attribute rows for this group
    let group_rows = /* slice rows for this range */;
    
    // Deduplicate within this PARENT_ID
    let mut seen: HashSet<OwnedRow> = HashSet::new();
    for row in group_rows.iter() {
        if seen.insert(row.owned()) {
            // First time seeing this attribute set
            assign_new_output_id();
        }
    }
}
```

## Supporting Tools from Arrow-RS

### 1. Batch Concatenation (`arrow-select::concat`)

**Location**: `arrow-rs/arrow-select/src/concat.rs`

```rust
use arrow_select::concat::concat_batches;

// Efficiently merge multiple RecordBatches with same schema
let combined = concat_batches(&schema, input_batches.iter())?;
```

**Characteristics**:

- Handles all Arrow data types
- Efficient for large numbers of batches
- Maintains data type consistency

### 2. Take Operations (`arrow-select::take`)

**Location**: `arrow-rs/arrow-select/src/take.rs`

```rust
use arrow_select::take::{take, take_record_batch};

// Select rows by indices - efficient for remapping
let indices = UInt32Array::from(vec![0, 2, 5, 7]);
let selected = take_record_batch(&batch, &indices)?;
```

**Use cases**:

- Apply deduplication results
- Reorder rows based on mappings
- Extract specific PARENT_ID groups

### 3. Interleave (`arrow-select::interleave`)

**Location**: `arrow-rs/arrow-select/src/interleave.rs`

```rust
use arrow_select::interleave::interleave;

// Merge rows from multiple arrays using (array_idx, row_idx) pairs
let indices = vec![
    (0, 0),  // First element from array 0
    (1, 2),  // Third element from array 1
    (0, 3),  // Fourth element from array 0
];
let merged = interleave(&arrays, &indices)?;
```

**Use cases**:

- Complex batch interleaving after deduplication
- Building output batches from multiple sources

### 4. Sorting (`arrow-ord::sort`)

**Location**: `arrow-rs/arrow-ord/src/sort.rs`

```rust
use arrow_ord::sort::{lexsort, lexsort_to_indices, SortColumn, SortOptions};

// Multi-column lexicographic sort
let sorted_columns = lexsort(&[
    SortColumn {
        values: resource_id.clone(),
        options: Some(SortOptions::default()),
    },
    SortColumn {
        values: scope_id.clone(),
        options: Some(SortOptions::default()),
    },
], None)?;

// Or get indices for later application
let indices = lexsort_to_indices(&sort_columns, None)?;
```

**Note**: For multi-column sorts, the documentation suggests that using `RowConverter` + standard sorting may be faster than `lexsort`.

### 5. Partitioning (`arrow-ord::partition`)

**Location**: `arrow-rs/arrow-ord/src/partition.rs`

```rust
use arrow_ord::partition::partition;

// Find ranges of consecutive equal values (requires sorted input)
let partitions = partition(&[parent_id_column])?;

for range in partitions.ranges() {
    // All rows in range.start..range.end have same PARENT_ID
    process_group(range);
}
```

**Characteristics**:

- Input must be sorted by the partition columns
- Returns contiguous ranges
- Very efficient for grouping operations

## Performance Considerations

### When to Use Row Format

**Best for**:

- Multi-column comparisons (your attribute deduplication use case)
- Hash-based operations (HashMap, HashSet)
- Custom sorting with multiple dimensions
- Group-by operations

**Row format advantages**:

- Single `memcmp` comparison for all columns
- Efficient hashing of composite keys
- No need to iterate over multiple arrays

### Memory Efficiency

**Row buffer size**:

- `Rows` has a 2GiB limit if using `try_into_binary()`
- For larger datasets, process in chunks
- Consider using `RowConverter::empty_rows()` to pre-allocate

**Mapping storage**:

```rust
// Dense mappings (PARENT_IDs are sequential, low values)
let mut mappings: Vec<Option<u16>> = vec![None; max_parent_id + 1];

// Sparse mappings (PARENT_IDs are high values or non-sequential)
let mut mappings: HashMap<u16, u16> = HashMap::new();
```

### Optimization Tips

1. **Batch size**: Combine multiple small batches before converting to row format
2. **Avoid redundant conversions**: Convert once, reuse the `Rows` structure
3. **Use unsafe accessors**: `rows.row_unchecked()` when bounds are known
4. **Pre-allocate**: Use `empty_rows()` with estimated capacity
5. **Consider sorting first**: Partitioning by PARENT_ID improves cache locality

## Complete Example: Attribute Deduplication

```rust
use arrow_array::{RecordBatch, ArrayRef, UInt16Array};
use arrow_row::{RowConverter, SortField, OwnedRow};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use std::collections::HashMap;

pub struct AttributeDeduplicator {
    converter: RowConverter,
    schema: SchemaRef,
}

impl AttributeDeduplicator {
    pub fn new(schema: SchemaRef) -> Result<Self, ArrowError> {
        // Build sort fields from schema
        let sort_fields: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        
        let converter = RowConverter::new(sort_fields)?;
        Ok(Self { converter, schema })
    }
    
    pub fn deduplicate(
        &self,
        input_batches: Vec<RecordBatch>,
    ) -> Result<(RecordBatch, Vec<Vec<u16>>), ArrowError> {
        // Combine all input batches
        let combined = concat_batches(&self.schema, input_batches.iter())?;
        
        // Convert to row format
        let rows = self.converter.convert_columns(combined.columns())?;
        
        // Track boundaries for per-batch mappings
        let mut boundaries = vec![0];
        for batch in &input_batches {
            boundaries.push(boundaries.last().unwrap() + batch.num_rows());
        }
        
        // Deduplicate and build output
        let mut dedup_map: HashMap<OwnedRow, u16> = HashMap::new();
        let mut output_indices = Vec::new();
        let mut next_id = 0u16;
        
        for (idx, row) in rows.iter().enumerate() {
            let owned = row.owned();
            let output_id = *dedup_map.entry(owned).or_insert_with(|| {
                output_indices.push(idx as u32);
                let id = next_id;
                next_id += 1;
                id
            });
        }
        
        // Build deduplicated output batch
        let output_indices_array = UInt32Array::from(output_indices);
        let output_batch = take_record_batch(&combined, &output_indices_array)?;
        
        // Build per-batch mappings
        let mappings: Vec<Vec<u16>> = (0..input_batches.len())
            .map(|batch_idx| {
                let start = boundaries[batch_idx];
                let end = boundaries[batch_idx + 1];
                (start..end)
                    .map(|idx| {
                        let row = unsafe { rows.row_unchecked(idx) };
                        dedup_map[&row.owned()]
                    })
                    .collect()
            })
            .collect();
        
        Ok((output_batch, mappings))
    }
}
```

## Integration with OTAP Batching

### Scope and Resource Attribute Deduplication

For your OTAP batching, you'll apply this pattern to both ScopeAttrs and ResourceAttrs tables:

```rust
// 1. Deduplicate resource attributes across all inputs
let (dedup_resources, resource_mappings) = 
    deduplicate_attributes(&resource_attr_batches)?;

// 2. Deduplicate scope attributes
let (dedup_scopes, scope_mappings) = 
    deduplicate_attributes(&scope_attr_batches)?;

// 3. Update Logs table Resource.ID and Scope.ID columns
for (batch_idx, logs_batch) in logs_batches.iter_mut().enumerate() {
    let resource_ids = logs_batch.column(resource_id_col);
    let new_resource_ids = remap_ids(resource_ids, &resource_mappings[batch_idx]);
    
    let scope_ids = logs_batch.column(scope_id_col);
    let new_scope_ids = remap_ids(scope_ids, &scope_mappings[batch_idx]);
    
    // Rebuild batch with updated IDs
}
```

### Sorting Integration

After deduplication, sort the Logs table by one or more dimensions:

```rust
// Option 1: Sort by Resource.ID to group logs by resource
let sorted = lexsort(&[
    SortColumn {
        values: logs_batch.column(resource_id_col).clone(),
        options: Some(SortOptions::default()),
    },
    SortColumn {
        values: logs_batch.column(timestamp_col).clone(),
        options: Some(SortOptions::default()),
    },
], None)?;

// Option 2: Use row format for custom multi-dimensional sort
let log_converter = RowConverter::new(vec![
    SortField::new(logs_batch.column(trace_id_col).data_type().clone()),
    SortField::new(logs_batch.column(resource_id_col).data_type().clone()),
])?;

let log_rows = log_converter.convert_columns(&[
    logs_batch.column(trace_id_col).clone(),
    logs_batch.column(resource_id_col).clone(),
])?;

let mut indices: Vec<_> = (0..log_rows.num_rows()).collect();
indices.sort_unstable_by(|&a, &b| {
    let row_a = unsafe { log_rows.row_unchecked(a) };
    let row_b = unsafe { log_rows.row_unchecked(b) };
    row_a.cmp(&row_b)
});
```

## Additional Resources

### Documentation References

1. **Row Format Blog Post**: The arrow-row documentation references a detailed blog post:
   <https://arrow.apache.org/blog/2022/11/07/multi-column-sorts-in-arrow-rust-part-1/>

2. **Arrow-RS Examples**: See `arrow-row/src/lib.rs` lines 50-120 for basic examples

3. **Deduplication Example**: Lines 860-875 in `arrow-row/src/lib.rs`

### Related Arrow Crates

- `arrow-row`: Row format conversion (core for your use case)
- `arrow-select`: Take, filter, concat, interleave operations
- `arrow-ord`: Sorting, partitioning, comparison operations
- `arrow-array`: Array types and builders

## Summary

For your OTAP attribute deduplication needs:

1. **Use `RowConverter`** as the primary tool for comparing multi-column attribute rows
2. **Leverage `concat_batches`** to combine inputs before processing
3. **Use `HashMap<OwnedRow, u16>`** for deduplication mapping
4. **Apply `partition`** if grouping by PARENT_ID first improves efficiency
5. **Use `take_record_batch`** to build final deduplicated outputs
6. **Store mappings per input batch** as `Vec<Vec<u16>>` or `Vec<HashMap<u16, u16>>`

The row format is specifically designed for this type of multi-column comparison and grouping operation, making it ideal for your attribute deduplication requirements.
