# Sort Order Encoding in OTAP

Research summary for understanding how sort order information is tracked in otel-arrow-rust
Josh MacDonald, Oct 31, 2025

## Overview

This document describes how the otel-arrow-rust codebase encodes and tracks information about sort order in OTAP (OpenTelemetry Arrow Protocol) record batches. Understanding sort order is critical for batching operations because:

1. Delta encoding requires sorted data
2. Quasi-delta encoding (for attributes) depends on sort order
3. Compression effectiveness improves with proper sorting
4. Deduplication and grouping benefit from sorted inputs

## Schema Metadata for Sort Order

### Primary Constant

**Location**: `src/schema/consts.rs` (line 78)

```rust
pub mod metadata {
    /// schema metadata for which columns the record batch is sorted by
    pub const SORT_COLUMNS: &str = "sort_columns";
    
    // ... other metadata constants
}
```

This constant defines the key used in Arrow schema metadata to indicate which columns a RecordBatch is sorted by.

### Usage Pattern

Sort order information is stored as **schema-level metadata** (not field-level metadata). This means it's attached to the `RecordBatch`'s schema using Arrow's built-in metadata capabilities.

**Expected format** (based on constant name and Arrow conventions):

- Key: `"sort_columns"`
- Value: A string listing the column names, likely comma-separated or as a JSON array

## Column Encoding Metadata

### Field-Level Encoding Indicators

**Location**: `src/schema/consts.rs` (lines 80-95)

```rust
pub mod metadata {
    /// field metadata key for the encoding of some column
    pub const COLUMN_ENCODING: &str = "encoding";

    pub mod encodings {
        /// delta encoding
        pub const DELTA: &str = "delta";

        /// plain encoding - e.g. the values in the array are not encoded
        pub const PLAIN: &str = "plain";

        /// quasi-delta encoding - in this encoding scheme subsequent runs of matching columns
        /// will have the parent_id field delta encoded.
        pub const QUASI_DELTA: &str = "quasidelta";
    }
}
```

### Encoding Types and Sort Requirements

1. **DELTA** (`"delta"`):
   - Requires data to be sorted by the encoded column
   - Used for ID columns that are already in sorted order
   - Example: `id` column sorted sequentially

2. **PLAIN** (`"plain"`):
   - No encoding applied
   - Values are stored as-is
   - Indicates encoding has been removed/materialized

3. **QUASI_DELTA** (`"quasidelta"`):
   - Transport-optimized encoding for `parent_id` columns in attribute tables
   - Requires sorting by: attribute type, then key, then value
   - Delta encodes parent IDs when subsequent rows have matching type/key/value

## Helper Functions for Metadata Access

### Schema Metadata Functions

**Location**: `src/schema.rs`

```rust
/// Get the value of the schema metadata for a given key.
pub fn get_schema_metadata<'a>(schema: &'a Schema, key: &'a str) -> Option<&'a str>

/// Returns a new record batch with the new key/value updated in the schema metadata.
pub fn update_schema_metadata(
    record_batch: &RecordBatch,
    key: String,
    value: String,
) -> RecordBatch
```

### Field Metadata Functions

**Location**: `src/schema.rs`

```rust
/// Get the value of the field metadata for a given column and key.
pub fn get_field_metadata<'a>(
    schema: &'a Schema,
    column_name: &str,
    key: &'a str,
) -> Option<&'a str>

/// Returns a new record batch with the new key/value updated in the field metadata.
pub fn update_field_metadata(
    schema: &Schema, 
    column_name: &str, 
    key: &str, 
    value: &str
) -> Schema
```

### Encoding Detection Functions

**Location**: `src/schema.rs` (lines 125-150)

```rust
/// Checks the Arrow schema field metadata to determine if the "id" field in this record batch is
/// plain encoded.
pub fn is_id_plain_encoded(record_batch: &RecordBatch) -> bool

/// Checks the Arrow schema field metadata to determine if the "parent_id" field in this record
/// batch is plain encoded.
pub fn is_parent_id_plain_encoded(record_batch: &RecordBatch) -> bool
```

These functions check if encoding metadata is present and equals `"plain"`. If metadata is absent, they assume the batch came from the Golang exporter (which doesn't add metadata but generally uses delta encoding by default).

### FieldExt Trait

**Location**: `src/schema.rs` (lines 153-181)

```rust
pub trait FieldExt {
    /// Sets the encoding field metadata to the value passed as `encoding`
    fn with_encoding(self, encoding: &str) -> Self;

    /// Sets the encoding column metadata key to "plain".
    fn with_plain_encoding(self) -> Self;
}
```

Used when constructing fields to mark their encoding type.

## Sort Column Determination

### Per-Payload-Type Sort Specifications

**Location**: `src/otap/transform/transport_optimize.rs` (lines 350-405)

```rust
const fn get_sort_column_paths(payload_type: &ArrowPayloadType) -> &'static [&'static str] {
    match payload_type {
        // Attribute tables: sorted by type, key, values, then parent_id
        ArrowPayloadType::ResourceAttrs
        | ArrowPayloadType::ScopeAttrs
        | ArrowPayloadType::SpanAttrs
        // ... other attr types
        => &[
            consts::ATTRIBUTE_TYPE,
            consts::ATTRIBUTE_KEY,
            consts::ATTRIBUTE_STR,
            consts::ATTRIBUTE_INT,
            consts::ATTRIBUTE_DOUBLE,
            consts::ATTRIBUTE_BOOL,
            consts::ATTRIBUTE_BYTES,
            consts::PARENT_ID,
        ],
        
        // Data points: sorted by parent_id only
        ArrowPayloadType::SummaryDataPoints
        | ArrowPayloadType::NumberDataPoints
        // ... other datapoint types
        => &[consts::PARENT_ID],
        
        // Exemplars: sorted by value then parent_id
        ArrowPayloadType::NumberDpExemplars
        // ... other exemplar types
        => &[consts::INT_VALUE, consts::DOUBLE_VALUE, consts::PARENT_ID],
        
        // Logs: sorted by resource, scope, trace_id
        ArrowPayloadType::Logs => &[
            RESOURCE_ID_COL_PATH,  // "resource.id"
            SCOPE_ID_COL_PATH,      // "scope.id"
            consts::TRACE_ID
        ],
        
        // Spans: sorted by resource, scope, name, trace_id
        ArrowPayloadType::Spans => &[
            RESOURCE_ID_COL_PATH,
            SCOPE_ID_COL_PATH,
            consts::NAME,
            consts::TRACE_ID,
        ],
        
        // Metrics: sorted by resource, scope, metric type, name
        ArrowPayloadType::UnivariateMetrics 
        | ArrowPayloadType::MultivariateMetrics => &[
            RESOURCE_ID_COL_PATH,
            SCOPE_ID_COL_PATH,
            consts::METRIC_TYPE,
            consts::NAME,
        ],
        
        // ... other payload types
    }
}
```

### Nested Column Paths

Special handling for columns nested within structs:

```rust
/// path within the record batch to the resource ID column
pub const RESOURCE_ID_COL_PATH: &str = "resource.id";

/// path within the record batch to the scope ID column
pub const SCOPE_ID_COL_PATH: &str = "scope.id";
```

These refer to the `id` field within `resource` and `scope` struct columns.

## Sort Implementation

### Sorting Logic

**Location**: `src/otap/transform/transport_optimize.rs` (lines 407-447)

```rust
fn sort_record_batch(
    payload_type: &ArrowPayloadType,
    record_batch: &RecordBatch,
) -> Result<RecordBatch> {
    let sort_columns_paths = get_sort_column_paths(payload_type);

    // choose which columns to sort by -- only sort by the columns that are present
    let schema = record_batch.schema_ref();
    let mut sort_inputs = vec![];
    let columns = record_batch.columns();
    for path in sort_columns_paths {
        if let Some(column) = access_column(path, schema, columns) {
            sort_inputs.push(column)
        }
    }

    if sort_inputs.is_empty() {
        Ok(record_batch.clone())
    } else {
        let sort_columns = sort_inputs
            .iter()
            .map(|array| SortColumn {
                values: array.clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
            .collect::<Vec<_>>();

        let indices = sort_to_indices(&sort_columns)?;

        if indices.values().is_sorted() {
            Ok(record_batch.clone())  // Already sorted
        } else {
            Ok(take_record_batch(record_batch, &indices)?)
        }
    }
}
```

**Key behaviors**:

1. Only sorts by columns that exist in the batch
2. Always uses ascending order (`descending: false`)
3. Nulls sort last (`nulls_first: false`)
4. Returns original batch if already sorted (optimization)

### Multi-Column Sort Helper

**Location**: `src/otap/transform.rs` (lines 1775-1810)

```rust
pub(crate) fn sort_to_indices(sort_columns: &[SortColumn]) -> arrow::error::Result<UInt32Array> {
    if sort_columns.len() == 1 {
        // Use single-column sort for efficiency
        arrow::compute::sort_to_indices(&sort_columns[0].values, sort_columns[0].options, None)
    } else {
        // Use row format for multi-column sorts
        let sort_fields = sort_columns
            .iter()
            .map(|sc| {
                let options = sc.options.unwrap_or_default();
                SortField::new_with_options(sc.values.data_type().clone(), options)
            })
            .collect::<Vec<_>>();

        let converter = RowConverter::new(sort_fields)?;
        let sort_arrays = sort_columns
            .iter()
            .map(|sc| sc.values.clone())
            .collect::<Vec<_>>();
        let rows = converter.convert_columns(&sort_arrays)?;

        let mut indices: Vec<u32> = (0..rows.num_rows() as u32).collect();
        indices.sort_unstable_by(|&a, &b| {
            let row_a = unsafe { rows.row_unchecked(a as usize) };
            let row_b = unsafe { rows.row_unchecked(b as usize) };
            row_a.cmp(&row_b)
        });

        Ok(UInt32Array::from(indices))
    }
}
```

**Performance optimization**: Uses Arrow's row format for multi-column sorts (more efficient than `lexsort` for many columns).

## Cursor-Based Sorted Traversal

### SortedBatchCursor

**Location**: `src/otlp/common.rs`

The codebase uses a `SortedBatchCursor` pattern for efficiently traversing sorted record batches, particularly when walking hierarchical relationships (Resource → Scope → Logs/Spans).

```rust
pub struct SortedBatchCursor {
    sorted_indices: Vec<usize>,
    // ... internal state
}

impl SortedBatchCursor {
    pub fn init_cursor_for_root_batch(
        &mut self,
        record_batch: &RecordBatch,
        cursor: &mut SortedBatchCursor,
    ) -> Result<()> {
        // Determines sort order based on Resource.ID and Scope.ID columns
        // Uses RowConverter for multi-column sorting
        // Falls back to single-column sorting if only one ID present
    }
}
```

**Usage pattern**:

- Logs: sorted by `(resource.id, scope.id)`
- Attributes: sorted by `parent_id` (after type/key/value sorting)
- Events/Links: sorted by `parent_id`

## Integration with Batching

### For Batching Operations

When implementing batching, you should:

1. **Check existing sort order**:

   ```rust
   use crate::schema::get_schema_metadata;
   
   let sort_cols = get_schema_metadata(batch.schema(), consts::metadata::SORT_COLUMNS);
   ```

2. **Preserve or update sort order after batching**:

   ```rust
   use crate::schema::update_schema_metadata;
   
   let new_batch = update_schema_metadata(
       &combined_batch,
       consts::metadata::SORT_COLUMNS.to_string(),
       "resource.id,scope.id,trace_id".to_string(),
   );
   ```

3. **Check encoding before materializing**:

   ```rust
   use crate::schema::{is_id_plain_encoded, is_parent_id_plain_encoded};
   
   if !is_parent_id_plain_encoded(&attr_batch) {
       // Materialize parent_id before grouping
       let materialized = materialize_parent_id_for_attributes::<UInt16Type>(&attr_batch)?;
   }
   ```

### Recommended Metadata Format

Based on the constant name and Arrow conventions, the `SORT_COLUMNS` metadata value should be a comma-separated list of column paths:

```rust
// Example values:
"resource.id,scope.id,trace_id"
"parent_id"
"type,key,str,int,double,bool,bytes,parent_id"
```

For nested columns, use dot notation (`resource.id`, `scope.id`).

## Example: Reading Sort Metadata

```rust
use arrow::array::RecordBatch;
use otel_arrow_rust::schema::{get_schema_metadata, consts};

fn get_sort_columns(batch: &RecordBatch) -> Option<Vec<&str>> {
    get_schema_metadata(batch.schema().as_ref(), consts::metadata::SORT_COLUMNS)
        .map(|s| s.split(',').collect())
}

// Usage:
if let Some(sort_cols) = get_sort_columns(&batch) {
    println!("Batch is sorted by: {:?}", sort_cols);
    // ["resource.id", "scope.id", "trace_id"]
}
```

## Example: Setting Sort Metadata

```rust
use arrow::array::RecordBatch;
use otel_arrow_rust::schema::{update_schema_metadata, consts};

fn mark_sorted_by_columns(
    batch: &RecordBatch,
    columns: &[&str],
) -> RecordBatch {
    let sort_value = columns.join(",");
    update_schema_metadata(
        batch,
        consts::metadata::SORT_COLUMNS.to_string(),
        sort_value,
    )
}

// Usage after sorting:
let sorted_batch = sort_by_resource_and_scope(&batch)?;
let annotated_batch = mark_sorted_by_columns(
    &sorted_batch,
    &["resource.id", "scope.id"],
);
```

## Attribute-Specific Sorting Helpers

### Attribute Sorting Mechanisms

While there are no dedicated high-level helper functions specifically named "sort attributes by parent_id and key", the codebase provides several mechanisms for attribute sorting:

### 1. Payload-Type-Based Automatic Sorting

**Location**: `src/otap/transform/transport_optimize.rs`

The `get_sort_column_paths()` function defines the sort order for all attribute table types:

```rust
ArrowPayloadType::ResourceAttrs
| ArrowPayloadType::ScopeAttrs
| ArrowPayloadType::SpanAttrs
| ArrowPayloadType::MetricAttrs
| ArrowPayloadType::LogAttrs
| ArrowPayloadType::SpanEventAttrs
| ArrowPayloadType::SpanLinkAttrs
// ... all attribute types
=> &[
    consts::ATTRIBUTE_TYPE,    // Sort dimension 1
    consts::ATTRIBUTE_KEY,     // Sort dimension 2
    consts::ATTRIBUTE_STR,     // Sort dimension 3 (and other value columns)
    consts::ATTRIBUTE_INT,
    consts::ATTRIBUTE_DOUBLE,
    consts::ATTRIBUTE_BOOL,
    consts::ATTRIBUTE_BYTES,
    consts::PARENT_ID,         // Sort dimension LAST
],
```

**Key insight**: Attributes are NOT sorted by parent_id first. Instead, they're sorted by:

1. Type → Key → Value(s) → Parent ID (for quasi-delta encoding)
2. This allows delta encoding of parent_id when consecutive rows have matching type/key/value

### 2. Legacy Helper: `sort_record_batch()`

**Location**: `src/otap/groups_old.rs` (lines 712-785)

This is an older helper function that can sort by parent_id:

```rust
enum HowToSort {
    SortByParentIdAndId,  // Sort by parent_id, then id
    SortById,             // Sort by id only
}

fn sort_record_batch(rb: RecordBatch, how: HowToSort) -> Result<RecordBatch>
```

**Usage**:

```rust
use HowToSort::SortByParentIdAndId;

// Sort by parent_id first, then id
let sorted = sort_record_batch(rb, SortByParentIdAndId)?;
```

**Note**: This is in `groups_old.rs` and may be deprecated in favor of newer approaches.

### 3. Transport Optimization Sorting

**Location**: `src/otap/transform/transport_optimize.rs` (line 407)

```rust
fn sort_record_batch(
    payload_type: &ArrowPayloadType,
    record_batch: &RecordBatch,
) -> Result<RecordBatch>
```

This function automatically sorts by the appropriate columns based on the payload type (using `get_sort_column_paths()`).

**For attributes**: Sorts by type → key → values → parent_id (NOT parent_id first).

### 4. Manual Sorting with Arrow Compute

If you need custom sorting by parent_id and key specifically:

```rust
use arrow::compute::{SortColumn, SortOptions};
use crate::otap::transform::sort_to_indices;
use crate::schema::consts;

// Create sort columns: parent_id first, then key
let sort_columns = vec![
    SortColumn {
        values: batch.column_by_name(consts::PARENT_ID).unwrap().clone(),
        options: Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
    },
    SortColumn {
        values: batch.column_by_name(consts::ATTRIBUTE_KEY).unwrap().clone(),
        options: Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
    },
];

// Get sort indices (uses row format for efficiency)
let indices = sort_to_indices(&sort_columns)?;

// Apply sort
let sorted_batch = arrow::compute::take_record_batch(&batch, &indices)?;
```

### 5. Deduplication Context: Why Parent ID + Key Matters

**Location**: `src/otap/groups.rs` and `src/otap/groups_v1.rs`

The batching implementation mentions:

- "Resource/Scope attributes are deduplicated within each output batch"
- Verification that "resource attrs and scope attrs are sorted by PARENT_ID"

**For deduplication** (as discussed in `ARROW_GROUPING_DEDUPLICATION.md`):

```rust
use arrow_row::{RowConverter, SortField};

// Convert to row format for grouping by parent_id + key + values
let fields = vec![
    SortField::new(DataType::UInt16),  // PARENT_ID
    SortField::new(DataType::Dictionary(  // KEY
        Box::new(DataType::Int16),
        Box::new(DataType::Utf8)
    )),
    // ... value columns
];

let converter = RowConverter::new(fields)?;
let rows = converter.convert_columns(&columns)?;

// Now you can group/deduplicate using row.owned() as HashMap keys
```

### Summary: No Direct Helper, But Multiple Approaches

**For attribute batching/deduplication**:

1. Use `RowConverter` to create comparable rows including parent_id and key
2. Group by `OwnedRow` in a HashMap for deduplication
3. Build remapping of old parent_id → new parent_id

**For transport optimization** (compression):

1. Sort by type → key → value → parent_id (enables quasi-delta encoding)
2. Use `transport_optimize::sort_record_batch()` with appropriate `ArrowPayloadType`

**For simple parent_id ordering**:

1. Use the manual Arrow compute approach above
2. Or adapt the legacy `groups_old::sort_record_batch()` function

The key insight is that **attribute sorting strategy depends on your goal**:

- **For deduplication**: Group by (parent_id, key, values) using RowConverter
- **For compression**: Sort by (type, key, values, parent_id) for quasi-delta encoding
- **For hierarchical access**: Sort by parent_id to group related attributes together

## Summary

### Key Takeaways

1. **Schema Metadata**: Sort order is stored as schema-level metadata with key `"sort_columns"`

2. **Field Metadata**: Individual columns have encoding metadata (`"encoding"`) with values:
   - `"plain"` - no encoding
   - `"delta"` - requires sorted data
   - `"quasidelta"` - requires multi-column sort for attributes

3. **Payload-Specific Sorts**: Each `ArrowPayloadType` has a defined sort order in `get_sort_column_paths()`

4. **Sort Order Matters For**:
   - Delta encoding (ID columns must be sorted)
   - Quasi-delta encoding (attributes must be sorted by type/key/value)
   - Compression effectiveness
   - Efficient hierarchical traversal

5. **Batching Implications**: When combining/rebatching:
   - Materialize delta-encoded columns first
   - Sort combined data according to batching policy
   - Update `SORT_COLUMNS` metadata to reflect new order
   - Reapply encodings if needed for compression

### Related Files

- `src/schema/consts.rs` - Metadata constant definitions
- `src/schema.rs` - Metadata helper functions
- `src/otap/transform/transport_optimize.rs` - Sort specifications and encoding
- `src/otap/transform.rs` - Sort implementation helpers
- `src/otlp/common.rs` - Sorted batch cursor for traversal
