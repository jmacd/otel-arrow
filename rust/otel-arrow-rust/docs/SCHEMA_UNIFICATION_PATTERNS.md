# Schema Unification Patterns in OTAP

Research summary for OTAP batching project  
Josh MacDonald, Oct 31, 2025

## Overview

This document summarizes existing patterns for schema unification in the otel-arrow-rust and otap-dataflow codebases. When batching OTAP payloads, we encounter situations where different input batches have varying schemas, particularly for attribute tables where optional value columns (int, double, str, bool, bytes, ser) may be present in some batches but absent in others.

## Core Problem

Attribute tables in OTAP have the following structure:

- **Required columns**: `parent_id`, `key`, `type`
- **Optional value columns**: `str`, `int`, `double`, `bool`, `bytes`, `ser`

Each input batch may contain only a subset of these value columns:

- Batch A: has `int` and `str` columns
- Batch B: has `double` and `bool` columns  
- Batch C: has all value columns

To concatenate or batch these together, we need to:

1. Unify schemas to include all columns present in any batch
2. Add null-filled columns for missing fields in each batch
3. Handle dictionary encoding variations (Dict8 vs Dict16 vs native)

## Existing Unification Implementation

### Location: `src/otap/groups_old.rs`

The primary schema unification logic exists in the `unify()` function at lines 1167-1296.

### High-Level Algorithm

```rust
fn unify<const N: usize>(batches: &mut [[Option<RecordBatch>; N]]) -> Result<()>
```

This function modifies record batches in-place to ensure they all have compatible schemas.

#### Phase 1: Discovery (Lines 1175-1243)

Three-pass discovery process:

```rust
// Pass 1: Discover dictionary and struct fields
for batches in batches.iter() {
    if let Some(rb) = &batches[payload_type_index] {
        try_discover_dictionaries(rb, &mut dict_fields)?;
        try_discover_structs(rb, &mut struct_fields)?;
    }
}

// Pass 2: Count dictionary values for key type selection
for batches in batches.iter() {
    if let Some(rb) = &batches[payload_type_index] {
        try_visit_dictionary_values(rb, &mut dict_fields)?;
        try_visit_struct_dictionary_values(rb, &mut struct_fields)?;
    }
}

// Pass 3: Unify dictionaries and structs
for batches in batches.iter_mut() {
    if let Some(rb) = batches[payload_type_index].take() {
        let rb = try_unify_dictionaries(&rb, &mut dict_fields)?;
        let rb = try_unify_structs(&rb, &mut struct_fields)?;
        let _ = batches[payload_type_index].replace(rb);
    }
}
```

**Why three passes?**

1. **Discovery**: Identify all dictionary and struct columns across all batches
2. **Value counting**: Determine optimal dictionary key type (UInt8 vs UInt16 vs native) based on total cardinality
3. **Unification**: Apply the selected types and add missing fields

#### Phase 2: Missing Optional Columns (Lines 1254-1296)

After handling dictionaries and structs, add missing optional columns:

```rust
// Find columns that exist in some batches but not all
for (missing_field_name, present_batch_indices) in field_name_to_batch_indices
    .iter()
    .filter(|(_, cols)| cols.len() != len)  // Not present in all batches
{
    // Get field definition from any batch that has it
    let field = Arc::new(
        schemas[*present_batch_indices.iter().next().unwrap()]
            .field_with_name(missing_field_name)?
            .clone(),
    );
    assert!(field.is_nullable());  // Optional columns must be nullable
    
    // Add null-filled column to batches that don't have it
    for missing_batch_index in all_batch_indices.difference(present_batch_indices) {
        if let Some(batch) = batches[missing_batch_index][payload_type_index].take() {
            let (schema, mut columns, num_rows) = batch.into_parts();
            let schema = Arc::unwrap_or_clone(schema);
            
            // Add field to schema
            let mut builder = SchemaBuilder::from(&schema);
            builder.push(field.clone());
            let schema = Arc::new(builder.finish());
            
            // Add null-filled array
            columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
            
            let batch = RecordBatch::try_new(schema, columns)?;
            let _ = batches[missing_batch_index][payload_type_index].replace(batch);
        }
    }
}
```

### Dictionary Unification

#### UnifiedDictionaryTypeSelector (Lines 1300-1420)

This struct determines the optimal dictionary key type or whether to use native encoding:

```rust
struct UnifiedDictionaryTypeSelector {
    total_batch_size: usize,
    values_arrays: Vec<ArrayRef>,
    smallest_key_type: DataType,  // UInt8 or UInt16
    selected_type: Option<UnifiedDictionaryType>,
}

enum UnifiedDictionaryType {
    Dictionary(DataType),  // DataType is the key type (UInt8/UInt16)
    Native,                // Convert to non-dictionary
}
```

**Selection logic**:

1. If total non-null values ≤ 255: Use `Dictionary(UInt8)`
2. If smallest_key_type is UInt16 and total ≤ 65535: Use `Dictionary(UInt16)`
3. Otherwise: Estimate cardinality and decide:
   - Small elements (≤4 bytes): Use `RoaringBitmap` for cardinality estimation
   - Large elements: Use `AHashSet` for cardinality estimation
   - If unique values fit in key type: Use dictionary
   - Otherwise: Use `Native` (flatten to non-dictionary)

**Key insight**: The algorithm tries to keep dictionary encoding when possible, but will convert to native arrays if cardinality is too high.

### Struct Unification

Structs are unified similarly to top-level columns, with special handling for nested dictionary fields:

```rust
fn try_discover_structs(
    record_batch: &RecordBatch,
    all_struct_fields: &mut BTreeMap<String, (Field, BTreeMap<String, StructFieldToUnify>)>,
) -> Result<()>
```

For each struct column:

1. Discover all fields that exist within the struct across batches
2. Track dictionary fields within structs for key type selection
3. Add missing struct fields as null-filled arrays

### Cardinality Estimation

Two estimators are used:

#### SmallCardinalityEstimator (Lines 1420-1550)

For primitive types ≤4 bytes, uses `RoaringBitmap`:

```rust
struct SmallCardinalityEstimator<'parent> {
    bitmap: roaring::RoaringBitmap,
    // ... other fields
}
```

- Efficient for checking unique values
- Amortizes cardinality checks to avoid overhead per element
- Early termination when cardinality exceeds key type capacity

#### LargeCardinalityEstimator (Lines 1550-1700)

For larger elements, uses `AHashSet<Vec<u8>>`:

```rust
struct LargeCardinalityEstimator<'parent> {
    seen_values: AHashSet<Vec<u8>>,
    // ... other fields
}
```

Both estimators implement early termination:

- Stop counting when cardinality exceeds UInt8::MAX if smallest_key_type is UInt8
- Stop counting when cardinality exceeds UInt16::MAX if smallest_key_type is UInt16

## Attribute-Specific Patterns

### Encoder Behavior: Optional Value Columns

**Location**: `src/encode/record/attributes.rs` (Lines 180-448)

The `AnyValuesRecordsBuilder` demonstrates how optional value columns are handled during encoding:

```rust
pub struct AnyValuesRecordsBuilder {
    value_type: UInt8ArrayBuilder,      // Always present (required)
    string_value: BinaryArrayBuilder,   // Optional (nullable)
    int_value: Int64ArrayBuilder,       // Optional (nullable)
    double_value: Float64ArrayBuilder,  // Optional (nullable)
    bool_value: AdaptiveBooleanArrayBuilder,  // Optional (nullable)
    bytes_value: BinaryArrayBuilder,    // Optional (nullable)
    ser_value: BinaryArrayBuilder,      // Optional (nullable)
    
    // Pending null tracking for efficient batching
    pending_string_nulls: usize,
    pending_int_nulls: usize,
    pending_double_nulls: usize,
    pending_bool_nulls: usize,
    pending_bytes_nulls: usize,
    pending_ser_nulls: usize,
}
```

**Key pattern**: When appending a value of one type, increment pending nulls for all other types:

```rust
pub fn append_int(&mut self, val: i64) {
    self.value_type.append_value(&(AttributeValueType::Int as u8));
    
    // Flush pending nulls for int array and append value
    if self.pending_int_nulls > 0 {
        self.int_value.append_nulls(self.pending_int_nulls);
        self.pending_int_nulls = 0;
    }
    self.int_value.append_value(&val);
    
    // Increment pending nulls for all other arrays
    self.pending_string_nulls += 1;
    self.pending_double_nulls += 1;
    self.pending_bool_nulls += 1;
    self.pending_bytes_nulls += 1;
    self.pending_ser_nulls += 1;
}
```

### Finish Behavior

The `finish()` method includes only non-empty columns (Lines 415-478):

```rust
pub fn finish(
    &mut self,
    columns: &mut Vec<ArrayRef>,
    fields: &mut Vec<Field>,
) -> Result<(), ArrowError> {
    // Flush all pending nulls
    self.fill_missing_nulls();
    
    // Only add columns that have data
    if let Some(array) = self.value_type.finish() {
        fields.push(Field::new(consts::ATTRIBUTE_TYPE, ...));
        columns.push(array);
    }
    
    if let Some(array) = self.string_value.finish() {
        // Only included if string values were appended
        fields.push(Field::new(consts::ATTRIBUTE_STR, ..., true));  // nullable=true
        columns.push(array);
    }
    
    // Similar for int, double, bool, bytes, ser...
}
```

**Important**: Optional arrays return `None` from `finish()` if no values were ever appended. This is how the encoder produces schemas with varying columns.

## Schema Merging Utility

### Arrow's Built-in Schema::try_merge

**Location**: Used in `groups_old.rs` line 850

```rust
let schema = Arc::new(
    Schema::try_merge(select(batches, i).map(|rb| Arc::unwrap_or_clone(rb.schema())))
        .context(error::BatchingSnafu)?,
);
```

Arrow's `Schema::try_merge()` merges multiple schemas by:

1. Including all fields from all schemas
2. Ensuring fields with the same name have the same type
3. Failing if there are type conflicts

**Limitation**: Doesn't handle dictionary type unification - hence the need for `unify()`.

## Batching Strategy for Attributes

Based on these patterns, here's the recommended approach for attribute batching:

### 1. Collect All Schemas

```rust
let mut all_attr_schemas: Vec<SchemaRef> = Vec::new();
for batch in input_batches {
    if let Some(attr_batch) = batch[attr_payload_index].as_ref() {
        all_attr_schemas.push(attr_batch.schema());
    }
}
```

### 2. Build Unified Field Set

```rust
use std::collections::BTreeMap;

struct FieldInfo {
    field: Field,
    present_in: HashSet<usize>,  // Batch indices
}

let mut unified_fields: BTreeMap<String, FieldInfo> = BTreeMap::new();

// Always include required fields
unified_fields.insert("parent_id".to_string(), FieldInfo { ... });
unified_fields.insert("key".to_string(), FieldInfo { ... });
unified_fields.insert("type".to_string(), FieldInfo { ... });

// Collect optional fields from all batches
for (batch_idx, schema) in all_attr_schemas.iter().enumerate() {
    for field in schema.fields() {
        unified_fields.entry(field.name().clone())
            .or_insert_with(|| FieldInfo {
                field: field.as_ref().clone(),
                present_in: HashSet::new(),
            })
            .present_in.insert(batch_idx);
    }
}
```

### 3. Apply Unification

Use the existing `unify()` function or implement a simplified version:

```rust
// Option A: Use existing unify() function
unify(&mut batches)?;

// Option B: Implement simplified version for attributes only
unify_attribute_batches(&mut attr_batches)?;
```

### 4. Concatenate with Unified Schema

```rust
use arrow::compute::concat_batches;

let unified_schema = Arc::new(Schema::new(
    unified_fields.values().map(|info| info.field.clone()).collect()
));

// Add missing columns to each batch
for (batch_idx, batch) in batches.iter_mut().enumerate() {
    if let Some(attr_batch) = batch.take() {
        let (schema, mut columns, num_rows) = attr_batch.into_parts();
        
        // Add null arrays for missing columns
        for field_name in unified_fields.keys() {
            if !schema.column_with_name(field_name).is_some() {
                let field = &unified_fields[field_name].field;
                columns.push(arrow::array::new_null_array(
                    field.data_type(), 
                    num_rows
                ));
            }
        }
        
        *batch = Some(RecordBatch::try_new(unified_schema.clone(), columns)?);
    }
}

// Now safe to concatenate
let combined = concat_batches(&unified_schema, batches.iter().flatten())?;
```

## Key Takeaways

### 1. Three-Phase Unification

- **Discovery**: Find all fields and dictionary columns
- **Analysis**: Determine optimal dictionary key types
- **Application**: Apply unified types and add missing fields

### 2. Dictionary Handling

- Use cardinality estimation to choose key type (UInt8, UInt16, or Native)
- Convert all variations to the selected type
- Flatten to native arrays when cardinality exceeds capacity

### 3. Optional Column Pattern

- Optional columns are nullable (`Field::new(..., true)`)
- Missing columns filled with `arrow::array::new_null_array()`
- Encoder only includes columns that have data

### 4. Schema Compatibility

- All batches must agree on field types for same-named columns
- Use `Schema::try_merge()` for basic merging
- Use `unify()` for dictionary type resolution

### 5. Struct Columns (Resource/Scope)

- Struct fields are unified recursively
- Nested dictionary fields handled separately
- Missing struct fields added as null-filled arrays

## Implementation Notes for Batching

### For Attribute Tables Specifically

1. **Required columns always present**: `parent_id`, `key`, `type`
2. **Optional value columns**: May be absent if no attributes of that type exist
3. **Dictionary encoding**: Keys use Dict8, values may use Dict8/Dict16
4. **Nullability**: All value columns are nullable

### Recommended Simplification

For the initial batching implementation, we can simplify:

1. **Skip dictionary type optimization**: Always use Dict16 for attribute values
2. **Pre-materialize**: Convert delta-encoded columns to plain encoding first
3. **Union all schemas**: Use `Schema::try_merge()` for basic unification
4. **Add nulls**: Fill missing columns with null arrays before concatenation

This avoids the complexity of cardinality estimation while still producing correct batches. Dictionary optimization can be added later as a refinement.

### Example: Simplified Attribute Unification

```rust
fn unify_attribute_schemas(
    batches: &mut [Option<RecordBatch>],
) -> Result<SchemaRef> {
    // Collect all field names and types
    let mut fields: BTreeMap<String, Field> = BTreeMap::new();
    
    for batch in batches.iter().flatten() {
        for field in batch.schema().fields() {
            if let Some(existing) = fields.get(field.name()) {
                // Verify type consistency
                if existing.data_type() != field.data_type() {
                    // Handle dictionary variations here
                    // For now, convert both to native type
                }
            } else {
                fields.insert(field.name().clone(), field.as_ref().clone());
            }
        }
    }
    
    // Build unified schema
    let unified_schema = Arc::new(Schema::new(
        fields.into_values().collect::<Vec<_>>()
    ));
    
    // Add missing columns to each batch
    for batch in batches.iter_mut() {
        if let Some(rb) = batch.take() {
            *batch = Some(add_missing_columns(rb, &unified_schema)?);
        }
    }
    
    Ok(unified_schema)
}

fn add_missing_columns(
    batch: RecordBatch,
    target_schema: &Schema,
) -> Result<RecordBatch> {
    let (schema, mut columns, num_rows) = batch.into_parts();
    let mut new_columns = Vec::new();
    
    for field in target_schema.fields() {
        if let Ok(idx) = schema.index_of(field.name()) {
            new_columns.push(columns[idx].clone());
        } else {
            // Add null-filled column for missing field
            new_columns.push(arrow::array::new_null_array(
                field.data_type(),
                num_rows,
            ));
        }
    }
    
    RecordBatch::try_new(Arc::new(target_schema.clone()), new_columns)
}
```

## Related Files

- `src/otap/groups_old.rs` - Main unification implementation (lines 1149-2098)
- `src/encode/record/attributes.rs` - Attribute encoder with optional columns
- `src/schema/consts.rs` - Column name constants
- Arrow-RS `arrow/datatypes/schema.rs` - `Schema::try_merge()` implementation

## Summary

The existing codebase has robust schema unification patterns that handle:

1. **Optional columns**: Via nullable fields and null-filled arrays
2. **Dictionary variations**: Via cardinality estimation and type selection
3. **Struct columns**: Via recursive field unification
4. **Type consistency**: Via validation and conversion

For attribute batching, we should:

1. Start with simplified unification (union all fields, add nulls)
2. Use the existing `unify()` function or adapt its patterns
3. Focus on correctness first, optimize dictionary encoding later
4. Leverage Arrow's built-in utilities (`Schema::try_merge`, `concat_batches`, `new_null_array`)

The three-phase discovery-analysis-application pattern is key to handling complex scenarios efficiently.
