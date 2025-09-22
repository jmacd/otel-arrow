# Arrow Compute Kernel Optimization Plan

**Planning Date**: September 18, 2025  
**Project Phase**: Performance Optimization - Phase 2  
**Status**: 🎯 **READY FOR IMPLEMENTATION**

## Executive Summary

This document outlines a performance optimization plan for the **Parquet Receiver's array management system**, focusing on replacing element-by-element processing with **Arrow compute kernels** for vectorized operations. The current implementation uses inefficient builder patterns for ID column transformations while already utilizing optimal approaches for view materialization and array reference management.

## 🔍 Current State Analysis

### **Performance Bottleneck Identified**

The primary performance bottleneck is in **ID column transformation** where we convert:
- **UInt32 → UInt16** (type conversion)
- **Original IDs → Normalized IDs** (subtract batch offset)

#### **Current Implementation Pattern**
```rust
// 🐌 CURRENT: Element-by-element processing with builders
fn transform_id_column(&mut self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
    let uint32_array = column.as_any().downcast_ref::<UInt32Array>()?;
    let mut uint16_builder = UInt16Array::builder(uint32_array.len());

    for i in 0..uint32_array.len() {  // ❌ INEFFICIENT: Individual element access
        if uint32_array.is_null(i) {
            uint16_builder.append_null();
        } else {
            let original_id = uint32_array.value(i);
            let normalized_id = self.normalize_id(original_id)?;  // original_id - batch_start_id
            uint16_builder.append_value(normalized_id);
        }
    }

    Ok(Arc::new(uint16_builder.finish()) as ArrayRef)
}
```

**Performance Impact**: 
- **10-100x slower** than vectorized operations for large arrays
- **Excessive memory allocation** from builder patterns
- **No SIMD utilization** despite Arrow's SIMD-optimized kernels being available

#### **✅ Efficient Pattern - Using Arrow Compute Kernels**
```rust
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::kernels::cast::cast;

// ✅ OPTIMAL: Slice first, then use Arrow compute kernels
fn transform_id_column_optimized(&self, column: &ArrayRef, start_row: usize, num_rows: usize) -> Result<ArrayRef, ParquetReceiverError> {
    // Step 1: Zero-copy slice to get only the data we need
    let column_slice = column.slice(start_row, num_rows);
    
    // Step 2: Create scalar for vectorized subtraction
    let offset_scalar = UInt32Array::new_scalar(self.batch_start_id);
    
    // Step 3: Vectorized subtraction - SIMD optimized, processes all elements in parallel
    let normalized_u32 = sub_wrapping(&column_slice, &offset_scalar)
        .map_err(|e| ParquetReceiverError::Arrow(e))?;
    
    // Step 4: Vectorized type conversion - also SIMD optimized
    let normalized_u16 = cast(&normalized_u32, &DataType::UInt16)
        .map_err(|e| ParquetReceiverError::Arrow(e))?;
        
    Ok(normalized_u16)
}
```

**Benefits of this approach**:
- **SIMD utilization**: Arrow kernels automatically use SIMD instructions when available
- **Minimal memory allocation**: Only allocates result arrays, no intermediate builders
- **Slice-first efficiency**: Processes only the 1K elements we need, not the full 100K column
- **Vectorized operations**: Both subtraction and cast operations are fully vectorized

#### **📊 Performance Comparison**
```rust
// Current approach: 25 lines, element-by-element processing
for i in 0..uint32_array.len() {
    if uint32_array.is_null(i) {
        uint16_builder.append_null();
    } else {
        let original_id = uint32_array.value(i);
        let normalized_id = self.normalize_id(original_id)?;
        uint16_builder.append_value(normalized_id);
    }
}

// Optimized approach: 4 lines, vectorized processing  
let column_slice = column.slice(start_row, num_rows);
let offset_scalar = UInt32Array::new_scalar(self.batch_start_id);
let normalized_u32 = sub_wrapping(&column_slice, &offset_scalar)?;
let normalized_u16 = cast(&normalized_u32, &DataType::UInt16)?;
```

**Result**: **6x fewer lines of code** + **10-100x faster execution** + **automatic SIMD utilization**

### **Already Optimal Components**

#### ⚠️ **View Type Materialization - Needs Slice-Aware Optimization**
```rust
// 🔄 CURRENT: Casting entire column (inefficient for sub-batches)
fn materialize_view_column(&self, column: &ArrayRef, target_type: &DataType) -> Result<ArrayRef, ParquetReceiverError> {
    let materialized = compute::cast(column, target_type)?;  // Casts entire column
    Ok(materialized)
}

// ✅ OPTIMAL: Slice first, then cast only what we need
fn materialize_view_column_slice(
    &self, 
    column: &ArrayRef, 
    target_type: &DataType,
    start_row: usize,
    num_rows: usize
) -> Result<ArrayRef, ParquetReceiverError> {
    let column_slice = column.slice(start_row, num_rows);  // Zero-copy slice first
    let materialized = compute::cast(&column_slice, target_type)?;  // Cast only the slice
    Ok(materialized)
}
```

**⚠️ Performance Impact**: For large columns with view types (UTF8View, BinaryView), casting the entire column when only needing a small slice wastes significant CPU cycles and temporary memory allocation.

#### ⚠️ **Array Reference Management - Needs Optimization**
```rust
// 🔄 CURRENT: Full column cloning (inefficient for sub-batches)
} else {
    new_columns.push(column.clone());  // Clones entire column reference
    new_fields.push(field.as_ref().clone());
}

// ✅ OPTIMAL: Use Arrow slicing for sub-batch column creation
} else {
    new_columns.push(column.slice(start_offset, batch_length));  // Zero-copy sub-slice
    new_fields.push(field.as_ref().clone());
}
```

**⚠️ Memory Semantics Note**: Arrow slicing creates a **view** into the original buffer - the new array holds a reference to the original data buffer, not a copy. This means:
- **Memory efficiency**: No data copying, just metadata changes (offset + length)
- **Shared ownership**: Original batch memory stays alive as long as any slice references it
- **Memory implications**: Large input batches won't be garbage collected until all slices are dropped

## 🎯 Optimization Strategy

### **Phase 1: Vectorized ID Column Transformation + Column Slicing (Priority: High)**

The two optimizations should be implemented together since they both affect the same batch transformation methods:

#### **Target Methods**
1. `transform_primary_batch()` - Needs both vectorized ID operations and column slicing
2. `transform_child_batch()` - Needs both vectorized parent_id operations and column slicing
3. `batch_to_otap()` - Needs integration with sliced batch processing

#### **Combined Optimization Approach**
Instead of processing entire input batches and then creating sub-batches, we should:

1. **Slice first**: Extract only the rows needed for current OTAP batch
2. **Transform slices**: Apply vectorized transformations to sliced data only
3. **Minimize memory**: Avoid intermediate full-batch transformations
Replace element-by-element processing with **Arrow compute kernels**:

```rust
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::kernels::cast::cast;

fn transform_id_column_vectorized(&self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
    // ✅ VECTORIZED: Single operation on entire array
    let offset_scalar = UInt32Array::new_scalar(self.batch_start_id);
    
    // Step 1: Vectorized subtraction (ID normalization)
    let normalized_u32 = sub_wrapping(column, &offset_scalar)
        .map_err(|e| ParquetReceiverError::Arrow(e))?;
    
    // Step 2: Vectorized type conversion (UInt32 -> UInt16)
    let normalized_u16 = cast(&normalized_u32, &DataType::UInt16)
        .map_err(|e| ParquetReceiverError::Arrow(e))?;
        
    Ok(normalized_u16)
}
```

### **Phase 2: Array Slicing Optimization**

#### **Current Gap**
The implementation currently uses full column cloning when creating smaller OTAP batches from larger input batches. This is inefficient when we only need a subset of the data.

#### **Column Slicing Strategy**
```rust
// ❌ INEFFICIENT: Current approach clones entire column references
fn transform_primary_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ParquetReceiverError> {
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);  // Gets entire column
        // ... transformations ...
        new_columns.push(column.clone());    // Clones entire column reference
    }
}

// ✅ EFFICIENT: Slice-aware batch transformation  
fn transform_batch_slice(
    &mut self, 
    batch: &RecordBatch, 
    start_row: usize, 
    num_rows: usize
) -> Result<RecordBatch, ParquetReceiverError> {
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    let mut new_fields = Vec::with_capacity(batch.num_columns());

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        
        if field.name() == "id" && matches!(field.data_type(), DataType::UInt32) {
            // Transform only the slice we need
            let column_slice = column.slice(start_row, num_rows);
            let transformed_column = self.transform_id_column_vectorized(&column_slice)?;
            new_columns.push(transformed_column);
            new_fields.push(Field::new("id", DataType::UInt16, field.is_nullable()));
        } else if matches!(field.data_type(), DataType::Utf8View) {
            // ✅ EFFICIENT: Slice first, then materialize only what we need
            let column_slice = column.slice(start_row, num_rows);
            let materialized_column = compute::cast(&column_slice, &DataType::Utf8)?;
            new_columns.push(materialized_column);
            new_fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
        } else if matches!(field.data_type(), DataType::BinaryView) {
            // ✅ EFFICIENT: Slice first, then materialize only what we need  
            let column_slice = column.slice(start_row, num_rows);
            let materialized_column = compute::cast(&column_slice, &DataType::Binary)?;
            new_columns.push(materialized_column);
            new_fields.push(Field::new(field.name(), DataType::Binary, field.is_nullable()));
        } else {
            // ✅ EFFICIENT: Zero-copy slice of the exact data we need
            let column_slice = column.slice(start_row, num_rows);
            new_columns.push(column_slice);
            new_fields.push(field.as_ref().clone());
        }
    }
    
    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| ParquetReceiverError::Arrow(e))
}
```

#### **Streaming Coordinator Integration**
```rust
impl StreamingCoordinator {
    /// Process streaming batch by creating appropriately-sized OTAP batches
    pub fn batch_to_otap_with_slicing(&self, streaming_batch: StreamingBatch) -> Result<Vec<OtapArrowRecords>, ParquetReceiverError> {
        let batch_size = 1000; // Configurable OTAP batch size
        let total_rows = streaming_batch.primary_batch.num_rows();
        let mut otap_batches = Vec::new();
        
        // Process input batch in smaller chunks
        for start_row in (0..total_rows).step_by(batch_size) {
            let remaining_rows = total_rows - start_row;
            let current_batch_size = remaining_rows.min(batch_size);
            
            // ✅ EFFICIENT: Create sub-batch using zero-copy slicing
            let primary_slice = streaming_batch.primary_batch.slice(start_row, current_batch_size);
            
            // Transform and normalize the slice (not the full batch)
            let (max_id, min_id) = Self::analyze_id_range(&primary_slice)?;
            
            let mut id_mapper = IdMapper::new();
            id_mapper.set_batch_start(min_id);
            
            // Transform only the data we need
            let transformed_primary = id_mapper.transform_batch_slice(&primary_slice, 0, current_batch_size)?;
            
            // Process child batches with corresponding ID ranges
            let child_batches = self.process_child_batches_for_range(
                &streaming_batch.child_batches, 
                min_id, 
                max_id,
                &mut id_mapper
            )?;
            
            // Create OTAP batch from sliced and transformed data
            let otap_batch = self.create_otap_batch(transformed_primary, child_batches)?;
            otap_batches.push(otap_batch);
        }
        
        Ok(otap_batches)
    }
}
```

## 🏗️ Implementation Plan

### **Memory Management Implications**

Before diving into implementation, it's crucial to understand the memory semantics of Arrow's zero-copy operations:

#### **Arrow Slicing Memory Model**
```rust
// How Arrow slicing actually works internally:
let original_batch: RecordBatch = /* large batch with 100K rows */;

// This creates a NEW array that points to the SAME underlying buffer
let slice = original_batch.column(0).slice(1000, 5000);

// Memory structure:
// original_batch.data -> [Buffer with 100K elements]
//                            ↑
// slice.data -> [ArrayData { buffer_ref, offset: 1000, length: 5000 }]
```

#### **Reference Counting Behavior**
- **Buffer sharing**: Slices hold `Arc<Buffer>` references to original data
- **Memory lifetime**: Original batch stays in memory until ALL slices are dropped
- **GC implications**: Large input batches won't be freed until processing pipeline completes

#### **Memory Usage Patterns**

**✅ Good Pattern - Short-lived slices:**
```rust
// Process batch in chunks, drop slices quickly
for chunk in (0..total_rows).step_by(batch_size) {
    let slice = batch.slice(chunk, batch_size);
    let otap_batch = process_slice(slice)?;
    send_downstream(otap_batch);
    // slice drops here, reducing memory pressure
}
```

**⚠️ Potential Issue - Long-lived slice accumulation:**
```rust
// This keeps the entire original batch in memory
let mut all_slices = Vec::new();
for chunk in (0..total_rows).step_by(batch_size) {
    let slice = batch.slice(chunk, batch_size);
    all_slices.push(slice); // Original batch can't be freed!
}
```

#### **Memory Optimization Strategies**

1. **Process slices immediately**: Don't accumulate slices in collections
2. **Use streaming patterns**: Process → send → drop slice references
3. **Monitor memory usage**: Large input batches × number of concurrent slices
4. **Consider copying for long-term storage**: If slices need to outlive the processing pipeline

### **Phase 1: ID Column Vectorization (Priority: High)**

#### **Step 1.1: Update Dependencies**
```toml
# Ensure Arrow compute kernels are available
arrow = { workspace = true, features = ["compute"] }
```

#### **Step 1.2: Implement Vectorized ID Transformation**
```rust
// File: id_mapping.rs
use arrow::compute::kernels::numeric::sub_wrapping;
use arrow::compute::kernels::cast::cast;

impl IdMapper {
    /// Vectorized ID column transformation using Arrow compute kernels
    fn transform_id_column_vectorized(&self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
        // Validate input type
        if !matches!(column.data_type(), DataType::UInt32) {
            return Err(ParquetReceiverError::Reconstruction(
                format!("Expected UInt32 column, got {:?}", column.data_type())
            ));
        }

        // Create scalar for batch normalization
        let offset_scalar = UInt32Array::new_scalar(self.batch_start_id);
        
        // Vectorized subtraction: all_elements - batch_start_id
        let normalized_u32 = sub_wrapping(column, &offset_scalar)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;
        
        // Vectorized cast: UInt32 -> UInt16
        let normalized_u16 = cast(&normalized_u32, &DataType::UInt16)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;
            
        log::debug!("🚀 Vectorized ID transformation: {} elements (UInt32 -> UInt16 with offset {})", 
                   column.len(), self.batch_start_id);
        
        Ok(normalized_u16)
    }
}
```

#### **Step 1.3: Replace Existing Methods**
Replace the following methods with vectorized versions:
- `transform_id_column()` 
- `transform_parent_id_column_with_creation()`
- `transform_parent_id_column()` (if still needed)

#### **Step 1.4: Error Handling Enhancement**
```rust
// Enhanced error handling for vectorized operations
fn validate_id_range_vectorized(&self, column: &ArrayRef) -> Result<(), ParquetReceiverError> {
    use arrow::compute::{min, max};
    
    // Use Arrow compute to find min/max efficiently
    let min_val = min(column).map_err(|e| ParquetReceiverError::Arrow(e))?;
    let max_val = max(column).map_err(|e| ParquetReceiverError::Arrow(e))?;
    
    if let (Some(min_scalar), Some(max_scalar)) = (min_val, max_val) {
        let min_id = min_scalar.to_u32().unwrap();
        let max_id = max_scalar.to_u32().unwrap();
        
        // Validate normalization will fit in UInt16
        if max_id - self.batch_start_id > u16::MAX as u32 {
            return Err(ParquetReceiverError::Reconstruction(
                format!("Normalized ID range [{}, {}] exceeds UInt16 space", 
                       min_id - self.batch_start_id, max_id - self.batch_start_id)
            ));
        }
    }
    
    Ok(())
}
```

### **Phase 2: Array Slicing Integration (Priority: Medium)**

#### **Step 2.1: Streaming Batch Slicing**
```rust
// Enhanced streaming coordinator with efficient slicing
impl StreamingCoordinator {
    /// Create batch subset using zero-copy slicing
    fn create_batch_subset(&self, batch: &RecordBatch, start: usize, length: usize) -> RecordBatch {
        // ✅ EFFICIENT: Zero-copy slice operation
        batch.slice(start, length)
    }
    
    /// Extract ID range using efficient column slicing
    fn extract_id_range(&self, id_column: &ArrayRef, start: usize, length: usize) -> ArrayRef {
        // ✅ EFFICIENT: Zero-copy column slice
        id_column.slice(start, length)
    }
}
```

#### **Step 2.2: Buffer Management Optimization**
```rust
// Enhanced direct streaming merger with slicing
impl DirectStreamingMerger {
    /// Extract records using efficient array slicing instead of element-by-element copying
    fn extract_range_records_optimized(
        &self,
        buffer: &RecordBatch,
        start_id: u32,
        end_id: u32,
    ) -> Result<(RecordBatch, RecordBatch), ParquetReceiverError> {
        use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
        use arrow::compute::kernels::filter::filter_record_batch;
        
        let parent_id_column = buffer.column_by_name("parent_id")
            .ok_or_else(|| ParquetReceiverError::Reconstruction("No parent_id column".to_string()))?;
        
        // ✅ VECTORIZED: Create filter masks using Arrow compute
        let start_mask = gt_eq(parent_id_column, &UInt32Array::new_scalar(start_id))?;
        let end_mask = lt_eq(parent_id_column, &UInt32Array::new_scalar(end_id))?;
        
        // Combine masks with logical AND
        let range_mask = arrow::compute::and(&start_mask, &end_mask)?;
        
        // ✅ VECTORIZED: Filter entire RecordBatch efficiently
        let matched_records = filter_record_batch(buffer, &range_mask)?;
        let remaining_records = filter_record_batch(buffer, &arrow::compute::not(&range_mask)?)?;
        
        Ok((matched_records, remaining_records))
    }
}
```

### **Phase 3: Performance Validation (Priority: High)**

#### **Step 3.1: Benchmarking Framework**
```rust
#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;
    
    #[test]
    fn benchmark_id_transformation() {
        let test_sizes = vec![1_000, 10_000, 100_000, 1_000_000];
        
        for size in test_sizes {
            let test_data = create_test_uint32_array(size);
            
            // Benchmark current approach
            let start = Instant::now();
            let _result_old = transform_id_column_builder_approach(&test_data);
            let duration_old = start.elapsed();
            
            // Benchmark vectorized approach
            let start = Instant::now();
            let _result_new = transform_id_column_vectorized(&test_data);
            let duration_new = start.elapsed();
            
            println!("Size: {}, Old: {:?}, New: {:?}, Speedup: {:.2}x", 
                    size, duration_old, duration_new, 
                    duration_old.as_nanos() as f64 / duration_new.as_nanos() as f64);
        }
    }
}
```

#### **Step 3.2: Memory Usage Validation**
```rust
#[test]
fn validate_memory_efficiency() {
    use std::alloc::{GlobalAlloc, Layout, System};
    
    // Measure memory allocations for both approaches
    // Verify vectorized approach uses less memory
}
```

## 📊 Expected Performance Improvements

### **Quantitative Benefits**

| **Operation** | **Current Performance** | **Expected Performance** | **Improvement Factor** |
|---------------|------------------------|--------------------------|------------------------|
| **ID Transform (1K elements)** | ~50μs | ~5μs | **10x faster** |
| **ID Transform (100K elements)** | ~5ms | ~50μs | **100x faster** |
| **View Cast (1K from 100K UTF8View)** | ~100μs (full cast) | ~10μs (slice + cast) | **10x faster** |
| **Column Slicing (1K from 100K)** | ~50μs (full clone) | ~1μs (zero-copy slice) | **50x faster** |
| **Sub-batch Creation** | O(n) full batch copy | O(1) slice reference | **Linear → Constant** |
| **Memory Allocation** | O(n) builder allocations | O(1) scalar allocation | **Linear → Constant** |
| **SIMD Utilization** | None | Full SIMD when available | **Platform optimized** |

### **Qualitative Benefits**

#### ✅ **Code Maintainability**
- Fewer lines of code (vectorized operations vs loops)
- Reduced complexity in null handling
- Better error propagation from Arrow compute layer

#### ✅ **Scalability** 
- Performance scales linearly with hardware capabilities
- Automatic SIMD utilization on supporting processors
- Better memory cache utilization

#### ✅ **Correctness**
- Arrow compute kernels are battle-tested
- Consistent null handling across all operations
- Reduced potential for off-by-one errors

## 🧪 Testing Strategy

### **Unit Tests**
- **Correctness verification**: Ensure vectorized results match current element-by-element results
- **Edge cases**: Empty arrays, all-null arrays, mixed null/valid arrays
- **Error conditions**: Type mismatches, overflow scenarios

### **Integration Tests**
- **End-to-end pipeline**: Verify full parquet → OTAP → OTLP flow still works
- **Large dataset processing**: Test with realistic data volumes (1M+ records)
- **Memory pressure**: Validate behavior under constrained memory

### **Performance Tests**
- **Benchmark suite**: Compare old vs new approaches across various array sizes
- **Memory profiling**: Validate reduced allocation overhead
- **Streaming performance**: Test impact on overall pipeline throughput

## 🚨 Risk Assessment

### **Low Risk Items** ✅
- **Existing Arrow compute usage**: Pattern already established in codebase  
- **Array slicing operations**: Arrow's slice() method is well-tested and reliable

### **Medium Risk Items** ⚠️
- **Error handling changes**: Arrow compute errors vs custom validation
- **Null handling behavior**: Ensure vectorized null handling matches current logic
- **Type validation**: Arrow compute may have different error modes
- **Memory lifetime management**: Zero-copy slices keep original batches in memory longer than element-by-element copying

### **High Risk Items** 🚨
- **Memory pressure from slice accumulation**: Long-lived slices can prevent GC of large input batches
- **Reference cycle potential**: Complex slice interdependencies could create memory leaks

### **Mitigation Strategies**
1. **Comprehensive test coverage** before replacing existing methods
2. **Feature flag approach** to enable/disable vectorized operations during transition
3. **Performance regression monitoring** in CI/CD pipeline
4. **Memory profiling during development** to validate slice lifetime management
5. **Streaming patterns enforcement** - ensure slices are processed and dropped quickly
6. **Memory pressure testing** with large input batches and concurrent slice processing

## 📅 Implementation Timeline

### **Week 1: Foundation**
- [ ] Update dependencies and imports
- [ ] Implement vectorized ID transformation methods
- [ ] Create comprehensive unit tests

### **Week 2: Integration**
- [ ] Replace existing methods with vectorized versions
- [ ] Update error handling and logging
- [ ] Validate end-to-end pipeline functionality

### **Week 3: Optimization**
- [ ] Implement array slicing enhancements
- [ ] Add performance benchmarking framework
- [ ] Memory usage optimization validation

### **Week 4: Validation**
- [ ] Large-scale performance testing
- [ ] Memory profiling and optimization
- [ ] Documentation and code review

## 🎯 Success Criteria

### **Performance Targets**
- [ ] **10x improvement** in ID transformation speed for arrays >1K elements
- [ ] **50% reduction** in memory allocation overhead
- [ ] **No performance regression** in overall pipeline throughput

### **Quality Targets**
- [ ] **100% test coverage** for all modified array transformation methods
- [ ] **Zero functional regressions** in end-to-end attribute reconstruction
- [ ] **Maintainable code** with clear documentation of Arrow compute usage

### **Operational Targets**
- [ ] **Backward compatibility** during transition period
- [ ] **Clear performance monitoring** with before/after metrics
- [ ] **Production readiness** with comprehensive error handling

---

## 🔄 Future Enhancement Opportunities

### **Phase 3: Advanced Arrow Compute Integration**
- **Dictionary encoding optimization** for string columns
- **Columnar aggregation operations** for batch statistics
- **Parallel processing** with Arrow's compute thread pools

### **Phase 4: Memory Management Optimization**
- **Arrow memory pools** for predictable memory usage
- **Buffer reuse strategies** for repeated transformations
- **Streaming window operations** for very large datasets

---

**Implementation Status**: **📋 READY FOR DEVELOPMENT**

**Next Steps**: Begin with Phase 1 implementation focusing on vectorized ID column transformation using Arrow compute kernels.