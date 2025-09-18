// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! ID mapping utilities for converting between UInt32 parquet IDs and UInt16 OTAP IDs
//!
//! This module handles the critical transformation from UInt32 ID space (used in parquet files)
//! to UInt16 ID space (required by OTAP). This enables streaming processing of large datasets
//! while maintaining OTAP compatibility.

use crate::parquet_receiver::error::ParquetReceiverError;
use arrow::array::{Array, ArrayRef, RecordBatch, UInt16Array, UInt32Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Maps UInt32 IDs from parquet to UInt16 IDs for OTAP using batch normalization
/// Each batch is normalized to start at 0, fitting within UInt16 space
#[derive(Debug)]
pub struct IdMapper {
    /// The starting ID for this batch (will be subtracted from all IDs)
    batch_start_id: u32,
}

impl IdMapper {
    /// Create a new ID mapper for a batch starting at min_id
    pub fn new() -> Self {
        Self {
            batch_start_id: 0,
        }
    }

    /// Set the starting ID for batch normalization
    pub fn set_batch_start(&mut self, start_id: u32) {
        self.batch_start_id = start_id;
        log::debug!("🎯 Batch normalization: will subtract {} from all IDs", start_id);
    }

    /// Normalize a UInt32 ID to UInt16 by subtracting the batch start
    fn normalize_id(&self, original_id: u32) -> Result<u16, ParquetReceiverError> {
        if original_id < self.batch_start_id {
            return Err(ParquetReceiverError::Reconstruction(
                format!("ID {} is less than batch start {}", original_id, self.batch_start_id)
            ));
        }
        
        let normalized_id = original_id - self.batch_start_id;
        if normalized_id > u16::MAX as u32 {
            return Err(ParquetReceiverError::Reconstruction(
                format!("Normalized ID {} exceeds UInt16 space", normalized_id)
            ));
        }
        
        Ok(normalized_id as u16)
    }

    /// Get the batch start ID for diagnostics
    pub fn batch_start(&self) -> u32 {
        self.batch_start_id
    }

    /// Transform a RecordBatch by converting UInt32 ID columns to UInt16 and UTF8View/BinaryView to UTF8/Binary
    pub fn transform_primary_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ParquetReceiverError> {
        let mut new_columns = Vec::with_capacity(batch.num_columns());
        let mut new_fields = Vec::with_capacity(batch.num_columns());

        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_idx);

            if field.name() == "id" && matches!(field.data_type(), DataType::UInt32) {
                // Transform the ID column from UInt32 to UInt16
                let transformed_column = self.transform_id_column(column)?;
                let new_field = Field::new("id", DataType::UInt16, field.is_nullable());
                
                new_columns.push(transformed_column);
                new_fields.push(new_field);
            } else if matches!(field.data_type(), DataType::Utf8View) {
                // Materialize UTF8View to UTF8 for OTAP compatibility
                let materialized_column = self.materialize_view_column(column, &DataType::Utf8)?;
                let new_field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
                
                new_columns.push(materialized_column);
                new_fields.push(new_field);
            } else if matches!(field.data_type(), DataType::BinaryView) {
                // Materialize BinaryView to Binary for OTAP compatibility
                let materialized_column = self.materialize_view_column(column, &DataType::Binary)?;
                let new_field = Field::new(field.name(), DataType::Binary, field.is_nullable());
                
                new_columns.push(materialized_column);
                new_fields.push(new_field);
            } else {
                // Keep other columns as-is
                new_columns.push(column.clone());
                new_fields.push(field.as_ref().clone());
            }
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        let transformed_batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;

        Ok(transformed_batch)
    }

    /// Transform a child RecordBatch by converting UInt32 parent_id columns to UInt16 and UTF8View/BinaryView to UTF8/Binary
    /// Ensures that all parent_ids in the batch have corresponding mappings for self-contained OTAP batches
    pub fn transform_child_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ParquetReceiverError> {
        let mut new_columns = Vec::with_capacity(batch.num_columns());
        let mut new_fields = Vec::with_capacity(batch.num_columns());

        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_idx);

            if field.name() == "parent_id" && matches!(field.data_type(), DataType::UInt32) {
                // Transform the parent_id column from UInt32 to UInt16
                // This method will create missing mappings to ensure self-contained batches
                let transformed_column = self.transform_parent_id_column_with_creation(column)?;
                let new_field = Field::new("parent_id", DataType::UInt16, field.is_nullable());
                
                new_columns.push(transformed_column);
                new_fields.push(new_field);
            } else if matches!(field.data_type(), DataType::Utf8View) {
                // Materialize UTF8View to UTF8 for OTAP compatibility
                let materialized_column = self.materialize_view_column(column, &DataType::Utf8)?;
                let new_field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
                
                new_columns.push(materialized_column);
                new_fields.push(new_field);
            } else if matches!(field.data_type(), DataType::BinaryView) {
                // Materialize BinaryView to Binary for OTAP compatibility
                let materialized_column = self.materialize_view_column(column, &DataType::Binary)?;
                let new_field = Field::new(field.name(), DataType::Binary, field.is_nullable());
                
                new_columns.push(materialized_column);
                new_fields.push(new_field);
            } else {
                // Keep other columns as-is
                new_columns.push(column.clone());
                new_fields.push(field.as_ref().clone());
            }
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        let transformed_batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;

        log::debug!("🔄 Child batch transformation: {} -> {} rows (should be same)", 
                   batch.num_rows(), transformed_batch.num_rows());
        Ok(transformed_batch)
    }

    /// Transform a UInt32 ID column to UInt16 using batch normalization
    fn transform_id_column(&mut self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
        let uint32_array = column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "ID column is not UInt32Array".to_string()
            ))?;

        let mut uint16_builder = UInt16Array::builder(uint32_array.len());

        for i in 0..uint32_array.len() {
            if uint32_array.is_null(i) {
                uint16_builder.append_null();
            } else {
                let original_id = uint32_array.value(i);
                let normalized_id = self.normalize_id(original_id)?;
                uint16_builder.append_value(normalized_id);
            }
        }

        let uint16_array = uint16_builder.finish();
        Ok(Arc::new(uint16_array) as ArrayRef)
    }

    /// Transform a UInt32 parent_id column to UInt16 using batch normalization
    fn transform_parent_id_column_with_creation(&mut self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
        let uint32_array = column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        let mut uint16_builder = UInt16Array::builder(uint32_array.len());

        for i in 0..uint32_array.len() {
            if uint32_array.is_null(i) {
                uint16_builder.append_null();
            } else {
                let original_parent_id = uint32_array.value(i);
                let normalized_id = self.normalize_id(original_parent_id)?;
                uint16_builder.append_value(normalized_id);
            }
        }

        let uint16_array = uint16_builder.finish();
        Ok(Arc::new(uint16_array) as ArrayRef)
    }

    /// Transform a UInt32 parent_id column to UInt16 using batch normalization (legacy method)
    fn transform_parent_id_column(&self, column: &ArrayRef) -> Result<ArrayRef, ParquetReceiverError> {
        let uint32_array = column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ParquetReceiverError::Reconstruction(
                "parent_id column is not UInt32Array".to_string()
            ))?;

        let mut uint16_builder = UInt16Array::builder(uint32_array.len());

        for i in 0..uint32_array.len() {
            if uint32_array.is_null(i) {
                uint16_builder.append_null();
            } else {
                let original_parent_id = uint32_array.value(i);
                match self.normalize_id(original_parent_id) {
                    Ok(normalized_id) => uint16_builder.append_value(normalized_id),
                    Err(_) => {
                        log::warn!("⚠️ Parent ID {} cannot be normalized with batch start {}", original_parent_id, self.batch_start_id);
                        uint16_builder.append_null();
                    }
                }
            }
        }

        let uint16_array = uint16_builder.finish();
        Ok(Arc::new(uint16_array) as ArrayRef)
    }

    /// Materialize a View column (UTF8View or BinaryView) to its standard type for OTAP compatibility
    fn materialize_view_column(&self, column: &ArrayRef, target_type: &DataType) -> Result<ArrayRef, ParquetReceiverError> {
        // Use Arrow's compute function to materialize View types to standard types
        let materialized = compute::cast(column, target_type)
            .map_err(|e| ParquetReceiverError::Arrow(e))?;
        
        let source_type_name = match column.data_type() {
            DataType::Utf8View => "UTF8View",
            DataType::BinaryView => "BinaryView",
            _ => "unknown view type",
        };
        
        let target_type_name = match target_type {
            DataType::Utf8 => "UTF8",
            DataType::Binary => "Binary",
            _ => "unknown target type",
        };
        
        log::debug!("🔄 Materialized {} column to {} ({} rows)", source_type_name, target_type_name, column.len());
        Ok(materialized)
    }
}

impl Default for IdMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_id_mapping() {
        let mut mapper = IdMapper::new();
        
        // Test basic mapping
        assert_eq!(mapper.map_id(100).unwrap(), 0);
        assert_eq!(mapper.map_id(200).unwrap(), 1);
        assert_eq!(mapper.map_id(100).unwrap(), 0); // Should return same mapping
        
        assert_eq!(mapper.mapping_count(), 2);
    }

    #[test]
    fn test_batch_transformation() {
        let mut mapper = IdMapper::new();
        
        // Create test batch with UInt32 ID column
        let id_array = Arc::new(UInt32Array::from(vec![100, 200, 300]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
        ]));
        
        let batch = RecordBatch::try_new(schema, vec![id_array]).unwrap();
        
        // Transform batch
        let transformed = mapper.transform_primary_batch(&batch).unwrap();
        
        // Verify transformation
        assert_eq!(transformed.num_rows(), 3);
        assert_eq!(transformed.num_columns(), 1);
        
        let id_field = transformed.schema().field(0);
        assert_eq!(id_field.name(), "id");
        assert!(matches!(id_field.data_type(), DataType::UInt16));
    }
}