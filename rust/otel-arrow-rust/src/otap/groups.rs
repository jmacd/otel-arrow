// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Support for rebatching sequences of `OtapArrowRecords`.
//!
//! # Current Implementation Status
//!
//! This module currently supports batching for **Logs only**.
//!
//! The rebatching approach:
//! - Takes a sequence of input batches and produces maximally-full output batches in a single pass
//! - Piece-by-piece processing: consumes chunks from input, builds maximally-full output batches
//! - Primary table (Logs) drives the batching
//! - Secondary tables (LogAttrs, ScopeAttrs, ResourceAttrs) follow via PARENT_ID references
//! - Resource/Scope attributes are deduplicated within each output batch
//! - Respects the u16::MAX constraint for ID columns
//!
//! ## Algorithm Overview
//!
//! For each output batch:
//! 1. Consume N logs from input (up to max_batch_size)
//! 2. Extract unique resource IDs and scope IDs from those logs
//! 3. Gather corresponding resource/scope attribute rows
//! 4. For logs with attributes (non-NULL log.id): gather log attribute rows
//! 5. Reindex all IDs to be sequential starting from 0
//! 6. Emit the output batch
//!
//! ## Metrics and Traces
//!
//! Batching for Metrics and Traces signals is **not yet implemented**.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, UInt16Array};
use arrow::compute;
use snafu::{IntoError, ResultExt};

use crate::{
    error::{self, Result},
    otap::{
        Logs, Metrics, OtapArrowRecordTag, OtapArrowRecords, OtapBatchStore, POSITION_LOOKUP,
        Traces,
    },
    proto::opentelemetry::arrow::v1::ArrowPayloadType,
    schema::consts,
};

/// A sequence of OtapArrowRecords that all share exactly the same tag.
/// Maintains invariant: primary table for each batch is not None and has more than zero records.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordsGroup {
    /// A sequence of batches representing log data
    Logs(Vec<[Option<RecordBatch>; Logs::COUNT]>),
    /// A sequence of batches representing metric data
    Metrics(Vec<[Option<RecordBatch>; Metrics::COUNT]>),
    /// A sequence of batches representing span data
    Traces(Vec<[Option<RecordBatch>; Traces::COUNT]>),
}

impl RecordsGroup {
    /// Convert a sequence of `OtapArrowRecords` into three `RecordsGroup` objects
    #[must_use]
    fn separate_by_type(records: Vec<OtapArrowRecords>) -> [Self; 3] {
        let log_count = tag_count(&records, OtapArrowRecordTag::Logs);
        let mut log_records = Vec::with_capacity(log_count);

        let metric_count = tag_count(&records, OtapArrowRecordTag::Metrics);
        let mut metric_records = Vec::with_capacity(metric_count);

        let trace_count = tag_count(&records, OtapArrowRecordTag::Traces);
        let mut trace_records = Vec::with_capacity(trace_count);

        for records in records {
            match records {
                OtapArrowRecords::Logs(logs) => {
                    let batches = logs.into_batches();
                    if primary_table(&batches)
                        .map(|batch| batch.num_rows() > 0)
                        .unwrap_or(false)
                    {
                        log_records.push(batches);
                    }
                }
                OtapArrowRecords::Metrics(metrics) => {
                    let batches = metrics.into_batches();
                    if primary_table(&batches)
                        .map(|batch| batch.num_rows() > 0)
                        .unwrap_or(false)
                    {
                        metric_records.push(batches);
                    }
                }
                OtapArrowRecords::Traces(traces) => {
                    let batches = traces.into_batches();
                    if primary_table(&batches)
                        .map(|batch| batch.num_rows() > 0)
                        .unwrap_or(false)
                    {
                        trace_records.push(batches);
                    }
                }
            }
        }

        [
            RecordsGroup::Logs(log_records),
            RecordsGroup::Metrics(metric_records),
            RecordsGroup::Traces(trace_records),
        ]
    }

    /// Separate expecting only logs
    pub fn separate_logs(records: Vec<OtapArrowRecords>) -> Result<Self> {
        let [logs, metrics, traces] = RecordsGroup::separate_by_type(records);
        if !metrics.is_empty() || !traces.is_empty() {
            Err(error::MixedSignalsSnafu.build())
        } else {
            Ok(logs)
        }
    }

    /// Separate expecting only metrics
    pub fn separate_metrics(records: Vec<OtapArrowRecords>) -> Result<Self> {
        let [logs, metrics, traces] = RecordsGroup::separate_by_type(records);
        if !logs.is_empty() || !traces.is_empty() {
            Err(error::MixedSignalsSnafu.build())
        } else {
            Ok(metrics)
        }
    }

    /// Separate expecting only traces
    pub fn separate_traces(records: Vec<OtapArrowRecords>) -> Result<Self> {
        let [logs, metrics, traces] = RecordsGroup::separate_by_type(records);
        if !logs.is_empty() || !metrics.is_empty() {
            Err(error::MixedSignalsSnafu.build())
        } else {
            Ok(traces)
        }
    }

    /// Rebatch records in a single pass, creating maximally-full output batches.
    ///
    /// Iterates through input batches once, building output batches that are as close
    /// to `max_output_batch` in size as possible (or u16::MAX if no limit specified).
    pub fn rebatch(self, max_output_batch: Option<NonZeroU64>) -> Result<Self> {
        let effective_max =
            max_output_batch.unwrap_or_else(|| NonZeroU64::new(u16::MAX as u64).unwrap());

        Ok(match self {
            RecordsGroup::Logs(items) => {
                RecordsGroup::Logs(rebatch_logs_single_pass(items, effective_max)?)
            }
            RecordsGroup::Metrics(_) => {
                unimplemented!("Metrics batching is not yet implemented")
            }
            RecordsGroup::Traces(_) => {
                unimplemented!("Traces batching is not yet implemented")
            }
        })
    }

    /// Convert into a sequence of `OtapArrowRecords`
    #[must_use]
    pub fn into_otap_arrow_records(self) -> Vec<OtapArrowRecords> {
        match self {
            RecordsGroup::Logs(items) => items
                .into_iter()
                .map(|batches| OtapArrowRecords::Logs(Logs { batches }))
                .collect(),
            RecordsGroup::Metrics(items) => items
                .into_iter()
                .map(|batches| OtapArrowRecords::Metrics(Metrics { batches }))
                .collect(),
            RecordsGroup::Traces(items) => items
                .into_iter()
                .map(|batches| OtapArrowRecords::Traces(Traces { batches }))
                .collect(),
        }
    }

    /// Is the container empty?
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        match self {
            Self::Logs(logs) => logs.is_empty(),
            Self::Metrics(metrics) => metrics.is_empty(),
            Self::Traces(traces) => traces.is_empty(),
        }
    }

    /// Find the number of OtapArrowRecords we've got.
    #[must_use]
    pub const fn len(&self) -> usize {
        match self {
            Self::Logs(logs) => logs.len(),
            Self::Metrics(metrics) => metrics.len(),
            Self::Traces(traces) => traces.len(),
        }
    }
}

// Helper functions
// *************************************************************************************************

fn tag_count(records: &[OtapArrowRecords], tag: OtapArrowRecordTag) -> usize {
    records
        .iter()
        .map(|records| (records.tag() == tag) as usize)
        .sum()
}

/// Fetch the primary table for a given batch
#[must_use]
fn primary_table<const N: usize>(batches: &[Option<RecordBatch>; N]) -> Option<&RecordBatch> {
    match N {
        Logs::COUNT => batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]].as_ref(),
        Metrics::COUNT => {
            batches[POSITION_LOOKUP[ArrowPayloadType::UnivariateMetrics as usize]].as_ref()
        }
        Traces::COUNT => batches[POSITION_LOOKUP[ArrowPayloadType::Spans as usize]].as_ref(),
        _ => {
            unreachable!()
        }
    }
}

// Single-pass rebatching implementation for Logs
// *************************************************************************************************

/// Rebatch logs in a single pass by moving pieces from input to output batches.
///
/// Algorithm:
/// 1. Verify/ensure resource attrs and scope attrs are sorted by PARENT_ID
/// 2. For each output batch (up to max_batch_size logs):
///    a. Consume next chunk of logs from input
///    b. Extract unique resource.id and scope.id values
///    c. Gather corresponding resource/scope attribute rows
///    d. For logs with attributes: gather log attribute rows
///    e. Reindex all IDs sequentially
///    f. Build output batch
/// 3. Emit maximally-full output batches
fn rebatch_logs_single_pass<const N: usize>(
    input_batches: Vec<[Option<RecordBatch>; N]>,
    max_batch_size: NonZeroU64,
) -> Result<Vec<[Option<RecordBatch>; N]>> {
    if input_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Step 1: Concatenate all input batches into single batches per table
    let concatenated = concatenate_input_batches(&input_batches)?;

    // Step 2: Verify sorting and extract table information
    let table_info = extract_table_info(&concatenated)?;

    // Step 3: Process piece-by-piece, creating output batches
    let output_batches = process_pieces(concatenated, table_info, max_batch_size)?;

    Ok(output_batches)
}

/// Concatenate all input batches into single batches per table
fn concatenate_input_batches<const N: usize>(
    input_batches: &[[Option<RecordBatch>; N]],
) -> Result<[Option<RecordBatch>; N]> {
    let mut result: [Option<RecordBatch>; N] = [const { None }; N];

    for table_idx in 0..N {
        let mut batches_for_table = Vec::new();

        for input in input_batches {
            if let Some(batch) = &input[table_idx] {
                batches_for_table.push(batch.clone());
            }
        }

        if !batches_for_table.is_empty() {
            // @@@ Batches can have different schemas, need to unify
            let schema = batches_for_table[0].schema();
            result[table_idx] = Some(
                compute::concat_batches(&schema, &batches_for_table)
                    .context(error::BatchingSnafu)?,
            );
        }
    }

    Ok(result)
}

/// Information about the tables we're processing
struct TableInfo {
    // @@@ The _idx fields are all constants, there are only three options here.
    // this feels awkward. would prefer to see the four _idx as const declarations,
    // and three booleans instead of this struct's four usize.
    logs_idx: usize,
    log_attrs_idx: Option<usize>,
    resource_attrs_idx: Option<usize>,
    scope_attrs_idx: Option<usize>,
}

/// Extract table indices and validate structure
fn extract_table_info<const N: usize>(batches: &[Option<RecordBatch>; N]) -> Result<TableInfo> {
    let logs_idx = POSITION_LOOKUP[ArrowPayloadType::Logs as usize];
    let log_attrs_idx = POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize];
    let resource_attrs_idx = POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize];
    let scope_attrs_idx = POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize];

    Ok(TableInfo {
        logs_idx,
        log_attrs_idx: if batches[log_attrs_idx].is_some() {
            Some(log_attrs_idx)
        } else {
            None
        },
        resource_attrs_idx: if batches[resource_attrs_idx].is_some() {
            Some(resource_attrs_idx)
        } else {
            None
        },
        scope_attrs_idx: if batches[scope_attrs_idx].is_some() {
            Some(scope_attrs_idx)
        } else {
            None
        },
    })
}

/// Process batches piece-by-piece
fn process_pieces<const N: usize>(
    batches: [Option<RecordBatch>; N],
    info: TableInfo,
    max_batch_size: NonZeroU64,
) -> Result<Vec<[Option<RecordBatch>; N]>> {
    let mut output_batches = Vec::new();

    // Get the primary logs batch
    let Some(logs_batch) = &batches[info.logs_idx] else {
        return Ok(output_batches);
    };

    let total_logs = logs_batch.num_rows();
    if total_logs == 0 {
        return Ok(output_batches);
    }

    let max_size = max_batch_size.get() as usize;
    let mut offset = 0;

    // Process piece by piece
    while offset < total_logs {
        let chunk_size = (total_logs - offset).min(max_size);

        // Build one output batch for this piece
        let output_batch = build_output_batch_for_piece(&batches, &info, offset, chunk_size)?;

        output_batches.push(output_batch);
        offset += chunk_size;
    }

    Ok(output_batches)
}

/// Build a single output batch for a piece of logs
fn build_output_batch_for_piece<const N: usize>(
    batches: &[Option<RecordBatch>; N],
    info: &TableInfo,
    offset: usize,
    length: usize,
) -> Result<[Option<RecordBatch>; N]> {
    let mut output: [Option<RecordBatch>; N] = [const { None }; N];

    // Get the logs batch and slice it
    let logs_batch = batches[info.logs_idx].as_ref().unwrap();
    let logs_slice = logs_batch.slice(offset, length);

    // Extract resource and scope IDs from the sliced logs
    let resource_ids = extract_resource_ids(&logs_slice)?;
    let scope_ids = extract_scope_ids(&logs_slice)?;

    // Find unique resource and scope IDs
    let unique_resource_ids = find_unique_ids(&resource_ids);
    let unique_scope_ids = find_unique_ids(&scope_ids);

    // Build ID mappings (old ID -> new sequential ID)
    let resource_id_map = build_id_mapping(&unique_resource_ids);
    let scope_id_map = build_id_mapping(&unique_scope_ids);

    // Process resource attributes if present
    if let Some(idx) = info.resource_attrs_idx {
        if let Some(resource_attrs) = &batches[idx] {
            output[idx] = Some(gather_and_reindex_attrs(
                resource_attrs,
                &unique_resource_ids,
            )?);
        }
    }

    // Process scope attributes if present
    if let Some(idx) = info.scope_attrs_idx {
        if let Some(scope_attrs) = &batches[idx] {
            output[idx] = Some(gather_and_reindex_attrs(scope_attrs, &unique_scope_ids)?);
        }
    }

    // Process log attributes if present
    if let Some(idx) = info.log_attrs_idx {
        if let Some(log_attrs) = &batches[idx] {
            // Extract log IDs from the slice to find which have attributes
            let log_ids = extract_log_ids(&logs_slice)?;
            output[idx] = Some(gather_log_attrs(log_attrs, &log_ids, offset, length)?);
        }
    }

    // Reindex the logs themselves and place in output
    output[info.logs_idx] = Some(reindex_logs(&logs_slice, &resource_id_map, &scope_id_map)?);

    Ok(output)
}

/// Extract log.id values from a logs batch (may be NULL)
fn extract_log_ids(logs_batch: &RecordBatch) -> Result<Arc<UInt16Array>> {
    let id_column = logs_batch
        .column_by_name(consts::ID)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: consts::ID.to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::ID.to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    Ok(Arc::new(id_column.clone()))
}

/// Gather attribute rows for the given unique IDs and reindex PARENT_ID
fn gather_and_reindex_attrs(attrs_batch: &RecordBatch, unique_ids: &[u16]) -> Result<RecordBatch> {
    // Get PARENT_ID column
    let parent_id_col = attrs_batch
        .column_by_name(consts::PARENT_ID)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: consts::PARENT_ID.to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::PARENT_ID.to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    // Build mask: which rows have PARENT_ID in unique_ids
    let id_set: HashSet<u16> = unique_ids.iter().copied().collect();
    let mut indices = Vec::new();
    let id_to_new: HashMap<u16, u16> = unique_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i as u16))
        .collect();

    for row_idx in 0..parent_id_col.len() {
        if parent_id_col.is_valid(row_idx) {
            let parent_id = parent_id_col.value(row_idx);
            if id_set.contains(&parent_id) {
                indices.push(row_idx as u32);
            }
        }
    }

    // Gather the rows
    let indices_array = arrow::array::UInt32Array::from(indices);
    let gathered =
        compute::take_record_batch(attrs_batch, &indices_array).context(error::BatchingSnafu)?;

    // Reindex PARENT_ID column
    let old_parent_ids = gathered
        .column_by_name(consts::PARENT_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();

    let new_parent_ids: UInt16Array = old_parent_ids
        .iter()
        .map(|opt_id| opt_id.map(|id| *id_to_new.get(&id).unwrap()))
        .collect();

    // Replace PARENT_ID column in the batch
    let schema = gathered.schema();
    let parent_id_idx = schema
        .index_of(consts::PARENT_ID)
        .map_err(|e| error::BatchingSnafu.into_error(e))?;

    let mut columns: Vec<ArrayRef> = gathered.columns().to_vec();
    columns[parent_id_idx] = Arc::new(new_parent_ids);

    RecordBatch::try_new(schema, columns).context(error::BatchingSnafu)
}

/// Gather log attributes for logs in the given range
fn gather_log_attrs(
    log_attrs_batch: &RecordBatch,
    log_ids: &UInt16Array,
    _offset: usize,
    _length: usize,
) -> Result<RecordBatch> {
    // Get PARENT_ID column from log attributes
    let parent_id_col = log_attrs_batch
        .column_by_name(consts::PARENT_ID)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: consts::PARENT_ID.to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::PARENT_ID.to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    // Find min/max non-NULL log IDs to determine range
    let mut min_id = u16::MAX;
    let mut max_id = 0u16;
    let mut has_ids = false;

    for i in 0..log_ids.len() {
        if log_ids.is_valid(i) {
            let id = log_ids.value(i);
            min_id = min_id.min(id);
            max_id = max_id.max(id);
            has_ids = true;
        }
    }

    if !has_ids {
        // No log IDs means no log attributes
        let empty = RecordBatch::new_empty(log_attrs_batch.schema());
        return Ok(empty);
    }

    // Gather rows where PARENT_ID is in [min_id, max_id]
    let mut indices = Vec::new();
    for row_idx in 0..parent_id_col.len() {
        if parent_id_col.is_valid(row_idx) {
            let parent_id = parent_id_col.value(row_idx);
            if parent_id >= min_id && parent_id <= max_id {
                indices.push(row_idx as u32);
            }
        }
    }

    if indices.is_empty() {
        let empty = RecordBatch::new_empty(log_attrs_batch.schema());
        return Ok(empty);
    }

    // Gather and reindex sequentially starting from 0
    let indices_array = arrow::array::UInt32Array::from(indices);
    let gathered = compute::take_record_batch(log_attrs_batch, &indices_array)
        .context(error::BatchingSnafu)?;

    // Reindex PARENT_ID: subtract min_id to start from 0
    let old_parent_ids = gathered
        .column_by_name(consts::PARENT_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();

    let new_parent_ids: UInt16Array = old_parent_ids
        .iter()
        .map(|opt_id| opt_id.map(|id| id - min_id))
        .collect();

    let schema = gathered.schema();
    let parent_id_idx = schema
        .index_of(consts::PARENT_ID)
        .map_err(|e| error::BatchingSnafu.into_error(e))?;

    let mut columns: Vec<ArrayRef> = gathered.columns().to_vec();
    columns[parent_id_idx] = Arc::new(new_parent_ids);

    RecordBatch::try_new(schema, columns).context(error::BatchingSnafu)
}

/// Reindex the logs batch: apply resource/scope ID mappings
fn reindex_logs(
    logs_slice: &RecordBatch,
    resource_id_map: &HashMap<u16, u16>,
    scope_id_map: &HashMap<u16, u16>,
) -> Result<RecordBatch> {
    let schema = logs_slice.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logs_slice.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let column = logs_slice.column(idx);

        match field.name().as_str() {
            consts::RESOURCE => {
                columns.push(reindex_struct_id(column, resource_id_map)?);
            }
            consts::SCOPE => {
                columns.push(reindex_struct_id(column, scope_id_map)?);
            }
            consts::ID => {
                // Reindex log IDs sequentially starting from 0
                columns.push(reindex_log_ids(column)?);
            }
            _ => {
                columns.push(column.clone());
            }
        }
    }

    RecordBatch::try_new(schema, columns).context(error::BatchingSnafu)
}

/// Reindex a struct column's id field using the provided mapping
fn reindex_struct_id(struct_column: &ArrayRef, id_map: &HashMap<u16, u16>) -> Result<ArrayRef> {
    let struct_array = struct_column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: "struct".to_string(),
                expect: arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::empty()),
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    let id_idx = struct_array.column_by_name(consts::ID).ok_or_else(|| {
        error::ColumnNotFoundSnafu {
            name: "struct.id".to_string(),
        }
        .build()
    })?;

    let old_ids = id_idx
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: "struct.id".to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    // Map old IDs to new IDs
    let new_ids: UInt16Array = old_ids
        .iter()
        .map(|opt_id| opt_id.map(|id| *id_map.get(&id).unwrap()))
        .collect();

    // Rebuild struct with new ID column
    let mut columns: Vec<(Arc<arrow::datatypes::Field>, ArrayRef)> = Vec::new();
    for (field, column) in struct_array.fields().iter().zip(struct_array.columns()) {
        if field.name() == consts::ID {
            columns.push((field.clone(), Arc::new(new_ids.clone())));
        } else {
            columns.push((field.clone(), column.clone()));
        }
    }

    Ok(Arc::new(StructArray::from(columns)))
}

/// Reindex log IDs sequentially starting from 0 (preserving NULLs)
fn reindex_log_ids(id_column: &ArrayRef) -> Result<ArrayRef> {
    let old_ids = id_column
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::ID.to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    // Reindex sequentially: 0, 1, 2, ... (or NULL if input is NULL)
    let new_ids: UInt16Array = (0..old_ids.len())
        .map(|i| {
            if old_ids.is_valid(i) {
                Some(i as u16)
            } else {
                None
            }
        })
        .collect();

    Ok(Arc::new(new_ids))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebatch_empty() {
        let input: Vec<[Option<RecordBatch>; 16]> = Vec::new();
        let result = rebatch_logs_single_pass(input, NonZeroU64::new(100).unwrap());
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_find_unique_ids() {
        let ids = UInt16Array::from(vec![5, 3, 5, 7, 3, 7]);
        let unique = find_unique_ids(&ids);
        assert_eq!(unique, vec![3, 5, 7]);
    }

    #[test]
    fn test_build_id_mapping() {
        let ids = vec![10, 20, 30];
        let mapping = build_id_mapping(&ids);
        assert_eq!(mapping.get(&10), Some(&0));
        assert_eq!(mapping.get(&20), Some(&1));
        assert_eq!(mapping.get(&30), Some(&2));
    }
}

/// Extract resource.id values from a logs batch
fn extract_resource_ids(logs_batch: &RecordBatch) -> Result<Arc<UInt16Array>> {
    let resource_struct = logs_batch
        .column_by_name(consts::RESOURCE)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: consts::RESOURCE.to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::RESOURCE.to_string(),
                expect: arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::empty()),
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    let id_column = resource_struct
        .column_by_name(consts::ID)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: "resource.id".to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: "resource.id".to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    Ok(Arc::new(id_column.clone()))
}

/// Extract scope.id values from a logs batch
fn extract_scope_ids(logs_batch: &RecordBatch) -> Result<Arc<UInt16Array>> {
    let scope_struct = logs_batch
        .column_by_name(consts::SCOPE)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: consts::SCOPE.to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: consts::SCOPE.to_string(),
                expect: arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::empty()),
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    let id_column = scope_struct
        .column_by_name(consts::ID)
        .ok_or_else(|| {
            error::ColumnNotFoundSnafu {
                name: "scope.id".to_string(),
            }
            .build()
        })?
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| {
            error::ColumnDataTypeMismatchSnafu {
                name: "scope.id".to_string(),
                expect: arrow::datatypes::DataType::UInt16,
                actual: arrow::datatypes::DataType::Null,
            }
            .build()
        })?;

    Ok(Arc::new(id_column.clone()))
}

/// Find unique IDs in a UInt16Array
fn find_unique_ids(ids: &UInt16Array) -> Vec<u16> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();

    for i in 0..ids.len() {
        if ids.is_valid(i) {
            let id = ids.value(i);
            if seen.insert(id) {
                unique.push(id);
            }
        }
    }

    unique.sort_unstable();
    unique
}

/// Build a mapping from old IDs to new sequential IDs
fn build_id_mapping(old_ids: &[u16]) -> HashMap<u16, u16> {
    old_ids
        .iter()
        .enumerate()
        .map(|(new_id, &old_id)| (old_id, new_id as u16))
        .collect()
}
