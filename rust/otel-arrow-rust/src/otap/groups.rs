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
