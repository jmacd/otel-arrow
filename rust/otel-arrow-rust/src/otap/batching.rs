// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Batching for `OtapArrowRecords`
//!
//!

use super::{OtapArrowRecordTag, OtapArrowRecords, error::Result, groups::RecordsGroup};
use std::num::NonZeroU64;

/// merge and combine batches to the appropriate size
/// error if not the same signal type.
pub fn make_output_batches(
    signal: OtapArrowRecordTag,
    records: Vec<OtapArrowRecords>,
    max_size: Option<NonZeroU64>,
) -> Result<Vec<OtapArrowRecords>> {
    // We have to deal with two complications here:
    // * batches that are too small
    // * batches that are too big
    // We presume batches are a single type at this level.
    // We do not specify sort order.
    // Note otap_batch_processor does this.

    for r in records.iter() {
        println!("batch input {}", r.batch_length());
    }

    let mut records = match signal {
        OtapArrowRecordTag::Logs => RecordsGroup::separate_logs(records),
        OtapArrowRecordTag::Metrics => RecordsGroup::separate_metrics(records),
        OtapArrowRecordTag::Traces => RecordsGroup::separate_traces(records),
    }?;

    if let Some(limit) = max_size {
        records = records.split(limit)?;
    }
    records = records.concatenate(max_size)?;

    Ok(records.into_otap_arrow_records())
}
