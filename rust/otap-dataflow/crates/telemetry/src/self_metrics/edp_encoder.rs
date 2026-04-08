// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! ExponentialHistogramDataPoints encoder for the internal metrics SDK.
//!
//! Encodes `otel_expohisto::Histogram<N>` and `Mmsc` snapshots into OTAP
//! Arrow ExponentialHistogramDataPoints record batches. All histogram
//! levels (Basic/Mmsc, Normal/Histogram<8>, Detailed/Histogram<16>) encode
//! uniformly as EDP for consistent downstream handling.

use crate::instrument::MmscSnapshot;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use otel_expohisto::HistogramView;
use otap_df_pdata::encode::record::metrics::ExponentialHistogramDataPointsRecordBatchBuilder;

/// Encode a slice of `HistogramView<N>` into an EDP RecordBatch.
///
/// Each view produces one EDP row. The `parent_ids` slice maps each
/// histogram to its parent metric row in the precomputed metrics table.
///
/// Zero_count is computed as `stats.count - sum(bucket_counts)` in a
/// single pass over the bucket iterator.
pub fn encode_histograms<const N: usize>(
    views: &[HistogramView<'_, N>],
    parent_ids: &[u16],
    start_time_ns: i64,
    time_ns: i64,
) -> Result<RecordBatch, ArrowError> {
    debug_assert_eq!(views.len(), parent_ids.len());

    let mut builder = ExponentialHistogramDataPointsRecordBatchBuilder::new();

    for (i, (view, &parent_id)) in views.iter().zip(parent_ids).enumerate() {
        let stats = view.stats();

        builder.append_id(i as u32);
        builder.append_parent_id(parent_id);
        builder.append_start_time_unix_nano(start_time_ns);
        builder.append_time_unix_nano(time_ns);
        builder.append_count(stats.count);
        builder.append_sum(Some(stats.sum));
        builder.append_scale(view.scale());

        // Positive buckets: walk iterator, collect counts, compute zero_count.
        let positive = view.positive();
        if positive.is_empty() {
            // No buckets — all observations are zero (or histogram is empty).
            builder.append_zero_count(stats.count);
            builder.positive.append(None::<(i32, std::iter::Empty<u64>)>);
        } else {
            let offset = positive.offset();
            let counts: Vec<u64> = positive.iter().collect();
            let bucket_sum: u64 = counts.iter().sum();
            let zero_count = stats.count.saturating_sub(bucket_sum);

            builder.append_zero_count(zero_count);
            builder
                .positive
                .append(Some((offset, counts.into_iter())));
        }

        // No negative buckets for positive-only histograms.
        builder.negative.append(None::<(i32, std::iter::Empty<u64>)>);

        builder.append_flags(0);
        builder.append_min(if stats.count > 0 {
            Some(stats.min)
        } else {
            None
        });
        builder.append_max(if stats.count > 0 {
            Some(stats.max)
        } else {
            None
        });
        builder.append_zero_threshold(0.0);
    }

    builder.finish()
}

/// Encode a slice of `MmscSnapshot` into an EDP RecordBatch.
///
/// MMSC is encoded as a degenerate exponential histogram: scale=0,
/// zero bucket counts, with sum/count/min/max populated. This gives
/// uniform EDP encoding across all histogram levels.
pub fn encode_mmsc(
    snapshots: &[MmscSnapshot],
    parent_ids: &[u16],
    start_time_ns: i64,
    time_ns: i64,
) -> Result<RecordBatch, ArrowError> {
    debug_assert_eq!(snapshots.len(), parent_ids.len());

    let mut builder = ExponentialHistogramDataPointsRecordBatchBuilder::new();

    for (i, (snap, &parent_id)) in snapshots.iter().zip(parent_ids).enumerate() {
        builder.append_id(i as u32);
        builder.append_parent_id(parent_id);
        builder.append_start_time_unix_nano(start_time_ns);
        builder.append_time_unix_nano(time_ns);
        builder.append_count(snap.count);
        builder.append_sum(Some(snap.sum));
        builder.append_scale(0);
        builder.append_zero_count(snap.count); // All in "zero" — no buckets.
        builder.positive.append(None::<(i32, std::iter::Empty<u64>)>);
        builder.negative.append(None::<(i32, std::iter::Empty<u64>)>);
        builder.append_flags(0);
        builder.append_min(if snap.count > 0 {
            Some(snap.min)
        } else {
            None
        });
        builder.append_max(if snap.count > 0 {
            Some(snap.max)
        } else {
            None
        });
        builder.append_zero_threshold(0.0);
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_expohisto::Histogram;

    #[test]
    fn encode_empty_histogram() {
        let hist: Histogram<8> = Histogram::new();
        let views = [hist.view()];
        let parent_ids = [0u16];

        let batch = encode_histograms(&views, &parent_ids, 100, 200).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn encode_histogram_with_values() {
        let mut hist: Histogram<8> = Histogram::new();
        hist.update(1.5).unwrap();
        hist.update(2.7).unwrap();
        hist.update(100.0).unwrap();

        let views = [hist.view()];
        let parent_ids = [0u16];

        let batch = encode_histograms(&views, &parent_ids, 100, 200).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // The batch should have columns for count, sum, scale, etc.
        assert!(batch.num_columns() > 5);
    }

    #[test]
    fn encode_mmsc_snapshot() {
        let snap = MmscSnapshot {
            min: 1.0,
            max: 10.0,
            sum: 15.0,
            count: 3,
        };
        let parent_ids = [0u16];

        let batch = encode_mmsc(&[snap], &parent_ids, 100, 200).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn encode_multiple_histograms() {
        let mut h1: Histogram<8> = Histogram::new();
        h1.update(5.0).unwrap();

        let mut h2: Histogram<8> = Histogram::new();
        h2.update(10.0).unwrap();
        h2.update(20.0).unwrap();

        let h3: Histogram<8> = Histogram::new(); // empty

        let views = [h1.view(), h2.view(), h3.view()];
        let parent_ids = [0u16, 1, 2];

        let batch = encode_histograms(&views, &parent_ids, 0, 1000).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }
}
