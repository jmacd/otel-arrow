// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Split-by-key: partition an OTAP batch by a per-row key.
//!
//! This is **Layer A** of the partition-dispatch design
//! (`docs/durable-dispatch-topic-design.md`, decisions D18/D19/D25): the
//! durability-independent foundation of the vertically-integrated ingest queue's
//! shuffle. Given an OTAP batch and a configured key, it produces sub-batches
//! such that every row sharing a key value lands in the same partition, tagged
//! with that partition's index.
//!
//! # Mechanism: selection-mask radix cascade, not range split
//!
//! Rows sharing a key are generally non-contiguous, so the contiguous,
//! size-based [`split`](crate::otap::transform::split) does not apply. Instead a
//! partition index is computed per *root* row, `partition = part_fn(key) & (N -
//! 1)` (power-of-two `N`), and for each present partition a boolean selection
//! mask over the root is run through [`filter_otap_batch`], which prunes the
//! child tables (attributes, data points, exemplars, events, links) by parent-id
//! integrity. This reuses exactly the cascade the admission processor's
//! `filter_metrics_time_window` already relies on (D19).
//!
//! # Per-signal partition function (ingest-queue D3)
//!
//! - [`PartitionKey::MetricName`] hashes the `name` column on the metrics root;
//!   metric names are low-cardinality and skewed, so hashing is what spreads
//!   them across partitions.
//! - [`PartitionKey::TraceId`] slices the low 56 bits (the right-most 7 bytes) of
//!   the `trace_id` column on the spans or logs root; those bits are uniform by
//!   the OpenTelemetry trace-id randomness definition, so no hash is needed and
//!   a whole trace stays in one partition.
//!
//! The key column is always on the **root** table, so the root-to-child cascade
//! in [`filter_otap_batch`] applies directly.
//!
//! # Preconditions
//!
//! Transport-optimized (delta-encoded) ids must be decoded before calling
//! [`partition_otap_batch`] (see
//! [`OtapArrowRecords::decode_transport_optimized_ids`]), because the cascade
//! relies on plain parent/child `id` integrity.
//!
//! # Scope
//!
//! For the in-memory core (D28) `N` independent cascade passes (one per present
//! partition) are simple and correct; a single scatter pass that distributes
//! rows into `N` builders in one traversal is the deferred optimization (D19).

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use arrow::array::BooleanArray;

use crate::arrays::{FixedSizeBinaryArrayAccessor, StringArrayAccessor, get_required_array};
use crate::otap::OtapArrowRecords;
use crate::otap::error::{Error, Result};
use crate::otap::filter::{IdBitmapPool, filter_otap_batch};
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::schema::consts;

/// Fixed seeds for the metric-name partition hash. Using constant seeds makes
/// partition assignment deterministic within a process, which is all the
/// in-memory core requires (D28). Cross-version or cross-process hash stability
/// is a concern only once durable placement lands (Layer B), at which point the
/// hash can be pinned without changing this interface.
const PARTITION_HASH_SEEDS: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

/// Length in bytes of a `trace_id` (128 bits).
const TRACE_ID_LEN: usize = 16;

/// Number of low-order `trace_id` bytes that hold the W3C randomness value: the
/// right-most 7 bytes carry the 56-bit random value used by consistent
/// probability sampling, so they are uniform by definition.
const TRACE_ID_RANDOM_BYTES: usize = 7;

/// The key by which an OTAP batch is split into partitions.
///
/// The partition function follows the key's entropy (ingest-queue D3): a skewed,
/// low-cardinality key is hashed, while an already-uniform key is sliced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionKey {
    /// Hash the metric `name` column on the metrics root (`UnivariateMetrics`
    /// or `MultivariateMetrics`).
    MetricName,
    /// Slice the low 56 bits of the `trace_id` column on the spans (`Spans`) or
    /// logs (`Logs`) root; co-locates a whole trace, and trace-correlated logs,
    /// on one partition.
    TraceId,
}

impl PartitionKey {
    /// The root column the key is read from.
    const fn column(self) -> &'static str {
        match self {
            PartitionKey::MetricName => consts::NAME,
            PartitionKey::TraceId => consts::TRACE_ID,
        }
    }

    /// Whether this key may be applied to a batch with the given root type.
    const fn accepts(self, root: ArrowPayloadType) -> bool {
        match self {
            PartitionKey::MetricName => matches!(
                root,
                ArrowPayloadType::UnivariateMetrics | ArrowPayloadType::MultivariateMetrics
            ),
            PartitionKey::TraceId => {
                matches!(root, ArrowPayloadType::Spans | ArrowPayloadType::Logs)
            }
        }
    }
}

/// Split `otap_batch` into up to `num_partitions` sub-batches by `key`, so that
/// every row sharing a key value lands in the same partition.
///
/// `num_partitions` (`N`) must be a power of two. The result holds one entry per
/// **present** partition, `(partition_index, sub_batch)`, ascending by index;
/// partitions with no rows are omitted. Each sub-batch is a complete OTAP batch
/// of the same signal type, with child tables pruned to the rows belonging to
/// the partition (parent/child `id` integrity preserved by the cascade).
///
/// A single-partition split (`N == 1`), an empty batch, or a batch with no root
/// are handled directly. A null or absent key value is routed deterministically
/// (an empty metric name hashes like any other; an absent or malformed
/// `trace_id` maps to partition 0).
///
/// `pool` is borrowed so paged-bitmap allocations are reused across calls, as in
/// [`filter_otap_batch`].
///
/// See the module documentation for the mechanism and preconditions.
pub fn partition_otap_batch(
    otap_batch: &OtapArrowRecords,
    key: PartitionKey,
    num_partitions: NonZeroUsize,
    pool: &mut IdBitmapPool,
) -> Result<Vec<(usize, OtapArrowRecords)>> {
    let n = num_partitions.get();
    if !n.is_power_of_two() {
        return Err(Error::Format {
            error: format!("num_partitions must be a power of two, got {n}"),
        });
    }

    let Some(root) = otap_batch.root_record_batch() else {
        return Ok(Vec::new());
    };
    let num_rows = root.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    // Single partition: the whole batch is partition 0 (cheap Arc-backed clone).
    if n == 1 {
        return Ok(vec![(0, otap_batch.clone())]);
    }

    let parts = root_partitions(otap_batch, key, n)?;

    // One cascade pass per distinct present partition, ascending by index.
    let present: BTreeSet<usize> = parts.iter().copied().collect();
    let mut out = Vec::with_capacity(present.len());
    for p in present {
        let selection = BooleanArray::from(parts.iter().map(|&part| part == p).collect::<Vec<_>>());
        let sub = filter_otap_batch(&selection, otap_batch, pool)?;
        out.push((p, sub));
    }
    Ok(out)
}

/// Compute the partition index of every root row of `otap_batch` under `key`.
///
/// Returns one index in `[0, n)` per root row, in row order. Errors if the key
/// does not apply to the batch's signal or the key column is missing.
fn root_partitions(
    otap_batch: &OtapArrowRecords,
    key: PartitionKey,
    n: usize,
) -> Result<Vec<usize>> {
    let root_type = otap_batch.root_payload_type();
    if !key.accepts(root_type) {
        return Err(Error::Format {
            error: format!("partition key {key:?} does not apply to a {root_type:?} batch"),
        });
    }

    let root = otap_batch
        .root_record_batch()
        .expect("root record batch present (checked by caller)");
    let num_rows = root.num_rows();
    let mask = n - 1; // n is a power of two, so this is the low log2(n) bits.
    let column = get_required_array(root, key.column())?;

    match key {
        PartitionKey::MetricName => {
            let names = StringArrayAccessor::try_new(column)?;
            let state = ahash::RandomState::with_seeds(
                PARTITION_HASH_SEEDS[0],
                PARTITION_HASH_SEEDS[1],
                PARTITION_HASH_SEEDS[2],
                PARTITION_HASH_SEEDS[3],
            );
            Ok((0..num_rows)
                .map(|i| {
                    let name = names.str_at(i).unwrap_or("");
                    (state.hash_one(name) as usize) & mask
                })
                .collect())
        }
        PartitionKey::TraceId => {
            let trace_ids = FixedSizeBinaryArrayAccessor::try_new(column, TRACE_ID_LEN as i32)?;
            Ok((0..num_rows)
                .map(|i| (trace_id_low_bits(trace_ids.slice_at(i)) as usize) & mask)
                .collect())
        }
    }
}

/// Extract the low 56 bits (the right-most [`TRACE_ID_RANDOM_BYTES`] bytes,
/// big-endian) of a `trace_id`. An absent or wrong-length id yields 0.
fn trace_id_low_bits(trace_id: Option<&[u8]>) -> u64 {
    match trace_id {
        Some(bytes) if bytes.len() == TRACE_ID_LEN => bytes[TRACE_ID_LEN - TRACE_ID_RANDOM_BYTES..]
            .iter()
            .fold(0u64, |acc, &b| (acc << 8) | u64::from(b)),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::OtlpProtoMessage;
    use crate::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use crate::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use crate::proto::opentelemetry::resource::v1::Resource;
    use crate::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData};
    use crate::testing::round_trip::otlp_to_otap;
    use std::collections::BTreeMap;

    fn nz(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("non-zero")
    }

    /// Build a metrics OTAP batch with one cumulative-sum metric per name; each
    /// metric carries two number data points, each with one attribute, so the
    /// split exercises the data-point and attribute cascade.
    fn metrics_batch(names: &[&str]) -> OtapArrowRecords {
        let metrics = names
            .iter()
            .enumerate()
            .map(|(m, name)| {
                let points = (0..2)
                    .map(|p| {
                        NumberDataPoint::build()
                            .time_unix_nano((m * 10 + p) as u64)
                            .value_int((m * 10 + p) as i64)
                            .attributes(vec![KeyValue::new("k", AnyValue::new_string("v"))])
                            .finish()
                    })
                    .collect::<Vec<_>>();
                Metric::build()
                    .name(*name)
                    .data_sum(Sum::new(AggregationTemporality::Cumulative, true, points))
                    .finish()
            })
            .collect::<Vec<_>>();

        let data = MetricsData::new(vec![ResourceMetrics::new(
            Resource::default(),
            vec![ScopeMetrics::new(
                InstrumentationScope::build().name("scope").finish(),
                metrics,
            )],
        )]);

        let mut otap = otlp_to_otap(&OtlpProtoMessage::Metrics(data));
        otap.decode_transport_optimized_ids().unwrap();
        otap
    }

    /// Build a traces OTAP batch with one span per trace id; each span carries
    /// one attribute so the split exercises the span-attribute cascade.
    fn traces_batch(trace_ids: &[u128]) -> OtapArrowRecords {
        let spans = trace_ids
            .iter()
            .enumerate()
            .map(|(i, &tid)| {
                Span::build()
                    .trace_id(u128::to_be_bytes(tid))
                    .span_id(u64::to_be_bytes(i as u64 + 1))
                    .name(format!("span{i}"))
                    .start_time_unix_nano(1u64)
                    .end_time_unix_nano(2u64)
                    .attributes(vec![KeyValue::new("k", AnyValue::new_string("v"))])
                    .finish()
            })
            .collect::<Vec<_>>();

        let data = TracesData::new(vec![ResourceSpans::new(
            Resource::default(),
            vec![ScopeSpans::new(
                InstrumentationScope::build().name("scope").finish(),
                spans,
            )],
        )]);

        let mut otap = otlp_to_otap(&OtlpProtoMessage::Traces(data));
        otap.decode_transport_optimized_ids().unwrap();
        otap
    }

    fn rows(b: &OtapArrowRecords, t: ArrowPayloadType) -> usize {
        b.get(t).map(|rb| rb.num_rows()).unwrap_or(0)
    }

    fn names_of(b: &OtapArrowRecords) -> Vec<String> {
        let root = b
            .root_record_batch()
            .expect("metrics root record batch present");
        let col = get_required_array(root, consts::NAME).unwrap();
        let acc = StringArrayAccessor::try_new(col).unwrap();
        (0..root.num_rows())
            .map(|i| acc.str_at(i).unwrap_or("").to_string())
            .collect()
    }

    #[test]
    fn test_groups_match_root_partitions_and_cascade_conserved() {
        let names = [
            "http.requests",
            "cpu.usage",
            "mem.free",
            "disk.io",
            "net.rx",
        ];
        let input = metrics_batch(&names);
        let n = nz(4);
        let mut pool = IdBitmapPool::new();

        // The per-root-row partition assignment we expect the split to follow.
        let expected = root_partitions(&input, PartitionKey::MetricName, n.get()).unwrap();
        let input_names = names_of(&input);

        let parts = partition_otap_batch(&input, PartitionKey::MetricName, n, &mut pool).unwrap();

        // Conservation: every metric row and every data point is accounted for
        // exactly once across the partitions.
        let total_root: usize = parts
            .iter()
            .map(|(_, b)| rows(b, ArrowPayloadType::UnivariateMetrics))
            .sum();
        let total_dp: usize = parts
            .iter()
            .map(|(_, b)| rows(b, ArrowPayloadType::NumberDataPoints))
            .sum();
        let total_attrs: usize = parts
            .iter()
            .map(|(_, b)| rows(b, ArrowPayloadType::NumberDpAttrs))
            .sum();
        assert_eq!(total_root, names.len());
        assert_eq!(total_dp, rows(&input, ArrowPayloadType::NumberDataPoints));
        assert_eq!(total_attrs, rows(&input, ArrowPayloadType::NumberDpAttrs));
        // Each metric has two data points, each with one attribute.
        assert_eq!(total_dp, names.len() * 2);
        assert_eq!(total_attrs, names.len() * 2);

        // Each returned partition's metric names equal exactly the input names
        // whose expected partition is that index, in input order.
        for (p, sub) in &parts {
            let want: Vec<String> = input_names
                .iter()
                .enumerate()
                .filter(|(i, _)| expected[*i] == *p)
                .map(|(_, name)| name.clone())
                .collect();
            assert!(!want.is_empty(), "a returned partition must be non-empty");
            assert_eq!(&names_of(sub), &want, "partition {p} names mismatch");
        }

        // Returned indices are ascending, distinct, and cover every expected one.
        let returned: Vec<usize> = parts.iter().map(|(p, _)| *p).collect();
        let mut sorted = returned.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(returned, sorted, "partitions ascending and distinct");
        let expected_set: BTreeSet<usize> = expected.iter().copied().collect();
        assert_eq!(returned.into_iter().collect::<BTreeSet<_>>(), expected_set);
    }

    #[test]
    fn test_same_name_colocated_in_one_partition() {
        // Names repeat across rows; every row sharing a name must land together.
        let names = ["a", "b", "a", "c", "b", "a", "d"];
        let input = metrics_batch(&names);
        let mut pool = IdBitmapPool::new();
        let parts =
            partition_otap_batch(&input, PartitionKey::MetricName, nz(8), &mut pool).unwrap();

        // Map each distinct name to the set of partitions it appears in.
        let mut name_to_parts: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
        for (p, sub) in &parts {
            for name in names_of(sub) {
                let _ = name_to_parts.entry(name).or_default().insert(*p);
            }
        }
        for (name, ps) in &name_to_parts {
            assert_eq!(ps.len(), 1, "name {name} spread across partitions {ps:?}");
        }
        // All four distinct names are present.
        assert_eq!(name_to_parts.len(), 4);
    }

    #[test]
    fn test_determinism_repeated_calls() {
        let names = ["one", "two", "three", "four", "five", "six"];
        let input = metrics_batch(&names);
        let mut pool = IdBitmapPool::new();
        let a = partition_otap_batch(&input, PartitionKey::MetricName, nz(4), &mut pool).unwrap();
        let b = partition_otap_batch(&input, PartitionKey::MetricName, nz(4), &mut pool).unwrap();
        let a_names: Vec<(usize, Vec<String>)> = a.iter().map(|(p, s)| (*p, names_of(s))).collect();
        let b_names: Vec<(usize, Vec<String>)> = b.iter().map(|(p, s)| (*p, names_of(s))).collect();
        assert_eq!(a_names, b_names);
    }

    #[test]
    fn test_single_partition_returns_whole_batch() {
        let names = ["a", "b", "c"];
        let input = metrics_batch(&names);
        let mut pool = IdBitmapPool::new();
        let parts =
            partition_otap_batch(&input, PartitionKey::MetricName, nz(1), &mut pool).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, 0);
        assert_eq!(names_of(&parts[0].1), names_of(&input));
        assert_eq!(
            rows(&parts[0].1, ArrowPayloadType::NumberDataPoints),
            rows(&input, ArrowPayloadType::NumberDataPoints),
        );
    }

    #[test]
    fn test_empty_batch_yields_no_partitions() {
        let input = metrics_batch(&[]);
        let mut pool = IdBitmapPool::new();
        let parts =
            partition_otap_batch(&input, PartitionKey::MetricName, nz(4), &mut pool).unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn test_non_power_of_two_is_rejected() {
        let input = metrics_batch(&["a"]);
        let mut pool = IdBitmapPool::new();
        let err = partition_otap_batch(&input, PartitionKey::MetricName, nz(3), &mut pool)
            .expect_err("non-power-of-two N must error");
        assert!(format!("{err:?}").contains("power of two"), "got {err:?}");
    }

    #[test]
    fn test_key_signal_mismatch_is_rejected() {
        let mut pool = IdBitmapPool::new();
        let metrics = metrics_batch(&["a"]);
        assert!(
            partition_otap_batch(&metrics, PartitionKey::TraceId, nz(4), &mut pool).is_err(),
            "TraceId key on a metrics batch must error"
        );
        let traces = traces_batch(&[1]);
        assert!(
            partition_otap_batch(&traces, PartitionKey::MetricName, nz(4), &mut pool).is_err(),
            "MetricName key on a traces batch must error"
        );
    }

    #[test]
    fn test_partition_traces_by_trace_id_low_bits() {
        // With N=4 (mask 0b11), the partition is the low two bits of the id.
        let ids: [u128; 6] = [1, 2, 3, 4, 5, 8];
        let input = traces_batch(&ids);
        let mut pool = IdBitmapPool::new();
        let parts = partition_otap_batch(&input, PartitionKey::TraceId, nz(4), &mut pool).unwrap();

        // Conservation across the span-attribute cascade.
        let total_spans: usize = parts
            .iter()
            .map(|(_, b)| rows(b, ArrowPayloadType::Spans))
            .sum();
        let total_attrs: usize = parts
            .iter()
            .map(|(_, b)| rows(b, ArrowPayloadType::SpanAttrs))
            .sum();
        assert_eq!(total_spans, ids.len());
        assert_eq!(total_attrs, rows(&input, ArrowPayloadType::SpanAttrs));

        // The split must follow the trace_id low-bit slice.
        let expected = root_partitions(&input, PartitionKey::TraceId, 4).unwrap();
        let by_part = parts
            .iter()
            .map(|(p, b)| (*p, rows(b, ArrowPayloadType::Spans)))
            .collect::<BTreeMap<_, _>>();
        let mut expected_counts: BTreeMap<usize, usize> = BTreeMap::new();
        for e in &expected {
            *expected_counts.entry(*e).or_default() += 1;
        }
        assert_eq!(by_part, expected_counts);
        // ids 1,5 -> 1; 2 -> 2; 3 -> 3; 4,8 -> 0. So partition 1 has two spans.
        assert_eq!(by_part.get(&1).copied(), Some(2));
        assert_eq!(by_part.get(&0).copied(), Some(2));
    }

    #[test]
    fn test_trace_id_low_bits_helper() {
        assert_eq!(trace_id_low_bits(None), 0);
        assert_eq!(trace_id_low_bits(Some(&[0u8; 8])), 0); // wrong length
        // Right-most 7 bytes are the value; high 9 bytes are ignored.
        let id = u128::to_be_bytes(0xAABB_CCDD_u128 << 96 | 0x00FF_FFFF_FFFF_FFFF_u128);
        assert_eq!(trace_id_low_bits(Some(&id)), 0x00FF_FFFF_FFFF_FFFF);
        let id2 = u128::to_be_bytes(0x42);
        assert_eq!(trace_id_low_bits(Some(&id2)), 0x42);
    }
}
