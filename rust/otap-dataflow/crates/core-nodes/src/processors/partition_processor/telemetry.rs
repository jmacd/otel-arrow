// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry definitions for the partition (split-by-key) processor.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Emitted when a batch of the target signal cannot be split (e.g. a missing or
/// malformed key column); the batch is forwarded unchanged.
pub const PARTITION_FAILED_EVENT: &str = "partition.split_failed";

/// Metrics for the partition processor.
#[metric_set(name = "processor.partition")]
#[derive(Debug, Default, Clone)]
pub struct PartitionMetrics {
    /// Batches of the target signal that were split by key.
    #[metric(unit = "{batch}")]
    pub batches: Counter<u64>,

    /// Batches of other signals forwarded unchanged.
    #[metric(unit = "{batch}")]
    pub passthrough_batches: Counter<u64>,

    /// Batches of the target signal that could not be split and were forwarded
    /// unchanged.
    #[metric(unit = "{batch}")]
    pub malformed_batches: Counter<u64>,

    /// Partition-tagged sub-batches emitted across all split batches.
    #[metric(unit = "{batch}")]
    pub sub_batches: Counter<u64>,
}
