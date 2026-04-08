// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Collection and encoding bridge for the internal metrics SDK.
//!
//! This module connects generated metric set structs to the OTAP encoding
//! pipeline. The key types are:
//!
//! - [`CollectableMetrics`]: trait implemented by generated metric set structs
//! - [`MetricsEncoder`]: pairs a precomputed schema with a snapshot encoder
//! - [`MetricSetCollector`]: atomic snapshot-encode-clear cycle

use super::precomputed::{NumberDataPointsBuilder, PrecomputedMetricSchema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

/// Trait implemented by generated metric set structs to support
/// snapshot collection.
///
/// The generated code provides `snapshot_into` to copy current values
/// into a flat array, and `clear` to reset for the next interval.
pub trait CollectableMetrics: Send + 'static {
    /// Copy current aggregated values into `dest`.
    /// The slice length must match the schema's `total_points`.
    fn snapshot_into(&self, dest: &mut [u64]);

    /// Reset all aggregated values for the next collection interval.
    fn clear(&mut self);

    /// Returns `true` if any value is non-zero (worth collecting).
    fn needs_flush(&self) -> bool;
}

/// Encodes metric snapshots into NumberDataPoints RecordBatches.
///
/// Holds the precomputed schema (metrics + attributes tables) and
/// a reusable data points builder. On each collection tick:
/// 1. Snapshot the metric set into a flat value array
/// 2. Encode values into a NumberDataPoints batch
/// 3. Return the three-table tuple for OTAP assembly
pub struct MetricsEncoder {
    precomputed: PrecomputedMetricSchema,
    snapshot_buf: Vec<u64>,
}

impl MetricsEncoder {
    /// Create an encoder from a precomputed schema.
    #[must_use]
    pub fn new(precomputed: PrecomputedMetricSchema) -> Self {
        let n = precomputed.total_rows;
        Self {
            precomputed,
            snapshot_buf: vec![0u64; n],
        }
    }

    /// Encode a snapshot into OTAP Arrow tables.
    ///
    /// Returns `(metrics_batch, attrs_batch, ndp_batch)` or `None`
    /// if the snapshot is all zeros.
    pub fn encode(
        &mut self,
        metrics: &dyn CollectableMetrics,
        start_time_ns: i64,
        time_ns: i64,
    ) -> Result<Option<EncodedMetrics>, ArrowError> {
        if !metrics.needs_flush() {
            return Ok(None);
        }

        // Snapshot into reusable buffer.
        metrics.snapshot_into(&mut self.snapshot_buf);

        // Check if all zeros after snapshot (belt-and-suspenders).
        if self.snapshot_buf.iter().all(|&v| v == 0) {
            return Ok(None);
        }

        // Build NDP batch.
        let mut builder = NumberDataPointsBuilder::new(&self.precomputed);
        builder.set_int_values(start_time_ns, time_ns, &self.snapshot_buf);
        let ndp_batch = builder.finish()?;

        Ok(Some(EncodedMetrics {
            metrics_batch: self.precomputed.metrics_batch.clone(),
            scope_attrs_batch: self.precomputed.scope_attrs_batch.clone(),
            ndp_batch,
        }))
    }

    /// Access the precomputed schema.
    #[must_use]
    pub fn precomputed(&self) -> &PrecomputedMetricSchema {
        &self.precomputed
    }
}

/// The three OTAP Arrow tables produced by a single metric set collection.
#[derive(Debug)]
pub struct EncodedMetrics {
    /// The metrics (root) table — `num_metrics × num_scopes` rows.
    pub metrics_batch: RecordBatch,
    /// The scope attributes table — dimension values per scope.
    pub scope_attrs_batch: Option<RecordBatch>,
    /// The NumberDataPoints table — counter/gauge values.
    pub ndp_batch: RecordBatch,
}

/// Atomic snapshot-encode-clear for a single metric set.
///
/// Pairs a mutable metric set with its encoder. The `collect` method
/// performs the full cycle: snapshot → encode → clear → return encoded.
pub struct MetricSetCollector<M: CollectableMetrics> {
    /// The metric set being collected.
    pub metrics: M,
    /// The encoder that produces OTAP Arrow tables.
    pub encoder: MetricsEncoder,
}

impl<M: CollectableMetrics> MetricSetCollector<M> {
    /// Create a new collector pairing a metric set with its encoder.
    #[must_use]
    pub fn new(metrics: M, encoder: MetricsEncoder) -> Self {
        Self { metrics, encoder }
    }

    /// Snapshot, encode, and clear the metric set.
    ///
    /// Returns `None` if the metrics have no non-zero values.
    pub fn collect(
        &mut self,
        start_time_ns: i64,
        time_ns: i64,
    ) -> Result<Option<EncodedMetrics>, ArrowError> {
        let result = self
            .encoder
            .encode(&self.metrics, start_time_ns, time_ns)?;
        if result.is_some() {
            self.metrics.clear();
        }
        Ok(result)
    }
}

/// Trait-object-safe interface for collecting metrics from any metric set.
///
/// Used by the Internal Telemetry Receiver to collect from heterogeneous
/// metric set types without knowing their concrete type.
pub trait DynMetricSetCollector: Send {
    /// Snapshot, encode, and clear. Returns `None` if all zeros.
    fn collect(
        &mut self,
        start_time_ns: i64,
        time_ns: i64,
    ) -> Result<Option<EncodedMetrics>, ArrowError>;
}

impl<M: CollectableMetrics> DynMetricSetCollector for MetricSetCollector<M> {
    fn collect(
        &mut self,
        start_time_ns: i64,
        time_ns: i64,
    ) -> Result<Option<EncodedMetrics>, ArrowError> {
        self.collect(start_time_ns, time_ns)
    }
}
