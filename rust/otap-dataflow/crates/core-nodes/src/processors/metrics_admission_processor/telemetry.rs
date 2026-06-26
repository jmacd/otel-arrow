// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry definitions for the metrics admission processor.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Emitted once when a metric name is first observed to carry a conflicting type
/// descriptor (a different instrument type, monotonicity, or unit than the
/// recorded primary).
pub const NAME_CONFLICT_EVENT: &str = "metrics_admission.name_conflict";

/// Emitted when an incoming metrics batch cannot be read as an OTAP metrics
/// view, so its identities cannot be recorded.
pub const VIEW_CREATION_FAILED_EVENT: &str = "metrics_admission.view.creation_failed";

/// Emitted when the event-time window filter fails on a batch (e.g. a malformed
/// timestamp column); the batch is forwarded unchanged.
pub const WINDOW_FILTER_FAILED_EVENT: &str = "metrics_admission.window.filter_failed";

/// Metrics for the metrics admission processor.
#[metric_set(name = "processor.metrics_admission")]
#[derive(Debug, Default, Clone)]
pub struct MetricsAdmissionMetrics {
    /// Metrics batches processed.
    #[metric(unit = "{batch}")]
    pub batches: Counter<u64>,

    /// Metrics batches that could not be parsed into a view or time-filtered and
    /// were therefore forwarded unchanged.
    #[metric(unit = "{batch}")]
    pub batches_malformed: Counter<u64>,

    /// Data points admitted (event time within the admission window and not
    /// rejected by strict conflict mode).
    #[metric(unit = "{point}")]
    pub points_admitted: Counter<u64>,

    /// Data points rejected because their event time was older than
    /// `now - max_lag`.
    #[metric(unit = "{point}")]
    pub points_rejected_too_old: Counter<u64>,

    /// Data points rejected because their event time was further ahead than
    /// `now + max_skew`.
    #[metric(unit = "{point}")]
    pub points_rejected_too_future: Counter<u64>,

    /// Data points rejected for other reasons (e.g. a missing event time).
    #[metric(unit = "{point}")]
    pub points_rejected_malformed: Counter<u64>,

    /// Data points rejected by strict conflict mode (metrics whose descriptor
    /// conflicts with the recorded primary).
    #[metric(unit = "{point}")]
    pub points_rejected_conflict: Counter<u64>,

    /// Distinct metric names recorded in the type registry for the first time.
    #[metric(unit = "{metric}")]
    pub names_registered: Counter<u64>,

    /// Distinct metric names newly observed to carry a conflicting descriptor.
    #[metric(unit = "{conflict}")]
    pub name_conflicts: Counter<u64>,
}
