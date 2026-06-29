// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry definitions for the event-time window (L2) processor.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Emitted when an incoming metrics batch cannot be read as an OTAP metrics
/// view, so its points cannot be windowed.
pub const VIEW_CREATION_FAILED_EVENT: &str = "event_time_window.view.creation_failed";

/// Metrics for the event-time window processor.
#[metric_set(name = "processor.event_time_window")]
#[derive(Debug, Default, Clone)]
pub struct EventTimeWindowMetrics {
    /// Metrics batches processed.
    #[metric(unit = "{batch}")]
    pub batches: Counter<u64>,

    /// Number data points folded into a window aggregate.
    #[metric(unit = "{point}")]
    pub points_aggregated: Counter<u64>,

    /// Data points dropped because their event time was below the watermark
    /// (their window had already fired).
    #[metric(unit = "{point}")]
    pub points_late: Counter<u64>,

    /// Data points skipped because their instrument type is not yet windowed by
    /// this proof of concept (histogram, exponential histogram, summary).
    #[metric(unit = "{point}")]
    pub points_skipped: Counter<u64>,

    /// `(series, window)` aggregates emitted as complete windows fired.
    #[metric(unit = "{point}")]
    pub windows_emitted: Counter<u64>,

    /// Aggregated output batches emitted downstream.
    #[metric(unit = "{batch}")]
    pub batches_emitted: Counter<u64>,
}
