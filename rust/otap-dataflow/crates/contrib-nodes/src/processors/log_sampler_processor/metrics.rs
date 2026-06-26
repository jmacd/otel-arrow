// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry metrics for the local log sampler processor.

use otap_df_telemetry::instrument::{Counter, Gauge};
use otap_df_telemetry_macros::metric_set;

/// Metrics for the local log sampler processor.
#[metric_set(name = "processor.log_sampler")]
#[derive(Debug, Default, Clone)]
pub struct LogSamplerMetrics {
    /// Total log records observed by the sampler.
    #[metric(unit = "{log}")]
    pub log_signals_consumed: Counter<u64>,

    /// Representatives emitted on window close.
    #[metric(unit = "{log}")]
    pub representatives_emitted: Counter<u64>,

    /// Number of sampling windows closed.
    #[metric(unit = "{window}")]
    pub windows_closed: Counter<u64>,

    /// Errors decoding incoming log payloads.
    #[metric(unit = "{error}")]
    pub decode_errors: Counter<u64>,

    /// Records rejected by the global gate before reaching the reservoir.
    #[metric(unit = "{log}")]
    pub globally_rejected: Counter<u64>,

    /// Times the global table was ignored as stale (sampler degraded to
    /// local-only sampling for that observation).
    #[metric(unit = "{event}")]
    pub table_stale_degradations: Counter<u64>,

    /// Distinct callsites among the representatives of the last window.
    #[metric(unit = "{callsite}")]
    pub last_distinct_callsites: Gauge<u64>,

    /// The local threshold `tau^L` of the last closed window.
    #[metric(unit = "1")]
    pub last_tau_l: Gauge<f64>,

    /// Version of the global table most recently observed by the sampler.
    #[metric(unit = "1")]
    pub last_table_version: Gauge<u64>,
}
