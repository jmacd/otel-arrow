// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry metrics for the global reservoir processor.

use otap_df_telemetry::instrument::{Counter, Gauge};
use otap_df_telemetry_macros::metric_set;

/// Metrics for the global reservoir processor.
#[metric_set(name = "processor.global_reservoir")]
#[derive(Debug, Default, Clone)]
pub struct GlobalReservoirMetrics {
    /// Representative records consumed from SDK-side samplers.
    #[metric(unit = "{log}")]
    pub representatives_consumed: Counter<u64>,

    /// Representative records forwarded downstream (nhat stripped).
    #[metric(unit = "{log}")]
    pub representatives_forwarded: Counter<u64>,

    /// Number of global windows closed.
    #[metric(unit = "{window}")]
    pub windows_closed: Counter<u64>,

    /// Global tables published to the shared channel.
    #[metric(unit = "{table}")]
    pub tables_published: Counter<u64>,

    /// Errors decoding incoming log payloads.
    #[metric(unit = "{error}")]
    pub decode_errors: Counter<u64>,

    /// Rejected or malformed runtime reconfiguration messages.
    #[metric(unit = "{error}")]
    pub config_errors: Counter<u64>,

    /// Heavy hitters in the last published table.
    #[metric(unit = "{callsite}")]
    pub last_heavy_hitters: Gauge<u64>,

    /// Distinct callsites observed in the last closed window.
    #[metric(unit = "{callsite}")]
    pub last_distinct_callsites: Gauge<u64>,

    /// The global threshold `tau^G` of the last published table.
    #[metric(unit = "1")]
    pub last_tau_g: Gauge<f64>,

    /// The rarest-seen floor `g_unseen` of the last published table.
    #[metric(unit = "1")]
    pub last_g_unseen: Gauge<f64>,
}
