// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP Arrow payload assembly for the internal metrics SDK.
//!
//! Combines the three tables (metrics, data points, attributes) produced
//! by [`MetricsEncoder`](super::collector::MetricsEncoder) into an
//! [`OtapArrowRecords::Metrics`](otap_df_pdata::otap::OtapArrowRecords::Metrics)
//! payload suitable for pipeline injection.

use super::collector::EncodedMetrics;
use otap_df_pdata::otap::raw_batch_store::RawMetricsStore;
use otap_df_pdata::otap::{Metrics, OtapArrowRecords};
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// Assemble an [`EncodedMetrics`] into an [`OtapArrowRecords::Metrics`].
///
/// Places each table at the correct payload type position in the
/// raw batch store.
pub fn assemble_metrics_payload(
    encoded: EncodedMetrics,
) -> Result<OtapArrowRecords, otap_df_pdata::error::Error> {
    let mut store = RawMetricsStore::default();

    store.set(
        ArrowPayloadType::UnivariateMetrics,
        encoded.metrics_batch,
    );

    store.set(
        ArrowPayloadType::NumberDataPoints,
        encoded.ndp_batch,
    );

    if let Some(scope_attrs) = encoded.scope_attrs_batch {
        store.set(ArrowPayloadType::ScopeAttrs, scope_attrs);
    }

    let metrics = Metrics::try_from(store)?;
    Ok(OtapArrowRecords::Metrics(metrics))
}
