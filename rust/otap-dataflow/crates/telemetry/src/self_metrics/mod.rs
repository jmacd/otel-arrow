// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP-native metrics SDK for internal self-observability.
//!
//! This module replaces the OpenTelemetry SDK metrics path with a direct
//! pipeline. The [`MetricsTap`] accumulates metric snapshots from the
//! telemetry registry and serves Prometheus text exposition. The
//! [`ViewResolver`] applies scoped instrument renaming at export time.

pub mod bridge;
pub mod metrics_tap;
pub mod precomputed;
pub mod views;
