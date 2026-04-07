// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP-native internal metrics SDK.
//!
//! This module implements a codegen-driven metrics SDK for the pipeline's
//! internal self-observability. It mirrors the ITS logs pattern: a dedicated
//! engine thread periodically collects metric snapshots, encodes them as
//! OTAP Arrow record batches, and injects them into the pipeline via the
//! `InternalTelemetryReceiver`.
//!
//! # Instrument Archetypes
//!
//! - **Counter**: monotonic sum (delta or cumulative interface)
//! - **Gauge**: instantaneous value (last-value)
//!
//! # Recording Modes
//!
//! - **Counting**: temporal sum aggregation (default for Counter)
//! - **Histogram**: value distribution; MMSC at Basic level, exponential
//!   histogram at Normal/Detailed (applies to Counter and Gauge)
//! - **LastValue**: keep the latest value (default for Gauge)

pub mod assembly;
pub mod collector;
pub mod dimension;
/// Generated metric set types — do not edit manually.
#[allow(missing_docs)]
pub mod generated;
pub mod precomputed;
