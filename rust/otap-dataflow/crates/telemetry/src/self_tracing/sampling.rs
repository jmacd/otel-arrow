// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Integrated logs-and-traces reservoir sampling.
//!
//! This module implements the sampler specified in
//! [`docs/integrated-logs-traces-reservoir.md`][design]. It samples logs and
//! spans together under fixed budgets and emits ordinary OpenTelemetry logs
//! that carry their own sampling weights as adjusted-count attributes.
//!
//! The pieces are:
//!
//! - [`callsite`] computes the stable identities the sampler keys on: a
//!   span-start callsite identity and a log callsite identity.
//!
//! [design]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-logs-traces-reservoir.md

pub mod aggregator;
pub mod callsite;
pub mod bottom_floor;
pub mod buffer;
pub mod encode;
pub mod heavy_hitter;
pub mod processor;
pub mod record;
pub mod span_start;

pub use aggregator::{RawInput, RepInput, WindowAggregator, WorkerFlush};
pub use bottom_floor::{BottomFloor, Kept, UniformReservoir, WindowOutput, inclusion_probability};
pub use buffer::{BufferFlush, LocalSampleBuffer, SampledRecord};
pub use callsite::{callsite_identity, fnv1a_str, log_callsite_identity, span_start_identity};
pub use encode::annotate_log_record;
pub use heavy_hitter::{HeavyHitterTable, SharedHeavyHitterTable, shared_local_only};
pub use processor::{IncomingRecord, IntegratedSampler, ProcessorConfig, SpanPhase};
pub use record::FlushedRecord;
pub use span_start::{
    SharedSpanStartTable, SpanStartSampler, SpanStartTable, build_span_start_table, shared_always,
};

/// Provisional attribute key carrying the global-independent log adjusted
/// count, criterion one. The estimated number of log arrivals this record
/// represents in the whole log stream. Exact-zero means the record is not a
/// member of this population.
pub const ATTR_LOGS_ADJUSTED_COUNT: &str = "otel.logs.adjusted_count";

/// Provisional attribute key carrying the span adjusted count. The estimated
/// number of spans this record's span represents. Exact-zero means the record
/// belongs to no sampled span.
pub const ATTR_TRACES_ADJUSTED_COUNT: &str = "otel.traces.adjusted_count";

/// Provisional attribute key carrying the in-span log adjusted count, criterion
/// two. The estimated number of in-span log arrivals this record represents
/// within its span. Exact-zero means the record is not a member of its span's
/// log population.
pub const ATTR_SPAN_LOGS_ADJUSTED_COUNT: &str = "otel.span_logs.adjusted_count";
