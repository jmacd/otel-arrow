//! Self-metrics: OTAP-native metrics with Arrow encoding.
//!
//! This module provides:
//! - [`PrecomputedMetricSchema`](precomputed::PrecomputedMetricSchema) for
//!   init-time Arrow batch construction
//! - [`CumulativeAccumulator`](accumulator::CumulativeAccumulator) for
//!   Arrow-native delta→cumulative accumulation
//! - [`PrometheusExporter`](prometheus::PrometheusExporter) for HTTP `/metrics`
//! - [`MetricsEncoder`](collector::MetricsEncoder) for snapshot→OTAP encoding
//! - [`bridge`] for converting `MetricsDescriptor` to precomputed schemas

pub mod accumulator;
pub mod assembly;
pub mod bridge;
pub mod collector;
pub mod openmetrics;
pub mod precomputed;
pub mod prometheus;
