// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The process-global feedback registry.
//!
//! Subsystem 3's two feedback tables are published once per window by the
//! all-CPU aggregator and read wait-free on the hot path by every worker and the
//! tracer. Because all participants share process memory, the registry holds the
//! current immutable table for each behind an [`arc_swap::ArcSwap`] rather than a
//! wire codec:
//!
//! - the **span-start threshold table**, written by the aggregator and read by
//!   the tracer's [`SpanStartSampler`](super::span_start::SpanStartSampler) to
//!   choose which root spans to sample, and
//! - the **heavy-hitter table**, written by the aggregator and read by each
//!   worker's criterion-one binding gate.
//!
//! Both tables start in their fail-safe states and stay there until an aggregator
//! runs, so the hot-path readers are exact no-ops when Subsystem 3 is disabled:
//! the span-start table samples every span start, and the heavy-hitter table's
//! infinite global threshold never binds, leaving each worker's local-only
//! criterion one unchanged. This is why the tracer sampler and the worker gate
//! can be wired unconditionally.

use std::sync::OnceLock;

use super::heavy_hitter::{SharedHeavyHitterTable, shared_local_only};
use super::span_start::{SharedSpanStartTable, shared_always};

static SPAN_START: OnceLock<SharedSpanStartTable> = OnceLock::new();
static HEAVY_HITTER: OnceLock<SharedHeavyHitterTable> = OnceLock::new();

/// The process-global span-start threshold table handle.
///
/// The aggregator publishes onto it each window; the tracer's `SpanStartSampler`
/// reads it on every root span. Initialized to sample every span start.
#[must_use]
pub fn span_start_table() -> SharedSpanStartTable {
    SPAN_START.get_or_init(shared_always).clone()
}

/// The process-global heavy-hitter table handle.
///
/// The aggregator publishes onto it each window; each worker's criterion-one
/// binding gate reads it. Initialized to the local-only fail-safe, whose
/// infinite global threshold never binds.
#[must_use]
pub fn heavy_hitter_table() -> SharedHeavyHitterTable {
    HEAVY_HITTER.get_or_init(shared_local_only).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_are_stable_and_shared() {
        // Repeated calls return handles onto the same process-global table, so a
        // store through one is visible through another.
        let a = span_start_table();
        let b = span_start_table();
        assert!(std::sync::Arc::ptr_eq(&a, &b));

        let h1 = heavy_hitter_table();
        let h2 = heavy_hitter_table();
        assert!(std::sync::Arc::ptr_eq(&h1, &h2));
    }

    #[test]
    fn fail_safe_defaults_are_no_ops() {
        // The heavy-hitter default never binds, and the span-start default keeps
        // every span start.
        assert!(heavy_hitter_table().load().tau().is_infinite());
        assert!(span_start_table().load().default_threshold().adjusted_count() <= 1.0 + 1e-9);
    }
}
