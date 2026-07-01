// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-worker thread-local installation of the integrated sample buffer.
//!
//! Each pipeline worker runs on a dedicated, core-pinned OS thread with a
//! current-thread runtime, so a `thread_local` buffer is coherent: every task on
//! that worker shares one buffer, and the worker's pipeline controller flushes
//! it on a periodic tick. This module owns that thread-local and the routing the
//! tracing layer uses to feed it.
//!
//! When a buffer is installed, [`route`] diverts an emitted record into the
//! per-worker [`LocalSampleBuffer`] instead of sending it straight to the
//! reporter. [`flush_to`] drains the buffer at the window boundary and forwards
//! the sample and the raw span stream to the reporter.
//!
//! This is the **local-only** mode of the integrated sampler: each worker
//! produces a self-contained, Horvitz-Thompson weighted sample annotated with
//! the three adjusted counts. The all-CPU aggregation and the heavy-hitter
//! feedback are the next phase; until then a worker's sample stands on its own,
//! which is the design's sanctioned fail-safe.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::event::{LogEvent, ObservedEventReporter};
use crate::self_tracing::sampling::{
    LocalSampleBuffer, annotate_log_record, heavy_hitter_table, log_callsite_identity,
};
use crate::self_tracing::{LogRecord, Threshold};

/// The criterion-one budget for a per-worker buffer. Fixed for now; a
/// configuration surface is a later concern.
const LOCAL_SAMPLE_BUDGET: usize = 2048;

/// A record awaiting flush, carrying the observation time alongside the record.
#[derive(Clone)]
pub(crate) struct BufferedRecord {
    pub(crate) time: SystemTime,
    pub(crate) record: LogRecord,
}

thread_local! {
    /// The per-worker integrated sample buffer, present only on worker threads
    /// that installed one. `None` elsewhere, so non-worker threads emit
    /// directly to the reporter.
    static LOCAL_BUFFER: RefCell<Option<LocalSampleBuffer<BufferedRecord>>> =
        const { RefCell::new(None) };
}

/// A distinct seed per installed buffer so workers do not share a random
/// sequence.
static SEED_COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

/// Installs a per-worker buffer on the current thread, returning a guard that
/// removes it on drop. Call once when a worker thread starts; the worker's
/// pipeline controller flushes it via [`flush_to`].
#[must_use]
pub fn install() -> LocalBufferGuard {
    let seed = SEED_COUNTER.fetch_add(0x2545_F491_4F6C_DD1D, Ordering::Relaxed);
    LOCAL_BUFFER.with(|cell| {
        // Criterion one reads the process-global heavy-hitter table so a
        // globally abundant callsite is throttled here in proportion to the
        // fleet-wide congestion; the local-only fail-safe never binds.
        *cell.borrow_mut() = Some(LocalSampleBuffer::new_gated(
            LOCAL_SAMPLE_BUDGET,
            seed,
            heavy_hitter_table(),
        ));
    });
    LocalBufferGuard { _private: () }
}

/// Removes the per-worker buffer from the current thread on drop.
pub struct LocalBufferGuard {
    _private: (),
}

impl Drop for LocalBufferGuard {
    fn drop(&mut self) {
        LOCAL_BUFFER.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

/// Routes an emitted record into the per-worker buffer when one is installed.
///
/// Returns `None` when the record was buffered, or `Some((time, record))` to
/// hand it back to the caller for direct emission when no buffer is installed.
#[must_use]
pub(crate) fn route(time: SystemTime, record: LogRecord) -> Option<(SystemTime, LogRecord)> {
    LOCAL_BUFFER.with(|cell| {
        let mut guard = cell.borrow_mut();
        let Some(buffer) = guard.as_mut() else {
            return Some((time, record));
        };
        let callsite = record.callsite();
        let log_callsite =
            log_callsite_identity(callsite.target(), callsite.name(), callsite.file(), callsite.line());
        let span_selected = record.trace.is_some_and(|ctx| ctx.locally_sampled);
        buffer.observe(BufferedRecord { time, record }, log_callsite, span_selected);
        None
    })
}

/// Flushes the per-worker buffer, forwarding the window's records onward.
///
/// Drains the buffer while holding the thread-local borrow, then releases it
/// before doing anything with the records, so a log emitted while reporting
/// cannot deadlock or panic on a re-entrant borrow. A no-op when no buffer is
/// installed.
///
/// When the process-global aggregator is running the raw flush is handed to it
/// in memory, and the aggregator produces the global annotated stream. When no
/// aggregator is installed the flush falls back to the local-only path, which
/// annotates each record with counts computed from this worker alone and emits
/// it straight to `reporter`.
pub fn flush_to(reporter: &ObservedEventReporter) {
    let flush = LOCAL_BUFFER.with(|cell| cell.borrow_mut().as_mut().map(LocalSampleBuffer::flush));
    let Some(flush) = flush else {
        return;
    };

    // Prefer the all-CPU aggregator; fall back to local-only when it is absent.
    let flush = match crate::self_tracing::aggregation::try_push(flush) {
        Ok(()) => return,
        Err(flush) => flush,
    };

    for sampled in flush.sample {
        let BufferedRecord { time, record } = sampled.record;
        let (traces, span_logs) = span_counts(&record, sampled.span_selected);
        let annotated = annotate_log_record(&record, sampled.adjusted_count, traces, span_logs);
        reporter.log(LogEvent {
            time,
            record: annotated,
        });
    }
    for raw in flush.raw_span {
        let (traces, span_logs) = span_counts(&raw.record, true);
        // A raw span record is not a member of the global-independent log
        // population, so its log adjusted count is exactly zero.
        let annotated = annotate_log_record(&raw.record, 0.0, traces, span_logs);
        reporter.log(LogEvent {
            time: raw.time,
            record: annotated,
        });
    }
}

/// The span and span-log adjusted counts for a record, given whether it belongs
/// to a locally-sampled span. The span count is the consistent-probability
/// adjusted count from the span's threshold; the span-log count is one in this
/// local-only mode, where every in-span record is present without a per-span
/// reservoir.
fn span_counts(record: &LogRecord, span_selected: bool) -> (f64, f64) {
    if span_selected {
        let traces = record
            .trace
            .and_then(|ctx| ctx.ot.th)
            .map_or(1.0, Threshold::adjusted_count);
        (traces, 1.0)
    } else {
        (0.0, 0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::__log_record_impl;
    use crate::self_tracing::LogContext;
    use otap_df_config::observed_state::SendPolicy;
    use tracing::Level;

    fn reporter() -> (ObservedEventReporter, flume::Receiver<crate::event::ObservedEvent>) {
        let (tx, rx) = flume::bounded(256);
        (ObservedEventReporter::new(SendPolicy::default(), tx), rx)
    }

    #[test]
    fn route_passes_through_without_buffer() {
        // With no buffer installed, the record is handed back for direct
        // emission.
        let record = __log_record_impl!(Level::INFO, "passthrough").into_record(LogContext::new());
        let now = SystemTime::now();
        let handed_back = route(now, record);
        assert!(handed_back.is_some());
    }

    #[test]
    fn install_buffers_and_flush_forwards() {
        let (reporter, rx) = reporter();
        let _guard = install();

        // While installed, records are buffered, not handed back.
        for _ in 0..10 {
            let record =
                __log_record_impl!(Level::INFO, "buffered").into_record(LogContext::new());
            assert!(route(SystemTime::now(), record).is_none());
        }
        // Nothing emitted until flush.
        assert!(rx.try_recv().is_err());

        flush_to(&reporter);
        let flushed = rx.drain().count();
        assert_eq!(flushed, 10, "all under-budget records forwarded at flush");

        // After the guard drops the buffer is gone and routing passes through.
        drop(_guard);
        let record = __log_record_impl!(Level::INFO, "after").into_record(LogContext::new());
        assert!(route(SystemTime::now(), record).is_some());
    }
}
