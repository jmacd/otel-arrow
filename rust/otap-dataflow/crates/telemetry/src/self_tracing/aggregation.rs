// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The process-global aggregation seam, Subsystem 3's transport and handle.
//!
//! Every engine worker flushes its thread-local
//! [`LocalSampleBuffer`](super::sampling::LocalSampleBuffer) once per window. In
//! the local-only mode of Subsystem 2 each worker annotates its own sample and
//! emits it straight to the reporter. This module is the all-CPU second side:
//! it carries the raw [`BufferFlush`] from every worker into one process-global
//! [`WindowAggregator`] and lets that aggregator produce the single global
//! annotated stream and republish the two feedback tables.
//!
//! The aggregator shares a record between the two criteria within a window by
//! reference count, so it is single-threaded. The transport is therefore a
//! non-blocking channel to one owner rather than a lock-shared instance: a
//! worker [`try_push`]es its flush and never blocks, a full channel drops a
//! whole flush and counts it, and the [`IntegratedSampleAggregator`] handle owns
//! the aggregator, drains the channel, and finalizes on demand.
//!
//! The handle is owned and driven by the Internal Telemetry Receiver: on each
//! `CollectTelemetry` tick the receiver [`finalize`](IntegratedSampleAggregator::finalize)s
//! the window and emits the annotated records as OTLP, since the receiver is the
//! single per-process bridge from internal telemetry into the pipeline. The
//! handle installs the process-global sender when built and clears it on drop.
//!
//! When no aggregator is installed, [`try_push`] hands the flush back so the
//! caller can fall back to the local-only path, which is the design's sanctioned
//! fail-safe. See [`docs/integrated-sampler-engine-mechanism.md`][mechanism].
//!
//! [mechanism]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-sampler-engine-mechanism.md

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::LogEvent;
use crate::self_tracing::local_buffer::BufferedRecord;
use crate::self_tracing::sampling::{
    BufferFlush, ProcessorConfig, RawInput, RepInput, WindowAggregator, WorkerFlush,
    annotate_log_record, heavy_hitter_table, log_callsite_identity, span_start_table,
};

/// The seed for the process-global aggregator. Fixed so a run is reproducible;
/// a single aggregator serves the whole process.
const AGGREGATOR_SEED: u64 = 0x0DDB_1A5E_5EED_1234;

/// The inbound channel capacity, in per-worker flushes. A few windows across a
/// modest core count, sized so a transient finalizer stall drops rather than
/// blocks a worker.
const CHANNEL_CAPACITY: usize = 1024;

/// The process-global sender workers push their flush onto. `None` until an
/// aggregator is started, so the worker falls back to local-only emission.
static AGGREGATOR: Mutex<Option<flume::Sender<BufferFlush<BufferedRecord>>>> = Mutex::new(None);

/// The count of per-worker flushes dropped because the inbound channel was full.
static DROPPED_FLUSHES: AtomicU64 = AtomicU64::new(0);

/// The process-global integrated-sample aggregator, owned by the Internal
/// Telemetry Receiver.
///
/// Building one installs the process-global sender so every worker's
/// [`flush_to`](super::local_buffer::flush_to) routes into it; dropping it
/// clears the sender so workers revert to the local-only fallback. The receiver
/// [`recv`](Self::recv)s worker flushes, [`ingest`](Self::ingest)s them, and on
/// each `CollectTelemetry` tick [`finalize`](Self::finalize)s the window into the
/// annotated records to emit as OTLP.
pub struct IntegratedSampleAggregator {
    flush_rx: flume::Receiver<BufferFlush<BufferedRecord>>,
    aggregator: WindowAggregator<BufferedRecord>,
    /// Whether this handle installed the process-global sender, so only the
    /// installing handle clears it on drop.
    installed_global: bool,
}

/// A worker flush received from the channel, awaiting ingestion. Opaque so the
/// receiver crate need not name the internal buffer types.
pub struct PendingSampleFlush(BufferFlush<BufferedRecord>);

impl IntegratedSampleAggregator {
    /// Build the aggregator under `config` and install the process-global sender.
    ///
    /// The span-start table starts in the sample-everything state and the
    /// heavy-hitter table in the local-only state until the first window flush.
    #[must_use]
    pub fn new(config: ProcessorConfig) -> Self {
        let (sender, flush_rx) = flume::bounded(CHANNEL_CAPACITY);
        {
            let mut slot = AGGREGATOR.lock().expect("aggregator lock poisoned");
            *slot = Some(sender);
        }
        let aggregator = WindowAggregator::new(
            config,
            span_start_table(),
            heavy_hitter_table(),
            AGGREGATOR_SEED,
        );
        Self {
            flush_rx,
            aggregator,
            installed_global: true,
        }
    }

    /// Await the next worker flush. `None` once the channel has closed, which the
    /// caller treats as a signal to stop ingesting.
    pub async fn recv(&self) -> Option<PendingSampleFlush> {
        self.flush_rx.recv_async().await.ok().map(PendingSampleFlush)
    }

    /// Take one already-available worker flush without blocking, for draining the
    /// channel at a window boundary or on shutdown.
    #[must_use]
    pub fn try_recv(&self) -> Option<PendingSampleFlush> {
        self.flush_rx.try_recv().ok().map(PendingSampleFlush)
    }

    /// Fold a received flush into the aggregator's estimators.
    pub fn ingest(&mut self, flush: PendingSampleFlush) {
        self.aggregator.push(to_worker_flush(flush.0));
    }

    /// Close the window: finalize the estimators, republish both feedback tables,
    /// and return the retained records annotated with the three adjusted counts,
    /// ready for the caller to encode as OTLP.
    #[must_use]
    pub fn finalize(&mut self) -> Vec<LogEvent> {
        self.aggregator
            .finalize()
            .into_iter()
            .map(|record| {
                let BufferedRecord { time, record: log } = record.payload;
                let annotated = annotate_log_record(
                    &log,
                    record.logs_adjusted_count,
                    record.traces_adjusted_count,
                    record.span_logs_adjusted_count,
                );
                LogEvent {
                    time,
                    record: annotated,
                }
            })
            .collect()
    }
}

impl Drop for IntegratedSampleAggregator {
    fn drop(&mut self) {
        // Detach the worker seam so workers revert to local-only emission, but
        // only when this handle installed it.
        if self.installed_global {
            let mut slot = AGGREGATOR.lock().expect("aggregator lock poisoned");
            *slot = None;
        }
    }
}

/// The number of per-worker flushes dropped so far because the aggregator's
/// inbound channel was full.
#[must_use]
pub fn dropped_flushes() -> u64 {
    DROPPED_FLUSHES.load(Ordering::Relaxed)
}

/// Try to hand a worker flush to the process-global aggregator.
///
/// Returns `Ok(())` when the flush was accepted or intentionally dropped under
/// back-pressure, both of which mean the caller must not also emit it. Returns
/// `Err(flush)` when no aggregator is installed or the finalizer has gone away,
/// so the caller falls back to the local-only path.
pub(crate) fn try_push(
    flush: BufferFlush<BufferedRecord>,
) -> Result<(), BufferFlush<BufferedRecord>> {
    let slot = AGGREGATOR.lock().expect("aggregator lock poisoned");
    let Some(sender) = slot.as_ref() else {
        return Err(flush);
    };
    match sender.try_send(flush) {
        Ok(()) => Ok(()),
        Err(flume::TrySendError::Full(_dropped)) => {
            // Never block a core-pinned worker: drop the whole flush and count
            // it. The estimators stay unbiased; only this window's coverage dips.
            let _ = DROPPED_FLUSHES.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(flume::TrySendError::Disconnected(flush)) => Err(flush),
    }
}

/// Classify a raw worker flush into the aggregator's input, computing each
/// record's log callsite identity and lifting its trace context, exactly the
/// classification the tracing layer's `route` performs on the way in.
fn to_worker_flush(flush: BufferFlush<BufferedRecord>) -> WorkerFlush<BufferedRecord> {
    let reps = flush
        .sample
        .into_iter()
        .map(|sampled| {
            let log_callsite = log_callsite_of(&sampled.record);
            let trace = sampled.record.record.trace;
            RepInput {
                payload: sampled.record,
                log_callsite,
                trace,
                local_nhat: sampled.adjusted_count,
            }
        })
        .collect();

    let raw = flush
        .raw_span
        .into_iter()
        .filter_map(|buffered| {
            // A raw record is always inside a locally-sampled span; skip the
            // impossible traceless case rather than fabricate a context.
            let trace = buffered.record.trace?;
            let log_callsite = log_callsite_of(&buffered);
            Some(RawInput {
                payload: buffered,
                log_callsite,
                trace,
            })
        })
        .collect();

    WorkerFlush { reps, raw }
}

/// The log callsite identity of a buffered record, from its callsite metadata.
fn log_callsite_of(buffered: &BufferedRecord) -> u64 {
    let callsite = buffered.record.callsite();
    log_callsite_identity(
        callsite.target(),
        callsite.name(),
        callsite.file(),
        callsite.line(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::__log_record_impl;
    use crate::self_tracing::LogContext;
    use crate::self_tracing::sampling::{SampledRecord, shared_always, shared_local_only};
    use std::time::SystemTime;
    use tracing::Level;

    impl IntegratedSampleAggregator {
        /// Build a handle for tests without touching the process-global sender,
        /// so parallel tests never interfere through the global seam. Returns the
        /// handle and a sender to feed it directly.
        fn detached(
            config: ProcessorConfig,
        ) -> (Self, flume::Sender<BufferFlush<BufferedRecord>>) {
            let (sender, flush_rx) = flume::bounded(CHANNEL_CAPACITY);
            let aggregator = WindowAggregator::new(
                config,
                shared_always(),
                shared_local_only(),
                AGGREGATOR_SEED,
            );
            (
                Self {
                    flush_rx,
                    aggregator,
                    installed_global: false,
                },
                sender,
            )
        }
    }

    fn buffered() -> BufferedRecord {
        BufferedRecord {
            time: SystemTime::now(),
            record: __log_record_impl!(Level::INFO, "sample.agg").into_record(LogContext::new()),
        }
    }

    #[test]
    fn to_worker_flush_classifies_reps_and_raw() {
        let flush = BufferFlush {
            sample: vec![SampledRecord {
                record: buffered(),
                adjusted_count: 4.0,
                span_selected: false,
            }],
            raw_span: vec![buffered()],
        };
        let worker = to_worker_flush(flush);
        assert_eq!(worker.reps.len(), 1);
        assert!((worker.reps[0].local_nhat - 4.0).abs() < 1e-9);
        // The out-of-span raw record has no trace, so it is dropped rather than
        // fabricated into a span record.
        assert_eq!(worker.raw.len(), 0);
    }

    #[test]
    fn handle_ingests_and_finalizes_to_annotated_events() {
        let (mut agg, tx) = IntegratedSampleAggregator::detached(ProcessorConfig::default());

        // Two representatives carrying local counts, delivered over the channel.
        tx.send(BufferFlush {
            sample: vec![
                SampledRecord {
                    record: buffered(),
                    adjusted_count: 3.0,
                    span_selected: false,
                },
                SampledRecord {
                    record: buffered(),
                    adjusted_count: 2.0,
                    span_selected: false,
                },
            ],
            raw_span: vec![],
        })
        .unwrap();

        // Drain the channel and close the window.
        while let Some(pending) = agg.try_recv() {
            agg.ingest(pending);
        }
        let events = agg.finalize();
        assert!(
            events.len() >= 2,
            "expected the two representatives to be emitted, got {}",
            events.len()
        );

        // A fresh window starts empty.
        assert!(agg.finalize().is_empty());
    }
}
