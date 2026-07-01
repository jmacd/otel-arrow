// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The process-global aggregation seam, Subsystem 3's transport and finalizer.
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
//! non-blocking channel to one finalizer thread rather than a lock-shared
//! instance: a worker [`try_push`]es its flush and never blocks, a full channel
//! drops a whole flush and counts it, and the finalizer thread owns the
//! aggregator, drains the channel, and finalizes on its own timer. See
//! [`docs/integrated-sampler-engine-mechanism.md`][mechanism].
//!
//! When no aggregator is installed, [`try_push`] hands the flush back so the
//! caller can fall back to the local-only path, which is the design's sanctioned
//! fail-safe.
//!
//! [mechanism]: https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/docs/integrated-sampler-engine-mechanism.md

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::event::{LogEvent, ObservedEventReporter};
use crate::self_tracing::local_buffer::BufferedRecord;
use crate::self_tracing::sampling::{
    BufferFlush, ProcessorConfig, RawInput, RepInput, WindowAggregator, WorkerFlush,
    annotate_log_record, log_callsite_identity, shared_always, shared_local_only,
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

/// Start the process-global aggregator, returning a guard that stops the
/// finalizer thread and detaches the seam on drop.
///
/// The finalizer emits its annotated global stream to `reporter`, runs the
/// three estimators under `config`, and finalizes every `window`. Call once when
/// internal telemetry starts.
#[must_use]
pub fn start(
    reporter: ObservedEventReporter,
    config: ProcessorConfig,
    window: Duration,
) -> AggregatorGuard {
    let (sender, receiver) = flume::bounded(CHANNEL_CAPACITY);
    {
        let mut slot = AGGREGATOR.lock().expect("aggregator lock poisoned");
        *slot = Some(sender);
    }
    let thread = std::thread::Builder::new()
        .name("otap-sample-aggregator".to_string())
        .spawn(move || run_finalizer(&receiver, &reporter, config, window))
        .expect("failed to spawn sample aggregator thread");
    AggregatorGuard {
        thread: Some(thread),
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

/// A guard that stops the process-global aggregator on drop.
pub struct AggregatorGuard {
    thread: Option<JoinHandle<()>>,
}

impl Drop for AggregatorGuard {
    fn drop(&mut self) {
        // Drop the sender so the finalizer's channel disconnects and its loop
        // exits after a final flush, then join the thread.
        {
            let mut slot = AGGREGATOR.lock().expect("aggregator lock poisoned");
            *slot = None;
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The finalizer loop: own the aggregator, drain the channel, and finalize on a
/// fixed-period timer. Exits on channel disconnect after one last flush.
fn run_finalizer(
    receiver: &flume::Receiver<BufferFlush<BufferedRecord>>,
    reporter: &ObservedEventReporter,
    config: ProcessorConfig,
    window: Duration,
) {
    let mut aggregator =
        WindowAggregator::<BufferedRecord>::new(config, shared_always(), shared_local_only(), AGGREGATOR_SEED);
    let mut deadline = Instant::now() + window;
    loop {
        let now = Instant::now();
        if now >= deadline {
            emit_flushed(reporter, aggregator.finalize());
            deadline = now + window;
            continue;
        }
        match receiver.recv_timeout(deadline - now) {
            Ok(flush) => aggregator.push(to_worker_flush(flush)),
            Err(flume::RecvTimeoutError::Timeout) => {
                emit_flushed(reporter, aggregator.finalize());
                deadline = Instant::now() + window;
            }
            Err(flume::RecvTimeoutError::Disconnected) => break,
        }
    }
    // Drain any records still buffered before the thread winds down.
    emit_flushed(reporter, aggregator.finalize());
}

/// Emit one window's annotated records to the reporter.
fn emit_flushed(
    reporter: &ObservedEventReporter,
    flushed: Vec<crate::self_tracing::sampling::FlushedRecord<BufferedRecord>>,
) {
    for record in flushed {
        let BufferedRecord { time, record: log } = record.payload;
        let annotated = annotate_log_record(
            &log,
            record.logs_adjusted_count,
            record.traces_adjusted_count,
            record.span_logs_adjusted_count,
        );
        reporter.log(LogEvent {
            time,
            record: annotated,
        });
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
    use crate::event::ObservedEvent;
    use crate::self_tracing::LogContext;
    use crate::self_tracing::sampling::SampledRecord;
    use otap_df_config::observed_state::SendPolicy;
    use std::time::SystemTime;
    use tracing::Level;

    fn reporter() -> (ObservedEventReporter, flume::Receiver<ObservedEvent>) {
        let (tx, rx) = flume::bounded(256);
        (ObservedEventReporter::new(SendPolicy::default(), tx), rx)
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
    fn finalizer_emits_global_annotated_stream() {
        let (reporter, rx) = reporter();
        let (tx, finalizer_rx) = flume::bounded::<BufferFlush<BufferedRecord>>(16);
        let window = Duration::from_millis(30);
        let handle = std::thread::spawn(move || {
            run_finalizer(&finalizer_rx, &reporter, ProcessorConfig::default(), window);
        });

        // Two representatives carrying local counts, aggregated globally.
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

        // Disconnect so the finalizer flushes and exits.
        drop(tx);
        handle.join().unwrap();

        let emitted: Vec<_> = rx.drain().collect();
        assert!(
            emitted.len() >= 2,
            "expected the two representatives to be emitted, got {}",
            emitted.len()
        );
    }
}
