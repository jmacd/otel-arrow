// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-thread CCKR sampler registration.
//!
//! Each engine pipeline thread installs at most one [`Cckr`] sampler via
//! [`install`], which returns an RAII [`SamplerGuard`]. While the guard is
//! alive, the [`crate::tracing_init::StructuredLoggingLayer`] hot path
//! consults the thread-local sampler to decide whether to format and
//! deliver each log event or to drop it (the `Skip` admission path).
//!
//! Flushes are normally driven by the pipeline thread's
//! `RuntimeCtrlMsgManager` sampler-flush timer through
//! [`flush_current_thread`]. The [`SamplerGuard`] also performs a
//! best-effort final flush on drop so the in-flight period is not
//! silently lost on a clean thread exit.

use std::cell::RefCell;

use tracing::callsite::Identifier;

use crate::event::{LogEvent, ObservedEventReporter};
// `LogRecord` is only constructed from the test module below.
#[cfg(test)]
use crate::self_tracing::LogRecord;

use super::{Admission, AdmitTicket, Cckr, LogSampler};

thread_local! {
    static THREAD_SAMPLER: RefCell<Option<ThreadSamplerState>> = const { RefCell::new(None) };
}

struct ThreadSamplerState {
    sampler: Cckr<Identifier, LogEvent>,
    reporter: ObservedEventReporter,
}

/// RAII handle for the per-thread sampler installation.  Dropping the
/// guard performs a best-effort final flush and clears the thread-local
/// sampler.
#[must_use = "dropping the SamplerGuard immediately uninstalls the per-thread sampler"]
pub struct SamplerGuard {
    // Make sure the guard is !Send so it cannot be moved across threads.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl SamplerGuard {
    /// Explicitly flush the current thread's sampler now.  Equivalent
    /// to [`flush_current_thread`] but anchored to the guard for
    /// readability at call sites that already hold one.
    pub fn flush(&self) {
        flush_current_thread();
    }
}

impl Drop for SamplerGuard {
    fn drop(&mut self) {
        // Best-effort final flush before tearing down the sampler so
        // the in-flight period is not lost on clean thread exit.
        flush_current_thread();
        THREAD_SAMPLER.with(|cell| {
            let _ = cell.borrow_mut().take();
        });
    }
}

/// Install a per-thread CCKR sampler.
///
/// Panics if a sampler is already installed on the current thread.
///
/// `reservoir_capacity` is `T` (statistical sample-size cap) and
/// `preserve_capacity` is `R` (novelty-preserve cap); see
/// `crates/telemetry/src/sampler/design.md`.
///
/// The `reporter` is the *tracing* reporter (the one held inside
/// [`crate::tracing_init::ProviderSetup::InternalAsync`]). Flushed
/// batches are shipped through it as
/// [`crate::event::ObservedEvent::LogBatch`].
pub fn install(
    reservoir_capacity: usize,
    preserve_capacity: usize,
    reporter: ObservedEventReporter,
) -> SamplerGuard {
    THREAD_SAMPLER.with(|cell| {
        let mut slot = cell.borrow_mut();
        assert!(
            slot.is_none(),
            "per-thread CCKR sampler already installed on this thread"
        );
        *slot = Some(ThreadSamplerState {
            sampler: Cckr::with_options(reservoir_capacity, preserve_capacity, 32, rand::random()),
            reporter,
        });
    });
    SamplerGuard {
        _not_send: std::marker::PhantomData,
    }
}

/// Returns `true` when a sampler is installed on the current thread.
#[must_use]
pub fn is_installed() -> bool {
    THREAD_SAMPLER.with(|cell| cell.borrow().is_some())
}

/// Outcome of [`admit`] (a thread-local convenience over the underlying
/// [`Cckr::admit`]) when a sampler is installed on the current thread.
pub type ThreadAdmission = Admission<AdmitTicket>;

/// Probe the per-thread sampler with `callsite`.  Returns `Some` when a
/// sampler is installed (with the admission outcome), `None` when no
/// sampler is installed on the current thread (caller should use the
/// non-sampled path).
#[inline]
#[must_use]
pub fn admit(callsite: &Identifier) -> Option<ThreadAdmission> {
    THREAD_SAMPLER.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|state| state.sampler.admit(callsite))
    })
}

/// Hand a formatted [`LogEvent`] to the per-thread sampler, using the
/// admission ticket previously returned by [`admit`].  Silently no-ops
/// when no sampler is installed (defensive; in normal use the caller
/// only invokes this after a non-`Skip` admission).
#[inline]
pub fn insert(callsite: Identifier, admission: ThreadAdmission, event: LogEvent) {
    THREAD_SAMPLER.with(|cell| {
        if let Some(state) = cell.borrow().as_ref() {
            state.sampler.insert(callsite, admission, event);
        }
    });
}

/// Drain the per-thread sampler's current period into one
/// [`crate::event::ObservedEvent::LogBatch`] and ship it via the
/// installed reporter.  Empty batches are suppressed.  No-op when no
/// sampler is installed on the current thread.
pub fn flush_current_thread() {
    THREAD_SAMPLER.with(|cell| {
        let borrow = cell.borrow();
        let Some(state) = borrow.as_ref() else {
            return;
        };
        let mut drained: Vec<(Identifier, LogEvent, f64)> = Vec::new();
        state.sampler.flush_into(&mut drained);
        if drained.is_empty() {
            return;
        }

        let mut batch: Vec<LogEvent> = Vec::with_capacity(drained.len());
        for (_callsite, mut event, sampling_count) in drained {
            event.record.count = Some(sampling_count);
            batch.push(event);
        }
        state.reporter.log_batch(batch);
    });
}

/// Visible-for-testing accessor: returns true if the per-thread sampler
/// is installed.  Same as [`is_installed`]; kept as a separate symbol so
/// the test surface is explicit.
#[cfg(test)]
pub(crate) fn has_thread_sampler_for_test() -> bool {
    is_installed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_tracing::LogContext;
    use bytes::Bytes;
    use otap_df_config::observed_state::SendPolicy;
    use std::time::SystemTime;
    use tracing::callsite::Callsite;
    use tracing::metadata::Kind;
    use tracing::{Level, Metadata};

    struct TestCallsite;
    impl Callsite for TestCallsite {
        fn set_interest(&self, _interest: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &Metadata<'_> {
            &TEST_METADATA
        }
    }
    static TEST_CALLSITE: TestCallsite = TestCallsite;
    static TEST_METADATA: Metadata<'static> = Metadata::new(
        "ts_test",
        "ts_test_target",
        Level::INFO,
        Some(file!()),
        Some(line!()),
        Some(module_path!()),
        tracing::field::FieldSet::new(&[], Identifier(&TEST_CALLSITE)),
        Kind::EVENT,
    );

    fn make_event() -> LogEvent {
        LogEvent {
            time: SystemTime::now(),
            record: LogRecord {
                callsite_id: Identifier(&TEST_CALLSITE),
                body_attrs_bytes: Bytes::new(),
                dropped_attributes_count: 0,
                count: None,
                context: LogContext::new(),
            },
        }
    }

    fn test_reporter() -> (
        ObservedEventReporter,
        flume::Receiver<crate::event::ObservedEvent>,
    ) {
        let (tx, rx) = flume::bounded(16);
        (ObservedEventReporter::new(SendPolicy::default(), tx), rx)
    }

    #[test]
    fn install_and_flush_roundtrip() {
        // Run in its own thread so it doesn't leak thread-local state.
        std::thread::spawn(|| {
            let (reporter, rx) = test_reporter();
            let _guard = install(8, 4, reporter);
            assert!(has_thread_sampler_for_test());

            // Admit a few events.
            for _ in 0..3 {
                let id = Identifier(&TEST_CALLSITE);
                let admission = admit(&id).expect("sampler installed");
                if !matches!(admission, Admission::Skip) {
                    insert(id, admission, make_event());
                }
            }

            flush_current_thread();
            let evt = rx.try_recv().expect("batch should be sent");
            match evt {
                crate::event::ObservedEvent::LogBatch(batch) => {
                    assert!(!batch.is_empty());
                    for ev in &batch {
                        // All flushed records must carry a sampling count.
                        assert!(ev.record.count.is_some());
                    }
                }
                other => panic!("expected LogBatch, got {other:?}"),
            }

            // Empty flush is suppressed.
            flush_current_thread();
            assert!(rx.try_recv().is_err());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn no_sampler_is_no_op() {
        std::thread::spawn(|| {
            assert!(!is_installed());
            assert!(admit(&Identifier(&TEST_CALLSITE)).is_none());
            flush_current_thread(); // does not panic
        })
        .join()
        .unwrap();
    }

    #[test]
    fn guard_drop_flushes_final_period() {
        std::thread::spawn(|| {
            let (reporter, rx) = test_reporter();
            {
                let _guard = install(4, 2, reporter);
                let id = Identifier(&TEST_CALLSITE);
                let admission = admit(&id).expect("sampler installed");
                if !matches!(admission, Admission::Skip) {
                    insert(id, admission, make_event());
                }
                // Guard drops at end of scope.
            }
            // A final batch should have been emitted.
            let evt = rx.try_recv().expect("final batch on drop");
            assert!(matches!(
                evt,
                crate::event::ObservedEvent::LogBatch(b) if !b.is_empty()
            ));
            assert!(!is_installed());
        })
        .join()
        .unwrap();
    }
}
