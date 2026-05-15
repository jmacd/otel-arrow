// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Adaptive priority sampling for log events.
//!
//! Two algorithms are provided side-by-side so they can be compared
//! head-to-head on identical workloads via the `sampling` benchmark
//! (see `crates/telemetry/benches/sampling.rs`):
//!
//! - [`AlgorithmJ`] — single-sample inverse-frequency-weighted
//!   priority sampling with a buffer of size `B > T` and bulk
//!   sample-down on buffer fill.  Uses *current-period* live
//!   frequencies as weights.  See `algo_j` for the full design.
//! - [`AlgorithmK`] — single-pass priority sampling using the
//!   *previous* period's frozen inverse-frequency table as weights,
//!   with Chao1/Good-Turing imputation for callsites unseen in the
//!   prior period.  No buffer.  See `algo_k` for the full design.
//!
//! Both algorithms expose the [`LogSampler`] trait so that the
//! benchmark and (eventually) the tracing-layer adapter can drive
//! them through the same API.
//!
//! # Concurrency
//!
//! Both samplers are `!Sync` by design.  They are intended for
//! single-threaded use behind a thread-local (one instance per
//! dataflow engine core).  All hot-path methods take `&self` with
//! interior mutability via [`std::cell::RefCell`] / [`std::cell::Cell`].

mod algo_j;
mod algo_k;

#[cfg(test)]
mod tests;

pub use algo_j::AlgorithmJ;
pub use algo_k::{AlgorithmK, chao1_unseen_weight};

/// Outcome of a sampler admission attempt.
///
/// `Ticket` carries algorithm-specific state (the priority `u` for
/// Algorithm J, or the priority key `k = u/w_c` for Algorithm K) that
/// must be passed verbatim to [`LogSampler::insert`] when the caller
/// chooses to insert the formatted payload.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Admission<Ticket> {
    /// The event must be skipped: do **not** format the record.
    Skip,
    /// The event was admitted.  The caller must format the payload
    /// and call [`LogSampler::insert`] with this ticket.
    Admit(Ticket),
}

/// Common API for the priority-sampling algorithms in this module.
///
/// `C` is the callsite identifier type and `P` is the payload (the
/// formatted log record in production use).  All operations are
/// `&self`; interior mutability is hidden inside each implementation.
pub trait LogSampler<C, P> {
    /// Algorithm-specific data carried from `admit` to `insert`.
    type Ticket: Copy;

    /// Hot path: try to admit an event for `callsite`.  Returns
    /// [`Admission::Skip`] if the caller should drop the event
    /// without formatting, or [`Admission::Admit`] with a ticket to
    /// pass to [`Self::insert`].
    fn admit(&self, callsite: &C) -> Admission<Self::Ticket>;

    /// Insert a formatted payload that was previously approved by
    /// [`Self::admit`].
    fn insert(&self, callsite: C, ticket: Self::Ticket, payload: P);

    /// Drain the current period's sample.  Returns `(callsite,
    /// payload, sampling_weight)` triples; `Σ sampling_weight` is an
    /// unbiased estimator of the period's total arrival count.
    ///
    /// Allocates a fresh `Vec` each call.  Use [`Self::flush_into`]
    /// to recycle a caller-owned buffer.
    fn flush(&self) -> Vec<(C, P, f64)> {
        let mut out = Vec::new();
        self.flush_into(&mut out);
        out
    }

    /// Same as [`Self::flush`] but appends into `out`, allowing the
    /// caller to recycle a buffer across periods (zero per-period
    /// allocations on the steady-state output path).  `out` is *not*
    /// cleared first.
    fn flush_into(&self, out: &mut Vec<(C, P, f64)>);
}
