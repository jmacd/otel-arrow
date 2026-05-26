// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Adaptive per-thread log-event sampler.
//!
//! See [`design`](../sampler/design.md) for the full algorithm
//! specification and references.  In one paragraph: BKCR is a
//! Bottom-K sketch with exponential ranks (a WS-sketch — Cohen &
//! Kaplan, PODC 2007 — built on the priority-sampling foundation of
//! Duffield, Lund & Thorup, JACM 2007) using a Chao1 / Good–Turing
//! estimate (Chao, *Scand. J. Statist.* 1984) for as-yet-unseen
//! callsites, plus a bounded *novelty reserve* that captures one
//! example per first-rejected callsite as a weight-0 observational
//! record.
//!
//! The crate exposes [`Bkcr`] as the single sampler implementation
//! behind the [`LogSampler`] trait.  The sampler is `!Sync` by
//! design: one instance per dataflow-engine thread, accessed via
//! `&self` with interior mutability through `RefCell`/`Cell`.

#![doc = include_str!("design.md")]

mod bkcr;
pub mod thread_local;

pub use bkcr::{Bkcr, chao1_unseen_weight};
pub use thread_local::{
    SamplerGuard, ThreadAdmission, admit, flush_current_thread, insert, install, is_installed,
};

/// Outcome of a sampler admission attempt.
///
/// The `Ticket` carries algorithm-specific state that must be passed
/// verbatim to [`LogSampler::insert`] when the caller chooses to
/// format and submit the payload.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Admission<Ticket> {
    /// The event must be dropped: do **not** format the record.
    Skip,
    /// The event was admitted to the statistical sample.  The caller
    /// must format the payload and call [`LogSampler::insert`] with
    /// this ticket.
    Admit(Ticket),
    /// The event was rejected by the statistical sample but accepted
    /// by the novelty reserve as a weight-0 observational record.
    /// The caller must format the payload and call
    /// [`LogSampler::insert`] with [`Admission::Reserve`] verbatim.
    Reserve,
}

/// Common API for the per-thread sampler.
///
/// `C` is the callsite identifier type and `P` is the payload (the
/// formatted log record in production use).  All operations take
/// `&self`; interior mutability is hidden inside the implementation.
pub trait LogSampler<C, P> {
    /// Algorithm-specific data carried from [`Self::admit`] to
    /// [`Self::insert`] for admitted events.
    type Ticket: Copy;

    /// Hot path: try to admit an event for `callsite`.  Returns
    /// [`Admission::Skip`] when the caller should drop the event
    /// without formatting, [`Admission::Admit`] when the caller
    /// should format and call [`Self::insert`] with the ticket, or
    /// [`Admission::Reserve`] when the caller should format and
    /// call [`Self::insert`] with `Admission::Reserve` for the
    /// novelty path.
    fn admit(&self, callsite: &C) -> Admission<Self::Ticket>;

    /// Insert a formatted payload that was previously approved by
    /// [`Self::admit`].
    fn insert(&self, callsite: C, admission: Admission<Self::Ticket>, payload: P);

    /// Drain the current period's sample.  Returns `(callsite,
    /// payload, weight)` triples; `Σ weight` is an unbiased
    /// Horvitz–Thompson estimator of the period's total arrival
    /// count.  Reserve entries appear with `weight == 0`.
    ///
    /// Allocates a fresh `Vec` each call.  Use [`Self::flush_into`]
    /// to recycle a caller-owned buffer.
    fn flush(&self) -> Vec<(C, P, f64)> {
        let mut out = Vec::new();
        self.flush_into(&mut out);
        out
    }

    /// Drain the current period's sample into a caller-supplied
    /// buffer, avoiding the per-flush allocation of [`Self::flush`].
    fn flush_into(&self, out: &mut Vec<(C, P, f64)>);
}
