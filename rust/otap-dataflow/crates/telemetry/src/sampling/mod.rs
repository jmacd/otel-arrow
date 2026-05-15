// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Adaptive stratified priority sampling for log events.
//!
//! This module implements a single-threaded, per-stratum
//! [bottom-`m`](https://en.wikipedia.org/wiki/Reservoir_sampling)
//! priority sampler in the style of Duffield, Lund, and Thorup
//! (2007), with adaptive thresholds following Ting (arXiv:1708.04970).
//!
//! # Mechanism
//!
//! Each event arrival draws an independent uniform priority `u`.  Per
//! stratum `c` (the *callsite*, in our intended use), the sampler
//! keeps the `m_c` events with the smallest priorities.  The current
//! skip threshold `τ_c` for stratum `c` is the largest priority that
//! would be evicted by the next admission — concretely, the root of a
//! max-heap of the `m_c + 1` smallest priorities seen so far.  An
//! arriving event with `u ≥ τ_c` is rejected without any further
//! work; this is the *fast skip* path that allows the calling
//! tracing-subscriber layer to short-circuit `enabled()` and avoid
//! formatting the log statement entirely.
//!
//! At the end of each sampling period, the sampler emits the kept
//! events, each annotated with its **priority-sampling weight**
//! `w_i = 1 / max(u_i, τ_c)`.  Sums of `w_i × x_i` over the emitted
//! batch are unbiased estimators of the corresponding sums over the
//! full pre-sampled stream.  Thanks to Ting's *predictable threshold*
//! theorem, this remains true even though `τ_c` evolves online as a
//! function of previously observed events.
//!
//! # Stratification policy
//!
//! The sampler maintains a global *target* sample size `T` and a
//! global *buffer* size `B` (with `B > T`).  It allocates a per-
//! stratum cap `m_c = max(T / K_active, m_min)`, where `K_active` is
//! the number of strata that have produced at least one event in the
//! current period.  When a new stratum first appears, all existing
//! buckets are *shrunk* to the new (smaller) cap by repeatedly
//! popping the largest priority — a step that is amortized
//! `O(log m_c)` per evicted entry.  This produces an "equal expected
//! count per stratum" sample under steady arrival rates, matching the
//! intent that rare callsites be preserved alongside chatty ones.
//!
//! # Concurrency
//!
//! The sampler is *not* `Sync`.  It is intended for single-threaded
//! use behind a thread-local (one instance per dataflow engine
//! core).  All hot-path methods take `&self` with interior mutability
//! via [`std::cell::RefCell`] / [`std::cell::Cell`], allowing it to
//! be used from `tracing::Subscriber::enabled` (which receives
//! `&self`).

mod bucket;
mod probability;
mod sampler;

#[cfg(test)]
mod tests;

pub use bucket::CallsiteBucket;
pub use probability::{PriorityKey, priority_weight};
pub use sampler::{Admission, SamplerConfig, StratifiedSampler};
