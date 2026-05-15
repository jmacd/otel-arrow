// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Priority-sampling primitives.
//!
//! This module defines the numeric core shared by the bucket and the
//! sampler: a [`PriorityKey`] newtype wrapping a `u ∈ [0, 1)` random
//! priority with a total ordering, plus the [`priority_weight`]
//! function that converts a kept item's priority and its bucket's
//! final threshold into a Horvitz–Thompson inverse-probability
//! weight.
//!
//! ## Weight derivation
//!
//! Under bottom-`m` priority sampling, the kept items in a stratum
//! are those whose priority `u` is among the `m` smallest seen.
//! Equivalently, an item `i` is kept iff `u_i < τ`, where `τ` is the
//! `(m+1)`-th smallest priority observed in that stratum (the
//! threshold).  The unbiased weight is
//!
//! ```text
//!     w_i = 1 / max(u_i, τ)
//! ```
//!
//! For all *kept* items we have `u_i < τ`, so `max(u_i, τ) = τ` and
//! every kept item in a saturated bucket receives the same weight
//! `1 / τ`.  When a bucket never saturated (it saw `≤ m_c` events in
//! the period), there is no rejected item and `τ` is effectively
//! `1.0`, so each kept item is weighted by `1.0` (it represents only
//! itself — sampling was the identity).

use std::cmp::Ordering;

/// A priority value in `[0, 1)` drawn from a uniform RNG.
///
/// Wraps an `f64` with a [`Ord`] implementation that is total over
/// the half-open interval `[0, 1)`.  By construction priorities are
/// always finite, so we forbid NaN at construction; the bit
/// representation of non-negative finite floats is monotone with
/// their numeric value, which we exploit to give a cheap branch-free
/// comparison.
#[derive(Copy, Clone, Debug)]
pub struct PriorityKey(f64);

impl PriorityKey {
    /// Construct from a raw `u` value.  Panics in debug builds if
    /// `u` is not in `[0, 1)`; in release builds the value is
    /// clamped (a defensive choice — RNG outputs from
    /// [`rand::Rng::random`] for `f64` are documented to be in
    /// `[0, 1)`, so panics indicate caller error).
    #[inline]
    #[must_use]
    pub fn new(u: f64) -> Self {
        debug_assert!(
            (0.0..1.0).contains(&u),
            "PriorityKey expects u in [0, 1), got {u}"
        );
        Self(u.clamp(0.0, f64::from_bits(0x3FEFFFFFFFFFFFFF)))
    }

    /// The largest possible priority value — used as the saturation
    /// threshold for a never-saturated bucket.  Numerically `1.0`
    /// minus one ULP, so all valid keys are strictly less than it.
    pub const MAX: PriorityKey = PriorityKey(f64::from_bits(0x3FEFFFFFFFFFFFFF));

    /// Underlying `f64`.
    #[inline]
    #[must_use]
    pub fn value(self) -> f64 {
        self.0
    }
}

impl PartialEq for PriorityKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for PriorityKey {}

impl Ord for PriorityKey {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Safe: priorities are non-negative finite, so bit ordering
        // matches numeric ordering.
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}

impl PartialOrd for PriorityKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Compute the priority-sampling weight `1 / max(u, τ)`.
///
/// `u` is the kept item's priority.  `threshold` is the bucket's
/// final τ — for a saturated bucket, the priority of the single
/// item evicted at the cap (the `(m+1)`-th smallest); for a never-
/// saturated bucket, [`PriorityKey::MAX`] (yielding a weight of
/// `≈ 1.0`).
#[inline]
#[must_use]
pub fn priority_weight(u: PriorityKey, threshold: PriorityKey) -> f64 {
    let denom = u.value().max(threshold.value());
    1.0 / denom
}
