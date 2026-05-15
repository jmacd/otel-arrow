// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-stratum bottom-`m` priority bucket.
//!
//! A [`CallsiteBucket`] holds the `m + 1` smallest-priority items
//! seen so far for a single stratum, where `m` is the per-stratum
//! cap.  The extra slot stores the *current threshold candidate* —
//! the largest priority that would be evicted on the next admission
//! — at the heap root.  Reading the threshold is therefore an O(1)
//! peek of the heap root.
//!
//! Holding `m + 1` rather than `m` items is what makes the threshold
//! always available without a separate "max-evicted" tracker, and it
//! is consistent with the textbook description of priority
//! sampling: at flush time, popping the root yields τ, and the
//! remaining `m` items form the sample.
//!
//! See [the module docs](super) for context and weight derivation.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::probability::PriorityKey;

/// One entry in the bucket: a priority and an arbitrary payload `T`.
#[derive(Debug)]
struct Entry<T> {
    u: PriorityKey,
    payload: T,
}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.u == other.u
    }
}
impl<T> Eq for Entry<T> {}
impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Default `BinaryHeap` is a max-heap, which is what we want:
        // the root is the largest priority among the `m + 1` smallest
        // seen so far, i.e., the threshold candidate.
        self.u.cmp(&other.u)
    }
}
impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A bottom-`m` priority bucket for one stratum.
///
/// Capacity is `m + 1`: the heap stores the `m` items currently kept
/// **plus** the threshold candidate at the root.
#[derive(Debug)]
pub struct CallsiteBucket<T> {
    heap: BinaryHeap<Entry<T>>,
    /// Logical cap `m` (number of items to keep at flush).  The heap
    /// is allowed to grow to `m + 1` entries, the extra slot being
    /// the threshold candidate.
    cap: usize,
    /// Total number of admission attempts for this bucket in the
    /// current period (kept + rejected).  Diagnostic only.
    seen: u64,
}

impl<T> CallsiteBucket<T> {
    /// Create a new bucket with cap `m`.  `m` must be ≥ 1.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        assert!(cap >= 1, "CallsiteBucket cap must be >= 1");
        Self {
            heap: BinaryHeap::with_capacity(cap + 1),
            cap,
            seen: 0,
        }
    }

    /// Logical cap `m`.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Total admission attempts this period.
    #[must_use]
    pub fn seen(&self) -> u64 {
        self.seen
    }

    /// Current skip threshold τ for this bucket.
    ///
    /// When the heap holds fewer than `m + 1` items the bucket is
    /// not yet saturated and the threshold is [`PriorityKey::MAX`]
    /// (i.e., admit everything).  Otherwise the root of the max-heap
    /// is the threshold.
    #[inline]
    #[must_use]
    pub fn threshold(&self) -> PriorityKey {
        if self.heap.len() <= self.cap {
            PriorityKey::MAX
        } else {
            // Saturated: heap.len() == cap + 1, root is the largest
            // of the `m + 1` smallest priorities.
            self.heap.peek().expect("non-empty heap").u
        }
    }

    /// True when the bucket is at capacity and would evict on the
    /// next admission.
    #[inline]
    #[must_use]
    pub fn is_saturated(&self) -> bool {
        self.heap.len() > self.cap
    }

    /// Try to admit `(u, payload)`.  Returns `true` if the entry was
    /// inserted (kept, at least for now), `false` if rejected
    /// because `u >= threshold`.
    ///
    /// The caller is responsible for incrementing `seen` regardless
    /// of outcome via [`record_attempt`](Self::record_attempt) — we
    /// keep the two operations separate so that the fast skip path
    /// (which calls only [`threshold`](Self::threshold) and
    /// [`record_attempt`](Self::record_attempt)) does not have to
    /// touch the heap at all.
    #[must_use = "ignoring the admission outcome may indicate a logic error"]
    pub fn try_admit(&mut self, u: PriorityKey, payload: T) -> bool {
        if u >= self.threshold() {
            return false;
        }
        self.heap.push(Entry { u, payload });
        // Maintain |heap| ≤ cap + 1 by evicting the (now-larger) root
        // if we overflowed.
        if self.heap.len() > self.cap + 1 {
            // Should be impossible since we add one at a time, but
            // guard against future cap shrinks racing with admit.
            let _ = self.heap.pop();
        }
        true
    }

    /// Record one admission attempt for diagnostics.  Always called
    /// by the sampler regardless of whether the event was admitted.
    #[inline]
    pub fn record_attempt(&mut self) {
        self.seen = self.seen.saturating_add(1);
    }

    /// Shrink the cap to `new_cap`, evicting the largest priorities
    /// until the heap holds at most `new_cap + 1` items.
    ///
    /// Used by the sampler when a new stratum first appears in a
    /// period and the global target sample size must be redivided
    /// among more strata.  Each evicted item is dropped (for the
    /// current period it is as if it were rejected).
    pub fn shrink_to(&mut self, new_cap: usize) {
        assert!(new_cap >= 1, "shrink_to cap must be >= 1");
        if new_cap >= self.cap {
            // Growing or unchanged: no items to evict.  We allow
            // grow-in-place because shrink_to is also the API used
            // when K_active *decreases* (rare, but possible if the
            // sampler is reset).
            self.cap = new_cap;
            return;
        }
        self.cap = new_cap;
        while self.heap.len() > new_cap + 1 {
            let _ = self.heap.pop();
        }
    }

    /// Drain the bucket, returning the kept items paired with their
    /// per-item weight `1 / max(u, τ)`.
    ///
    /// Calling this consumes the bucket's contents and resets `seen`
    /// to zero, leaving an empty bucket ready for the next period
    /// with the same cap.
    pub fn drain_with_weights(&mut self) -> Vec<(T, f64)> {
        let cap = self.cap;
        let saturated = self.heap.len() > cap;
        // For a saturated bucket, the root is τ; pop it and discard
        // (it is the (m+1)-th smallest item, which by definition was
        // the boundary "rejected" item).  For an unsaturated bucket,
        // every item kept represents only itself.
        let threshold = if saturated {
            self.heap.pop().expect("saturated heap pops root").u
        } else {
            PriorityKey::MAX
        };
        let mut out = Vec::with_capacity(self.heap.len());
        for Entry { u, payload } in self.heap.drain() {
            let denom = u.value().max(threshold.value());
            out.push((payload, 1.0 / denom));
        }
        self.seen = 0;
        out
    }
}
