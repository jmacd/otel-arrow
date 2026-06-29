// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Event-time tumbling windows and per-stream watermarks: the foundation of the
//! durable metrics appliance's L2 aggregation (phase A0, decisions D10-D12 in
//! `docs/metrics-appliance-design.md`).
//!
//! This module is the pure, signal-agnostic windowing core in event-time
//! nanoseconds. It does not touch OTAP; [`WindowedAggregators`] is the generic
//! `(stream, window)` aggregation state machine, and a processor layers OTAP
//! event-time extraction, stream-identity keying, and the OTel
//! reset/gap/overlap rules (D17) on top.
//!
//! - [`TumblingWindows`] is the window geometry: non-overlapping, fixed-width,
//!   **epoch-aligned** windows over event time (D10), so two appliances and the
//!   central platform produce mergeable buckets for the same series.
//! - [`Watermark`] is the per-stream completeness estimate: `max(observed
//!   event_time) - allowed_lateness`, with a `processing_time - max_lag` idle
//!   floor so idle streams still close (D11).
//! - [`WatermarkPolicy`] turns a watermark into the v1 triggers (D12): a window
//!   is complete when the watermark reaches `window_end + allowed_lateness`
//!   (single on-time firing), and a point is late when its event time is below
//!   the watermark (drop-and-count in v1).

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

/// One epoch-aligned tumbling window over event time, the half-open interval
/// `[start_nanos, end_nanos)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// `floor(event_time / window_size)`; identifies the window across producers.
    pub index: i64,
    /// Inclusive start of the window, in nanoseconds since the Unix epoch.
    pub start_nanos: i64,
    /// Exclusive end of the window, in nanoseconds since the Unix epoch.
    pub end_nanos: i64,
}

impl Window {
    /// Whether `event_time_nanos` falls in this window (`start <= t < end`).
    #[must_use]
    pub fn contains(&self, event_time_nanos: i64) -> bool {
        self.start_nanos <= event_time_nanos && event_time_nanos < self.end_nanos
    }
}

/// Epoch-aligned tumbling windows of a fixed width (D10). Window boundaries are
/// aligned to the Unix epoch rather than to first-seen time, so the same series
/// produces identical buckets on every appliance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TumblingWindows {
    size_nanos: i64,
}

impl TumblingWindows {
    /// Create windows of `size_nanos` width. A non-positive size is clamped to 1
    /// nanosecond.
    #[must_use]
    pub fn new(size_nanos: i64) -> Self {
        Self {
            size_nanos: size_nanos.max(1),
        }
    }

    /// The window width in nanoseconds.
    #[must_use]
    pub fn size_nanos(&self) -> i64 {
        self.size_nanos
    }

    /// The index `floor(event_time / size)` of the window containing
    /// `event_time_nanos` (floor division so it is correct for any sign).
    #[must_use]
    pub fn index_of(&self, event_time_nanos: i64) -> i64 {
        event_time_nanos.div_euclid(self.size_nanos)
    }

    /// The window containing `event_time_nanos`.
    #[must_use]
    pub fn window(&self, event_time_nanos: i64) -> Window {
        self.window_by_index(self.index_of(event_time_nanos))
    }

    /// The window with the given index.
    #[must_use]
    pub fn window_by_index(&self, index: i64) -> Window {
        let start = index.saturating_mul(self.size_nanos);
        Window {
            index,
            start_nanos: start,
            end_nanos: start.saturating_add(self.size_nanos),
        }
    }
}

/// Per-stream watermark state: the maximum event time observed for one stream
/// (D11). Bounded by the processor's cardinality limit, one per stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Watermark {
    max_event_nanos: Option<i64>,
}

impl Watermark {
    /// A watermark that has observed no events yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observed event time, advancing the maximum.
    pub fn observe(&mut self, event_time_nanos: i64) {
        self.max_event_nanos = Some(match self.max_event_nanos {
            Some(current) => current.max(event_time_nanos),
            None => event_time_nanos,
        });
    }

    /// The maximum event time observed, or `None` if the stream is idle.
    #[must_use]
    pub fn max_event(&self) -> Option<i64> {
        self.max_event_nanos
    }
}

/// The watermark and trigger policy for v1 windowing (D11/D12). `allowed_lateness`
/// is the per-stream out-of-orderness bound and never need exceed the ingest
/// queue's `max_lag`; `max_lag` is the idle floor that closes idle streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatermarkPolicy {
    /// How far below the maximum observed event time the watermark trails, and
    /// the on-time firing offset past a window's end.
    pub allowed_lateness_nanos: i64,
    /// The idle floor: the watermark is at least `processing_time - max_lag`, so
    /// an idle stream's open windows still close.
    pub max_lag_nanos: i64,
}

impl WatermarkPolicy {
    /// The watermark value for a stream at `processing_time_nanos`:
    /// `max(max_event - allowed_lateness, processing_time - max_lag)` (D11). With
    /// no observed event the value is the idle floor.
    #[must_use]
    pub fn watermark(&self, watermark: &Watermark, processing_time_nanos: i64) -> i64 {
        let idle_floor = processing_time_nanos.saturating_sub(self.max_lag_nanos);
        match watermark.max_event() {
            Some(max_event) => max_event
                .saturating_sub(self.allowed_lateness_nanos)
                .max(idle_floor),
            None => idle_floor,
        }
    }

    /// Whether `window` is complete and should fire: the watermark has reached
    /// `window_end + allowed_lateness` (D12 single on-time trigger).
    #[must_use]
    pub fn is_complete(&self, window: &Window, watermark: i64) -> bool {
        watermark >= window.end_nanos.saturating_add(self.allowed_lateness_nanos)
    }

    /// Whether a point with `event_time_nanos` is late -- it belongs to a window
    /// the watermark has already passed (`event_time < watermark`). In v1 such a
    /// point is dropped and counted (D12).
    #[must_use]
    pub fn is_late(&self, event_time_nanos: i64, watermark: i64) -> bool {
        event_time_nanos < watermark
    }
}

/// One stream's open windows plus its watermark state.
struct StreamState<A> {
    watermark: Watermark,
    /// Open windows, `window_index -> aggregate`, ascending by index.
    open: BTreeMap<i64, A>,
}

impl<A> Default for StreamState<A> {
    fn default() -> Self {
        Self {
            watermark: Watermark::default(),
            open: BTreeMap::new(),
        }
    }
}

/// The event-time windowing state machine: one aggregate `A` per `(stream,
/// window)` plus a per-stream watermark, driving the v1 triggers (D10-D12).
///
/// It is generic over the stream key `K` and the per-window aggregate `A`, so a
/// processor layers OTAP event-time extraction, stream-identity keying, and the
/// OTel reset/gap/overlap and cumulative-conversion rules (D17) on top:
/// [`admit`](WindowedAggregators::admit) hands back the aggregate to fold a data
/// point into (or drops a late point), and
/// [`drain_complete`](WindowedAggregators::drain_complete) yields the windows the
/// watermark has closed. Per-stream watermark state is bounded by the
/// processor's cardinality limit; there is no time-based GC in v1, so a stream's
/// watermark persists to keep rejecting late points for windows that already
/// fired.
pub struct WindowedAggregators<K, A> {
    windows: TumblingWindows,
    policy: WatermarkPolicy,
    streams: HashMap<K, StreamState<A>>,
    late_points: u64,
}

impl<K, A> WindowedAggregators<K, A> {
    /// Create a windower with the given window geometry and watermark policy.
    #[must_use]
    pub fn new(windows: TumblingWindows, policy: WatermarkPolicy) -> Self {
        Self {
            windows,
            policy,
            streams: HashMap::new(),
            late_points: 0,
        }
    }

    /// The number of late points dropped so far (D12 drop-and-count).
    #[must_use]
    pub fn late_points(&self) -> u64 {
        self.late_points
    }

    /// The number of streams with open state -- a proxy for active series.
    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// The number of open `(stream, window)` aggregators across all streams.
    #[must_use]
    pub fn open_window_count(&self) -> usize {
        self.streams.values().map(|s| s.open.len()).sum()
    }

    /// Whether no stream has any state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }
}

impl<K: Hash + Eq + Clone, A: Default> WindowedAggregators<K, A> {
    /// Route an event for `stream` at `event_time_nanos` to its window's
    /// aggregate, creating the aggregate if needed, and return a mutable
    /// reference for the caller to fold the data point into.
    ///
    /// Returns `None` when the point is **late** -- its event time is below the
    /// stream's current watermark, so it belongs to a window that already fired;
    /// the late-point counter is incremented (drop-and-count, D12). Admitting a
    /// point advances the stream's watermark.
    pub fn admit(
        &mut self,
        stream: K,
        event_time_nanos: i64,
        processing_time_nanos: i64,
    ) -> Option<&mut A> {
        let windows = self.windows;
        let policy = self.policy;
        let state = self.streams.entry(stream).or_default();

        // Lateness is judged against the watermark *before* this point advances
        // it; an admitted point cannot be late relative to its own arrival.
        let watermark = policy.watermark(&state.watermark, processing_time_nanos);
        if policy.is_late(event_time_nanos, watermark) {
            self.late_points = self.late_points.saturating_add(1);
            return None;
        }

        state.watermark.observe(event_time_nanos);
        Some(
            state
                .open
                .entry(windows.index_of(event_time_nanos))
                .or_default(),
        )
    }

    /// Remove and return every `(stream, window, aggregate)` whose window the
    /// watermark has closed at `processing_time_nanos` -- the watermark has
    /// reached `window_end + allowed_lateness` (D12 single on-time firing). The
    /// idle floor lets an idle stream's windows close even with no new points.
    pub fn drain_complete(&mut self, processing_time_nanos: i64) -> Vec<(K, Window, A)> {
        let windows = self.windows;
        let policy = self.policy;
        let mut fired = Vec::new();
        for (key, state) in self.streams.iter_mut() {
            let watermark = policy.watermark(&state.watermark, processing_time_nanos);
            let complete: Vec<i64> = state
                .open
                .keys()
                .copied()
                .take_while(|&index| policy.is_complete(&windows.window_by_index(index), watermark))
                .collect();
            for index in complete {
                if let Some(aggregate) = state.open.remove(&index) {
                    fired.push((key.clone(), windows.window_by_index(index), aggregate));
                }
            }
        }
        fired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000_000;

    #[test]
    fn windows_are_epoch_aligned() {
        let w = TumblingWindows::new(10 * SEC);
        assert_eq!(w.index_of(0), 0);
        assert_eq!(w.index_of(10 * SEC - 1), 0);
        assert_eq!(w.index_of(10 * SEC), 1);
        assert_eq!(w.index_of(25 * SEC), 2);

        let win = w.window(25 * SEC);
        assert_eq!(win.index, 2);
        assert_eq!(win.start_nanos, 20 * SEC);
        assert_eq!(win.end_nanos, 30 * SEC);
        assert!(win.contains(25 * SEC));
        assert!(win.contains(20 * SEC));
        assert!(!win.contains(30 * SEC));
        assert_eq!(win.end_nanos - win.start_nanos, w.size_nanos());
    }

    #[test]
    fn window_by_index_round_trips() {
        let w = TumblingWindows::new(60 * SEC);
        for t in [0, 59 * SEC, 60 * SEC, 121 * SEC, 3600 * SEC] {
            let win = w.window(t);
            assert_eq!(w.window_by_index(win.index), win);
            assert!(win.contains(t));
        }
    }

    #[test]
    fn non_positive_size_is_clamped() {
        assert_eq!(TumblingWindows::new(0).size_nanos(), 1);
        assert_eq!(TumblingWindows::new(-5).size_nanos(), 1);
    }

    #[test]
    fn watermark_tracks_max_event() {
        let mut wm = Watermark::new();
        assert_eq!(wm.max_event(), None);
        wm.observe(100);
        wm.observe(50);
        wm.observe(200);
        wm.observe(150);
        assert_eq!(wm.max_event(), Some(200));
    }

    #[test]
    fn watermark_value_trails_max_event_by_allowed_lateness() {
        let policy = WatermarkPolicy {
            allowed_lateness_nanos: 5 * SEC,
            max_lag_nanos: 3600 * SEC,
        };
        let mut wm = Watermark::new();
        wm.observe(100 * SEC);
        // processing time is recent, so the idle floor (processing - 1h) is far
        // below max_event - allowed_lateness.
        let processing = 100 * SEC;
        assert_eq!(policy.watermark(&wm, processing), 95 * SEC);
    }

    #[test]
    fn idle_floor_advances_a_stale_stream() {
        let policy = WatermarkPolicy {
            allowed_lateness_nanos: 5 * SEC,
            max_lag_nanos: 60 * SEC,
        };
        // A stream whose last event is old; wall clock has moved on.
        let mut wm = Watermark::new();
        wm.observe(100 * SEC);
        let processing = 1000 * SEC;
        // max_event - lateness = 95s; idle floor = 1000 - 60 = 940s wins.
        assert_eq!(policy.watermark(&wm, processing), 940 * SEC);
        // With no observed event at all, the value is the idle floor.
        assert_eq!(policy.watermark(&Watermark::new(), processing), 940 * SEC);
    }

    #[test]
    fn completion_trigger_waits_for_window_end_plus_lateness() {
        let windows = TumblingWindows::new(10 * SEC);
        let policy = WatermarkPolicy {
            allowed_lateness_nanos: 2 * SEC,
            max_lag_nanos: 3600 * SEC,
        };
        let win = windows.window(5 * SEC); // [0, 10s)
        // Fires only once the watermark reaches end (10s) + allowed_lateness (2s).
        assert!(!policy.is_complete(&win, 11 * SEC));
        assert!(!policy.is_complete(&win, 12 * SEC - 1));
        assert!(policy.is_complete(&win, 12 * SEC));
        assert!(policy.is_complete(&win, 100 * SEC));
    }

    #[test]
    fn lateness_is_below_the_watermark() {
        let policy = WatermarkPolicy {
            allowed_lateness_nanos: 5 * SEC,
            max_lag_nanos: 3600 * SEC,
        };
        let watermark = 50 * SEC;
        assert!(policy.is_late(50 * SEC - 1, watermark));
        assert!(!policy.is_late(50 * SEC, watermark));
        assert!(!policy.is_late(60 * SEC, watermark));
    }

    fn windower() -> WindowedAggregators<&'static str, u64> {
        WindowedAggregators::new(
            TumblingWindows::new(10 * SEC),
            WatermarkPolicy {
                allowed_lateness_nanos: 2 * SEC,
                max_lag_nanos: 3600 * SEC,
            },
        )
    }

    /// Fold a point into its window aggregate; returns false if it was late.
    fn fold(
        w: &mut WindowedAggregators<&'static str, u64>,
        s: &'static str,
        t: i64,
        v: u64,
    ) -> bool {
        match w.admit(s, t, t) {
            Some(agg) => {
                *agg += v;
                true
            }
            None => false,
        }
    }

    #[test]
    fn admit_routes_points_to_per_stream_window_aggregates() {
        let mut w = windower();
        // Stream "a": two points in window 0, one in window 1.
        assert!(fold(&mut w, "a", 2 * SEC, 10));
        assert!(fold(&mut w, "a", 8 * SEC, 5));
        assert!(fold(&mut w, "a", 15 * SEC, 7));
        // Stream "b": one point in window 0.
        assert!(fold(&mut w, "b", 3 * SEC, 100));

        assert_eq!(w.stream_count(), 2);
        assert_eq!(w.open_window_count(), 3); // a:{0,1}, b:{0}
        assert_eq!(w.late_points(), 0);
    }

    #[test]
    fn late_points_are_dropped_and_counted() {
        let mut w = windower();
        assert!(fold(&mut w, "a", 100 * SEC, 1)); // max_event = 100s
        // 99s is within allowed_lateness (2s) of the max -> admitted.
        assert!(fold(&mut w, "a", 99 * SEC, 1));
        // 50s is far behind the max -> late (50 < watermark 98s).
        assert!(!fold(&mut w, "a", 50 * SEC, 999));
        assert_eq!(w.late_points(), 1);
    }

    #[test]
    fn drain_emits_complete_windows_and_keeps_incomplete() {
        let mut w = windower();
        let _ = fold(&mut w, "a", 5 * SEC, 10); // window 0 = [0,10s)
        let _ = fold(&mut w, "a", 55 * SEC, 20); // window 5 = [50,60s), max_event=55s
        assert_eq!(w.open_window_count(), 2);

        // Watermark = 55 - 2 = 53s. Window 0 fires (53 >= 10+2); window 5 does
        // not (53 < 60+2).
        let fired = w.drain_complete(55 * SEC);
        assert_eq!(fired.len(), 1);
        let (stream, window, agg) = &fired[0];
        assert_eq!(*stream, "a");
        assert_eq!(window.index, 0);
        assert_eq!(*agg, 10);
        assert_eq!(w.open_window_count(), 1); // window 5 still open
    }

    #[test]
    fn idle_stream_window_closes_via_idle_floor() {
        let mut w = windower();
        let _ = fold(&mut w, "b", 100 * SEC, 42); // window 10 = [100,110s)
        // No new points. Wall clock advances far; idle floor = 1000 - 3600 < 0
        // here, so use a tighter max_lag windower to exercise the floor.
        let mut w2 = WindowedAggregators::<&'static str, u64>::new(
            TumblingWindows::new(10 * SEC),
            WatermarkPolicy {
                allowed_lateness_nanos: 2 * SEC,
                max_lag_nanos: 60 * SEC,
            },
        );
        let _ = w2.admit("b", 100 * SEC, 100 * SEC).map(|a| *a += 42);
        // Idle floor at processing 1000s = 1000 - 60 = 940s, far past window 10's
        // end (110s) + lateness, so the idle window closes with no new points.
        let fired = w2.drain_complete(1000 * SEC);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].1.index, 10);
        assert!(w2.is_empty() || w2.open_window_count() == 0);
        // The first windower (max_lag 3600s) keeps the window open at 200s.
        assert_eq!(w.drain_complete(200 * SEC).len(), 0);
    }
}
