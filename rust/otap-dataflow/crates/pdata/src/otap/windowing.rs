// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Event-time tumbling windows and per-stream watermarks: the foundation of the
//! durable metrics appliance's L2 aggregation (phase A0, decisions D10-D12 in
//! `docs/metrics-appliance-design.md`).
//!
//! This module is the pure, signal-agnostic windowing core in event-time
//! nanoseconds. It does not touch OTAP or aggregation state; a processor layers
//! per-`(stream, window)` aggregators, OTAP event-time extraction, and the OTel
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
}
