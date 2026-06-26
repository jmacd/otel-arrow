// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pure global-reservoir window logic (agent side of two-level sampling).
//!
//! The agent receives the representatives chosen by many SDK-side
//! `log_sampler` instances. Each representative carries a Horvitz-Thompson
//! count `nhat_c` under [`OTEL_SAMPLING_NHAT`]. This module:
//!
//! 1. **Accumulates** the exact global per-callsite count
//!    `N_c = sum of nhat_c` across all representatives in the window.
//! 2. **Runs a key-only bottom-`(k+1)` reservoir** keyed by
//!    `-ln(u) * N_prev_c` to estimate the global threshold `tau^G`. Using the
//!    *previous* window's counts as weights keeps the gate stable and avoids a
//!    second pass over the data.
//! 3. On window close, **publishes** a [`GlobalTable`]: `tau^G`, the rarest
//!    floor `g_unseen = 1 / min_c N_c`, and the top-`K'` heavy hitters with
//!    `g_c = 1 / N_c`.
//!
//! The reservoir holds only callsite *keys* (not records), so the agent never
//! buffers payloads. Representatives are forwarded downstream by the processor
//! after the `nhat` attribute is stripped.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use otap_df_otap::sampling::{GlobalTable, HeavyHitter, OTEL_SAMPLING_NHAT, callsite_id};
use otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value;
use otap_df_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData};

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// A total-ordering wrapper over `f64` reservoir keys (max-heap order).
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdKey(f64);

impl Eq for OrdKey {}
impl PartialOrd for OrdKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Summary statistics for a closed global window.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GlobalStats {
    /// Distinct callsites observed this window.
    pub distinct_callsites: u64,
    /// Number of heavy hitters published.
    pub heavy_hitters: u64,
    /// The published global threshold `tau^G`.
    pub tau_g: f64,
    /// The published rarest-seen floor `g_unseen`.
    pub g_unseen: f64,
}

/// Weighted, key-only bottom-`(k+1)` global reservoir over a window.
pub(crate) struct GlobalWindow {
    /// Reservoir size `k` (the spare `+1` slot estimates `tau^G`).
    k: usize,
    /// Maximum number of heavy hitters to publish (`K'`).
    k_prime: usize,
    /// Exact per-callsite global count `N_c` for the current window.
    n_curr: HashMap<u64, f64>,
    /// EWMA-smoothed per-callsite count carried from prior windows. Used as the
    /// reservoir weight during observation and, after folding in the current
    /// window on close, as the basis for the published heavy-hitter weights.
    n_smoothed: HashMap<u64, f64>,
    /// Smoothed weight applied to callsites absent from `n_smoothed`
    /// (the rarest-seen smoothed count).
    n_unseen_smoothed: f64,
    /// EWMA smoothing factor `alpha` in `(0, 1]`. `1.0` disables smoothing
    /// (each window replaces the previous counts outright); smaller values
    /// blend in history so a heavy hitter persists across a quiet window.
    count_smoothing: f64,
    /// Bottom-`(k+1)` keys for this window.
    keys: BinaryHeap<OrdKey>,
    /// Monotonic table version; `0` is reserved for the absent table.
    version: u32,
    rng: SmallRng,
}

impl GlobalWindow {
    /// Create a new global window with reservoir size `k`, heavy-hitter budget
    /// `k_prime`, and EWMA count-smoothing factor `count_smoothing` (`1.0`
    /// disables smoothing).
    pub(crate) fn new(k: usize, k_prime: usize, count_smoothing: f64) -> Self {
        let seed = rand::rng().random::<u64>();
        Self {
            k,
            k_prime,
            n_curr: HashMap::new(),
            n_smoothed: HashMap::new(),
            n_unseen_smoothed: 1.0,
            count_smoothing,
            keys: BinaryHeap::new(),
            version: 0,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    /// Smoothed weight for a callsite (rarest-seen smoothed weight if unseen).
    fn smoothed_weight(&self, callsite: u64) -> f64 {
        self.n_smoothed
            .get(&callsite)
            .copied()
            .unwrap_or(self.n_unseen_smoothed)
    }

    /// Insert a fresh reservoir key for `weight`, keeping only the smallest
    /// `k + 1` keys.
    fn push_key(&mut self, weight: f64) {
        // u in (0, 1]; key = -ln(u) * weight is an exponential race variable.
        let u = 1.0 - self.rng.random::<f64>();
        let key = -u.ln() * weight;
        self.keys.push(OrdKey(key));
        while self.keys.len() > self.k + 1 {
            let _ = self.keys.pop();
        }
    }

    /// Observe a decoded logs payload: accumulate global counts, feed the
    /// reservoir, and strip the `nhat` attribute from each record in place.
    /// Returns the number of representative records observed.
    pub(crate) fn observe_and_strip(&mut self, logs: &mut LogsData) -> u64 {
        let mut count = 0u64;
        for rl in &mut logs.resource_logs {
            for sl in &mut rl.scope_logs {
                let scope_name = sl.scope.as_ref().map_or("", |s| s.name.as_str());
                for rec in &mut sl.log_records {
                    count += 1;
                    let callsite = callsite_id(scope_name, rec);
                    let nhat = take_nhat(rec);
                    *self.n_curr.entry(callsite).or_insert(0.0) += nhat;
                    let weight = self.smoothed_weight(callsite);
                    self.push_key(weight);
                }
            }
        }
        count
    }

    /// Close the window. Returns the published table and stats, or `None` when
    /// no representatives were observed (the previous table stays in force).
    pub(crate) fn close(&mut self) -> Option<(GlobalTable, GlobalStats)> {
        if self.n_curr.is_empty() {
            self.keys.clear();
            return None;
        }

        // tau^G: pop the spare (k+1)-th key when the reservoir overfilled,
        // otherwise the gate is slack.
        let tau_g = if self.keys.len() > self.k {
            self.keys.pop().map_or(f64::INFINITY, |k| k.0)
        } else {
            f64::INFINITY
        };

        // Rarest-seen floor: g_unseen = max_c g_c = 1 / min_c N_c.
        let min_n = self
            .n_curr
            .values()
            .copied()
            .fold(f64::INFINITY, f64::min);

        // Fold this window's exact counts into the EWMA-smoothed counts. At
        // alpha = 1.0 this is a plain replacement (smoothed == this window's
        // exact counts); smaller alpha blends in history so a heavy hitter
        // persists across a transiently quiet window.
        let alpha = self.count_smoothing;
        let n_curr = std::mem::take(&mut self.n_curr);
        let distinct_callsites = n_curr.len() as u64;

        let mut smoothed: HashMap<u64, f64> = HashMap::with_capacity(n_curr.len());
        // Decay/blend callsites carried from prior windows.
        for (&c, &prev) in &self.n_smoothed {
            let raw = n_curr.get(&c).copied().unwrap_or(0.0);
            let _ = smoothed.insert(c, alpha * raw + (1.0 - alpha) * prev);
        }
        // Introduce callsites seen for the first time (no prior smoothed value).
        for (&c, &raw) in &n_curr {
            let _ = smoothed.entry(c).or_insert(alpha * raw);
        }
        // Bound memory: drop decayed callsites that fell below the rarest count
        // actually seen this window (dominated for ranking and the gate), but
        // always keep callsites observed this window.
        smoothed.retain(|c, v| n_curr.contains_key(c) || *v >= min_n);

        // Rarest-seen floor over the smoothed counts.
        let min_smoothed = smoothed.values().copied().fold(f64::INFINITY, f64::min);
        let g_unseen = 1.0 / min_smoothed;

        // Heavy hitters: top K' callsites by smoothed global count.
        let mut by_count: Vec<(u64, f64)> = smoothed.iter().map(|(&c, &n)| (c, n)).collect();
        by_count.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        by_count.truncate(self.k_prime);
        let heavy: Vec<HeavyHitter> = by_count
            .iter()
            .map(|&(callsite, n)| HeavyHitter {
                callsite,
                g_c: 1.0 / n,
            })
            .collect();

        self.version = self.version.wrapping_add(1);
        if self.version == 0 {
            // Never publish the reserved absent version.
            self.version = 1;
        }

        let stats = GlobalStats {
            distinct_callsites,
            heavy_hitters: heavy.len() as u64,
            tau_g,
            g_unseen,
        };
        let table = GlobalTable {
            version: self.version,
            tau_g,
            g_unseen,
            heavy,
        };

        // Roll the window forward: smoothed counts become next window's
        // reservoir weights; the rarest smoothed count is the unseen weight.
        self.n_smoothed = smoothed;
        self.n_unseen_smoothed = min_smoothed;
        self.keys.clear();

        Some((table, stats))
    }
}

/// Remove the `nhat` attribute from a record and return its value, defaulting
/// to `1.0` when absent or malformed (an unannotated record counts as one).
fn take_nhat(rec: &mut LogRecord) -> f64 {
    let Some(pos) = rec
        .attributes
        .iter()
        .position(|kv| kv.key == OTEL_SAMPLING_NHAT)
    else {
        return 1.0;
    };
    let kv = rec.attributes.remove(pos);
    match kv.value.and_then(|v| v.value) {
        Some(Value::DoubleValue(d)) if d.is_finite() && d > 0.0 => d,
        Some(Value::IntValue(i)) if i > 0 => i as f64,
        _ => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue,
    };
    use otap_df_pdata::proto::opentelemetry::logs::v1::{ResourceLogs, ScopeLogs};

    fn rec(event: &str, nhat: Option<f64>) -> LogRecord {
        let mut r = LogRecord {
            event_name: event.to_string(),
            ..Default::default()
        };
        if let Some(n) = nhat {
            r.attributes.push(KeyValue {
                key: OTEL_SAMPLING_NHAT.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(n)),
                }),
            });
        }
        r
    }

    fn logs(records: Vec<LogRecord>) -> LogsData {
        LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "scope".to_string(),
                        ..Default::default()
                    }),
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn accumulates_counts_and_strips_nhat() {
        let mut w = GlobalWindow::new(8, 4, 1.0);
        let mut data = logs(vec![
            rec("a", Some(10.0)),
            rec("a", Some(5.0)),
            rec("b", Some(2.0)),
            rec("c", None),
        ]);
        let observed = w.observe_and_strip(&mut data);
        assert_eq!(observed, 4);
        // nhat attribute stripped from every record.
        for rl in &data.resource_logs {
            for sl in &rl.scope_logs {
                for r in &sl.log_records {
                    assert!(r.attributes.iter().all(|kv| kv.key != OTEL_SAMPLING_NHAT));
                }
            }
        }
        let (table, stats) = w.close().expect("non-empty window publishes");
        assert_eq!(stats.distinct_callsites, 3);
        assert!(table.version >= 1);
        // 'a' is the heaviest (15) -> smallest g_c.
        let a = callsite_id("scope", &rec("a", None));
        let b = callsite_id("scope", &rec("b", None));
        assert!(table.g_for(a) < table.g_for(b));
        // g_unseen is the rarest floor 1/min_N. Callsite 'c' has nhat=None ->
        // count 1, so min_N = 1 and g_unseen = 1.0.
        assert!((table.g_unseen - 1.0).abs() < 1e-12);
    }

    #[test]
    fn empty_window_publishes_nothing() {
        let mut w = GlobalWindow::new(4, 2, 1.0);
        assert!(w.close().is_none());
    }

    #[test]
    fn heavy_hitter_budget_is_respected() {
        let mut w = GlobalWindow::new(16, 2, 1.0);
        let mut data = logs(vec![
            rec("a", Some(100.0)),
            rec("b", Some(50.0)),
            rec("c", Some(25.0)),
            rec("d", Some(10.0)),
        ]);
        let _ = w.observe_and_strip(&mut data);
        let (table, stats) = w.close().expect("publishes");
        assert_eq!(stats.heavy_hitters, 2);
        assert_eq!(table.heavy.len(), 2);
        // The two heaviest are 'a' and 'b'.
        let a = callsite_id("scope", &rec("a", None));
        let b = callsite_id("scope", &rec("b", None));
        let listed: Vec<u64> = table.heavy.iter().map(|h| h.callsite).collect();
        assert!(listed.contains(&a));
        assert!(listed.contains(&b));
    }

    #[test]
    fn ewma_persists_a_heavy_hitter_across_a_quiet_window() {
        // alpha = 0.5: a callsite that goes quiet decays gradually rather than
        // dropping to zero, so its smoothed count (and heavy-hitter status)
        // persists for a window.
        let mut w = GlobalWindow::new(16, 4, 0.5);
        let a = callsite_id("scope", &rec("a", None));

        // Window 1: 'a' is very heavy.
        let mut d1 = logs(vec![rec("a", Some(100.0)), rec("b", Some(1.0))]);
        let _ = w.observe_and_strip(&mut d1);
        let (t1, _) = w.close().expect("publishes");
        let g_a_1 = t1.g_for(a);

        // Window 2: 'a' is absent; only 'b' is seen. With smoothing, 'a' should
        // still be carried (decayed) as a heavy hitter.
        let mut d2 = logs(vec![rec("b", Some(1.0))]);
        let _ = w.observe_and_strip(&mut d2);
        let (t2, _) = w.close().expect("publishes");
        assert!(
            t2.heavy.iter().any(|h| h.callsite == a),
            "smoothed heavy hitter should persist across a quiet window"
        );
        // Its weight decayed (count halved -> g_c doubled), but it is still a
        // smaller g_c than a brand-new rare callsite would receive.
        assert!(t2.g_for(a) > g_a_1);
    }
}
