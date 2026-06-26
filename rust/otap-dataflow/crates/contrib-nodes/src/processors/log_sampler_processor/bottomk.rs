// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Weighted bottom-`(k+1)` reservoir sampler with Horvitz-Thompson counts.
//!
//! This is the local ("Bottom-Floor") level of the two-level log sampling
//! design. Over a time window it keeps the `k` log records with the smallest
//! exponential priority keys `-ln(u) / w_c`, where `u ~ Uniform(0, 1]` and
//! `w_c` is the per-callsite weight. The `(k+1)`-th smallest key is the local
//! threshold `tau^L`.
//!
//! On window close each kept representative is annotated with the
//! Horvitz-Thompson count `nhat_c = m_c / (1 - exp(-tau^L * w_c))`, where
//! `m_c` is the number of representatives of callsite `c`. The per-callsite
//! weight is then rolled forward as `w_next_c = 1 / nhat_c` (inverse
//! frequency), with unseen callsites scored at `w_unseen = max_c w_next_c`
//! (the most-permissive "rarest-seen" floor).
//!
//! The structure is `O(k)` in state and pure: it owns no engine concerns and
//! is unit-testable in isolation.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;

use otap_df_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use otap_df_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;

use otap_df_otap::sampling::{GlobalTable, OTEL_SAMPLING_NHAT, callsite_id};

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Identity of a `(resource, scope)` grouping, used to faithfully regroup
/// representatives into `ResourceLogs` / `ScopeLogs` on emission.
#[derive(Clone, PartialEq)]
struct GroupKey {
    resource: Option<Resource>,
    resource_schema_url: String,
    scope: Option<InstrumentationScope>,
    scope_schema_url: String,
}

/// A single reservoir entry: the retained record plus its sampling key.
struct Item {
    key: f64,
    callsite: u64,
    group: u32,
    record: LogRecord,
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.key.total_cmp(&other.key) == Ordering::Equal
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    // Order solely by key so the `BinaryHeap` (a max-heap) keeps the largest
    // key on top, i.e. the entry to evict when the reservoir overflows.
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.total_cmp(&other.key)
    }
}

/// Summary statistics of the most recently closed window, for telemetry.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct WindowStats {
    /// Number of representatives emitted.
    pub representatives: u64,
    /// Number of distinct callsites among the representatives.
    pub distinct_callsites: u64,
    /// The local threshold `tau^L` (`+inf` if the reservoir did not fill).
    pub tau_l: f64,
}

/// Result of observing a batch under the binding gate.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ObserveStats {
    /// Records seen.
    pub observed: usize,
    /// Records rejected by the global gate before reaching the reservoir.
    pub globally_rejected: usize,
}

/// Weighted bottom-`(k+1)` reservoir over a window.
pub(crate) struct WindowSampler {
    /// Number of representatives retained per window (`k`).
    k: usize,
    /// Bounded max-heap holding up to `k + 1` smallest-key entries.
    heap: BinaryHeap<Item>,
    /// Interned `(resource, scope)` groups for the current window.
    groups: Vec<GroupKey>,
    /// Per-callsite weights carried from the previous window.
    w_seen: HashMap<u64, f64>,
    /// Weight applied to callsites not present in `w_seen`.
    w_unseen: f64,
    /// Random source for exponential priority keys.
    rng: SmallRng,
}

impl WindowSampler {
    /// Create a new sampler retaining `k` representatives per window.
    pub(crate) fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k + 1),
            groups: Vec::new(),
            w_seen: HashMap::new(),
            w_unseen: 1.0,
            rng: SmallRng::seed_from_u64(rand::rng().random()),
        }
    }

    /// Update the per-window representative budget `k` at runtime. Takes effect
    /// immediately; if `k` shrank, the reservoir is trimmed to the new `k + 1`
    /// smallest keys.
    pub(crate) fn set_k(&mut self, k: usize) {
        self.k = k;
        while self.heap.len() > self.k + 1 {
            let _ = self.heap.pop();
        }
    }

    fn intern_group(&mut self, key: GroupKey) -> u32 {
        if let Some(idx) = self.groups.iter().position(|g| *g == key) {
            return idx as u32;
        }
        let idx = self.groups.len() as u32;
        self.groups.push(key);
        idx
    }

    fn weight_for(&self, callsite: u64) -> f64 {
        self.w_seen.get(&callsite).copied().unwrap_or(self.w_unseen)
    }

    fn admit(&mut self, key: f64, callsite: u64, group: u32, record: LogRecord) {
        self.heap.push(Item {
            key,
            callsite,
            group,
            record,
        });
        if self.heap.len() > self.k + 1 {
            // Evict the largest key: keep the (k+1) smallest.
            let _ = self.heap.pop();
        }
    }

    /// Observe a batch of logs, applying the binding gate and admitting
    /// surviving records into the reservoir.
    ///
    /// For each record a single `u ~ Uniform(0, 1]` is drawn and shared
    /// between the global and local tests, so the realized admission rule is
    /// exactly `-ln(u) < min(tau^L * w_c, tau^G * g_c)`. The global test is a
    /// pre-admission skip: a record is dropped before entering the reservoir
    /// when `-ln(u) >= tau^G * g_c`. An absent (slack) table makes `tau^G * g_c`
    /// infinite, so nothing is rejected and behaviour reduces to local-only.
    pub(crate) fn observe_logs(&mut self, logs: LogsData, table: &GlobalTable) -> ObserveStats {
        let mut stats = ObserveStats::default();
        for rl in logs.resource_logs {
            let resource = rl.resource;
            let resource_schema_url = rl.schema_url;
            for sl in rl.scope_logs {
                let scope = sl.scope;
                let scope_name = scope.as_ref().map_or("", |s| s.name.as_str());
                let group = self.intern_group(GroupKey {
                    resource: resource.clone(),
                    resource_schema_url: resource_schema_url.clone(),
                    scope: scope.clone(),
                    scope_schema_url: sl.schema_url,
                });
                for rec in sl.log_records {
                    stats.observed += 1;
                    let callsite = callsite_id(scope_name, &rec);
                    let w = self.weight_for(callsite);
                    // u in (0, 1]: random() is [0, 1), so 1 - random() is (0, 1].
                    let u = 1.0 - self.rng.random::<f64>();
                    let neg_ln_u = -u.ln();
                    // Global gate (shared u). theta_g is +inf for a slack table.
                    let theta_g = table.tau_g * table.g_for(callsite);
                    if neg_ln_u >= theta_g {
                        stats.globally_rejected += 1;
                        continue;
                    }
                    let key = neg_ln_u / w;
                    self.admit(key, callsite, group, rec);
                }
            }
        }
        stats
    }

    /// Close the current window, returning the representatives as `LogsData`
    /// annotated with per-callsite `nhat`, and roll the weights forward.
    ///
    /// The HT count uses the *binding* inclusion probability
    /// `1 - exp(-min(tau^L * w_c, tau^G * g_c))`, so the per-callsite estimate
    /// stays unbiased under the global gate and the agent's sum recovers the
    /// true counts. `table` is the global table in force at window close.
    ///
    /// Returns `None` when the window is empty.
    pub(crate) fn close_window(
        &mut self,
        table: &GlobalTable,
    ) -> Option<(LogsData, WindowStats)> {
        if self.heap.is_empty() {
            self.groups.clear();
            return None;
        }

        // Determine the local threshold tau^L. When the reservoir filled to
        // k+1, the largest retained key is the threshold and its owning entry
        // is the spare (discarded); otherwise tau^L is +inf and all entries
        // are exact representatives.
        let tau_l = if self.heap.len() > self.k {
            self.heap.pop().map_or(f64::INFINITY, |spare| spare.key)
        } else {
            f64::INFINITY
        };

        let items: Vec<Item> = self.heap.drain().collect();

        // m_c: count of representatives per callsite.
        let mut counts: HashMap<u64, u32> = HashMap::new();
        for it in &items {
            *counts.entry(it.callsite).or_insert(0) += 1;
        }

        // Horvitz-Thompson counts using the weights that were in effect during
        // this window, and the rolled-forward weights for the next window.
        let mut nhat: HashMap<u64, f64> = HashMap::with_capacity(counts.len());
        let mut next_w: HashMap<u64, f64> = HashMap::with_capacity(counts.len());
        let mut max_next_w = 0.0_f64;
        for (&c, &m) in &counts {
            let w = self.weight_for(c);
            // Binding inclusion threshold: min of the local and global scores.
            let theta_local = tau_l * w;
            let theta_global = table.tau_g * table.g_for(c);
            let theta = theta_local.min(theta_global);
            let denom = if theta.is_finite() {
                1.0 - (-theta).exp()
            } else {
                1.0
            };
            let nh = if denom > 0.0 {
                f64::from(m) / denom
            } else {
                f64::from(m)
            };
            let w_next = if nh > 0.0 { 1.0 / nh } else { self.w_unseen };
            let _ = nhat.insert(c, nh);
            let _ = next_w.insert(c, w_next);
            max_next_w = max_next_w.max(w_next);
        }

        let stats = WindowStats {
            representatives: items.len() as u64,
            distinct_callsites: counts.len() as u64,
            tau_l,
        };

        // Roll weights forward (prune to callsites seen this window).
        if !next_w.is_empty() {
            self.w_seen = next_w;
            self.w_unseen = max_next_w;
        }

        // Regroup representatives, annotating each with its callsite nhat.
        let mut by_group: HashMap<u32, Vec<LogRecord>> = HashMap::new();
        for mut it in items {
            let nh = nhat.get(&it.callsite).copied().unwrap_or(1.0);
            it.record.attributes.push(KeyValue {
                key: OTEL_SAMPLING_NHAT.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::DoubleValue(nh)),
                }),
            });
            by_group.entry(it.group).or_default().push(it.record);
        }

        let mut resource_logs = Vec::with_capacity(by_group.len());
        for (group_idx, log_records) in by_group {
            let g = &self.groups[group_idx as usize];
            resource_logs.push(ResourceLogs {
                resource: g.resource.clone(),
                scope_logs: vec![ScopeLogs {
                    scope: g.scope.clone(),
                    log_records,
                    schema_url: g.scope_schema_url.clone(),
                }],
                schema_url: g.resource_schema_url.clone(),
            });
        }

        self.groups.clear();
        Some((LogsData { resource_logs }, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_with_event(name: &str) -> LogRecord {
        LogRecord {
            event_name: name.to_string(),
            ..Default::default()
        }
    }

    fn logs_data(scope_name: &str, records: Vec<LogRecord>) -> LogsData {
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: scope_name.to_string(),
                        ..Default::default()
                    }),
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn nhat_of(rec: &LogRecord) -> Option<f64> {
        rec.attributes.iter().find_map(|kv| {
            if kv.key == OTEL_SAMPLING_NHAT {
                match kv.value.as_ref()?.value.as_ref()? {
                    Value::DoubleValue(d) => Some(*d),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    fn count_records(logs: &LogsData) -> usize {
        logs.resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .map(|sl| sl.log_records.len())
            .sum()
    }

    #[test]
    fn reservoir_is_bounded_by_k_plus_one() {
        let mut s = WindowSampler::new(8);
        let records: Vec<LogRecord> =
            (0..1000).map(|i| log_with_event(&format!("e{i}"))).collect();
        let _ = s.observe_logs(logs_data("scope", records), &GlobalTable::absent());
        assert!(s.heap.len() <= 9);
    }

    #[test]
    fn under_k_keeps_everything_with_exact_counts() {
        let mut s = WindowSampler::new(100);
        let records = vec![log_with_event("a"), log_with_event("a"), log_with_event("b")];
        let _ = s.observe_logs(logs_data("scope", records), &GlobalTable::absent());
        let (out, stats) = s.close_window(&GlobalTable::absent()).expect("non-empty");
        assert_eq!(count_records(&out), 3);
        assert!(stats.tau_l.is_infinite());
        // With tau^L = +inf, nhat == m_c (exact).
        for rl in &out.resource_logs {
            for sl in &rl.scope_logs {
                for rec in &sl.log_records {
                    let nh = nhat_of(rec).expect("nhat present");
                    let expected = if rec.event_name == "a" { 2.0 } else { 1.0 };
                    assert!((nh - expected).abs() < 1e-9);
                }
            }
        }
    }

    #[test]
    fn empty_window_returns_none() {
        let mut s = WindowSampler::new(4);
        assert!(s.close_window(&GlobalTable::absent()).is_none());
    }

    #[test]
    fn global_gate_suppresses_a_heavy_callsite() {
        use otap_df_otap::sampling::HeavyHitter;
        // A table that makes the "hot" callsite extremely unlikely to pass the
        // global gate (tiny g_c, finite tau^G), while leaving everything else
        // at the permissive g_unseen floor.
        let hot = callsite_id("scope", &log_with_event("hot"));
        let table = GlobalTable {
            version: 1,
            tau_g: 1.0,
            g_unseen: f64::INFINITY,
            heavy: vec![HeavyHitter {
                callsite: hot,
                g_c: 1e-9,
            }],
        };
        let mut s = WindowSampler::new(1000);
        let mut records: Vec<LogRecord> = (0..2000).map(|_| log_with_event("hot")).collect();
        records.push(log_with_event("rare"));
        let stats = s.observe_logs(logs_data("scope", records), &table);
        assert_eq!(stats.observed, 2001);
        // Nearly all "hot" records are dropped by the global gate; the
        // non-enumerated "rare" callsite (g_unseen = +inf) is never throttled.
        assert!(
            stats.globally_rejected > 1900,
            "expected heavy suppression, got {}",
            stats.globally_rejected
        );
        let (out, _) = s.close_window(&table).expect("rare survives");
        let survived_rare = out
            .resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .flat_map(|sl| &sl.log_records)
            .any(|r| r.event_name == "rare");
        assert!(survived_rare, "globally-unique callsite must be preserved");
    }

    #[test]
    fn ht_counts_are_unbiased_on_average() {
        // One callsite emitting a fixed number of records per window; the
        // mean HT estimate over many windows should recover the true count.
        let true_count = 500usize;
        let k = 20usize;
        let mut s = WindowSampler::new(k);
        let windows = 2000;
        let mut sum = 0.0_f64;
        for _ in 0..windows {
            let records: Vec<LogRecord> =
                (0..true_count).map(|_| log_with_event("hot")).collect();
            let _ = s.observe_logs(logs_data("scope", records), &GlobalTable::absent());
            if let Some((out, _)) = s.close_window(&GlobalTable::absent()) {
                let rec = &out.resource_logs[0].scope_logs[0].log_records[0];
                sum += nhat_of(rec).expect("nhat");
            }
        }
        let mean = sum / f64::from(windows);
        let rel_err = (mean - true_count as f64).abs() / true_count as f64;
        assert!(rel_err < 0.1, "mean nhat {mean} too far from {true_count}");
    }
}
