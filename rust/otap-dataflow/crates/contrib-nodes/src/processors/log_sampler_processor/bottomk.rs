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

use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

/// Reserved log attribute key carrying the per-callsite Horvitz-Thompson
/// count of a representative across SDK -> agent transport.
pub(crate) const OTEL_SAMPLING_NHAT: &str = "otel.sampling.nhat";

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Compute the stable callsite identity of a log record.
///
/// Prefers `event_name` (the OTel-model field closest to a statement
/// identity); falls back to a hash of `scope_name` + the string body when
/// `event_name` is absent. The result is a 64-bit FNV-1a hash so the same
/// callsite produces an identical token fleet-wide.
pub(crate) fn callsite_id(scope_name: &str, rec: &LogRecord) -> u64 {
    if !rec.event_name.is_empty() {
        return fnv1a(FNV_OFFSET, rec.event_name.as_bytes());
    }
    let mut h = fnv1a(FNV_OFFSET, scope_name.as_bytes());
    h = fnv1a(h, &[0]);
    if let Some(body) = &rec.body {
        if let Some(Value::StringValue(s)) = &body.value {
            h = fnv1a(h, s.as_bytes());
        }
    }
    h
}

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

    /// Observe a batch of logs, admitting each record into the reservoir.
    /// Returns the number of records observed.
    pub(crate) fn observe_logs(&mut self, logs: LogsData) -> usize {
        let mut observed = 0;
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
                    observed += 1;
                    let callsite = callsite_id(scope_name, &rec);
                    let w = self.weight_for(callsite);
                    // u in (0, 1]: random() is [0, 1), so 1 - random() is (0, 1].
                    let u = 1.0 - self.rng.random::<f64>();
                    let key = -u.ln() / w;
                    self.admit(key, callsite, group, rec);
                }
            }
        }
        observed
    }

    /// Close the current window, returning the representatives as `LogsData`
    /// annotated with per-callsite `nhat`, and roll the weights forward.
    ///
    /// Returns `None` when the window is empty.
    pub(crate) fn close_window(&mut self) -> Option<(LogsData, WindowStats)> {
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
            let denom = if tau_l.is_finite() {
                1.0 - (-tau_l * w).exp()
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
        let _ = s.observe_logs(logs_data("scope", records));
        assert!(s.heap.len() <= 9);
    }

    #[test]
    fn under_k_keeps_everything_with_exact_counts() {
        let mut s = WindowSampler::new(100);
        let records = vec![log_with_event("a"), log_with_event("a"), log_with_event("b")];
        let _ = s.observe_logs(logs_data("scope", records));
        let (out, stats) = s.close_window().expect("non-empty");
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
        assert!(s.close_window().is_none());
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
            let _ = s.observe_logs(logs_data("scope", records));
            if let Some((out, _)) = s.close_window() {
                let rec = &out.resource_logs[0].scope_logs[0].log_records[0];
                sum += nhat_of(rec).expect("nhat");
            }
        }
        let mean = sum / f64::from(windows);
        let rel_err = (mean - true_count as f64).abs() / true_count as f64;
        assert!(rel_err < 0.1, "mean nhat {mean} too far from {true_count}");
    }
}
