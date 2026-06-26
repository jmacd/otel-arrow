// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared state for two-level log sampling feedback.
//!
//! The global ("heavy-hitter") table computed by the agent-side
//! `global_reservoir` processor is published here and read back by other
//! nodes:
//!
//! - **Agent side**: the `global_reservoir` processor is the writer; the OTLP
//!   receiver is the reader (it attaches the table to OTLP responses).
//! - **SDK side**: the OTLP exporter is the writer (it decodes the table from
//!   the OTLP response); the local `log_sampler` is the reader (it consults
//!   the table in its admission decision).
//!
//! Publication uses [`arc_swap::ArcSwap`] for wait-free reads and a single
//! atomic store on publish — no locks on the per-record hot path.
//!
//! Nodes that must share a table reference it by a **channel name** drawn from
//! their configuration. [`shared_global_table`] returns the same
//! [`SharedGlobalTable`] handle for a given channel, so a writer node and a
//! reader node configured with the same channel observe each other's updates
//! without any engine-level wiring.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arc_swap::ArcSwap;
use otap_df_pdata::proto::opentelemetry::common::v1::any_value::Value;
use otap_df_pdata::proto::opentelemetry::logs::v1::LogRecord;

/// Reserved log attribute key carrying a representative's per-callsite
/// Horvitz-Thompson count `nhat_c` across SDK -> agent transport.
pub const OTEL_SAMPLING_NHAT: &str = "otel.sampling.nhat";

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
/// `event_name` is absent. The result is a 64-bit FNV-1a hash, so the same
/// callsite yields an identical token at both sampling levels and fleet-wide.
#[must_use]
pub fn callsite_id(scope_name: &str, rec: &LogRecord) -> u64 {
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

/// A single globally heavy callsite and its global inclusion weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HeavyHitter {
    /// Stable 64-bit callsite identity (FNV-1a of `event_name`, or of
    /// `scope_name` + body when `event_name` is absent).
    pub callsite: u64,
    /// Global per-callsite weight `g_c = 1 / N_c`.
    pub g_c: f64,
}

/// The global heavy-hitter table returned to SDKs on the OTLP response path.
///
/// A *missing* table (see [`GlobalTable::absent`]) means the global gate is
/// slack (`tau^G = +inf`), i.e. the SDK degrades to local-only sampling. A
/// *present* table scores callsites not listed in [`GlobalTable::heavy`] at
/// [`GlobalTable::g_unseen`], the most-permissive global floor.
#[derive(Debug, Clone, PartialEq)]
pub struct GlobalTable {
    /// Encoding/format version (see the return wire protocol). `0` denotes the
    /// absent table.
    pub version: u32,
    /// Global threshold `tau^G`. `+inf` denotes a slack global gate.
    pub tau_g: f64,
    /// Global rarest-seen floor `g_unseen = max_c g_c`, applied to every
    /// callsite not present in `heavy`.
    pub g_unseen: f64,
    /// The heavy-hitter list (top `K'` callsites by global count `N_c`).
    pub heavy: Vec<HeavyHitter>,
}

impl GlobalTable {
    /// The absent table: a slack global gate that never suppresses.
    #[must_use]
    pub fn absent() -> Self {
        Self {
            version: 0,
            tau_g: f64::INFINITY,
            g_unseen: f64::INFINITY,
            heavy: Vec::new(),
        }
    }

    /// True if this is the absent (slack) table.
    #[must_use]
    pub fn is_absent(&self) -> bool {
        self.version == 0
    }

    /// The global weight `g_c` for a callsite: the listed value if the
    /// callsite is a heavy hitter, otherwise the rarest-seen floor.
    #[must_use]
    pub fn g_for(&self, callsite: u64) -> f64 {
        self.heavy
            .iter()
            .find(|h| h.callsite == callsite)
            .map_or(self.g_unseen, |h| h.g_c)
    }
}

/// A wait-free, atomically-swappable handle to the current [`GlobalTable`].
pub type SharedGlobalTable = Arc<ArcSwap<GlobalTable>>;

/// Create a fresh shared handle initialized to the absent table.
#[must_use]
pub fn new_shared_global_table() -> SharedGlobalTable {
    Arc::new(ArcSwap::from_pointee(GlobalTable::absent()))
}

static REGISTRY: LazyLock<Mutex<HashMap<String, SharedGlobalTable>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Return the shared global-table handle for `channel`, creating it (with an
/// absent table) on first use. All callers naming the same channel receive
/// clones of the same `Arc`, so a writer and a reader configured with the same
/// channel share state.
#[must_use]
pub fn shared_global_table(channel: &str) -> SharedGlobalTable {
    let mut registry = REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry
        .entry(channel.to_string())
        .or_insert_with(new_shared_global_table)
        .clone()
}

// ---------------------------------------------------------------------------
// C6: return wire protocol (OTLP/HTTP response headers).
//
// The agent attaches the current global table to OTLP responses as HTTP/2
// headers (lowercase names so HPACK static/dynamic tables apply). The SDK
// decodes them back into a `GlobalTable`. The format is fail-safe: any
// missing or malformed field yields the absent (slack) table, so the SDK
// degrades to local-only sampling rather than to incorrect suppression.
// ---------------------------------------------------------------------------

/// Header carrying the table's monotonic version (`u32`, never `0`).
/// Its presence-and-parse also gates the format: an unparseable or `0` value
/// is treated as the absent table.
pub const HEADER_VER: &str = "otel-sample-ver";
/// Header carrying the global threshold `tau^G`.
pub const HEADER_TAU_G: &str = "otel-sample-tau-g";
/// Header carrying the rarest-seen floor `g_unseen`.
pub const HEADER_G_UNSEEN: &str = "otel-sample-g-unseen";
/// Header carrying the heavy-hitter list: space-separated `<id>:<g_c>` entries
/// where `<id>` is the 16-hex-digit callsite token.
pub const HEADER_HEAVY: &str = "otel-sample-heavy";

/// Encode a global table to OTLP/HTTP response header `(name, value)` pairs.
///
/// Returns an empty vector for the absent table (the agent attaches nothing,
/// and the SDK reads that as a slack global gate).
#[must_use]
pub fn encode_headers(table: &GlobalTable) -> Vec<(&'static str, String)> {
    if table.is_absent() {
        return Vec::new();
    }
    let mut out = vec![
        (HEADER_VER, table.version.to_string()),
        (HEADER_TAU_G, fmt_f64(table.tau_g)),
        (HEADER_G_UNSEEN, fmt_f64(table.g_unseen)),
    ];
    if !table.heavy.is_empty() {
        let heavy = table
            .heavy
            .iter()
            .map(|h| format!("{:016x}:{}", h.callsite, fmt_f64(h.g_c)))
            .collect::<Vec<_>>()
            .join(" ");
        out.push((HEADER_HEAVY, heavy));
    }
    out
}

/// Decode a global table from OTLP/HTTP response header values.
///
/// Any missing or malformed required field (`ver`, `tau-g`, `g-unseen`)
/// yields [`GlobalTable::absent`]. A present table with an absent or partly
/// malformed `heavy` list simply carries fewer heavy hitters (those entries
/// fall back to `g_unseen`).
#[must_use]
pub fn decode_headers(
    ver: Option<&str>,
    tau_g: Option<&str>,
    g_unseen: Option<&str>,
    heavy: Option<&str>,
) -> GlobalTable {
    let absent = GlobalTable::absent();
    let Some(version) = ver.and_then(|s| s.trim().parse::<u32>().ok()) else {
        return absent;
    };
    if version == 0 {
        return absent;
    }
    let (Some(tau_g), Some(g_unseen)) = (
        tau_g.and_then(parse_f64),
        g_unseen.and_then(parse_f64),
    ) else {
        return absent;
    };

    let heavy = heavy
        .map(|s| {
            s.split_whitespace()
                .filter_map(|entry| {
                    let (id, g) = entry.split_once(':')?;
                    let callsite = u64::from_str_radix(id.trim(), 16).ok()?;
                    let g_c = parse_f64(g)?;
                    Some(HeavyHitter { callsite, g_c })
                })
                .collect()
        })
        .unwrap_or_default();

    GlobalTable {
        version,
        tau_g,
        g_unseen,
        heavy,
    }
}

/// Format an `f64` for a header value (round-trippable; `inf`/`-inf` for
/// infinities).
fn fmt_f64(v: f64) -> String {
    if v.is_infinite() {
        if v > 0.0 { "inf".to_string() } else { "-inf".to_string() }
    } else {
        format!("{v}")
    }
}

/// Parse a header `f64`, accepting `inf`/`-inf` and rejecting `NaN`.
fn parse_f64(s: &str) -> Option<f64> {
    let s = s.trim();
    let v = match s {
        "inf" | "+inf" | "Infinity" => f64::INFINITY,
        "-inf" | "-Infinity" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().ok()?,
    };
    if v.is_nan() { None } else { Some(v) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_table_is_slack() {
        let t = GlobalTable::absent();
        assert!(t.is_absent());
        assert!(t.tau_g.is_infinite());
        assert!(t.g_for(123).is_infinite());
    }

    #[test]
    fn g_for_prefers_heavy_then_floor() {
        let t = GlobalTable {
            version: 1,
            tau_g: 2.0,
            g_unseen: 0.5,
            heavy: vec![HeavyHitter {
                callsite: 7,
                g_c: 0.01,
            }],
        };
        assert!((t.g_for(7) - 0.01).abs() < 1e-12);
        assert!((t.g_for(999) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn registry_shares_by_channel() {
        let a = shared_global_table("chan-x");
        let b = shared_global_table("chan-x");
        a.store(Arc::new(GlobalTable {
            version: 5,
            tau_g: 1.0,
            g_unseen: 0.25,
            heavy: Vec::new(),
        }));
        assert_eq!(b.load().version, 5);
        // Distinct channel is independent.
        let c = shared_global_table("chan-y");
        assert!(c.load().is_absent());
    }

    fn get<'a>(headers: &'a [(&'static str, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    fn round_trip(table: &GlobalTable) -> GlobalTable {
        let h = encode_headers(table);
        decode_headers(
            get(&h, HEADER_VER),
            get(&h, HEADER_TAU_G),
            get(&h, HEADER_G_UNSEEN),
            get(&h, HEADER_HEAVY),
        )
    }

    #[test]
    fn headers_round_trip_present_table() {
        let table = GlobalTable {
            version: 42,
            tau_g: 0.001_234,
            g_unseen: 0.5,
            heavy: vec![
                HeavyHitter {
                    callsite: 0xdead_beef_0000_0001,
                    g_c: 0.01,
                },
                HeavyHitter {
                    callsite: 7,
                    g_c: 0.002_5,
                },
            ],
        };
        assert_eq!(round_trip(&table), table);
    }

    #[test]
    fn headers_round_trip_infinite_tau_g() {
        let table = GlobalTable {
            version: 3,
            tau_g: f64::INFINITY,
            g_unseen: 0.125,
            heavy: Vec::new(),
        };
        let decoded = round_trip(&table);
        assert!(decoded.tau_g.is_infinite());
        assert_eq!(decoded, table);
    }

    #[test]
    fn absent_table_encodes_to_no_headers() {
        assert!(encode_headers(&GlobalTable::absent()).is_empty());
    }

    #[test]
    fn missing_or_malformed_fields_decode_absent() {
        // No version.
        assert!(decode_headers(None, Some("1.0"), Some("0.5"), None).is_absent());
        // Version 0 is reserved for absent.
        assert!(decode_headers(Some("0"), Some("1.0"), Some("0.5"), None).is_absent());
        // Missing required g_unseen.
        assert!(decode_headers(Some("2"), Some("1.0"), None, None).is_absent());
        // Unparseable tau_g.
        assert!(decode_headers(Some("2"), Some("oops"), Some("0.5"), None).is_absent());
    }

    #[test]
    fn malformed_heavy_entries_are_skipped() {
        let decoded = decode_headers(
            Some("9"),
            Some("2.0"),
            Some("0.5"),
            Some("000000000000000a:0.01 garbage badhex:0.02 0000000000000007:nope"),
        );
        assert!(!decoded.is_absent());
        // Only the first well-formed entry survives.
        assert_eq!(decoded.heavy.len(), 1);
        assert_eq!(decoded.heavy[0].callsite, 10);
        assert!((decoded.heavy[0].g_c - 0.01).abs() < 1e-12);
    }
}
