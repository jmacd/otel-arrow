// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Transport header validation helpers.
//!
//! Given SUV messages with optional [`PdataContextBytes`], verify that certain
//! header keys or key/value pairs are present (or absent) on every message.
//!
//! For **require** checks (`require_keys`, `require_key_values`), every
//! message must carry transport headers (`Some`). A single `None` entry
//! (a signal that arrived without headers) causes immediate failure.
//!
//! For **deny** checks, `None` entries are acceptable -- a signal without
//! headers cannot contain a forbidden key.

use otap_df_otap::context_bytes::PdataContextBytes;
use serde::{Deserialize, Serialize};

/// A key/value pair for transport header assertions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportHeaderKeyValue {
    /// Header key (stored/logical name).
    pub key: String,
    /// Expected header value (UTF-8 text).
    pub value: String,
}

impl TransportHeaderKeyValue {
    /// Create a new transport header key/value pair.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Validate that **every** SUV message has transport headers and that each
/// set of headers contains all specified keys.
///
/// Returns `false` when:
/// - `suv` is empty (no messages to validate),
/// - any entry is `None` (a signal arrived without transport headers), or
/// - any packed context is missing a required key.
///
/// Returns `true` when `keys` is empty (nothing to check).
#[must_use]
pub fn validate_transport_header_require_keys(
    suv: &[Option<PdataContextBytes>],
    keys: &[String],
) -> bool {
    if keys.is_empty() {
        return true;
    }
    if suv.is_empty() {
        return false;
    }

    for entry in suv {
        let headers = match entry {
            Some(h) => h,
            None => return false,
        };
        let mut found = vec![false; keys.len()];
        for header in headers.items() {
            if let Some(name) = header.stored_name() {
                for (index, key) in keys.iter().enumerate() {
                    if !found[index] && name == key {
                        found[index] = true;
                    }
                }
            }
        }
        if found.iter().any(|found| !found) {
            return false;
        }
    }

    true
}

/// Validate that **every** SUV message has transport headers and that each
/// set of headers contains all specified key/value pairs.
///
/// Values are compared as UTF-8 text (case-sensitive).
///
/// Returns `false` when:
/// - `suv` is empty (no messages to validate),
/// - any entry is `None` (a signal arrived without transport headers), or
/// - any packed context is missing a required key or has a
///   mismatched value.
///
/// Returns `true` when `pairs` is empty (nothing to check).
#[must_use]
pub fn validate_transport_header_require_key_values(
    suv: &[Option<PdataContextBytes>],
    pairs: &[TransportHeaderKeyValue],
) -> bool {
    if pairs.is_empty() {
        return true;
    }
    if suv.is_empty() {
        return false;
    }

    for entry in suv {
        let headers = match entry {
            Some(h) => h,
            None => return false,
        };
        let mut found = vec![false; pairs.len()];
        for header in headers.items() {
            let Some(name) = header.stored_name() else {
                continue;
            };
            let Some(value) = header
                .value()
                .and_then(|(_, value)| std::str::from_utf8(value).ok())
            else {
                continue;
            };
            for (index, pair) in pairs.iter().enumerate() {
                if !found[index] && name == pair.key && value == pair.value {
                    found[index] = true;
                }
            }
        }
        if found.iter().any(|found| !found) {
            return false;
        }
    }

    true
}

/// Validate that no SUV message carrying transport headers contains any of
/// the specified keys.
///
/// `None` entries (signals without transport headers) are acceptable -- a
/// signal that never received headers cannot contain a forbidden key.
#[must_use]
pub fn validate_transport_header_deny_keys(
    suv: &[Option<PdataContextBytes>],
    keys: &[String],
) -> bool {
    if keys.is_empty() {
        return true;
    }

    for headers in suv.iter().flatten() {
        if headers.items().any(|header| {
            header
                .stored_name()
                .is_some_and(|name| keys.iter().any(|key| name == key))
        }) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_otap::context_bytes::{HeaderInput, HeaderValueKind};

    fn make_headers(entries: &[(&str, &str)]) -> PdataContextBytes {
        PdataContextBytes::build(
            0,
            entries
                .iter()
                .enumerate()
                .map(|(rule_id, (name, value))| HeaderInput {
                    wire_name: name,
                    stored_name: name,
                    value: value.as_bytes(),
                    kind: HeaderValueKind::Text,
                    rule_id: u16::try_from(rule_id).expect("test rule id fits in u16"),
                    entry: None,
                }),
        )
        .expect("packed context")
    }

    #[test]
    fn require_keys_passes_when_all_present() {
        let headers = make_headers(&[("x-tenant-id", "acme"), ("x-request-id", "abc")]);
        let suv = vec![Some(headers)];
        assert!(validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into(), "x-request-id".into()],
        ));
    }

    #[test]
    fn require_keys_fails_when_key_missing() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(!validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into(), "x-missing".into()],
        ));
    }

    #[test]
    fn require_keys_fails_when_no_messages_have_headers() {
        let suv: Vec<Option<PdataContextBytes>> = vec![None, None];
        assert!(!validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into()],
        ));
    }

    #[test]
    fn require_key_values_passes_on_match() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-tenant-id", "acme")],
        ));
    }

    #[test]
    fn require_key_values_fails_on_value_mismatch() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(!validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-tenant-id", "other")],
        ));
    }

    #[test]
    fn require_key_values_fails_on_missing_key() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(!validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-missing", "value")],
        ));
    }

    #[test]
    fn deny_keys_passes_when_key_absent() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(validate_transport_header_deny_keys(
            &suv,
            &["x-secret".into()],
        ));
    }

    #[test]
    fn deny_keys_fails_when_key_present() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers)];
        assert!(!validate_transport_header_deny_keys(
            &suv,
            &["x-tenant-id".into()],
        ));
    }

    #[test]
    fn deny_keys_passes_on_no_headers() {
        let suv: Vec<Option<PdataContextBytes>> = vec![None];
        assert!(validate_transport_header_deny_keys(
            &suv,
            &["x-tenant-id".into()],
        ));
    }

    #[test]
    fn empty_keys_always_passes() {
        let suv: Vec<Option<PdataContextBytes>> = vec![None];
        assert!(validate_transport_header_require_keys(&suv, &[]));
        assert!(validate_transport_header_require_key_values(&suv, &[]));
        assert!(validate_transport_header_deny_keys(&suv, &[]));
    }

    #[test]
    fn multiple_messages_all_must_pass() {
        let h1 = make_headers(&[("x-tenant-id", "acme"), ("x-request-id", "1")]);
        let h2 = make_headers(&[("x-tenant-id", "acme")]); // missing x-request-id
        let suv = vec![Some(h1), Some(h2)];
        assert!(!validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into(), "x-request-id".into()],
        ));
    }

    #[test]
    fn require_keys_fails_when_one_message_has_no_headers() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers), None];
        assert!(!validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into()],
        ));
    }

    #[test]
    fn require_key_values_fails_when_one_message_has_no_headers() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers), None];
        assert!(!validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-tenant-id", "acme")],
        ));
    }

    #[test]
    fn require_keys_fails_on_empty_suv() {
        let suv: Vec<Option<PdataContextBytes>> = vec![];
        assert!(!validate_transport_header_require_keys(
            &suv,
            &["x-tenant-id".into()],
        ));
    }

    #[test]
    fn require_key_values_fails_on_empty_suv() {
        let suv: Vec<Option<PdataContextBytes>> = vec![];
        assert!(!validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-tenant-id", "acme")],
        ));
    }

    #[test]
    fn deny_keys_passes_when_mixed_some_and_none() {
        let headers = make_headers(&[("x-tenant-id", "acme")]);
        let suv = vec![Some(headers), None];
        assert!(validate_transport_header_deny_keys(
            &suv,
            &["x-secret".into()],
        ));
    }

    #[test]
    fn require_key_values_matches_duplicate_key_second_value() {
        // Two headers with the same key but different values.
        // The required value matches the second entry.
        let headers = make_headers(&[("x-env", "staging"), ("x-env", "production")]);
        let suv = vec![Some(headers)];
        assert!(validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-env", "production")],
        ));
    }

    #[test]
    fn require_key_values_matches_duplicate_key_first_value() {
        let headers = make_headers(&[("x-env", "staging"), ("x-env", "production")]);
        let suv = vec![Some(headers)];
        assert!(validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-env", "staging")],
        ));
    }

    #[test]
    fn require_key_values_fails_when_no_duplicate_matches() {
        let headers = make_headers(&[("x-env", "staging"), ("x-env", "production")]);
        let suv = vec![Some(headers)];
        assert!(!validate_transport_header_require_key_values(
            &suv,
            &[TransportHeaderKeyValue::new("x-env", "development")],
        ));
    }
}
