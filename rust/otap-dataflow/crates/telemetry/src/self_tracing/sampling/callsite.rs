// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Stable callsite identities for the integrated sampler.
//!
//! The sampler keys its tables on hashable callsite identities. A *span-start*
//! callsite identifies where a span was created, and a *log* callsite
//! identifies where a log statement fired. Both are 64-bit FNV-1a hashes, which
//! keeps the keys small, allocation-free, and stable across runs of the same
//! binary so the feedback tables remain comparable from window to window.
//!
//! Following the design's open decision, the span-start callsite is the hash of
//! the span name, mirroring the `event.name` log callsite identity, so the key
//! stays stable and fleet-wide comparable as long as names are consistent. The
//! log callsite additionally folds in the target, file, and line so two log
//! statements that share a name remain distinct.
//!
//! [`callsite_identity`] offers an alternative span-start key derived from the
//! Tokio `tracing` callsite [`Identifier`] instead of the name. It is the
//! canonical per-callsite identity the tracing system already assigns, is more
//! specific than the name because it distinguishes two same-named spans created
//! at different source sites, and needs no string hashing on the hot path. The
//! tradeoff is that it is a process-local pointer identity: stable and unique
//! within one process run, but not comparable across restarts or processes. The
//! span-start key is process-local and is never serialized, so either choice
//! works; the name hash is retained for any future cross-process or persisted
//! use, where name-based identity is the comparable one.

use std::hash::{Hash, Hasher};

use tracing::callsite::Identifier;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A separator folded between fields so that concatenated inputs cannot collide
/// across field boundaries.
const FIELD_SEPARATOR: u8 = 0xff;

/// Fold one byte into a running FNV-1a hash.
#[inline]
const fn fnv1a_byte(hash: u64, byte: u8) -> u64 {
    (hash ^ byte as u64).wrapping_mul(FNV_PRIME)
}

/// Fold a byte slice into a running FNV-1a hash.
#[inline]
const fn fnv1a_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash = fnv1a_byte(hash, bytes[i]);
        i += 1;
    }
    hash
}

/// The 64-bit FNV-1a hash of a string.
#[must_use]
pub const fn fnv1a_str(s: &str) -> u64 {
    fnv1a_bytes(FNV_OFFSET_BASIS, s.as_bytes())
}

/// The span-start callsite identity, the FNV-1a hash of the span name.
///
/// This keys the span-start threshold table, the per-span reservoir, the
/// span-start value table, and the optional SDK-wide span-log weight table.
#[must_use]
pub const fn span_start_identity(span_name: &str) -> u64 {
    fnv1a_str(span_name)
}

/// A span-start callsite identity derived from a Tokio `tracing` callsite
/// [`Identifier`] rather than the span name.
///
/// The identity is folded from the callsite's stable [`Hash`] implementation,
/// which hashes the callsite pointer, so it is unique per source site and
/// stable for the life of the process. It is more specific than the name and
/// avoids hashing the name string on the hot path. Because it is a pointer
/// identity it is process-local; use [`span_start_identity`] when a span-kind
/// identity must be comparable across processes.
#[must_use]
pub fn callsite_identity(id: &Identifier) -> u64 {
    let mut hasher = Fnv1aHasher::default();
    id.hash(&mut hasher);
    hasher.finish()
}

/// A 64-bit FNV-1a [`Hasher`], used to fold a `tracing` callsite identity into a
/// `u64` through its stable [`Hash`] implementation without depending on the
/// callsite's unstable internal fields.
struct Fnv1aHasher(u64);

impl Default for Fnv1aHasher {
    fn default() -> Self {
        Self(FNV_OFFSET_BASIS)
    }
}

impl Hasher for Fnv1aHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0 = fnv1a_bytes(self.0, bytes);
    }
}

/// The log callsite identity, the FNV-1a hash of the target, name, file, and
/// line, separated so that distinct statements that share a name do not
/// collide.
///
/// This keys criterion one, the global-independent log sampler.
#[must_use]
pub fn log_callsite_identity(
    target: &str,
    name: &str,
    file: Option<&str>,
    line: Option<u32>,
) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv1a_bytes(hash, target.as_bytes());
    hash = fnv1a_byte(hash, FIELD_SEPARATOR);
    hash = fnv1a_bytes(hash, name.as_bytes());
    hash = fnv1a_byte(hash, FIELD_SEPARATOR);
    if let Some(file) = file {
        hash = fnv1a_bytes(hash, file.as_bytes());
    }
    hash = fnv1a_byte(hash, FIELD_SEPARATOR);
    hash = fnv1a_bytes(hash, &line.unwrap_or(0).to_le_bytes());
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_matches_reference_vectors() {
        // Reference FNV-1a 64-bit test vectors.
        assert_eq!(fnv1a_str(""), FNV_OFFSET_BASIS);
        assert_eq!(fnv1a_str("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_str("foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn span_start_identity_is_name_hash() {
        assert_eq!(
            span_start_identity("pipeline.start"),
            fnv1a_str("pipeline.start")
        );
        assert_ne!(span_start_identity("a"), span_start_identity("b"));
    }

    #[test]
    fn log_callsite_identity_separates_fields() {
        // Moving the boundary between target and name changes the identity, so
        // the separator is doing its job.
        let a = log_callsite_identity("ab", "c", Some("f.rs"), Some(1));
        let b = log_callsite_identity("a", "bc", Some("f.rs"), Some(1));
        assert_ne!(a, b);
        // Line and file participate in the identity.
        assert_ne!(
            log_callsite_identity("t", "n", Some("f.rs"), Some(1)),
            log_callsite_identity("t", "n", Some("f.rs"), Some(2)),
        );
        assert_ne!(
            log_callsite_identity("t", "n", Some("a.rs"), Some(1)),
            log_callsite_identity("t", "n", Some("b.rs"), Some(1)),
        );
        // Identity is deterministic.
        assert_eq!(
            log_callsite_identity("t", "n", Some("f.rs"), Some(9)),
            log_callsite_identity("t", "n", Some("f.rs"), Some(9)),
        );
    }

    #[test]
    fn callsite_identity_is_deterministic_and_site_specific() {
        use crate::__log_record_impl;
        use crate::self_tracing::LogContext;
        use tracing::Level;

        // Two distinct macro invocations register distinct callsites, exposed
        // through the resulting record's public `callsite_id`.
        let a = __log_record_impl!(Level::INFO, "cs.alpha").into_record(LogContext::new());
        let b = __log_record_impl!(Level::INFO, "cs.beta").into_record(LogContext::new());

        // The same callsite hashes the same value every time.
        assert_eq!(
            callsite_identity(&a.callsite_id),
            callsite_identity(&a.callsite_id)
        );
        // Distinct callsites hash to distinct identities.
        assert_ne!(
            callsite_identity(&a.callsite_id),
            callsite_identity(&b.callsite_id)
        );
    }
}
