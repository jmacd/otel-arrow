// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One hash function, used by every compiled lookup table.
//!
//! Hashing is written out here rather than delegated to `Hash for [u8]` for two
//! reasons. A case-folded probe must hash the folded bytes without allocating a
//! folded copy, which means feeding the hasher in chunks. And every table in
//! this crate must be able to reproduce the hash of an entry it already holds
//! when it grows, which is only possible if the rule is stated once.
//!
//! Hashing only ever finds a candidate. Every lookup then compares the candidate
//! byte for byte, so a collision costs a comparison and never a wrong answer.

use std::hash::{BuildHasher, Hasher};

/// The hasher every compiled table uses.
pub(crate) type Hasher64 = hashbrown::DefaultHashBuilder;

/// Whether names are compared exactly or without regard to ASCII case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaseFolding {
    /// Compare byte for byte. Claim names and derived value names are exact.
    Exact,
    /// Compare with ASCII case folded away, as transport headers require.
    AsciiInsensitive,
}

impl CaseFolding {
    /// Returns whether two names are equal under this folding.
    pub(crate) fn eq(self, left: &[u8], right: &[u8]) -> bool {
        match self {
            Self::Exact => left == right,
            Self::AsciiInsensitive => left.eq_ignore_ascii_case(right),
        }
    }
}

/// How many bytes are folded and fed to the hasher at a time.
const FOLD_CHUNK: usize = 64;

/// Hashes `bytes` under `folding`.
pub(crate) fn hash_bytes(hasher: &Hasher64, bytes: &[u8], folding: CaseFolding) -> u64 {
    let mut state = hasher.build_hasher();
    match folding {
        CaseFolding::Exact => state.write(bytes),
        CaseFolding::AsciiInsensitive => {
            let mut folded = [0u8; FOLD_CHUNK];
            for chunk in bytes.chunks(FOLD_CHUNK) {
                let folded = &mut folded[..chunk.len()];
                folded.copy_from_slice(chunk);
                folded.make_ascii_lowercase();
                state.write(folded);
            }
        }
    }
    // Terminate so that concatenations of different names cannot collide by
    // construction, which chunked feeding would otherwise permit.
    state.write_u8(0xff);
    state.finish()
}
