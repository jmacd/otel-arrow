// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Signatures: what a condition actually constrains.
//!
//! An operator writes a condition the way Envoy requires, naming every key of
//! its token and giving the ones it does not care about a wildcard. A wildcard
//! cannot change whether the condition matches, because the token having
//! resolved already guarantees the key is present. So the compiler drops them.
//! What remains is the signature: the keys the condition tests by value.
//!
//! Signatures are shared. Every condition that constrains the same keys, whatever
//! literals it demands and whatever set it belongs to, gets the same signature.
//! Each matching token then shares one PairSlot definition, while a request
//! encodes each extractor's dictionary symbol once. This is the whole reason
//! the compiled state is a token-by-signature matrix rather than a table per
//! condition.

use crate::ids::{KeyId, SignatureId};
use hashbrown::HashMap;

/// The keys one condition constrains, in ascending key order.
///
/// The ordering is canonical so that two conditions written in different orders
/// share a signature.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Signature {
    keys: Box<[KeyId]>,
}

impl Signature {
    /// Returns the keys this signature constrains.
    pub(crate) fn keys(&self) -> &[KeyId] {
        &self.keys
    }
}

/// Deduplicates signatures as conditions are compiled.
#[derive(Debug, Default)]
pub(crate) struct SignatureTable {
    signatures: Vec<Signature>,
    index: HashMap<Signature, SignatureId>,
}

impl SignatureTable {
    /// Returns the signature for a set of constrained keys, interning it if it
    /// is new. `keys` need not be sorted or deduplicated.
    pub(crate) fn intern(&mut self, keys: &[KeyId]) -> SignatureId {
        let mut sorted = keys.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let signature = Signature {
            keys: sorted.into_boxed_slice(),
        };

        if let Some(&existing) = self.index.get(&signature) {
            return existing;
        }
        let id = SignatureId::from_index(self.signatures.len());
        self.signatures.push(signature.clone());
        let _ = self.index.insert(signature, id);
        id
    }

    pub(crate) fn get(&self, id: SignatureId) -> &Signature {
        &self.signatures[id.index()]
    }

    pub(crate) fn len(&self) -> usize {
        self.signatures.len()
    }
}
