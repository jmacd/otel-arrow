// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Identifiers handed out by the compiler.
//!
//! Every identifier is a dense index into a `Box<[_]>` owned by
//! [`CompiledMetadata`](crate::CompiledMetadata). Callers hold identifiers
//! across an epoch and use them to address compiled state directly, which is
//! what keeps request-time lookups free of name comparison.
//!
//! Identifiers are only meaningful within the epoch that issued them. A context
//! carries its epoch so that a consumer can detect and reject a stale one.

/// Declares a dense identifier newtype over `u16`.
macro_rules! dense_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u16);

        impl $name {
            /// Builds an identifier from a dense index.
            #[must_use]
            pub(crate) const fn from_index(index: usize) -> Self {
                Self(index as u16)
            }

            /// Returns the dense index this identifier addresses.
            #[must_use]
            pub const fn index(self) -> usize {
                self.0 as usize
            }
        }
    };
}

dense_id! {
    /// A qualified metadata field name, such as `tenant_id`.
    KeyId
}

dense_id! {
    /// One rule that produces a value for a key, or fails.
    ///
    /// The equivalent of an Envoy rate-limit action.
    ExtractorId
}

dense_id! {
    /// A named group of extractors that resolves all-or-nothing.
    ///
    /// The equivalent of an Envoy descriptor.
    TokenId
}

dense_id! {
    /// The set of keys some condition constrains, after wildcards are dropped.
    SignatureId
}

dense_id! {
    /// A compatible (token, signature) pair, and the bit layout of its word.
    PairSlotId
}

dense_id! {
    /// One consumer's ordered list of conditions.
    ConditionSetId
}

dense_id! {
    /// One value-matched extractor's field in the packed symbol region.
    ///
    /// The symbol belongs to the extractor rather than to the key, because two
    /// tokens that read the same header share one extractor and therefore one
    /// dictionary probe and one field. A key read by two different extractors
    /// gets a field for each, and a PairSlot picks the one its token uses.
    SymbolSlotId
}

/// A metadata value produced by one key of one token.
///
/// Conditions use [`KeyId`] because Envoy descriptors are key sequences.
/// Carriers use `MetadataFieldId` because values from different tokens with the
/// same key are distinct until a caller explicitly chooses one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetadataFieldId {
    token: TokenId,
    key: KeyId,
}

impl MetadataFieldId {
    /// Qualifies `key` by the token that produces it.
    #[must_use]
    pub const fn new(token: TokenId, key: KeyId) -> Self {
        Self { token, key }
    }

    /// Returns the descriptor token that produces this value.
    #[must_use]
    pub const fn token(self) -> TokenId {
        self.token
    }

    /// Returns the logical key this field carries.
    #[must_use]
    pub const fn key(self) -> KeyId {
        self.key
    }
}

dense_id! {
    /// A retained value's position in the packed context's value index.
    ValueSlotId
}

dense_id! {
    /// A pre-encoded OTLP attribute region within the packed context.
    BagId
}

dense_id! {
    /// A site that builds contexts, such as a receiver.
    ProducerId
}

dense_id! {
    /// A site that observes contexts, such as a router or an exporter.
    ConsumerId
}

/// The position of one condition within its condition set.
///
/// Branches are numbered in declaration order. A router takes the first
/// matching branch; a limiter walks all matching branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchIndex(u8);

impl BranchIndex {
    /// Builds a branch index from a dense index.
    #[must_use]
    pub(crate) const fn from_index(index: usize) -> Self {
        Self(index as u8)
    }

    /// Returns the dense index this branch addresses.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// The version of the compiled state a context was built against.
///
/// Live reconfiguration compiles a new epoch. Contexts outlive their producer's
/// reconfiguration, so a consumer compares this against its own compiled state
/// and fails the request rather than reading a slot that has since moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(u32);

impl Epoch {
    /// Builds an epoch from a monotonically increasing counter.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the underlying counter.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}
