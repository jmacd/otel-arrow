// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! What callers declare, before any of it is compiled.
//!
//! Declarations are held verbatim so that every pass can see the whole
//! configuration at once, which is what lets reachability prune globally and
//! lets validation report every problem in one pass instead of stopping at the
//! first.
//!
//! Nothing here knows about configuration syntax. A caller translates its own
//! configuration into these declarations, and the pipeline graph arrives only as
//! an opaque producer-to-consumer relation, so this crate stays independent of
//! both.

use crate::condition::Condition;
use crate::ids::{ConsumerId, ExtractorId, KeyId, MetadataFieldId, ProducerId, TokenId};
use crate::source::{ExtractorSource, ValueKind};

/// A consumer's declaration that it consumes a token.
///
/// This list is Envoy's route-level `rate_limits: []`: it says which descriptors
/// exist for this consumer. It is what scopes the consumer's conditions, because
/// a condition can only describe a token its consumer declared, and it is what
/// tells reachability the token is observed even when nothing reads its keys.
///
/// The two strengths differ only in what happens when the token did not resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// Nack the request. Nothing downstream of the check runs, so the consumer's
    /// condition sets are never tested.
    ///
    /// Envoy has no equivalent -- it simply skips a descriptor that was not
    /// produced -- so this is an extension, added because the engine uses tokens
    /// for admission and tenancy and not only for limiting.
    Required,
    /// Proceed. Conditions over the token match nothing and the consumer applies
    /// its own default, which is exactly what Envoy does with a descriptor that
    /// was not produced.
    Optional,
}

/// A carrier's request for a metadata value.
///
/// [`Self::Key`] is convenient when exactly one token declared by that
/// consumer produces the key. [`Self::Field`] is required when several do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataField {
    /// Resolve this key unambiguously among the consumer's declared tokens.
    Key(KeyId),
    /// Read this exact token-qualified value.
    Field(MetadataFieldId),
}

impl From<KeyId> for MetadataField {
    fn from(key: KeyId) -> Self {
        Self::Key(key)
    }
}

impl From<MetadataFieldId> for MetadataField {
    fn from(field: MetadataFieldId) -> Self {
        Self::Field(field)
    }
}

impl MetadataField {
    pub(crate) const fn key(self) -> KeyId {
        match self {
            Self::Key(key) => key,
            Self::Field(field) => field.key(),
        }
    }
}

/// A qualified metadata field.
#[derive(Debug)]
pub(crate) struct KeyDeclaration {
    pub(crate) name: Box<str>,
    pub(crate) value_kind: ValueKind,
}

/// One rule that produces a value for a key.
#[derive(Debug)]
pub(crate) struct ExtractorDeclaration {
    pub(crate) key: KeyId,
    pub(crate) source: ExtractorSource,
}

/// A named group of extractors that resolves all-or-nothing.
#[derive(Debug)]
pub(crate) struct TokenDeclaration {
    pub(crate) name: Box<str>,
    pub(crate) extractors: Vec<ExtractorId>,
}

/// A site that builds contexts, or one that observes them.
#[derive(Debug)]
pub(crate) struct SiteDeclaration {
    pub(crate) name: Box<str>,
}

/// One consumer's ordered conditions.
#[derive(Debug)]
pub(crate) struct ConditionSetDeclaration {
    pub(crate) name: Box<str>,
    pub(crate) consumer: ConsumerId,
    pub(crate) conditions: Vec<Condition>,
}

/// A consumer that reads a key's value out of the context.
#[derive(Debug)]
pub(crate) struct ReadDeclaration {
    pub(crate) consumer: ConsumerId,
    pub(crate) field: MetadataField,
}

/// A pre-encoded OTLP attribute region a consumer wants to copy out whole.
#[derive(Debug)]
pub(crate) struct BagDeclaration {
    pub(crate) consumer: ConsumerId,
    /// The protobuf field number of the repeated `KeyValue` field the bytes are
    /// destined for, so that the region is a valid fragment of the consumer's
    /// own message and needs no re-tagging.
    pub(crate) attributes_field_number: u32,
    pub(crate) fields: Vec<MetadataField>,
}

/// A node's admission contract for one token.
#[derive(Debug)]
pub(crate) struct RequirementDeclaration {
    pub(crate) consumer: ConsumerId,
    pub(crate) token: TokenId,
    pub(crate) requirement: Requirement,
}

/// Everything declared, before compilation.
#[derive(Debug, Default)]
pub(crate) struct Declarations {
    pub(crate) keys: Vec<KeyDeclaration>,
    pub(crate) extractors: Vec<ExtractorDeclaration>,
    pub(crate) tokens: Vec<TokenDeclaration>,
    pub(crate) producers: Vec<SiteDeclaration>,
    pub(crate) consumers: Vec<SiteDeclaration>,
    pub(crate) condition_sets: Vec<ConditionSetDeclaration>,
    pub(crate) reads: Vec<ReadDeclaration>,
    pub(crate) bags: Vec<BagDeclaration>,
    pub(crate) requirements: Vec<RequirementDeclaration>,
    /// Which consumers each producer can reach.
    ///
    /// An empty relation means the caller did not supply a pipeline graph, in
    /// which case every producer is taken to reach every consumer and the second
    /// level of pruning does nothing.
    pub(crate) reachable: Vec<(ProducerId, ConsumerId)>,
}

impl Declarations {
    pub(crate) fn key_name(&self, key: KeyId) -> &str {
        &self.keys[key.index()].name
    }

    pub(crate) fn token_name(&self, token: TokenId) -> &str {
        &self.tokens[token.index()].name
    }

    /// Returns the tokens a consumer declared, in declaration order.
    pub(crate) fn consumer_tokens(
        &self,
        consumer: ConsumerId,
    ) -> impl Iterator<Item = TokenId> + '_ {
        self.requirements
            .iter()
            .filter(move |declared| declared.consumer == consumer)
            .map(|declared| declared.token)
    }

    /// Returns whether a token produces a key, and through which extractor.
    pub(crate) fn extractor_for(&self, token: TokenId, key: KeyId) -> Option<ExtractorId> {
        self.tokens[token.index()]
            .extractors
            .iter()
            .copied()
            .find(|&extractor| self.extractors[extractor.index()].key == key)
    }

    /// Returns the keys a token produces, in declaration order.
    pub(crate) fn token_keys(&self, token: TokenId) -> impl Iterator<Item = KeyId> + '_ {
        self.tokens[token.index()]
            .extractors
            .iter()
            .map(move |&extractor| self.extractors[extractor.index()].key)
    }
}
