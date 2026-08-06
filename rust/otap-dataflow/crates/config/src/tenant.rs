// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tenant context declarations.
//!
//! An operator declares, once for the engine, which request-scoped facts the
//! pipeline is allowed to know about. Those declarations are compiled at
//! startup into a [`compiled::TenantRegistry`], which does all of the string
//! work ahead of time so that resolving a request costs a fixed number of
//! lookups into a positionally addressed buffer.
//!
//! Three nouns, and no others:
//!
//! - a **key** is a named request-scoped dimension, and is engine-global
//!   vocabulary: `tenant_id` means the same thing in every pipeline;
//! - an **extractor** says how a key gets a value on a given request, and is
//!   also the key's declaration -- a key exists because some extractor fills
//!   it;
//! - a **token** is a named set of extractors, and resolves all-or-nothing, so
//!   that it is an identity rather than a partially populated map.
//!
//! ```yaml
//! policies:
//!   tenant:
//!     tokens:
//!       edge:
//!         extractors:
//!           - key: tenant_id
//!             transport_header: x-tenant-id
//!           - key: project_id
//!             transport_header: x-project-id
//! ```
//!
//! `edge` resolves only when both headers are present, so nothing downstream
//! ever observes a half-identity.
//!
//! "Tenant" names the motivating case rather than the limit of the mechanism:
//! a tenant context is a bundle of declared request-scoped dimensions, and
//! multitenancy is one use of it. See `docs/tenant-context.md` for the design
//! and the sequence of changes that implements it.

pub mod compiled;

use std::borrow::Cow;
use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Name of a tenant token, as written in configuration.
pub type TenantTokenName = Cow<'static, str>;

/// Name of a tenant key, as written in configuration.
pub type TenantKeyName = Cow<'static, str>;

/// Engine-scoped declaration of the tenant vocabulary.
///
/// This policy is honored at engine scope only. Scope precedence is
/// deliberately not used: a key is shared vocabulary, and a group that could
/// redefine `tenant_id` would defeat the reason the vocabulary exists.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TenantPolicy {
    /// Token definitions, keyed by token name.
    #[serde(default)]
    pub tokens: BTreeMap<TenantTokenName, TenantTokenSpec>,
}

impl TenantPolicy {
    /// Returns true when no token is declared, in which case no request
    /// carries a tenant context.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Returns validation errors for this declaration, prefixed with
    /// `path_prefix`.
    ///
    /// The declaration is validated by compiling it, so a configuration that
    /// validates is a configuration the engine can build a registry from.
    #[must_use]
    pub fn validation_errors(&self, path_prefix: &str) -> Vec<String> {
        match compiled::TenantRegistry::compile(self) {
            Ok(_) => Vec::new(),
            Err(errors) => errors
                .into_iter()
                .map(|error| format!("{path_prefix}: {error}"))
                .collect(),
        }
    }
}

/// A named identity bundle: the extractors that must all resolve for the
/// token to be present on a request.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TenantTokenSpec {
    /// The extractors making up this token. A token with no extractor cannot
    /// resolve and is rejected at startup.
    #[serde(default)]
    pub extractors: Vec<Extractor>,
}

/// One rule filling one key from the request.
///
/// `deny_unknown_fields` is not applied here because `source` is flattened;
/// an unrecognized field is instead reported as an unknown extractor source,
/// which is the more useful message.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct Extractor {
    /// The key this extractor fills.
    pub key: TenantKeyName,
    /// Where the value comes from.
    #[serde(flatten)]
    pub source: ExtractorSource,
}

/// The sources an extractor may read.
///
/// Further kinds -- a locally minted value, the peer address, a value handed
/// across a pipeline boundary -- are additive and are introduced with the
/// producers that need them.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtractorSource {
    /// An inbound transport header, matched exactly and case-insensitively.
    ///
    /// Glob and regex matching are not supported: a compiled table cannot
    /// intern the literals a pattern would admit.
    TransportHeader(String),
}
