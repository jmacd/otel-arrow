// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tenant token configuration types.
//!
//! A *tenant token* is a small vector of `key: value` identifiers describing
//! the tenant that a request belongs to. Tokens are produced by receivers from
//! request-scoped material (transport headers, peer address, and eventually
//! authorization data) and consumed by downstream nodes that evaluate
//! first-match-wins *conditions* over them.
//!
//! This module holds only the user-facing configuration shapes. The compiled,
//! hot-path representation lives in [`crate::tenant::compiled`].
//!
//! Extracted values may optionally be *retained*, which is what allows tenant
//! tokens to subsume the general-purpose transport header map: instead of
//! carrying every captured header as an owned string pair, the engine carries
//! only the configured token keys.
//!
//! A token deliberately says nothing about the wire name a retained value is
//! re-emitted under. The token is the portable identity; how it appears on the
//! wire is a site-specific decision belonging to the node that does the
//! emitting. Exporters therefore map `key -> outbound header name` themselves,
//! and the same token can be emitted under different names by two exporters.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod compiled;

/// Identifier of a tenant token definition, as used in engine configuration.
pub type TenantTokenId = String;

/// A single rule that resolves one token key from the request context.
///
/// Variants are untagged and disambiguated by their distinguishing field, so
/// an extractor reads as a flat map in YAML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum Extractor {
    /// Copy a transport header value into the token key.
    TransportHeader {
        /// Token key this extractor resolves.
        key: String,
        /// Transport header name to read, matched case-insensitively.
        transport_header: String,
        /// Retain the value in the request context. Retained values can be
        /// re-emitted by an exporter under a name that exporter chooses, and
        /// offered to a downstream pipeline by a boundary policy. Keys that
        /// are not retained participate in matching only and cost no bytes.
        #[serde(default)]
        retain: bool,
        /// Also carry the key's name, so the pair can be appended to telemetry
        /// attributes without re-encoding. This is the only way a name travels
        /// with a request; every other use of a key name is compiled out.
        /// Implies `retain`.
        #[serde(default)]
        bag: bool,
    },
    /// Resolve the token key to a static, configured value.
    ///
    /// This is how a pipeline mints an identity of its own, such as the tenant
    /// a dedicated pipeline is dedicated to.
    GenericKey {
        /// Token key this extractor resolves.
        key: String,
        /// Static value assigned to the key.
        generic_key: String,
        /// Retain the value in the request context. Retained values can be
        /// re-emitted by an exporter under a name that exporter chooses, and
        /// offered to a downstream pipeline by a boundary policy. Keys that
        /// are not retained participate in matching only and cost no bytes.
        #[serde(default)]
        retain: bool,
        /// Also carry the key's name, so the pair can be appended to telemetry
        /// attributes without re-encoding. This is the only way a name travels
        /// with a request; every other use of a key name is compiled out.
        /// Implies `retain`.
        #[serde(default)]
        bag: bool,
    },
    /// Resolve the token key to the network peer's address.
    RemoteAddress {
        /// Token key this extractor resolves.
        key: String,
        /// Must be `true`; selects this extractor kind.
        remote_address: bool,
        /// Retain the value in the request context. Retained values can be
        /// re-emitted by an exporter under a name that exporter chooses, and
        /// offered to a downstream pipeline by a boundary policy. Keys that
        /// are not retained participate in matching only and cost no bytes.
        #[serde(default)]
        retain: bool,
        /// Also carry the key's name, so the pair can be appended to telemetry
        /// attributes without re-encoding. This is the only way a name travels
        /// with a request; every other use of a key name is compiled out.
        /// Implies `retain`.
        #[serde(default)]
        bag: bool,
    },
    /// Resolve the token key from a value retained by an upstream pipeline
    /// and carried across a pipeline or group boundary.
    ///
    /// This is the import half of cross-boundary propagation: the upstream
    /// pipeline retains a key, the boundary policy admits it, and this
    /// extractor binds it into a token belonging to the downstream pipeline.
    ImportedKey {
        /// Token key this extractor resolves.
        key: String,
        /// Key name to read from the inbound cross-boundary context.
        imported_key: String,
        /// Retain the value in the request context. Retained values can be
        /// re-emitted by an exporter under a name that exporter chooses, and
        /// offered to a downstream pipeline by a boundary policy. Keys that
        /// are not retained participate in matching only and cost no bytes.
        #[serde(default)]
        retain: bool,
        /// Also carry the key's name, so the pair can be appended to telemetry
        /// attributes without re-encoding. This is the only way a name travels
        /// with a request; every other use of a key name is compiled out.
        /// Implies `retain`.
        #[serde(default)]
        bag: bool,
    },
}

impl Extractor {
    /// Token key resolved by this extractor.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::TransportHeader { key, .. }
            | Self::GenericKey { key, .. }
            | Self::RemoteAddress { key, .. }
            | Self::ImportedKey { key, .. } => key,
        }
    }

    /// Whether this extractor's value is retained in the request context.
    #[must_use]
    pub fn retain(&self) -> bool {
        match self {
            Self::TransportHeader { retain, bag, .. }
            | Self::GenericKey { retain, bag, .. }
            | Self::RemoteAddress { retain, bag, .. }
            | Self::ImportedKey { retain, bag, .. } => *retain || *bag,
        }
    }

    /// Whether this extractor's key name travels alongside its value.
    #[must_use]
    pub fn bag(&self) -> bool {
        match self {
            Self::TransportHeader { bag, .. }
            | Self::GenericKey { bag, .. }
            | Self::RemoteAddress { bag, .. }
            | Self::ImportedKey { bag, .. } => *bag,
        }
    }
}

/// A named tenant token definition: the list of extractors that must all
/// resolve for the token to be present on a request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantTokenSpec {
    /// Extractors that must all resolve for this token to be resolved.
    pub extractors: Vec<Extractor>,
}

/// One `{ key, value }` term of a condition. A missing `value` is a wildcard:
/// the key must be present with any value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    /// Token key tested by this entry.
    pub key: String,
    /// Required value, or `None` to accept any value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

/// An ordered list of entries selecting a destination. All entries must match
/// for the condition to match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Condition {
    /// Entries that must all match.
    pub entries: Vec<Entry>,
}

/// Engine-level map of tenant token definitions, shared across pipeline groups.
pub type TenantTokens = HashMap<TenantTokenId, TenantTokenSpec>;

/// Node configuration key holding a [`TenantRouting`] route table.
///
/// Every node that decides a destination from a tenant condition puts its
/// route table under this key, so the controller can collect the conditions
/// without knowing which node types exist.
pub const TENANT_ROUTING_KEY: &str = "tenant_routing";

/// Node configuration key holding [`TenantContextRules`].
///
/// Both sides of a boundary use the same key, so the allowlist that lets a
/// value out and the allowlist that lets it back in read the same way.
pub const TENANT_CONTEXT_KEY: &str = "tenant_context";

/// One route: the condition that selects it and the destination it names.
///
/// The destination is a plain name because what it names depends on the node
/// evaluating it -- an output port for `processor:tenant_router`, a topic for
/// `exporter:topic`. Keeping one route type means both kinds of routing are
/// declared, collected and validated by the same code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantRoute {
    /// Entries that must all match for this route to be selected.
    pub entries: Vec<Entry>,
    /// Destination this route selects, interpreted by the node.
    pub to: String,
}

/// Tenant-token routing configuration, shared by the controller -- which must
/// know every declared condition before nodes are built -- and by each routing
/// node itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantRouting {
    /// Tenant tokens this router binds. Empty binds every declared token.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tenant_tokens: Vec<TenantTokenId>,
    /// Routes evaluated first-match-wins.
    pub routes: Vec<TenantRoute>,
    /// Destination used when no route matches. Without it, unmatched data is
    /// nacked rather than delivered somewhere arbitrary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_to: Option<String>,
}

/// Names an outbound header and the token key whose retained value fills it.
///
/// This is where a retained token key acquires a wire name. Keeping the name
/// here rather than in the token definition means the same portable token can
/// be emitted as `x-acme-customer` by one exporter and `x-customer-id` by
/// another, and that a token carries no assumptions about any backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantHeader {
    /// Token key supplying the value.
    pub key: String,
    /// Outbound header name.
    pub header: String,
    /// Emit the value as binary metadata (gRPC `-bin` keys).
    #[serde(default)]
    pub binary: bool,
}

/// What tenant material may cross one trust boundary, in both directions.
///
/// A boundary between two pipeline groups is where tenant material can leak
/// between tenants, so each side names the keys it admits and everything
/// unnamed is dropped. One type serves both sides: an egress node reads
/// `export_keys`, an ingress node reads `import_keys` and `tenant_tokens`.
///
/// The inbound context is never adopted as-is. The receiving side admits the
/// keys it names, then resolves its own tokens over the admitted values plus
/// any locally minted ones, so the downstream pipeline evaluates conditions
/// against identities it declared itself.
///
/// This does not apply to a store-and-forward boundary such as
/// `processor:durable_buffer`, which writes and reads in one pipeline against
/// one registry. A context that leaves and returns there means what it always
/// meant, so there is nothing to re-derive and nothing to police.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantContextRules {
    /// Keys admitted from the inbound context, read by the ingress side.
    ///
    /// Absent leaves the decision to the node. A trust boundary reads that as
    /// admitting nothing, which is the fail-closed answer for a policy nobody
    /// wrote. An empty list says the same thing explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_keys: Option<Vec<String>>,
    /// Keys allowed to leave with the published data, read by the egress side.
    ///
    /// Independent of whether that side also routes: a node publishing to one
    /// fixed destination still has a boundary to police. Absent is read by the
    /// node, the same way as `import_keys`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export_keys: Option<Vec<String>>,
    /// Tokens resolved after import. Empty resolves every declared token.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tenant_tokens: Vec<TenantTokenId>,
}

impl TenantContextRules {
    /// The bound token names, or `None` to bind every declared token.
    #[must_use]
    pub fn bound_tokens(&self) -> Option<&[TenantTokenId]> {
        (!self.tenant_tokens.is_empty()).then_some(self.tenant_tokens.as_slice())
    }

    /// Keys this side admits, with an absent policy admitting nothing.
    #[must_use]
    pub fn import_or_none(&self) -> &[String] {
        self.import_keys.as_deref().unwrap_or(&[])
    }

    /// Keys this side lets out, with an absent policy letting nothing out.
    #[must_use]
    pub fn export_or_none(&self) -> &[String] {
        self.export_keys.as_deref().unwrap_or(&[])
    }
}

/// Groups work by tenant condition so that a merged unit never mixes tenants.
///
/// Used by the batch processor: each condition is one partition, so every
/// output batch carries a single tenant context and the retained values
/// survive the merge intact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantPartitioning {
    /// Tenant tokens this node binds. Empty binds every declared token.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tenant_tokens: Vec<TenantTokenId>,
    /// Partitions, evaluated first-match-wins. Data matching no partition
    /// falls into a catch-all partition that carries no tenant context.
    pub partitions: Vec<Condition>,
}

impl TenantPartitioning {
    /// The bound token names, or `None` to bind every declared token.
    #[must_use]
    pub fn bound_tokens(&self) -> Option<&[TenantTokenId]> {
        (!self.tenant_tokens.is_empty()).then_some(self.tenant_tokens.as_slice())
    }
}

impl TenantRouting {
    /// The bound token names, or `None` to bind every declared token.
    #[must_use]
    pub fn bound_tokens(&self) -> Option<&[TenantTokenId]> {
        (!self.tenant_tokens.is_empty()).then_some(self.tenant_tokens.as_slice())
    }

    /// The routes' conditions, in route order.
    #[must_use]
    pub fn conditions(&self) -> Vec<Condition> {
        self.routes
            .iter()
            .map(|route| Condition {
                entries: route.entries.clone(),
            })
            .collect()
    }
}
