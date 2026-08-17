// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Declarative configuration surface for pdata context entries.
//!
//! This module holds the user-facing YAML shapes only. Nothing here is
//! used on the per-message path: the [`ContextCompiler`] consumes these
//! declarations once, at configuration time, and produces the dense
//! lookup tables described in [`super::schema`].
//!
//! [`ContextCompiler`]: super::ContextCompiler

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The `policies.context` section.
///
/// ```yaml
/// policies:
///   context:
///     entries:
///       product_user:
///         - type: authorized_identity
///           name: customer_id
///         - type: transport_header
///           name: workspace_id
///     required_in:
///       - node: receiver0
///         entry: product_user
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPolicy {
    /// User-defined context entries, keyed by entry name.
    ///
    /// A `BTreeMap` is used so that slot assignment is deterministic
    /// across processes: two engines given the same configuration
    /// compile to byte-identical lookup tables.
    #[serde(default)]
    pub entries: BTreeMap<String, EntryDecl>,

    /// Entries that must be present at a given node.
    #[serde(default)]
    pub required_in: Vec<EntryRequirement>,

    /// Entries that may be present at a given node. Declaring an entry
    /// optional has no runtime effect; it documents intent and forces
    /// the entry to be reachable from the node.
    #[serde(default)]
    pub optional_in: Vec<EntryRequirement>,
}

/// A per-node statement about one entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EntryRequirement {
    /// Node identifier the requirement applies to.
    pub node: String,
    /// Name of the context entry.
    pub entry: String,
}

/// An entry declaration: either a single component or an ordered list.
///
/// A single-component entry is written without a list for readability,
/// matching the `constant` and `network_info` examples in the RFC.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum EntryDecl {
    /// Exactly one component.
    Single(Component),
    /// An ordered list of components.
    List(Vec<Component>),
}

impl EntryDecl {
    /// Returns the components of this declaration in order.
    #[must_use]
    pub fn components(&self) -> &[Component] {
        match self {
            Self::Single(one) => std::slice::from_ref(one),
            Self::List(many) => many.as_slice(),
        }
    }
}

/// One component of an entry declaration.
///
/// Components come in two flavors, distinguished by
/// [`Component::is_condition`]:
///
/// - **Value components** contribute one dimension to the entry's key.
///   An entry with N value components is an N-dimensional entry.
/// - **Condition components** contribute no dimension. They gate the
///   presence of the whole entry.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Component {
    /// Value: an authorization claim previously stored under `name`.
    AuthorizedIdentity {
        /// Claim entry name.
        name: String,
    },
    /// Value: a captured transport header previously stored under `name`.
    TransportHeader {
        /// Header entry name.
        name: String,
    },
    /// Value: network information, for example `peer_socket_addr`.
    NetworkInfo {
        /// Network attribute name.
        name: String,
    },
    /// Value: a fixed string, resolved once at compile time.
    Constant {
        /// The constant text.
        value: String,
    },
    /// Value: freshly generated per message, for example `uuid7`.
    Randomness {
        /// Generator name. Only `uuid7` is recognized by this prototype.
        value: String,
    },
    /// Condition: transport header `name` must equal `value`.
    TransportHeaderMatch {
        /// Header entry name.
        name: String,
        /// Required value.
        value: String,
    },
    /// Condition: authorization claim `name` must equal `value`.
    AuthorizedIdentityMatch {
        /// Claim entry name.
        name: String,
        /// Required value.
        value: String,
    },
}

impl Component {
    /// Returns true when this component gates presence instead of
    /// contributing a dimension.
    #[must_use]
    pub const fn is_condition(&self) -> bool {
        matches!(
            self,
            Self::TransportHeaderMatch { .. } | Self::AuthorizedIdentityMatch { .. }
        )
    }

    /// The source this component reads from.
    #[must_use]
    pub const fn source_kind(&self) -> SourceKind {
        match self {
            Self::AuthorizedIdentity { .. } | Self::AuthorizedIdentityMatch { .. } => {
                SourceKind::AuthorizedIdentity
            }
            Self::TransportHeader { .. } | Self::TransportHeaderMatch { .. } => {
                SourceKind::TransportHeader
            }
            Self::NetworkInfo { .. } => SourceKind::NetworkInfo,
            Self::Constant { .. } => SourceKind::Constant,
            Self::Randomness { .. } => SourceKind::Randomness,
        }
    }

    /// The name this component is known by inside its entry, used to
    /// resolve `entry:component` references.
    ///
    /// Named sources use their source name. Anonymous sources
    /// (`constant`, `randomness`) use their kind, so `receiver:constant`
    /// addresses the single dimension of a constant entry.
    #[must_use]
    pub fn dimension_name(&self) -> &str {
        match self {
            Self::AuthorizedIdentity { name }
            | Self::AuthorizedIdentityMatch { name, .. }
            | Self::TransportHeader { name }
            | Self::TransportHeaderMatch { name, .. }
            | Self::NetworkInfo { name } => name,
            Self::Constant { .. } => "constant",
            Self::Randomness { .. } => "randomness",
        }
    }

    /// The key this component uses to address its source. For anonymous
    /// sources the key is the literal value, so that two entries naming
    /// the same constant share one source slot.
    #[must_use]
    pub fn source_key(&self) -> &str {
        match self {
            Self::AuthorizedIdentity { name }
            | Self::AuthorizedIdentityMatch { name, .. }
            | Self::TransportHeader { name }
            | Self::TransportHeaderMatch { name, .. }
            | Self::NetworkInfo { name } => name,
            Self::Constant { value } | Self::Randomness { value } => value,
        }
    }

    /// For condition components, the value the source must equal.
    #[must_use]
    pub fn condition_value(&self) -> Option<&str> {
        match self {
            Self::TransportHeaderMatch { value, .. }
            | Self::AuthorizedIdentityMatch { value, .. } => Some(value),
            _ => None,
        }
    }
}

/// The kind of a context value, carried alongside its bytes.
///
/// Equality and hashing are typed: two values are the same only when
/// their kinds *and* their bytes agree. This mirrors the telemetry
/// `EntityAttributeSet`, which hashes `AttributeField::type` next to the
/// value so that flattening to a comparable form cannot merge
/// semantically distinct values.
///
/// The concrete risk this closes is flattening. A multi-valued claim
/// `["a", "b"]` and a single-valued claim `"ab"` have identical
/// concatenated bytes; a gRPC `-bin` metadata value and a text header
/// can too. Without the kind they would name the same tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum ValueKind {
    /// UTF-8 text.
    #[default]
    Text,
    /// Arbitrary binary, for example gRPC `-bin` metadata.
    Binary,
    /// An ordered list of text values, encoded as `u32` length prefix
    /// followed by bytes, repeated. Used for multi-valued claims.
    TextList,
}

impl ValueKind {
    /// Wire encoding of the kind inside a [`ContextRecord`].
    ///
    /// [`ContextRecord`]: super::ContextRecord
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Text => 0,
            Self::Binary => 1,
            Self::TextList => 2,
        }
    }

    /// Decodes a kind byte, defaulting to [`ValueKind::Text`].
    #[must_use]
    pub const fn from_u8(byte: u8) -> Self {
        match byte {
            1 => Self::Binary,
            2 => Self::TextList,
            _ => Self::Text,
        }
    }

    /// Short label used in diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Binary => "binary",
            Self::TextList => "text_list",
        }
    }
}

impl From<otap_df_config::transport_headers::ValueKind> for ValueKind {
    fn from(kind: otap_df_config::transport_headers::ValueKind) -> Self {
        match kind {
            otap_df_config::transport_headers::ValueKind::Text => Self::Text,
            otap_df_config::transport_headers::ValueKind::Binary => Self::Binary,
        }
    }
}

/// Where a raw context value comes from.
///
/// Each kind owns an independent name space, mirroring Envoy's practice
/// of registering inline headers separately per header map type: a claim
/// named `customer_id` and a header named `customer_id` are different
/// sources and get different slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SourceKind {
    /// A claim returned by an authorization extension.
    AuthorizedIdentity,
    /// A captured transport header.
    TransportHeader,
    /// Network information about the peer.
    NetworkInfo,
    /// A compile-time constant.
    Constant,
    /// Per-message generated randomness.
    Randomness,
}

impl SourceKind {
    /// Every source kind, in slot-index order.
    pub const ALL: [SourceKind; 5] = [
        SourceKind::AuthorizedIdentity,
        SourceKind::TransportHeader,
        SourceKind::NetworkInfo,
        SourceKind::Constant,
        SourceKind::Randomness,
    ];

    /// Dense index of this kind, used to pick a name table.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::AuthorizedIdentity => 0,
            Self::TransportHeader => 1,
            Self::NetworkInfo => 2,
            Self::Constant => 3,
            Self::Randomness => 4,
        }
    }

    /// Short label used in diagnostics.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::AuthorizedIdentity => "authorized_identity",
            Self::TransportHeader => "transport_header",
            Self::NetworkInfo => "network_info",
            Self::Constant => "constant",
            Self::Randomness => "randomness",
        }
    }

    /// True when names of this kind are matched case-insensitively.
    /// Transport header names are ASCII case-insensitive on the wire.
    #[must_use]
    pub const fn is_case_insensitive(self) -> bool {
        matches!(self, Self::TransportHeader)
    }

    /// True when the value is known before any message arrives, so the
    /// compiler can pre-seed it into the record template.
    #[must_use]
    pub const fn is_compile_time(self) -> bool {
        matches!(self, Self::Constant)
    }
}
