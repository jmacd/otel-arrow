// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context configuration.
//!
//! This module defines context entry names and typed selectors. Its submodules hold the rest of
//! the configured context model:
//!
//! - [`policy`]: scoped composite context definitions.
//! - [`authorized_identity`]: authorized identity claim capture.
//! - [`transport_policy`]: transport header capture and propagation policies.
//! - [`transport_headers`]: captured transport header data and the compiled
//!   capture and propagation engines.

/// Authorized identity claim capture policy.
pub mod authorized_identity;
/// Declarative context entry policies.
pub mod policy;
/// Context transfer contracts for topic and storage boundaries.
pub mod transfer;
/// Transport header core types and capture/propagation engines.
pub mod transport_headers;
/// Transport header capture and propagation policy declarations.
pub mod transport_policy;

use crate::error::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A context entry name.
///
/// The configured spelling is preserved. Equality, ordering, and hashing are
/// case-sensitive; callers that need transport-header matching semantics must
/// compare names case-insensitively at that boundary.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(try_from = "String", into = "String")]
pub struct ContextEntryName(Box<str>);

impl ContextEntryName {
    /// Returns the configured name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns an ASCII-lowercase copy.
    #[must_use]
    pub(crate) fn to_ascii_lowercase(&self) -> Self {
        Self(self.0.to_ascii_lowercase().into())
    }
}

impl AsRef<str> for ContextEntryName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for ContextEntryName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for ContextEntryName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ContextEntryName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.contains(':')
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid context entry name `{value}`; expected a single printable ASCII name"
                ),
            });
        }
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for ContextEntryName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl From<ContextEntryName> for String {
    fn from(value: ContextEntryName) -> Self {
        value.0.into()
    }
}

/// The name of a context entry, one of two forms:
///
/// 1. Primitive names resolve within a domain inferred from its
///    containing policy or explicitly configured.
/// 2. Qualified names resolve to a composite entry named field, which
///    defines the domain.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct ContextEntryRef {
    scope: Option<ContextEntryName>,
    name: ContextEntryName,
}

impl ContextEntryRef {
    /// Returns the containing scope, if the name is qualified.
    #[must_use]
    pub fn scope(&self) -> Option<&ContextEntryName> {
        self.scope.as_ref()
    }

    /// Returns the referenced context entry name.
    #[must_use]
    pub fn name(&self) -> &ContextEntryName {
        &self.name
    }
}

impl From<ContextEntryName> for ContextEntryRef {
    fn from(name: ContextEntryName) -> Self {
        Self { scope: None, name }
    }
}

impl TryFrom<&str> for ContextEntryRef {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut parts = value.split(':');
        let first = ContextEntryName::try_from(parts.next().unwrap_or_default())?;
        let second = parts.next().map(ContextEntryName::try_from).transpose()?;
        match (second, parts.next()) {
            (None, None) => Ok(Self {
                scope: None,
                name: first,
            }),
            (Some(name), None) => Ok(Self {
                scope: Some(first),
                name,
            }),
            _ => Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid context entry reference `{value}`; expected `name` or `scope:name`"
                ),
            }),
        }
    }
}

impl TryFrom<String> for ContextEntryRef {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl std::fmt::Display for ContextEntryRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(scope) = &self.scope {
            write!(f, "{scope}:")?;
        }
        self.name.fmt(f)
    }
}

impl From<ContextEntryRef> for String {
    fn from(value: ContextEntryRef) -> Self {
        value.to_string()
    }
}

impl PartialEq<str> for ContextEntryName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ContextEntryName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Interpretation of a context projection selector.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum ContextSelectorType {
    /// All captured transport headers.
    AllCaptured,
    /// No default selection.
    #[default]
    None,
    /// Transport-header primitives or qualified members from any composite domain.
    Named,
    /// Verified authorization primitives, not qualified composite members.
    AuthorizedIdentity,
    /// Every member of each named composite.
    Composite,
    /// Imported configured constants.
    Constant,
    /// Imported generated values.
    Randomness,
    /// Receiver-observed network metadata.
    NetworkInfo,
}

impl ContextSelectorType {
    /// Returns the configuration spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllCaptured => "all_captured",
            Self::None => "none",
            Self::Named => "named",
            Self::AuthorizedIdentity => "authorized_identity",
            Self::Composite => "composite",
            Self::Constant => "constant",
            Self::Randomness => "randomness",
            Self::NetworkInfo => "network_info",
        }
    }
}

/// Selects context values independently of a consumer's action or output encoding.
///
/// All name-based strategies use ordered `ContextEntryRef` lists. Unqualified
/// `named` references select transport headers; qualified references retain their
/// composite's domain and presence gate. Other name-based strategies require
/// unqualified names. Consumers validate which strategies they support.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(deny_unknown_fields)]
pub struct ContextSelector {
    /// Projection strategy, defaulting to `none` for legacy propagation settings.
    #[serde(rename = "type", default)]
    pub selector_type: ContextSelectorType,
    /// Required, nonempty references for name-based strategies; absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub named: Option<Vec<ContextEntryRef>>,
}

impl ContextSelector {
    /// Validates the common selector shape, independently of consumer capabilities.
    pub fn validate(&self) -> Result<(), String> {
        let kind = self.selector_type.as_str();
        match (&self.selector_type, &self.named) {
            (ContextSelectorType::AllCaptured | ContextSelectorType::None, Some(_)) => {
                return Err(format!("'named' must not be set when type is '{kind}'"));
            }
            (ContextSelectorType::AllCaptured | ContextSelectorType::None, None) => {}
            (_, None) => return Err(format!("'named' list is required when type is '{kind}'")),
            (_, Some(names)) if names.is_empty() => {
                return Err(format!(
                    "'named' list must not be empty when type is '{kind}'"
                ));
            }
            (
                ContextSelectorType::AuthorizedIdentity
                | ContextSelectorType::Composite
                | ContextSelectorType::Constant
                | ContextSelectorType::Randomness
                | ContextSelectorType::NetworkInfo,
                Some(names),
            ) => {
                if let Some(reference) = names.iter().find(|reference| reference.scope().is_some())
                {
                    return Err(format!(
                        "type '{kind}' requires unqualified names, not '{reference}'; use type 'named' for composite members"
                    ));
                }
            }
            (ContextSelectorType::Named, Some(_)) => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: typed selectors contain primitives, qualified members, or a whole composite.
    /// Guarantees: their tags and ordered references round-trip without untagged shape inference.
    #[test]
    fn context_selectors_round_trip() {
        for json in [
            serde_json::json!({"type": "named", "named": ["workspace", "tenant:customer"]}),
            serde_json::json!({"type": "authorized_identity", "named": ["customer", "roles"]}),
            serde_json::json!({"type": "composite", "named": ["tenant", "delivery"]}),
            serde_json::json!({"type": "none"}),
            serde_json::json!({"type": "all_captured"}),
        ] {
            let selector: ContextSelector = serde_json::from_value(json.clone()).expect("selector");
            selector.validate().expect("nonempty selection");
            assert_eq!(serde_json::to_value(selector).expect("serialize"), json);
        }
    }

    /// Scenario: a selector uses an old shorthand, an unknown tag, or malformed fields.
    /// Guarantees: invalid shapes and empty selections fail rather than becoming default selections.
    #[test]
    fn context_selectors_reject_invalid_shapes() {
        for json in [
            serde_json::json!("tenant"),
            serde_json::json!({"type": "transport_header", "name": "workspace"}),
            serde_json::json!({"named": ["workspace"]}),
            serde_json::json!({"type": "named", "name": "workspace"}),
            serde_json::json!({"type": "named", "named": ["workspace"], "extra": true}),
            serde_json::json!({"type": "authorized_identity", "named": ["tenant:customer"]}),
            serde_json::json!({"type": "composite", "name": "tenant"}),
            serde_json::json!({"type": "composite", "named": ["tenant:customer"]}),
            serde_json::json!({"type": "none", "named": ["tenant"]}),
            serde_json::json!({"type": "all_captured", "named": ["tenant"]}),
        ] {
            if let Ok(selector) = serde_json::from_value::<ContextSelector>(json) {
                assert!(selector.validate().is_err());
            }
        }
        for kind in ["named", "authorized_identity", "composite"] {
            let selector: ContextSelector =
                serde_json::from_value(serde_json::json!({"type": kind, "named": []}))
                    .expect("typed selector");
            assert!(selector.validate().is_err());
            let selector: ContextSelector =
                serde_json::from_value(serde_json::json!({"type": kind})).expect("typed selector");
            assert!(selector.validate().is_err());
        }
    }

    /// Scenario: a legacy propagation selector omits its type or optional name list.
    /// Guarantees: an empty selector still means none and explicit null names retain legacy behavior.
    #[test]
    fn context_selector_preserves_legacy_defaults() {
        for json in [serde_json::json!({}), serde_json::json!({"named": null})] {
            let selector: ContextSelector = serde_json::from_value(json).expect("legacy selector");
            selector.validate().expect("default shape");
            assert_eq!(selector, ContextSelector::default());
            assert_eq!(selector.selector_type, ContextSelectorType::None);
        }
    }

    /// Scenario: a context name contains mixed-case ASCII.
    /// Guarantees: construction and string conversion preserve the configured spelling.
    #[test]
    fn context_entry_name_preserves_and_converts() {
        let name = ContextEntryName::try_from("X-Tenant_Id.1".to_owned()).expect("valid name");

        assert_eq!(name.as_str(), "X-Tenant_Id.1");
        assert_eq!(name.as_ref(), "X-Tenant_Id.1");
        assert_eq!(name.to_string(), "X-Tenant_Id.1");

        let owned: String = name.into();
        assert_eq!(owned, "X-Tenant_Id.1");
    }

    /// Scenario: context names differ only by ASCII case.
    /// Guarantees: their stored identities remain distinct.
    #[test]
    fn context_entry_name_identity_is_case_sensitive() {
        use std::collections::{BTreeSet, HashSet};

        let upper = ContextEntryName::try_from("X-Tenant").expect("valid name");
        let lower = ContextEntryName::try_from("x-tenant").expect("valid name");

        assert_ne!(upper, lower);
        assert_eq!(HashSet::from([upper.clone(), lower.clone()]).len(), 2);
        assert_eq!(BTreeSet::from([upper, lower]).len(), 2);
    }

    /// Scenario: a context name is empty, composite, contains whitespace, or is non-ASCII.
    /// Guarantees: invalid context entry names are rejected.
    #[test]
    fn context_entry_name_rejects_invalid_forms() {
        for value in ["", "entry:member", "two words", "line\nbreak", "caf\u{e9}"] {
            assert!(ContextEntryName::try_from(value).is_err(), "{value:?}");
        }
    }

    /// Scenario: a mixed-case name passes through serde.
    /// Guarantees: serde preserves the configured spelling.
    #[test]
    fn context_entry_name_serde_preserves_string() {
        let name: ContextEntryName = serde_json::from_str("\"X-Tenant\"").expect("deserialize");

        assert_eq!(name, "X-Tenant");
        assert_eq!(
            serde_json::to_string(&name).expect("serialize"),
            "\"X-Tenant\""
        );
    }

    /// Scenario: unqualified and scoped context entry references pass through parsing and serde.
    /// Guarantees: name spelling, case, and optional scope round trip exactly.
    #[test]
    fn context_entry_ref_round_trips_exactly() {
        let unqualified = ContextEntryRef::try_from("Customer_ID").expect("valid reference");
        assert_eq!(unqualified.scope(), None);
        assert_eq!(unqualified.name().as_str(), "Customer_ID");
        assert_eq!(unqualified.to_string(), "Customer_ID");

        let scoped =
            ContextEntryRef::try_from("Product_User:Customer_ID").expect("valid reference");
        assert_eq!(
            scoped.scope().map(ContextEntryName::as_str),
            Some("Product_User")
        );
        assert_eq!(scoped.name().as_str(), "Customer_ID");
        assert_eq!(scoped.to_string(), "Product_User:Customer_ID");
        assert_eq!(
            serde_json::to_string(&scoped).expect("serialize"),
            "\"Product_User:Customer_ID\""
        );
    }

    /// Scenario: JSON schema is generated for a context entry reference.
    /// Guarantees: the schema matches the string representation used by serde.
    #[test]
    fn context_entry_ref_schema_is_string() {
        let schema = serde_json::to_value(schemars::schema_for!(ContextEntryRef))
            .expect("schema should serialize");

        assert_eq!(schema["type"], "string");
    }

    /// Scenario: a context entry name is converted into a reference.
    /// Guarantees: conversion preserves the name without inventing a scope.
    #[test]
    fn context_entry_name_converts_to_unqualified_ref() {
        let reference =
            ContextEntryRef::from(ContextEntryName::try_from("tenant").expect("valid name"));

        assert_eq!(reference.scope(), None);
        assert_eq!(reference.name().as_str(), "tenant");
    }

    /// Scenario: a reference is empty, has an empty side, has extra separators, or is malformed.
    /// Guarantees: only exact `name` and `scope:name` forms are accepted.
    #[test]
    fn context_entry_ref_rejects_malformed_forms() {
        for configured in [
            "",
            ":field",
            "entry:",
            "entry:field:extra",
            "entry:two words",
            "entry:caf\u{e9}",
        ] {
            assert!(
                ContextEntryRef::try_from(configured).is_err(),
                "{configured:?}"
            );
        }
    }
}
