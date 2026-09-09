// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Validated, normalized names for context entries.

use crate::error::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A validated context entry name in ASCII lowercase.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(try_from = "String", into = "String")]
pub struct ContextEntryName(Box<str>);

impl ContextEntryName {
    /// Returns the normalized name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid transport-header context entry reference `{value}`; expected a single printable ASCII name"
                ),
            });
        }
        Ok(Self(value.to_ascii_lowercase().into()))
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

/// An exact entry selection or qualified member reference.
///
/// Bare names select whole entries. Consumers requiring a field accept a bare
/// name only for a standalone, singleton-field entry.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct ContextEntryRef {
    entry: ContextEntryName,
    field: Option<ContextEntryName>,
}

impl ContextEntryRef {
    /// Returns the explicitly selected entry.
    #[must_use]
    pub fn entry(&self) -> &ContextEntryName {
        &self.entry
    }

    /// Returns the explicitly selected member, if any.
    #[must_use]
    pub fn field(&self) -> Option<&ContextEntryName> {
        self.field.as_ref()
    }
}

impl TryFrom<&str> for ContextEntryRef {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut parts = value.split(':');
        let entry = ContextEntryName::try_from(parts.next().unwrap_or_default())?;
        let field = parts.next().map(ContextEntryName::try_from).transpose()?;
        if parts.next().is_some() {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid context reference `{value}`; expected `entry` or `entry:field`"
                ),
            });
        }
        Ok(Self { entry, field })
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
        self.entry.fmt(f)?;
        if let Some(field) = &self.field {
            write!(f, ":{field}")?;
        }
        Ok(())
    }
}

impl From<ContextEntryRef> for String {
    fn from(value: ContextEntryRef) -> Self {
        value.to_string()
    }
}

/// Allows string comparisons in tests.
#[cfg(test)]
impl PartialEq<str> for ContextEntryName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

/// Allows string comparisons in tests.
#[cfg(test)]
impl PartialEq<&str> for ContextEntryName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a context name contains mixed-case ASCII.
    /// Guarantees: construction and string conversion return the lowercase name.
    #[test]
    fn context_entry_name_normalizes_and_converts() {
        let name = ContextEntryName::try_from("X-Tenant_Id.1".to_owned()).expect("valid name");

        assert_eq!(name.as_str(), "x-tenant_id.1");
        assert_eq!(name.as_ref(), "x-tenant_id.1");
        assert_eq!(name.to_string(), "x-tenant_id.1");

        let owned: String = name.into();
        assert_eq!(owned, "x-tenant_id.1");
    }

    /// Scenario: a name is empty or contains whitespace or non-ASCII text.
    /// Guarantees: invalid names return a configuration error.
    #[test]
    fn context_entry_name_rejects_invalid_input() {
        for invalid in ["", "two words", "line\nbreak", "caf\u{e9}"] {
            let error = ContextEntryName::try_from(invalid).expect_err("invalid name");
            assert!(matches!(error, Error::InvalidUserConfig { .. }));
        }
    }

    /// Scenario: a mixed-case name passes through serde.
    /// Guarantees: serde reads and writes the lowercase name.
    #[test]
    fn context_entry_name_serde_uses_normalized_string() {
        let name: ContextEntryName = serde_json::from_str("\"X-Tenant\"").expect("deserialize");

        assert_eq!(name, "x-tenant");
        assert_eq!(
            serde_json::to_string(&name).expect("serialize"),
            "\"x-tenant\""
        );
    }
}
