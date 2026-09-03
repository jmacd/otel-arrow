// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context entry references and compiler primitives for global context registers.

use crate::error::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A context entry reference is a string that is resolved to a
/// context register name. Always normalized.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
pub struct ContextEntryName(Cow<'static, str>);

impl ContextEntryName {
    /// Parses a context entry reference while normalizing it. The
    /// value is not permitted to contain whitespace or control
    /// characters.
    pub fn parse(raw: &str) -> Result<Self, Error> {
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid transport-header context entry reference `{raw}`; expected a single printable ASCII name"
                ),
            });
        }
        Ok(Self::normalize_from(raw))
    }

    /// Different from parse, this simply normalizes the input
    /// assuming it is already valid.
    #[must_use]
    pub fn normalize_from(raw: &str) -> Self {
        Self(raw.to_ascii_lowercase().into())
    }

    /// Returns the name of the context entry, e.g., the value
    /// in the `store_as` field of a transport header capture.
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

impl std::fmt::Display for ContextEntryName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for ContextEntryName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<ContextEntryName> for String {
    fn from(value: ContextEntryName) -> Self {
        value.0.into()
    }
}

impl From<&'static str> for ContextEntryName {
    fn from(value: &'static str) -> Self {
        Self::parse(value).expect("invalid static context entry reference")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: mixed-case producers declare one logical name.
    /// Guarantees: declarations share one dense register.
    #[test]
    fn declarations_are_canonical_and_dense() {
        let mut compiler = ContextCompiler::new();
        let first = compiler
            .declare(ContextRegisterRequirement::new("Tenant"))
            .expect("first");
        let second = compiler
            .declare(ContextRegisterRequirement::new("tenant"))
            .expect("second");
        let context = compiler.finish();

        assert_eq!(first, second);
        assert_eq!(first.index(), 0);
        assert_eq!(context.register_layout().len(), 1);
        assert_eq!(context.resolve("TENANT"), Ok(first));
    }

    /// Scenario: multiple declarations require different name metadata for one register.
    /// Guarantees: the compiled context retains the strongest requirement.
    #[test]
    fn retention_uses_strongest_requirement() {
        let mut compiler = ContextCompiler::new();
        let register = compiler
            .declare(ContextRegisterRequirement::with_retention(
                "tenant",
                ContextAssociationListRetention::Canonical,
            ))
            .expect("canonical tenant");
        assert_eq!(
            compiler.declare(ContextRegisterRequirement::with_retention(
                "tenant",
                ContextAssociationListRetention::Observed,
            )),
            Ok(register)
        );
        let context = compiler.finish();

        assert_eq!(
            context.register(register).expect("register").retention(),
            ContextAssociationListRetention::Observed
        );
    }

    /// Scenario: a consumer names an undeclared register.
    /// Guarantees: resolution returns an explicit compile error.
    #[test]
    fn unknown_register_is_an_error() {
        let context = ContextCompiler::new().finish();

        assert!(matches!(
            context.resolve("missing"),
            Err(ContextCompileError::UnknownRegister { .. })
        ));
    }

    /// Scenario: declarations contain empty or non-ASCII names.
    /// Guarantees: invalid names fail before consuming a register.
    #[test]
    fn invalid_register_names_are_rejected() {
        let mut compiler = ContextCompiler::new();

        assert_eq!(
            compiler.declare(ContextRegisterRequirement::new(" ")),
            Err(ContextCompileError::EmptyRegister)
        );
        assert!(matches!(
            compiler.declare(ContextRegisterRequirement::new("t\u{e9}nant")),
            Err(ContextCompileError::NonAsciiRegister { .. })
        ));
        assert_eq!(
            compiler
                .declare(ContextRegisterRequirement::new("tenant"))
                .expect("tenant")
                .index(),
            0
        );
    }

    /// Scenario: A transport-header context entry reference is parsed.
    /// Guarantees: the header name is normalized to lowercase.
    #[test]
    fn parses_transport_header_reference() {
        let reference = ContextEntryName::parse("X-Tenant").unwrap();
        assert_eq!(reference.as_str(), "x-tenant");

        let punctuation = ContextEntryName::parse("Tenant/Region@1").unwrap();
        assert_eq!(punctuation.as_str(), "tenant/region@1");
    }

    /// Scenario: A transport-header reference has an unsupported form.
    /// Guarantees: empty, non-ASCII, whitespace, and composite names are rejected.
    #[test]
    fn rejects_invalid_reference_forms() {
        for invalid in ["", "entry:member", "entry member", "t\u{e9}nant"] {
            assert!(ContextEntryName::parse(invalid).is_err(), "{invalid}");
        }
    }

    /// Scenario: A transport-header context entry reference is deserialized from YAML.
    /// Guarantees: configuration uses the canonical normalized string form.
    #[test]
    fn serde_uses_string_form() {
        let parsed: ContextEntryName = serde_yaml::from_str("X-Tenant").unwrap();
        assert_eq!(parsed.as_str(), "x-tenant");
        assert_eq!(serde_yaml::to_string(&parsed).unwrap(), "x-tenant\n");
    }
}
