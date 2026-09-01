// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context entry references and compiler primitives for global context registers.

use crate::error::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

/// Dense register number within one compiled context policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextRegisterId(u16);

impl ContextRegisterId {
    /// Returns the register index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// Returns the compact register representation.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Restores a register from its compact representation.
    #[must_use]
    pub const fn from_u16(value: u16) -> Self {
        Self(value)
    }
}

/// Describes whether context consumers require name metadata for a register.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextNameRetention {
    /// Consumers only need the captured value.
    #[default]
    None,
    /// Consumers need the schema-normalized name.
    Canonical,
    /// Consumers need the original name observed on ingress.
    Observed,
}

/// One named context register and its name metadata requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContextRegisterRequirement<'a> {
    /// Logical context register name.
    pub name: &'a str,
    /// Strongest name metadata requirement for this register.
    pub retention: ContextNameRetention,
}

impl<'a> ContextRegisterRequirement<'a> {
    /// Declares a register with no name metadata requirement.
    #[must_use]
    pub const fn new(name: &'a str) -> Self {
        Self {
            name,
            retention: ContextNameRetention::None,
        }
    }

    /// Declares a register with a name metadata requirement.
    #[must_use]
    pub const fn with_retention(name: &'a str, retention: ContextNameRetention) -> Self {
        Self { name, retention }
    }
}

/// Compiled metadata for one dense context register.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContextRegisterDescriptor {
    name: Box<str>,
    retention: ContextNameRetention,
}

impl ContextRegisterDescriptor {
    /// Returns the canonical register name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the name metadata requirement.
    #[must_use]
    pub const fn retention(&self) -> ContextNameRetention {
        self.retention
    }
}

/// Immutable executable register layout.
#[derive(Debug, PartialEq, Eq)]
pub struct ContextRegisterLayout {
    register_count: usize,
}

impl ContextRegisterLayout {
    /// Returns the number of registers.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.register_count
    }

    /// Returns whether the layout has no registers.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.register_count == 0
    }
}

/// Global compiler output used to build node access plans.
#[derive(Debug, PartialEq, Eq)]
pub struct CompiledContext {
    register_layout: Arc<ContextRegisterLayout>,
    symbols: HashMap<Box<str>, ContextRegisterId>,
    registers: Box<[ContextRegisterDescriptor]>,
}

impl CompiledContext {
    /// Returns the executable register layout.
    #[must_use]
    pub fn register_layout(&self) -> &Arc<ContextRegisterLayout> {
        &self.register_layout
    }

    /// Resolves one configuration symbol.
    pub fn resolve(&self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        self.symbols
            .get(canonical.as_str())
            .copied()
            .ok_or(ContextCompileError::UnknownRegister { symbol: canonical })
    }

    /// Returns the compiled metadata for one register.
    #[must_use]
    pub fn register(&self, register: ContextRegisterId) -> Option<&ContextRegisterDescriptor> {
        self.registers.get(register.index())
    }
}

/// The context compiler.
#[derive(Debug, Default)]
pub struct ContextCompiler {
    /// Symbol name to index
    symbols: HashMap<Box<str>, ContextRegisterId>,
    /// Registers in index position
    registers: Vec<ContextRegisterDescriptor>,
}

impl ContextCompiler {
    /// Starts an empty compiler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares or reuses one register requirement.
    pub fn declare(
        &mut self,
        requirement: ContextRegisterRequirement<'_>,
    ) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(requirement.name)?;
        if let Some(register) = self.symbols.get(canonical.as_str()).copied() {
            let slot = self
                .registers
                .get_mut(register.index())
                .expect("declared register has retention slot");
            slot.retention = slot.retention.max(requirement.retention);
            return Ok(register);
        }
        if self.registers.len() >= usize::from(u16::MAX) {
            return Err(ContextCompileError::TooManyRegisters);
        }
        let register = ContextRegisterId(
            u16::try_from(self.registers.len())
                .map_err(|_| ContextCompileError::TooManyRegisters)?,
        );
        let symbol: Box<str> = canonical.into_boxed_str();
        let _ = self.symbols.insert(symbol.clone(), register);
        self.registers.push(ContextRegisterDescriptor {
            name: symbol,
            retention: requirement.retention,
        });
        Ok(register)
    }

    /// Finishes the compiler.
    #[must_use]
    pub fn finish(self) -> Arc<CompiledContext> {
        Arc::new(CompiledContext {
            register_layout: Arc::new(ContextRegisterLayout {
                register_count: self.registers.len(),
            }),
            symbols: self.symbols,
            registers: self.registers.into_boxed_slice(),
        })
    }
}

fn canonical_symbol(symbol: &str) -> Result<String, ContextCompileError> {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return Err(ContextCompileError::EmptyRegister);
    }
    if !symbol.is_ascii() {
        return Err(ContextCompileError::NonAsciiRegister {
            symbol: symbol.to_string(),
        });
    }
    Ok(symbol.to_ascii_lowercase())
}

/// Context compilation failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ContextCompileError {
    /// A register symbol was empty.
    #[error("context register name must not be empty")]
    EmptyRegister,
    /// A register symbol was not ASCII.
    #[error("context register name '{symbol}' must contain only ASCII characters")]
    NonAsciiRegister {
        /// Invalid source symbol.
        symbol: String,
    },
    /// A plan referenced an undeclared register.
    #[error("unknown context register '{symbol}'")]
    UnknownRegister {
        /// Unresolved source symbol.
        symbol: String,
    },
    /// Dense register identifiers overflowed.
    #[error("context compiler supports at most 65535 registers")]
    TooManyRegisters,
}

/// A normalized transport-header context entry reference.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(try_from = "String", into = "String")]
#[schemars(with = "String")]
pub struct ContextEntryRef(String);

impl ContextEntryRef {
    /// Parses and normalizes a transport-header context entry reference.
    pub fn parse(raw: &str) -> Result<Self, Error> {
        if raw.is_empty() || raw.contains(':') || !raw.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "invalid transport-header context entry reference `{raw}`; expected a single printable ASCII name"
                ),
            });
        }
        Ok(Self(raw.to_ascii_lowercase()))
    }

    /// Returns the normalized reference.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ContextEntryRef {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for ContextEntryRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for ContextEntryRef {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<ContextEntryRef> for String {
    fn from(value: ContextEntryRef) -> Self {
        value.0
    }
}

impl From<&'static str> for ContextEntryRef {
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
                ContextNameRetention::Canonical,
            ))
            .expect("canonical tenant");
        assert_eq!(
            compiler.declare(ContextRegisterRequirement::with_retention(
                "tenant",
                ContextNameRetention::Observed,
            )),
            Ok(register)
        );
        let context = compiler.finish();

        assert_eq!(
            context.register(register).expect("register").retention(),
            ContextNameRetention::Observed
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
        let reference = ContextEntryRef::parse("X-Tenant").unwrap();
        assert_eq!(reference.as_str(), "x-tenant");

        let punctuation = ContextEntryRef::parse("Tenant/Region@1").unwrap();
        assert_eq!(punctuation.as_str(), "tenant/region@1");
    }

    /// Scenario: A transport-header reference has an unsupported form.
    /// Guarantees: empty, non-ASCII, whitespace, and composite names are rejected.
    #[test]
    fn rejects_invalid_reference_forms() {
        for invalid in ["", "entry:member", "entry member", "t\u{e9}nant"] {
            assert!(ContextEntryRef::parse(invalid).is_err(), "{invalid}");
        }
    }

    /// Scenario: A transport-header context entry reference is deserialized from YAML.
    /// Guarantees: configuration uses the canonical normalized string form.
    #[test]
    fn serde_uses_string_form() {
        let parsed: ContextEntryRef = serde_yaml::from_str("X-Tenant").unwrap();
        assert_eq!(parsed.as_str(), "x-tenant");
        assert_eq!(serde_yaml::to_string(&parsed).unwrap(), "x-tenant\n");
    }
}
