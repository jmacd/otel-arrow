// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compiler primitives for global context registers.

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

/// Immutable executable register layout.
#[derive(Debug, PartialEq, Eq)]
pub struct ContextRegisterFile {
    register_count: usize,
}

impl ContextRegisterFile {
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
    register_file: Arc<ContextRegisterFile>,
    symbols: HashMap<Box<str>, ContextRegisterId>,
    register_symbols: Box<[Box<str>]>,
}

impl CompiledContext {
    /// Returns the executable register layout.
    #[must_use]
    pub fn register_file(&self) -> &Arc<ContextRegisterFile> {
        &self.register_file
    }

    /// Resolves one configuration symbol.
    pub fn resolve(&self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        self.symbols
            .get(canonical.as_str())
            .copied()
            .ok_or(ContextCompileError::UnknownRegister { symbol: canonical })
    }

    /// Returns a source symbol for compatibility output.
    #[must_use]
    pub fn symbol(&self, register: ContextRegisterId) -> Option<&str> {
        self.register_symbols
            .get(register.index())
            .map(AsRef::as_ref)
    }
}

/// Builds one global register layout.
#[derive(Debug, Default)]
pub struct ContextCompiler {
    symbols: HashMap<Box<str>, ContextRegisterId>,
    register_symbols: Vec<Box<str>>,
}

impl ContextCompiler {
    /// Starts an empty compiler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares or reuses one register symbol.
    pub fn declare(&mut self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        if let Some(register) = self.symbols.get(canonical.as_str()).copied() {
            return Ok(register);
        }
        if self.register_symbols.len() >= usize::from(u16::MAX) {
            return Err(ContextCompileError::TooManyRegisters);
        }
        let register = ContextRegisterId(
            u16::try_from(self.register_symbols.len())
                .map_err(|_| ContextCompileError::TooManyRegisters)?,
        );
        let symbol: Box<str> = canonical.into_boxed_str();
        let _ = self.symbols.insert(symbol.clone(), register);
        self.register_symbols.push(symbol);
        Ok(register)
    }

    /// Finishes the compiler.
    #[must_use]
    pub fn finish(self) -> Arc<CompiledContext> {
        Arc::new(CompiledContext {
            register_file: Arc::new(ContextRegisterFile {
                register_count: self.register_symbols.len(),
            }),
            symbols: self.symbols,
            register_symbols: self.register_symbols.into_boxed_slice(),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: mixed-case producers declare one logical name.
    /// Guarantees: declarations share one dense register.
    #[test]
    fn declarations_are_canonical_and_dense() {
        let mut compiler = ContextCompiler::new();
        let first = compiler.declare("Tenant").expect("first");
        let second = compiler.declare("tenant").expect("second");
        let context = compiler.finish();

        assert_eq!(first, second);
        assert_eq!(first.index(), 0);
        assert_eq!(context.register_file().len(), 1);
        assert_eq!(context.resolve("TENANT"), Ok(first));
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
            compiler.declare(" "),
            Err(ContextCompileError::EmptyRegister)
        );
        assert!(matches!(
            compiler.declare("t\u{e9}nant"),
            Err(ContextCompileError::NonAsciiRegister { .. })
        ));
        assert_eq!(compiler.declare("tenant").expect("tenant").index(), 0);
    }
}
