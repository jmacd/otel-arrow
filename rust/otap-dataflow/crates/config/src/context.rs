// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compiler primitives for schema-defined pdata context registers.
//!
//! Configuration symbols are resolved by [`ContextCompiler`] into dense
//! [`ContextRegisterId`] values. Executable register files contain no symbols
//! or transport names. The separate [`ContextLinker`] retains source-level
//! symbols only for compiling node plans and compatibility output mappings.

use ahash::AHashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

static NEXT_CONTEXT_REVISION: AtomicU64 = AtomicU64::new(1);

/// Identifies one immutable context compiler output.
///
/// Multiple versions may remain alive concurrently. Register identifiers are
/// meaningful only within the register file carrying this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContextVersion {
    deployment_generation: u64,
    compiler_revision: u64,
}

impl ContextVersion {
    /// Creates a new version within a deployment generation.
    fn next(deployment_generation: u64) -> Self {
        Self {
            deployment_generation,
            compiler_revision: NEXT_CONTEXT_REVISION.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Returns the orchestration-owned deployment generation.
    #[must_use]
    pub const fn deployment_generation(self) -> u64 {
        self.deployment_generation
    }

    /// Returns the process-local compiler revision.
    #[must_use]
    pub const fn compiler_revision(self) -> u64 {
        self.compiler_revision
    }
}

/// Dense register number local to one [`ContextRegisterFile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextRegisterId(u16);

impl ContextRegisterId {
    /// Returns the dense register index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// Returns the compact register representation used by context envelopes.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Restores an identifier from a register-file-local envelope slot.
    #[must_use]
    pub const fn from_u16(value: u16) -> Self {
        Self(value)
    }

    /// Creates an identifier from a checked dense index.
    pub(crate) fn from_index(index: usize) -> Result<Self, ContextCompileError> {
        u16::try_from(index)
            .map(Self)
            .map_err(|_| ContextCompileError::TooManyRegisters)
    }
}

/// Immutable executable register layout.
///
/// This type intentionally contains no configuration symbols or transport
/// names. Runtime contexts retain this object while their compiler generation
/// remains in flight.
#[derive(Debug, PartialEq, Eq)]
pub struct ContextRegisterFile {
    version: ContextVersion,
    register_count: usize,
    compatible_predecessors: Box<[ContextVersion]>,
}

impl ContextRegisterFile {
    /// Returns this layout's explicit version.
    #[must_use]
    pub const fn version(&self) -> ContextVersion {
        self.version
    }

    /// Returns the number of registers in the layout.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.register_count
    }

    /// Returns whether the layout contains no registers.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.register_count == 0
    }

    /// Returns whether register IDs compiled for `version` remain valid here.
    ///
    /// Derived register files only append declarations, so every inherited
    /// register retains its numeric ID.
    #[must_use]
    pub fn is_compatible_with(&self, version: ContextVersion) -> bool {
        self.version == version || self.compatible_predecessors.contains(&version)
    }
}

/// Compiler-only symbol linker for one register file.
///
/// Bindings use this object while building an executable plan. Message
/// processing uses only the resulting numeric register identifiers.
#[derive(Debug, PartialEq, Eq)]
pub struct ContextLinker {
    register_file: Arc<ContextRegisterFile>,
    symbols: AHashMap<Box<str>, ContextRegisterId>,
    register_symbols: Box<[Box<str>]>,
}

impl ContextLinker {
    /// Returns the executable register layout.
    #[must_use]
    pub fn register_file(&self) -> &Arc<ContextRegisterFile> {
        &self.register_file
    }

    /// Resolves one configuration symbol into a numeric register.
    pub fn resolve(&self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        self.symbols
            .get(canonical.as_str())
            .copied()
            .ok_or(ContextCompileError::UnknownRegister { symbol: canonical })
    }

    /// Resolves an ordered list of configuration symbols.
    pub fn resolve_all<'a>(
        &self,
        symbols: impl IntoIterator<Item = &'a str>,
    ) -> Result<Box<[ContextRegisterId]>, ContextCompileError> {
        symbols
            .into_iter()
            .map(|symbol| self.resolve(symbol))
            .collect()
    }

    /// Returns a source symbol for compatibility output compilation.
    ///
    /// Runtime values and executable register layouts do not retain this name.
    #[must_use]
    pub fn compatibility_symbol(&self, register: ContextRegisterId) -> Option<&str> {
        self.register_symbols
            .get(register.index())
            .map(AsRef::as_ref)
    }
}

/// Complete compiler output for one context version.
#[derive(Debug, PartialEq, Eq)]
pub struct CompiledContext {
    linker: Arc<ContextLinker>,
}

impl CompiledContext {
    /// Returns the compiler-only linker.
    #[must_use]
    pub fn linker(&self) -> &Arc<ContextLinker> {
        &self.linker
    }

    /// Returns the executable register file.
    #[must_use]
    pub fn register_file(&self) -> &Arc<ContextRegisterFile> {
        self.linker.register_file()
    }
}

/// Builds one immutable context register file and its link metadata.
#[derive(Debug)]
pub struct ContextCompiler {
    version: ContextVersion,
    symbols: AHashMap<Box<str>, ContextRegisterId>,
    register_symbols: Vec<Box<str>>,
    compatible_predecessors: Vec<ContextVersion>,
}

impl ContextCompiler {
    /// Starts a compiler for an orchestration-owned deployment generation.
    #[must_use]
    pub fn new(deployment_generation: u64) -> Self {
        Self {
            version: ContextVersion::next(deployment_generation),
            symbols: AHashMap::new(),
            register_symbols: Vec::new(),
            compatible_predecessors: Vec::new(),
        }
    }

    /// Starts a compiler by copying an existing generation's declarations.
    ///
    /// The derived output receives a new version and can coexist with its
    /// source while processors project additional registers.
    #[must_use]
    pub fn derive(compiled: &CompiledContext) -> Self {
        let linker = compiled.linker();
        let register_file = linker.register_file();
        let mut compatible_predecessors = register_file.compatible_predecessors.to_vec();
        compatible_predecessors.push(register_file.version());
        Self {
            version: ContextVersion::next(register_file.version().deployment_generation()),
            symbols: linker.symbols.clone(),
            register_symbols: linker.register_symbols.to_vec(),
            compatible_predecessors,
        }
    }

    /// Declares or reuses a register symbol.
    ///
    /// Repeated declarations reuse the same register. This supports multiple
    /// input selectors targeting one logical register without making the
    /// symbol part of the runtime value.
    pub fn declare(&mut self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        if let Some(register) = self.symbols.get(canonical.as_str()).copied() {
            return Ok(register);
        }

        let register = ContextRegisterId::from_index(self.register_symbols.len())?;
        let symbol: Box<str> = canonical.into_boxed_str();
        let _ = self.symbols.insert(symbol.clone(), register);
        self.register_symbols.push(symbol);
        Ok(register)
    }

    /// Resolves a previously declared symbol while compiling a consumer.
    pub fn resolve(&self, symbol: &str) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        self.symbols
            .get(canonical.as_str())
            .copied()
            .ok_or(ContextCompileError::UnknownRegister { symbol: canonical })
    }

    /// Finishes the immutable executable layout and compiler linker.
    #[must_use]
    pub fn finish(self) -> Arc<CompiledContext> {
        let register_count = self.register_symbols.len();
        let register_file = Arc::new(ContextRegisterFile {
            version: self.version,
            register_count,
            compatible_predecessors: self.compatible_predecessors.into_boxed_slice(),
        });
        Arc::new(CompiledContext {
            linker: Arc::new(ContextLinker {
                register_file,
                symbols: self.symbols,
                register_symbols: self.register_symbols.into_boxed_slice(),
            }),
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
    /// Register identifiers currently use an ASCII configuration namespace.
    #[error("context register name '{symbol}' must contain only ASCII characters")]
    NonAsciiRegister {
        /// Invalid source symbol.
        symbol: String,
    },
    /// A consumer referenced an undeclared register.
    #[error("unknown context register '{symbol}'")]
    UnknownRegister {
        /// Unresolved source symbol.
        symbol: String,
    },
    /// Dense register identifiers overflowed.
    #[error("context compiler supports at most 65536 registers")]
    TooManyRegisters,
}

impl fmt::Display for ContextVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}",
            self.deployment_generation, self.compiler_revision
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: producers and consumers refer to the same mixed-case configuration symbol.
    /// Guarantees: compilation resolves both references to one dense numeric register.
    #[test]
    fn compiler_eliminates_register_symbols() {
        let mut compiler = ContextCompiler::new(17);
        let declared = compiler.declare("Tenant").expect("declare register");
        let referenced = compiler.resolve("tenant").expect("resolve register");
        let compiled = compiler.finish();

        assert_eq!(declared, referenced);
        assert_eq!(declared.index(), 0);
        assert_eq!(compiled.register_file().len(), 1);
    }

    /// Scenario: two compiler runs use the same deployment generation.
    /// Guarantees: each immutable result receives a distinct concurrent version.
    #[test]
    fn compiler_versions_are_concurrent_and_explicit() {
        let first = ContextCompiler::new(9).finish();
        let second = ContextCompiler::new(9).finish();

        assert_eq!(first.register_file().version().deployment_generation(), 9);
        assert_eq!(second.register_file().version().deployment_generation(), 9);
        assert_ne!(
            first.register_file().version(),
            second.register_file().version()
        );
    }

    /// Scenario: several source selectors declare the same logical register and shape.
    /// Guarantees: declarations share one register without storing a source name in its shape.
    #[test]
    fn compatible_declarations_share_register() {
        let mut compiler = ContextCompiler::new(1);
        let first = compiler.declare("tenant").expect("first declaration");
        let second = compiler.declare("TENANT").expect("second declaration");

        assert_eq!(first, second);
        assert_eq!(compiler.finish().register_file().len(), 1);
    }

    /// Scenario: a consumer references a symbol absent from the compiler generation.
    /// Guarantees: linking fails explicitly rather than becoming runtime absence.
    #[test]
    fn unknown_register_is_a_compile_error() {
        let compiler = ContextCompiler::new(1);

        assert!(matches!(
            compiler.resolve("missing"),
            Err(ContextCompileError::UnknownRegister { .. })
        ));
    }
}
