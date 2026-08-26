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

/// Scalar value representation stored in a context register.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextScalarType {
    /// UTF-8 text.
    Text,
    /// Arbitrary bytes.
    Bytes,
    /// An OTLP `AnyValue`.
    AnyValue,
}

/// Runtime shape of a context register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextRegisterShape {
    /// One unnamed value.
    Scalar(ContextScalarType),
    /// Ordered unnamed values.
    ScalarList(ContextScalarType),
    /// One runtime string key and value.
    KeyValue(ContextScalarType),
    /// Ordered runtime string key/value associations.
    KeyValueList(ContextScalarType),
    /// A fixed record whose field identities and order are compiled.
    Record(ContextRecordId),
}

/// Dense record-shape number local to one [`ContextRegisterFile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContextRecordId(u16);

impl ContextRecordId {
    /// Returns the dense record-shape index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Dense field number local to one compiled record shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextFieldId(u16);

impl ContextFieldId {
    /// Returns the compiled field position.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }

    /// Returns the compact field representation used by context envelopes.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Restores an identifier from a record-local envelope position.
    #[must_use]
    pub const fn from_u16(value: u16) -> Self {
        Self(value)
    }
}

/// Fixed field layout for a schema-defined record register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRecordShape {
    fields: Box<[ContextRegisterField]>,
}

impl ContextRecordShape {
    /// Creates a fixed record shape.
    #[must_use]
    pub fn new(fields: impl Into<Box<[ContextRegisterField]>>) -> Self {
        Self {
            fields: fields.into(),
        }
    }

    /// Returns the record fields in compiled order.
    #[must_use]
    pub const fn fields(&self) -> &[ContextRegisterField] {
        &self.fields
    }
}

/// One field in a compiled record register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRegisterField {
    scalar_type: ContextScalarType,
    repeated: bool,
}

impl ContextRegisterField {
    /// Creates a scalar record field.
    #[must_use]
    pub const fn scalar(scalar_type: ContextScalarType) -> Self {
        Self {
            scalar_type,
            repeated: false,
        }
    }

    /// Creates a repeated record field.
    #[must_use]
    pub const fn repeated(scalar_type: ContextScalarType) -> Self {
        Self {
            scalar_type,
            repeated: true,
        }
    }

    /// Returns the field's scalar representation.
    #[must_use]
    pub const fn scalar_type(&self) -> ContextScalarType {
        self.scalar_type
    }

    /// Returns whether the field contains an ordered list.
    #[must_use]
    pub const fn is_repeated(&self) -> bool {
        self.repeated
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
    registers: Box<[ContextRegisterShape]>,
    records: Box<[ContextRecordShape]>,
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
        self.registers.len()
    }

    /// Returns whether the layout contains no registers.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.registers.is_empty()
    }

    /// Returns a register's runtime shape.
    #[must_use]
    pub fn shape(&self, register: ContextRegisterId) -> Option<&ContextRegisterShape> {
        self.registers.get(register.index())
    }

    /// Returns a compiled record shape.
    #[must_use]
    pub fn record(&self, record: ContextRecordId) -> Option<&ContextRecordShape> {
        self.records.get(record.index())
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
    record_fields: Box<[AHashMap<Box<str>, ContextFieldId>]>,
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

    /// Resolves one record-field symbol into a compiled position.
    pub fn resolve_field(
        &self,
        record: ContextRecordId,
        symbol: &str,
    ) -> Result<ContextFieldId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        self.record_fields
            .get(record.index())
            .and_then(|fields| fields.get(canonical.as_str()))
            .copied()
            .ok_or(ContextCompileError::UnknownField {
                record,
                symbol: canonical,
            })
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
    registers: Vec<ContextRegisterShape>,
    records: Vec<ContextRecordShape>,
    record_fields: Vec<AHashMap<Box<str>, ContextFieldId>>,
}

impl ContextCompiler {
    /// Starts a compiler for an orchestration-owned deployment generation.
    #[must_use]
    pub fn new(deployment_generation: u64) -> Self {
        Self {
            version: ContextVersion::next(deployment_generation),
            symbols: AHashMap::new(),
            register_symbols: Vec::new(),
            registers: Vec::new(),
            records: Vec::new(),
            record_fields: Vec::new(),
        }
    }

    /// Starts a compiler by copying an existing generation's declarations.
    ///
    /// The derived output receives a new version and can coexist with its
    /// source while processors project additional registers.
    #[must_use]
    pub fn derive(compiled: &CompiledContext) -> Self {
        let linker = compiled.linker();
        Self {
            version: ContextVersion::next(linker.register_file().version().deployment_generation()),
            symbols: linker.symbols.clone(),
            register_symbols: linker.register_symbols.to_vec(),
            registers: linker.register_file.registers.to_vec(),
            records: linker.register_file.records.to_vec(),
            record_fields: linker.record_fields.to_vec(),
        }
    }

    /// Declares a record shape and compiles its field symbols to positions.
    pub fn declare_record<'a>(
        &mut self,
        fields: impl IntoIterator<Item = (&'a str, ContextRegisterField)>,
    ) -> Result<ContextRecordId, ContextCompileError> {
        let id = u16::try_from(self.records.len())
            .map(ContextRecordId)
            .map_err(|_| ContextCompileError::TooManyRecords)?;
        let mut symbols = AHashMap::new();
        let mut compiled_fields = Vec::new();
        for (symbol, field) in fields {
            let canonical = canonical_symbol(symbol)?;
            if compiled_fields.len() >= usize::from(u16::MAX) {
                return Err(ContextCompileError::TooManyFields);
            }
            let field_id = ContextFieldId(
                u16::try_from(compiled_fields.len()).expect("field count checked above"),
            );
            if symbols
                .insert(canonical.clone().into_boxed_str(), field_id)
                .is_some()
            {
                return Err(ContextCompileError::DuplicateField {
                    record: id,
                    symbol: canonical,
                });
            }
            compiled_fields.push(field);
        }
        self.records
            .push(ContextRecordShape::new(compiled_fields.into_boxed_slice()));
        self.record_fields.push(symbols);
        Ok(id)
    }

    /// Declares or reuses a register symbol.
    ///
    /// Repeated declarations are accepted only when their runtime shape is
    /// identical. This supports multiple input selectors targeting one logical
    /// register without making the symbol part of the runtime value.
    pub fn declare(
        &mut self,
        symbol: &str,
        shape: ContextRegisterShape,
    ) -> Result<ContextRegisterId, ContextCompileError> {
        let canonical = canonical_symbol(symbol)?;
        if let Some(register) = self.symbols.get(canonical.as_str()).copied() {
            let declared = self
                .registers
                .get(register.index())
                .expect("declared register");
            return if declared == &shape {
                Ok(register)
            } else {
                Err(ContextCompileError::ConflictingRegister {
                    symbol: canonical,
                    first: declared.clone(),
                    second: shape,
                })
            };
        }

        let register = ContextRegisterId::from_index(self.registers.len())?;
        let symbol: Box<str> = canonical.into_boxed_str();
        let _ = self.symbols.insert(symbol.clone(), register);
        self.register_symbols.push(symbol);
        self.registers.push(shape);
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
        let register_file = Arc::new(ContextRegisterFile {
            version: self.version,
            registers: self.registers.into_boxed_slice(),
            records: self.records.into_boxed_slice(),
        });
        Arc::new(CompiledContext {
            linker: Arc::new(ContextLinker {
                register_file,
                symbols: self.symbols,
                register_symbols: self.register_symbols.into_boxed_slice(),
                record_fields: self.record_fields.into_boxed_slice(),
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
    /// A consumer referenced an undeclared record field.
    #[error("unknown field '{symbol}' in context record {record:?}")]
    UnknownField {
        /// Record containing the requested field.
        record: ContextRecordId,
        /// Unresolved source symbol.
        symbol: String,
    },
    /// A record declared one field symbol more than once.
    #[error("duplicate field '{symbol}' in context record {record:?}")]
    DuplicateField {
        /// Record containing the duplicate field.
        record: ContextRecordId,
        /// Duplicate source symbol.
        symbol: String,
    },
    /// Two declarations assigned incompatible shapes to one symbol.
    #[error("context register '{symbol}' has conflicting shapes: {first:?} and {second:?}")]
    ConflictingRegister {
        /// Conflicting source symbol.
        symbol: String,
        /// Shape declared first.
        first: ContextRegisterShape,
        /// Shape declared later.
        second: ContextRegisterShape,
    },
    /// Dense register identifiers overflowed.
    #[error("context compiler supports at most 65536 registers")]
    TooManyRegisters,
    /// Dense record identifiers overflowed.
    #[error("context compiler supports at most 65536 record shapes")]
    TooManyRecords,
    /// Dense field identifiers overflowed.
    #[error("context compiler supports at most 65535 fields per record")]
    TooManyFields,
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
        let declared = compiler
            .declare(
                "Tenant",
                ContextRegisterShape::Scalar(ContextScalarType::Text),
            )
            .expect("declare register");
        let referenced = compiler.resolve("tenant").expect("resolve register");
        let compiled = compiler.finish();

        assert_eq!(declared, referenced);
        assert_eq!(declared.index(), 0);
        assert_eq!(compiled.register_file().len(), 1);
        assert_eq!(
            compiled.register_file().shape(declared),
            Some(&ContextRegisterShape::Scalar(ContextScalarType::Text))
        );
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
        let first = compiler
            .declare(
                "tenant",
                ContextRegisterShape::ScalarList(ContextScalarType::Text),
            )
            .expect("first declaration");
        let second = compiler
            .declare(
                "TENANT",
                ContextRegisterShape::ScalarList(ContextScalarType::Text),
            )
            .expect("second declaration");

        assert_eq!(first, second);
        assert_eq!(compiler.finish().register_file().len(), 1);
    }

    /// Scenario: declarations assign incompatible runtime shapes to one symbol.
    /// Guarantees: compilation rejects ambiguity before producing executable state.
    #[test]
    fn conflicting_register_shapes_are_rejected() {
        let mut compiler = ContextCompiler::new(1);
        let _ = compiler
            .declare(
                "tenant",
                ContextRegisterShape::Scalar(ContextScalarType::Text),
            )
            .expect("first declaration");

        assert!(matches!(
            compiler.declare(
                "tenant",
                ContextRegisterShape::KeyValue(ContextScalarType::Text)
            ),
            Err(ContextCompileError::ConflictingRegister { .. })
        ));
    }

    /// Scenario: a composite register declares two named fields.
    /// Guarantees: field symbols compile to positions and do not enter the executable record shape.
    #[test]
    fn record_field_symbols_compile_to_positions() {
        let mut compiler = ContextCompiler::new(1);
        let record = compiler
            .declare_record([
                (
                    "customer_id",
                    ContextRegisterField::scalar(ContextScalarType::Text),
                ),
                (
                    "workspace_id",
                    ContextRegisterField::scalar(ContextScalarType::Text),
                ),
            ])
            .expect("record shape");
        let _ = compiler
            .declare("product_user", ContextRegisterShape::Record(record))
            .expect("record register");
        let compiled = compiler.finish();

        assert_eq!(
            compiled
                .linker()
                .resolve_field(record, "WORKSPACE_ID")
                .expect("compiled field")
                .index(),
            1
        );
        assert_eq!(
            compiled
                .register_file()
                .record(record)
                .expect("record shape")
                .fields()
                .len(),
            2
        );
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
