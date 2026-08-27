// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use otel_arrow_dfe_config::context::{
    CompiledContext, ContextCompileError, ContextRegisterId, ContextVersion,
};
use otel_arrow_dfe_config::transport_headers_policy::CompiledHeaderSchema;

use super::packed::{ContextBytesError, ContextRegister, HeaderValueKind, PdataContextBytes};

// We admit a limited number of compiled schemas at once.
pub(super) const SCHEMA_CACHE_CAPACITY: usize = 2;

struct SchemaPlanCache<P> {
    entries: Vec<(ContextVersion, P)>,
}

impl<P> SchemaPlanCache<P> {
    const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn get(&self, version: ContextVersion) -> Option<&P> {
        self.entries
            .iter()
            .find(|(cached, _)| *cached == version)
            .map(|(_, plan)| plan)
    }

    fn insert(&mut self, version: ContextVersion, plan: P) -> &P {
        if self.entries.len() >= SCHEMA_CACHE_CAPACITY {
            let _ = self.entries.remove(0);
        }
        self.entries.push((version, plan));
        &self.entries.last().expect("inserted schema plan").1
    }
}

/// A configured binding failed to link against a context compiler version.
#[derive(Debug, thiserror::Error)]
pub enum ContextBindingError {
    /// A configured binding has no reachable context compiler to link against.
    #[error("context binding for {symbols} has no reachable context compiler")]
    NoReachableContextCompiler {
        /// Configured symbol or symbol list.
        symbols: String,
    },
    /// A configured register symbol is not declared by this compiler version.
    #[error("context binding configuration error for compiler version {version}: {source}")]
    Configuration {
        /// Version that failed to link.
        version: ContextVersion,
        /// Compiler diagnostic identifying the invalid binding.
        #[source]
        source: ContextCompileError,
    },
    /// Runtime data carried a compiler version omitted from the binding plan.
    #[error("context binding has no plan for compiler version {version}")]
    UnlinkedContextVersion {
        /// Unexpected compiler version.
        version: ContextVersion,
    },
}

fn binding_error(version: ContextVersion, source: ContextCompileError) -> ContextBindingError {
    ContextBindingError::Configuration { version, source }
}

/// Version-linked binding for one context register.
///
/// The configured symbol is linked against every reachable compiler before
/// the node starts. Runtime evaluation performs only a version-plan lookup and
/// numeric register access.
pub struct ContextRegisterBinding {
    plans: Box<[(ContextVersion, ContextRegisterId)]>,
}

impl ContextRegisterBinding {
    /// Links one source-level register symbol against every reachable compiler.
    pub fn new(
        symbol: &str,
        compilers: &[Arc<CompiledContext>],
    ) -> Result<Self, ContextBindingError> {
        let symbol = symbol.to_ascii_lowercase();
        if compilers.is_empty() {
            return Err(ContextBindingError::NoReachableContextCompiler { symbols: symbol });
        }
        let plans = compilers
            .iter()
            .map(|compiler| {
                let version = compiler.register_file().version();
                compiler
                    .linker()
                    .resolve(&symbol)
                    .map(|register| (version, register))
                    .map_err(|error| binding_error(version, error))
            })
            .collect::<Result<Box<[_]>, _>>()?;
        Ok(Self { plans })
    }

    /// Returns the configured register when it is present in this context.
    pub fn register<'a>(
        &self,
        context: &'a PdataContextBytes,
    ) -> Result<Option<ContextRegister<'a>>, ContextBindingError> {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let register_file = context.schema().compiled_context().register_file();
        let register = self
            .plans
            .iter()
            .find_map(|(planned_version, register)| {
                register_file
                    .is_compatible_with(*planned_version)
                    .then_some(*register)
            })
            .ok_or(ContextBindingError::UnlinkedContextVersion { version })?;
        Ok(context.register(register))
    }
}

/// Version-linked binding for an ordered set of context registers.
///
/// Configuration symbols are linked against every reachable compiler before
/// the node starts. Runtime evaluation visits numeric registers in
/// configuration order without name lookup or cache mutation.
pub struct ContextRegisterSetBinding {
    plans: Box<[(ContextVersion, Box<[ContextRegisterId]>)]>,
}

impl ContextRegisterSetBinding {
    /// Links an ordered set of symbols against every reachable compiler.
    pub fn new<'a>(
        symbols: impl IntoIterator<Item = &'a str>,
        compilers: &[Arc<CompiledContext>],
    ) -> Result<Self, ContextBindingError> {
        let symbols: Box<[String]> = symbols.into_iter().map(str::to_ascii_lowercase).collect();
        if compilers.is_empty() && !symbols.is_empty() {
            return Err(ContextBindingError::NoReachableContextCompiler {
                symbols: symbols.join(", "),
            });
        }
        let plans = compilers
            .iter()
            .map(|compiler| {
                let version = compiler.register_file().version();
                compiler
                    .linker()
                    .resolve_all(symbols.iter().map(String::as_str))
                    .map(|registers| (version, registers))
                    .map_err(|error| binding_error(version, error))
            })
            .collect::<Result<Box<[_]>, _>>()?;
        Ok(Self { plans })
    }

    /// Visits each configured register that is present in the context.
    ///
    /// Returns the number of entries visited.
    pub fn visit_present(
        &self,
        context: &PdataContextBytes,
        mut visitor: impl FnMut(usize, ContextRegister<'_>),
    ) -> Result<usize, ContextBindingError> {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let register_file = context.schema().compiled_context().register_file();
        let slots = self
            .plans
            .iter()
            .find_map(|(planned_version, slots)| {
                register_file
                    .is_compatible_with(*planned_version)
                    .then_some(slots.as_ref())
            })
            .ok_or(ContextBindingError::UnlinkedContextVersion { version })?;
        let mut visited = 0;
        for (ordinal, register) in slots.iter().copied().enumerate() {
            let Some(entry) = context.register(register) else {
                continue;
            };
            visitor(ordinal, entry);
            visited += 1;
        }
        Ok(visited)
    }
}

/// Compiled singleton context-entry projector for partition output.
///
/// This is a narrowly scoped initial projector that:
/// - Owns the configured output context-entry schema definition
/// - Caches prefix-preserving derived schemas keyed by input schema identity
/// - Appends a schema-indexed item with a singleton context entry
/// - Never creates inline items
///
/// Composite entry support remains future work. This binding is
/// processor-local and mutable (no shared state or locks). The cache is
/// bounded to [`SCHEMA_CACHE_CAPACITY`] entries with FIFO eviction.
pub struct ContextScalarProjectionBinding {
    /// Source-level symbol compiled for the projected output register.
    register_symbol: String,
    /// Schema with one item and one entry, used when there is no input context.
    standalone_schema: Arc<CompiledHeaderSchema>,
    /// schema_index within the standalone schema.
    standalone_schema_index: u16,
    /// Bounded FIFO cache of derived schemas keyed by input schema Arc pointer.
    /// Each plan contains (derived_schema, schema_index, entry_slot).
    cache: SchemaPlanCache<(Arc<CompiledHeaderSchema>, u16, u16)>,
}

impl ContextScalarProjectionBinding {
    /// Creates a scalar projection for the given register symbol.
    pub fn new(register_symbol: &str) -> Self {
        let (standalone_schema, standalone_schema_index, _entry_slot) =
            CompiledHeaderSchema::singleton_entry(register_symbol);
        Self {
            register_symbol: register_symbol.to_string(),
            standalone_schema_index,
            standalone_schema,
            cache: SchemaPlanCache::new(),
        }
    }

    /// Projects a partition value onto a context as a singleton context entry.
    ///
    /// If `input` is `Some`, the existing context entries/items are preserved
    /// and the partition entry is appended with a derived schema. If `input` is
    /// `None`, a new single-entry context is created using the standalone schema.
    pub fn project(
        &mut self,
        input: Option<&PdataContextBytes>,
        value: &[u8],
        kind: HeaderValueKind,
    ) -> Result<PdataContextBytes, ContextBytesError> {
        match input {
            Some(ctx) => {
                let (derived_schema, schema_index, entry_slot) =
                    self.derived_schema(ctx.schema())?;
                ctx.project_scalar(
                    schema_index,
                    entry_slot,
                    &self.register_symbol,
                    value,
                    kind,
                    derived_schema,
                )
            }
            None => {
                // Build a single-item context with the standalone schema (1 entry)
                PdataContextBytes::from_scalar(
                    &self.register_symbol,
                    value,
                    kind,
                    self.standalone_schema_index,
                    self.standalone_schema.clone(),
                )
            }
        }
    }

    /// Returns (derived_schema, schema_index, entry_slot) for the given input
    /// schema. Uses Arc pointer identity for cache lookup. Bounded to
    /// [`SCHEMA_CACHE_CAPACITY`] with FIFO eviction.
    fn derived_schema(
        &mut self,
        input_schema: &Arc<CompiledHeaderSchema>,
    ) -> Result<(Arc<CompiledHeaderSchema>, u16, u16), ContextBytesError> {
        let version = input_schema.compiled_context().register_file().version();
        if let Some((derived, index, slot)) = self.cache.get(version) {
            return Ok((derived.clone(), *index, *slot));
        }
        let (derived, schema_index, entry_slot) =
            CompiledHeaderSchema::derive_with_entry(input_schema, &self.register_symbol).map_err(
                |_| ContextBytesError::TooMany {
                    what: "schema items",
                },
            )?;
        let _ = self
            .cache
            .insert(version, (derived.clone(), schema_index, entry_slot));
        Ok((derived, schema_index, entry_slot))
    }
}
