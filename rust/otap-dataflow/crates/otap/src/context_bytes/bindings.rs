use std::sync::Arc;

use otel_arrow_dfe_config::context::{ContextRegisterId, ContextVersion};
use otel_arrow_dfe_config::transport_headers_policy::CompiledHeaderSchema;

use super::packed::{ContextBytesError, ContextRegister, HeaderValueKind, PdataContextBytes};

pub(super) const SCHEMA_CACHE_CAPACITY: usize = 8;

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

    fn get_or_insert_with(&mut self, version: ContextVersion, build: impl FnOnce() -> P) -> &P {
        if let Some(index) = self
            .entries
            .iter()
            .position(|(cached, _)| *cached == version)
        {
            return &self.entries[index].1;
        }
        self.insert(version, build())
    }
}

/// Version-linked lookup binding for the first value of one context register.
///
/// This is a transitional predicate-binding primitive: it resolves the
/// configured name once per input schema, then evaluates contexts using only
/// schema indices. The bounded cache is local to the node that owns the
/// binding, so replacing the binding also replaces its configuration
/// generation.
pub struct ContextRegisterValueBinding {
    symbol: String,
    cache: SchemaPlanCache<Option<ContextRegisterId>>,
}

impl ContextRegisterValueBinding {
    /// Creates a binding for one source-level register symbol.
    #[must_use]
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_ascii_lowercase(),
            cache: SchemaPlanCache::new(),
        }
    }

    /// Returns the first matching value in context arrival order.
    pub fn value<'a>(
        &mut self,
        context: &'a PdataContextBytes,
    ) -> Option<(HeaderValueKind, &'a [u8])> {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let register = *self.cache.get_or_insert_with(version, || {
            context
                .schema()
                .compiled_context()
                .linker()
                .resolve(&self.symbol)
                .ok()
        });
        register
            .and_then(|register| context.register(register))
            .and_then(|entry| entry.values().next())
    }
}

/// Version-linked binding for an ordered set of context registers.
///
/// Configuration symbols are linked once per compiler version. Evaluation
/// visits numeric registers in configuration order without name lookup.
pub struct ContextRegisterSetBinding {
    symbols: Box<[String]>,
    cache: SchemaPlanCache<Box<[Option<ContextRegisterId>]>>,
}

impl ContextRegisterSetBinding {
    /// Creates a binding for an ordered set of register symbols.
    #[must_use]
    pub fn new<'a>(symbols: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            symbols: symbols.into_iter().map(str::to_ascii_lowercase).collect(),
            cache: SchemaPlanCache::new(),
        }
    }

    /// Visits each configured register that is present in the context.
    ///
    /// Returns the number of entries visited.
    pub fn visit_present(
        &mut self,
        context: &PdataContextBytes,
        mut visitor: impl FnMut(usize, ContextRegister<'_>),
    ) -> usize {
        let version = context
            .schema()
            .compiled_context()
            .register_file()
            .version();
        let symbols = &self.symbols;
        let slots = self.cache.get_or_insert_with(version, || {
            let linker = context.schema().compiled_context().linker();
            symbols
                .iter()
                .map(|symbol| linker.resolve(symbol).ok())
                .collect()
        });
        let mut visited = 0;
        for (ordinal, register) in slots.iter().copied().enumerate() {
            let Some(entry) = register.and_then(|register| context.register(register)) else {
                continue;
            };
            visitor(ordinal, entry);
            visited += 1;
        }
        visited
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
