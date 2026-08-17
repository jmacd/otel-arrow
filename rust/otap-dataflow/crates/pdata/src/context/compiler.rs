// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The context compiler: config declarations in, dense tables out.
//!
//! The compiler is the direct analogue of Envoy's
//! `CustomInlineHeaderRegistry`. Registration is open while the
//! configuration is being read and closed by
//! [`ContextCompiler::finalize`]. After finalize the set of interesting
//! sources and the set of entries can no longer change, which is what
//! makes every per-message operation an array index.
//!
//! ```text
//!   configuration time                       message time
//!   ------------------                       ------------
//!   ContextCompiler                          ContextBuilder
//!     declare_policy(yaml)                     set_header(name, value)   1 hash
//!     declare_entry(name, components)          set_claim(name, value)    1 hash
//!     finalize()  ---> Arc<ContextSchema> ---> build() -> ContextRecord
//!                          |                                  |
//!                   schema.entry("x") -> EntryHandle -----> record.key(handle)
//!                        (once)                             (array index)
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use ahash::AHashMap;

use super::config::{Component, ContextPolicy, SourceKind, ValueKind};
use super::schema::{
    Condition, ContextSchema, EntryDef, EntryHandle, RecordLayout, SourceDesc, SourceIndex,
    SourceSlot, ValueRange,
};

/// Hash seeds are fixed rather than randomized per process so that the
/// partition key a node computes for a given entry value is the same on
/// every engine in a fleet.
const HASH_SEEDS: [u64; 4] = [
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
];

/// Slots are addressed with `u16`, so a configuration may not exceed
/// this many sources or entries.
const MAX_SLOTS: usize = u16::MAX as usize;

/// Errors produced while compiling `policies.context`.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    /// The same entry name was declared twice.
    #[error("duplicate context entry `{name}`")]
    DuplicateEntry {
        /// Entry name.
        name: String,
    },
    /// An entry declared no value components, so it could never carry a
    /// value. Conditions alone do not make an entry.
    #[error("context entry `{name}` has no value components")]
    EmptyEntry {
        /// Entry name.
        name: String,
    },
    /// Two value components of one entry share a name, which would make
    /// `entry:component` references ambiguous.
    #[error("context entry `{entry}` has duplicate component name `{dim}`")]
    DuplicateDimension {
        /// Entry name.
        entry: String,
        /// Ambiguous component name.
        dim: String,
    },
    /// A `randomness` component named a generator this build does not
    /// implement.
    #[error("context entry `{entry}` uses unsupported randomness generator `{generator}`")]
    UnsupportedRandomness {
        /// Entry name.
        entry: String,
        /// Requested generator.
        generator: String,
    },
    /// A `required_in` or `optional_in` statement named an entry that
    /// was never declared.
    #[error("node `{node}` references undeclared context entry `{entry}`")]
    UndeclaredRequirement {
        /// Node identifier.
        node: String,
        /// Entry name.
        entry: String,
    },
    /// The configuration exceeded the addressable slot space.
    #[error("context configuration exceeds {MAX_SLOTS} {what}")]
    TooManySlots {
        /// What overflowed, `sources` or `entries`.
        what: &'static str,
    },
}

/// A pending entry, before slots are assigned.
struct PendingEntry {
    components: Vec<Component>,
}

/// Collects context declarations and compiles them into a
/// [`ContextSchema`].
#[derive(Default)]
pub struct ContextCompiler {
    /// Ordered by name so slot assignment is deterministic.
    entries: BTreeMap<String, PendingEntry>,
    /// Per-node requirements, validated at finalize.
    requirements: Vec<(String, String, bool)>,
}

impl ContextCompiler {
    /// Creates an empty compiler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares everything in a `policies.context` section.
    pub fn declare_policy(&mut self, policy: &ContextPolicy) -> Result<(), CompileError> {
        for (name, decl) in &policy.entries {
            self.declare_entry(name, decl.components())?;
        }
        for req in &policy.required_in {
            self.requirements
                .push((req.node.clone(), req.entry.clone(), true));
        }
        for req in &policy.optional_in {
            self.requirements
                .push((req.node.clone(), req.entry.clone(), false));
        }
        Ok(())
    }

    /// Declares one entry by name.
    pub fn declare_entry(
        &mut self,
        name: &str,
        components: &[Component],
    ) -> Result<(), CompileError> {
        if self.entries.contains_key(name) {
            return Err(CompileError::DuplicateEntry {
                name: name.to_owned(),
            });
        }
        if !components.iter().any(|c| !c.is_condition()) {
            return Err(CompileError::EmptyEntry {
                name: name.to_owned(),
            });
        }

        let mut seen: Vec<&str> = Vec::new();
        for component in components.iter().filter(|c| !c.is_condition()) {
            let dim = component.dimension_name();
            if seen.contains(&dim) {
                return Err(CompileError::DuplicateDimension {
                    entry: name.to_owned(),
                    dim: dim.to_owned(),
                });
            }
            seen.push(dim);
            if let Component::Randomness { value } = component
                && value != "uuid7"
            {
                return Err(CompileError::UnsupportedRandomness {
                    entry: name.to_owned(),
                    generator: value.clone(),
                });
            }
        }

        let previous = self.entries.insert(
            name.to_owned(),
            PendingEntry {
                components: components.to_vec(),
            },
        );
        debug_assert!(previous.is_none());
        Ok(())
    }

    /// Closes registration and produces the immutable schema.
    ///
    /// After this point the source table, the entry table and the record
    /// layout are fixed. Nodes bind by calling
    /// [`ContextSchema::entry`](super::ContextSchema::entry) and
    /// [`ContextSchema::resolve`](super::ContextSchema::resolve).
    pub fn finalize(self) -> Result<Arc<ContextSchema>, CompileError> {
        let mut builder = SchemaBuilder::default();

        // Pass 1: assign source slots and entry slots.
        let mut entry_defs: Vec<EntryDef> = Vec::with_capacity(self.entries.len());
        let mut entry_index: AHashMap<Box<str>, EntryHandle> = AHashMap::new();
        let mut dim_cursor: usize = 0;

        for (name, pending) in &self.entries {
            let mut conditions = Vec::new();
            let mut dims = Vec::new();
            let mut dim_names = Vec::new();

            for component in &pending.components {
                let slot = builder.intern_source(component)?;
                if component.is_condition() {
                    let value = builder
                        .intern_const(component.condition_value().unwrap_or_default().as_bytes());
                    conditions.push(Condition {
                        source: slot,
                        value,
                    });
                } else {
                    dims.push(slot);
                    dim_names.push(component.dimension_name().into());
                }
            }

            if dim_cursor + dims.len() > MAX_SLOTS {
                return Err(CompileError::TooManySlots { what: "dimensions" });
            }
            let handle = EntryHandle(
                u16::try_from(entry_defs.len())
                    .map_err(|_| CompileError::TooManySlots { what: "entries" })?,
            );
            let previous = entry_index.insert(name.as_str().into(), handle);
            debug_assert!(previous.is_none());

            entry_defs.push(EntryDef {
                name: name.as_str().into(),
                conditions: conditions.into_boxed_slice(),
                dim_base: dim_cursor as u16,
                dims: dims.into_boxed_slice(),
                dim_names: dim_names.into_boxed_slice(),
            });
            dim_cursor += entry_defs[entry_defs.len() - 1].dims.len();
        }

        // Pass 2: validate node requirements against the entry table.
        for (node, entry, _required) in &self.requirements {
            if !entry_index.contains_key(entry.as_str()) {
                return Err(CompileError::UndeclaredRequirement {
                    node: node.clone(),
                    entry: entry.clone(),
                });
            }
        }

        let layout = RecordLayout::new(entry_defs.len(), dim_cursor);
        let empty_bytes: Arc<[u8]> = Arc::from(vec![0u8; layout.header_len()]);

        Ok(Arc::new(ContextSchema {
            sources: builder.sources.into_boxed_slice(),
            source_index: builder.source_index.map(SourceIndex::build),
            random_slots: builder.random_slots.into_boxed_slice(),
            entries: entry_defs.into_boxed_slice(),
            entry_index,
            consts: builder.consts.into_boxed_slice(),
            initial_slots: builder.initial_slots.into_boxed_slice(),
            layout,
            hash_seed: ahash::RandomState::with_seeds(
                HASH_SEEDS[0],
                HASH_SEEDS[1],
                HASH_SEEDS[2],
                HASH_SEEDS[3],
            ),
            empty_bytes,
        }))
    }
}

/// Mutable accumulator used only inside [`ContextCompiler::finalize`].
#[derive(Default)]
struct SchemaBuilder {
    sources: Vec<SourceDesc>,
    source_index: [AHashMap<Box<str>, SourceSlot>; 5],
    initial_slots: Vec<ValueRange>,
    random_slots: Vec<SourceSlot>,
    consts: Vec<u8>,
}

impl SchemaBuilder {
    /// Appends bytes to the constant pool, returning their range.
    /// Identical constants are shared.
    ///
    /// Everything declared in YAML is text, so constants and condition
    /// match values are [`ValueKind::Text`]. A condition therefore does
    /// not match a binary header that happens to carry the same bytes.
    fn intern_const(&mut self, bytes: &[u8]) -> ValueRange {
        if let Some(off) = find_subslice(&self.consts, bytes) {
            return ValueRange {
                off: off as u32,
                len: bytes.len() as u32,
                kind: ValueKind::Text,
            };
        }
        let off = self.consts.len() as u32;
        self.consts.extend_from_slice(bytes);
        ValueRange {
            off,
            len: bytes.len() as u32,
            kind: ValueKind::Text,
        }
    }

    /// Returns the slot for a component's source, allocating one on
    /// first use. Two components naming the same source share a slot,
    /// so a header read by three entries is captured once.
    fn intern_source(&mut self, component: &Component) -> Result<SourceSlot, CompileError> {
        let kind = component.source_kind();
        let raw = component.source_key();
        let name: Box<str> = if kind.is_case_insensitive() {
            raw.to_ascii_lowercase().into()
        } else {
            raw.into()
        };

        if let Some(slot) = self.source_index[kind.index()].get(&name) {
            return Ok(*slot);
        }
        if self.sources.len() >= MAX_SLOTS {
            return Err(CompileError::TooManySlots { what: "sources" });
        }

        let slot = SourceSlot(self.sources.len() as u16);

        // A compile-time constant is resolved now and pre-seeded into
        // the initial slot table, so the message path treats it exactly
        // like any other source without ever recomputing it.
        let initial = if kind.is_compile_time() {
            self.intern_const(raw.as_bytes())
        } else {
            ValueRange::ABSENT
        };

        self.sources.push(SourceDesc {
            kind,
            name: name.clone(),
        });
        self.initial_slots.push(initial);
        if kind == SourceKind::Randomness {
            self.random_slots.push(slot);
        }
        let previous = self.source_index[kind.index()].insert(name, slot);
        debug_assert!(previous.is_none());
        Ok(slot)
    }
}

/// Finds `needle` inside `haystack`, used to share constant pool bytes.
/// Constant pools are tiny, so a naive scan is appropriate here.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    (0..=haystack.len() - needle.len()).find(|&i| &haystack[i..i + needle.len()] == needle)
}
