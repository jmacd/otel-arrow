// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The compiled context artifact: two lookup tables and one fixed layout.
//!
//! A [`ContextSchema`] is produced once by
//! [`ContextCompiler::finalize`](super::ContextCompiler::finalize) and is
//! then immutable and shared. It contains exactly three things:
//!
//! 1. **The source table.** Every raw value the configuration actually
//!    reads -- a header, a claim, a network attribute, a constant, a
//!    randomness generator -- is assigned a dense `SourceSlot`. Values
//!    the configuration never mentions have no slot and are dropped on
//!    arrival at zero cost. This is Envoy's inline-header registry.
//!
//! 2. **The entry table.** Every user-named context entry is assigned a
//!    dense `EntryHandle`. An entry is a short program over source
//!    slots: a list of conditions to check and a list of dimensions to
//!    concatenate.
//!
//! 3. **The record layout.** Because both tables are closed at
//!    finalize time, the size and shape of every per-message
//!    [`ContextRecord`](super::ContextRecord) header is a compile-time
//!    constant. Runtime lookup is therefore pointer arithmetic on a
//!    precomputed offset, never a hash and never a search.

use ahash::AHashMap;

use super::config::{SourceKind, ValueKind};

/// Dense index of a raw value in the source table.
///
/// Internal to the schema and the record builder; nodes never see one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceSlot(pub(crate) u16);

/// Handle to a whole context entry.
///
/// Obtained once, at configuration time, from
/// [`ContextSchema::entry`]. A node stores the handle and uses it on
/// every message; the lookup it performs is an array index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntryHandle(pub(crate) u16);

impl EntryHandle {
    /// Dense index of this entry, stable for the life of the schema.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Handle to a single dimension of a context entry.
///
/// Produced by resolving an `entry:component` reference such as
/// `product_user:customer_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DimHandle {
    pub(crate) entry: u16,
    pub(crate) dim: u16,
}

impl DimHandle {
    /// The entry this dimension belongs to.
    #[must_use]
    pub const fn entry(self) -> EntryHandle {
        EntryHandle(self.entry)
    }
}

/// A resolved reference to either a whole entry or one of its dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextRef {
    /// The entry as a whole, all dimensions concatenated.
    Entry(EntryHandle),
    /// A single dimension of an entry.
    Dim(DimHandle),
}

/// A typed range into a byte buffer. Absent is `off == u32::MAX`.
///
/// The kind travels with the range so that every comparison in this
/// module -- condition matching, memo equality, key hashing -- is typed
/// without any caller having to remember to include it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValueRange {
    pub(crate) off: u32,
    pub(crate) len: u32,
    pub(crate) kind: ValueKind,
}

impl ValueRange {
    pub(crate) const ABSENT: ValueRange = ValueRange {
        off: u32::MAX,
        len: 0,
        kind: ValueKind::Text,
    };

    pub(crate) const fn is_present(self) -> bool {
        self.off != u32::MAX
    }
}

/// Past this many names of one kind, hashing beats scanning.
const SCAN_LIMIT: usize = 16;

/// Name -> slot for one source kind.
///
/// Configurations name a handful of sources per kind, so a linear scan
/// that rejects on length before comparing bytes beats hashing on the
/// ingest path. A hash map takes over past [`SCAN_LIMIT`] so that large
/// configurations degrade gracefully rather than quadratically.
#[derive(Debug)]
pub(crate) struct SourceIndex {
    scan: Box<[(Box<str>, SourceSlot)]>,
    map: Option<AHashMap<Box<str>, SourceSlot>>,
}

impl SourceIndex {
    pub(crate) fn build(names: AHashMap<Box<str>, SourceSlot>) -> Self {
        let mut scan: Vec<(Box<str>, SourceSlot)> = names.clone().into_iter().collect();
        // Deterministic order keeps the schema reproducible.
        scan.sort_by(|a, b| a.0.cmp(&b.0));
        let map = (scan.len() > SCAN_LIMIT).then_some(names);
        Self {
            scan: scan.into_boxed_slice(),
            map,
        }
    }

    #[inline]
    pub(crate) fn get(&self, name: &str) -> Option<SourceSlot> {
        if let Some(map) = &self.map {
            return map.get(name).copied();
        }
        let wanted = name.as_bytes();
        self.scan
            .iter()
            .find(|(candidate, _)| candidate.as_bytes() == wanted)
            .map(|(_, slot)| *slot)
    }
}

/// A description of one source table slot, kept for diagnostics.
#[derive(Debug, Clone)]
pub struct SourceDesc {
    /// Where the value comes from.
    pub kind: SourceKind,
    /// Name the value is addressed by, lowercased for case-insensitive
    /// kinds. For `constant` and `randomness` this is the literal value.
    pub name: Box<str>,
}

/// A condition that must hold for an entry to be present.
#[derive(Debug, Clone)]
pub(crate) struct Condition {
    pub(crate) source: SourceSlot,
    pub(crate) value: ValueRange,
}

/// The compiled program for one context entry.
#[derive(Debug, Clone)]
pub(crate) struct EntryDef {
    pub(crate) name: Box<str>,
    /// Conditions, all of which must match.
    pub(crate) conditions: Box<[Condition]>,
    /// Value components in declaration order, one per dimension.
    pub(crate) dims: Box<[SourceSlot]>,
    /// Names of the dimensions, for `entry:component` resolution.
    pub(crate) dim_names: Box<[Box<str>]>,
    /// Index of this entry's first dimension in the global dimension
    /// space. Dimensions of one entry are contiguous both in the index
    /// and in the record data, so an entry's full key is one slice.
    pub(crate) dim_base: u16,
}

/// Byte offsets of the fixed regions of a [`ContextRecord`] buffer.
///
/// The record is a single allocation laid out as:
///
/// ```text
/// +---------------------+ 0
/// | presence bitmap     |  ceil(n_entries / 64) words
/// +---------------------+ hash_off
/// | entry hashes        |  n_entries * u64
/// +---------------------+ dim_off
/// | dimension index     |  n_dims * (u32 off, u32 len)
/// +---------------------+ kind_off
/// | dimension kinds     |  n_dims * u8
/// +---------------------+ data_off
/// | concatenated data   |  variable
/// +---------------------+
/// ```
///
/// Every offset below is known at finalize time, so reading entry `i`
/// costs one bit test plus one 8-byte load at a constant offset.
#[derive(Debug, Clone, Copy)]
pub struct RecordLayout {
    /// Number of entries in the schema.
    pub n_entries: usize,
    /// Total number of dimensions across all entries.
    pub n_dims: usize,
    /// Number of 64-bit words in the presence bitmap.
    pub presence_words: usize,
    /// Byte offset of the entry hash array.
    pub hash_off: usize,
    /// Byte offset of the dimension index.
    pub dim_off: usize,
    /// Byte offset of the dimension kind bytes.
    pub kind_off: usize,
    /// Byte offset of the data region.
    pub data_off: usize,
}

impl RecordLayout {
    pub(crate) fn new(n_entries: usize, n_dims: usize) -> Self {
        let presence_words = n_entries.div_ceil(64);
        let hash_off = presence_words * 8;
        let dim_off = hash_off + n_entries * 8;
        let kind_off = dim_off + n_dims * 8;
        let data_off = kind_off + n_dims;
        Self {
            n_entries,
            n_dims,
            presence_words,
            hash_off,
            dim_off,
            kind_off,
            data_off,
        }
    }

    /// Size of the fixed header, before any data bytes.
    #[must_use]
    pub const fn header_len(&self) -> usize {
        self.data_off
    }
}

/// Errors returned when binding a node to the compiled schema.
#[derive(Debug, thiserror::Error)]
pub enum BindError {
    /// The configuration never declared an entry with this name.
    #[error("unknown context entry `{name}`")]
    UnknownEntry {
        /// Entry name that was requested.
        name: String,
    },
    /// The entry exists but has no component with this name.
    #[error("context entry `{entry}` has no component `{dim}`")]
    UnknownDimension {
        /// Entry name.
        entry: String,
        /// Component name that was requested.
        dim: String,
    },
    /// The reference had more than one `:` separator.
    #[error("malformed context reference `{reference}`")]
    MalformedRef {
        /// The reference text.
        reference: String,
    },
}

/// The immutable, shared result of compiling `policies.context`.
///
/// See the [module documentation](self) for the three tables it holds.
#[derive(Debug)]
pub struct ContextSchema {
    /// Slot -> description, for diagnostics and for the build pass.
    pub(crate) sources: Box<[SourceDesc]>,
    /// Name -> slot, one table per source kind. Consulted once per
    /// arriving header or claim, never per entry lookup.
    pub(crate) source_index: [SourceIndex; 5],
    /// Slots of kind `randomness`, filled in at build time.
    pub(crate) random_slots: Box<[SourceSlot]>,
    /// Entry programs, in handle order.
    pub(crate) entries: Box<[EntryDef]>,
    /// Name -> handle, consulted only at configuration time.
    pub(crate) entry_index: AHashMap<Box<str>, EntryHandle>,
    /// Constant pool. Condition values and `constant` sources point here.
    pub(crate) consts: Box<[u8]>,
    /// Initial source slot table, with compile-time constants already
    /// resolved into the constant pool. Cloned into each builder.
    pub(crate) initial_slots: Box<[ValueRange]>,
    /// Fixed record geometry.
    pub(crate) layout: RecordLayout,
    /// Fixed hash seeds, so that partition keys agree across processes.
    pub(crate) hash_seed: ahash::RandomState,
    /// A shared all-absent record buffer, cloned instead of allocated
    /// when a message carries no context at all.
    pub(crate) empty_bytes: std::sync::Arc<[u8]>,
}

impl ContextSchema {
    /// Resolves an entry name to a handle. Call this at configuration
    /// time and keep the handle.
    pub fn entry(&self, name: &str) -> Result<EntryHandle, BindError> {
        self.entry_index
            .get(name)
            .copied()
            .ok_or_else(|| BindError::UnknownEntry {
                name: name.to_owned(),
            })
    }

    /// Resolves an `entry:component` reference to a dimension handle.
    pub fn dim(&self, entry: &str, dim: &str) -> Result<DimHandle, BindError> {
        let handle = self.entry(entry)?;
        let def = &self.entries[handle.index()];
        let pos = def
            .dim_names
            .iter()
            .position(|candidate| &**candidate == dim)
            .ok_or_else(|| BindError::UnknownDimension {
                entry: entry.to_owned(),
                dim: dim.to_owned(),
            })?;
        Ok(DimHandle {
            entry: handle.0,
            dim: pos as u16,
        })
    }

    /// Resolves either `entry` or `entry:component` syntax.
    pub fn resolve(&self, reference: &str) -> Result<ContextRef, BindError> {
        let mut parts = reference.splitn(3, ':');
        let entry = parts.next().unwrap_or("");
        match (parts.next(), parts.next()) {
            (None, _) => self.entry(entry).map(ContextRef::Entry),
            (Some(dim), None) => self.dim(entry, dim).map(ContextRef::Dim),
            (Some(_), Some(_)) => Err(BindError::MalformedRef {
                reference: reference.to_owned(),
            }),
        }
    }

    /// Number of declared entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of source slots the configuration actually reads.
    #[must_use]
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Fixed record geometry.
    #[must_use]
    pub const fn layout(&self) -> &RecordLayout {
        &self.layout
    }

    /// Name of an entry, for diagnostics.
    #[must_use]
    pub fn entry_name(&self, handle: EntryHandle) -> &str {
        &self.entries[handle.index()].name
    }

    /// Number of dimensions of an entry.
    #[must_use]
    pub fn entry_arity(&self, handle: EntryHandle) -> usize {
        self.entries[handle.index()].dims.len()
    }

    /// Name of one dimension of an entry, for diagnostics.
    #[must_use]
    pub fn dim_name(&self, handle: DimHandle) -> &str {
        &self.entries[handle.entry as usize].dim_names[handle.dim as usize]
    }

    /// Iterates every entry handle in slot order.
    pub fn entry_handles(&self) -> impl Iterator<Item = EntryHandle> + '_ {
        (0..self.entries.len()).map(|i| EntryHandle(i as u16))
    }

    /// Iterates the dimension handles of an entry in declaration order.
    pub fn dim_handles(&self, handle: EntryHandle) -> impl Iterator<Item = DimHandle> + '_ {
        let entry = handle.0;
        (0..self.entries[handle.index()].dims.len()).map(move |dim| DimHandle {
            entry,
            dim: dim as u16,
        })
    }

    /// Iterates the source table in slot order, for diagnostics.
    pub fn source_table(&self) -> impl Iterator<Item = (usize, &SourceDesc)> {
        self.sources.iter().enumerate()
    }

    /// Looks up the slot a raw value would be stored in, or `None` when
    /// the configuration never reads it.
    #[inline]
    pub(crate) fn source_slot(&self, kind: SourceKind, name: &str) -> Option<SourceSlot> {
        let table = &self.source_index[kind.index()];
        if kind.is_case_insensitive() && name.bytes().any(|b| b.is_ascii_uppercase()) {
            // Lowercase on the stack: header names are short and ASCII,
            // so the common path must not allocate.
            let mut lowered: smallvec::SmallVec<[u8; 64]> =
                smallvec::SmallVec::from_slice(name.as_bytes());
            lowered.make_ascii_lowercase();
            let lowered = std::str::from_utf8(&lowered).ok()?;
            table.get(lowered)
        } else {
            table.get(name)
        }
    }
}
