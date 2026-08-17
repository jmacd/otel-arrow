// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The per-message side: filling source slots and reading entries back.
//!
//! [`ContextBuilder`] is used once per arriving message. It performs one
//! hash lookup per *arriving* header or claim -- not per configured
//! entry -- and stores the value into a dense slot array. Values the
//! configuration never reads are rejected by the lookup and cost one
//! failed hash probe and nothing else.
//!
//! [`ContextRecord`] is the result: a single contiguous byte array
//! whose header geometry was fixed at compile time. Reading an entry is
//! a bit test plus a load at a constant offset. Cloning a record clones
//! two `Arc`s, so a record rides along with pdata through fan-out
//! without copying its bytes.

use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use smallvec::SmallVec;

use super::config::{SourceKind, ValueKind};
use super::schema::{ContextSchema, DimHandle, EntryHandle, SourceSlot, ValueRange};

/// Number of bytes in a generated UUIDv7 value.
const UUID_LEN: usize = 16;

/// Source slots held inline before spilling to the heap.
const INLINE_SLOTS: usize = 16;

/// Staging bytes held inline before spilling to the heap.
const INLINE_ARENA: usize = 256;

/// Fills source slots for one message, then seals them into a
/// [`ContextRecord`].
///
/// A builder is meant to be created once per receiver and reused:
/// [`ContextBuilder::build`] resets it, so the steady state performs no
/// allocation beyond the record itself.
pub struct ContextBuilder {
    schema: Arc<ContextSchema>,
    /// Slot -> range into `arena`. Length equals the source table.
    slots: SmallVec<[ValueRange; INLINE_SLOTS]>,
    /// Staging bytes. Seeded with the schema constant pool so that
    /// compile-time constants and message-time values share one address
    /// space and the evaluation pass has no special cases.
    arena: SmallVec<[u8; INLINE_ARENA]>,
    /// Reusable output buffer. The record is one `Arc` allocation
    /// copied from here, so a message costs exactly one allocation.
    scratch: Vec<u8>,
    /// Last message's staged inputs and the record they produced.
    ///
    /// Successive messages on one connection almost always carry the
    /// same headers and the same identity, so comparing a few dozen
    /// staged bytes usually replaces the whole evaluate-encode-allocate
    /// sequence with an `Arc` clone. Disabled when the schema contains a
    /// per-message generator, whose output must never repeat.
    memo: Option<Memo>,
}

/// The previous message's inputs, and the record they produced.
struct Memo {
    slots: SmallVec<[ValueRange; INLINE_SLOTS]>,
    arena: SmallVec<[u8; INLINE_ARENA]>,
    bytes: Arc<[u8]>,
}

impl ContextBuilder {
    /// Creates a builder for the given schema.
    #[must_use]
    pub fn new(schema: Arc<ContextSchema>) -> Self {
        let mut arena = SmallVec::new();
        arena.extend_from_slice(&schema.consts);
        let memoizable = schema.random_slots.is_empty();
        Self {
            slots: SmallVec::from_slice(&schema.initial_slots),
            arena,
            scratch: Vec::with_capacity(schema.layout().header_len() + 256),
            memo: memoizable.then(|| Memo {
                slots: SmallVec::new(),
                arena: SmallVec::new(),
                bytes: Arc::clone(&schema.empty_bytes),
            }),
            schema,
        }
    }

    /// The schema this builder was created from.
    #[must_use]
    pub fn schema(&self) -> &Arc<ContextSchema> {
        &self.schema
    }

    /// Whether this builder memoizes the encoded record between
    /// identical messages. False when the schema contains a per-message
    /// generator.
    #[must_use]
    pub fn is_memoizing(&self) -> bool {
        self.memo.is_some()
    }

    /// Discards anything set so far, returning to the post-`new` state.
    #[inline]
    pub fn reset(&mut self) {
        self.arena.truncate(self.schema.consts.len());
        self.slots.copy_from_slice(&self.schema.initial_slots);
    }

    /// Offers a raw value to the context.
    ///
    /// Returns `true` when the configuration reads this source and the
    /// value was stored, `false` when no entry mentions it. Callers do
    /// not need to filter beforehand; this *is* the filter.
    ///
    /// `value_kind` describes the bytes and becomes part of the entry
    /// key, so a caller must pass the kind it actually observed rather
    /// than a convenient default.
    #[inline]
    pub fn set(
        &mut self,
        source: SourceKind,
        name: &str,
        value: &[u8],
        value_kind: ValueKind,
    ) -> bool {
        match self.schema.source_slot(source, name) {
            Some(slot) => {
                self.store(slot, value, value_kind);
                true
            }
            None => false,
        }
    }

    /// Offers a captured transport header. Names are matched
    /// case-insensitively. Pass the `value_kind` recorded at capture:
    /// gRPC `-bin` metadata is [`ValueKind::Binary`].
    #[inline]
    pub fn set_header(&mut self, name: &str, value: &[u8], value_kind: ValueKind) -> bool {
        self.set(SourceKind::TransportHeader, name, value, value_kind)
    }

    /// Offers a text transport header.
    #[inline]
    pub fn set_header_text(&mut self, name: &str, value: &str) -> bool {
        self.set_header(name, value.as_bytes(), ValueKind::Text)
    }

    /// Offers a single-valued authorization claim.
    #[inline]
    pub fn set_claim(&mut self, name: &str, value: &str) -> bool {
        self.set(
            SourceKind::AuthorizedIdentity,
            name,
            value.as_bytes(),
            ValueKind::Text,
        )
    }

    /// Offers a multi-valued authorization claim.
    ///
    /// Values are length-prefixed rather than concatenated, and the
    /// stored kind is [`ValueKind::TextList`], so `["a", "b"]` can never
    /// compare equal to the single value `"ab"`.
    pub fn set_claim_values<'a>(
        &mut self,
        name: &str,
        values: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        let Some(slot) = self
            .schema
            .source_slot(SourceKind::AuthorizedIdentity, name)
        else {
            return false;
        };
        let off = self.arena.len() as u32;
        for value in values {
            self.arena
                .extend_from_slice(&(value.len() as u32).to_ne_bytes());
            self.arena.extend_from_slice(value.as_bytes());
        }
        self.slots[slot.0 as usize] = ValueRange {
            off,
            len: self.arena.len() as u32 - off,
            kind: ValueKind::TextList,
        };
        true
    }

    /// Offers a network attribute, for example `peer_socket_addr`.
    #[inline]
    pub fn set_network(&mut self, name: &str, value: &str) -> bool {
        self.set(
            SourceKind::NetworkInfo,
            name,
            value.as_bytes(),
            ValueKind::Text,
        )
    }

    #[inline]
    fn store(&mut self, slot: SourceSlot, value: &[u8], kind: ValueKind) {
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(value);
        self.slots[slot.0 as usize] = ValueRange {
            off,
            len: value.len() as u32,
            kind,
        };
    }

    /// Evaluates every entry, seals the result, and resets the builder
    /// for the next message.
    ///
    /// The evaluation pass is a single walk of the entry table. For each
    /// entry it checks the conditions, checks that every dimension is
    /// present, then appends the dimension bytes contiguously into the
    /// data region. Entry keys are therefore one slice, and the
    /// canonical hash of that key is computed here once rather than by
    /// every node that partitions on it.
    #[must_use]
    pub fn build(&mut self) -> ContextRecord {
        self.fill_randomness();

        // Split the borrow so the output buffer can be reused across
        // messages while the arena is still being read.
        let Self {
            schema,
            slots,
            arena,
            scratch,
            memo,
        } = self;
        let layout = schema.layout();

        if let Some(memo) = memo.as_ref()
            && memo.slots.as_slice() == slots.as_slice()
            && memo.arena.as_slice() == arena.as_slice()
        {
            let record = ContextRecord {
                schema: Arc::clone(schema),
                bytes: Arc::clone(&memo.bytes),
            };
            self.reset();
            return record;
        }

        scratch.clear();
        scratch.resize(layout.header_len(), 0);

        for (index, def) in schema.entries.iter().enumerate() {
            let conditions_hold = def.conditions.iter().all(|cond| {
                let observed = slots[cond.source.0 as usize];
                observed.is_present()
                    && observed.kind == cond.value.kind
                    && slice(arena, observed) == slice(arena, cond.value)
            });
            if !conditions_hold {
                continue;
            }
            if !def
                .dims
                .iter()
                .all(|slot| slots[slot.0 as usize].is_present())
            {
                continue;
            }

            let mut hasher = schema.hash_seed.build_hasher();
            // The entry index participates in the hash so that two
            // entries carrying identical bytes do not collide when they
            // share a routing table.
            hasher.write_u16(index as u16);

            for (dim, slot) in def.dims.iter().enumerate() {
                let range = slots[slot.0 as usize];
                let bytes = slice(arena, range);
                let data_off = (scratch.len() - layout.data_off) as u32;

                let at = layout.dim_off + (def.dim_base as usize + dim) * 8;
                scratch[at..at + 4].copy_from_slice(&data_off.to_ne_bytes());
                scratch[at + 4..at + 8].copy_from_slice(&range.len.to_ne_bytes());

                scratch[layout.kind_off + def.dim_base as usize + dim] = range.kind.as_u8();

                // Length-prefix and kind both participate: ("ab", "c")
                // must not hash like ("a", "bc"), and a binary value
                // must not hash like the text with the same bytes.
                hasher.write_u8(range.kind.as_u8());
                hasher.write_u32(range.len);
                hasher.write(bytes);

                scratch.extend_from_slice(bytes);
            }

            let word = index / 64;
            let mut bits = u64::from_ne_bytes(
                scratch[word * 8..word * 8 + 8]
                    .try_into()
                    .expect("presence word is eight bytes"),
            );
            bits |= 1u64 << (index % 64);
            scratch[word * 8..word * 8 + 8].copy_from_slice(&bits.to_ne_bytes());

            let hash_at = layout.hash_off + index * 8;
            scratch[hash_at..hash_at + 8].copy_from_slice(&hasher.finish().to_ne_bytes());
        }

        let bytes = if scratch.is_empty() {
            Arc::clone(&schema.empty_bytes)
        } else {
            Arc::from(&scratch[..])
        };
        let schema = Arc::clone(schema);
        if let Some(memo) = memo.as_mut() {
            memo.slots.clear();
            memo.slots.extend_from_slice(slots);
            memo.arena.clear();
            memo.arena.extend_from_slice(arena);
            memo.bytes = Arc::clone(&bytes);
        }
        self.reset();
        ContextRecord { schema, bytes }
    }

    /// Generates values for any `randomness` source still unset.
    fn fill_randomness(&mut self) {
        for index in 0..self.schema.random_slots.len() {
            let slot = self.schema.random_slots[index];
            if self.slots[slot.0 as usize].is_present() {
                continue;
            }
            let value = uuid7();
            self.store(slot, &value, ValueKind::Binary);
        }
    }
}

/// The sealed, per-message context.
///
/// The byte buffer holds, in order: the presence bitmap, the entry hash
/// array, the dimension index, and the concatenated key data. Every
/// offset except the data region is a compile-time constant taken from
/// [`RecordLayout`](super::RecordLayout).
///
/// Integers in the buffer use native byte order. The buffer is a
/// process-local representation, not a wire format; the stable value a
/// node should propagate or compare across processes is the entry key
/// returned by [`ContextRecord::key`], or its [`ContextRecord::hash`],
/// which is computed with fixed seeds.
#[derive(Debug, Clone)]
pub struct ContextRecord {
    schema: Arc<ContextSchema>,
    bytes: Arc<[u8]>,
}

impl ContextRecord {
    /// A record in which no entry is present.
    #[must_use]
    pub fn empty(schema: Arc<ContextSchema>) -> Self {
        let bytes = Arc::clone(&schema.empty_bytes);
        Self { schema, bytes }
    }

    /// The schema this record was built against.
    #[must_use]
    pub fn schema(&self) -> &Arc<ContextSchema> {
        &self.schema
    }

    /// Total size of the encoded record in bytes.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the entry is present. One bit test.
    #[must_use]
    #[inline]
    pub fn is_present(&self, handle: EntryHandle) -> bool {
        let index = handle.index();
        if index >= self.schema.layout.n_entries || self.bytes.is_empty() {
            return false;
        }
        let word = index / 64;
        let bits = u64::from_ne_bytes(
            self.bytes[word * 8..word * 8 + 8]
                .try_into()
                .expect("presence word is eight bytes"),
        );
        bits & (1u64 << (index % 64)) != 0
    }

    /// The precomputed canonical hash of the entry key.
    ///
    /// This is the value to use as a partition key, a routing table
    /// bucket, or a batching bucket. It is stable for identical keys
    /// across processes and across runs.
    ///
    /// It is a *bucket*, not an identity. Entry values derive from
    /// headers and claims, so their cardinality is unbounded and their
    /// content is at least partly attacker-influenced. A table keyed by
    /// this hash must compare [`ContextRecord::key`] on a hit, the way
    /// the telemetry `EntityRegistry` hashes an attribute set to find a
    /// bucket and then compares the set structurally.
    #[must_use]
    #[inline]
    pub fn hash(&self, handle: EntryHandle) -> Option<u64> {
        if !self.is_present(handle) {
            return None;
        }
        let at = self.schema.layout.hash_off + handle.index() * 8;
        Some(u64::from_ne_bytes(
            self.bytes[at..at + 8]
                .try_into()
                .expect("hash word is eight bytes"),
        ))
    }

    /// The entry key: all dimensions concatenated, as one slice.
    #[must_use]
    #[inline]
    pub fn key(&self, handle: EntryHandle) -> Option<&[u8]> {
        if !self.is_present(handle) {
            return None;
        }
        let def = &self.schema.entries[handle.index()];
        let first = self.dim_range(def.dim_base as usize)?;
        let last = self.dim_range(def.dim_base as usize + def.dims.len() - 1)?;
        let start = first.0;
        let end = last.0 + last.1;
        let base = self.schema.layout.data_off;
        Some(&self.bytes[base + start..base + end])
    }

    /// One dimension of an entry, as bytes.
    ///
    /// Prefer [`ContextRecord::dim_typed`] when comparing values:
    /// bytes alone are not an identity.
    #[must_use]
    #[inline]
    pub fn dim(&self, handle: DimHandle) -> Option<&[u8]> {
        self.dim_typed(handle).map(|(_, bytes)| bytes)
    }

    /// One dimension of an entry, with the kind of its value.
    ///
    /// Two dimension values are the same only when both the kind and
    /// the bytes agree.
    #[must_use]
    #[inline]
    pub fn dim_typed(&self, handle: DimHandle) -> Option<(ValueKind, &[u8])> {
        if !self.is_present(handle.entry()) {
            return None;
        }
        let def = &self.schema.entries[handle.entry as usize];
        let dim = def.dim_base as usize + handle.dim as usize;
        let (off, len) = self.dim_range(dim)?;
        let kind = ValueKind::from_u8(*self.bytes.get(self.schema.layout.kind_off + dim)?);
        let base = self.schema.layout.data_off;
        Some((kind, &self.bytes[base + off..base + off + len]))
    }

    /// Decodes a [`ValueKind::TextList`] dimension into its elements.
    ///
    /// Returns `None` for a dimension of any other kind, so a caller
    /// cannot silently treat a flattened list as a single value.
    #[must_use]
    pub fn dim_list(&self, handle: DimHandle) -> Option<Vec<&str>> {
        let (kind, mut bytes) = self.dim_typed(handle)?;
        if kind != ValueKind::TextList {
            return None;
        }
        let mut out = Vec::new();
        while bytes.len() >= 4 {
            let len = u32::from_ne_bytes(bytes[..4].try_into().ok()?) as usize;
            let rest = bytes.get(4..4 + len)?;
            out.push(std::str::from_utf8(rest).ok()?);
            bytes = &bytes[4 + len..];
        }
        Some(out)
    }

    /// Whether two records carry the same value for an entry.
    ///
    /// Compares the hash first, then the kinds and bytes, so a hash
    /// collision cannot merge two tenants.
    #[must_use]
    pub fn entry_eq(&self, handle: EntryHandle, other: &Self, other_handle: EntryHandle) -> bool {
        match (self.hash(handle), other.hash(other_handle)) {
            (Some(lhs), Some(rhs)) if lhs == rhs => {}
            _ => return false,
        }
        let lhs = &self.schema.entries[handle.index()];
        let rhs = &other.schema.entries[other_handle.index()];
        if lhs.dims.len() != rhs.dims.len() {
            return false;
        }
        self.schema
            .dim_handles(handle)
            .zip(other.schema.dim_handles(other_handle))
            .all(|(lhs, rhs)| self.dim_typed(lhs) == other.dim_typed(rhs))
    }

    /// Iterates the present entries as `(name, key)` pairs, for
    /// diagnostics and logging. The key is the raw concatenation; use
    /// [`ContextRecord::dim_typed`] when the kind matters.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.schema.entry_handles().filter_map(move |handle| {
            self.key(handle)
                .map(|key| (self.schema.entry_name(handle), key))
        })
    }

    #[inline]
    fn dim_range(&self, dim: usize) -> Option<(usize, usize)> {
        let at = self.schema.layout.dim_off + dim * 8;
        if at + 8 > self.bytes.len() {
            return None;
        }
        let off = u32::from_ne_bytes(
            self.bytes[at..at + 4]
                .try_into()
                .expect("offset is four bytes"),
        );
        let len = u32::from_ne_bytes(
            self.bytes[at + 4..at + 8]
                .try_into()
                .expect("length is four bytes"),
        );
        Some((off as usize, len as usize))
    }
}

/// Reads a range out of the staging arena.
#[inline]
fn slice(arena: &[u8], range: ValueRange) -> &[u8] {
    let start = range.off as usize;
    &arena[start..start + range.len as usize]
}

/// Generates a UUIDv7 value: a 48-bit millisecond timestamp followed by
/// randomness, with the version and variant bits set.
fn uuid7() -> [u8; UUID_LEN] {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut out = [0u8; UUID_LEN];
    out[0..6].copy_from_slice(&millis.to_be_bytes()[2..8]);
    out[6..14].copy_from_slice(&rand::random::<u64>().to_be_bytes());
    out[14..16].copy_from_slice(&rand::random::<u16>().to_be_bytes());
    out[6] = (out[6] & 0x0f) | 0x70;
    out[8] = (out[8] & 0x3f) | 0x80;
    out
}
