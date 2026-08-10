// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The byte layout of a packed context.
//!
//! # What a context carries, and why
//!
//! A context carries the *inputs* to matching, not the answers. That is Envoy's
//! arrangement: the filter builds descriptors and hands them to the limiter,
//! which does the lookup. Here a receiver builds the equivalent -- which tokens
//! resolved, and what symbol each value-matched field took -- and each consumer
//! does its own lookup when it is reached.
//!
//! Three properties follow, and they are the reason for the arrangement.
//!
//! *Only the path taken is paid for.* A consumer evaluates its own condition
//! set, not every set in the pipeline. Static reachability cannot achieve that,
//! because which descriptor tokens resolve is decided at request time.
//!
//! *Size tracks the data model, not the pipeline.* The symbol field has one
//! entry per value-matched extractor, which is a property of what is extracted.
//! Precomputed answers would instead need one field per condition set, growing
//! with every node added.
//!
//! *Condition answers do not disturb contexts already in flight.* The layout
//! below contains no condition answers. A component retains the compiler epoch
//! that built in-flight contexts and reads each context through that epoch. A
//! new compiler epoch may freely change its condition sets without adding
//! per-request result fields. Adding a new *literal* can widen its key's symbol
//! field, which changes the layout fingerprint and requires the new epoch.
//!
//! # The layout
//!
//! ```text
//!   offset  region                bytes
//!   ------  --------------------  ----------------------------------------
//!   0       epoch                 4
//!           resolved tokens       ceil(live tokens / 8)
//!           symbol field          ceil(sum of symbol widths / 8)
//!           matched names         one per name-preserving value slot
//!           region index          2 x (regions + 1)
//!   ------  --------------------  ----------------------------------------
//!           data                  retained values, then attribute bags
//! ```
//!
//! Every offset is a compile-time constant of the epoch, so reaching any region
//! is an addition, and reaching a field within one is a shift and a mask.
//!
//! **epoch.** Which compiled state built this. Checked once, when a context and
//! compiled state are paired into a
//! [`MetadataView`](crate::MetadataView); every read after that is infallible.
//!
//! **resolved tokens.** One bit per token, set when every one of its extractors
//! produced a value. Because a token is all-or-nothing, this bitmap also answers
//! "is this key present", which is why no key spends a presence bit of its own.
//! It is also what a node's admission check reads: a required token that is
//! clear means Nack, before any condition is tested.
//!
//! **symbol field.** One bit-field per value-matched extractor, as wide as that
//! key's dictionary needs. A key with four declared literals needs three bits:
//! four literals plus `ABSENT` and `UNKNOWN`. A field left at zero reads as
//! `ABSENT`, which matches nothing, so a producer that never ran an extractor
//! and a request that never offered the value are indistinguishable and both
//! safe. Keys that are only ever read, never tested, have no dictionary and
//! appear here not at all.
//!
//! **matched names.** One byte per value slot whose key preserves which wire
//! name the sender used. The candidate names are compile-time constants of the
//! extractor, so an ordinal suffices and the name itself never travels.
//!
//! **region index.** Where each variable-length region begins, relative to the
//! start of the data area. Regions are the value slots in slot order, then the
//! attribute bags. There is one more offset than there are regions, so region
//! `i` is `data[index[i]..index[i + 1]]` and an absent value is two equal
//! offsets costing no bytes. Offsets are `u16`, which is what bounds a context
//! to 64 KiB.
//!
//! **data.** Retained values, then bags. A repeated key's values are each
//! preceded by a `u16` length so the reader can walk them. A bag is a valid
//! fragment of the consumer's own repeated `KeyValue` field, so it is copied
//! out rather than encoded.
//!
//! # A worked context
//!
//! Tokens `edge{tenant_id, project_id}` and `auth{user_id, role}`; conditions
//! testing `tenant_id` against two literals and `role` against one; an exporter
//! reading `tenant_id` and `project_id` and bagging both. For a request carrying
//! `x-tenant-id: acme`, `x-project-id: p1`, `sub=u9`, `role=admin`:
//!
//! ```text
//!   0..4    epoch                 07 00 00 00
//!   4..12   layout fingerprint    ...
//!   12..13  resolved tokens       03            edge and auth resolved
//!   13..14  symbol field          0a            tenant=2 (bits 0..2), role=2 (bits 2..4)
//!   14..16  matched names         00 00         both matched their first name
//!   16..24  region index          0000 0004 0006 002f
//!   24..    data                  "acme" "p1" <41 bytes of encoded KeyValue>
//! ```
//!
//! The consumer then assembles the PairSlot word for
//! `(edge, {tenant_id})` -- three bits out of the symbol field -- and indexes
//! its branch table with it. The table returns one selected entry; the consumer
//! yields that entry together with `edge` as one `ConditionMatch`.

/// Bytes the epoch occupies at the head of every context.
pub(crate) const EPOCH_BYTES: usize = 4;
/// Bytes of layout fingerprint after the epoch.
pub(crate) const LAYOUT_FINGERPRINT_BYTES: usize = 8;
/// Fixed context identity header: epoch and layout fingerprint.
pub(crate) const CONTEXT_ID_BYTES: usize = EPOCH_BYTES + LAYOUT_FINGERPRINT_BYTES;

/// Where each region of a packed context begins.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ContextLayout {
    /// Identifies the byte layout independently of its epoch number.
    ///
    /// Epochs are operational generation counters and can be reused after a
    /// restart. The fingerprint keeps such reuse from treating old bytes as a
    /// new layout.
    pub(crate) fingerprint: u64,
    /// Bytes of resolved-token bitmap.
    pub(crate) token_bitmap_bytes: usize,
    /// Byte offset of the symbol field.
    pub(crate) symbol_field_offset: usize,
    /// Bytes of symbol field.
    pub(crate) symbol_field_bytes: usize,
    /// Byte offset of the matched-name ordinals.
    pub(crate) name_ordinals_offset: usize,
    /// Byte offset of the region index.
    pub(crate) region_index_offset: usize,
    /// How many regions the index addresses: value slots, then bags.
    pub(crate) regions: usize,
    /// Byte offset of the data region, which is also the header's size.
    pub(crate) data_offset: usize,
}

impl ContextLayout {
    /// Computes the layout for an epoch.
    pub(crate) fn new(
        tokens: usize,
        symbol_bits: u32,
        name_ordinals: usize,
        value_slots: usize,
        bags: usize,
    ) -> Self {
        let token_bitmap_bytes = tokens.div_ceil(8);
        let symbol_field_offset = CONTEXT_ID_BYTES + token_bitmap_bytes;
        let symbol_field_bytes = (symbol_bits as usize).div_ceil(8);
        let name_ordinals_offset = symbol_field_offset + symbol_field_bytes;
        let region_index_offset = name_ordinals_offset + name_ordinals;
        let regions = value_slots + bags;
        let region_index_bytes = if regions == 0 {
            0
        } else {
            (regions + 1) * size_of::<u16>()
        };

        let mut layout = Self {
            fingerprint: 0,
            token_bitmap_bytes,
            symbol_field_offset,
            symbol_field_bytes,
            name_ordinals_offset,
            region_index_offset,
            regions,
            data_offset: region_index_offset + region_index_bytes,
        };
        layout.fingerprint = layout.fingerprint();
        layout
    }

    fn fingerprint(&self) -> u64 {
        const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET_BASIS;
        for value in [
            self.token_bitmap_bytes,
            self.symbol_field_offset,
            self.symbol_field_bytes,
            self.name_ordinals_offset,
            self.region_index_offset,
            self.regions,
            self.data_offset,
        ] {
            for byte in (value as u64).to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        hash
    }
}

/// Reads `bits` bits at `bit_offset` from a bitfield.
///
/// A field never straddles more than eight bytes, because
/// [`Limits::pair_slot_bits`](crate::Limits::pair_slot_bits) caps one key's
/// symbol at the width of a word.
pub(crate) fn read_bits(field: &[u8], bit_offset: u32, bits: u32) -> u64 {
    if bits == 0 {
        return 0;
    }
    let mut value = 0u64;
    let mut taken = 0;
    let mut byte = (bit_offset / 8) as usize;
    let mut shift = bit_offset % 8;
    while taken < bits {
        value |= u64::from(field[byte] >> shift) << taken;
        taken += 8 - shift;
        byte += 1;
        shift = 0;
    }
    value & (u64::MAX >> (64 - bits))
}

/// Writes the low `bits` bits of `value` at `bit_offset` in a zeroed bitfield.
pub(crate) fn write_bits(field: &mut [u8], bit_offset: u32, bits: u32, value: u64) {
    if bits == 0 {
        return;
    }
    let mut remaining = value & (u64::MAX >> (64 - bits));
    let mut written = 0;
    let mut byte = (bit_offset / 8) as usize;
    let mut shift = bit_offset % 8;
    while written < bits {
        field[byte] |= ((remaining << shift) & 0xff) as u8;
        let advanced = 8 - shift;
        remaining >>= advanced;
        written += advanced;
        byte += 1;
        shift = 0;
    }
}
