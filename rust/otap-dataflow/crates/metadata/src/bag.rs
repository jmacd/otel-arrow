// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pre-encoded OTLP attribute regions.
//!
//! A bag is the answer to "give me these metadata fields as OpenTelemetry
//! attributes". Rather than hand a consumer a list of key-value pairs to encode,
//! the context stores the bytes already encoded, already tagged with the field
//! number of the repeated `KeyValue` field they are destined for. A consumer
//! copies the region into its own message and is done: no re-encoding, no
//! per-attribute allocation, and the key names never travel as strings anywhere
//! else in the context.
//!
//! Everything that does not depend on the request is computed once, at compile
//! time: the field tag, and each member's encoded `KeyValue.key`. Only the
//! lengths, which depend on the value, are computed per request.
//!
//! ```text
//! <field tag> <len> KeyValue { key: <name>, value: AnyValue { string|bytes } }
//! ```

use crate::source::ValueKind;

/// Protobuf wire type 2, length-delimited, the only one used here.
const WIRE_TYPE_LEN: u32 = 2;
/// `KeyValue.key`, field 1.
const KEY_VALUE_KEY_TAG: u8 = (1 << 3) | WIRE_TYPE_LEN as u8;
/// `KeyValue.value`, field 2.
const KEY_VALUE_VALUE_TAG: u8 = (2 << 3) | WIRE_TYPE_LEN as u8;
/// `AnyValue.string_value`, field 1.
const ANY_VALUE_STRING_TAG: u8 = (1 << 3) | WIRE_TYPE_LEN as u8;
/// `AnyValue.bytes_value`, field 7.
const ANY_VALUE_BYTES_TAG: u8 = (7 << 3) | WIRE_TYPE_LEN as u8;

/// Writes into a slice that was sized in advance.
///
/// The packed context is measured before it is written, so every length is
/// already known and the bytes go straight into their final home. This is what
/// keeps a context to one allocation rather than one plus a staging buffer.
#[derive(Debug)]
pub(crate) struct ByteCursor<'a> {
    buffer: &'a mut [u8],
    at: usize,
}

impl<'a> ByteCursor<'a> {
    pub(crate) fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, at: 0 }
    }

    pub(crate) fn push(&mut self, byte: u8) {
        self.buffer[self.at] = byte;
        self.at += 1;
    }

    pub(crate) fn extend(&mut self, bytes: &[u8]) {
        self.buffer[self.at..self.at + bytes.len()].copy_from_slice(bytes);
        self.at += bytes.len();
    }

    pub(crate) fn position(&self) -> usize {
        self.at
    }
}

/// Writes a base-128 varint.
pub(crate) fn write_varint(out: &mut ByteCursor<'_>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Returns how many bytes a base-128 varint occupies.
pub(crate) const fn varint_len(value: u64) -> usize {
    let bits = u64::BITS - value.leading_zeros();
    if bits == 0 {
        1
    } else {
        bits.div_ceil(7) as usize
    }
}

/// Builds the bytes of a bag member that do not depend on the request: the
/// encoded `KeyValue.key` field.
pub(crate) fn encode_key_field(name: &str) -> Vec<u8> {
    let mut out = vec![0u8; 1 + varint_len(name.len() as u64) + name.len()];
    let mut cursor = ByteCursor::new(&mut out);
    cursor.push(KEY_VALUE_KEY_TAG);
    write_varint(&mut cursor, name.len() as u64);
    cursor.extend(name.as_bytes());
    out
}

/// Builds the field tag that introduces every member of a bag.
pub(crate) fn encode_field_tag(field_number: u32) -> Vec<u8> {
    let tag = u64::from((field_number << 3) | WIRE_TYPE_LEN);
    let mut out = vec![0u8; varint_len(tag)];
    write_varint(&mut ByteCursor::new(&mut out), tag);
    out
}

/// Returns how many bytes one member occupies for a value of `value_len` bytes.
pub(crate) fn member_len(tag: &[u8], key_field: &[u8], value_len: usize) -> usize {
    let any_value = 1 + varint_len(value_len as u64) + value_len;
    let value_field = 1 + varint_len(any_value as u64) + any_value;
    let key_value = key_field.len() + value_field;
    tag.len() + varint_len(key_value as u64) + key_value
}

/// Appends one fully encoded member.
pub(crate) fn write_member(
    out: &mut ByteCursor<'_>,
    tag: &[u8],
    key_field: &[u8],
    value_kind: ValueKind,
    value: &[u8],
) {
    let any_value = 1 + varint_len(value.len() as u64) + value.len();
    let value_field = 1 + varint_len(any_value as u64) + any_value;

    out.extend(tag);
    write_varint(out, (key_field.len() + value_field) as u64);
    out.extend(key_field);
    out.push(KEY_VALUE_VALUE_TAG);
    write_varint(out, any_value as u64);
    out.push(match value_kind {
        ValueKind::Text => ANY_VALUE_STRING_TAG,
        ValueKind::Binary => ANY_VALUE_BYTES_TAG,
    });
    write_varint(out, value.len() as u64);
    out.extend(value);
}
