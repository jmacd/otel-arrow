// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct OTLP bytes encoder for tokio-tracing events.

use super::structured::{ArrayWriter, MAX_DEPTH, MapWriter, StructCarrier, ValueWriter};
use super::{LogRecord, SavedCallsite};
use crate::attributes::AttributeValue;
use crate::event::LogEvent;
use crate::registry::{EntityKey, TelemetryRegistryHandle};
use bytes::Bytes;
use otel_arrow_dfe_config::pipeline::telemetry::{
    AttributeValue as ConfigAttributeValue, AttributeValueArray as ConfigAttributeValueArray,
};
use otel_arrow_dfe_pdata::otlp::common::{BoundedBuf, EncodeFailure, EncodeResult, ProtoBuffer};
use otel_arrow_dfe_pdata::proto::consts::{
    field_num::common::*, field_num::logs::*, field_num::resource::*, wire_types,
};
use slotmap::Key;
use std::collections::HashMap;
use std::time::SystemTime;
use tracing::Level;

/// Direct encoder that writes a single LogRecord from a tracing Event.
pub struct DirectLogRecordEncoder<'buf, B: BoundedBuf> {
    buf: &'buf mut B,
}

impl<'buf, B: BoundedBuf> DirectLogRecordEncoder<'buf, B> {
    /// Create a new encoder that writes to the provided buffer.
    #[inline]
    pub const fn new(buf: &'buf mut B) -> Self {
        Self { buf }
    }

    /// Reset the underlying buffer.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// Encode a tracing Event as a complete LogRecord message.
    ///
    /// Returns the number of bytes written.
    pub fn encode_log_record(&mut self, time: SystemTime, record: &LogRecord) -> usize {
        let start_len = self.buf.len();

        // Convert SystemTime to nanoseconds since UNIX epoch
        let timestamp_ns = time
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        // Encode time_unix_nano (field 1, fixed64)
        let _ = self
            .buf
            .encode_field_tag(LOG_RECORD_TIME_UNIX_NANO, wire_types::FIXED64);
        let _ = self.buf.extend_from_slice(&timestamp_ns.to_le_bytes());

        // Encode severity_number (field 2, varint)
        let severity = level_to_severity_number(record.callsite().level());
        let _ = self
            .buf
            .encode_field_tag(LOG_RECORD_SEVERITY_NUMBER, wire_types::VARINT);
        let _ = self.buf.encode_varint(severity as u64);

        // Note we skip encoding severity_text (field 3, string)

        // Encode event_name (field 12, string) - format: "target::name (file:line)"
        let _ = encode_event_name(self.buf, record.callsite());

        // Pre-encoded body and attributes.
        let _ = self.buf.extend_from_slice(&record.body_attrs_bytes);

        // Encode dropped_attributes_count (field 7, varint) if any were dropped.
        if record.dropped_attributes_count > 0 {
            let _ = self
                .buf
                .encode_field_tag(LOG_RECORD_DROPPED_ATTRIBUTES_COUNT, wire_types::VARINT);
            let _ = self
                .buf
                .encode_varint(record.dropped_attributes_count as u64);
        }

        self.buf.len() - start_len
    }
}

/// Encode the event name from callsite metadata.
///
/// Emits the bare `callsite.name()` (the first argument to `otel_info!` /
/// `otel_warn!` / ...). The tracing target (`callsite.target()`) is **not**
/// prefixed here; it identifies the static logical software unit and is
/// conveyed separately through
/// `InstrumentationScope.name` by [`encode_export_logs_request`] so that
/// `event.name` on the wire matches the value declared in the
/// `rust/otap-dataflow/semconv/` registry and validated by
/// `weaver registry live-check`.
fn encode_event_name<B: BoundedBuf>(buf: &mut B, callsite: SavedCallsite) -> EncodeResult {
    buf.encode_len_delimited(LOG_RECORD_EVENT_NAME, |buf| {
        buf.try_extend(callsite.name().as_bytes())
    })
}

/// Visitor that directly encodes tracing fields to protobuf.
pub struct DirectFieldVisitor<'buf, B: BoundedBuf> {
    buf: &'buf mut B,
    dropped_count: u32,
}

/// A value location is a Body or Attribute field.
enum ValueLocation<'a> {
    /// Log body, an AnyValue. By convention, the "message" property
    /// translates into Body.
    Body,
    /// Log attribute, a KeyValue where Key != "message".
    Attribute(&'a str),
}

fn value_location(field: &tracing::field::Field) -> ValueLocation<'_> {
    match field.name() {
        "message" => ValueLocation::Body,
        name => ValueLocation::Attribute(name),
    }
}

impl<'buf, B: BoundedBuf> DirectFieldVisitor<'buf, B> {
    /// Create a new DirectFieldVisitor that writes to the provided buffer.
    pub const fn new(buf: &'buf mut B) -> Self {
        Self {
            buf,
            dropped_count: 0,
        }
    }

    /// Returns the number of fields dropped due to size limits.
    #[must_use]
    pub const fn dropped_count(&self) -> u32 {
        self.dropped_count
    }

    /// Encodes one attribute within the per-attribute budget, counting a hard
    /// failure or a truncation as one dropped attribute. A hard failure rolls
    /// back any partial bytes; `Ok(true)` from `encode` reports truncation.
    fn write_attribute(&mut self, encode: impl FnOnce(&mut B) -> Result<bool, EncodeFailure>) {
        if bounded(self.buf, encode).unwrap_or(true) {
            self.dropped_count += 1;
        }
    }

    /// Encodes a scalar field according to its body or attribute location.
    fn record_value(
        &mut self,
        field: &tracing::field::Field,
        write: impl FnOnce(&mut ValueEncoder<'_, B>),
    ) {
        self.write_any_value(value_location(field), write);
    }

    /// Encodes an AnyValue as a body or attribute.
    fn write_any_value(
        &mut self,
        location: ValueLocation<'_>,
        write: impl FnOnce(&mut ValueEncoder<'_, B>),
    ) {
        match location {
            ValueLocation::Attribute(key) => {
                self.write_attribute(|buf| {
                    write_key_value(buf, LOG_RECORD_ATTRIBUTES, key, 0, write)
                });
            }
            ValueLocation::Body => {
                let _ = self
                    .buf
                    .try_encode(|buf| write_value(buf, LOG_RECORD_BODY, 0, write).map(drop));
            }
        }
    }
}

/// Adapter that lets `write!` format directly into a [`BoundedBuf`] without
/// an intermediate `String`.
///
/// When the buffer fills up, the adapter backtracks to make room for a
/// `[...]` truncation suffix and silently discards all subsequent writes.
/// The `truncated` flag records whether truncation occurred so callers can
/// report the partial value instead of dropping it entirely.
struct BoundedBufFmt<'a, B: BoundedBuf> {
    buf: &'a mut B,
    /// Buffer position where formatted content starts (after protobuf framing).
    content_start: usize,
    /// Set to `true` once truncation has occurred.
    truncated: bool,
}

impl<'a, B: BoundedBuf> BoundedBufFmt<'a, B> {
    /// Create a new adapter. `content_start` is recorded as the current
    /// buffer length so the backtracking logic knows which bytes are ours.
    #[inline]
    fn new(buf: &'a mut B) -> Self {
        let content_start = buf.len();
        Self {
            buf,
            content_start,
            truncated: false,
        }
    }

    /// Backtrack from the end of the buffer to make room for `TRUNCATION_SUFFIX`,
    /// append it, and mark ourselves as truncated.
    ///
    /// If there is not enough content to free space for the suffix the buffer
    /// is left at `content_start` (empty content) and the caller should treat
    /// this as a hard drop.
    fn truncate_with_suffix(&mut self) {
        use otel_arrow_dfe_pdata::otlp::common::TRUNCATION_SUFFIX;

        let suffix_len = TRUNCATION_SUFFIX.len();
        let cur = self.buf.len();

        // Target position: back up `suffix_len` bytes from the current end,
        // then walk backwards (at most 3 extra bytes) to a UTF-8 char boundary.
        // UTF-8 continuation bytes have the bit pattern 10xxxxxx, so we skip
        // them until we land on a leading byte.
        let target = cur.saturating_sub(suffix_len).max(self.content_start);
        let mut pos = target;
        let slice = self.buf.as_slice();
        while pos > self.content_start && (slice[pos] & 0xC0) == 0x80 {
            pos -= 1;
        }
        self.buf.truncate(pos);

        // Try to append the suffix. If even this fails (degenerate budget),
        // leave the buffer at whatever position truncate() set.
        let _ = self.buf.try_extend(TRUNCATION_SUFFIX);
        self.truncated = true;
    }
}

impl<B: BoundedBuf> std::fmt::Write for BoundedBufFmt<'_, B> {
    #[inline]
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.truncated {
            // Already truncated -- silently discard further output so the
            // formatter finishes cleanly.
            return Ok(());
        }
        match self.buf.try_extend(s.as_bytes()) {
            Ok(()) => Ok(()),
            Err(_) => {
                // The full chunk did not fit. Write as much as we can, then
                // backtrack to append the truncation suffix.
                let remaining = self.buf.remaining();
                if remaining > 0 {
                    // Find the last UTF-8 char boundary within the portion
                    // of `s` that fits.
                    let mut end = remaining;
                    while end > 0 && !s.is_char_boundary(end) {
                        end -= 1;
                    }
                    if end > 0 {
                        // This extend cannot fail -- we checked remaining.
                        let _ = self.buf.try_extend(&s.as_bytes()[..end]);
                    }
                }
                self.truncate_with_suffix();
                Ok(())
            }
        }
    }
}

/// Encode a `Debug` value as a protobuf string field with truncation support.
///
/// If the formatted output fits, returns `Ok(false)`. If it overflows the
/// buffer, the value is truncated with a `[...]` suffix and `Ok(true)` is
/// returned. `Err(Dropped)` is returned only when even a minimal truncated
/// form cannot fit (nothing useful was written).
#[inline]
fn encode_debug_string<B: BoundedBuf>(
    buf: &mut B,
    value: &dyn std::fmt::Debug,
) -> Result<bool, EncodeFailure> {
    let mut truncated = false;
    buf.encode_len_delimited_partial(ANY_VALUE_STRING_VALUE, |buf| {
        use std::fmt::Write as _;
        let mut adapter = BoundedBufFmt::new(buf);
        // Ignore the fmt result -- truncation is handled internally by the
        // adapter; the write!() call always returns Ok because the adapter
        // silently discards overflow bytes after appending the suffix.
        let _ = write!(adapter, "{:?}", value);
        // Extract state before dropping the adapter (which borrows buf).
        let was_truncated = adapter.truncated;
        let content_start = adapter.content_start;
        truncated = was_truncated;
        if was_truncated && buf.len() <= content_start {
            // Nothing useful was written (not even the suffix fit).
            return Err(EncodeFailure::Dropped);
        }
        Ok(())
    })?;
    Ok(truncated)
}

/// Compute the per-attribute budget: at most half of remaining space, but
/// at least 1 byte (to ensure a chance of a tag byte being written and
/// triggering an atomic drop). Intentionally halves for every attribute,
/// including the last -- this keeps the policy single-pass without requiring
/// pre-counting the field set, while still preventing one large value from
/// consuming the entire remaining buffer.
#[inline]
fn attr_budget<B: BoundedBuf>(buf: &B) -> usize {
    buf.remaining().div_ceil(2)
}

/// Runs `encode` transactionally within the per-attribute budget. `Ok(true)`
/// means it succeeded but truncated; `Err` means it was rolled back.
fn bounded<B: BoundedBuf>(
    buf: &mut B,
    encode: impl FnOnce(&mut B) -> Result<bool, EncodeFailure>,
) -> Result<bool, EncodeFailure> {
    let budget = attr_budget(buf);
    let mut truncated = false;
    buf.with_max_remaining(budget, |buf| {
        buf.try_encode(|buf| {
            truncated = encode(buf)?;
            Ok::<_, EncodeFailure>(())
        })
    })?;
    Ok(truncated)
}

fn write_tagged<B: BoundedBuf>(
    buf: &mut B,
    field: u64,
    wire_type: u64,
    encode: impl FnOnce(&mut B) -> EncodeResult,
) -> Result<bool, EncodeFailure> {
    buf.encode_field_tag(field, wire_type)
        .and_then(|()| encode(buf))
        .map(|()| false)
}

/// Encodes `f`'s value as a length-delimited `field`. `Ok(true)`
/// means part of the value was lost; `Err` means it could not be written and
/// must be rolled back.
fn write_value<B: BoundedBuf>(
    buf: &mut B,
    field: u64,
    depth: usize,
    f: impl FnOnce(&mut ValueEncoder<'_, B>),
) -> Result<bool, EncodeFailure> {
    let mut lost = false;
    buf.encode_len_delimited(field, |buf| {
        let mut writer = ValueEncoder {
            buf,
            depth,
            outcome: Ok(false),
            #[cfg(debug_assertions)]
            wrote_value: false,
        };
        f(&mut writer);
        writer.outcome.map(|writer_lost| lost = writer_lost)
    })?;
    Ok(lost)
}

/// Encodes a `KeyValue` as a length-delimited `field`; see [`write_value`].
fn write_key_value<B: BoundedBuf>(
    buf: &mut B,
    field: u64,
    key: &str,
    depth: usize,
    f: impl FnOnce(&mut ValueEncoder<'_, B>),
) -> Result<bool, EncodeFailure> {
    let mut lost = false;
    buf.encode_len_delimited(field, |buf| {
        buf.encode_string(KEY_VALUE_KEY, key)?;
        lost = write_value(buf, KEY_VALUE_VALUE, depth, f)?;
        Ok::<_, EncodeFailure>(())
    })?;
    Ok(lost)
}

/// Writes one AnyValue into a bounded buffer.
struct ValueEncoder<'a, B: BoundedBuf> {
    buf: &'a mut B,
    depth: usize,
    /// `Ok(true)` once any part of the value was lost; a sticky `Err` once
    /// part of it could not be written.
    outcome: Result<bool, EncodeFailure>,
    #[cfg(debug_assertions)]
    wrote_value: bool,
}

impl<B: BoundedBuf> ValueEncoder<'_, B> {
    fn put(&mut self, result: Result<bool, EncodeFailure>) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                !self.wrote_value,
                "value callback must write at most one AnyValue variant"
            );
            self.wrote_value = true;
        }
        self.outcome = self.outcome.and_then(|lost| Ok(lost | result?));
    }

    /// Writes a container, retaining its prefix when a member cannot be encoded.
    fn nested(&mut self, field: u64, f: impl FnOnce(&mut NestedEncoder<'_, B>)) {
        if self.depth >= MAX_DEPTH {
            self.put(Err(EncodeFailure::Dropped));
            return;
        }
        let depth = self.depth + 1;
        let mut lost = false;
        let result = self.buf.encode_len_delimited(field, |buf| {
            let mut writer = NestedEncoder {
                buf,
                depth,
                outcome: Ok(false),
            };
            f(&mut writer);
            lost = writer.outcome.unwrap_or(true);
            Ok::<_, EncodeFailure>(())
        });
        self.put(result.map(|()| lost));
    }

    fn map(&mut self, f: impl FnOnce(&mut NestedEncoder<'_, B>)) {
        self.nested(ANY_VALUE_KVLIST_VALUE, f);
    }

    fn array(&mut self, f: impl FnOnce(&mut NestedEncoder<'_, B>)) {
        self.nested(ANY_VALUE_ARRAY_VALUE, f);
    }
}

/// Writes a budgeted prefix of map entries or array elements.
struct NestedEncoder<'a, B: BoundedBuf> {
    buf: &'a mut B,
    depth: usize,
    /// `Ok(true)` retains partial members; `Err` stops subsequent callbacks.
    outcome: Result<bool, EncodeFailure>,
}

impl<B: BoundedBuf> NestedEncoder<'_, B> {
    fn entry(&mut self, key: &str, f: impl FnOnce(&mut ValueEncoder<'_, B>)) {
        self.outcome = self.outcome.and_then(|lost| {
            bounded(self.buf, |buf| {
                write_key_value(buf, KEY_VALUE_LIST_VALUES, key, self.depth, f)
            })
            .map(|v| lost | v)
        });
    }

    fn element(&mut self, f: impl FnOnce(&mut ValueEncoder<'_, B>)) {
        self.outcome = self.outcome.and_then(|lost| {
            bounded(self.buf, |buf| {
                write_value(buf, ARRAY_VALUE_VALUES, self.depth, f)
            })
            .map(|v| lost | v)
        });
    }
}

impl<B: BoundedBuf> ValueWriter for ValueEncoder<'_, B> {
    fn write_str(&mut self, value: &str) {
        let result = self
            .buf
            .encode_string_truncating(ANY_VALUE_STRING_VALUE, value);
        self.put(result);
    }

    fn write_debug(&mut self, value: &dyn std::fmt::Debug) {
        let result = encode_debug_string(self.buf, value);
        self.put(result);
    }

    fn write_i64(&mut self, value: i64) {
        let result = write_tagged(self.buf, ANY_VALUE_INT_VALUE, wire_types::VARINT, |buf| {
            buf.encode_varint(value as u64)
        });
        self.put(result);
    }

    fn write_f64(&mut self, value: f64) {
        let result = write_tagged(
            self.buf,
            ANY_VALUE_DOUBLE_VALUE,
            wire_types::FIXED64,
            |buf| buf.extend_from_slice(&value.to_le_bytes()),
        );
        self.put(result);
    }

    fn write_bool(&mut self, value: bool) {
        let result = write_tagged(self.buf, ANY_VALUE_BOOL_VALUE, wire_types::VARINT, |buf| {
            buf.encode_varint(u64::from(value))
        });
        self.put(result);
    }

    fn write_map(&mut self, f: &mut dyn FnMut(&mut dyn MapWriter)) {
        self.map(|w| f(w));
    }

    fn write_array(&mut self, f: &mut dyn FnMut(&mut dyn ArrayWriter)) {
        self.array(|w| f(w));
    }
}

impl<B: BoundedBuf> MapWriter for NestedEncoder<'_, B> {
    fn write_entry(&mut self, key: &str, f: &mut dyn FnMut(&mut dyn ValueWriter)) {
        self.entry(key, |w| f(w));
    }
}

impl<B: BoundedBuf> ArrayWriter for NestedEncoder<'_, B> {
    fn write_element(&mut self, f: &mut dyn FnMut(&mut dyn ValueWriter)) {
        self.element(|w| f(w));
    }
}

impl<B: BoundedBuf> tracing::field::Visit for DirectFieldVisitor<'_, B> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.record_value(field, |w| w.write_f64(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.record_value(field, |w| w.write_i64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.record_i64(field, value as i64);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.record_value(field, |w| w.write_bool(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match value_location(field) {
            ValueLocation::Body if value.is_empty() => {}
            location => self.write_any_value(location, |w| w.write_str(value)),
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.write_any_value(value_location(field), |w| w.write_debug(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        match value.downcast_ref::<StructCarrier>() {
            Some(structured) => {
                self.write_any_value(value_location(field), |w| structured.value().write(w))
            }
            None => self.record_debug(field, &format_args!("{value}")),
        }
    }
}

/// Convert tracing Level to OTLP severity number.
///
/// See: <https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber>
#[inline]
#[must_use]
pub const fn level_to_severity_number(level: &Level) -> u8 {
    match *level {
        Level::TRACE => 1,
        Level::DEBUG => 5,
        Level::INFO => 9,
        Level::WARN => 13,
        Level::ERROR => 17,
    }
}

/// Encode configured resource attributes as the resource fragment shared by
/// OTLP `ResourceLogs` and `ResourceMetrics` messages.
///
/// The fragment contains only the encoded `resource` field. Both OTLP message
/// types assign that field the same number, so callers can reuse these bytes
/// without constructing an intermediate SDK resource.
pub(crate) fn encode_config_resource_field(
    resource_attributes: &HashMap<String, ConfigAttributeValue>,
) -> Result<Bytes, EncodeFailure> {
    let mut buf = ProtoBuffer::with_capacity(256);
    write_config_resource_field(&mut buf, resource_attributes)?;
    Ok(buf.into_bytes())
}

fn write_config_resource_field<B: BoundedBuf>(
    buf: &mut B,
    resource_attributes: &HashMap<String, ConfigAttributeValue>,
) -> EncodeResult {
    buf.try_encode(|buf| {
        buf.encode_len_delimited(RESOURCE_LOGS_RESOURCE, |buf| {
            let mut dropped = 0u32;
            for (key, value) in resource_attributes {
                let lost = encode_attribute(buf, RESOURCE_ATTRIBUTES, key, |w| {
                    write_config_value(w, value)
                });
                dropped = dropped.saturating_add(u32::from(lost));
            }
            write_dropped_count(buf, RESOURCE_DROPPED_ATTRIBUTES_COUNT, dropped)
        })
    })
}

fn write_dropped_count<B: BoundedBuf>(buf: &mut B, field: u64, count: u32) -> EncodeResult {
    if count != 0 {
        buf.encode_field_tag(field, wire_types::VARINT)?;
        buf.encode_varint(u64::from(count))?;
    }
    Ok(())
}

/// Writes one configured resource value.
fn write_config_value<B: BoundedBuf>(w: &mut ValueEncoder<'_, B>, value: &ConfigAttributeValue) {
    match value {
        ConfigAttributeValue::String(v) => w.write_str(v),
        ConfigAttributeValue::Bool(v) => w.write_bool(*v),
        ConfigAttributeValue::I64(v) => w.write_i64(*v),
        ConfigAttributeValue::F64(v) => w.write_f64(*v),
        ConfigAttributeValue::Array(values) => w.array(|a| match values {
            ConfigAttributeValueArray::Bool(vs) => elements(a, vs, |w, v| w.write_bool(*v)),
            ConfigAttributeValueArray::I64(vs) => elements(a, vs, |w, v| w.write_i64(*v)),
            ConfigAttributeValueArray::F64(vs) => elements(a, vs, |w, v| w.write_f64(*v)),
            ConfigAttributeValueArray::String(vs) => elements(a, vs, |w, v| w.write_str(v)),
        }),
    }
}

/// Writes a homogeneous slice as array elements.
fn elements<B: BoundedBuf, T>(
    a: &mut NestedEncoder<'_, B>,
    values: &[T],
    write: impl Fn(&mut ValueEncoder<'_, B>, &T),
) {
    for value in values {
        a.element(|w| write(w, value));
        if a.outcome.is_err() {
            break;
        }
    }
}

/// Encodes one independently budgeted attribute, reporting any partial or total loss.
fn encode_attribute<B: BoundedBuf>(
    buf: &mut B,
    field: u64,
    key: &str,
    f: impl FnOnce(&mut ValueEncoder<'_, B>),
) -> bool {
    bounded(buf, |buf| write_key_value(buf, field, key, 0, f)).unwrap_or(true)
}

// Field numbers for ExportLogsServiceRequest and related messages
const EXPORT_LOGS_REQUEST_RESOURCE_LOGS: u64 = 1;

/// Pre-encoded scope attribute bytes keyed by EntityKey.
///
/// The Internal Telemetry Receiver uses this to avoid re-encoding scope
/// attributes for each log event. Entity attributes are looked up once
/// from the registry and encoded as InstrumentationScope.attributes bytes.
/// Entity keys must remain registered until their first lookup populates this
/// cache; a key that is neither registered nor cached cannot be encoded.
///
/// TODO(4453): Encode scope attributes during entity construction, returning
/// errors there, and share those bytes with metrics to remove this local cache.
///
/// TODO: ScopeToBytesMap grows without any attention to managing memory.
/// We will require a way to de-register entities that are no longer use
/// or to flush memory to maintain a fixed size.
#[derive(Debug)]
pub struct ScopeToBytesMap {
    cache: HashMap<EntityKey, Bytes>,
    registry: TelemetryRegistryHandle,
}

/// A dedicated error value indicating that an EntityKey was not
/// found.
pub enum MapError {
    /// EntityKey is not registered
    NotRegistered,
    /// Encoding the registered entity's attributes failed.
    Failed(EncodeFailure),
}

impl ScopeToBytesMap {
    /// Create a new empty scope attribute cache.
    #[must_use]
    pub fn new(registry: TelemetryRegistryHandle) -> Self {
        Self {
            cache: HashMap::new(),
            registry,
        }
    }

    /// Get or compute complete scope attribute bytes for an entity key.
    pub fn get_or_encode(&mut self, key: EntityKey) -> Result<Bytes, MapError> {
        if let Some(cached) = self.cache.get(&key) {
            return Ok(cached.clone());
        }

        match self.registry.visit_entity(key, |attrs| {
            encode_scope_attributes(attrs.iter_attributes())
        }) {
            Some(Ok(bytes)) => {
                let _ = self.cache.insert(key, bytes.clone());
                Ok(bytes)
            }
            Some(Err(err)) => Err(MapError::Failed(err)),
            None => Err(MapError::NotRegistered),
        }
    }

    /// Clear the cache. Call this when entities may have been updated.
    pub fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Encode complete scope attributes into growable storage, rejecting any loss.
fn encode_scope_attributes<'k, 'v>(
    attrs: impl IntoIterator<Item = (&'k str, &'v AttributeValue)>,
) -> Result<Bytes, EncodeFailure> {
    let mut buf = ProtoBuffer::with_capacity(128);
    for (key, value) in attrs {
        if write_key_value(&mut buf, INSTRUMENTATION_SCOPE_ATTRIBUTES, key, 0, |w| {
            write_attribute_value(w, value)
        })? {
            return Err(EncodeFailure::Dropped);
        }
    }
    Ok(buf.into_bytes())
}

/// Writes an `AttributeValue`; maps become kvlists.
fn write_attribute_value<B: BoundedBuf>(w: &mut ValueEncoder<'_, B>, value: &AttributeValue) {
    match value {
        AttributeValue::String(v) => w.write_str(v),
        AttributeValue::Boolean(v) => w.write_bool(*v),
        AttributeValue::Int(v) => w.write_i64(*v),
        AttributeValue::UInt(v) => w.write_i64(*v as i64),
        AttributeValue::Double(v) => w.write_f64(*v),
        AttributeValue::Map(m) => w.map(|mw| {
            for (k, v) in m {
                mw.entry(k, |w| write_attribute_value(w, v));
                if mw.outcome.is_err() {
                    break;
                }
            }
        }),
    }
}

fn encode_scope_logs(
    buf: &mut ProtoBuffer,
    target: &str,
    context: &[EntityKey],
    events: &[LogEvent],
    scope_cache: &mut ScopeToBytesMap,
) -> EncodeResult {
    buf.encode_len_delimited(RESOURCE_LOGS_SCOPE_LOGS, |buf| {
        buf.encode_len_delimited(SCOPE_LOG_SCOPE, |buf| {
            buf.encode_string(INSTRUMENTATION_SCOPE_NAME, target)?;
            for entity_key in context {
                match scope_cache.get_or_encode(*entity_key) {
                    Ok(bytes) => buf.extend_from_slice(&bytes)?,
                    Err(MapError::NotRegistered) => {
                        buf.extend_from_slice(&encode_scope_attributes(std::iter::once((
                            "unregistered.entity",
                            &AttributeValue::Int(entity_key.data().as_ffi() as i64),
                        )))?)?;
                    }
                    Err(MapError::Failed(err)) => return Err(err),
                }
            }
            Ok(())
        })?;

        for event in events {
            buf.encode_len_delimited(SCOPE_LOGS_LOG_RECORDS, |buf| {
                let mut encoder = DirectLogRecordEncoder::new(buf);
                let _ = encoder.encode_log_record(event.time, &event.record);
                Ok(())
            })?;
        }
        Ok(())
    })
}

/// Encode logs as one request, grouping by scope.
///
/// Every entity key referenced by an event must remain registered until its
/// scope attributes have been cached.
pub fn encode_export_logs_request(
    buf: &mut ProtoBuffer,
    events: &mut [LogEvent],
    resource_bytes: &Bytes,
    scope_cache: &mut ScopeToBytesMap,
) -> EncodeResult {
    buf.clear();
    if events.is_empty() {
        return Ok(());
    }

    // Sort the events in scope order.
    events.sort_by(|a, b| {
        a.record
            .callsite()
            .target()
            .cmp(b.record.callsite().target())
            .then_with(|| {
                a.record
                    .context
                    .iter()
                    .map(|key| key.data().as_ffi())
                    .cmp(b.record.context.iter().map(|key| key.data().as_ffi()))
            })
    });

    buf.encode_len_delimited(EXPORT_LOGS_REQUEST_RESOURCE_LOGS, |buf| {
        buf.extend_from_slice(resource_bytes)?;

        for events in events.chunk_by(|a, b| {
            a.record.callsite().target() == b.record.callsite().target()
                && a.record.context == b.record.context
        }) {
            let first = &events[0];

            encode_scope_logs(
                buf,
                first.record.callsite().target(),
                first.record.context.as_slice(),
                events,
                scope_cache,
            )?;
        }
        Ok(())
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::__log_record_impl;
    use crate::LogContext;
    use crate::attributes::AttributeSetHandler;
    use crate::descriptor::{AttributeField, AttributeValueType, AttributesDescriptor};
    use crate::self_tracing::format_log_record_to_string;
    use crate::self_tracing::{StructBox, StructuredValue};
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value::Value as AnyValueKind,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs, SeverityNumber,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use prost::Message;
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};
    use tracing::Level;

    static TEST_SCOPE_ATTRIBUTES_DESCRIPTOR: AttributesDescriptor = AttributesDescriptor {
        name: "TestScope",
        fields: &[
            AttributeField {
                key: "pipeline.name",
                r#type: AttributeValueType::String,
                brief: "Pipeline name",
            },
            AttributeField {
                key: "cpu.id",
                r#type: AttributeValueType::Int,
                brief: "CPU ID",
            },
        ],
    };

    /// Mock attribute set for testing scope attributes.
    #[derive(Debug)]
    struct TestScopeAttributes {
        values: Vec<AttributeValue>,
    }

    impl TestScopeAttributes {
        fn new(name: &str, id: i64) -> Self {
            Self {
                // Note: order matches the AttributeFields.
                values: vec![AttributeValue::String(name.into()), AttributeValue::Int(id)],
            }
        }
    }

    impl AttributeSetHandler for TestScopeAttributes {
        fn descriptor(&self) -> &'static AttributesDescriptor {
            &TEST_SCOPE_ATTRIBUTES_DESCRIPTOR
        }

        fn attribute_values(&self) -> &[AttributeValue] {
            &self.values
        }
    }

    /// Scenario: configured scalar and array resource attributes are encoded directly.
    /// Guarantees: the reusable OTLP resource fragment preserves every supported value type.
    #[test]
    fn encode_config_resource_field_preserves_supported_values() {
        let empty = ResourceLogs::decode(
            encode_config_resource_field(&HashMap::new()).expect("resource fits"),
        )
        .expect("empty resource fragment should decode");
        assert_eq!(
            empty
                .resource
                .expect("empty resource should still be present")
                .attributes,
            []
        );

        let resource = HashMap::from([
            (
                "service.name".to_owned(),
                ConfigAttributeValue::String("test-svc".to_owned()),
            ),
            ("enabled".to_owned(), ConfigAttributeValue::Bool(true)),
            ("replicas".to_owned(), ConfigAttributeValue::I64(3)),
            ("ratio".to_owned(), ConfigAttributeValue::F64(0.5)),
            (
                "zones".to_owned(),
                ConfigAttributeValue::Array(ConfigAttributeValueArray::String(vec![
                    "west".to_owned(),
                    "east".to_owned(),
                ])),
            ),
            (
                "flags".to_owned(),
                ConfigAttributeValue::Array(ConfigAttributeValueArray::Bool(vec![true, false])),
            ),
            (
                "ports".to_owned(),
                ConfigAttributeValue::Array(ConfigAttributeValueArray::I64(vec![4317, 4318])),
            ),
            (
                "weights".to_owned(),
                ConfigAttributeValue::Array(ConfigAttributeValueArray::F64(vec![0.25, 0.75])),
            ),
        ]);
        let bytes = encode_config_resource_field(&resource).expect("resource fits");
        let decoded = ResourceLogs::decode(bytes).expect("resource fragment should decode");
        let actual = decoded
            .resource
            .expect("resource should exist")
            .attributes
            .into_iter()
            .map(|attribute| {
                (
                    attribute.key,
                    attribute.value.expect("attribute value should be present"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let expected = BTreeMap::from([
            ("enabled".to_owned(), AnyValue::new_bool(true)),
            (
                "flags".to_owned(),
                AnyValue::new_array(vec![AnyValue::new_bool(true), AnyValue::new_bool(false)]),
            ),
            (
                "ports".to_owned(),
                AnyValue::new_array(vec![AnyValue::new_int(4317), AnyValue::new_int(4318)]),
            ),
            ("ratio".to_owned(), AnyValue::new_double(0.5)),
            ("replicas".to_owned(), AnyValue::new_int(3)),
            ("service.name".to_owned(), AnyValue::new_string("test-svc")),
            (
                "weights".to_owned(),
                AnyValue::new_array(vec![AnyValue::new_double(0.25), AnyValue::new_double(0.75)]),
            ),
            (
                "zones".to_owned(),
                AnyValue::new_array(vec![
                    AnyValue::new_string("west"),
                    AnyValue::new_string("east"),
                ]),
            ),
        ]);

        assert_eq!(actual, expected);
        assert!(decoded.scope_logs.is_empty());
    }

    /// Scenario: one log carries registered scope attributes.
    /// Guarantees: the request preserves the scope, record, timestamp, and resource.
    #[test]
    fn encode_export_logs_request_with_scope_attributes() {
        let registry = TelemetryRegistryHandle::new();
        let entity_key = registry.register_entity(TestScopeAttributes::new("my-pipeline", 3));
        let mut scope_cache = ScopeToBytesMap::new(registry.clone());

        // Use the macro to create a LogRecord, then override context with entity
        let record = __log_record_impl!(Level::INFO, "test.scope.encoding")
            .into_record(LogContext::from_buf([entity_key]));

        // Create a LogEvent with known timestamp, empty resource.
        let timestamp_ns: u64 = 1_705_321_845_000_000_000;
        let time = SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp_ns);
        let log_event = LogEvent { time, record };

        let resource_bytes = encode_config_resource_field(&HashMap::new()).expect("resource fits");

        let mut buf = ProtoBuffer::default();
        encode_export_logs_request(
            &mut buf,
            &mut [log_event.clone()],
            &resource_bytes,
            &mut scope_cache,
        )
        .unwrap();

        let decoded = ExportLogsServiceRequest::decode(buf.into_bytes().as_ref()).unwrap();
        let scope_logs = &decoded.resource_logs.first().unwrap().scope_logs;
        let scope = scope_logs
            .first()
            .unwrap()
            .scope
            .as_ref()
            .expect("scope present");
        let event_name = &scope_logs
            .first()
            .unwrap()
            .log_records
            .first()
            .unwrap()
            .event_name;

        // event_name is the bare semconv identifier; the emitting crate
        // is conveyed via InstrumentationScope.name. See
        // encode_event_name + encode_export_logs_request.
        assert_eq!(event_name, "test.scope.encoding");
        assert_eq!(scope.name, "otel-arrow-dfe-telemetry");

        let expected = ExportLogsServiceRequest::new([ResourceLogs::new(
            Resource::build().finish(),
            [ScopeLogs::new(
                InstrumentationScope::build()
                    .name("otel-arrow-dfe-telemetry")
                    .attributes([
                        KeyValue::new("pipeline.name", AnyValue::new_string("my-pipeline")),
                        KeyValue::new("cpu.id", AnyValue::new_int(3)),
                    ])
                    .finish(),
                [LogRecord::build()
                    .event_name(event_name) // from the decoded value
                    .time_unix_nano(timestamp_ns)
                    .severity_number(SeverityNumber::Info)
                    .finish()],
            )],
        )]);

        // Inspect the printed format. Console output still uses the
        // `target::name` style for human readability; only OTLP changes.
        assert_eq!(
            format_log_record_to_string(None, &log_event.record),
            format!(
                "INFO  otel-arrow-dfe-telemetry::{event_name} entity={:?}\n",
                entity_key
            ),
        );
        assert_eq!(expected, decoded);
    }

    /// Scenario: a log references an entity key that has been unregistered.
    /// Guarantees: the scope records the unregistered entity's FFI key as an int64 attribute.
    #[test]
    fn encode_export_logs_request_with_unregistered_entity() {
        let registry = TelemetryRegistryHandle::new();
        let entity_key = registry.register_entity(TestScopeAttributes::new("removed", 1));
        assert!(registry.unregister_entity(entity_key));
        let mut scope_cache = ScopeToBytesMap::new(registry);
        let record = __log_record_impl!(Level::INFO, "test.unregistered.entity")
            .into_record(LogContext::from_buf([entity_key]));
        let mut events = [LogEvent {
            time: SystemTime::UNIX_EPOCH,
            record,
        }];
        let mut buf = ProtoBuffer::default();

        encode_export_logs_request(&mut buf, &mut events, &Bytes::new(), &mut scope_cache).unwrap();

        let decoded =
            ExportLogsServiceRequest::decode(buf.into_bytes()).expect("valid logs request");
        let scope = decoded.resource_logs[0].scope_logs[0]
            .scope
            .as_ref()
            .expect("scope present");
        assert_eq!(
            scope.attributes,
            [KeyValue::new(
                "unregistered.entity",
                AnyValue::new_int(entity_key.data().as_ffi() as i64),
            )]
        );
    }

    /// Scenario: a batch interleaves records from two entity contexts.
    /// Guarantees: equal contexts share one scope regardless of entity-key sort order.
    #[test]
    fn encode_export_logs_request_groups_equal_scopes() {
        fn assert_scope<const N: usize>(
            scope_logs: &[ScopeLogs],
            pipeline_name: &'static str,
            cpu_id: i64,
            event_names: [&'static str; N],
        ) {
            let expected_attributes = [
                KeyValue::new("pipeline.name", AnyValue::new_string(pipeline_name)),
                KeyValue::new("cpu.id", AnyValue::new_int(cpu_id)),
            ];
            let scope_logs = scope_logs
                .iter()
                .find(|scope_logs| {
                    scope_logs
                        .scope
                        .as_ref()
                        .is_some_and(|scope| scope.attributes == expected_attributes)
                })
                .expect("expected scope attributes");

            assert_eq!(
                scope_logs
                    .log_records
                    .iter()
                    .map(|record| record.event_name.as_str())
                    .collect::<Vec<_>>(),
                event_names
            );
        }

        let registry = TelemetryRegistryHandle::new();
        let key_a = registry.register_entity(TestScopeAttributes::new("pipeline-a", 1));
        let key_b = registry.register_entity(TestScopeAttributes::new("pipeline-b", 2));
        let mut scope_cache = ScopeToBytesMap::new(registry);
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let mut events = vec![
            LogEvent {
                time,
                record: __log_record_impl!(Level::INFO, "test.batch.b-first")
                    .into_record(LogContext::from_buf([key_b])),
            },
            LogEvent {
                time,
                record: __log_record_impl!(Level::INFO, "test.batch.a")
                    .into_record(LogContext::from_buf([key_a])),
            },
            LogEvent {
                time,
                record: __log_record_impl!(Level::INFO, "test.batch.b-second")
                    .into_record(LogContext::from_buf([key_b])),
            },
        ];

        let mut buf = ProtoBuffer::default();
        encode_export_logs_request(
            &mut buf,
            events.as_mut_slice(),
            &Bytes::new(),
            &mut scope_cache,
        )
        .unwrap();
        let decoded =
            ExportLogsServiceRequest::decode(buf.into_bytes()).expect("valid logs request");
        let scope_logs = &decoded.resource_logs[0].scope_logs;

        assert_eq!(scope_logs.len(), 2);
        assert_scope(
            scope_logs,
            "pipeline-b",
            2,
            ["test.batch.b-first", "test.batch.b-second"],
        );
        assert_scope(scope_logs, "pipeline-a", 1, ["test.batch.a"]);
    }

    // --- Test infrastructure for Map (kvlist) scope attributes ---

    static TEST_MAP_ATTRIBUTES_DESCRIPTOR: AttributesDescriptor = AttributesDescriptor {
        name: "TestMap",
        fields: &[AttributeField {
            key: "custom",
            r#type: AttributeValueType::Map,
            brief: "Custom user-defined attributes",
        }],
    };

    /// Mirrors engine::CustomAttributeSet: a single "custom" field of type Map.
    #[derive(Debug)]
    struct TestMapAttributes {
        values: Vec<AttributeValue>,
    }

    impl TestMapAttributes {
        fn new(map: BTreeMap<String, AttributeValue>) -> Self {
            Self {
                values: vec![AttributeValue::Map(map)],
            }
        }
    }

    impl AttributeSetHandler for TestMapAttributes {
        fn descriptor(&self) -> &'static AttributesDescriptor {
            &TEST_MAP_ATTRIBUTES_DESCRIPTOR
        }

        fn attribute_values(&self) -> &[AttributeValue] {
            &self.values
        }
    }

    /// Scenario: a registered scope attribute contains a map.
    /// Guarantees: the map is encoded as an OTLP kvlist with all entries preserved.
    #[test]
    fn encode_export_logs_request_with_map_scope_attribute() {
        let registry = TelemetryRegistryHandle::new();

        let mut custom_map = BTreeMap::new();
        let _ = custom_map.insert("priority".to_string(), AttributeValue::Int(5));
        let _ = custom_map.insert(
            "region".to_string(),
            AttributeValue::String("us-east-1".into()),
        );

        let entity_key = registry.register_entity(TestMapAttributes::new(custom_map));
        let mut scope_cache = ScopeToBytesMap::new(registry.clone());

        let record = __log_record_impl!(Level::INFO, "test.map.encoding")
            .into_record(LogContext::from_buf([entity_key]));

        let timestamp_ns: u64 = 1_705_321_845_000_000_000;
        let time = SystemTime::UNIX_EPOCH + Duration::from_nanos(timestamp_ns);
        let log_event = LogEvent { time, record };

        let resource_bytes = encode_config_resource_field(&HashMap::new()).expect("resource fits");
        let mut buf = ProtoBuffer::default();
        encode_export_logs_request(
            &mut buf,
            &mut [log_event],
            &resource_bytes,
            &mut scope_cache,
        )
        .unwrap();

        let decoded = ExportLogsServiceRequest::decode(buf.into_bytes().as_ref()).unwrap();
        let scope = decoded.resource_logs[0].scope_logs[0]
            .scope
            .as_ref()
            .expect("scope present");
        let event_name = &decoded.resource_logs[0].scope_logs[0].log_records[0].event_name;
        assert_eq!(event_name, "test.map.encoding");
        assert_eq!(scope.name, "otel-arrow-dfe-telemetry");

        // BTreeMap iterates in sorted key order: "priority" before "region".
        let expected = ExportLogsServiceRequest::new([ResourceLogs::new(
            Resource::build().finish(),
            [ScopeLogs::new(
                InstrumentationScope::build()
                    .name("otel-arrow-dfe-telemetry")
                    .attributes([KeyValue::new(
                        "custom",
                        AnyValue::new_kvlist(vec![
                            KeyValue::new("priority", AnyValue::new_int(5)),
                            KeyValue::new("region", AnyValue::new_string("us-east-1")),
                        ]),
                    )])
                    .finish(),
                [LogRecord::build()
                    .event_name(event_name)
                    .time_unix_nano(timestamp_ns)
                    .severity_number(SeverityNumber::Info)
                    .finish()],
            )],
        )]);

        assert_eq!(expected, decoded);
    }

    /// Scenario: an attribute callback moves an owned string through map and array callbacks.
    /// Guarantees: internal callbacks accept consuming closures and encode the value exactly once.
    #[test]
    fn value_callbacks_can_consume_owned_values() {
        let mut buf = ProtoBuffer::with_capacity(128);
        let value = String::from("owned");

        let lost = encode_attribute(&mut buf, LOG_RECORD_ATTRIBUTES, "structured", move |w| {
            w.map(move |m| {
                m.entry("items", move |w| {
                    w.array(move |a| {
                        a.element(move |w| {
                            w.write_str(&value);
                            drop(value);
                        });
                    });
                });
            });
        });
        assert!(!lost);

        let record = LogRecord::decode(buf.as_ref()).expect("valid record");
        assert_eq!(
            record.attributes,
            vec![KeyValue::new(
                "structured",
                AnyValue::new_kvlist(vec![KeyValue::new(
                    "items",
                    AnyValue::new_array(vec![AnyValue::new_string("owned")]),
                )]),
            )]
        );
    }

    /// Scenario: one value callback writes two AnyValue variants.
    /// Guarantees: debug builds reject ambiguous oneof encodings.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "value callback must write at most one AnyValue variant")]
    fn value_encoder_rejects_multiple_variants() {
        let mut buf = ProtoBuffer::with_capacity(32);
        let write = |writer: &mut ValueEncoder<'_, ProtoBuffer>| {
            writer.write_i64(1);
            writer.write_bool(true);
        };

        let _ = write_value(&mut buf, LOG_RECORD_BODY, 0, write);
    }

    /// Scenario: four large string attributes compete for a 256-byte buffer.
    /// Guarantees: each uses at most half the remaining budget, is truncated,
    /// and increments `dropped_attributes_count`.
    #[test]
    fn record_str_halves_remaining_budget_across_attributes() {
        use crate::self_tracing::encoder::DirectFieldVisitor;
        use otel_arrow_dfe_pdata::otlp::common::TRUNCATION_SUFFIX;
        use otel_arrow_dfe_pdata::otlp::common::{BoundedBuf, StackProtoBuffer};
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::KeyValue as ProtoKeyValue;
        use prost::Message;
        use tracing::field::Visit;

        const INLINE: usize = 256;
        let big = "x".repeat(1000);

        let meta = &BUDGET_TEST_METADATA;
        let fields = meta.fields();
        let a = fields.field("a").unwrap();
        let b = fields.field("b").unwrap();
        let c = fields.field("c").unwrap();
        let d = fields.field("d").unwrap();

        // Drive the visitor directly with each (field, value) pair, which is
        // what `event.record(visitor)` does internally.
        let mut buf = StackProtoBuffer::<INLINE>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_str(&a, big.as_str());
        visitor.record_str(&b, big.as_str());
        visitor.record_str(&c, big.as_str());
        visitor.record_str(&d, big.as_str());
        let dropped = visitor.dropped_count();

        // All four values should be truncated.
        assert_eq!(dropped, 4, "expected 4 truncated attributes");

        // Buffer should not exceed the inline limit.
        assert!(
            buf.len() <= INLINE,
            "buffer len {} exceeds INLINE {}",
            buf.len(),
            INLINE
        );

        // Decode each attribute as a KeyValue (field tag = LOG_RECORD_ATTRIBUTES = 6,
        // wire type LEN). Walk the buffer manually.
        let bytes = buf.as_ref().to_vec();
        let mut cursor = bytes.as_slice();
        let mut decoded: Vec<(String, String)> = Vec::new();
        while !cursor.is_empty() {
            let (tag, n) = read_varint(cursor);
            cursor = &cursor[n..];
            let field_num = tag >> 3;
            let wire_type = tag & 0x7;
            assert_eq!(wire_type, wire_types::LEN, "expected LEN wire type");
            assert_eq!(field_num, LOG_RECORD_ATTRIBUTES, "expected ATTRIBUTES tag");
            let (len, n) = read_varint(cursor);
            cursor = &cursor[n..];
            let len = len as usize;
            let kv_bytes = &cursor[..len];
            cursor = &cursor[len..];
            let kv = ProtoKeyValue::decode(kv_bytes).unwrap();
            let val = kv
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .map(|v| match v {
                    otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(s) => s.clone(),
                    _ => panic!("expected string value"),
                })
                .unwrap();
            decoded.push((kv.key, val));
        }

        assert_eq!(decoded.len(), 4, "expected 4 decoded attributes");

        // Each value should end with the truncation suffix.
        let suffix = std::str::from_utf8(TRUNCATION_SUFFIX).unwrap();
        for (k, v) in &decoded {
            assert!(
                v.ends_with(suffix),
                "attribute {k} value should end with {suffix}, got len {}",
                v.len()
            );
        }

        // Each successive value should be smaller (halving budget). We compare
        // value lengths rather than exact sizes to avoid brittleness.
        let lens: Vec<usize> = decoded.iter().map(|(_, v)| v.len()).collect();
        assert!(
            lens.windows(2).all(|w| w[0] > w[1]),
            "expected strictly decreasing value lengths, got {lens:?}"
        );

        // Sanity-check the rough magnitudes from the user's spec
        // (~100 / ~50 / ~25 / ~10), accommodating overhead variation.
        // Observed: ~[118, 55, 23, 7].
        assert!(
            lens[0] > 60 && lens[0] < 140,
            "1st value len {} out of expected range",
            lens[0]
        );
        assert!(
            lens[1] > 25 && lens[1] < 80,
            "2nd value len {} out of expected range",
            lens[1]
        );
        assert!(
            lens[2] > 10 && lens[2] < 50,
            "3rd value len {} out of expected range",
            lens[2]
        );
        // Last value may be just the suffix (~5 bytes) up to ~25 bytes.
        assert!(
            lens[3] >= suffix.len() && lens[3] < 30,
            "4th value len {} out of expected range",
            lens[3]
        );
    }

    /// Scenario: a Debug value fits in the attribute budget.
    /// Guarantees: its direct encoding is decodable and matches `format!("{:?}")`.
    #[test]
    fn record_debug_writes_fmt_output_without_intermediate_string() {
        use otel_arrow_dfe_pdata::otlp::common::StackProtoBuffer;
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::KeyValue as ProtoKeyValue;
        use prost::Message;
        use tracing::field::Visit;

        #[derive(Debug)]
        #[allow(dead_code)]
        struct Payload {
            id: u32,
            label: &'static str,
        }
        let value = Payload {
            id: 7,
            label: "hello",
        };
        let expected_repr = format!("{:?}", value);

        let meta = &DEBUG_TEST_METADATA;
        let fields = meta.fields();
        let dbg = fields.field("dbg").unwrap();

        let mut buf = StackProtoBuffer::<256>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_debug(&dbg, &value);
        assert_eq!(visitor.dropped_count(), 0, "no fields should be dropped");

        // Decode the single KeyValue from the buffer.
        let bytes = buf.as_ref().to_vec();
        let mut cursor = bytes.as_slice();
        let (tag, n) = read_varint(cursor);
        cursor = &cursor[n..];
        assert_eq!(tag >> 3, LOG_RECORD_ATTRIBUTES);
        assert_eq!(tag & 0x7, wire_types::LEN);
        let (len, n) = read_varint(cursor);
        cursor = &cursor[n..];
        let kv = ProtoKeyValue::decode(&cursor[..len as usize]).unwrap();
        assert_eq!(kv.key, "dbg");
        let s = match kv.value.as_ref().unwrap().value.as_ref().unwrap() {
            otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(s) => {
                s.clone()
            }
            other => panic!("expected StringValue, got {other:?}"),
        };
        assert_eq!(s, expected_repr);
    }

    /// Scenario: a Debug value overflows the available buffer.
    /// Guarantees: the value is truncated with a `[...]` suffix instead of
    /// being dropped entirely, `dropped_count` is incremented, and the
    /// encoded KeyValue is decodable with the suffix at the end.
    #[test]
    fn record_debug_overflow_truncates_with_suffix() {
        use otel_arrow_dfe_pdata::otlp::common::{BoundedBuf, StackProtoBuffer, TRUNCATION_SUFFIX};
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::KeyValue as ProtoKeyValue;
        use prost::Message;
        use tracing::field::Visit;

        // A Debug impl that writes far more than the buffer can hold.
        struct Huge;
        impl std::fmt::Debug for Huge {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                // 4 KiB of output into a 64-byte buffer.
                for _ in 0..512 {
                    f.write_str("xxxxxxxx")?;
                }
                Ok(())
            }
        }

        let meta = &DEBUG_TEST_METADATA;
        let fields = meta.fields();
        let dbg = fields.field("dbg").unwrap();

        let mut buf = StackProtoBuffer::<64>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_debug(&dbg, &Huge);
        assert_eq!(
            visitor.dropped_count(),
            1,
            "the single oversized debug field should be counted as dropped (truncated)"
        );

        // Buffer should contain a truncated KeyValue, not be empty.
        assert!(
            buf.len() > 0,
            "buffer should contain a truncated attribute, not be empty"
        );

        // Decode the KeyValue and verify the value ends with the suffix.
        let bytes = buf.as_ref().to_vec();
        let mut cursor = bytes.as_slice();
        let (tag, n) = read_varint(cursor);
        cursor = &cursor[n..];
        assert_eq!(tag >> 3, LOG_RECORD_ATTRIBUTES);
        assert_eq!(tag & 0x7, wire_types::LEN);
        let (len, n) = read_varint(cursor);
        cursor = &cursor[n..];
        let kv = ProtoKeyValue::decode(&cursor[..len as usize]).unwrap();
        assert_eq!(kv.key, "dbg");
        let s = match kv.value.as_ref().unwrap().value.as_ref().unwrap() {
            otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(s) => {
                s.clone()
            }
            other => panic!("expected StringValue, got {other:?}"),
        };
        let suffix = std::str::from_utf8(TRUNCATION_SUFFIX).unwrap();
        assert!(
            s.ends_with(suffix),
            "truncated debug value should end with {suffix}, got: {s}"
        );
        // The value should contain some content before the suffix.
        assert!(
            s.len() > suffix.len(),
            "truncated value should have content before the suffix"
        );
    }

    /// Scenario: a Debug value cannot fit even in truncated form because the
    /// buffer is too small for key + suffix + protobuf overhead.
    /// Guarantees: the attribute is hard-dropped (buffer rolled back to
    /// pre-call length) and `dropped_count` is incremented.
    #[test]
    fn record_debug_hard_drop_when_budget_too_small() {
        use otel_arrow_dfe_pdata::otlp::common::{BoundedBuf, StackProtoBuffer};
        use tracing::field::Visit;

        struct Tiny;
        impl std::fmt::Debug for Tiny {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("x")
            }
        }

        let meta = &DEBUG_TEST_METADATA;
        let fields = meta.fields();
        let dbg = fields.field("dbg").unwrap();

        // A buffer so tiny that even the key "dbg" + protobuf overhead
        // cannot fit, forcing a hard drop.
        let mut buf = StackProtoBuffer::<8>::default();
        let before_len = buf.len();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_debug(&dbg, &Tiny);
        assert_eq!(
            visitor.dropped_count(),
            1,
            "the field should be counted as dropped"
        );
        assert_eq!(
            buf.len(),
            before_len,
            "buffer must be rolled back to its pre-call length on hard drop"
        );
    }

    /// Scenario: a bare `&str` `message` body overflows the available buffer.
    /// Guarantees: the decoded body ends with `[...]` without incrementing
    /// OTLP `dropped_attributes_count`.
    #[test]
    fn record_str_message_overflow_truncates_with_suffix() {
        use otel_arrow_dfe_pdata::otlp::common::{StackProtoBuffer, TRUNCATION_SUFFIX};
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value;
        use prost::Message;
        use tracing::field::Visit;

        let meta = &MESSAGE_TEST_METADATA;
        let fields = meta.fields();
        let message = fields.field("message").unwrap();

        // 4 KiB of content into a 64-byte buffer.
        let huge = "x".repeat(4096);

        let mut buf = StackProtoBuffer::<64>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_str(&message, &huge);
        assert_eq!(
            visitor.dropped_count(),
            0,
            "body truncation must not be counted as a dropped attribute"
        );

        assert!(
            buf.len() > 0,
            "buffer should contain a truncated body, not be empty"
        );

        let record = LogRecord::decode(buf.as_ref()).expect("log record should decode");
        assert_eq!(record.dropped_attributes_count, 0);
        let s = match record.body.unwrap().value.unwrap() {
            Value::StringValue(s) => s,
            other => panic!("expected StringValue, got {other:?}"),
        };
        let suffix = std::str::from_utf8(TRUNCATION_SUFFIX).unwrap();
        assert!(
            s.ends_with(suffix),
            "truncated body should end with {suffix}, got: {s}"
        );
        assert!(
            s.len() > suffix.len(),
            "truncated body should have content before the suffix"
        );
    }

    /// Scenario: a Debug-formatted `message` body overflows the available buffer.
    /// Guarantees: the decoded body ends with `[...]` without incrementing
    /// OTLP `dropped_attributes_count`.
    #[test]
    fn record_debug_message_overflow_truncates_with_suffix() {
        use otel_arrow_dfe_pdata::otlp::common::{StackProtoBuffer, TRUNCATION_SUFFIX};
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value;
        use prost::Message;
        use tracing::field::Visit;

        struct Huge;
        impl std::fmt::Debug for Huge {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for _ in 0..512 {
                    f.write_str("xxxxxxxx")?;
                }
                Ok(())
            }
        }

        let fields = MESSAGE_TEST_METADATA.fields();
        let message = fields.field("message").unwrap();
        let mut buf = StackProtoBuffer::<64>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_debug(&message, &Huge);
        assert_eq!(
            visitor.dropped_count(),
            0,
            "body truncation must not be counted as a dropped attribute"
        );

        let record = LogRecord::decode(buf.as_ref()).expect("log record should decode");
        assert_eq!(record.dropped_attributes_count, 0);
        let s = match record.body.unwrap().value.unwrap() {
            Value::StringValue(s) => s,
            other => panic!("expected StringValue, got {other:?}"),
        };
        let suffix = std::str::from_utf8(TRUNCATION_SUFFIX).unwrap();
        assert!(
            s.ends_with(suffix),
            "truncated body should end with {suffix}, got: {s}"
        );
        assert!(
            s.len() > suffix.len(),
            "truncated body should have content before the suffix"
        );
    }

    static MESSAGE_TEST_CALLSITE: MessageTestCallsite = MessageTestCallsite;

    struct MessageTestCallsite;

    impl tracing::Callsite for MessageTestCallsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            &MESSAGE_TEST_METADATA
        }
    }

    static MESSAGE_TEST_METADATA: tracing::Metadata<'static> = tracing::Metadata::new(
        "message_test",
        "otel-arrow-dfe-telemetry",
        Level::INFO,
        Some(file!()),
        Some(line!()),
        Some(module_path!()),
        tracing::field::FieldSet::new(
            &["message"],
            tracing::callsite::Identifier(&MESSAGE_TEST_CALLSITE),
        ),
        tracing::metadata::Kind::EVENT,
    );

    /// Scenario: four Debug values compete for a 256-byte buffer with the
    /// halving budget policy.
    /// Guarantees: all four values are truncated (each ends with `[...]`),
    /// each successive value is shorter than the previous, and the buffer
    /// does not exceed the inline limit.
    #[test]
    fn record_debug_halves_remaining_budget_across_attributes() {
        use otel_arrow_dfe_pdata::otlp::common::TRUNCATION_SUFFIX;
        use otel_arrow_dfe_pdata::otlp::common::{BoundedBuf, StackProtoBuffer};
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::KeyValue as ProtoKeyValue;
        use prost::Message;
        use tracing::field::Visit;

        const INLINE: usize = 256;

        // A Debug impl that writes 1000 bytes.
        struct Big;
        impl std::fmt::Debug for Big {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for _ in 0..125 {
                    f.write_str("xxxxxxxx")?;
                }
                Ok(())
            }
        }

        let meta = &BUDGET_TEST_METADATA;
        let fields = meta.fields();
        let a = fields.field("a").unwrap();
        let b = fields.field("b").unwrap();
        let c = fields.field("c").unwrap();
        let d = fields.field("d").unwrap();

        let mut buf = StackProtoBuffer::<INLINE>::default();
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor.record_debug(&a, &Big);
        visitor.record_debug(&b, &Big);
        visitor.record_debug(&c, &Big);
        visitor.record_debug(&d, &Big);
        let dropped = visitor.dropped_count();

        assert_eq!(dropped, 4, "expected 4 truncated attributes");
        assert!(
            buf.len() <= INLINE,
            "buffer len {} exceeds INLINE {}",
            buf.len(),
            INLINE
        );

        // Decode each attribute.
        let bytes = buf.as_ref().to_vec();
        let mut cursor = bytes.as_slice();
        let mut decoded: Vec<(String, String)> = Vec::new();
        while !cursor.is_empty() {
            let (tag, n) = read_varint(cursor);
            cursor = &cursor[n..];
            let field_num = tag >> 3;
            let wire_type = tag & 0x7;
            assert_eq!(wire_type, wire_types::LEN, "expected LEN wire type");
            assert_eq!(field_num, LOG_RECORD_ATTRIBUTES, "expected ATTRIBUTES tag");
            let (len, n) = read_varint(cursor);
            cursor = &cursor[n..];
            let len = len as usize;
            let kv_bytes = &cursor[..len];
            cursor = &cursor[len..];
            let kv = ProtoKeyValue::decode(kv_bytes).unwrap();
            let val = kv
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .map(|v| match v {
                    otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value::StringValue(s) => s.clone(),
                    _ => panic!("expected string value"),
                })
                .unwrap();
            decoded.push((kv.key, val));
        }

        assert_eq!(decoded.len(), 4, "expected 4 decoded attributes");

        let suffix = std::str::from_utf8(TRUNCATION_SUFFIX).unwrap();
        for (k, v) in &decoded {
            assert!(
                v.ends_with(suffix),
                "attribute {k} value should end with {suffix}, got len {}",
                v.len()
            );
        }

        // Each successive value should be no longer than the previous one
        // as the remaining budget is halved across attributes.
        let lens: Vec<usize> = decoded.iter().map(|(_, v)| v.len()).collect();
        assert!(
            lens.windows(2).all(|w| w[0] >= w[1]),
            "expected non-increasing value lengths, got {lens:?}"
        );
    }

    static DEBUG_TEST_CALLSITE: DebugTestCallsite = DebugTestCallsite;

    struct DebugTestCallsite;

    impl tracing::Callsite for DebugTestCallsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            &DEBUG_TEST_METADATA
        }
    }

    static DEBUG_TEST_METADATA: tracing::Metadata<'static> = tracing::Metadata::new(
        "debug_test",
        "otel-arrow-dfe-telemetry",
        Level::INFO,
        Some(file!()),
        Some(line!()),
        Some(module_path!()),
        tracing::field::FieldSet::new(
            &["dbg"],
            tracing::callsite::Identifier(&DEBUG_TEST_CALLSITE),
        ),
        tracing::metadata::Kind::EVENT,
    );

    /// Read an unsigned varint, returning (value, bytes_consumed).
    fn read_varint(bytes: &[u8]) -> (u64, usize) {
        let mut value = 0u64;
        let mut shift = 0;
        for (i, &b) in bytes.iter().enumerate() {
            value |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return (value, i + 1);
            }
            shift += 7;
        }
        panic!("truncated varint");
    }

    struct Probe(i64);

    impl StructuredValue for Probe {
        fn write(&self, w: &mut dyn ValueWriter) {
            w.map(|m| {
                m.entry("count", |v| v.write_i64(self.0));
                m.entry("tags", |v| {
                    v.array(|a| {
                        for tag in ["a", "b"] {
                            a.element(|v| v.write_str(tag));
                        }
                    })
                });
            });
        }
    }

    /// Scenario: a boxed map/array value is mutated between events and encoded under small and roomy budgets.
    /// Guarantees: logging borrows current state, preserves wire types, stays within each budget, and counts partial values once.
    #[test]
    fn boxed_values_preserve_mutation_and_budgets() {
        use tracing::field::Visit;

        let mut slot = StructBox::new(Probe(0));
        let field = BUDGET_TEST_METADATA
            .fields()
            .field("a")
            .expect("known field");
        for count in [1, 2] {
            slot.0 = count;
            let record = __log_record_impl!(Level::INFO, "test.structured", a = slot.as_struct())
                .into_record(LogContext::new());
            assert_eq!(record.dropped_attributes_count, 0);
            let expected =
                LogRecord::decode(record.body_attrs_bytes.as_ref()).expect("valid record");
            let Some(AnyValueKind::KvlistValue(map)) =
                &expected.attributes[0].value.as_ref().expect("value").value
            else {
                panic!("expected map");
            };
            assert_eq!(
                map.values[0],
                KeyValue::new("count", AnyValue::new_int(count))
            );
            for limit in [8, 32, 64, 128, 16384] {
                let mut buf = ProtoBuffer::with_capacity_and_limit(limit, limit);
                let mut visitor = DirectFieldVisitor::new(&mut buf);
                visitor.record_error(&field, slot.as_struct());
                assert_eq!(
                    visitor.dropped_count(),
                    u32::from(limit < 16384),
                    "limit {limit}"
                );
                assert!(buf.len() <= limit);
                let decoded = LogRecord::decode(buf.as_ref()).expect("valid bounded record");
                if limit == 16384 {
                    assert_eq!(decoded, expected);
                }
            }
        }
    }

    /// Scenario: scalar and structured `message` fields write AnyValues.
    /// Guarantees: every non-empty message value uses the body; an empty string is omitted.
    #[test]
    fn message_values_are_log_body() {
        let empty = __log_record_impl!(Level::INFO, "test.empty_message", message = "")
            .into_record(LogContext::new());
        let decoded = LogRecord::decode(empty.body_attrs_bytes).expect("valid empty record");
        assert_eq!(decoded.body, None);

        let scalar = __log_record_impl!(Level::INFO, "test.scalar_message", message = 7i64)
            .into_record(LogContext::new());
        let decoded = LogRecord::decode(scalar.body_attrs_bytes).expect("valid scalar record");
        assert!(decoded.attributes.is_empty());
        assert_eq!(decoded.body, Some(AnyValue::new_int(7)));

        let slot = StructBox::new(Probe(7));
        let record = __log_record_impl!(
            Level::INFO,
            "test.structured_message",
            message = slot.as_struct()
        )
        .into_record(LogContext::new());
        let decoded = LogRecord::decode(record.body_attrs_bytes).expect("valid record");

        assert!(decoded.attributes.is_empty());
        assert_eq!(
            decoded.body,
            Some(AnyValue::new_kvlist(vec![
                KeyValue::new("count", AnyValue::new_int(7)),
                KeyValue::new(
                    "tags",
                    AnyValue::new_array(
                        vec![AnyValue::new_string("a"), AnyValue::new_string("b"),]
                    ),
                ),
            ]))
        );
    }

    fn encode_partial_with_tail(limit: usize, f: impl FnOnce(&mut dyn ValueWriter)) -> AnyValue {
        use tracing::field::Visit;

        let mut buf = ProtoBuffer::with_capacity_and_limit(limit, limit);
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        visitor
            .write_attribute(|buf| write_key_value(buf, LOG_RECORD_ATTRIBUTES, "a", 0, |w| f(w)));
        visitor.record_bool(
            &BUDGET_TEST_METADATA
                .fields()
                .field("b")
                .expect("known field"),
            true,
        );
        assert_eq!(visitor.dropped_count(), 1);
        assert!(buf.len() <= limit);
        let mut attributes = LogRecord::decode(buf.as_ref())
            .expect("valid record")
            .attributes;
        assert_eq!(
            attributes.pop(),
            Some(KeyValue::new("b", AnyValue::new_bool(true)))
        );
        assert_eq!(attributes.len(), 1);
        let first = attributes.pop().expect("partial attribute");
        assert_eq!(first.key, "a");
        first.value.expect("partial value")
    }

    /// Scenario: a map key or array element cannot fit although a later small member would fit.
    /// Guarantees: both containers retain their prefix, skip later callbacks, count loss once, and allow the next top-level attribute.
    #[test]
    fn containers_retain_prefix_after_hard_drop() {
        for (array, limit) in [(false, 128), (true, 64)] {
            let value = encode_partial_with_tail(limit, |w| {
                if array {
                    w.array(|a| {
                        a.element(|v| v.write_bool(true));
                        a.element(|v| v.write_i64(-1));
                        a.element(|_| panic!("array stopped before this element"));
                    });
                } else {
                    w.map(|m| {
                        m.entry("first", |v| v.write_bool(true));
                        m.entry(&"x".repeat(256), |_| {
                            panic!("oversized key skips its value")
                        });
                        m.entry("last", |_| panic!("map stopped before this entry"));
                    });
                }
            });
            let expected = if array {
                AnyValue::new_array(vec![AnyValue::new_bool(true)])
            } else {
                AnyValue::new_kvlist(vec![KeyValue::new("first", AnyValue::new_bool(true))])
            };
            assert_eq!(value, expected);
        }
    }

    /// Scenario: a map or array string is truncated, leaving space for another member.
    /// Guarantees: partial UTF-8 strings and later members survive, with one loss count for the containing attribute.
    #[test]
    fn partial_members_do_not_stop_containers() {
        let large = "\u{1f642}".repeat(1000);
        for array in [false, true] {
            let value = encode_partial_with_tail(256, |w| {
                if array {
                    w.array(|a| {
                        a.element(|v| v.write_str(&large));
                        a.element(|v| v.write_bool(true));
                    });
                } else {
                    w.map(|m| {
                        m.entry("first", |v| v.write_str(&large));
                        m.entry("last", |v| v.write_bool(true));
                    });
                }
            });
            let members = match value.value.expect("container") {
                AnyValueKind::ArrayValue(a) => a.values,
                AnyValueKind::KvlistValue(m) => m
                    .values
                    .into_iter()
                    .map(|kv| kv.value.expect("value"))
                    .collect::<Vec<_>>(),
                _ => panic!("expected container"),
            };
            assert_eq!(members.len(), 2);
            let Some(AnyValueKind::StringValue(first)) = &members[0].value else {
                panic!("expected string");
            };
            assert!(first.ends_with("[...]"));
            assert_ne!(first, &large);
            assert_eq!(members[1], AnyValue::new_bool(true));
        }
    }

    /// Scenario: a top-level key is too large for its budget in a log or resource.
    /// Guarantees: the failed attribute is rolled back and a later small attribute still fits.
    #[test]
    fn top_level_attribute_budgets_are_independent() {
        for field in [LOG_RECORD_ATTRIBUTES, RESOURCE_ATTRIBUTES] {
            let mut buf = ProtoBuffer::with_capacity_and_limit(64, 64);
            assert!(encode_attribute(&mut buf, field, &"x".repeat(64), |_| {
                panic!("oversized key skips its value");
            }));
            assert!(buf.is_empty());
            assert!(!encode_attribute(&mut buf, field, "small", |w| w.write_bool(true)));
            let attributes = match field {
                LOG_RECORD_ATTRIBUTES => LogRecord::decode(buf.as_ref()).expect("log").attributes,
                RESOURCE_ATTRIBUTES => Resource::decode(buf.as_ref()).expect("resource").attributes,
                _ => unreachable!(),
            };
            assert_eq!(
                attributes,
                vec![KeyValue::new("small", AnyValue::new_bool(true))]
            );
        }
    }

    fn write_chain(w: &mut dyn ValueWriter, depth: usize, visited: &mut usize) {
        if depth == 0 {
            w.write_bool(true);
        } else {
            w.map(|m| {
                *visited += 1;
                m.entry("child", |v| write_chain(v, depth - 1, visited));
                m.entry("tail", |v| v.write_bool(true));
            });
        }
    }

    /// Scenario: maps reach the exact depth boundary with enough byte budget for every level.
    /// Guarantees: MAX_DEPTH maps and their scalar leaf fit; deeper callbacks are skipped and the retained prefix is counted once.
    #[test]
    fn structured_depth_limit_is_independent_of_byte_budget() {
        for depth in [0, MAX_DEPTH - 1, MAX_DEPTH, MAX_DEPTH + 1] {
            let mut buf = ProtoBuffer::with_capacity(256);
            let mut visited = 0;
            let lost = encode_attribute(&mut buf, LOG_RECORD_ATTRIBUTES, "chain", |w| {
                write_chain(w, depth, &mut visited);
            });
            let truncated = depth > MAX_DEPTH;
            assert_eq!(lost, truncated);
            assert_eq!(visited, depth.min(MAX_DEPTH));
            let mut expected = if truncated {
                AnyValue::new_kvlist(vec![])
            } else {
                AnyValue::new_bool(true)
            };
            for _ in 0..depth.min(MAX_DEPTH) - usize::from(truncated) {
                expected = AnyValue::new_kvlist(vec![
                    KeyValue::new("child", expected),
                    KeyValue::new("tail", AnyValue::new_bool(true)),
                ]);
            }
            let record = LogRecord::decode(buf.as_ref()).expect("valid depth-limited value");
            assert_eq!(record.attributes, vec![KeyValue::new("chain", expected)]);
        }
    }

    /// Scenario: a resource array exceeds its budget, or its outer framing cannot fit.
    /// Guarantees: a valid prefix reports one lost attribute; framing failures return an error and roll back.
    #[test]
    fn resource_overflow_reports_loss_or_framing_failure() {
        let values: Vec<i64> = (-10..0).collect();
        let resource = HashMap::from([(
            "array".to_owned(),
            ConfigAttributeValue::Array(ConfigAttributeValueArray::I64(values.clone())),
        )]);
        for limit in [0, 1, 2, 3, 128] {
            let mut buf = ProtoBuffer::with_capacity_and_limit(limit, limit);
            let result = write_config_resource_field(&mut buf, &resource);
            if limit < 4 {
                assert_eq!(result, Err(EncodeFailure::Dropped));
                assert!(buf.is_empty());
                continue;
            }
            result.expect("resource framing fits");
            assert!(buf.len() <= limit);
            let resource = ResourceLogs::decode(buf.as_ref())
                .expect("valid fragment")
                .resource
                .expect("resource");
            assert_eq!(resource.dropped_attributes_count, 1);
            assert_eq!(resource.attributes.len(), 1);
            let Some(AnyValueKind::ArrayValue(array)) =
                &resource.attributes[0].value.as_ref().expect("value").value
            else {
                panic!("expected array");
            };
            assert!(!array.values.is_empty());
            assert!(array.values.len() < values.len());
            assert_eq!(
                array.values,
                values
                    .iter()
                    .take(array.values.len())
                    .map(|v| AnyValue::new_int(*v))
                    .collect::<Vec<_>>(),
            );
        }
    }

    fn deep_scope_map(depth: usize) -> BTreeMap<String, AttributeValue> {
        let mut value = AttributeValue::Boolean(true);
        for _ in 0..depth {
            value = AttributeValue::Map(BTreeMap::from([("child".to_owned(), value)]));
        }
        let AttributeValue::Map(map) = value else {
            panic!("expected nonzero depth");
        };
        map
    }

    /// Scenario: scope attributes reach or exceed the depth limit before their entity is unregistered.
    /// Guarantees: incomplete scopes fail export without being cached; complete cached bytes survive unregistration.
    #[test]
    fn scope_cache_accepts_only_complete_attributes() {
        let registry = TelemetryRegistryHandle::new();
        let mut cache = ScopeToBytesMap::new(registry.clone());
        for depth in [MAX_DEPTH, MAX_DEPTH + 1] {
            let key = registry.register_entity(TestMapAttributes::new(deep_scope_map(depth)));
            let mut events = [LogEvent {
                time: SystemTime::UNIX_EPOCH,
                record: __log_record_impl!(Level::INFO, "test.scope.depth")
                    .into_record(LogContext::from_buf([key])),
            }];
            let mut buf = ProtoBuffer::with_capacity(256);
            let result =
                encode_export_logs_request(&mut buf, &mut events, &Bytes::new(), &mut cache);
            assert!(registry.unregister_entity(key));
            if depth > MAX_DEPTH {
                assert_eq!(result, Err(EncodeFailure::Dropped));
                assert!(matches!(
                    cache.get_or_encode(key),
                    Err(MapError::NotRegistered)
                ));
                continue;
            }
            result.expect("complete scope fits");
            let request = ExportLogsServiceRequest::decode(buf.as_ref()).expect("valid request");
            let scope_logs = &request.resource_logs[0].scope_logs[0];
            let scope = scope_logs.scope.as_ref().expect("scope");
            assert_eq!(scope.dropped_attributes_count, 0);
            assert_eq!(scope.attributes.len(), 1);
            assert_eq!(scope_logs.log_records.len(), 1);
            encode_export_logs_request(&mut buf, &mut events, &Bytes::new(), &mut cache)
                .expect("cached bytes survive unregistration");
            assert_eq!(
                ExportLogsServiceRequest::decode(buf.as_ref()).expect("valid cache hit"),
                request
            );
        }
    }

    static BUDGET_TEST_CALLSITE: BudgetTestCallsite = BudgetTestCallsite;

    struct BudgetTestCallsite;

    impl tracing::Callsite for BudgetTestCallsite {
        fn set_interest(&self, _: tracing::subscriber::Interest) {}
        fn metadata(&self) -> &tracing::Metadata<'_> {
            &BUDGET_TEST_METADATA
        }
    }

    static BUDGET_TEST_METADATA: tracing::Metadata<'static> = tracing::Metadata::new(
        "budget_test",
        "otel-arrow-dfe-telemetry",
        Level::INFO,
        Some(file!()),
        Some(line!()),
        Some(module_path!()),
        tracing::field::FieldSet::new(
            &["a", "b", "c", "d"],
            tracing::callsite::Identifier(&BUDGET_TEST_CALLSITE),
        ),
        tracing::metadata::Kind::EVENT,
    );
}
