// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Stamping flushed records with the three adjusted-count attributes.
//!
//! The integrated sampler's output is the ordinary OpenTelemetry logs data
//! model with three adjusted-count attributes carried as log attributes, one
//! per population. A consumer that ignores them still sees valid logs; one that
//! understands them recovers unbiased totals. All three are written on every
//! flushed record, including exact zeros, because a zero is the explicit
//! statement that the record is present but not a member of that population.

use otap_df_pdata::otlp::common::{BoundedBuf, ProtoBuffer};

use super::super::LogRecord;
use super::super::encoder::append_double_attribute;
use super::{
    ATTR_LOGS_ADJUSTED_COUNT, ATTR_SPAN_LOGS_ADJUSTED_COUNT, ATTR_TRACES_ADJUSTED_COUNT,
};

/// Headroom, in bytes, reserved for the three appended double attributes. Each
/// is a key string of at most about thirty bytes plus a fixed-width double, so
/// this is comfortably sufficient.
const ADJUSTED_COUNT_HEADROOM: usize = 256;

/// Return a copy of `record` with the three adjusted-count attributes appended.
///
/// The counts are appended to the record's pre-encoded body-and-attributes
/// bytes as additional `LogRecord.attributes` entries, which is valid because
/// the bytes are emitted inline within the `LogRecord` message and protobuf
/// repeated fields may be concatenated.
#[must_use]
pub fn annotate_log_record(
    record: &LogRecord,
    logs_adjusted_count: f64,
    traces_adjusted_count: f64,
    span_logs_adjusted_count: f64,
) -> LogRecord {
    let existing = record.body_attrs_bytes.as_ref();
    let capacity = existing.len() + ADJUSTED_COUNT_HEADROOM;
    let mut buf = ProtoBuffer::with_capacity_and_limit(capacity, capacity);
    let _ = buf.extend_from_slice(existing);
    let _ = append_double_attribute(&mut buf, ATTR_LOGS_ADJUSTED_COUNT, logs_adjusted_count);
    let _ = append_double_attribute(&mut buf, ATTR_TRACES_ADJUSTED_COUNT, traces_adjusted_count);
    let _ = append_double_attribute(
        &mut buf,
        ATTR_SPAN_LOGS_ADJUSTED_COUNT,
        span_logs_adjusted_count,
    );

    LogRecord {
        callsite_id: record.callsite_id.clone(),
        body_attrs_bytes: buf.into_bytes(),
        dropped_attributes_count: record.dropped_attributes_count,
        context: record.context.clone(),
        trace: record.trace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::__log_record_impl;
    use crate::self_tracing::LogContext;
    use tracing::Level;

    #[test]
    fn appends_three_double_attributes() {
        let record = __log_record_impl!(Level::INFO, "sample.test").into_record(LogContext::new());
        let before = record.body_attrs_bytes.len();
        let annotated = annotate_log_record(&record, 50.0, 0.0, 10.0);
        // The annotated record carries strictly more attribute bytes.
        assert!(annotated.body_attrs_bytes.len() > before);
        // Decoding the OTLP bytes should surface the three attribute keys with
        // their double values, including the exact zero.
        let decoded = decode_double_attrs(annotated.body_attrs_bytes.as_ref());
        assert_eq!(decoded.get(ATTR_LOGS_ADJUSTED_COUNT), Some(&50.0));
        assert_eq!(decoded.get(ATTR_TRACES_ADJUSTED_COUNT), Some(&0.0));
        assert_eq!(decoded.get(ATTR_SPAN_LOGS_ADJUSTED_COUNT), Some(&10.0));
    }

    /// Minimal decoder pulling double-valued `LogRecord.attributes` entries out
    /// of the partial body-and-attributes bytes, for test assertions.
    fn decode_double_attrs(bytes: &[u8]) -> std::collections::HashMap<String, f64> {
        use std::collections::HashMap;
        const LOG_RECORD_ATTRIBUTES: u64 = 6;
        const KEY_VALUE_KEY: u64 = 1;
        const KEY_VALUE_VALUE: u64 = 2;
        const ANY_VALUE_DOUBLE_VALUE: u64 = 4;

        let mut out = HashMap::new();
        let mut i = 0usize;
        // Read a varint, advancing the cursor.
        fn varint(bytes: &[u8], i: &mut usize) -> u64 {
            let mut shift = 0;
            let mut value = 0u64;
            while *i < bytes.len() {
                let b = bytes[*i];
                *i += 1;
                value |= u64::from(b & 0x7f) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            value
        }

        while i < bytes.len() {
            let tag = varint(bytes, &mut i);
            let field = tag >> 3;
            let wire = tag & 0x7;
            if field == LOG_RECORD_ATTRIBUTES && wire == 2 {
                let len = varint(bytes, &mut i) as usize;
                let end = i + len;
                let mut key = String::new();
                let mut value: Option<f64> = None;
                while i < end {
                    let inner_tag = varint(bytes, &mut i);
                    let inner_field = inner_tag >> 3;
                    let inner_wire = inner_tag & 0x7;
                    if inner_field == KEY_VALUE_KEY && inner_wire == 2 {
                        let klen = varint(bytes, &mut i) as usize;
                        key = String::from_utf8_lossy(&bytes[i..i + klen]).into_owned();
                        i += klen;
                    } else if inner_field == KEY_VALUE_VALUE && inner_wire == 2 {
                        let vlen = varint(bytes, &mut i) as usize;
                        let vend = i + vlen;
                        while i < vend {
                            let any_tag = varint(bytes, &mut i);
                            if (any_tag >> 3) == ANY_VALUE_DOUBLE_VALUE && (any_tag & 0x7) == 1 {
                                let mut b = [0u8; 8];
                                b.copy_from_slice(&bytes[i..i + 8]);
                                value = Some(f64::from_le_bytes(b));
                                i += 8;
                            } else {
                                i = vend;
                            }
                        }
                    } else {
                        // Skip an unexpected field within the KeyValue.
                        if inner_wire == 2 {
                            let skip = varint(bytes, &mut i) as usize;
                            i += skip;
                        }
                    }
                }
                if let Some(v) = value {
                    let _ = out.insert(key, v);
                }
                i = end;
            } else if wire == 2 {
                let len = varint(bytes, &mut i) as usize;
                i += len;
            } else if wire == 0 {
                let _ = varint(bytes, &mut i);
            } else if wire == 1 {
                i += 8;
            } else if wire == 5 {
                i += 4;
            } else {
                break;
            }
        }
        out
    }
}
