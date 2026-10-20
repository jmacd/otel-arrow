// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Verify suppression happens before subscribers receive operation diagnostics.

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    KeyValue, any_value::Value as AnyValueKind,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::LogRecord as ProtoLogRecord;
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogRecord;
use otel_arrow_dfe_pdata_views::views::common::{AnyValueView, AttributeView, ValueType};
use otel_arrow_dfe_pdata_views::views::logs::LogRecordView;
use otel_arrow_dfe_telemetry::diagnostics::{DiagnosticErrorKind, DiagnosticTracker};
use otel_arrow_dfe_telemetry::self_tracing::encoder::level_to_severity_number;
use otel_arrow_dfe_telemetry::self_tracing::{
    LOG_ARGUMENTS_ENCODE_LIMIT, LogContext, LogRecord, MapWriter, StructBox, StructuredValue,
    ValueWriter,
};
use prost::Message;
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<(&'static str, &'static str, Level)>>>);

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        self.0
            .lock()
            .expect("capture lock must not be poisoned")
            .push((metadata.name(), metadata.target(), *metadata.level()));
    }
}

#[derive(Clone, Default)]
struct EncodedCapture(Arc<Mutex<Vec<LogRecord>>>);

impl<S: Subscriber> Layer<S> for EncodedCapture {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let record = LogRecord::new(event, LogContext::new());
        self.0
            .lock()
            .expect("capture lock must not be poisoned")
            .push(record);
    }
}

struct SavedRecord(LogRecord);

impl StructuredValue for SavedRecord {
    fn write(&self, writer: &mut dyn ValueWriter) {
        let callsite = self.0.callsite();
        let record = RawLogRecord::new(&self.0.body_attrs_bytes);
        writer.map(|map| {
            map.entry("event_name", |value| value.write_str(callsite.name()));
            map.entry("severity_number", |value| {
                value.write_i64(i64::from(level_to_severity_number(callsite.level())));
            });
            map.entry("scope", |value| value.write_str(callsite.target()));
            if let Some(body) = record.body() {
                map.entry("body", |value| write_any_value(value, &body));
            }
            map.entry("attributes", |value| {
                value.map(|map| write_attributes(map, record.attributes()));
            });
            map.entry("dropped_attributes_count", |value| {
                value.write_i64(i64::from(self.0.dropped_attributes_count));
            });
        });
    }
}

fn write_attributes<A: AttributeView>(
    writer: &mut dyn MapWriter,
    attributes: impl Iterator<Item = A>,
) {
    for attribute in attributes {
        let key = std::str::from_utf8(attribute.key()).expect("attribute key must be UTF-8");
        writer.entry(key, |writer| {
            if let Some(value) = attribute.value() {
                write_any_value(writer, &value);
            }
        });
    }
}

fn write_any_value<'a, V: AnyValueView<'a>>(writer: &mut dyn ValueWriter, value: &V) {
    match value.value_type() {
        ValueType::Empty => {}
        ValueType::String => writer.write_str(
            std::str::from_utf8(value.as_string().expect("string value must exist"))
                .expect("string value must be UTF-8"),
        ),
        ValueType::Bool => writer.write_bool(value.as_bool().expect("bool value must exist")),
        ValueType::Int64 => writer.write_i64(value.as_int64().expect("int value must exist")),
        ValueType::Double => writer.write_f64(value.as_double().expect("double value must exist")),
        // StructuredValue has no bytes variant; saved diagnostics omit bytes values.
        ValueType::Bytes => {}
        ValueType::Array => writer.array(|array| {
            for child in value.as_array().expect("array value must exist") {
                array.element(|writer| write_any_value(writer, &child));
            }
        }),
        ValueType::KeyValueList => writer.map(|map| {
            write_attributes(map, value.as_kvlist().expect("key-value list must exist"));
        }),
    }
}

fn map_value<'a>(values: &'a [KeyValue], key: &str) -> &'a AnyValueKind {
    values
        .iter()
        .find(|value| value.key == key)
        .and_then(|value| value.value.as_ref())
        .and_then(|value| value.value.as_ref())
        .expect("map entry must exist")
}

/// Scenario: diagnostics retain an encoded log record in a reusable structured box.
/// Guarantees: a later diagnostic emits the original event metadata, body, and attributes as one nested AnyValue map.
#[test]
fn saved_log_record_encodes_as_structured_diagnostic_field() {
    let capture = EncodedCapture::default();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(capture.clone()), || {
        otel_arrow_dfe_telemetry::otel_warn!(
            "test.original_failure",
            message = "connection refused",
            endpoint = "collector:4317"
        );
        let original = capture
            .0
            .lock()
            .expect("capture lock must not be poisoned")
            .remove(0);
        let saved: StructBox<SavedRecord> = StructBox::new(SavedRecord(original));
        otel_arrow_dfe_telemetry::otel_warn!(
            "test.diagnostic_summary",
            previous = saved.as_struct()
        );
    });

    let events = capture.0.lock().expect("capture lock must not be poisoned");
    assert_eq!(events.len(), 1);
    let diagnostic =
        ProtoLogRecord::decode(events[0].body_attrs_bytes.clone()).expect("valid diagnostic");
    let previous = diagnostic
        .attributes
        .iter()
        .find(|attribute| attribute.key == "previous")
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| value.value.as_ref())
        .expect("structured previous record");
    let AnyValueKind::KvlistValue(previous) = previous else {
        panic!("previous record must be a map");
    };

    assert_eq!(
        map_value(&previous.values, "event_name"),
        &AnyValueKind::StringValue("test.original_failure".to_owned())
    );
    assert_eq!(
        map_value(&previous.values, "severity_number"),
        &AnyValueKind::IntValue(13)
    );
    assert_eq!(
        map_value(&previous.values, "body"),
        &AnyValueKind::StringValue("connection refused".to_owned())
    );
    let AnyValueKind::KvlistValue(attributes) = map_value(&previous.values, "attributes") else {
        panic!("saved attributes must be a map");
    };
    assert_eq!(
        map_value(&attributes.values, "endpoint"),
        &AnyValueKind::StringValue("collector:4317".to_owned())
    );
    assert_eq!(
        map_value(&previous.values, "dropped_attributes_count"),
        &AnyValueKind::IntValue(0)
    );
}

/// Scenario: Two subscribers observe a busy outage, a summary, and confirmed recovery.
/// Guarantees: Both receive only bounded reports with the component target and correct severity.
#[test]
fn suppression_precedes_all_subscribers() {
    let first = Capture::default();
    let second = Capture::default();
    let subscriber = tracing_subscriber::registry()
        .with(first.clone())
        .with(second.clone());
    let formats = Cell::new(0);
    tracing::subscriber::with_default(subscriber, || {
        let start = Instant::now();
        let mut tracker = DiagnosticTracker::default();
        for second in 0..=60 {
            for _ in 0..100 {
                if let Some(report) = tracker.failure(
                    start + Duration::from_secs(second),
                    DiagnosticErrorKind::Transport,
                    || {
                        formats.set(formats.get() + 1);
                        "connection refused"
                    },
                ) {
                    otel_arrow_dfe_telemetry::otel_diagnostic_report!(
                        target: "otel.exporter.test", emit: otel_warn,
                        name: "test.export_error", report: &report,
                        stage = "delivery", signal = "logs"
                    );
                }
            }
        }
        let report = tracker
            .success(
                start + Duration::from_secs(61),
                start + Duration::from_secs(90),
            )
            .expect("fresh success confirms recovery");
        otel_arrow_dfe_telemetry::otel_diagnostic_report!(
            target: "otel.exporter.test", emit: otel_info,
            name: "test.export_recovered", report: &report,
            stage = "delivery", signal = "logs"
        );
    });
    let expected = vec![
        ("test.export_error", "otel.exporter.test", Level::WARN),
        ("test.export_error", "otel.exporter.test", Level::WARN),
        ("test.export_recovered", "otel.exporter.test", Level::INFO),
    ];
    assert_eq!(
        *first.0.lock().expect("capture lock must not be poisoned"),
        expected
    );
    assert_eq!(
        *second.0.lock().expect("capture lock must not be poisoned"),
        expected
    );
    assert_eq!(formats.get(), 2);
}

/// Scenario: A diagnostic's retained detail fits the grown bounded ITS event buffer.
/// Guarantees: Priority context and the complete bounded error body survive real ITS encoding.
#[test]
fn priority_detail_survives_bounded_its_encoding() {
    let capture = EncodedCapture::default();
    let expected_body = tracing::subscriber::with_default(
        tracing_subscriber::registry().with(capture.clone()),
        || {
            let mut tracker = DiagnosticTracker::default();
            let report = tracker
                .failure(Instant::now(), DiagnosticErrorKind::Transport, || {
                    format!("root cause: {}", "x".repeat(LOG_ARGUMENTS_ENCODE_LIMIT * 2))
                })
                .expect("first failure must produce a report");
            let expected_body = report.detail.clone();
            otel_arrow_dfe_telemetry::otel_diagnostic_report!(
                target: "otel.exporter.test", emit: otel_warn,
                name: "test.export_error", report: &report,
                signal = "logs", retryable = true,
                diagnostic_kind = "first_failure", message = %report.detail.as_str()
            );
            expected_body
        },
    );

    let events = capture.0.lock().expect("capture lock must not be poisoned");
    assert_eq!(events.len(), 1);
    let captured = &events[0];
    let record = RawLogRecord::new(&captured.body_attrs_bytes);
    let body_value = record
        .body()
        .expect("diagnostic error body must survive encoding");
    let body = std::str::from_utf8(
        body_value
            .as_string()
            .expect("diagnostic error body must be a string"),
    )
    .expect("diagnostic error body must be valid UTF-8");
    assert_eq!(body, expected_body);

    let attribute_keys = record
        .attributes()
        .map(|attribute| String::from_utf8_lossy(attribute.key()).into_owned())
        .collect::<Vec<_>>();
    for required in ["signal", "retryable", "diagnostic_kind"] {
        assert!(
            attribute_keys.iter().any(|key| key == required),
            "missing priority attribute {required}"
        );
    }
    assert_eq!(
        captured.dropped_attributes_count, 0,
        "the grown event buffer must preserve all diagnostic fields"
    );
}
