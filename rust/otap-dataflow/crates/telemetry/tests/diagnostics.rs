// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Verify sampling happens before constructing engine diagnostic records.

use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogRecord;
use otel_arrow_dfe_pdata_views::views::common::{AnyValueView, AttributeView};
use otel_arrow_dfe_pdata_views::views::logs::LogRecordView;
use otel_arrow_dfe_telemetry::diagnostics::{DiagnosticErrorKind, SignalDiagnostics};
use otel_arrow_dfe_telemetry::event::{LogEvent, ObservedEvent, ObservedEventReporter};
use otel_arrow_dfe_telemetry::self_tracing::LogContext;
use otel_arrow_dfe_telemetry::tracing_init::{ProviderSetup, TracingSetup};
use std::cell::Cell;
use std::time::{Duration, Instant};
use tracing::Level;

fn counted_message(formats: &Cell<u64>) -> &'static str {
    formats.set(formats.get() + 1);
    "connection refused"
}

/// Scenario: A busy outage produces a first warning, summary, and recovery.
/// Guarantees: Only selected details are formatted, and recovery retains the sampled error.
#[test]
fn suppression_precedes_record_construction() {
    let (sender, receiver) = flume::unbounded();
    let setup = TracingSetup::new(
        ProviderSetup::InternalAsync {
            reporter: ObservedEventReporter::new(
                otel_arrow_dfe_config::observed_state::SendPolicy::default(),
                sender,
            ),
        },
        otel_arrow_dfe_config::settings::telemetry::logs::LogLevel::default(),
        LogContext::new,
    );
    let formats = Cell::new(0);
    setup.with_subscriber(|| {
        let start = Instant::now();
        let mut diagnostics = SignalDiagnostics::new(setup.log_emitter());
        for second in 0..=60 {
            for _ in 0..100 {
                otel_arrow_dfe_telemetry::otel_summary_warn!(
                    target: "otel.exporter.test",
                    at: start + Duration::from_secs(second),
                    &mut diagnostics,
                    otel_arrow_dfe_config::SignalType::Logs,
                    DiagnosticErrorKind::Transport,
                    "test.export_error",
                    stage = "delivery",
                    message = counted_message(&formats)
                );
            }
        }
        otel_arrow_dfe_telemetry::otel_summary_recover!(
            target: "otel.exporter.test",
            at: start + Duration::from_secs(90),
            &mut diagnostics,
            otel_arrow_dfe_config::SignalType::Logs,
            start + Duration::from_secs(61),
            "test.export_recovered",
            stage = "delivery"
        );
    });
    let expected = vec![
        ("test.export_error", "otel.exporter.test", Level::WARN),
        ("test.export_error", "otel.exporter.test", Level::WARN),
        ("test.export_recovered", "otel.exporter.test", Level::INFO),
    ];
    let actual = receiver
        .try_iter()
        .map(|event| match event {
            ObservedEvent::Log(LogEvent { record, .. }) => {
                if *record.callsite().level() == Level::INFO {
                    let raw = RawLogRecord::new(&record.body_attrs_bytes);
                    assert!(raw.body().is_none());
                    let error = raw
                        .attributes()
                        .find(|attr| attr.key() == b"error")
                        .expect("recovery retains the sampled error");
                    let value = error.value().expect("error has a value");
                    assert_eq!(value.as_string(), Some(b"connection refused".as_slice()));
                }
                (
                    record.callsite().name(),
                    record.callsite().target(),
                    *record.callsite().level(),
                )
            }
            ObservedEvent::Engine(_) => panic!("expected log event"),
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(formats.get(), 2);
}

/// Scenario: Oversized error detail is sampled and then reported on confirmed recovery.
/// Guarantees: Priority context survives encoding, and recovery preserves the sampled text verbatim.
#[test]
fn priority_detail_survives_bounded_its_encoding() {
    let (sender, receiver) = flume::unbounded();
    let setup = TracingSetup::new(
        ProviderSetup::InternalAsync {
            reporter: ObservedEventReporter::new(
                otel_arrow_dfe_config::observed_state::SendPolicy::default(),
                sender,
            ),
        },
        otel_arrow_dfe_config::settings::telemetry::logs::LogLevel::default(),
        LogContext::new,
    );
    setup.with_subscriber(|| {
        let mut diagnostics = SignalDiagnostics::new(setup.log_emitter());
        let start = Instant::now();
        let text = format!("root cause: \n{}", "x".repeat(4_000));
        otel_arrow_dfe_telemetry::otel_summary_warn!(
            target: "otel.exporter.test",
            at: start,
            &mut diagnostics,
            otel_arrow_dfe_config::SignalType::Logs,
            DiagnosticErrorKind::Transport,
            "test.export_error",
            retryable = true,
            message = %text
        );
        otel_arrow_dfe_telemetry::otel_summary_recover!(
            target: "otel.exporter.test",
            at: start + Duration::from_secs(30),
            &mut diagnostics,
            otel_arrow_dfe_config::SignalType::Logs,
            start + Duration::from_secs(1),
            "test.export_recovered"
        );
    });

    let ObservedEvent::Log(LogEvent { record, .. }) =
        receiver.try_recv().expect("diagnostic should be delivered")
    else {
        panic!("expected log event");
    };
    let body_attrs = record.body_attrs_bytes;
    let record = RawLogRecord::new(&body_attrs);
    let body_value = record
        .body()
        .expect("diagnostic error body must survive encoding");
    let body = std::str::from_utf8(
        body_value
            .as_string()
            .expect("diagnostic error body must be a string"),
    )
    .expect("diagnostic error body must be valid UTF-8");
    assert!(body.starts_with("root cause: "));
    assert!(body.ends_with("[...]"));

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

    let ObservedEvent::Log(LogEvent { record, .. }) =
        receiver.try_recv().expect("recovery should be delivered")
    else {
        panic!("expected log event");
    };
    assert_eq!(*record.callsite().level(), Level::INFO);
    let recovery = RawLogRecord::new(&record.body_attrs_bytes);
    assert!(recovery.body().is_none());
    let error = recovery
        .attributes()
        .find(|attr| attr.key() == b"error")
        .expect("recovery retains the sampled error");
    let value = error.value().expect("error has a value");
    assert_eq!(value.as_string(), Some(body.as_bytes()));
    assert!(!recovery.attributes().any(|attr| attr.key() == b"retryable"));
    assert_eq!(record.dropped_attributes_count, 0);
    assert!(receiver.is_empty(), "only failure and recovery are emitted");
}
