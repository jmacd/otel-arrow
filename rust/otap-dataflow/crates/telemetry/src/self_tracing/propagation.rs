// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context injection for self-tracing spans.
//!
//! Injection writes the active [`SpanContext`] into an outgoing carrier through
//! the [`Injector`] trait, which mirrors the OpenTelemetry propagator setter.
//! The W3C `traceparent` and `tracestate` values are formatted into stack
//! buffers, so injection allocates nothing on the heap.
//!
//! Extraction is the symmetric operation and is a planned follow-up.

use super::span::SpanContext;

/// A carrier setter for context injection.
///
/// Implementations write a header or field with the given key and value, for
/// example a map of HTTP headers or gRPC metadata.
pub trait Injector {
    /// Set the carrier field `key` to `value`.
    fn set(&mut self, key: &str, value: &str);
}

/// Inject the trace context into a carrier.
///
/// Writes the W3C `traceparent` header always, and the `tracestate` header when
/// the context carries a `th` or `rv` value. Only the OpenTelemetry `ot` member
/// is written for `tracestate`.
pub fn inject(context: &SpanContext, injector: &mut dyn Injector) {
    let traceparent = context.traceparent();
    injector.set("traceparent", traceparent.as_str());
    if let Some(tracestate) = context.tracestate() {
        injector.set("tracestate", tracestate.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_tracing::span::{
        OtelTraceState, Randomness, SpanContext, SpanId, Threshold, TraceFlags, TraceId,
    };
    use std::collections::HashMap;

    #[derive(Default)]
    struct MapInjector(HashMap<String, String>);

    impl Injector for MapInjector {
        fn set(&mut self, key: &str, value: &str) {
            let _ = self.0.insert(key.to_string(), value.to_string());
        }
    }

    fn context_with(th: Option<Threshold>, rv: Option<Randomness>) -> SpanContext {
        SpanContext {
            trace_id: TraceId::from_bytes([
                0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
                0x47, 0x36,
            ]),
            span_id: SpanId::from_bytes([0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
            flags: TraceFlags::new(true, true),
            ot: OtelTraceState { rv, th },
            ..Default::default()
        }
    }

    #[test]
    fn injects_traceparent() {
        let mut carrier = MapInjector::default();
        inject(&context_with(None, None), &mut carrier);
        assert_eq!(
            carrier.0.get("traceparent").map(String::as_str),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03")
        );
        // No th or rv means no tracestate.
        assert!(!carrier.0.contains_key("tracestate"));
    }

    #[test]
    fn injects_tracestate_with_threshold_and_randomness() {
        let mut carrier = MapInjector::default();
        let ctx = context_with(
            Threshold::from_th_value("c"),
            Randomness::from_rv_value("0123456789abcd"),
        );
        inject(&ctx, &mut carrier);
        assert_eq!(
            carrier.0.get("tracestate").map(String::as_str),
            Some("ot=th:c;rv:0123456789abcd")
        );
    }

    #[test]
    fn injects_tracestate_threshold_only() {
        let mut carrier = MapInjector::default();
        inject(
            &context_with(Threshold::from_th_value("8"), None),
            &mut carrier,
        );
        assert_eq!(
            carrier.0.get("tracestate").map(String::as_str),
            Some("ot=th:8")
        );
    }
}
