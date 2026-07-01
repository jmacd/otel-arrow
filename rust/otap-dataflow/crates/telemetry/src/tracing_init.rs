// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tokio tracing subscriber initialization.
//!
//! This module handles the setup of the global and per-thread tokio
//! tracing subscriber. The tracing subscriber determines how log and
//! trace events are captured and routed.

use crate::event::{LogEvent, ObservedEventReporter};
use crate::self_tracing::encoder::{
    DirectFieldVisitor, append_int_attribute, append_string_attribute,
};
use crate::self_tracing::sampler::{self, ComposableSampler, Sampler, SpanKind};
use crate::self_tracing::sampling::callsite::callsite_identity;
use crate::self_tracing::span::{SpanContext, SpanId, TraceId};
use crate::self_tracing::{
    ATTR_SPAN_DURATION_NANO, ATTR_SPAN_PARENT_SPAN_ID, ATTR_SPAN_PHASE, ConsoleWriter,
    LOG_ARGUMENTS_ENCODE_INLINE, LogContext, LogContextFn, LogRecord, SPAN_PHASE_END,
    SPAN_PHASE_START,
};
use otap_df_config::settings::telemetry::logs::LogLevel;
use otap_df_pdata::otlp::common::ProtoBuffer;
use smallvec::SmallVec;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tracing::callsite::Identifier;
use tracing::span::{Attributes, Id};
use tracing::{Dispatch, Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer as TracingLayer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt};

/// Creates an `EnvFilter` for the given log level.
///
/// The base filter comes from the `RUST_LOG` environment variable if set;
/// otherwise it falls back to the level's
/// [`RUST_LOG`-style directive string][env-filter]. The suppression directive
/// described below is then appended unconditionally on top of that base, so it
/// can override a conflicting user-supplied directive for the
/// `opentelemetry-prometheus[{metric_description}]` target.
///
/// In all cases the filter suppresses one specific benign per-scrape warning
/// from the `opentelemetry-prometheus` crate: the
/// `MetricValidationFailed` event carrying a `metric_description` field. The
/// Prometheus exporter flattens OpenTelemetry instrumentation scopes into a
/// single namespace keyed by metric name (scope is exposed only as the
/// `otel_scope_name` label, per the OTel/Prometheus interop spec). When two
/// scopes emit the same metric name with different descriptions, the exporter
/// keeps the first `# HELP` and logs this warning on every scrape. No data is
/// lost (each scope remains a distinct time series), so it is pure noise.
///
/// The directive is field-scoped, so it is surgical: it caps *only* the
/// description-conflict warning (which carries `metric_description`) at `ERROR`.
/// The sibling type-conflict warning (which carries `metric_type` and *does*
/// drop data) lacks that field, so it is left untouched and remains visible at
/// `WARN`, as do all other diagnostics from the crate.
/// See https://github.com/open-telemetry/otel-arrow/issues/2734 for more
/// details.
///
/// [env-filter]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
#[must_use]
pub fn create_env_filter(level: &LogLevel) -> EnvFilter {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.as_str()));
    // Target matches `env!("CARGO_PKG_NAME")` set by opentelemetry's `otel_warn!`.
    // The `[{metric_description}]` field filter matches only the benign
    // description-conflict `MetricValidationFailed` event, leaving the
    // data-dropping type-conflict variant (field `metric_type`) visible.
    filter.add_directive(
        "opentelemetry-prometheus[{metric_description}]=error"
            .parse()
            .expect("valid tracing directive"),
    )
}

/// Combined tracing configuration for a thread.
///
/// This struct bundles the provider setup with the log level, allowing
/// the `InternalTelemetrySystem` to control all tracing configuration.
/// Future enhancements may include per-thread log level overrides.
#[derive(Clone)]
pub struct TracingSetup {
    /// The provider mode configuration.
    pub provider: ProviderSetup,
    /// The log level for filtering.
    pub log_level: LogLevel,
    /// Context function.
    pub context_fn: LogContextFn,
}

impl TracingSetup {
    /// Create a new tracing setup.
    #[must_use]
    pub fn new(provider: ProviderSetup, log_level: LogLevel, context_fn: LogContextFn) -> Self {
        Self {
            provider,
            log_level,
            context_fn,
        }
    }

    /// Initialize this setup as the global tracing subscriber.
    pub fn try_init_global(&self) -> Result<(), tracing::dispatcher::SetGlobalDefaultError> {
        self.provider
            .try_init_global(&self.log_level, self.context_fn)
    }

    /// Run a closure with the appropriate tracing subscriber for this setup.
    pub fn with_subscriber<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.provider
            .with_subscriber(&self.log_level, self.context_fn, f)
    }

    #[cfg(test)]
    pub(crate) fn with_subscriber_ignoring_env<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.provider
            .with_subscriber_ignoring_env(&self.log_level, self.context_fn, f)
    }
}

/// Provider configuration for setting up a tracing subscriber.
#[derive(Clone)]
pub enum ProviderSetup {
    /// Logs are silently dropped.
    Noop,

    /// Synchronous console logging via `StructuredLoggingLayer`.
    ConsoleDirect,

    /// Asynchronous console logging via an observed event reporter which
    /// is either the admin component ("console_async") or an internal telemetry
    /// pipeline engine ("its").
    InternalAsync {
        /// Reporter to send log events through.
        reporter: ObservedEventReporter,
    },
}

impl ProviderSetup {
    fn build_dispatch_with_filter(&self, filter: EnvFilter, context_fn: LogContextFn) -> Dispatch {
        match self {
            ProviderSetup::Noop => Dispatch::new(tracing::subscriber::NoSubscriber::new()),

            ProviderSetup::ConsoleDirect => {
                let layer =
                    StructuredLoggingLayer::new(Some(ConsoleWriter::color()), None, context_fn);
                Dispatch::new(Registry::default().with(filter).with(layer))
            }

            ProviderSetup::InternalAsync { reporter } => {
                let layer = StructuredLoggingLayer::new(None, Some(reporter.clone()), context_fn);
                Dispatch::new(Registry::default().with(filter).with(layer))
            }
        }
    }

    /// Build a `Dispatch` for this provider setup with the given log level.
    fn build_dispatch(&self, log_level: &LogLevel, context_fn: LogContextFn) -> Dispatch {
        self.build_dispatch_with_filter(create_env_filter(log_level), context_fn)
    }

    /// Initialize this setup as the global tracing subscriber.
    pub fn try_init_global(
        &self,
        log_level: &LogLevel,
        context_fn: LogContextFn,
    ) -> Result<(), tracing::dispatcher::SetGlobalDefaultError> {
        let dispatch = self.build_dispatch(log_level, context_fn);
        tracing::dispatcher::set_global_default(dispatch)
    }

    /// Run a closure with the appropriate tracing subscriber for this setup.
    pub fn with_subscriber<F, R>(&self, log_level: &LogLevel, context_fn: LogContextFn, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let dispatch = self.build_dispatch(log_level, context_fn);
        tracing::dispatcher::with_default(&dispatch, f)
    }

    #[cfg(test)]
    fn with_subscriber_ignoring_env<F, R>(
        &self,
        log_level: &LogLevel,
        context_fn: LogContextFn,
        f: F,
    ) -> R
    where
        F: FnOnce() -> R,
    {
        let dispatch =
            self.build_dispatch_with_filter(EnvFilter::new(log_level.as_str()), context_fn);
        tracing::dispatcher::with_default(&dispatch, f)
    }
}

/// Per-span state stored in the tracing registry's span extensions.
///
/// This is the only data we retain for a live span. It holds the immutable
/// [`SpanContext`] used for propagation and stamping, the sampling decision, and
/// the start instant used to compute the END event duration. Span attributes
/// and nested events are emitted immediately as independent log records and are
/// never accumulated here.
struct SpanState {
    context: SpanContext,
    sampled: bool,
    start: Instant,
}

thread_local! {
    /// Stack of trace contexts for the spans currently entered on this thread.
    /// Updated by `on_enter` and `on_exit`, read by [`current_span_context`].
    static CURRENT_SPAN_CONTEXT: RefCell<SmallVec<[SpanContext; 8]>> =
        RefCell::new(SmallVec::new());
}

tokio::task_local! {
    /// Ambient data-plane span context for the current task.
    ///
    /// Swapped in around a node's processing of one data item so that logs and
    /// child spans emitted during processing link to the data's trace, even
    /// across `await` points and thread migration. This is a `tokio` task-local,
    /// not a thread-local, precisely because node processing is asynchronous: a
    /// thread-local set around an `.await` would leak to other tasks and would
    /// not follow a work-stolen future. It is distinct from the synchronous
    /// [`CURRENT_SPAN_CONTEXT`] stack of entered tracing spans, which the tracing
    /// machinery maintains per poll.
    static AMBIENT_SPAN_CONTEXT: SpanContext;
}

/// Run `fut` with `ctx` installed as the ambient data-plane span context.
///
/// The engine wraps a node's per-message processing in this scope so that the
/// node's emitted logs and child spans inherit the data's trace. Receivers,
/// which originate spans, may use it directly while building outbound data.
pub fn scope_span_context<F>(ctx: SpanContext, fut: F) -> impl Future<Output = F::Output>
where
    F: Future,
{
    AMBIENT_SPAN_CONTEXT.scope(ctx, fut)
}

/// The ambient data-plane span context for the current task, if one is in
/// scope. Returns `None` outside any [`scope_span_context`] and outside a
/// `tokio` task.
#[must_use]
pub fn ambient_span_context() -> Option<SpanContext> {
    AMBIENT_SPAN_CONTEXT.try_with(|ctx| *ctx).ok()
}

/// Returns the trace context of the span currently entered on this thread, or
/// the ambient data-plane span context when no tracing span is entered.
///
/// This is the value to pass to [`crate::self_tracing::propagation::inject`]
/// when propagating context to an outgoing carrier.
#[must_use]
pub fn current_span_context() -> Option<SpanContext> {
    CURRENT_SPAN_CONTEXT
        .with(|stack| stack.borrow().last().copied())
        .or_else(ambient_span_context)
}

/// Build a span START log record from the span attributes and trace context.
fn build_start_record(
    callsite: Identifier,
    context: SpanContext,
    attrs: &Attributes<'_>,
    parent: Option<SpanContext>,
    log_context: LogContext,
) -> LogRecord {
    let mut buf = ProtoBuffer::with_capacity_and_limit(
        LOG_ARGUMENTS_ENCODE_INLINE,
        LOG_ARGUMENTS_ENCODE_INLINE,
    );
    let dropped = {
        let mut visitor = DirectFieldVisitor::new(&mut buf);
        attrs.record(&mut visitor);
        visitor.dropped_count()
    };
    let _ = append_string_attribute(&mut buf, ATTR_SPAN_PHASE, SPAN_PHASE_START);
    if let Some(parent) = parent {
        let _ = append_string_attribute(
            &mut buf,
            ATTR_SPAN_PARENT_SPAN_ID,
            parent.span_id.to_hex().as_str(),
        );
    }
    LogRecord {
        callsite_id: callsite,
        body_attrs_bytes: buf.into_bytes(),
        dropped_attributes_count: dropped as u16,
        context: log_context,
        trace: Some(context),
    }
}

/// Build a span END log record carrying the phase and elapsed duration.
fn build_end_record(
    callsite: Identifier,
    context: SpanContext,
    duration_nanos: i64,
    log_context: LogContext,
) -> LogRecord {
    let mut buf = ProtoBuffer::with_capacity_and_limit(
        LOG_ARGUMENTS_ENCODE_INLINE,
        LOG_ARGUMENTS_ENCODE_INLINE,
    );
    let _ = append_string_attribute(&mut buf, ATTR_SPAN_PHASE, SPAN_PHASE_END);
    let _ = append_int_attribute(&mut buf, ATTR_SPAN_DURATION_NANO, duration_nanos);
    LogRecord {
        callsite_id: callsite,
        body_attrs_bytes: buf.into_bytes(),
        dropped_attributes_count: 0,
        context: log_context,
        trace: Some(context),
    }
}

/// A tracing layer that emits a structured log record to either console or an async sink.
///
/// Spans surface as two independent log records: a START record when the span
/// opens and an END record when it closes, both gated by the configured
/// [`Sampler`]. Log events emitted inside a span are stamped with the active
/// span's trace context.
pub struct StructuredLoggingLayer {
    writer: Option<ConsoleWriter>,
    reporter: Option<ObservedEventReporter>,
    context_fn: LogContextFn,
    sampler: Arc<dyn Sampler>,
}

impl StructuredLoggingLayer {
    /// Create a new structured logging layer with the default sampler.
    #[must_use]
    fn new(
        writer: Option<ConsoleWriter>,
        reporter: Option<ObservedEventReporter>,
        context_fn: LogContextFn,
    ) -> Self {
        Self {
            writer,
            reporter,
            context_fn,
            sampler: default_sampler(),
        }
    }

    /// Replace the sampler used to decide whether spans emit START and END
    /// events.
    #[must_use]
    pub fn with_sampler(mut self, sampler: Arc<dyn Sampler>) -> Self {
        self.sampler = sampler;
        self
    }

    /// Print to console and forward to the async sink, as configured.
    fn emit(&self, time: SystemTime, record: LogRecord) {
        if let Some(writer) = self.writer {
            writer.print_log_record(time, &record.as_view(), |w| {
                w.format_entity_suffix_without_registry(&record.context);
            });
        }
        let Some(reporter) = &self.reporter else {
            return;
        };
        // Route through the per-worker sample buffer when one is installed,
        // otherwise forward the record directly to the reporter.
        match crate::self_tracing::local_buffer::route(time, record) {
            None => {}
            Some((time, record)) => reporter.log(LogEvent { time, record }),
        }
    }
}

/// The default sampler: a root sampler that always records, with children
/// honoring the propagated threshold.
fn default_sampler() -> Arc<dyn Sampler> {
    Arc::new(ComposableSampler::ParentThreshold(Box::new(
        ComposableSampler::AlwaysOn,
    )))
}

impl<S> TracingLayer<S> for StructuredLoggingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();

        // Resolve the parent trace context from the tokio span parent chain,
        // falling back to the ambient data-plane span context so a node's root
        // span links to the trace of the data it is processing.
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .and_then(|parent| parent.extensions().get::<SpanState>().map(|st| st.context))
            .or_else(ambient_span_context);

        let trace_id = parent.map_or_else(TraceId::generate, |p| p.trace_id);
        let span_id = SpanId::generate();

        // The span-start callsite identity comes from the Tokio tracing
        // callsite, the canonical per-site identity, rather than re-hashing the
        // span name. It is process-local and is never serialized.
        let start_callsite = callsite_identity(&metadata.callsite());

        let decision = sampler::evaluate(
            self.sampler.as_ref(),
            parent,
            trace_id,
            span_id,
            metadata.name(),
            start_callsite,
            SpanKind::Internal,
        );

        // Retain state for propagation regardless of the sampling decision so
        // descendants inherit a correct context.
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanState {
                context: decision.context,
                sampled: decision.sampled,
                start: Instant::now(),
            });
        }

        if decision.sampled {
            let record = build_start_record(
                metadata.callsite(),
                decision.context,
                attrs,
                parent,
                (self.context_fn)(),
            );
            self.emit(SystemTime::now(), record);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(context) = span.extensions().get::<SpanState>().map(|st| st.context) {
                CURRENT_SPAN_CONTEXT.with(|stack| stack.borrow_mut().push(context));
            }
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if span.extensions().get::<SpanState>().is_some() {
                CURRENT_SPAN_CONTEXT.with(|stack| {
                    let _ = stack.borrow_mut().pop();
                });
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            let callsite = span.metadata().callsite();
            let state = match span.extensions().get::<SpanState>() {
                Some(st) => (st.sampled, st.context, st.start),
                None => return,
            };
            let (sampled, context, start) = state;
            if !sampled {
                return;
            }
            let duration_nanos = start.elapsed().as_nanos() as i64;
            let record = build_end_record(callsite, context, duration_nanos, (self.context_fn)());
            self.emit(SystemTime::now(), record);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let time = SystemTime::now();
        let context = (self.context_fn)();
        // Stamp the event with the enclosing tracing span's context, falling
        // back to the ambient data-plane span context so a plain log emitted
        // while processing a data item still links to that data's trace.
        let trace = ctx
            .event_span(event)
            .and_then(|span| span.extensions().get::<SpanState>().map(|st| st.context))
            .or_else(ambient_span_context);
        let record = LogRecord::new(event, context).with_trace(trace);
        self.emit(time, record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::ObservedEvent;
    use crate::self_tracing::LogContext;
    use crate::{otel_debug, otel_error, otel_info, otel_warn};
    use otap_df_config::observed_state::SendPolicy;

    fn test_reporter() -> (ObservedEventReporter, flume::Receiver<ObservedEvent>) {
        let (tx, rx) = flume::bounded(16);
        let reporter = ObservedEventReporter::new(SendPolicy::default(), tx);
        (reporter, rx)
    }

    fn noop_provider() -> ProviderSetup {
        ProviderSetup::Noop
    }

    fn console_direct_provider() -> ProviderSetup {
        ProviderSetup::ConsoleDirect
    }

    fn internal_async_provider(reporter: ObservedEventReporter) -> ProviderSetup {
        ProviderSetup::InternalAsync { reporter }
    }

    fn test_setup(p: ProviderSetup, l: LogLevel) -> TracingSetup {
        TracingSetup::new(p, l, LogContext::new)
    }

    fn level(s: &str) -> LogLevel {
        serde_yaml::from_str(&format!("\"{s}\"")).unwrap()
    }

    fn all_simple_levels() -> Vec<LogLevel> {
        vec![
            level("off"),
            level("debug"),
            level("info"),
            level("warn"),
            level("error"),
        ]
    }

    /// Counts how many events reach the subscriber after filtering.
    fn count_events_through_filter<F>(emit: F) -> usize
    where
        F: FnOnce() + std::panic::UnwindSafe,
    {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingLayer(Arc<AtomicUsize>);
        impl<S: Subscriber> TracingLayer<S> for CountingLayer {
            fn on_event(&self, _event: &Event<'_>, _ctx: Context<'_, S>) {
                let _ = self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let count = Arc::new(AtomicUsize::new(0));
        crate::with_cleared_rust_log(|| {
            let subscriber = Registry::default()
                .with(create_env_filter(&level("info")))
                .with(CountingLayer(count.clone()));
            tracing::subscriber::with_default(subscriber, emit);
        });
        count.load(Ordering::SeqCst)
    }

    #[test]
    fn create_env_filter_parses_for_all_levels() {
        // The embedded `opentelemetry-prometheus[{metric_description}]=error`
        // directive must parse for every level (otherwise `create_env_filter`
        // panics at startup via its `.expect`).
        crate::with_cleared_rust_log(|| {
            for l in all_simple_levels() {
                let _ = create_env_filter(&l);
            }
        });
    }

    #[test]
    fn prometheus_description_conflict_warning_is_suppressed() {
        // Benign description-conflict warning (field `metric_description`): the
        // field-scoped directive caps it at ERROR, so a WARN is dropped.
        let count = count_events_through_filter(|| {
            tracing::warn!(
                target: "opentelemetry-prometheus",
                metric_description = "conflict",
                "Instrument description conflict, using existing"
            );
        });
        assert_eq!(
            count, 0,
            "benign description-conflict warning should be suppressed"
        );
    }

    #[test]
    fn prometheus_type_conflict_warning_remains_visible() {
        // Data-dropping type-conflict warning (field `metric_type`, not
        // `metric_description`): the directive does not match it, so it stays
        // visible at WARN.
        let count = count_events_through_filter(|| {
            tracing::warn!(
                target: "opentelemetry-prometheus",
                metric_type = "conflict",
                "Instrument type conflict, using existing type definition"
            );
        });
        assert_eq!(
            count, 1,
            "data-dropping type-conflict warning should remain visible"
        );
    }

    #[test]
    fn noop_provider_runs() {
        crate::with_cleared_rust_log(|| {
            let setup = test_setup(noop_provider(), level("info"));
            setup.with_subscriber_ignoring_env(|| {
                otel_info!("log_dropped");
            });
        });
    }

    #[test]
    fn noop_provider_all_levels() {
        crate::with_cleared_rust_log(|| {
            for l in all_simple_levels() {
                let setup = test_setup(noop_provider(), l);
                setup.with_subscriber_ignoring_env(|| {
                    otel_debug!("debug", "debug message");
                    otel_info!("info");
                    otel_warn!("warn");
                    otel_error!("error");
                });
            }
        });
    }

    #[test]
    fn console_direct_provider_runs() {
        crate::with_cleared_rust_log(|| {
            let setup = test_setup(console_direct_provider(), level("info"));
            setup.with_subscriber_ignoring_env(|| {
                otel_info!("console_log");
            });
        });
    }

    #[test]
    fn console_direct_all_levels() {
        crate::with_cleared_rust_log(|| {
            for l in all_simple_levels() {
                let setup = test_setup(console_direct_provider(), l);
                setup.with_subscriber_ignoring_env(|| {
                    otel_debug!("debug", "debug message");
                    otel_info!("info");
                    otel_warn!("warn");
                    otel_error!("error");
                });
            }
        });
    }

    #[test]
    fn console_async_provider_sends_logs() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            setup.with_subscriber_ignoring_env(|| {
                otel_info!("async_log");
            });

            // Verify the log was sent through the channel
            let event = receiver.try_recv().expect("should receive log event");
            assert!(
                matches!(event, ObservedEvent::Log(_)),
                "event should be a log"
            );
        });
    }

    #[test]
    fn console_async_all_levels() {
        crate::with_cleared_rust_log(|| {
            for l in all_simple_levels() {
                let (reporter, receiver) = test_reporter();
                let setup = test_setup(internal_async_provider(reporter), l.clone());
                setup.with_subscriber_ignoring_env(|| {
                    otel_debug!("debug", "debug message");
                    otel_info!("info");
                    otel_warn!("warn");
                    otel_error!("error");
                });
                drop(setup);

                let cnt = receiver.into_iter().count();
                let expect = match l.as_str() {
                    "off" => 0,
                    "debug" => 4,
                    "info" => 3,
                    "warn" => 2,
                    "error" => 1,
                    _ => unreachable!(),
                };
                assert_eq!(cnt, expect);
            }
        });
    }

    #[test]
    fn log_level_filters_debug() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("filtered", "debug message filtered out");
            });

            assert!(
                receiver.try_recv().is_err(),
                "debug log should not be received at Info level"
            );
        });
    }

    #[test]
    fn log_level_warn_filters_lower() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("warn"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("filtered", "debug message filtered out");
                otel_info!("filtered");
                otel_warn!("not_filtered");
            });

            // Should only receive the warn
            let event = receiver.try_recv().expect("should receive warn");
            assert!(matches!(event, ObservedEvent::Log(_)));
            assert!(receiver.try_recv().is_err(), "should only have one event");
        });
    }

    #[test]
    fn ambient_span_context_scope_sets_and_clears() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let ctx = SpanContext {
            trace_id: TraceId(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            span_id: SpanId(0x42),
            ..Default::default()
        };
        rt.block_on(async {
            assert_eq!(ambient_span_context(), None);
            scope_span_context(ctx, async {
                assert_eq!(ambient_span_context(), Some(ctx));
                // The task-local survives an await point, unlike a thread-local
                // around the same await.
                tokio::task::yield_now().await;
                assert_eq!(ambient_span_context(), Some(ctx));
            })
            .await;
            assert_eq!(ambient_span_context(), None);
        });
    }

    #[test]
    fn ambient_span_context_stamps_in_scope_logs() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));
            let ctx = SpanContext {
                trace_id: TraceId(0x0fed_cba9_8765_4321_0fed_cba9_8765_4321),
                span_id: SpanId(0x7),
                ..Default::default()
            };
            setup.with_subscriber_ignoring_env(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("runtime");
                // A plain log emitted while the ambient span context is in scope
                // is stamped with that context, even though no tracing span was
                // entered.
                rt.block_on(scope_span_context(ctx, async {
                    otel_info!("in_scope");
                }));
            });
            drop(setup);

            let event = receiver.try_recv().expect("should receive the in-scope log");
            match event {
                ObservedEvent::Log(log) => {
                    assert_eq!(log.record.trace, Some(ctx));
                }
                other => panic!("expected a log event, got {other:?}"),
            }
        });
    }

    #[test]
    fn log_level_error_filters_lower() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("error"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("filtered", "debug message filtered out");
                otel_info!("filtered");
                otel_warn!("filtered");
                otel_error!("not_filtered");
            });

            let event = receiver.try_recv().expect("should receive error");
            assert!(matches!(event, ObservedEvent::Log(_)));
            assert!(receiver.try_recv().is_err(), "should only have one event");
        });
    }

    #[test]
    fn log_level_off_filters_all() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("off"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("filtered", "debug message filtered out");
                otel_info!("filtered");
                otel_warn!("filtered");
                otel_error!("filtered");
            });

            assert!(receiver.try_recv().is_err(), "all logs should be filtered");
        });
    }

    #[test]
    fn log_level_debug_allows_all() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("debug"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("d", "debug message");
                otel_info!("i");
                otel_warn!("w");
                otel_error!("e");
            });

            // Should receive all 4
            for _ in 0..4 {
                let _ = receiver.try_recv().expect("should receive log");
            }
        });
    }

    #[test]
    fn dropped_attributes_count_propagates() {
        // Regression test: when too many attributes are passed to overflow
        // the inline encoding buffer, the visitor's dropped_attributes_count
        // must be preserved end-to-end through the ITS encode path
        // (encode_export_logs_request) and parsed back via the same
        // RawLogsData view used by the console exporter.
        //
        // Historically, a partial body write left an unpatched length
        // placeholder + trailing garbage bytes in the inline buffer, which
        // corrupted subsequent fields appended by encode_log_record (notably
        // dropped_attributes_count itself). encode_body_string is now wrapped
        // in try_encode to roll back partial bytes on overflow.
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            // Use enough long-string attributes to overflow any reasonable
            // LOG_ARGUMENTS_ENCODE_INLINE (well above 256 bytes worth of payload).
            setup.with_subscriber_ignoring_env(|| {
                otel_info!(
                    "overflow.test",
                    a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                    d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                    e = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    f = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    g = "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
                    message = "Body that itself is fairly long and may not fit alongside the attributes above"
                );
            });

            let event = receiver.try_recv().expect("should receive log");
            let log_event = match event {
                ObservedEvent::Log(le) => le,
                _ => panic!("expected log"),
            };
            let visitor_dropped = log_event.record.dropped_attributes_count;
            assert!(
                visitor_dropped > 0,
                "expected visitor to drop attrs, got {visitor_dropped}"
            );

            // Encode through the full ITS path and parse via RawLogsData
            // (the same path used by internal_telemetry_receiver → console
            // exporter).
            use crate::self_tracing::{ScopeToBytesMap, encode_export_logs_request};
            use bytes::Bytes;
            use otap_df_pdata::otlp::ProtoBuffer;
            use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
            use otap_df_pdata_views::views::logs::{
                LogRecordView, LogsDataView, ResourceLogsView, ScopeLogsView,
            };

            let resource_bytes = Bytes::new();
            let registry = crate::registry::TelemetryRegistryHandle::new();
            let mut scope_cache = ScopeToBytesMap::new(registry);
            let mut buf = ProtoBuffer::default();
            encode_export_logs_request(&mut buf, &log_event, &resource_bytes, &mut scope_cache);
            let bytes_vec = buf.into_bytes();

            let raw = RawLogsData::new(bytes_vec.as_ref());
            let mut parsed_dropped = None;
            for rl in raw.resources() {
                for sl in rl.scopes() {
                    for lr in sl.log_records() {
                        parsed_dropped = Some(lr.dropped_attributes_count());
                    }
                }
            }
            assert_eq!(
                parsed_dropped,
                Some(visitor_dropped as u32),
                "dropped_attributes_count must round-trip through encode/parse"
            );
        });
    }

    #[test]
    fn console_async_layer_with_fields() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            setup.with_subscriber_ignoring_env(|| {
                otel_info!("structured", key = "value", number = 42);
            });

            let event = receiver.try_recv().expect("should receive log");
            assert!(matches!(event, ObservedEvent::Log(_)));
            let text = event.to_string();
            assert!(text.contains("key=value"), "text is {}", text);
            assert!(text.contains("number=42"), "text is {}", text);
            assert!(text.contains("structured"), "text is {}", text);
        });
    }

    #[test]
    fn provider_setup_with_subscriber_all_variants() {
        crate::with_cleared_rust_log(|| {
            let info = level("info");
            noop_provider().with_subscriber_ignoring_env(&info, LogContext::new, || {
                otel_info!("noop");
            });

            console_direct_provider().with_subscriber_ignoring_env(&info, LogContext::new, || {
                otel_info!("console_direct");
            });

            let (reporter, _rx) = test_reporter();
            internal_async_provider(reporter).with_subscriber_ignoring_env(
                &info,
                LogContext::new,
                || {
                    otel_info!("console_async");
                },
            );
        });
    }

    #[test]
    fn its_provider_filters_correctly() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("warn"));

            setup.with_subscriber_ignoring_env(|| {
                otel_debug!("filtered", "debug message filtered out");
                otel_info!("filtered");
                otel_warn!("not_filtered");
                otel_error!("not_filtered");
            });
            drop(setup);

            assert_eq!(receiver.into_iter().count(), 2);
        });
    }

    #[test]
    fn nested_with_subscriber() {
        crate::with_cleared_rust_log(|| {
            let (reporter1, receiver1) = test_reporter();
            let (reporter2, receiver2) = test_reporter();

            let setup1 = test_setup(internal_async_provider(reporter1), level("info"));
            let setup2 = test_setup(internal_async_provider(reporter2), level("info"));

            let result = setup1.with_subscriber_ignoring_env(|| {
                otel_info!("outer");
                setup2.with_subscriber_ignoring_env(|| {
                    otel_info!("inner");
                });
                otel_info!("outer_again");
                100
            });

            assert_eq!(result, 100);

            // Outer should receive 2, inner should receive 1 and no more.
            assert!(receiver1.try_recv().is_ok());
            assert!(receiver2.try_recv().is_ok());
            assert!(receiver1.try_recv().is_ok());

            assert!(receiver1.try_recv().is_err());
            assert!(receiver2.try_recv().is_err());
        });
    }

    #[test]
    fn sampled_span_emits_start_and_end_with_shared_ids() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            setup.with_subscriber_ignoring_env(|| {
                let span = crate::otel_info_span!("test.span", key = "value");
                let entered = span.enter();
                otel_info!("inside.span");
                drop(entered);
                drop(span);
            });

            let mut logs = Vec::new();
            while let Ok(ObservedEvent::Log(le)) = receiver.try_recv() {
                logs.push(le);
            }
            assert_eq!(logs.len(), 3, "expected START, in-span event, END");

            let start = logs[0].record.trace.expect("START carries trace");
            let inside = logs[1].record.trace.expect("event carries trace");
            let end = logs[2].record.trace.expect("END carries trace");

            assert!(start.is_valid() && start.flags.is_sampled());
            // All three share the trace id; START, the event, and END share the
            // span id.
            assert_eq!(start.trace_id, inside.trace_id);
            assert_eq!(start.trace_id, end.trace_id);
            assert_eq!(start.span_id, inside.span_id);
            assert_eq!(start.span_id, end.span_id);

            let start_text = logs[0].to_string();
            assert!(start_text.contains("test.span"), "{start_text}");
            assert!(start_text.contains("key=value"), "{start_text}");
            assert!(start_text.contains("otel.span.phase=start"), "{start_text}");
            assert!(start_text.contains("trace="), "{start_text}");

            let end_text = logs[2].to_string();
            assert!(end_text.contains("otel.span.phase=end"), "{end_text}");
            assert!(end_text.contains("otel.span.duration_nano"), "{end_text}");
        });
    }

    #[test]
    fn unsampled_span_suppresses_start_and_end_but_keeps_events() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let layer = StructuredLoggingLayer::new(None, Some(reporter), LogContext::new)
                .with_sampler(Arc::new(ComposableSampler::AlwaysOff));
            let subscriber = Registry::default()
                .with(create_env_filter(&level("info")))
                .with(layer);

            tracing::subscriber::with_default(subscriber, || {
                let span = crate::otel_info_span!("unsampled");
                let entered = span.enter();
                otel_info!("inside.unsampled");
                drop(entered);
                drop(span);
            });

            let mut logs = Vec::new();
            while let Ok(ObservedEvent::Log(le)) = receiver.try_recv() {
                logs.push(le);
            }
            // No START or END events, only the in-span event, which still carries
            // the unsampled span context for correlation.
            assert_eq!(logs.len(), 1, "only the in-span event should be emitted");
            let ctx = logs[0].record.trace.expect("event carries trace");
            assert!(ctx.is_valid());
            assert!(!ctx.flags.is_sampled());
        });
    }

    #[test]
    fn nested_spans_inherit_trace_id_and_link_parent() {
        crate::with_cleared_rust_log(|| {
            let (reporter, receiver) = test_reporter();
            let setup = test_setup(internal_async_provider(reporter), level("info"));

            setup.with_subscriber_ignoring_env(|| {
                let parent = crate::otel_info_span!("parent.span");
                let parent_entered = parent.enter();
                let child = crate::otel_info_span!("child.span");
                let child_entered = child.enter();
                drop(child_entered);
                drop(child);
                drop(parent_entered);
                drop(parent);
            });

            let mut starts = Vec::new();
            while let Ok(ObservedEvent::Log(le)) = receiver.try_recv() {
                if le.to_string().contains("otel.span.phase=start") {
                    starts.push(le);
                }
            }
            assert_eq!(starts.len(), 2, "expected a START for parent and child");
            let parent_ctx = starts[0].record.trace.expect("parent trace");
            let child_ctx = starts[1].record.trace.expect("child trace");
            // The child shares the trace id but has its own span id.
            assert_eq!(parent_ctx.trace_id, child_ctx.trace_id);
            assert_ne!(parent_ctx.span_id, child_ctx.span_id);
            // The child START links to the parent via parent_span_id.
            let child_text = starts[1].to_string();
            let parent_hex = parent_ctx.span_id.to_hex();
            assert!(
                child_text.contains(&format!("otel.span.parent_span_id={parent_hex}")),
                "{child_text}"
            );
        });
    }
}
