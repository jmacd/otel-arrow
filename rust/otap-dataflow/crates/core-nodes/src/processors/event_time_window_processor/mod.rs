// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Event-time window processor (metrics-appliance Layer 2), a proof of concept.
//!
//! # THIS PROCESSOR DUPLICATES CODE ON PURPOSE
//!
//! This is a **throwaway proof of concept**. It deliberately re-implements
//! identity extraction, metric-point aggregation, and OTLP output assembly that
//! already exist, in more complete and OTAP-native form, in the
//! [`temporal_reaggregation_processor`](super::temporal_reaggregation_processor)
//! and its private `identity` and `builder` modules. None of that logic is
//! reused here, because those modules are private and the purpose of this PoC is
//! to drive the new **event-time** windowing core
//! ([`otap_df_pdata::otap::windowing`]) end to end inside a real processor node,
//! not to ship production code. When the event-time path graduates, this module
//! should be deleted and folded into the temporal reaggregation processor. See
//! `README.md` for the full rationale and the list of PoC limitations.
//!
//! For every incoming OTAP metrics batch it reads each NUMBER data point (sum and
//! gauge only), computes the point's series identity and event time, routes it to
//! its epoch-aligned tumbling window, and folds it into a per-(series, window)
//! aggregate (delta-sum for sums, last value for gauges). Late points are dropped
//! and counted (D12). After ingesting the batch it drains every window the
//! watermark has closed and emits the completed aggregates as a new metrics
//! batch. Non-metric signals pass through unchanged.
//!
//! As a shuffle owner it keeps an independent windower per partition tag, so a
//! partition's active-series count is exactly its aggregator's stream count. When
//! a [`LoadReportSender`] is wired in via
//! [`set_load_report_sender`](EventTimeWindowProcessor::set_load_report_sender),
//! each telemetry collection reports per-partition load so a placement scheduler
//! can rebalance partitions across owners, closing the load feedback loop.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::{Error, ProcessorErrorKind};
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::process_duration::ComputeDuration;
use otap_df_engine::processor::{ProcessorRuntimeRequirements, ProcessorWrapper};
use otap_df_engine::topic::{LoadReportSender, PartitionLoadTracker};
use otap_df_otap::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use otap_df_pdata::OtlpProtoBytes;
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::otap::windowing::{
    TumblingWindows, WatermarkPolicy, Window, WindowedAggregators,
};
use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use otap_df_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
    ScopeMetrics, Sum,
};
use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
use otap_df_pdata::views::otap::OtapMetricsView;
use otap_df_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, ValueType,
};
use otap_df_pdata_views::views::metrics::{
    DataType, DataView, ExponentialHistogramView, GaugeView, HistogramView, MetricView,
    MetricsView, NumberDataPointView, ResourceMetricsView, ScopeMetricsView, SumView, SummaryView,
    Value,
};
use otap_df_pdata_views::views::resource::ResourceView;
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::otel_warn;
use prost::Message as _;
use serde_json::Value as JsonValue;

mod config;
mod telemetry;

use self::config::Config;
use self::telemetry::{EventTimeWindowMetrics, VIEW_CREATION_FAILED_EVENT};

/// The URN for the event-time window processor.
pub const EVENT_TIME_WINDOW_PROCESSOR_URN: &str = "urn:otel:processor:event_time_window";

/// Factory function to create an [`EventTimeWindowProcessor`].
pub fn create_event_time_window_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        EventTimeWindowProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the event-time window processor as an OTAP processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static EVENT_TIME_WINDOW_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: EVENT_TIME_WINDOW_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
                create_event_time_window_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
    };

/// The event-time window processor.
pub struct EventTimeWindowProcessor {
    /// Processor metrics.
    metrics: MetricSet<EventTimeWindowMetrics>,
    /// Accumulated compute-time telemetry.
    compute_duration: ComputeDuration,
    /// Window geometry, used to lazily construct a per-partition aggregator.
    windows: TumblingWindows,
    /// Watermark policy, used to lazily construct a per-partition aggregator.
    policy: WatermarkPolicy,
    /// Per-partition event-time window aggregation state, keyed by the partition
    /// tag the shuffle assigned. As a shuffle owner the windower runs an
    /// independent windower per partition it holds, so a partition's
    /// `active_series` is exactly its aggregator's stream count.
    aggregators: BTreeMap<u32, WindowedAggregators<SeriesKey, WindowAgg>>,
    /// Owner-side per-partition load accounting (the producer end of the load
    /// feedback loop, `docs/durable-dispatch-topic-design.md`).
    load: PartitionLoadTracker,
    /// Where to send load snapshots, if load reporting is wired to a scheduler.
    load_sender: Option<LoadReportSender>,
}

impl EventTimeWindowProcessor {
    /// Create a processor from a configuration JSON value.
    pub fn from_config(
        pipeline_ctx: PipelineContext,
        config: &JsonValue,
    ) -> Result<Self, ConfigError> {
        let metrics = pipeline_ctx.register_metrics::<EventTimeWindowMetrics>();
        let compute_duration = ComputeDuration::new(&pipeline_ctx);
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: e.to_string(),
            })?;
        config.validate()?;

        let windows = TumblingWindows::new(duration_to_nanos_i64(config.window_size));
        let policy = WatermarkPolicy {
            allowed_lateness_nanos: duration_to_nanos_i64(config.allowed_lateness),
            max_lag_nanos: duration_to_nanos_i64(config.max_lag),
        };
        Ok(Self {
            metrics,
            compute_duration,
            windows,
            policy,
            aggregators: BTreeMap::new(),
            load: PartitionLoadTracker::new(),
            load_sender: None,
        })
    }

    /// Wire this owner's load reports to a placement scheduler. Once set, each
    /// telemetry collection reports per-partition load (active series plus the
    /// interval ingest count) so the scheduler can rebalance partitions across
    /// owners. Closing the load feedback loop with a real aggregating owner.
    pub fn set_load_report_sender(&mut self, sender: LoadReportSender) {
        self.load_sender = Some(sender);
    }

    /// Report per-partition load to the scheduler, if wired. The `active_series`
    /// gauge is read from each partition's live stream count; the interval
    /// `points` were accumulated as batches arrived.
    fn report_load(&mut self) {
        let Some(sender) = self.load_sender.as_ref() else {
            return;
        };
        for (&partition, aggregator) in &self.aggregators {
            self.load
                .set_active_series(partition as usize, aggregator.stream_count() as u64);
        }
        sender.send(self.load.snapshot());
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for EventTimeWindowProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        match msg {
            Message::Control(control) => {
                if let NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } = control
                {
                    _ = metrics_reporter.report(&mut self.metrics);
                    self.compute_duration.report(&mut metrics_reporter);
                    self.report_load();
                }
                Ok(())
            }
            Message::PData(pdata) => {
                // Non-metric signals pass through unchanged.
                if pdata.signal_type() != SignalType::Metrics {
                    return effect_handler
                        .send_message_with_source_node(pdata)
                        .await
                        .map_err(Into::into);
                }

                let processor_id = effect_handler.processor_id();
                // The shuffle tags each batch with the partition it dispatched to;
                // untagged input (e.g. an un-shuffled pipeline) folds into a single
                // default partition.
                let partition = pdata.partition().unwrap_or(0);
                let (context, payload) = pdata.into_parts();
                let mut records: OtapArrowRecords = payload.try_into_with_default()?;
                records.decode_transport_optimized_ids()?;

                let now = unix_now_nanos();
                let windows = self.windows;
                let policy = self.policy;

                // Ingest the batch's number data points into the partition's
                // windower. The view borrow is confined to this match so `records`
                // is free to move afterward.
                let admitted = match OtapMetricsView::try_from(&records) {
                    Ok(view) => Some(effect_handler.timed(
                        &self.compute_duration,
                        || -> Result<u64, Error> {
                            let aggregator = self
                                .aggregators
                                .entry(partition)
                                .or_insert_with(|| WindowedAggregators::new(windows, policy));
                            Ok(ingest_view(aggregator, &mut self.metrics, &view, now))
                        },
                    )?),
                    Err(e) => {
                        otel_warn!(VIEW_CREATION_FAILED_EVENT, error = %e);
                        None
                    }
                };

                self.metrics.batches.inc();

                // A batch we could not read is forwarded unchanged rather than
                // dropped, so no data is silently lost in the PoC.
                let Some(admitted) = admitted else {
                    return effect_handler
                        .send_message_with_source_node(OtapPdata::new(context, records.into()))
                        .await
                        .map_err(Into::into);
                };

                // Drain the partition's completed windows and record this owner's
                // per-partition load (ingest rate plus live series count).
                let aggregator = self
                    .aggregators
                    .get_mut(&partition)
                    .expect("partition aggregator created during ingest");
                let fired = aggregator.drain_complete(now);
                let active_series = aggregator.stream_count() as u64;
                self.load.add_points(partition as usize, admitted);
                self.load
                    .set_active_series(partition as usize, active_series);

                if fired.is_empty() {
                    return Ok(());
                }
                self.metrics.windows_emitted.add(fired.len() as u64);

                let data = build_output(fired);
                let mut bytes = Vec::new();
                data.encode(&mut bytes).map_err(|e| Error::ProcessorError {
                    processor: processor_id.clone(),
                    kind: ProcessorErrorKind::Other,
                    error: format!("failed to encode windowed metrics: {e}"),
                    source_detail: String::new(),
                })?;

                self.metrics.batches_emitted.inc();
                effect_handler
                    .send_message_with_source_node(OtapPdata::new_todo_context(
                        OtlpProtoBytes::ExportMetricsRequest(bytes.into()).into(),
                    ))
                    .await
                    .map_err(Into::into)
            }
        }
    }

    fn runtime_requirements(&self) -> ProcessorRuntimeRequirements {
        ProcessorRuntimeRequirements::none()
    }
}

/// Ingest every NUMBER data point in a metrics view into the windower, skipping
/// (and counting) histogram, exponential-histogram, and summary points. Returns
/// the number of points admitted (folded into a window, not dropped as late).
fn ingest_view<V: MetricsView>(
    aggregators: &mut WindowedAggregators<SeriesKey, WindowAgg>,
    metrics: &mut MetricSet<EventTimeWindowMetrics>,
    view: &V,
    now: i64,
) -> u64 {
    let mut admitted = 0u64;
    for resource_metrics in view.resources() {
        let resource = resource_metrics
            .resource()
            .map(|r| collect_attributes(r.attributes()))
            .unwrap_or_default();
        for scope_metrics in resource_metrics.scopes() {
            let scope = scope_metrics
                .scope()
                .and_then(|s| s.name().map(bytes_to_string))
                .unwrap_or_default();
            for metric in scope_metrics.metrics() {
                let name = bytes_to_string(metric.name());
                let unit = bytes_to_string(metric.unit());
                let Some(data) = metric.data() else {
                    continue;
                };
                match data.value_type() {
                    DataType::Sum => {
                        if let Some(sum) = data.as_sum() {
                            let kind = SeriesKind::Sum {
                                monotonic: sum.is_monotonic(),
                            };
                            for dp in sum.data_points() {
                                admitted += u64::from(fold_point(
                                    aggregators,
                                    metrics,
                                    &resource,
                                    &scope,
                                    &name,
                                    &unit,
                                    kind,
                                    &dp,
                                    now,
                                ));
                            }
                        }
                    }
                    DataType::Gauge => {
                        if let Some(gauge) = data.as_gauge() {
                            for dp in gauge.data_points() {
                                admitted += u64::from(fold_point(
                                    aggregators,
                                    metrics,
                                    &resource,
                                    &scope,
                                    &name,
                                    &unit,
                                    SeriesKind::Gauge,
                                    &dp,
                                    now,
                                ));
                            }
                        }
                    }
                    DataType::Histogram => {
                        if let Some(h) = data.as_histogram() {
                            metrics.points_skipped.add(h.data_points().count() as u64);
                        }
                    }
                    DataType::ExponentialHistogram => {
                        if let Some(h) = data.as_exponential_histogram() {
                            metrics.points_skipped.add(h.data_points().count() as u64);
                        }
                    }
                    DataType::Summary => {
                        if let Some(s) = data.as_summary() {
                            metrics.points_skipped.add(s.data_points().count() as u64);
                        }
                    }
                }
            }
        }
    }
    admitted
}

/// Route one number data point to its window and fold its value in. Returns
/// `true` if the point was admitted, or `false` if it was counted as late
/// because its window had already fired.
#[allow(clippy::too_many_arguments)]
fn fold_point<DP: NumberDataPointView>(
    aggregators: &mut WindowedAggregators<SeriesKey, WindowAgg>,
    metrics: &mut MetricSet<EventTimeWindowMetrics>,
    resource: &[(String, OwnedVal)],
    scope: &str,
    metric: &str,
    unit: &str,
    kind: SeriesKind,
    dp: &DP,
    now: i64,
) -> bool {
    let Some(value) = dp.value() else {
        return false;
    };
    let value = match value {
        Value::Double(d) => d,
        Value::Integer(i) => i as f64,
    };
    let event_time = dp.time_unix_nano() as i64;
    let key = SeriesKey {
        resource: resource.to_vec(),
        scope: scope.to_string(),
        metric: metric.to_string(),
        unit: unit.to_string(),
        kind,
        point: collect_attributes(dp.attributes()),
    };
    match aggregators.admit(key, event_time, now) {
        Some(agg) => {
            fold(agg, kind, event_time, value);
            metrics.points_aggregated.inc();
            true
        }
        None => {
            metrics.points_late.inc();
            false
        }
    }
}

/// Fold a value into a window aggregate: delta-sum for sums, last value (by event
/// time) for gauges.
fn fold(agg: &mut WindowAgg, kind: SeriesKind, time: i64, value: f64) {
    match kind {
        SeriesKind::Sum { .. } => {
            agg.value += value;
            agg.last_time = time;
            agg.has_value = true;
        }
        SeriesKind::Gauge => {
            if !agg.has_value || time >= agg.last_time {
                agg.value = value;
                agg.last_time = time;
                agg.has_value = true;
            }
        }
    }
}

/// Assemble fired `(series, window, aggregate)` tuples into an OTLP metrics
/// payload, grouping data points back under their resource, scope, and metric.
fn build_output(fired: Vec<(SeriesKey, Window, WindowAgg)>) -> MetricsData {
    type MetricGroupKey = (String, String, SeriesKind);
    type ByScope = HashMap<String, HashMap<MetricGroupKey, Vec<NumberDataPoint>>>;
    let mut by_resource: HashMap<Vec<(String, OwnedVal)>, ByScope> = HashMap::new();

    for (key, window, agg) in fired {
        let dp = NumberDataPoint::build()
            .start_time_unix_nano(window.start_nanos.max(0) as u64)
            .time_unix_nano(window.end_nanos.max(0) as u64)
            .value_double(agg.value)
            .attributes(key.point.iter().filter_map(kv_of).collect::<Vec<_>>())
            .finish();
        by_resource
            .entry(key.resource)
            .or_default()
            .entry(key.scope)
            .or_default()
            .entry((key.metric, key.unit, key.kind))
            .or_default()
            .push(dp);
    }

    let resource_metrics = by_resource
        .into_iter()
        .map(|(resource_attrs, scopes)| {
            let scope_metrics = scopes
                .into_iter()
                .map(|(scope_name, metric_groups)| {
                    let metrics = metric_groups
                        .into_iter()
                        .map(|((metric_name, unit, kind), points)| {
                            let builder = Metric::build().name(metric_name).unit(unit);
                            match kind {
                                SeriesKind::Sum { monotonic } => builder
                                    .data_sum(Sum::new(
                                        AggregationTemporality::Delta,
                                        monotonic,
                                        points,
                                    ))
                                    .finish(),
                                SeriesKind::Gauge => {
                                    builder.data_gauge(Gauge::new(points)).finish()
                                }
                            }
                        })
                        .collect::<Vec<_>>();
                    ScopeMetrics::new(
                        InstrumentationScope::build().name(scope_name).finish(),
                        metrics,
                    )
                })
                .collect::<Vec<_>>();
            ResourceMetrics::new(
                Resource::build()
                    .attributes(resource_attrs.iter().filter_map(kv_of).collect::<Vec<_>>())
                    .finish(),
                scope_metrics,
            )
        })
        .collect::<Vec<_>>();

    MetricsData::new(resource_metrics)
}

/// Collect an attribute iterator into a key-sorted owned vector so that the same
/// attribute set always hashes to the same series identity.
fn collect_attributes<A, I>(attrs: I) -> Vec<(String, OwnedVal)>
where
    A: AttributeView,
    I: Iterator<Item = A>,
{
    let mut out: Vec<(String, OwnedVal)> = attrs
        .map(|a| {
            let key = bytes_to_string(a.key());
            let val = a.value().map_or(OwnedVal::Empty, |v| owned_val(&v));
            (key, val)
        })
        .collect();
    out.sort_by(|l, r| l.0.cmp(&r.0));
    out
}

/// Convert an attribute value view into an owned, hashable value. Array and
/// kvlist values collapse to `Empty` (a PoC limitation).
fn owned_val<'a, V: AnyValueView<'a>>(v: &V) -> OwnedVal {
    match v.value_type() {
        ValueType::String => OwnedVal::Str(bytes_to_string(v.as_string().unwrap_or_default())),
        ValueType::Bool => OwnedVal::Bool(v.as_bool().unwrap_or(false)),
        ValueType::Int64 => OwnedVal::Int(v.as_int64().unwrap_or(0)),
        ValueType::Double => OwnedVal::Double(v.as_double().unwrap_or(0.0).to_bits()),
        ValueType::Bytes => OwnedVal::Bytes(v.as_bytes().unwrap_or_default().to_vec()),
        ValueType::Empty | ValueType::Array | ValueType::KeyValueList => OwnedVal::Empty,
    }
}

/// Rebuild an OTLP key-value pair from an owned identity attribute, dropping
/// empty values (which carry no OTLP representation here).
fn kv_of(pair: &(String, OwnedVal)) -> Option<KeyValue> {
    any_value_of(&pair.1).map(|v| KeyValue::new(pair.0.clone(), v))
}

/// Rebuild an OTLP value from an owned identity value.
fn any_value_of(v: &OwnedVal) -> Option<AnyValue> {
    Some(match v {
        OwnedVal::Str(s) => AnyValue::new_string(s.clone()),
        OwnedVal::Int(i) => AnyValue::new_int(*i),
        OwnedVal::Bool(b) => AnyValue::new_bool(*b),
        OwnedVal::Double(bits) => AnyValue::new_double(f64::from_bits(*bits)),
        OwnedVal::Bytes(b) => AnyValue::new_bytes(b.clone()),
        OwnedVal::Empty => return None,
    })
}

fn bytes_to_string(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Convert a [`Duration`] to nanoseconds as `i64`, saturating at [`i64::MAX`].
fn duration_to_nanos_i64(d: Duration) -> i64 {
    d.as_nanos().min(i64::MAX as u128) as i64
}

/// Current wall-clock time in nanoseconds since the Unix epoch (saturating).
fn unix_now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// The instrument kind of a series, part of its identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum SeriesKind {
    Sum { monotonic: bool },
    Gauge,
}

/// The owned identity of one metric stream: enough to bucket points by series
/// and to rebuild the OTLP output for a fired window.
#[derive(Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    resource: Vec<(String, OwnedVal)>,
    scope: String,
    metric: String,
    unit: String,
    kind: SeriesKind,
    point: Vec<(String, OwnedVal)>,
}

/// An owned, hashable attribute value (floats stored as bits for `Eq`/`Hash`).
#[derive(Clone, PartialEq, Eq, Hash)]
enum OwnedVal {
    Str(String),
    Int(i64),
    Bool(bool),
    Double(u64),
    Bytes(Vec<u8>),
    Empty,
}

/// The aggregate accumulated for one `(series, window)`.
#[derive(Default)]
struct WindowAgg {
    /// Running value: the delta-sum for sums, or the latest value for gauges.
    value: f64,
    /// Event time of the most recent fold (used for gauge last-value).
    last_time: i64,
    /// Whether any point has been folded in yet.
    has_value: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::config::ProcessorConfig;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::control::NodeControlMsg;
    use otap_df_engine::message::Message;
    use otap_df_engine::testing::processor::{TestPhase, TestRuntime};
    use otap_df_engine::testing::test_node;
    use otap_df_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use otap_df_telemetry::reporter::MetricsReporter;
    use serde_json::json;

    const SEC_NS: u64 = 1_000_000_000;

    fn setup(
        cfg: JsonValue,
    ) -> (
        TelemetryRegistryHandle,
        MetricsReporter,
        TestPhase<OtapPdata>,
    ) {
        let rt = TestRuntime::new();
        let registry = rt.metrics_registry();
        let reporter = rt.metrics_reporter();
        let controller = ControllerContext::new(registry.clone());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let node = test_node("event-time-window-test");
        let mut node_config = NodeUserConfig::new_processor_config(EVENT_TIME_WINDOW_PROCESSOR_URN);
        node_config.config = cfg;
        let proc_config = ProcessorConfig::new("event-time-window");
        let proc = create_event_time_window_processor(
            pipeline_ctx,
            node,
            Arc::new(node_config),
            &proc_config,
        )
        .expect("create processor");
        (registry, reporter, rt.set_processor(proc))
    }

    /// Build the windower as a load-reporting shuffle owner wired to `sender`.
    fn setup_owner(
        cfg: JsonValue,
        sender: LoadReportSender,
    ) -> (MetricsReporter, TestPhase<OtapPdata>) {
        let rt = TestRuntime::new();
        let registry = rt.metrics_registry();
        let reporter = rt.metrics_reporter();
        let controller = ControllerContext::new(registry);
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let node = test_node("event-time-window-owner");
        let mut node_config = NodeUserConfig::new_processor_config(EVENT_TIME_WINDOW_PROCESSOR_URN);
        node_config.config = cfg;
        let proc_config = ProcessorConfig::new("event-time-window-owner");
        let mut proc = EventTimeWindowProcessor::from_config(pipeline_ctx, &node_config.config)
            .expect("config");
        proc.set_load_report_sender(sender);
        let wrapper = ProcessorWrapper::local(proc, node, Arc::new(node_config), &proc_config);
        (reporter, rt.set_processor(wrapper))
    }

    fn sum_metric(name: &str, monotonic: bool, points: &[(u64, i64)]) -> Metric {
        Metric::build()
            .name(name)
            .data_sum(Sum::new(
                AggregationTemporality::Delta,
                monotonic,
                points
                    .iter()
                    .map(|&(t, v)| {
                        NumberDataPoint::build()
                            .time_unix_nano(t)
                            .value_int(v)
                            .finish()
                    })
                    .collect::<Vec<_>>(),
            ))
            .finish()
    }

    fn gauge_metric(name: &str, points: &[(u64, i64)]) -> Metric {
        Metric::build()
            .name(name)
            .data_gauge(Gauge::new(
                points
                    .iter()
                    .map(|&(t, v)| {
                        NumberDataPoint::build()
                            .time_unix_nano(t)
                            .value_int(v)
                            .finish()
                    })
                    .collect::<Vec<_>>(),
            ))
            .finish()
    }

    fn metrics_data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData::new(vec![ResourceMetrics::new(
            Resource::default(),
            vec![ScopeMetrics::new(
                InstrumentationScope::build().name("scope").finish(),
                metrics,
            )],
        )])
    }

    fn as_input(data: MetricsData) -> OtapPdata {
        let mut bytes = vec![];
        data.encode(&mut bytes).expect("encode metrics");
        OtapPdata::new_default(OtlpProtoBytes::ExportMetricsRequest(bytes.into()).into())
    }

    /// Build a metrics input batch carrying the shuffle partition tag `partition`.
    fn as_input_on(data: MetricsData, partition: u32) -> OtapPdata {
        let mut pdata = as_input(data);
        pdata.set_partition(partition);
        pdata
    }

    /// A batch of `count` distinct single-point sum series sharing a name prefix.
    fn distinct_sums(prefix: &str, count: usize, t: u64) -> MetricsData {
        let metrics = (0..count)
            .map(|i| sum_metric(&format!("{prefix}_{i}"), true, &[(t, 1)]))
            .collect::<Vec<_>>();
        metrics_data(metrics)
    }

    /// Decode an output batch into the sum of every number data point value and
    /// the total point count.
    fn output_values(pdata: &OtapPdata) -> (f64, usize) {
        let (_, payload) = pdata.clone().into_parts();
        let bytes: OtlpProtoBytes = payload.try_into_with_default().expect("otlp bytes out");
        let OtlpProtoBytes::ExportMetricsRequest(bytes) = bytes else {
            panic!("expected metrics output");
        };
        let data = MetricsData::decode(&bytes[..]).expect("decode output");
        let mut sum = 0.0;
        let mut count = 0;
        for rm in &data.resource_metrics {
            for sm in &rm.scope_metrics {
                for m in &sm.metrics {
                    let points = match &m.data {
                        Some(metric::Data::Sum(s)) => &s.data_points,
                        Some(metric::Data::Gauge(g)) => &g.data_points,
                        _ => continue,
                    };
                    for p in points {
                        count += 1;
                        if let Some(number_data_point::Value::AsDouble(d)) = p.value {
                            sum += d;
                        }
                    }
                }
            }
        }
        (sum, count)
    }

    use otap_df_pdata::proto::opentelemetry::metrics::v1::{metric, number_data_point};

    fn counter(registry: &TelemetryRegistryHandle, metric_name: &str) -> u64 {
        let mut found = 0u64;
        registry.visit_current_metrics(|desc, _attrs, iter| {
            if desc.name == "processor.event_time_window" {
                for (field, value) in iter {
                    if field.name == metric_name {
                        found = value.to_u64_lossy();
                    }
                }
            }
        });
        found
    }

    /// A counter point lands in an early window; a much later point on the same
    /// series advances the watermark and fires the early window, whose emitted
    /// aggregate is the delta-sum of the early points.
    #[test]
    fn fires_completed_window_with_summed_value() {
        let (registry, reporter, phase) =
            setup(json!({ "window_size": "10s", "allowed_lateness": "2s", "max_lag": "1h" }));

        phase
            .run_test(move |mut ctx| async move {
                // Align a base to a 10s window boundary near now.
                let now = unix_now_nanos() as u64;
                let base = (now / (10 * SEC_NS)) * (10 * SEC_NS);

                // Two points in window W0: delta-sum = 5 + 7 = 12.
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "c",
                    true,
                    &[(base, 5), (base + SEC_NS, 7)],
                )]))))
                .await
                .expect("process batch 1");
                let out1 = ctx.drain_pdata().await;
                assert!(out1.is_empty(), "W0 not complete after first batch");

                // A point 20s later (window W2) advances the watermark past W0.
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "c",
                    true,
                    &[(base + 20 * SEC_NS, 99)],
                )]))))
                .await
                .expect("process batch 2");
                let out2 = ctx.drain_pdata().await;
                assert_eq!(out2.len(), 1, "W0 fires after watermark advances");
                let (value, count) = output_values(&out2[0]);
                assert_eq!(count, 1, "one aggregated point for W0");
                assert_eq!(value, 12.0, "delta-sum of the early window");

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "points.aggregated"), 3);
                assert_eq!(counter(&registry, "windows.emitted"), 1);
                assert_eq!(counter(&registry, "batches.emitted"), 1);
                assert_eq!(counter(&registry, "batches"), 2);
            });
    }

    /// A point whose event time is below the stream watermark (its window has
    /// already fired) is dropped and counted as late.
    #[test]
    fn drops_and_counts_late_points() {
        let (registry, reporter, phase) =
            setup(json!({ "window_size": "10s", "allowed_lateness": "2s", "max_lag": "1h" }));

        phase
            .run_test(move |mut ctx| async move {
                let now = unix_now_nanos() as u64;
                let base = (now / (10 * SEC_NS)) * (10 * SEC_NS);

                // Establish a high watermark with a point far ahead.
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "c",
                    true,
                    &[(base + 50 * SEC_NS, 1)],
                )]))))
                .await
                .expect("process batch 1");
                _ = ctx.drain_pdata().await;

                // A point far below the watermark is late.
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "c",
                    true,
                    &[(base, 1)],
                )]))))
                .await
                .expect("process batch 2");
                _ = ctx.drain_pdata().await;

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "points.late"), 1);
                assert_eq!(counter(&registry, "points.aggregated"), 1);
            });
    }

    /// A gauge window keeps the latest value by event time, not a sum.
    #[test]
    fn gauge_keeps_last_value() {
        let (registry, reporter, phase) =
            setup(json!({ "window_size": "10s", "allowed_lateness": "2s", "max_lag": "1h" }));

        phase
            .run_test(move |mut ctx| async move {
                let now = unix_now_nanos() as u64;
                let base = (now / (10 * SEC_NS)) * (10 * SEC_NS);

                // Gauge points in W0: latest by event time (base+2s) wins -> 30.
                ctx.process(Message::PData(as_input(metrics_data(vec![gauge_metric(
                    "g",
                    &[(base, 10), (base + 2 * SEC_NS, 30), (base + SEC_NS, 20)],
                )]))))
                .await
                .expect("process batch 1");
                _ = ctx.drain_pdata().await;

                // Advance watermark to fire W0.
                ctx.process(Message::PData(as_input(metrics_data(vec![gauge_metric(
                    "g",
                    &[(base + 20 * SEC_NS, 99)],
                )]))))
                .await
                .expect("process batch 2");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1);
                let (value, count) = output_values(&out[0]);
                assert_eq!(count, 1);
                assert_eq!(value, 30.0, "gauge keeps the latest value");

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "windows.emitted"), 1);
            });
    }

    /// Non-metric signals pass through unchanged.
    #[test]
    fn passes_through_non_metrics() {
        let (_registry, _reporter, phase) = setup(json!({}));

        phase
            .run_test(move |mut ctx| async move {
                let logs =
                    OtapPdata::new_default(OtlpProtoBytes::ExportLogsRequest(vec![].into()).into());
                ctx.process(Message::PData(logs)).await.expect("process");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1, "logs forwarded unchanged");
                assert_eq!(out[0].signal_type(), SignalType::Logs);
            })
            .validate(|_| async {});
    }

    /// Closes the load feedback loop with a real aggregating owner: the windower
    /// measures each partition's live series count, reports it through a
    /// `LoadReportSender`, and the `PlacementScheduler` rebalances the live
    /// partition-dispatch topic. Owner 0's partitions carry far more series, so
    /// the scheduler sheds one of them to the idle owner 1.
    #[test]
    fn reports_per_partition_load_that_drives_a_rebalance() {
        use otap_df_engine::topic::{
            LoadWeights, PartitionDispatchBackend, PlacementScheduler, SubscriberOptions,
            SubscriptionMode, TopicBroker, TopicOptions,
        };
        use std::num::NonZeroUsize;

        // A live 4-partition shuffle topic, two owners: owner0 -> {0,1},
        // owner1 -> {2,3}.
        let broker = TopicBroker::<OtapPdata>::new();
        let topic = broker
            .create_topic(
                "shuffle",
                TopicOptions::default(),
                PartitionDispatchBackend {
                    num_partitions: 4,
                    capacity: 64,
                },
            )
            .expect("create topic");
        let _sub0 = topic
            .subscribe(
                SubscriptionMode::PartitionDispatch {
                    owned_partitions: vec![0, 1],
                },
                SubscriberOptions::default(),
            )
            .expect("owner0 subscribes");
        let _sub1 = topic
            .subscribe(
                SubscriptionMode::PartitionDispatch {
                    owned_partitions: vec![2, 3],
                },
                SubscriberOptions::default(),
            )
            .expect("owner1 subscribes");

        let mut scheduler = PlacementScheduler::new(
            topic,
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            LoadWeights::default(),
            1.2,
        );

        let (reporter, phase) = setup_owner(
            json!({ "window_size": "60s", "allowed_lateness": "2s", "max_lag": "1h" }),
            scheduler.report_sender(),
        );
        let now = unix_now_nanos() as u64;

        phase
            .run_test(move |mut ctx| async move {
                // Hot owner 0: 50 distinct series each on partitions 0 and 1.
                ctx.process(Message::PData(as_input_on(distinct_sums("p0", 50, now), 0)))
                    .await
                    .expect("p0");
                ctx.process(Message::PData(as_input_on(distinct_sums("p1", 50, now), 1)))
                    .await
                    .expect("p1");
                // Cold owner 1: one series each on partitions 2 and 3.
                ctx.process(Message::PData(as_input_on(distinct_sums("p2", 1, now), 2)))
                    .await
                    .expect("p2");
                ctx.process(Message::PData(as_input_on(distinct_sums("p3", 1, now), 3)))
                    .await
                    .expect("p3");
                // Report the measured per-partition load to the scheduler.
                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(|_| async {});

        // The scheduler consumes the owner's report and rebalances the topic.
        let moves = scheduler.tick().expect("scheduler tick");
        assert!(
            !moves.is_empty(),
            "skewed per-partition series counts must trigger a rebalance"
        );
        assert!(
            moves.iter().any(|m| m.from == 0),
            "the hot owner sheds a partition"
        );
        assert!(
            moves.iter().all(|m| m.to == 1),
            "shed load lands on the idle owner"
        );
    }
}
