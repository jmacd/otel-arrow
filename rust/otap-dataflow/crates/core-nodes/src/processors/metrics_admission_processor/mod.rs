// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics admission processor (ingest-queue phase 0).
//!
//! This processor is the admission front-end of the vertically-integrated
//! ingest queue described in `docs/ingest-queue-design.md`. For every incoming
//! OTAP metrics batch it:
//!
//! 1. **Records type identity.** Each metric's canonical type descriptor
//!    (instrument type, monotonicity, unit) is recorded in a per-core
//!    [`TypeRegistry`]. The first descriptor seen for a name becomes its primary;
//!    a later, differing descriptor is a *name conflict*. By default both streams
//!    are admitted and a `name_conflict` warning is emitted once (matching the
//!    OpenTelemetry metrics SDK's instrument-conflict handling). An optional
//!    strict mode rejects the conflicting points instead.
//! 2. **Applies the admission time window.** Data points whose event time falls
//!    outside `[now - max_lag, now + max_skew]` are dropped and counted
//!    (`too_old` / `too_future` / `malformed`). This crude, protective bound
//!    caps backfill into durable storage and rejects bad future clocks; the
//!    downstream event-time windowing layer does the fine-grained completeness
//!    estimation within this envelope.
//!
//! Admitted data is forwarded downstream unchanged in representation (OTAP in,
//! OTAP out). Temporality and value type are *non-manifesting*: a difference in
//! either is never treated as a conflict. Non-metric signals pass through.
//!
//! This phase-0 implementation is single-core: the registry lives in memory, one
//! per processor replica. The thread-per-core model and (phase 1) shuffle by
//! metric name make each core the sole authority for its names, so no cross-core
//! coordination is needed.

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
use otap_df_engine::error::{Error, ProcessorErrorKind, format_error_sources};
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::process_duration::ComputeDuration;
use otap_df_engine::processor::{ProcessorRuntimeRequirements, ProcessorWrapper};
use otap_df_otap::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::otap::filter::metrics::{MetricFilter, MetricMatchProperties};
use otap_df_pdata::otap::filter::{IdBitmapPool, MatchType, filter_metrics_time_window};
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otap_df_pdata::views::otap::OtapMetricsView;
use otap_df_pdata_views::views::metrics::{
    DataType, DataView, MetricView, MetricsView, ResourceMetricsView, ScopeMetricsView, SumView,
};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::otel_warn;
use serde_json::Value;

mod config;
mod registry;
mod telemetry;

use self::config::Config;
use self::registry::{Observation, TypeDescriptor, TypeRegistry};
use self::telemetry::{
    MetricsAdmissionMetrics, NAME_CONFLICT_EVENT, VIEW_CREATION_FAILED_EVENT,
    WINDOW_FILTER_FAILED_EVENT,
};

/// The URN for the metrics admission processor.
pub const METRICS_ADMISSION_PROCESSOR_URN: &str = "urn:otel:processor:metrics_admission";

/// Factory function to create a [`MetricsAdmissionProcessor`].
pub fn create_metrics_admission_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        MetricsAdmissionProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the metrics admission processor as an OTAP processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static METRICS_ADMISSION_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: METRICS_ADMISSION_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
                create_metrics_admission_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
    };

/// The metrics admission processor.
pub struct MetricsAdmissionProcessor {
    /// Processor metrics.
    metrics: MetricSet<MetricsAdmissionMetrics>,
    /// Accumulated compute-time telemetry.
    compute_duration: ComputeDuration,
    /// Admission window lower bound offset (`now - max_lag`), in nanoseconds.
    max_lag_nanos: i64,
    /// Admission window upper bound offset (`now + max_skew`), in nanoseconds.
    max_skew_nanos: i64,
    /// When true, reject the data points of metrics that conflict with the
    /// recorded primary descriptor instead of admitting and warning.
    strict_conflicts: bool,
    /// The per-core type registry (manifest).
    registry: TypeRegistry,
    /// Reusable paged-bitmap pool for cascade filtering across batches.
    id_pool: IdBitmapPool,
}

impl MetricsAdmissionProcessor {
    /// Create a processor from a configuration JSON value.
    pub fn from_config(pipeline_ctx: PipelineContext, config: &Value) -> Result<Self, ConfigError> {
        let metrics = pipeline_ctx.register_metrics::<MetricsAdmissionMetrics>();
        let compute_duration = ComputeDuration::new(&pipeline_ctx);
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: e.to_string(),
            })?;
        config.validate()?;
        Ok(Self {
            metrics,
            compute_duration,
            max_lag_nanos: duration_to_nanos_i64(config.max_lag),
            max_skew_nanos: duration_to_nanos_i64(config.max_skew),
            strict_conflicts: config.strict_conflicts,
            registry: TypeRegistry::new(),
            id_pool: IdBitmapPool::new(),
        })
    }

    /// Record every metric's observed `(name, descriptor)` against the registry,
    /// emitting a one-shot warning per newly-conflicting name. When strict mode
    /// is enabled, returns the names whose descriptor conflicts with the recorded
    /// primary (so their points can be dropped); otherwise returns an empty list.
    fn record_identities<V: MetricsView>(&mut self, view: &V) -> Vec<String> {
        let mut conflicts: Vec<String> = Vec::new();
        for resource_metrics in view.resources() {
            for scope_metrics in resource_metrics.scopes() {
                for metric in scope_metrics.metrics() {
                    let Some(descriptor) = descriptor_of(&metric) else {
                        continue;
                    };
                    let name = metric.name();
                    match self.registry.observe(name, &descriptor) {
                        Observation::Registered => self.metrics.names_registered.inc(),
                        Observation::Matched => {}
                        Observation::NewConflict => {
                            self.metrics.name_conflicts.inc();
                            otel_warn!(
                                NAME_CONFLICT_EVENT,
                                metric_name = %String::from_utf8_lossy(name)
                            );
                            if self.strict_conflicts {
                                conflicts.push(String::from_utf8_lossy(name).into_owned());
                            }
                        }
                        Observation::KnownConflict => {
                            if self.strict_conflicts {
                                conflicts.push(String::from_utf8_lossy(name).into_owned());
                            }
                        }
                    }
                }
            }
        }
        conflicts
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for MetricsAdmissionProcessor {
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
                let (context, payload) = pdata.into_parts();
                let mut records: OtapArrowRecords = payload.try_into_with_default()?;
                records.decode_transport_optimized_ids()?;

                // 1. Record observed identities into the per-core registry.
                let conflict_names = match OtapMetricsView::try_from(&records) {
                    Ok(view) => self.record_identities(&view),
                    Err(e) => {
                        self.metrics.batches_malformed.inc();
                        otel_warn!(VIEW_CREATION_FAILED_EVENT, error = %e);
                        Vec::new()
                    }
                };

                // 2. Apply the event-time admission window.
                let now = unix_now_nanos();
                let lo = now.saturating_sub(self.max_lag_nanos);
                let hi = now.saturating_add(self.max_skew_nanos);

                let admitted =
                    effect_handler.timed(&self.compute_duration, || -> Result<_, Error> {
                        let (mut out, counts) =
                            match filter_metrics_time_window(&records, lo, hi, &mut self.id_pool) {
                                Ok(x) => x,
                                Err(e) => {
                                    // Cannot time-filter (e.g. malformed timestamp column):
                                    // forward unchanged rather than drop.
                                    self.metrics.batches_malformed.inc();
                                    otel_warn!(WINDOW_FILTER_FAILED_EVENT, error = %e);
                                    return Ok(records.clone());
                                }
                            };
                        self.metrics.points_rejected_too_old.add(counts.too_old);
                        self.metrics
                            .points_rejected_too_future
                            .add(counts.too_future);
                        self.metrics.points_rejected_malformed.add(counts.malformed);

                        // 3. Strict conflict mode: drop the data points of metrics
                        //    whose descriptor conflicts with the recorded primary.
                        if self.strict_conflicts && !conflict_names.is_empty() {
                            let exclude = MetricMatchProperties::new(
                                MatchType::Strict,
                                conflict_names.clone(),
                            );
                            let (dropped, _consumed, _filtered) =
                                MetricFilter::new(None, Some(exclude))
                                    .filter(out, &mut self.id_pool)
                                    .map_err(|e| Error::ProcessorError {
                                        processor: processor_id.clone(),
                                        kind: ProcessorErrorKind::Other,
                                        error: format!("strict conflict filter failed: {e}"),
                                        source_detail: format_error_sources(&e),
                                    })?;
                            out = dropped;
                        }

                        let admitted_points = total_data_point_rows(&out);
                        self.metrics.points_admitted.add(admitted_points);
                        let conflict_dropped = counts.kept.saturating_sub(admitted_points);
                        if conflict_dropped > 0 {
                            self.metrics.points_rejected_conflict.add(conflict_dropped);
                        }
                        Ok(out)
                    })?;

                self.metrics.batches.inc();
                effect_handler
                    .send_message_with_source_node(OtapPdata::new(context, admitted.into()))
                    .await
                    .map_err(Into::into)
            }
        }
    }

    fn runtime_requirements(&self) -> ProcessorRuntimeRequirements {
        ProcessorRuntimeRequirements::none()
    }
}

/// Extract the manifesting type descriptor (instrument type, monotonicity, unit)
/// from a metric view, or `None` if the metric carries no data view.
fn descriptor_of<M: MetricView>(metric: &M) -> Option<TypeDescriptor> {
    let data = metric.data()?;
    let dt = data.value_type();
    let is_monotonic = match dt {
        DataType::Sum => data.as_sum().map(|s| s.is_monotonic()).unwrap_or(false),
        DataType::Histogram | DataType::ExponentialHistogram => true,
        DataType::Gauge | DataType::Summary => false,
    };
    Some(TypeDescriptor {
        instrument_type: dt as u8,
        is_monotonic,
        unit: metric.unit().to_vec().into_boxed_slice(),
    })
}

/// Total number of data-point rows across all metric families in a batch.
fn total_data_point_rows(records: &OtapArrowRecords) -> u64 {
    [
        ArrowPayloadType::NumberDataPoints,
        ArrowPayloadType::SummaryDataPoints,
        ArrowPayloadType::HistogramDataPoints,
        ArrowPayloadType::ExpHistogramDataPoints,
    ]
    .iter()
    .map(|&pt| records.get(pt).map(|rb| rb.num_rows() as u64).unwrap_or(0))
    .sum()
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

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::config::ProcessorConfig;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::control::NodeControlMsg;
    use otap_df_engine::message::Message;
    use otap_df_engine::testing::processor::{TestPhase, TestRuntime};
    use otap_df_engine::testing::test_node;
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use otap_df_telemetry::reporter::MetricsReporter;
    use prost::Message as _;
    use serde_json::json;

    const HOUR_NS: u64 = 3_600_000_000_000;
    const MINUTE_NS: u64 = 60_000_000_000;

    fn setup(
        cfg: Value,
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
        let node = test_node("metrics-admission-test");
        let mut node_config = NodeUserConfig::new_processor_config(METRICS_ADMISSION_PROCESSOR_URN);
        node_config.config = cfg;
        let proc_config = ProcessorConfig::new("metrics-admission");
        let proc = create_metrics_admission_processor(
            pipeline_ctx,
            node,
            Arc::new(node_config),
            &proc_config,
        )
        .expect("create processor");
        (registry, reporter, rt.set_processor(proc))
    }

    fn sum_metric(name: &str, points: &[(u64, i64)]) -> Metric {
        Metric::build()
            .name(name)
            .data_sum(Sum::new(
                AggregationTemporality::Cumulative,
                true,
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

    fn gauge_metric(name: &str, t: u64, v: i64) -> Metric {
        Metric::build()
            .name(name)
            .data_gauge(Gauge::new(vec![
                NumberDataPoint::build()
                    .time_unix_nano(t)
                    .value_int(v)
                    .finish(),
            ]))
            .finish()
    }

    fn metrics_data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData::new(vec![ResourceMetrics::new(
            Resource::default(),
            vec![ScopeMetrics::new(
                InstrumentationScope::build()
                    .name("scope".to_string())
                    .finish(),
                metrics,
            )],
        )])
    }

    fn as_input(data: MetricsData) -> OtapPdata {
        let mut bytes = vec![];
        data.encode(&mut bytes).expect("encode metrics");
        OtapPdata::new_default(OtlpProtoBytes::ExportMetricsRequest(bytes.into()).into())
    }

    fn output_point_count(pdata: &OtapPdata) -> u64 {
        let (_, payload) = pdata.clone().into_parts();
        let records: OtapArrowRecords = payload.try_into_with_default().expect("to otap");
        total_data_point_rows(&records)
    }

    fn counter(registry: &TelemetryRegistryHandle, metric_name: &str) -> u64 {
        let mut found = 0u64;
        registry.visit_current_metrics(|desc, _attrs, iter| {
            if desc.name == "processor.metrics_admission" {
                for (field, value) in iter {
                    if field.name == metric_name {
                        found = value.to_u64_lossy();
                    }
                }
            }
        });
        found
    }

    /// End-to-end: too-old and too-future points are rejected; the in-window
    /// point is admitted, and the rejection telemetry reflects each reason.
    #[test]
    fn admission_window_rejects_out_of_window_points() {
        let (registry, reporter, phase) = setup(json!({ "max_lag": "1h", "max_skew": "5m" }));

        phase
            .run_test(move |mut ctx| async move {
                let now = unix_now_nanos() as u64;
                let input = as_input(metrics_data(vec![sum_metric(
                    "test.counter",
                    &[
                        (now, 1),                  // in window -> admitted
                        (now - 2 * HOUR_NS, 2),    // older than now - 1h -> too_old
                        (now + 10 * MINUTE_NS, 3), // further than now + 5m -> too_future
                    ],
                )]));

                ctx.process(Message::PData(input)).await.expect("process");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1, "one output batch");
                assert_eq!(
                    output_point_count(&out[0]),
                    1,
                    "only the in-window point survives"
                );

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_vctx| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "points.admitted"), 1);
                assert_eq!(counter(&registry, "points.rejected.too.old"), 1);
                assert_eq!(counter(&registry, "points.rejected.too.future"), 1);
                assert_eq!(counter(&registry, "names.registered"), 1);
                assert_eq!(counter(&registry, "batches"), 1);
            });
    }

    /// End-to-end: a name observed with two different instrument types is a
    /// conflict. The default policy admits both streams and warns once.
    #[test]
    fn admit_all_warns_on_name_conflict() {
        let (registry, reporter, phase) = setup(json!({ "max_lag": "1h", "max_skew": "5m" }));

        phase
            .run_test(move |mut ctx| async move {
                let now = unix_now_nanos() as u64;
                // First: "x" as a monotonic sum (becomes the primary descriptor).
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "x",
                    &[(now, 1)],
                )]))))
                .await
                .expect("process sum");
                // Then: "x" as a gauge -> conflicting descriptor, still admitted.
                ctx.process(Message::PData(as_input(metrics_data(vec![gauge_metric(
                    "x", now, 2,
                )]))))
                .await
                .expect("process gauge");

                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 2, "both batches admitted");
                assert_eq!(output_point_count(&out[0]), 1);
                assert_eq!(output_point_count(&out[1]), 1);

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_vctx| async move {
                // One name recorded; one conflict observed; nothing rejected.
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "names.registered"), 1);
                assert_eq!(counter(&registry, "name.conflicts"), 1);
                assert_eq!(counter(&registry, "points.admitted"), 2);
                assert_eq!(counter(&registry, "points.rejected.conflict"), 0);
            });
    }

    /// End-to-end: strict mode rejects the points of the conflicting metric.
    #[test]
    fn strict_mode_rejects_conflicting_points() {
        let (registry, reporter, phase) =
            setup(json!({ "max_lag": "1h", "max_skew": "5m", "strict_conflicts": true }));

        phase
            .run_test(move |mut ctx| async move {
                let now = unix_now_nanos() as u64;
                // Establish the primary (sum).
                ctx.process(Message::PData(as_input(metrics_data(vec![sum_metric(
                    "x",
                    &[(now, 1)],
                )]))))
                .await
                .expect("process sum");
                // Conflicting gauge: its point must be rejected in strict mode.
                ctx.process(Message::PData(as_input(metrics_data(vec![gauge_metric(
                    "x", now, 2,
                )]))))
                .await
                .expect("process gauge");

                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 2);
                assert_eq!(output_point_count(&out[0]), 1, "primary admitted");
                assert_eq!(output_point_count(&out[1]), 0, "conflicting point rejected");

                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter.clone(),
                }))
                .await
                .expect("collect telemetry");
            })
            .validate(move |_vctx| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                assert_eq!(counter(&registry, "points.rejected.conflict"), 1);
                assert_eq!(counter(&registry, "name.conflicts"), 1);
            });
    }
}
