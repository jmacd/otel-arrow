// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Partition (split-by-key) processor -- Layer A of the partition-dispatch
//! design (`docs/durable-dispatch-topic-design.md`).
//!
//! For every incoming OTAP batch of the configured key's target signal, this
//! processor splits the batch by key into `num_partitions` sub-batches such that
//! all rows sharing a key value land in the same partition, and emits each
//! sub-batch tagged with its integer partition index (carried on the request
//! [`Context`](otap_df_otap::pdata::Context)). A downstream partition-dispatch
//! hop (Layer C) routes each sub-batch to its owner by that tag without
//! re-deriving the key (the A->C contract, D25).
//!
//! Splitting reuses the OTAP selection-mask cascade
//! ([`partition_otap_batch`]); the per-signal key follows ingest-queue D3:
//! metrics hash the `name` column, traces slice the low 56 bits of `trace_id`.
//! Batches of any other signal pass through unchanged.
//!
//! # Acknowledgement semantics (prototype)
//!
//! This in-memory node (durable-dispatch D28) preserves the single-output
//! ack/nack behavior of the surrounding processors: the request context (its
//! ack/nack subscribers) is carried on the **first** emitted partition only;
//! the remaining partitions carry a detached context that still propagates
//! transport metadata (tenant headers, peer address) but no ack subscription.
//! Full per-partition ack/nack fan-in -- acking the input once *all* partitions
//! resolve -- is deferred to the dispatch/slot integration (D22), matching the
//! loss-tolerant default of the in-memory profile (ingest-queue D6).

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::process_duration::ComputeDuration;
use otap_df_engine::processor::{ProcessorRuntimeRequirements, ProcessorWrapper};
use otap_df_otap::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otap::OtapArrowRecords;
use otap_df_pdata::otap::filter::IdBitmapPool;
use otap_df_pdata::otap::partition::{PartitionKey, partition_otap_batch};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::otel_warn;
use serde_json::Value;

mod config;
mod telemetry;

use self::config::Config;
use self::telemetry::{PARTITION_FAILED_EVENT, PartitionMetrics};

/// The URN for the partition processor.
pub const PARTITION_PROCESSOR_URN: &str = "urn:otel:processor:partition";

/// Factory function to create a [`PartitionProcessor`].
pub fn create_partition_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        PartitionProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the partition processor as an OTAP processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static PARTITION_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: PARTITION_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
                create_partition_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
    };

/// The partition (split-by-key) processor.
pub struct PartitionProcessor {
    /// Processor metrics.
    metrics: MetricSet<PartitionMetrics>,
    /// Accumulated compute-time telemetry.
    compute_duration: ComputeDuration,
    /// The pdata partition key (which column and partition function).
    key: PartitionKey,
    /// The signal this processor splits; other signals pass through.
    target_signal: SignalType,
    /// The number of partitions `N` (validated power of two).
    num_partitions: NonZeroUsize,
    /// Reusable paged-bitmap pool for cascade filtering across batches.
    id_pool: IdBitmapPool,
}

impl PartitionProcessor {
    /// Create a processor from a configuration JSON value.
    pub fn from_config(pipeline_ctx: PipelineContext, config: &Value) -> Result<Self, ConfigError> {
        let metrics = pipeline_ctx.register_metrics::<PartitionMetrics>();
        let compute_duration = ComputeDuration::new(&pipeline_ctx);
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: e.to_string(),
            })?;
        config.validate()?;
        Ok(Self {
            metrics,
            compute_duration,
            key: config.key.pdata_key(),
            target_signal: config.key.target_signal(),
            num_partitions: config.num_partitions,
            id_pool: IdBitmapPool::new(),
        })
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for PartitionProcessor {
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
                // Batches of any other signal pass through unchanged.
                if pdata.signal_type() != self.target_signal {
                    self.metrics.passthrough_batches.inc();
                    return effect_handler
                        .send_message_with_source_node(pdata)
                        .await
                        .map_err(Into::into);
                }

                let (context, payload) = pdata.into_parts();
                let mut records: OtapArrowRecords = payload.try_into_with_default()?;
                records.decode_transport_optimized_ids()?;

                let key = self.key;
                let n = self.num_partitions;
                let pool = &mut self.id_pool;
                let split = effect_handler.timed(&self.compute_duration, || {
                    partition_otap_batch(&records, key, n, pool)
                });

                let parts = match split {
                    Ok(parts) => parts,
                    Err(e) => {
                        // Cannot split (e.g. a missing or malformed key column):
                        // forward unchanged rather than drop.
                        self.metrics.malformed_batches.inc();
                        otel_warn!(PARTITION_FAILED_EVENT, error = %e);
                        return effect_handler
                            .send_message_with_source_node(OtapPdata::new(context, records.into()))
                            .await
                            .map_err(Into::into);
                    }
                };

                // An empty batch (no root rows) yields no partitions; forward it
                // unchanged so the request's ack/nack still propagates upstream.
                if parts.is_empty() {
                    return effect_handler
                        .send_message_with_source_node(OtapPdata::new(context, records.into()))
                        .await
                        .map_err(Into::into);
                }

                self.metrics.batches.inc();

                // Emit each partition tagged with its index. The request context
                // (ack/nack subscribers) rides the first partition only; the rest
                // carry a detached, transport-metadata-preserving context (see
                // the module-level acknowledgement note).
                for (idx, (partition, sub)) in parts.into_iter().enumerate() {
                    let mut out = OtapPdata::new(context.clone(), sub.into());
                    if idx != 0 {
                        out = out.clone_without_context();
                    }
                    out.set_partition(u32::try_from(partition).unwrap_or(u32::MAX));
                    self.metrics.sub_batches.inc();
                    effect_handler.send_message_with_source_node(out).await?;
                }
                Ok(())
            }
        }
    }

    fn runtime_requirements(&self) -> ProcessorRuntimeRequirements {
        ProcessorRuntimeRequirements::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::config::ProcessorConfig;
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::message::Message;
    use otap_df_engine::testing::processor::{TestPhase, TestRuntime};
    use otap_df_engine::testing::test_node;
    use otap_df_pdata::OtlpProtoBytes;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otap_df_pdata::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue,
    };
    use otap_df_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otap_df_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::views::otap::OtapMetricsView;
    use otap_df_pdata_views::views::metrics::{
        MetricView, MetricsView, ResourceMetricsView, ScopeMetricsView,
    };
    use prost::Message as _;
    use serde_json::json;

    fn setup(cfg: Value) -> TestPhase<OtapPdata> {
        let rt = TestRuntime::new();
        let registry = rt.metrics_registry();
        let controller = ControllerContext::new(registry.clone());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let node = test_node("partition-test");
        let mut node_config = NodeUserConfig::new_processor_config(PARTITION_PROCESSOR_URN);
        node_config.config = cfg;
        let proc_config = ProcessorConfig::new("partition");
        let proc =
            create_partition_processor(pipeline_ctx, node, Arc::new(node_config), &proc_config)
                .expect("create processor");
        rt.set_processor(proc)
    }

    fn sum_metric(name: &str) -> Metric {
        Metric::build()
            .name(name)
            .data_sum(Sum::new(
                AggregationTemporality::Cumulative,
                true,
                vec![
                    NumberDataPoint::build()
                        .time_unix_nano(1u64)
                        .value_int(1)
                        .attributes(vec![KeyValue::new("k", AnyValue::new_string("v"))])
                        .finish(),
                ],
            ))
            .finish()
    }

    fn metrics_input(names: &[&str]) -> OtapPdata {
        let data = MetricsData::new(vec![ResourceMetrics::new(
            Resource::default(),
            vec![ScopeMetrics::new(
                InstrumentationScope::build()
                    .name("scope".to_string())
                    .finish(),
                names.iter().map(|n| sum_metric(n)).collect::<Vec<_>>(),
            )],
        )]);
        let mut bytes = vec![];
        data.encode(&mut bytes).expect("encode metrics");
        OtapPdata::new_default(OtlpProtoBytes::ExportMetricsRequest(bytes.into()).into())
    }

    fn logs_input() -> OtapPdata {
        let data = LogsData::new(vec![ResourceLogs::new(
            Resource::default(),
            vec![ScopeLogs::new(
                InstrumentationScope::build()
                    .name("scope".to_string())
                    .finish(),
                vec![LogRecord::build().time_unix_nano(1u64).finish()],
            )],
        )]);
        let mut bytes = vec![];
        data.encode(&mut bytes).expect("encode logs");
        OtapPdata::new_default(OtlpProtoBytes::ExportLogsRequest(bytes.into()).into())
    }

    fn names_of(pdata: &OtapPdata) -> Vec<String> {
        let (_, payload) = pdata.clone().into_parts();
        let records: OtapArrowRecords = payload.try_into_with_default().expect("to otap");
        let view = OtapMetricsView::try_from(&records).expect("metrics view");
        let mut names = Vec::new();
        for rm in view.resources() {
            for sm in rm.scopes() {
                for m in sm.metrics() {
                    names.push(String::from_utf8_lossy(m.name()).into_owned());
                }
            }
        }
        names
    }

    #[test]
    fn splits_metrics_into_tagged_partitions() {
        let phase = setup(json!({ "key": "metric_name", "num_partitions": 4 }));
        phase
            .run_test(move |mut ctx| async move {
                let names = [
                    "http.requests",
                    "cpu.usage",
                    "mem.free",
                    "disk.io",
                    "net.rx",
                ];
                ctx.process(Message::PData(metrics_input(&names)))
                    .await
                    .expect("process");
                let out = ctx.drain_pdata().await;

                // Every emitted sub-batch carries a partition tag, and the union of
                // their metric names reconstructs the input (co-location: no name in
                // two partitions).
                assert!(!out.is_empty());
                let mut all_names: Vec<String> = Vec::new();
                let mut seen_partitions: Vec<u32> = Vec::new();
                for sub in &out {
                    let p = sub.partition().expect("partition tag set");
                    assert!(p < 4, "partition in range");
                    seen_partitions.push(p);
                    all_names.extend(names_of(sub));
                }
                all_names.sort();
                let mut want: Vec<String> = names.iter().map(|s| s.to_string()).collect();
                want.sort();
                assert_eq!(all_names, want, "all names accounted for exactly once");

                // Distinct partition tags (one sub-batch per present partition).
                let mut sorted = seen_partitions.clone();
                sorted.sort_unstable();
                sorted.dedup();
                assert_eq!(sorted.len(), seen_partitions.len(), "partitions distinct");
            })
            .validate(|_| async {});
    }

    #[test]
    fn single_partition_emits_one_tagged_batch() {
        let phase = setup(json!({ "key": "metric_name", "num_partitions": 1 }));
        phase
            .run_test(move |mut ctx| async move {
                ctx.process(Message::PData(metrics_input(&["a", "b", "c"])))
                    .await
                    .expect("process");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1);
                assert_eq!(out[0].partition(), Some(0));
                let mut names = names_of(&out[0]);
                names.sort();
                assert_eq!(
                    names,
                    vec!["a".to_string(), "b".to_string(), "c".to_string()]
                );
            })
            .validate(|_| async {});
    }

    #[test]
    fn passes_through_non_target_signal() {
        let phase = setup(json!({ "key": "metric_name", "num_partitions": 4 }));
        phase
            .run_test(move |mut ctx| async move {
                ctx.process(Message::PData(logs_input()))
                    .await
                    .expect("process");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1, "logs forwarded unchanged as one batch");
                assert_eq!(out[0].signal_type(), SignalType::Logs);
                assert_eq!(out[0].partition(), None, "passthrough carries no tag");
            })
            .validate(|_| async {});
    }

    #[test]
    fn empty_metrics_batch_forwarded_unchanged() {
        let phase = setup(json!({ "key": "metric_name", "num_partitions": 4 }));
        phase
            .run_test(move |mut ctx| async move {
                // A metrics batch with no metrics yields no partitions; it is
                // forwarded unchanged so the request's ack/nack still flows.
                let data = MetricsData::new(vec![]);
                let mut bytes = vec![];
                data.encode(&mut bytes).expect("encode metrics");
                let input = OtapPdata::new_default(
                    OtlpProtoBytes::ExportMetricsRequest(bytes.into()).into(),
                );
                ctx.process(Message::PData(input)).await.expect("process");
                let out = ctx.drain_pdata().await;
                assert_eq!(out.len(), 1, "empty batch forwarded as one batch");
                assert_eq!(out[0].partition(), None, "forwarded empty carries no tag");
            })
            .validate(|_| async {});
    }

    #[test]
    fn data_points_conserved_across_partitions() {
        let phase = setup(json!({ "key": "metric_name", "num_partitions": 8 }));
        phase
            .run_test(move |mut ctx| async move {
                let names = ["a", "b", "c", "d", "e", "f", "g"];
                ctx.process(Message::PData(metrics_input(&names)))
                    .await
                    .expect("process");
                let out = ctx.drain_pdata().await;

                let total_dp: usize = out
                    .iter()
                    .map(|sub| {
                        let (_, payload) = sub.clone().into_parts();
                        let r: OtapArrowRecords = payload.try_into_with_default().unwrap();
                        r.get(ArrowPayloadType::NumberDataPoints)
                            .map(|rb| rb.num_rows())
                            .unwrap_or(0)
                    })
                    .sum();
                // One data point per metric, conserved across the split.
                assert_eq!(total_dp, names.len());
            })
            .validate(|_| async {});
    }
}
