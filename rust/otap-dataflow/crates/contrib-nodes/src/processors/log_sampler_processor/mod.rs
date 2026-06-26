// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local log sampler processor (SDK side of two-level log sampling).
//!
//! Maintains a weighted bottom-`(k+1)` reservoir over a fixed time window
//! (see [`bottomk`]). Incoming log batches are absorbed into the reservoir
//! and acknowledged; on each window-close [`NodeControlMsg::TimerTick`] the
//! kept representatives are emitted downstream, each annotated with its
//! Horvitz-Thompson count under the [`bottomk::OTEL_SAMPLING_NHAT`] attribute.
//!
//! Non-log signals pass through unchanged.
//!
//! This is milestone M0 of the feedback design: the global gate is always
//! slack (`tau^G = +inf`), so admission is purely local. Later milestones add
//! the global reservoir and the feedback return path.

mod bottomk;
mod metrics;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::Deserialize;

use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ConsumerEffectHandlerExtension;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NodeControlMsg};
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;
use otap_df_otap::pdata::{Context, OtapPdata};
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::OtapPayload;
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_telemetry::metrics::MetricSet;

use self::bottomk::WindowSampler;
use self::metrics::LogSamplerMetrics;

/// URN identifier for the local log sampler processor.
const LOG_SAMPLER_PROCESSOR_URN: &str = "urn:otel:processor:log_sampler";

/// Configuration for the local log sampler processor.
#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// Number of representatives retained per window (`k`).
    k: usize,
    /// Length of the sampling window (e.g. "5s", "1m").
    #[serde(with = "humantime_serde")]
    interval: Duration,
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.k == 0 {
            return Err(ConfigError::InvalidUserConfig {
                error: "log_sampler.k must be greater than 0".to_string(),
            });
        }
        if self.interval.is_zero() {
            return Err(ConfigError::InvalidUserConfig {
                error: "log_sampler.interval must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// Local log sampler processor.
struct LogSamplerProcessor {
    sampler: WindowSampler,
    interval: Duration,
    timer_started: bool,
    metrics: MetricSet<LogSamplerMetrics>,
}

impl LogSamplerProcessor {
    fn from_config(pipeline_ctx: PipelineContext, config: &serde_json::Value) -> Result<Self, ConfigError> {
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: e.to_string(),
            })?;
        config.validate()?;

        let metrics = pipeline_ctx.register_metrics::<LogSamplerMetrics>();
        Ok(Self {
            sampler: WindowSampler::new(config.k),
            interval: config.interval,
            timer_started: false,
            metrics,
        })
    }

    /// Decode an incoming log payload, absorb its records into the reservoir,
    /// and acknowledge the input (sampling takes ownership of the data).
    async fn absorb_logs(
        &mut self,
        pdata: OtapPdata,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let total = pdata.num_items();
        self.metrics.log_signals_consumed.add(total as u64);

        let (context, payload) = pdata.into_parts();
        let otlp: OtlpProtoBytes = {
            let res: Result<OtlpProtoBytes, _> = payload.try_into_with_default();
            res.map_err(|e| EngineError::PdataConversionError {
                error: e.to_string(),
            })?
        };
        let logs = match OtlpProtoMessage::try_from(otlp) {
            Ok(OtlpProtoMessage::Logs(logs)) => logs,
            Ok(_) => unreachable!("signal_type was Logs"),
            Err(e) => {
                self.metrics.decode_errors.inc();
                return Err(EngineError::PdataConversionError {
                    error: e.to_string(),
                });
            }
        };

        let _ = self.sampler.observe_logs(logs);

        // The data now lives in the reservoir; acknowledge the input so the
        // upstream sender's delivery accounting completes.
        effect_handler
            .notify_ack(AckMsg::new(OtapPdata::new(
                context,
                OtapPayload::empty(SignalType::Logs),
            )))
            .await?;
        Ok(())
    }

    /// Close the current window and emit representatives downstream.
    async fn close_window(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let Some((logs, stats)) = self.sampler.close_window() else {
            return Ok(());
        };

        let mut buf = Vec::new();
        {
            use prost::Message as _;
            logs.encode(&mut buf)
                .map_err(|e| EngineError::InternalError {
                    message: format!("failed to encode sampled logs: {e}"),
                })?;
        }

        self.metrics
            .representatives_emitted
            .add(stats.representatives);
        self.metrics.windows_closed.inc();
        self.metrics
            .last_distinct_callsites
            .set(stats.distinct_callsites);
        // tau^L is +inf when the reservoir never filled; report 0 in that
        // case so the gauge stays finite.
        self.metrics
            .last_tau_l
            .set(if stats.tau_l.is_finite() { stats.tau_l } else { 0.0 });

        let payload = OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(buf.into()));
        let pdata = OtapPdata::new(Context::default(), payload);
        effect_handler.send_message_with_source_node(pdata).await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for LogSamplerProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        if !self.timer_started {
            let _handle = effect_handler.start_periodic_timer(self.interval).await?;
            self.timer_started = true;
        }

        match msg {
            Message::PData(pdata) => match pdata.signal_type() {
                SignalType::Logs => self.absorb_logs(pdata, effect_handler).await,
                SignalType::Metrics | SignalType::Traces => {
                    effect_handler.send_message_with_source_node(pdata).await?;
                    Ok(())
                }
            },
            Message::Control(ctrl) => match ctrl {
                NodeControlMsg::TimerTick {} => self.close_window(effect_handler).await,
                NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } => {
                    let _ = metrics_reporter.report(&mut self.metrics);
                    Ok(())
                }
                NodeControlMsg::Shutdown { .. } => {
                    // Flush any buffered representatives before stopping.
                    self.close_window(effect_handler).await
                }
                NodeControlMsg::Config { .. }
                | NodeControlMsg::Ack(_)
                | NodeControlMsg::Nack(_)
                | NodeControlMsg::MemoryPressureChanged { .. }
                | NodeControlMsg::DrainIngress { .. }
                | NodeControlMsg::Wakeup { .. }
                | NodeControlMsg::DelayedData { .. } => Ok(()),
            },
        }
    }
}

/// Factory function to create a local log sampler processor.
fn create_log_sampler_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        LogSamplerProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the local log sampler processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static LOG_SAMPLER_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: LOG_SAMPLER_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
                create_log_sampler_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    };
