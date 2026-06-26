// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local log sampler processor (SDK side of two-level log sampling).
//!
//! Maintains a weighted bottom-`(k+1)` reservoir over a fixed time window
//! (see [`bottomk`]). Incoming log batches are filtered by the binding gate
//! and absorbed into the reservoir, then acknowledged; on each window-close
//! [`NodeControlMsg::TimerTick`] the kept representatives are emitted
//! downstream, each annotated with its Horvitz-Thompson count under the
//! `otel.sampling.nhat` attribute.
//!
//! When a `feedback_channel` is configured, the sampler consults the global
//! heavy-hitter table published on that channel (decoded from the agent's OTLP
//! responses by the exporter) as the global half of the binding admission rule
//! `-ln(u) < min(tau^L * w_c, tau^G * g_c)`. Without a channel the global gate
//! is slack (`tau^G = +inf`) and admission is purely local.
//!
//! Non-log signals pass through unchanged.

mod bottomk;
mod metrics;

use std::sync::Arc;
use std::time::{Duration, Instant};

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
use otap_df_engine::effect_handler::TimerCancelHandle;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;
use otap_df_otap::pdata::{Context, OtapPdata};
use otap_df_otap::sampling::{GlobalTable, SharedGlobalTable, shared_global_table};
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
    /// Optional two-level sampling feedback channel. When set, the sampler
    /// consults the global heavy-hitter table published on this channel (by
    /// the OTLP exporter that decoded it from the agent's responses) as the
    /// global half of the binding gate. When unset, the global gate is always
    /// slack (local-only sampling).
    #[serde(default)]
    feedback_channel: Option<String>,
    /// Optional maximum age of the global table before the sampler ignores it
    /// and degrades to local-only sampling. Age is measured locally as the time
    /// since the table's version last advanced (i.e. since the exporter last
    /// applied a fresh table from the agent), so no cross-host clocks are
    /// involved. When unset, the table is trusted indefinitely.
    #[serde(default, with = "humantime_serde::option")]
    max_table_staleness: Option<Duration>,
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
    /// Handle to the running window timer; `None` until the first message
    /// starts it. Retained so a runtime interval change can cancel and restart.
    timer: Option<TimerCancelHandle<OtapPdata>>,
    /// Shared global heavy-hitter table for the binding gate; `None` means the
    /// global gate is always slack.
    shared: Option<SharedGlobalTable>,
    /// Maximum tolerated table age before degrading to local-only; `None`
    /// trusts the table indefinitely.
    max_staleness: Option<Duration>,
    /// Last global table version observed (for staleness tracking).
    last_version: u32,
    /// Local instant at which `last_version` last changed.
    last_update: Instant,
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
            timer: None,
            shared: config.feedback_channel.as_deref().map(shared_global_table),
            max_staleness: config.max_table_staleness,
            last_version: 0,
            last_update: Instant::now(),
            metrics,
        })
    }

    /// Snapshot the current global table (absent when no feedback channel is
    /// configured), used as the global half of the binding gate. Tracks table
    /// freshness by version change and, when `max_staleness` is configured,
    /// degrades to the absent (slack) table once the table stops advancing for
    /// longer than that bound.
    fn current_table(&mut self) -> Arc<GlobalTable> {
        let Some(shared) = &self.shared else {
            return Arc::new(GlobalTable::absent());
        };
        let table = shared.load_full();
        if table.version != self.last_version {
            self.last_version = table.version;
            self.last_update = Instant::now();
            self.metrics.last_table_version.set(table.version as u64);
        }
        if let Some(max) = self.max_staleness {
            if !table.is_absent() && self.last_update.elapsed() > max {
                self.metrics.table_stale_degradations.inc();
                return Arc::new(GlobalTable::absent());
            }
        }
        table
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

        let table = self.current_table();
        let stats = self.sampler.observe_logs(logs, &table);
        self.metrics
            .globally_rejected
            .add(stats.globally_rejected as u64);

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
        let table = self.current_table();
        let Some((logs, stats)) = self.sampler.close_window(&table) else {
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

    /// Apply a runtime [`NodeControlMsg::Config`] update: reconfigure `k`, the
    /// window `interval`, the staleness bound, and the feedback channel. A
    /// malformed or invalid config is ignored (counted under `config_errors`)
    /// so the node keeps running with its previous settings.
    async fn apply_config(
        &mut self,
        config: serde_json::Value,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let cfg = match serde_json::from_value::<Config>(config) {
            Ok(cfg) if cfg.validate().is_ok() => cfg,
            _ => {
                self.metrics.config_errors.inc();
                return Ok(());
            }
        };

        self.sampler.set_k(cfg.k);
        self.max_staleness = cfg.max_table_staleness;
        // Re-resolve the feedback channel and reset staleness tracking.
        self.shared = cfg.feedback_channel.as_deref().map(shared_global_table);
        self.last_version = 0;
        self.last_update = Instant::now();

        // Restart the window timer only when the interval actually changed.
        if cfg.interval != self.interval {
            self.interval = cfg.interval;
            if let Some(timer) = self.timer.take() {
                let _ = timer.cancel().await;
            }
            self.timer = Some(effect_handler.start_periodic_timer(self.interval).await?);
        }
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
        if self.timer.is_none() {
            self.timer = Some(effect_handler.start_periodic_timer(self.interval).await?);
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
                NodeControlMsg::Config { config } => {
                    self.apply_config(config, effect_handler).await
                }
                NodeControlMsg::Shutdown { .. } => {
                    // Flush any buffered representatives before stopping.
                    self.close_window(effect_handler).await
                }
                NodeControlMsg::Ack(_)
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
