// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Global reservoir processor (agent side of two-level log sampling).
//!
//! Sits in the agent pipeline downstream of the OTLP receiver. It consumes the
//! representatives selected by SDK-side `log_sampler` processors, maintains an
//! exact global per-callsite count and a key-only bottom-`(k+1)` reservoir
//! (see [`global_window`]), and on each window-close publishes a
//! [`GlobalTable`] to a shared channel keyed by configuration. The OTLP
//! receiver reads that table back and attaches it to OTLP responses, closing
//! the feedback loop.
//!
//! Log records pass through to downstream nodes with the transport-only
//! [`OTEL_SAMPLING_NHAT`] attribute stripped. Non-log signals pass through
//! unchanged. The processor does not acknowledge inputs itself: it forwards
//! the (stripped) payload under the original context so downstream delivery
//! accounting flows back to the upstream sender.
//!
//! [`GlobalTable`]: otap_df_otap::sampling::GlobalTable
//! [`OTEL_SAMPLING_NHAT`]: otap_df_otap::sampling::OTEL_SAMPLING_NHAT

mod global_window;
mod metrics;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::Deserialize;

use otap_df_config::SignalType;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_otap::sampling::{SharedGlobalTable, shared_global_table};
use otap_df_pdata::OtapPayload;
use otap_df_pdata::TryIntoWithOptions;
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_telemetry::metrics::MetricSet;

use self::global_window::GlobalWindow;
use self::metrics::GlobalReservoirMetrics;

/// URN identifier for the global reservoir processor.
const GLOBAL_RESERVOIR_PROCESSOR_URN: &str = "urn:otel:processor:global_reservoir";

/// Configuration for the global reservoir processor.
#[derive(Debug, Clone, Deserialize)]
struct Config {
    /// Global reservoir size `k` (controls the `tau^G` estimate).
    k: usize,
    /// Maximum number of heavy hitters published per window (`K'`).
    k_prime: usize,
    /// Length of the global sampling window (e.g. "5s", "1m").
    #[serde(with = "humantime_serde")]
    interval: Duration,
    /// Shared-table channel name. Nodes naming the same channel (e.g. the OTLP
    /// receiver attaching the table to responses) share the published table.
    channel: String,
    /// EWMA smoothing factor for per-callsite counts, in `(0, 1]`. `1.0` (the
    /// default) disables smoothing; smaller values blend in history so a heavy
    /// hitter persists across a transiently quiet window.
    #[serde(default = "default_count_smoothing")]
    count_smoothing: f64,
}

/// Default EWMA smoothing factor: `1.0` (no smoothing).
fn default_count_smoothing() -> f64 {
    1.0
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.k == 0 {
            return Err(ConfigError::InvalidUserConfig {
                error: "global_reservoir.k must be greater than 0".to_string(),
            });
        }
        if self.k_prime == 0 {
            return Err(ConfigError::InvalidUserConfig {
                error: "global_reservoir.k_prime must be greater than 0".to_string(),
            });
        }
        if self.interval.is_zero() {
            return Err(ConfigError::InvalidUserConfig {
                error: "global_reservoir.interval must be greater than 0".to_string(),
            });
        }
        if self.channel.is_empty() {
            return Err(ConfigError::InvalidUserConfig {
                error: "global_reservoir.channel must not be empty".to_string(),
            });
        }
        if !(self.count_smoothing > 0.0 && self.count_smoothing <= 1.0) {
            return Err(ConfigError::InvalidUserConfig {
                error: "global_reservoir.count_smoothing must be in (0, 1]".to_string(),
            });
        }
        Ok(())
    }
}

/// Global reservoir processor.
struct GlobalReservoirProcessor {
    window: GlobalWindow,
    interval: Duration,
    timer_started: bool,
    shared: SharedGlobalTable,
    metrics: MetricSet<GlobalReservoirMetrics>,
}

impl GlobalReservoirProcessor {
    fn from_config(
        pipeline_ctx: PipelineContext,
        config: &serde_json::Value,
    ) -> Result<Self, ConfigError> {
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: e.to_string(),
            })?;
        config.validate()?;

        let metrics = pipeline_ctx.register_metrics::<GlobalReservoirMetrics>();
        Ok(Self {
            window: GlobalWindow::new(config.k, config.k_prime, config.count_smoothing),
            interval: config.interval,
            timer_started: false,
            shared: shared_global_table(&config.channel),
            metrics,
        })
    }

    /// Decode a log payload, accumulate its representatives into the global
    /// reservoir (stripping the transport `nhat` attribute), and forward the
    /// stripped payload downstream under the original context.
    async fn pass_through_logs(
        &mut self,
        pdata: OtapPdata,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let total = pdata.num_items();
        self.metrics.representatives_consumed.add(total as u64);

        let (context, payload) = pdata.into_parts();
        let otlp: OtlpProtoBytes = {
            let res: Result<OtlpProtoBytes, _> = payload.try_into_with_default();
            res.map_err(|e| EngineError::PdataConversionError {
                error: e.to_string(),
            })?
        };
        let mut logs = match OtlpProtoMessage::try_from(otlp) {
            Ok(OtlpProtoMessage::Logs(logs)) => logs,
            Ok(_) => unreachable!("signal_type was Logs"),
            Err(e) => {
                self.metrics.decode_errors.inc();
                return Err(EngineError::PdataConversionError {
                    error: e.to_string(),
                });
            }
        };

        let forwarded = self.window.observe_and_strip(&mut logs);
        self.metrics.representatives_forwarded.add(forwarded);

        let mut buf = Vec::new();
        {
            use prost::Message as _;
            logs.encode(&mut buf)
                .map_err(|e| EngineError::InternalError {
                    message: format!("failed to encode forwarded logs: {e}"),
                })?;
        }

        let payload = OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(buf.into()));
        effect_handler
            .send_message_with_source_node(OtapPdata::new(context, payload))
            .await?;
        Ok(())
    }

    /// Close the global window and publish the resulting table to the shared
    /// channel. No-op when the window saw no representatives.
    fn close_window(&mut self) {
        let Some((table, stats)) = self.window.close() else {
            return;
        };
        self.metrics.windows_closed.inc();
        self.metrics.tables_published.inc();
        self.metrics.last_heavy_hitters.set(stats.heavy_hitters);
        self.metrics
            .last_distinct_callsites
            .set(stats.distinct_callsites);
        self.metrics
            .last_tau_g
            .set(if stats.tau_g.is_finite() { stats.tau_g } else { 0.0 });
        self.metrics.last_g_unseen.set(if stats.g_unseen.is_finite() {
            stats.g_unseen
        } else {
            0.0
        });

        self.shared.store(Arc::new(table));
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for GlobalReservoirProcessor {
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
                SignalType::Logs => self.pass_through_logs(pdata, effect_handler).await,
                SignalType::Metrics | SignalType::Traces => {
                    effect_handler.send_message_with_source_node(pdata).await?;
                    Ok(())
                }
            },
            Message::Control(ctrl) => match ctrl {
                NodeControlMsg::TimerTick {} => {
                    self.close_window();
                    Ok(())
                }
                NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } => {
                    let _ = metrics_reporter.report(&mut self.metrics);
                    Ok(())
                }
                NodeControlMsg::Shutdown { .. } => {
                    self.close_window();
                    Ok(())
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

/// Factory function to create a global reservoir processor.
fn create_global_reservoir_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    Ok(ProcessorWrapper::local(
        GlobalReservoirProcessor::from_config(pipeline_ctx, &node_config.config)?,
        node,
        node_config,
        processor_config,
    ))
}

/// Register the global reservoir processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static GLOBAL_RESERVOIR_PROCESSOR_FACTORY: otap_df_engine::ProcessorFactory<OtapPdata> =
    otap_df_engine::ProcessorFactory {
        name: GLOBAL_RESERVOIR_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
                create_global_reservoir_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        validate_config: otap_df_config::validation::validate_typed_config::<Config>,
        wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    };
