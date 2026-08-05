// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tenant router processor for OTAP pipelines.
//!
//! Routes each message to a named output port by evaluating the request's
//! tenant context against an ordered list of conditions, first match wins.
//!
//! The routing decision reads no telemetry. A receiver resolved the request's
//! tenant tokens once, into a packed context addressed by position, and this
//! node probes that context: one bit test and one hash lookup per bound
//! (token, signature) pair. Cost is therefore independent of how many routes
//! are configured, how many entries each route tests, and how large the batch
//! is -- which is what separates it from `processor:content_router`, whose
//! decision requires decoding the payload and walking every resource.
//!
//! Conditions are compiled by the engine before any pipeline starts, so a
//! route naming a key no token declares, or a value no condition interned,
//! fails at startup rather than silently matching nothing.
//!
//! # Selected-route admission
//!
//! Admission is shared with the other exclusive routers: after a route is
//! selected, a blocked downstream port is handled by router-local state rather
//! than by awaiting the send, so one blocked tenant cannot stall the router
//! task and starve the others.
//!
//! - `reject_immediately` (default): emit a retryable route-local NACK when
//!   the selected route is full
//! - `backpressure`: park one message per blocked output port and keep
//!   admitting until every selectable route has a parked message
//!
//! A selected route that is closed is always rejected immediately.

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::PortName;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_config::tenant::compiled::ConditionSet;
use otap_df_config::tenant::{Condition, Entry};
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{NackCause, NackMsg, NodeControlMsg, WakeupRevision, WakeupSlot};
use otap_df_engine::error::{Error as EngineError, ProcessorErrorKind};
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_engine::{
    ConsumerEffectHandlerExtension, MessageSourceLocalEffectHandlerExtension, ProcessorFactory,
    ProcessorRuntimeRequirements, RouteAdmission, WakeupError,
};
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry_macros::metric_set;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use crate::processors::exclusive_router_admission::{
    ExclusiveRouteScheduler, FullRouteHandling, PendingRoute, SelectedRouteAdmissionPolicy,
};

/// URN for the TenantRouter processor.
pub const TENANT_ROUTER_URN: &str = "urn:otel:processor:tenant_router";

/// Metrics for the TenantRouter processor.
#[metric_set(name = "processor.tenant_router")]
#[derive(Debug, Default, Clone)]
pub struct TenantRouterMetrics {
    /// Number of messages received by the router.
    #[metric(unit = "{msg}")]
    pub signals_received: Counter<u64>,
    /// Number of messages routed by a matching tenant condition.
    #[metric(unit = "{msg}")]
    pub signals_routed: Counter<u64>,
    /// Number of messages routed via the default output port.
    #[metric(unit = "{msg}")]
    pub signals_routed_default: Counter<u64>,
    /// Number of messages that carried no tenant context at all.
    #[metric(unit = "{msg}")]
    pub signals_without_tenant_context: Counter<u64>,
    /// Number of messages whose tenant context matched no route.
    #[metric(unit = "{msg}")]
    pub signals_unmatched: Counter<u64>,
    /// Number of messages NACKed by the router.
    #[metric(unit = "{msg}")]
    pub signals_nacked: Counter<u64>,
    /// Number of messages rejected because the selected route was full.
    #[metric(unit = "{msg}")]
    pub signals_rejected_route_full: Counter<u64>,
    /// Number of messages rejected because the selected route was closed.
    #[metric(unit = "{msg}")]
    pub signals_rejected_route_closed: Counter<u64>,
}

/// One route: the condition that selects it and the port it names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantOutputRoute {
    /// Entries that must all match for this route to be selected.
    pub entries: Vec<Entry>,
    /// Output port the message is sent to.
    pub output: String,
}

/// The route table this node publishes for the engine to compile.
///
/// The field name is fixed by `otap_df_config::tenant::TENANT_ROUTING_KEY`:
/// the controller finds every node's conditions by that key and interns them
/// before the registry is frozen, because a condition that was not declared
/// then cannot be looked up afterwards.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantRouteTable {
    /// Tenant tokens this router binds. Empty binds every declared token.
    #[serde(default)]
    pub tenant_tokens: Vec<String>,
    /// Routes evaluated first-match-wins.
    pub routes: Vec<TenantOutputRoute>,
    /// Port for messages matching no route. Without it, unmatched messages
    /// are NACKed rather than delivered somewhere arbitrary.
    #[serde(default)]
    pub default_output: Option<String>,
}

/// Configuration for the TenantRouter processor.
///
/// ```yaml
/// type: processor:tenant_router
/// outputs: [acme, globex, unmatched]
/// config:
///   tenant_routing:
///     tenant_tokens: [edge]
///     routes:
///       - entries: [{ key: tenant_id, value: acme }]
///         output: acme
///       - entries: [{ key: tenant_id, value: globex }]
///         output: globex
///     default_output: unmatched
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantRouterConfig {
    /// Conditions and the ports they select.
    pub tenant_routing: TenantRouteTable,
    /// Policy for selected-route `Full` admission.
    #[serde(default)]
    pub admission_policy: SelectedRouteAdmissionPolicy,
}

impl TenantRouterConfig {
    /// Validates the configuration against the node's declared output ports.
    fn validate(&self, declared_outputs: &[PortName]) -> Result<(), ConfigError> {
        let table = &self.tenant_routing;
        if table.routes.is_empty() {
            return Err(ConfigError::InvalidUserConfig {
                error: "tenant_routing.routes must not be empty".to_owned(),
            });
        }
        self.admission_policy.validate()?;
        for (idx, route) in table.routes.iter().enumerate() {
            if route.entries.is_empty() {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!("tenant_routing.routes[{idx}] declares no entries"),
                });
            }
            if route.output.trim().is_empty() {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!("tenant_routing.routes[{idx}] has an empty output port name"),
                });
            }
            for entry in &route.entries {
                if entry.key.trim().is_empty() {
                    return Err(ConfigError::InvalidUserConfig {
                        error: format!("tenant_routing.routes[{idx}] declares an empty key"),
                    });
                }
            }
        }
        if let Some(default) = &table.default_output
            && default.trim().is_empty()
        {
            return Err(ConfigError::InvalidUserConfig {
                error: "tenant_routing.default_output must not be empty when specified".to_owned(),
            });
        }
        // Skipped when the node declares no ports, which is how pipeline-level
        // wiring without explicit port declarations is expressed.
        if !declared_outputs.is_empty() {
            for (idx, route) in table.routes.iter().enumerate() {
                if !declared_outputs
                    .iter()
                    .any(|o| o.as_ref() == route.output.as_str())
                {
                    return Err(ConfigError::InvalidUserConfig {
                        error: format!(
                            "tenant_routing.routes[{idx}] references undeclared output port '{}'",
                            route.output
                        ),
                    });
                }
            }
            if let Some(default) = &table.default_output
                && !declared_outputs
                    .iter()
                    .any(|o| o.as_ref() == default.as_str())
            {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!(
                        "tenant_routing.default_output '{default}' references undeclared output port"
                    ),
                });
            }
        }
        Ok(())
    }

    /// The route conditions, in route order, as the registry expects them.
    fn conditions(&self) -> Vec<Condition> {
        self.tenant_routing
            .routes
            .iter()
            .map(|route| Condition {
                entries: route.entries.clone(),
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum SelectedRouteKind {
    Matched,
    Default,
}

/// Routes messages to output ports on the request's tenant context.
pub struct TenantRouter {
    /// Compiled probe tables, built once from the shared registry.
    conditions: ConditionSet,
    /// Output port per route, indexed by condition index.
    outputs: Box<[PortName]>,
    /// Port for messages matching no route.
    default_output: Option<PortName>,
    /// Selected-route admission scheduler.
    admission: ExclusiveRouteScheduler<OtapPdata, SelectedRouteKind>,
    /// Telemetry metrics.
    metrics: Option<MetricSet<TenantRouterMetrics>>,
}

impl TenantRouter {
    /// Builds a router by compiling its conditions against the engine registry.
    ///
    /// Every failure here is a startup failure by design. A router that cannot
    /// evaluate its conditions would send every tenant to its default port,
    /// which is the one outcome a tenant boundary must never produce silently.
    pub fn try_new(
        pipeline_ctx: &PipelineContext,
        config: TenantRouterConfig,
    ) -> Result<Self, ConfigError> {
        let registry = pipeline_ctx.tenant_registry().cloned().ok_or_else(|| {
            ConfigError::InvalidUserConfig {
                error: "tenant_router routes on tenant conditions, but this engine \
                            declares no `tenant_tokens`"
                    .to_owned(),
            }
        })?;

        let table = &config.tenant_routing;
        let bound = (!table.tenant_tokens.is_empty()).then_some(table.tenant_tokens.as_slice());
        let conditions = registry.condition_set(bound, &config.conditions())?;

        let outputs: Box<[PortName]> = table
            .routes
            .iter()
            .map(|route| PortName::from(route.output.clone()))
            .collect();

        Ok(Self {
            conditions,
            outputs,
            default_output: table.default_output.clone().map(PortName::from),
            admission: ExclusiveRouteScheduler::new(config.admission_policy),
            metrics: Some(pipeline_ctx.register_metrics::<TenantRouterMetrics>()),
        })
    }

    /// Selects the output port for one message.
    ///
    /// The whole decision is a probe of the packed context: no payload is
    /// decoded, no string is compared, and nothing is allocated.
    fn resolve_route(&mut self, pdata: &OtapPdata) -> Option<PortName> {
        let Some(view) = pdata.tenant_view() else {
            if let Some(m) = self.metrics.as_mut() {
                m.signals_without_tenant_context.inc();
            }
            return None;
        };
        let Some(idx) = self.conditions.first_match(&view) else {
            if let Some(m) = self.metrics.as_mut() {
                m.signals_unmatched.inc();
            }
            return None;
        };
        // `first_match` returns the index of the condition within the slice
        // handed to `condition_set`, which is route order.
        self.outputs.get(usize::from(idx)).cloned()
    }

    /// Selects the output port for one message, for benchmarking.
    ///
    /// Exposes the routing decision alone so it can be compared against
    /// another router's decision without the admission and channel work that
    /// both share.
    #[cfg(feature = "bench")]
    pub fn bench_select(&mut self, pdata: &OtapPdata) -> Option<PortName> {
        self.resolve_route(pdata)
    }

    fn record_forwarded_route(&mut self, route_kind: SelectedRouteKind) {
        if let Some(m) = self.metrics.as_mut() {
            match route_kind {
                SelectedRouteKind::Matched => m.signals_routed.inc(),
                SelectedRouteKind::Default => m.signals_routed_default.inc(),
            }
        }
    }

    fn observe_backpressure_candidates(
        &mut self,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) {
        let connected: HashSet<_> = effect_handler.connected_ports().into_iter().collect();
        let mut candidates = HashSet::new();

        for port in self.outputs.iter() {
            if connected.contains(port) {
                let _ = candidates.insert(port.clone());
            }
        }
        if let Some(default_port) = self.default_output.as_ref()
            && connected.contains(default_port)
        {
            let _ = candidates.insert(default_port.clone());
        }

        self.admission.observe_pause_candidate_ports(candidates);
    }

    fn wakeup_error(
        effect_handler: &local::EffectHandler<OtapPdata>,
        error: WakeupError,
    ) -> EngineError {
        EngineError::ProcessorError {
            processor: effect_handler.processor_id(),
            kind: ProcessorErrorKind::Other,
            error: format!("tenant_router admission scheduler failed: {error:?}"),
            source_detail: String::new(),
        }
    }

    async fn emit_route_full_nack(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        port: &str,
        data: OtapPdata,
    ) -> Result<(), EngineError> {
        if let Some(m) = self.metrics.as_mut() {
            m.signals_nacked.inc();
            m.signals_rejected_route_full.inc();
        }
        effect_handler
            .notify_nack(NackMsg::new_with_cause(
                format!("tenant_router route overload: output port '{port}' is full"),
                data,
                NackCause::RouteFull,
            ))
            .await?;
        Ok(())
    }

    async fn emit_route_closed_nack(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        port: &str,
        data: OtapPdata,
    ) -> Result<(), EngineError> {
        if let Some(m) = self.metrics.as_mut() {
            m.signals_nacked.inc();
            m.signals_rejected_route_closed.inc();
        }
        effect_handler
            .notify_nack(NackMsg::new_with_cause(
                format!("tenant_router route unavailable: output port '{port}' is closed"),
                data,
                NackCause::RouteClosed,
            ))
            .await?;
        Ok(())
    }

    async fn emit_shutdown_nack(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        port: &str,
        data: OtapPdata,
        reason: &str,
    ) -> Result<(), EngineError> {
        if let Some(m) = self.metrics.as_mut() {
            m.signals_nacked.inc();
        }
        effect_handler
            .notify_nack(NackMsg::new_with_cause(
                format!(
                    "tenant_router admission canceled for output port '{port}' during shutdown: {reason}"
                ),
                data,
                NackCause::NodeShutdown,
            ))
            .await?;
        Ok(())
    }

    /// Apply the configured `Full` policy for a selected route.
    async fn handle_selected_route_full(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        port: PortName,
        route_kind: SelectedRouteKind,
        data: OtapPdata,
    ) -> Result<(), EngineError> {
        self.observe_backpressure_candidates(effect_handler);
        match self
            .admission
            .handle_selected_route_full(port.clone(), data, route_kind, effect_handler)
            .map_err(|error| Self::wakeup_error(effect_handler, error))?
        {
            FullRouteHandling::ImmediateNack(data) => {
                self.emit_route_full_nack(effect_handler, port.as_ref(), data)
                    .await
            }
            FullRouteHandling::Parked => Ok(()),
        }
    }

    /// Send one message to an already-selected port.
    async fn admit_to(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        port: PortName,
        route_kind: SelectedRouteKind,
        data: OtapPdata,
    ) -> Result<(), EngineError> {
        let admission = effect_handler
            .try_admit_message_with_source_node_to(port.clone(), data)
            .map_err(EngineError::from)?;
        match admission {
            RouteAdmission::Accepted => {
                self.record_forwarded_route(route_kind);
                Ok(())
            }
            RouteAdmission::RejectedFull(data) => {
                self.handle_selected_route_full(effect_handler, port, route_kind, data)
                    .await
            }
            RouteAdmission::RejectedClosed(data) => {
                self.emit_route_closed_nack(effect_handler, port.as_ref(), data)
                    .await
            }
        }
    }

    /// Retry locally parked selected routes when the shared wakeup fires.
    async fn handle_wakeup(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        slot: WakeupSlot,
        when: Instant,
        revision: WakeupRevision,
    ) -> Result<(), EngineError> {
        let now = std::cmp::max(when, Instant::now());
        let due = self.admission.take_due_routes(slot, revision, now);
        if due.is_empty() {
            return Ok(());
        }

        for pending in due {
            let (port, data, route_kind) = pending.into_parts();
            let admission = effect_handler
                .try_admit_message_with_source_node_to(port.clone(), data)
                .map_err(EngineError::from)?;
            match admission {
                RouteAdmission::Accepted => self.record_forwarded_route(route_kind),
                RouteAdmission::RejectedClosed(data) => {
                    self.emit_route_closed_nack(effect_handler, port.as_ref(), data)
                        .await?;
                }
                RouteAdmission::RejectedFull(data) => {
                    self.admission
                        .repark_after_full(PendingRoute::from_retry_parts(
                            port, data, route_kind, now,
                        ));
                }
            }
        }

        self.admission
            .sync_armed_wakeup(effect_handler)
            .map_err(|error| Self::wakeup_error(effect_handler, error))?;
        Ok(())
    }

    /// Drain router-local parked work during shutdown entry.
    async fn handle_shutdown(
        &mut self,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
        reason: &str,
    ) -> Result<(), EngineError> {
        for pending in self.admission.drain_for_shutdown(effect_handler) {
            let (port, data, _) = pending.into_parts();
            self.emit_shutdown_nack(effect_handler, port.as_ref(), data, reason)
                .await?;
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for TenantRouter {
    fn accept_pdata(&self) -> bool {
        self.admission.accept_pdata()
    }

    fn runtime_requirements(&self) -> ProcessorRuntimeRequirements {
        self.admission.runtime_requirements()
    }

    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        match msg {
            Message::Control(ctrl) => match ctrl {
                NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } => {
                    if let Some(m) = self.metrics.as_mut() {
                        let _ = metrics_reporter.report(m);
                    }
                    Ok(())
                }
                NodeControlMsg::Wakeup {
                    slot,
                    when,
                    revision,
                } => {
                    self.handle_wakeup(effect_handler, slot, when, revision)
                        .await
                }
                NodeControlMsg::Shutdown { reason, .. } => {
                    self.handle_shutdown(effect_handler, reason.as_str()).await
                }
                _ => Ok(()),
            },
            Message::PData(data) => {
                if let Some(m) = self.metrics.as_mut() {
                    m.signals_received.inc();
                }
                match self.resolve_route(&data) {
                    Some(port) => {
                        self.admit_to(effect_handler, port, SelectedRouteKind::Matched, data)
                            .await
                    }
                    None => match self.default_output.clone() {
                        Some(port) => {
                            self.admit_to(effect_handler, port, SelectedRouteKind::Default, data)
                                .await
                        }
                        None => {
                            // Delivering unmatched data to an arbitrary port is
                            // how one tenant's data reaches another tenant's
                            // backend, so refusing it is the safe answer.
                            if let Some(m) = self.metrics.as_mut() {
                                m.signals_nacked.inc();
                            }
                            effect_handler
                                .notify_nack(NackMsg::new_permanent(
                                    "tenant_router: no tenant condition matched and no \
                                     default_output is configured"
                                        .to_owned(),
                                    data,
                                ))
                                .await?;
                            Ok(())
                        }
                    },
                }
            }
        }
    }
}

/// Register TenantRouter as an OTAP processor factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static TENANT_ROUTER_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: TENANT_ROUTER_URN,
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<TenantRouterConfig>,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        let router_config: TenantRouterConfig = serde_json::from_value(node_config.config.clone())
            .map_err(|e| ConfigError::InvalidUserConfig {
                error: format!("Failed to parse TenantRouter configuration: {e}"),
            })?;
        router_config.validate(&node_config.outputs)?;

        let router = TenantRouter::try_new(&pipeline, router_config)?;

        Ok(ProcessorWrapper::local(router, node, node_config, proc_cfg))
    },
};

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_channel::mpsc;
    use otap_df_config::tenant::compiled::{
        TenantTokenRegistry, TenantTokenRegistryBuilder, TokenInputs, TokenScratch,
    };
    use otap_df_config::tenant::{Extractor, TenantTokenSpec, TenantTokens};
    use otap_df_engine::context::ControllerContext;
    use otap_df_engine::local::message::LocalSender;
    use otap_df_engine::local::processor::{EffectHandler as LocalEffectHandler, Processor as _};
    use otap_df_engine::message::{Message, Sender};
    use otap_df_engine::testing::{setup_test_runtime, test_node};
    use otap_df_otap::pdata::OtapPdata;
    use otap_df_pdata::otlp::OtlpProtoBytes;
    use otap_df_telemetry::InternalTelemetrySystem;
    use std::collections::HashMap;

    /// An engine registry declaring one token over one header, plus the two
    /// tenant values the routes below test against.
    fn registry(values: &[&str]) -> Arc<TenantTokenRegistry> {
        let mut tokens = TenantTokens::default();
        let _ = tokens.insert(
            "edge".to_owned(),
            TenantTokenSpec {
                extractors: vec![Extractor::TransportHeader {
                    key: "tenant_id".to_owned(),
                    transport_header: "x-tenant-id".to_owned(),
                    retain: true,
                    bag: false,
                }],
            },
        );
        let mut builder = TenantTokenRegistryBuilder::new();
        builder.add_tokens(&tokens).expect("tokens compile");
        let conditions: Vec<Condition> = values
            .iter()
            .map(|v| Condition {
                entries: vec![Entry {
                    key: "tenant_id".to_owned(),
                    value: Some((*v).to_owned()),
                }],
            })
            .collect();
        builder
            .declare_conditions(None, &conditions)
            .expect("conditions declare");
        Arc::new(builder.build(0).expect("registry builds"))
    }

    fn route(value: &str, output: &str) -> TenantOutputRoute {
        TenantOutputRoute {
            entries: vec![Entry {
                key: "tenant_id".to_owned(),
                value: Some(value.to_owned()),
            }],
            output: output.to_owned(),
        }
    }

    fn config(routes: Vec<TenantOutputRoute>, default_output: Option<&str>) -> TenantRouterConfig {
        TenantRouterConfig {
            tenant_routing: TenantRouteTable {
                tenant_tokens: Vec::new(),
                routes,
                default_output: default_output.map(str::to_owned),
            },
            admission_policy: SelectedRouteAdmissionPolicy::default(),
        }
    }

    /// An empty logs payload carrying the tenant context a receiver would have
    /// resolved from the given header value. `None` means the request arrived
    /// without any tenant context at all.
    fn pdata(registry: &TenantTokenRegistry, tenant: Option<&str>) -> OtapPdata {
        let mut data =
            OtapPdata::new_default(OtlpProtoBytes::ExportLogsRequest(Vec::new().into()).into());
        if let Some(value) = tenant {
            let mut scratch = TokenScratch::new();
            let words = registry
                .resolve(
                    &mut scratch,
                    TokenInputs::new([("x-tenant-id", value.as_bytes())]),
                )
                .expect("token resolves");
            data.set_tenant(words);
        }
        data
    }

    /// Drives one message through a router wired to the named ports and
    /// reports which port received it.
    fn route_one(
        registry: Arc<TenantTokenRegistry>,
        cfg: TenantRouterConfig,
        ports: &[&str],
        tenant: Option<&str>,
    ) -> Option<String> {
        let (rt, local) = setup_test_runtime();
        let ports: Vec<String> = ports.iter().map(|p| (*p).to_owned()).collect();
        rt.block_on(local.run_until(async move {
            let telemetry = InternalTelemetrySystem::default();
            let controller = ControllerContext::new(telemetry.registry());
            let mut pipeline =
                controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            pipeline.set_tenant_registry(registry.clone());

            let mut router = TenantRouter::try_new(&pipeline, cfg).expect("router builds");

            let mut senders = HashMap::new();
            let mut receivers = Vec::new();
            for port in &ports {
                let (tx, rx) = mpsc::Channel::new(4);
                let _ = senders.insert(
                    PortName::from(port.clone()),
                    Sender::Local(LocalSender::mpsc(tx)),
                );
                receivers.push((port.clone(), rx));
            }
            let mut eh = LocalEffectHandler::new(
                test_node("tenant_router_test"),
                senders,
                None,
                telemetry.reporter(),
            );

            let data = pdata(&registry, tenant);
            router
                .process(Message::PData(data), &mut eh)
                .await
                .expect("router failed");

            for (port, rx) in &receivers {
                if rx.try_recv().is_ok() {
                    return Some(port.clone());
                }
            }
            None
        }))
    }

    /// Scenario: two tenants each declare a route, and a request arrives
    /// carrying the header that identifies the second one.
    /// Guarantees: the message is delivered to that tenant's port and to no
    /// other, which is the whole point of routing on the tenant context
    /// rather than on the payload.
    #[test]
    fn routes_each_tenant_to_its_own_port() {
        let reg = registry(&["acme", "globex"]);
        let cfg = config(
            vec![route("acme", "acme_port"), route("globex", "globex_port")],
            Some("fallback"),
        );
        let landed = route_one(
            reg,
            cfg,
            &["acme_port", "globex_port", "fallback"],
            Some("globex"),
        );
        assert_eq!(landed.as_deref(), Some("globex_port"));
    }

    /// Scenario: a request carries a tenant value that no route declares.
    /// Guarantees: it takes the configured default port instead of any
    /// tenant's port, so an unrecognized tenant can never be delivered to a
    /// destination belonging to a recognized one.
    #[test]
    fn unmatched_tenant_takes_the_default_port() {
        let reg = registry(&["acme", "globex"]);
        let cfg = config(
            vec![route("acme", "acme_port"), route("globex", "globex_port")],
            Some("fallback"),
        );
        let landed = route_one(
            reg,
            cfg,
            &["acme_port", "globex_port", "fallback"],
            Some("initech"),
        );
        assert_eq!(landed.as_deref(), Some("fallback"));
    }

    /// Scenario: a request arrives with no tenant context at all, and the
    /// router declares no default output.
    /// Guarantees: nothing is delivered to any tenant port. Data whose tenant
    /// is unknown must not reach a tenant's destination, so the router refuses
    /// it rather than picking one.
    #[test]
    fn missing_tenant_context_reaches_no_tenant_port() {
        let reg = registry(&["acme"]);
        let cfg = config(vec![route("acme", "acme_port")], None);
        let landed = route_one(reg, cfg, &["acme_port"], None);
        assert_eq!(landed, None);
    }

    /// Scenario: two routes name the same tenant value, the first one winning.
    /// Guarantees: first-match-wins is decided by route order, so a later
    /// duplicate cannot capture traffic the operator assigned earlier.
    #[test]
    fn first_matching_route_wins() {
        let reg = registry(&["acme"]);
        let cfg = config(
            vec![route("acme", "first_port"), route("acme", "second_port")],
            None,
        );
        let landed = route_one(reg, cfg, &["first_port", "second_port"], Some("acme"));
        assert_eq!(landed.as_deref(), Some("first_port"));
    }

    /// Scenario: a route names an output port the node never declared.
    /// Guarantees: validation rejects it at configuration time, so a route
    /// that could only ever fail to deliver never reaches the running engine.
    #[test]
    fn route_to_undeclared_port_is_rejected() {
        let cfg = config(vec![route("acme", "missing_port")], None);
        let declared = [PortName::from("acme_port".to_owned())];
        assert!(cfg.validate(&declared).is_err());
    }

    /// Scenario: an engine that declares no tenant tokens builds a router.
    /// Guarantees: construction fails with a message naming the missing
    /// `tenant_tokens`, rather than starting a router that would send every
    /// request to its default port.
    #[test]
    fn router_without_a_registry_fails_to_build() {
        let (rt, local) = setup_test_runtime();
        rt.block_on(local.run_until(async move {
            let telemetry = InternalTelemetrySystem::default();
            let controller = ControllerContext::new(telemetry.registry());
            let pipeline = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            let cfg = config(vec![route("acme", "acme_port")], None);
            let Err(err) = TenantRouter::try_new(&pipeline, cfg) else {
                panic!("router must not build without a registry");
            };
            assert!(
                format!("{err}").contains("tenant_tokens"),
                "error should name the missing engine configuration, got: {err}"
            );
        }));
    }
}
