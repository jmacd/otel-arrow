// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! What tenant-specific routing costs in a local processor.
//!
//! This is the ideal case: routing decided and delivered entirely within one
//! pipeline, on one thread, over a local channel. It exists to be the floor
//! that the inter-pipeline path (`exporter:topic` -> topic -> `receiver:topic`)
//! is later measured against, so both halves must time the same thing --
//! delivering one message to the destination its tenant selects.
//!
//! The comparison is against `processor:signal_type_router`, which is the
//! right control because it is the same node minus the interesting part:
//!
//! ```text
//! signal_type_router = exclusive-router dispatch + a signal enum read
//! tenant_router      = exclusive-router dispatch + a tenant condition probe
//! difference         = what tenant-specific routing actually costs
//! ```
//!
//! Both share `exclusive_router_admission`, so the parking, wakeup and
//! Ack/Nack machinery is identical and cancels out of the subtraction. Both
//! are handed the *same* pdata, carrying a resolved tenant context, so the
//! per-message clone costs the same on each arm even though the control never
//! reads the context.
//!
//! `process` is timed whole, including the send to a local output port, rather
//! than just the selection step. The topic path cannot be measured without its
//! hop, so a selection-only baseline would not be comparable to it.
//!
//! Neither node reads the payload. `by_payload` confirms that rather than
//! assuming it.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use otap_df_channel::mpsc;
use otap_df_config::PortName;
use otap_df_config::tenant::compiled::{
    BoundaryFilter, ConditionSet, KeyId, TenantTokenRegistry, TenantTokenRegistryBuilder,
    TokenInputs, TokenScratch,
};
use otap_df_config::tenant::{
    Condition, Entry, Extractor, TenantBoundaryPolicy, TenantRoute, TenantRouting, TenantTokenSpec,
    TenantTokens,
};
use otap_df_core_nodes::processors::signal_type_router::{
    PORT_LOGS, SignalTypeRouter, SignalTypeRouterConfig,
};
use otap_df_core_nodes::processors::tenant_router::{TenantRouter, TenantRouterConfig};
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::context::{ControllerContext, PipelineContext};
use otap_df_engine::local::message::LocalSender;
use otap_df_engine::local::processor::{EffectHandler as LocalEffectHandler, Processor as _};
use otap_df_engine::message::{Message, Sender};
use otap_df_engine::testing::{setup_test_runtime, test_node};
use otap_df_engine::topic::{
    InMemoryBackend, PublishOutcome, RecvItem, SubscriberOptions, SubscriptionMode, TopicBroker,
    TopicOptions,
};
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::proto::opentelemetry::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use otap_df_telemetry::InternalTelemetrySystem;
use prost::Message as _;

/// Route counts swept. An operator names a handful of tenants, not thousands,
/// but the tail is included to show the probe does not degrade.
const ROUTES: [usize; 4] = [1, 4, 16, 64];

/// Resources per message, used only to confirm that neither router's cost
/// depends on the payload.
const RESOURCES: [usize; 3] = [1, 64, 512];

/// The header the tenant token reads.
const HEADER: &str = "x-tenant-id";
/// A resource attribute, present only to give the payload realistic bulk.
const ATTRIBUTE: &str = "service.namespace";

fn tenant_value(idx: usize) -> String {
    format!("tenant-{idx}")
}

fn port_name(idx: usize) -> String {
    format!("port_{idx}")
}

/// A pipeline context carrying an engine registry, as the controller builds it.
fn pipeline_context(registry: Option<Arc<TenantTokenRegistry>>) -> PipelineContext {
    let telemetry = InternalTelemetrySystem::default();
    let controller = ControllerContext::new(telemetry.registry());
    let mut ctx = controller.pipeline_context_with("bench".into(), "bench".into(), 0, 1, 0);
    if let Some(registry) = registry {
        ctx.set_tenant_registry(registry);
    }
    ctx
}

/// An engine registry with one token over one header, and every route's literal
/// interned, exactly as the controller would have declared them.
fn registry(routes: usize) -> Arc<TenantTokenRegistry> {
    let mut tokens = TenantTokens::default();
    let _ = tokens.insert(
        "edge".to_owned(),
        TenantTokenSpec {
            extractors: vec![Extractor::TransportHeader {
                key: "tenant_id".to_owned(),
                transport_header: HEADER.to_owned(),
                retain: false,
                bag: false,
            }],
        },
    );
    let mut builder = TenantTokenRegistryBuilder::new();
    builder.add_tokens(&tokens).expect("tokens compile");
    let conditions: Vec<Condition> = (0..routes)
        .map(|i| Condition {
            entries: vec![Entry {
                key: "tenant_id".to_owned(),
                value: Some(tenant_value(i)),
            }],
        })
        .collect();
    builder
        .declare_conditions(None, &conditions)
        .expect("conditions declare");
    Arc::new(builder.build(0).expect("registry builds"))
}

fn tenant_router(registry: Arc<TenantTokenRegistry>, routes: usize) -> TenantRouter {
    let config = TenantRouterConfig {
        tenant_routing: TenantRouting {
            tenant_tokens: Vec::new(),
            routes: (0..routes)
                .map(|i| TenantRoute {
                    entries: vec![Entry {
                        key: "tenant_id".to_owned(),
                        value: Some(tenant_value(i)),
                    }],
                    to: port_name(i),
                })
                .collect(),
            default_to: None,
        },
        admission_policy: Default::default(),
    };
    TenantRouter::try_new(&pipeline_context(Some(registry)), config).expect("router builds")
}

/// One message carrying `resources` resources.
fn logs(resources: usize, value: &str) -> Bytes {
    let resource_logs: Vec<ResourceLogs> = (0..resources)
        .map(|_| {
            ResourceLogs::new(
                Resource {
                    attributes: vec![KeyValue::new(
                        ATTRIBUTE,
                        AnyValue::new_string(value.to_owned()),
                    )],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                },
                vec![ScopeLogs::new(
                    InstrumentationScope::default(),
                    vec![
                        LogRecord::build()
                            .time_unix_nano(1u64)
                            .severity_number(SeverityNumber::Info)
                            .finish(),
                    ],
                )],
            )
        })
        .collect();
    Bytes::from(ExportLogsServiceRequest::new(resource_logs).encode_to_vec())
}

/// One message as a receiver would hand it on: the payload, plus the tenant
/// context resolved from the request's headers.
///
/// Both routers are given this same shape. The control ignores the context but
/// still pays to clone it, so the difference between the two arms is the
/// routing decision and nothing else.
fn pdata(registry: &TenantTokenRegistry, resources: usize, value: &str) -> OtapPdata {
    let mut data = OtapPdata::new_todo_context(
        OtlpProtoBytes::ExportLogsRequest(logs(resources, value)).into(),
    );
    let mut scratch = TokenScratch::new();
    let words = registry
        .resolve(&mut scratch, TokenInputs::new([(HEADER, value.as_bytes())]))
        .expect("token resolves");
    data.set_tenant(words);
    data
}

/// A local effect handler wired to the named output ports, with the receivers
/// returned so the channels stay open and can be drained.
fn wire(
    ports: &[String],
) -> (
    LocalEffectHandler<OtapPdata>,
    Vec<mpsc::Receiver<OtapPdata>>,
) {
    let telemetry = InternalTelemetrySystem::default();
    let mut senders = HashMap::new();
    let mut receivers = Vec::new();
    for port in ports {
        let (tx, rx) = mpsc::Channel::new(64);
        let _ = senders.insert(
            PortName::from(port.clone()),
            Sender::Local(LocalSender::mpsc(tx)),
        );
        receivers.push(rx);
    }
    (
        LocalEffectHandler::new(
            test_node("bench_router"),
            senders,
            None,
            telemetry.reporter(),
        ),
        receivers,
    )
}

/// Drain what the router delivered, so a bounded channel never fills and turns
/// the measurement into an admission-failure benchmark.
fn drain(receivers: &[mpsc::Receiver<OtapPdata>]) {
    for rx in receivers {
        while rx.try_recv().is_ok() {}
    }
}

/// Time `process` end to end, including the send to a local output port.
///
/// The loop runs inside one `block_on` per sample rather than one per message:
/// entering an executor costs more than the message being measured, so timing
/// it per iteration would measure the harness.
///
/// Channels are drained every `CHUNK` messages, inside the timed region. The
/// drain is identical on both arms, and doing it outside would let a bounded
/// channel fill and silently convert the benchmark into a measurement of the
/// route-full path.
macro_rules! bench_process {
    ($group:expr, $id:expr, $param:expr, $rt:expr, $local:expr,
     $router:expr, $eh:expr, $rx:expr, $data:expr) => {{
        const CHUNK: u64 = 32;
        let router = &mut $router;
        let eh = &mut $eh;
        let rx = &$rx;
        let data = &$data;
        let _ = $group.bench_with_input($id, $param, |b, _| {
            b.iter_custom(|iters| {
                $rt.block_on($local.run_until(async {
                    let start = Instant::now();
                    for i in 0..iters {
                        router
                            .process(Message::PData(black_box(data.clone())), eh)
                            .await
                            .expect("router failed");
                        if i % CHUNK == CHUNK - 1 {
                            drain(rx);
                        }
                    }
                    let elapsed = start.elapsed();
                    drain(rx);
                    elapsed
                }))
            })
        });
    }};
}

/// The headline: what one message costs, delivered, as the number of tenant
/// routes grows.
fn bench_dispatch(c: &mut Criterion) {
    let (rt, local) = setup_test_runtime();
    let resources = 1;
    let mut group = c.benchmark_group("tenant_routing/dispatch");

    for routes in ROUTES {
        // Match the last route so the probe is not measured on a lucky first
        // comparison.
        let value = tenant_value(routes - 1);
        let reg = registry(routes);
        let data = pdata(&reg, resources, &value);

        let ports: Vec<String> = (0..routes).map(port_name).collect();
        let (mut eh, rx) = wire(&ports);
        let mut router = tenant_router(reg.clone(), routes);
        bench_process!(
            group,
            BenchmarkId::new("tenant", routes),
            &routes,
            rt,
            local,
            router,
            eh,
            rx,
            data
        );

        // The control routes the same message with the same machinery, and
        // decides by reading the signal enum.
        let (mut control_eh, control_rx) = wire(&[PORT_LOGS.to_owned()]);
        let mut control = SignalTypeRouter::with_pipeline_ctx(
            pipeline_context(None),
            SignalTypeRouterConfig::default(),
        );
        bench_process!(
            group,
            BenchmarkId::new("signal_type", routes),
            &routes,
            rt,
            local,
            control,
            control_eh,
            control_rx,
            data
        );
    }
    group.finish();
}

/// Confirms neither router's cost depends on the payload it is routing.
///
/// The tenant context is one answer for the whole request and the signal type
/// is one enum, so batch size should not appear in either number.
fn bench_payload(c: &mut Criterion) {
    let (rt, local) = setup_test_runtime();
    let routes = 16;
    let value = tenant_value(routes - 1);
    let mut group = c.benchmark_group("tenant_routing/by_payload");

    for resources in RESOURCES {
        let reg = registry(routes);
        let data = pdata(&reg, resources, &value);

        let ports: Vec<String> = (0..routes).map(port_name).collect();
        let (mut eh, rx) = wire(&ports);
        let mut router = tenant_router(reg.clone(), routes);
        bench_process!(
            group,
            BenchmarkId::new("tenant", resources),
            &resources,
            rt,
            local,
            router,
            eh,
            rx,
            data
        );

        let (mut control_eh, control_rx) = wire(&[PORT_LOGS.to_owned()]);
        let mut control = SignalTypeRouter::with_pipeline_ctx(
            pipeline_context(None),
            SignalTypeRouterConfig::default(),
        );
        bench_process!(
            group,
            BenchmarkId::new("signal_type", resources),
            &resources,
            rt,
            local,
            control,
            control_eh,
            control_rx,
            data
        );
    }
    group.finish();
}

/// The selection step alone, to decompose the dispatch number.
///
/// Subtracting the control from `dispatch` gives the marginal cost of tenant
/// routing; this measures that quantity directly, so the two should agree.
fn bench_decision(c: &mut Criterion) {
    let mut group = c.benchmark_group("tenant_routing/decision");
    for routes in ROUTES {
        let value = tenant_value(routes - 1);
        let reg = registry(routes);
        let mut router = tenant_router(reg.clone(), routes);
        let data = pdata(&reg, 1, &value);
        assert!(
            router.bench_select(&data).is_some(),
            "the probe must match, or this measures the miss path"
        );
        let _ = group.bench_with_input(BenchmarkId::new("tenant", routes), &routes, |b, _| {
            b.iter(|| black_box(router.bench_select(black_box(&data))))
        });
    }
    group.finish();
}

// -- The inter-pipeline hop -------------------------------------------------

/// A registry shaped for a boundary: an edge pipeline retains the tenant id,
/// and a downstream pipeline binds it by import.
///
/// Retention is what a hop costs before anything is published. A local router
/// can match on a key without carrying it, but a value that has to reach
/// another pipeline must be retained, so the same routing decision implies a
/// larger per-request context on this path.
fn hop_registry(routes: usize) -> Arc<TenantTokenRegistry> {
    let mut tokens = TenantTokens::default();
    let _ = tokens.insert(
        "edge".to_owned(),
        TenantTokenSpec {
            extractors: vec![Extractor::TransportHeader {
                key: "tenant_id".to_owned(),
                transport_header: HEADER.to_owned(),
                retain: true,
                bag: false,
            }],
        },
    );
    let _ = tokens.insert(
        "downstream".to_owned(),
        TenantTokenSpec {
            extractors: vec![Extractor::ImportedKey {
                key: "local_tenant".to_owned(),
                imported_key: "tenant_id".to_owned(),
                retain: true,
                bag: false,
            }],
        },
    );
    let mut builder = TenantTokenRegistryBuilder::new();
    builder.add_tokens(&tokens).expect("tokens compile");
    let conditions: Vec<Condition> = (0..routes)
        .map(|i| Condition {
            entries: vec![Entry {
                key: "tenant_id".to_owned(),
                value: Some(tenant_value(i)),
            }],
        })
        .collect();
    builder
        .declare_conditions(Some(&["edge".to_owned()]), &conditions)
        .expect("conditions declare");
    Arc::new(builder.build(0).expect("registry builds"))
}

/// The compiled boundary a message crosses, both halves.
struct Boundary {
    registry: Arc<TenantTokenRegistry>,
    conditions: ConditionSet,
    export: Box<[KeyId]>,
    import: BoundaryFilter,
    bound: u64,
}

fn boundary(routes: usize) -> Boundary {
    let registry = hop_registry(routes);
    let conditions: Vec<Condition> = (0..routes)
        .map(|i| Condition {
            entries: vec![Entry {
                key: "tenant_id".to_owned(),
                value: Some(tenant_value(i)),
            }],
        })
        .collect();
    let allow = TenantBoundaryPolicy {
        allow_keys: vec!["tenant_id".to_owned()],
    };
    Boundary {
        conditions: registry
            .condition_set(Some(&["edge".to_owned()]), &conditions)
            .expect("conditions compile"),
        export: registry.compile_policy(&allow),
        import: registry.compile_filter(&allow),
        bound: registry
            .token_mask(Some(&["downstream".to_owned()]))
            .expect("downstream is declared"),
        registry,
    }
}

/// What it costs to move one message to the destination its tenant selects,
/// staying inside one pipeline versus crossing a topic into another.
///
/// Both arms start from the same decision -- one `ConditionSet` probe over the
/// same packed context -- and end the same way, with the message admitted to a
/// local downstream port. Everything between them is what the boundary costs:
/// two clones, a repack out, a publish, a receive, and a repack in.
fn bench_hop(c: &mut Criterion) {
    let (rt, local) = setup_test_runtime();
    let mut group = c.benchmark_group("tenant_routing/hop");

    for routes in [1usize, 16] {
        let value = tenant_value(routes - 1);

        // Local: the floor, re-measured on the retained-key registry so the
        // two arms differ only by the hop.
        let reg = hop_registry(routes);
        let data = pdata(&reg, 1, &value);
        let ports: Vec<String> = (0..routes).map(port_name).collect();
        let (mut eh, rx) = wire(&ports);
        let mut router = tenant_router(reg.clone(), routes);
        bench_process!(
            group,
            BenchmarkId::new("local", routes),
            &routes,
            rt,
            local,
            router,
            eh,
            rx,
            data
        );

        // Topic: the same decision, then the boundary.
        let b = boundary(routes);
        let hop_data = pdata(&b.registry, 1, &value);
        let broker = TopicBroker::<OtapPdata>::new();
        let topic = broker
            .create_topic(
                "bench_hop",
                TopicOptions::BalancedOnly { capacity: 4096 },
                InMemoryBackend,
            )
            .expect("topic creates");
        let mut subscription = topic
            .subscribe(
                SubscriptionMode::Balanced { group: "g1".into() },
                SubscriberOptions::default(),
            )
            .expect("subscription creates");
        let (hop_eh, hop_rx) = wire(&ports);
        let out_port = PortName::from(port_name(routes - 1));
        let mut scratch = TokenScratch::new();

        let _ = group.bench_with_input(BenchmarkId::new("topic", routes), &routes, |bch, _| {
            bch.iter_custom(|iters| {
                rt.block_on(local.run_until(async {
                    let start = Instant::now();
                    for i in 0..iters {
                        // Egress: decide, then repack to what the boundary admits.
                        let selected = hop_data
                            .tenant_view()
                            .and_then(|v| b.conditions.first_match(&v));
                        let _ = black_box(selected.expect("the probe must match"));

                        let mut published = hop_data.clone_without_context();
                        let repacked = published
                            .tenant_view()
                            .and_then(|v| b.registry.export_boundary(&mut scratch, &v, &b.export));
                        match repacked {
                            Some(words) => published.set_tenant(words),
                            None => published.clear_tenant(),
                        }
                        match topic.try_publish(Arc::new(published)).expect("publish") {
                            PublishOutcome::Published => {}
                            PublishOutcome::DroppedOnFull => panic!("topic queue full"),
                        }

                        // Ingress: admit, then rebuild this pipeline's tokens.
                        let RecvItem::Message(env) = subscription.recv().await.expect("delivery")
                        else {
                            panic!("unexpected lag on a balanced subscription");
                        };
                        let mut inbound = env.payload.clone_without_context();
                        let rebuilt = inbound.tenant_view().and_then(|v| {
                            b.registry
                                .resolve_imported(&mut scratch, &v, &b.import, b.bound)
                        });
                        match rebuilt {
                            Some(words) => inbound.set_tenant(words),
                            None => inbound.clear_tenant(),
                        }

                        let _ = hop_eh
                            .try_admit_message_with_source_node_to(out_port.clone(), inbound)
                            .expect("downstream admits");
                        if i % 32 == 31 {
                            drain(&hop_rx);
                        }
                    }
                    let elapsed = start.elapsed();
                    drain(&hop_rx);
                    elapsed
                }))
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_dispatch,
    bench_payload,
    bench_decision,
    bench_hop
);
criterion_main!(benches);
