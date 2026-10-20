// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_config::node::NodeKind;
use otel_arrow_dfe_config::transport_headers::{TransportHeader, TransportHeaders, ValueKind};
use otel_arrow_dfe_engine::context_declaration::{ContextMaterializationPlan, PreparedContext};
use otel_arrow_dfe_engine::testing::processor::TestPhase;
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;

/// Scenario: two selected header references contain equal, different, absent, and empty values.
/// Guarantees: batching uses the full tuple and preserves available dimensions when another is missing.
#[test]
fn partition_reference_list_preserves_partial_absence() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let mut cfg = config(fmt, 2, 4);
        cfg["partition_by"] = json!([{ "type": "named", "named": ["tenant", "workspace"] }]);
        let (phase, _) = setup(cfg, json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                for (index, tenant, workspace) in [
                    (0, "a", None),
                    (1, "b", None),
                    (2, "a", None),
                    (3, "a", Some("")),
                ] {
                    let mut pdata = input(fmt, Some(tenant), 1, index);
                    let mut selected = vec![("tenant", tenant)];
                    if let Some(workspace) = workspace {
                        selected.push(("workspace", workspace));
                    }
                    headers(&mut pdata, &selected);
                    ctx.process(Message::PData(pdata)).await.expect("input");
                }
                flush_timer(&mut ctx).await;
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 3);
                let mut seen = Vec::new();
                for output in outputs {
                    let fields = output
                        .transport_headers()
                        .expect("present tenant")
                        .iter()
                        .map(|header| {
                            assert!(header.value.original_name.is_none());
                            (
                                header.name.to_string(),
                                std::str::from_utf8(header.value.bytes)
                                    .expect("text")
                                    .to_owned(),
                            )
                        })
                        .collect::<HashMap<_, _>>();
                    assert!(!fields.contains_key("unrelated"));
                    let tenant = fields.get("tenant").expect("tenant").as_str();
                    let workspace = fields.get("workspace").map(String::as_str);
                    let indices = match (tenant, workspace) {
                        ("a", None) => vec![0, 2],
                        ("b", None) => vec![1],
                        ("a", Some("")) => vec![3],
                        other => panic!("unexpected tuple {other:?}"),
                    };
                    let expected = indices
                        .iter()
                        .map(|&i| OtlpProtoMessage::Logs(residual_logs(i, 1)))
                        .collect::<Vec<_>>();
                    assert_equivalent(&expected, &[otap_pdata_to_message(&output)]);
                    seen.push(indices);
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                seen.sort();
                assert_eq!(seen, [vec![0, 2], vec![1], vec![3]]);
                let mut done = Completions::new(4);
                done.drain(&mut rx);
                assert_eq!(done.acks, [1; 4]);
                assert_eq!(done.nacks, [0; 4]);
            })
            .validate(|_| async {});
    }
}

/// Scenario: a qualified aliased member is equal across otherwise different conditional composites.
/// Guarantees: only that member groups and propagates; failed parent gates share the missing dimension.
#[test]
fn partition_qualified_member_omits_unselected_siblings() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let mut cfg = config(fmt, 2, 3);
        cfg["partition_by"] = json!([{ "type": "named", "named": ["product_user:customer"] }]);
        let (phase, plan) = setup(
            cfg,
            json!({
                "product_user": [
                    { "type": "transport_header", "name": "account", "store_as": "customer" },
                    { "type": "transport_header", "name": "workspace" },
                    { "type": "randomness", "name": "correlation", "value": "uuid7" },
                    { "type": "transport_header_match", "name": "environment", "value": "prod" }
                ]
            }),
        );
        let plan = plan.expect("member selection materializes full gate");
        let projection = plan
            .layout()
            .resolve_member(
                &"product_user".try_into().expect("name"),
                &"customer".try_into().expect("member"),
            )
            .expect("qualified projection");
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                for (index, account, workspace, environment) in [
                    (0, "a", Some("w"), "prod"),
                    (1, "a", Some("x"), "prod"),
                    (2, "b", None, "prod"),
                    (3, "a", Some("w"), "dev"),
                ] {
                    let mut pdata = residual_pdata(fmt, &residual_logs(index, 1), index);
                    let mut selected = vec![("account", account), ("environment", environment)];
                    if let Some(workspace) = workspace {
                        selected.push(("workspace", workspace));
                    }
                    headers(&mut pdata, &selected);
                    pdata.context_mut().materialize_context(&plan);
                    ctx.process(Message::PData(pdata)).await.expect("input");
                }
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 2);
                let mut present = 0;
                for output in outputs {
                    assert!(output.transport_headers().is_none());
                    assert!(output.peer_addr().is_none());
                    if let Some(selected) = output.resolve_materialized_context(&plan, &projection)
                    {
                        present += 1;
                        selected.visit_members(|member| {
                            assert_eq!(member.name().as_str(), "customer");
                            assert_eq!(member.values().values()[0].as_str(), Some("a"));
                            assert!(member.values().values()[0].original_name().is_none());
                        });
                        let entries = output.materialized_context().expect("projected").entries();
                        assert_eq!(entries.len(), 1);
                        assert_eq!(entries[0].members().len(), 1);
                        assert_equivalent(
                            &[
                                OtlpProtoMessage::Logs(residual_logs(0, 1)),
                                OtlpProtoMessage::Logs(residual_logs(1, 1)),
                            ],
                            &[otap_pdata_to_message(&output)],
                        );
                    } else {
                        assert!(output.materialized_context().is_none());
                        assert_equivalent(
                            &[
                                OtlpProtoMessage::Logs(residual_logs(2, 1)),
                                OtlpProtoMessage::Logs(residual_logs(3, 1)),
                            ],
                            &[otap_pdata_to_message(&output)],
                        );
                    }
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                assert_eq!(present, 1);
                let mut done = Completions::new(4);
                done.drain(&mut rx);
                assert_eq!(done.acks, [1; 4]);
            })
            .validate(|_| async {});
    }
}

async fn flush_timer(ctx: &mut otel_arrow_dfe_engine::testing::processor::TestContext<OtapPdata>) {
    let now = Instant::now() + Duration::from_secs(10);
    while let Some(message) = ctx.take_due_local_control(now) {
        ctx.process(Message::Control(message))
            .await
            .expect("scheduled wakeup");
    }
}

/// Scenario: independent partition keys exhaust shared inbound or outbound completion slots.
/// Guarantees: per-signal tracking limits are not multiplied by the number of active keys.
#[test]
fn partition_completion_limits_are_shared() {
    for inbound_limit in [true, false] {
        let mut cfg = config(ResidualFormat::Otap, 4, 2);
        cfg[if inbound_limit {
            "inbound_request_limit"
        } else {
            "outbound_request_limit"
        }] = json!(1);
        let (phase, _) = setup(cfg, json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                let mut done = Completions::new(if inbound_limit { 2 } else { 4 });
                if inbound_limit {
                    ctx.process(Message::PData(input(ResidualFormat::Otap, Some("a"), 1, 0)))
                        .await
                        .expect("a");
                    ctx.process(Message::PData(input(ResidualFormat::Otap, Some("b"), 1, 1)))
                        .await
                        .expect("b refused");
                    done.drain(&mut rx);
                    assert_eq!(done.nacks, [0, 1]);
                    flush_timer(&mut ctx).await;
                    for output in ctx.drain_pdata().await {
                        assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                        respond(&mut ctx, output, &AckPolicy::Ack).await;
                    }
                    done.drain(&mut rx);
                    assert_eq!(done.acks, [1, 0]);
                } else {
                    for (index, tenant) in [(0, "a"), (1, "a"), (2, "b"), (3, "b")] {
                        ctx.process(Message::PData(input(
                            ResidualFormat::Otap,
                            Some(tenant),
                            2,
                            index,
                        )))
                        .await
                        .expect("input");
                    }
                    done.drain(&mut rx);
                    assert_eq!(done.nacks, [0, 0, 1, 1]);
                    let outputs = ctx.drain_pdata().await;
                    assert_eq!(
                        outputs.len(),
                        2,
                        "slot exhaustion preserves the existing send behavior"
                    );
                    for output in outputs {
                        if output_tenant(&output).as_deref() == Some("a") {
                            respond(&mut ctx, output, &AckPolicy::Ack).await;
                        } else {
                            assert!(!output.has_ack_or_nack_interests());
                        }
                    }
                    done.drain(&mut rx);
                    assert_eq!(done.acks, [1, 1, 0, 0]);
                }
            })
            .validate(|_| async {});
    }
}

/// Scenario: every signal/format pair buffers the same context key and then shuts down.
/// Guarantees: formats and signals stay separate, each has its own key capacity, and shutdown drains all.
#[test]
fn partition_shutdown_preserves_signal_and_format_isolation() {
    use otel_arrow_dfe_pdata::testing::fixtures::{
        metrics_sum_with_no_resource, traces_with_no_resource,
    };
    let (phase, _) = setup(
        json!({
            "format": "preserve",
            "otap": { "min_size": 100000, "sizer": "items" },
            "otlp": { "min_size": 1000000, "sizer": "bytes" },
            "partition_by": [{ "type": "named", "named": ["tenant"] }],
            "max_active_partitions": 1
        }),
        json!({}),
    );
    phase
        .run_test(move |mut ctx| async move {
            let mut expected = Vec::new();
            for otap in [false, true] {
                for message in [
                    OtlpProtoMessage::Logs(residual_logs(0, 1)),
                    OtlpProtoMessage::Traces(traces_with_no_resource()),
                    OtlpProtoMessage::Metrics(metrics_sum_with_no_resource()),
                ] {
                    let payload: OtapPayload = if otap {
                        otlp_to_otap(&message).into()
                    } else {
                        otlp_message_to_bytes(&message).into()
                    };
                    let mut pdata = OtapPdata::new_default(payload);
                    headers(&mut pdata, &[("tenant", "a")]);
                    expected.push((pdata.signal_format(), pdata.signal_type()));
                    ctx.process(Message::PData(pdata)).await.expect("input");
                }
            }
            assert!(ctx.drain_pdata().await.is_empty());
            ctx.process(Message::Control(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "test".into(),
            }))
            .await
            .expect("shutdown");
            let outputs = ctx.drain_pdata().await;
            assert_eq!(outputs.len(), 6);
            for output in outputs {
                assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                let index = expected
                    .iter()
                    .position(|pair| *pair == (output.signal_format(), output.signal_type()))
                    .expect("one output for each pair");
                _ = expected.remove(index);
            }
            assert!(expected.is_empty());
            assert!(ctx.next_local_control_deadline().is_none());
        })
        .validate(|_| async {});
}

fn prepare(config: &Value, entries: Value) -> Result<PreparedContext, EngineError> {
    let spec = OtelDataflowSpec::from_json(
        &json!({
            "version": "otel_dataflow/v1",
            "engine": {},
            "policies": { "context": { "entries": entries } },
            "groups": { "grp": { "pipelines": { "pipe": {
                "nodes": {
                    "receiver": { "type": "receiver:internal_telemetry", "config": {} },
                    "batch-processor-test": { "type": "processor:batch", "config": config },
                    "exporter": { "type": "exporter:noop", "config": {} }
                },
                "connections": [
                    { "from": "receiver", "to": "batch-processor-test" },
                    { "from": "batch-processor-test", "to": "exporter" }
                ]
            } } } }
        })
        .to_string(),
    )
    .expect("pipeline configuration");
    OTAP_PIPELINE_FACTORY.compile_initial_context(&spec.resolve())
}

fn setup(
    config: Value,
    entries: Value,
) -> (TestPhase<OtapPdata>, Option<ContextMaterializationPlan>) {
    let prepared = prepare(&config, entries).expect("compile partition declarations");
    let rt = TestRuntime::new();
    let controller = ControllerContext::new(rt.metrics_registry());
    let mut pipeline = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
    let plan = prepared
        .bindings
        .context_materialization_plan(&pipeline.pipeline_key())
        .cloned();
    pipeline.set_compiled_context_bindings(prepared.bindings);
    let pipeline = pipeline.with_node_context(
        "batch-processor-test".into(),
        OTAP_BATCH_PROCESSOR_URN.into(),
        NodeKind::Processor,
        HashMap::new(),
    );
    let mut node_config = NodeUserConfig::new_processor_config(OTAP_BATCH_PROCESSOR_URN);
    node_config.config = config;
    let processor = create_otap_batch_processor(
        pipeline,
        test_node("batch-processor-test"),
        Arc::new(node_config),
        &ProcessorConfig::new("batch"),
    )
    .expect("construct bound processor");
    (rt.set_processor(processor), plan)
}

fn config(fmt: ResidualFormat, units: usize, cap: usize) -> Value {
    let mut config = residual_config(fmt, units);
    config["partition_by"] = json!([{ "type": "named", "named": ["tenant"] }]);
    config["max_active_partitions"] = json!(cap);
    config
}

fn headers(pdata: &mut OtapPdata, selected: &[(&str, &str)]) {
    let mut headers = TransportHeaders::new();
    for (name, value) in selected.iter().copied().chain([("unrelated", "discard")]) {
        headers.push(TransportHeader::captured(
            name.try_into().expect("name"),
            &format!("X-{name}"),
            true,
            ValueKind::Text,
            value.as_bytes(),
        ));
    }
    pdata.context_mut().set_transport_headers(headers);
    pdata.set_peer_addr("127.0.0.1:1234".parse().expect("peer address"));
}

fn input(fmt: ResidualFormat, tenant: Option<&str>, units: usize, index: usize) -> OtapPdata {
    let mut pdata = residual_pdata(fmt, &residual_logs(index, units), index);
    headers(
        &mut pdata,
        &tenant
            .map(|value| ("tenant", value))
            .into_iter()
            .collect::<Vec<_>>(),
    );
    pdata
}

fn output_tenant(pdata: &OtapPdata) -> Option<String> {
    assert!(pdata.peer_addr().is_none());
    assert!(pdata.authorized_identity_entries().is_none());
    assert!(pdata.materialized_context().is_none());
    let headers = pdata.transport_headers()?;
    assert_eq!(headers.len(), 1, "only the partition entry survives");
    let header = headers.iter().next().expect("partition header");
    assert_eq!(header.name.as_str(), "tenant");
    assert!(header.value.original_name.is_none());
    Some(
        std::str::from_utf8(header.value.bytes)
            .expect("text")
            .to_owned(),
    )
}

/// Scenario: configurations use typed primitives, qualified members, and whole composites.
/// Guarantees: selector lists compile while obsolete shapes and invalid references fail at startup.
#[test]
fn partition_configuration_and_factory_binding() {
    for selection in [
        json!([]),
        json!([{ "type": "named", "named": ["workspace", "tenant:id"] }]),
        json!([{ "type": "authorized_identity", "named": ["tenant"] }]),
        json!([{ "type": "composite", "name": "tenant" }]),
        json!([
            { "type": "named", "named": ["workspace", "tenant:id"] },
            { "type": "authorized_identity", "named": ["tenant"] },
            { "type": "composite", "name": "tenant" }
        ]),
    ] {
        let cfg = json!({ "partition_by": selection });
        assert!(
            prepare(
                &cfg,
                json!({
                    "tenant": [{ "type": "constant", "name": "id", "value": "fixed" }]
                })
            )
            .is_ok()
        );
    }
    for selection in [
        json!(["tenant", "other"]),
        json!(["tenant:member"]),
        json!(["unknown"]),
        json!([{ "name": "tenant" }]),
        json!([{ "type": "transport_header", "name": "tenant", "typo": true }]),
        json!([{ "type": "named", "named": [] }]),
        json!([{ "type": "authorized_identity", "named": [] }]),
        json!([{ "type": "composite", "name": "unknown" }]),
        json!([{ "type": "named", "named": ["tenant:unknown"] }]),
        json!([{ "type": "authorized_identity", "named": ["tenant:id"] }]),
    ] {
        assert!(prepare(&json!({ "partition_by": selection }), json!({})).is_err());
    }
    assert!(prepare(&json!({ "max_active_partitions": 0 }), json!({})).is_err());
    let (pipeline, _registry) = create_test_pipeline_context();
    assert!(
        BatchProcessor::build_from_json(
            &config(ResidualFormat::Otap, 4, 2),
            BatchProcessorMetrics::register(&pipeline),
        )
        .is_err(),
        "partitioning cannot silently run without its engine binding"
    );
}

/// Scenario: interleaved keys size-flush independently while another key awaits its deadline.
/// Guarantees: payloads do not mix, draining frees key capacity, and size flushes do not postpone peers.
#[test]
fn partition_size_timer_and_capacity_reuse() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let (phase, _) = setup(config(fmt, 4, 2), json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                let mut done = Completions::new(4);
                ctx.process(Message::PData(input(fmt, Some("a"), 2, 0)))
                    .await
                    .expect("a");
                let deadline = ctx.next_local_control_deadline().expect("first window");
                ctx.process(Message::PData(input(fmt, Some("b"), 1, 1)))
                    .await
                    .expect("b");
                ctx.process(Message::PData(input(fmt, Some("a"), 2, 2)))
                    .await
                    .expect("a again");
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                let output = outputs.into_iter().next().expect("a output");
                assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                assert_equivalent(
                    &[
                        OtlpProtoMessage::Logs(residual_logs(0, 2)),
                        OtlpProtoMessage::Logs(residual_logs(2, 2)),
                    ],
                    &[otap_pdata_to_message(&output)],
                );
                assert_eq!(ctx.next_local_control_deadline(), Some(deadline));
                respond(&mut ctx, output, &AckPolicy::Ack).await;
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 0, 1, 0]);
                ctx.process(Message::PData(input(fmt, Some("c"), 1, 3)))
                    .await
                    .expect("freed capacity");
                assert!(ctx.drain_pdata().await.is_empty());
                flush_timer(&mut ctx).await;
                let mut tenants = Vec::new();
                for output in ctx.drain_pdata().await {
                    tenants.push(output_tenant(&output).expect("present key"));
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                tenants.sort();
                assert_eq!(tenants, ["b", "c"]);
                assert!(ctx.next_local_control_deadline().is_none());
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 1, 1, 1]);
                assert_eq!(done.nacks, [0; 4]);
            })
            .validate(|_| async {});
    }
}

/// Scenario: the sole active key is missing and a present key arrives before it drains.
/// Guarantees: missing inputs share one bounded partition, overflow Nacks, and later keys are admitted.
#[test]
fn partition_missing_key_counts_toward_limit() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let (phase, _) = setup(config(fmt, 4, 1), json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                let mut done = Completions::new(5);
                for (index, tenant, units) in
                    [(0, None, 1), (1, None, 1), (2, Some("a"), 1), (3, None, 2)]
                {
                    ctx.process(Message::PData(input(fmt, tenant, units, index)))
                        .await
                        .expect("input");
                }
                done.drain(&mut rx);
                assert_eq!(done.nacks, [0, 0, 1, 0, 0]);
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                let output = outputs.into_iter().next().expect("missing partition");
                assert_eq!(output_tenant(&output), None);
                respond(&mut ctx, output, &AckPolicy::Ack).await;
                ctx.process(Message::PData(input(fmt, Some("a"), 1, 4)))
                    .await
                    .expect("new key");
                flush_timer(&mut ctx).await;
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                for output in outputs {
                    assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 1, 0, 1, 1]);
            })
            .validate(|_| async {});
    }
}

/// Scenario: an interleaved partition size-flush retains a remainder that is later Nacked.
/// Guarantees: the remainder keeps its key and input alive, and unrelated partitions complete normally.
#[test]
fn partition_remainder_routes_completion_to_its_input() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let (phase, _) = setup(config(fmt, 4, 2), json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                let mut done = Completions::new(3);
                for (index, tenant, units) in [(0, "a", 3), (1, "b", 1), (2, "a", 6)] {
                    ctx.process(Message::PData(input(fmt, Some(tenant), units, index)))
                        .await
                        .expect("input");
                }
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 2);
                for output in outputs {
                    assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 0, 0]);
                flush_timer(&mut ctx).await;
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 2);
                for output in outputs {
                    let policy = if output_tenant(&output).as_deref() == Some("a") {
                        AckPolicy::Nack("remainder rejected")
                    } else {
                        AckPolicy::Ack
                    };
                    respond(&mut ctx, output, &policy).await;
                }
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 1, 0]);
                assert_eq!(done.nacks, [0, 0, 1]);
            })
            .validate(|_| async {});
    }
}

/// Scenario: a different key passes through while partition and inbound tracking capacity are full.
/// Guarantees: passthrough consumes neither capacity, projects metadata, and retains direct Ack routing.
#[test]
fn partition_passthrough_projects_without_buffering() {
    for fmt in [ResidualFormat::Otap, ResidualFormat::Otlp] {
        let mut cfg = config(fmt, 4, 1);
        cfg["inbound_request_limit"] = json!(1);
        let (phase, _) = setup(cfg, json!({}));
        phase
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);
                ctx.process(Message::PData(input(fmt, Some("a"), 1, 0)))
                    .await
                    .expect("buffered");
                ctx.process(Message::PData(input(fmt, Some("b"), 4, 1)))
                    .await
                    .expect("passthrough");
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                let output = outputs.into_iter().next().expect("passthrough");
                assert_eq!(output_tenant(&output).as_deref(), Some("b"));
                let (node, ack) = next_ack(AckMsg::new(output)).expect("input subscriber");
                assert_eq!(node, 1);
                let calldata: TestCallData =
                    ack.unwind.route.calldata.try_into().expect("calldata");
                assert_eq!(calldata, TestCallData::new_with(1, 0));
                flush_timer(&mut ctx).await;
                for output in ctx.drain_pdata().await {
                    assert_eq!(output_tenant(&output).as_deref(), Some("a"));
                    respond(&mut ctx, output, &AckPolicy::Ack).await;
                }
                let mut done = Completions::new(2);
                done.drain(&mut rx);
                assert_eq!(done.acks, [1, 0]);
                assert_eq!(done.nacks, [0, 0]);
            })
            .validate(|_| async {});
    }
}

/// Scenario: a composite is consumed only by batching and inputs fail different presence requirements.
/// Guarantees: engine declarations trigger materialization, complete tuples stay isolated, and absence coalesces.
#[test]
fn partition_composites_materialize_and_project_atomically() {
    let mut cfg = config(ResidualFormat::Otap, 2, 3);
    cfg["partition_by"] = json!([{ "type": "composite", "name": "product_user" }]);
    let (phase, plan) = setup(
        cfg,
        json!({
            "product_user": [
                { "type": "transport_header", "name": "account" },
                { "type": "transport_header", "name": "workspace" },
                { "type": "transport_header_match", "name": "environment", "value": "prod" }
            ]
        }),
    );
    let plan = plan.expect("batch consumer selects the composite for materialization");
    let projection = plan
        .layout()
        .resolve_composite(&"product_user".try_into().expect("name"))
        .expect("composite projection");
    phase
        .run_test(move |mut ctx| async move {
            let (tx, _rx) = pipeline_completion_msg_channel(16);
            ctx.set_pipeline_completion_sender(tx);
            let mut tuples = Vec::new();
            for (index, workspace, environment) in [
                (0, Some("w"), "prod"),
                (1, Some("w"), "prod"),
                (2, Some("x"), "prod"),
                (3, None, "prod"),
                (4, Some("w"), "dev"),
            ] {
                let mut pdata =
                    residual_pdata(ResidualFormat::Otap, &residual_logs(index, 1), index);
                let mut selected = vec![("account", "a"), ("environment", environment)];
                if let Some(workspace) = workspace {
                    selected.push(("workspace", workspace));
                }
                headers(&mut pdata, &selected);
                pdata.context_mut().materialize_context(&plan);
                ctx.process(Message::PData(pdata)).await.expect("input");
            }
            flush_timer(&mut ctx).await;
            for output in ctx.drain_pdata().await {
                assert!(output.transport_headers().is_none());
                assert!(output.peer_addr().is_none());
                let tuple =
                    output
                        .resolve_materialized_context(&plan, &projection)
                        .map(|selected| {
                            let mut tuple = Vec::new();
                            selected.visit_members(|member| {
                                for value in member.values().values() {
                                    assert!(value.original_name().is_none());
                                    tuple.push(value.as_str().expect("text").to_owned());
                                }
                            });
                            tuple
                        });
                tuples.push(tuple);
                respond(&mut ctx, output, &AckPolicy::Ack).await;
            }
            tuples.sort();
            assert_eq!(
                tuples,
                [
                    None,
                    Some(vec!["a".into(), "w".into()]),
                    Some(vec!["a".into(), "x".into()])
                ]
            );
        })
        .validate(|_| async {});
}
