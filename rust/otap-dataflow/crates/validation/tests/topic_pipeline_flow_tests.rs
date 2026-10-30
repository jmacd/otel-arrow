// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end topic flow tests for OTAP topic nodes.

use bytes::Bytes;
use otel_arrow_dfe_config::TopicName;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_core_nodes::exporters::topic_exporter::{TOPIC_EXPORTER, TOPIC_EXPORTER_URN};
use otel_arrow_dfe_core_nodes::receivers::topic_receiver::{TOPIC_RECEIVER, TOPIC_RECEIVER_URN};
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::config::{ExporterConfig, ReceiverConfig};
use otel_arrow_dfe_engine::control::{
    Controllable, NodeControlMsg, pipeline_completion_msg_channel, runtime_ctrl_msg_channel,
};
use otel_arrow_dfe_engine::effect_handler::SourceTagging;
use otel_arrow_dfe_engine::local::message::{LocalReceiver, LocalSender};
use otel_arrow_dfe_engine::message::{Receiver as PDataReceiver, Sender as PDataSender};
use otel_arrow_dfe_engine::node::{NodeWithPDataReceiver, NodeWithPDataSender};
use otel_arrow_dfe_engine::testing::exporter::create_test_pipeline_context;
use otel_arrow_dfe_engine::testing::{
    create_not_send_channel, setup_test_runtime, test_node, test_pipeline_runtime_services,
};
use otel_arrow_dfe_engine::topic::{
    TopicBroadcastAckMode, TopicBroadcastOnLagPolicy, TopicBroker, TopicOptions, TopicSet,
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtlpProtoBytes;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use prost::Message as _;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_test_pdata() -> OtapPdata {
    let logs = otel_arrow_dfe_pdata::testing::fixtures::log_with_no_scope();
    let mut bytes = Vec::new();
    logs.encode(&mut bytes).expect("log payload should encode");
    OtapPdata::new_todo_context(OtlpProtoBytes::ExportLogsRequest(Bytes::from(bytes)).into())
}

/// Scenario: an OTAP topic exporter sends pdata to a receiver on the same broker topic.
/// Guarantees: the receiver obtains the original signal payload.
#[test]
fn topic_exporter_to_topic_receiver_transfers_pdata() {
    topic_transfer_case(false, false);
}

/// Scenario: a topic declares a flat header contract and both nodes rename their side of the transfer.
/// Guarantees: factories bind the contract, only mapped metadata crosses, and payload/routing stay independent.
#[test]
fn topic_exporter_to_receiver_transfers_mapped_context() {
    topic_transfer_case(true, false);
}

/// Scenario: the receiver names its imported header/randomness transfer as a local composite.
/// Guarantees: generated values survive unchanged, qualified references bind, and flat fields do not leak.
#[test]
fn topic_receiver_stores_implicit_composite() {
    topic_transfer_case(true, true);
}

/// Scenario: implicit receiver composites collide with visible entries or reference unknown condition keys.
/// Guarantees: context compilation rejects the configuration before topic subscriptions are created.
#[test]
fn topic_implicit_composite_validates_namespace_and_conditions() {
    let spec = json!({
        "version": "otel_dataflow/v1",
        "topics": {"ingress": {"context": {"transfer": {"tenant_id": {"type": "authorized_identity"}}}}},
        "groups": {"g": {"pipelines": {"p": {"nodes": {
            "receiver": {"type": "receiver:topic", "config": {
                "topic": "ingress", "context": {"store_composite": "topic_exp", "conditions": {"tenant_id": "1"}}
            }}
        }}}}}
    });
    for (collision, unknown_key) in [(true, false), (false, true)] {
        let mut value = spec.clone();
        if collision {
            value["groups"]["g"]["pipelines"]["p"]["policies"] = json!({"context": {"entries": {
                "topic_exp": [{"type": "constant", "name": "route", "value": "existing"}]
            }}});
        }
        if unknown_key {
            value["groups"]["g"]["pipelines"]["p"]["nodes"]["receiver"]["config"]["context"]["conditions"] =
                json!({"unknown": "1"});
        }
        let resolved =
            otel_arrow_dfe_config::engine::OtelDataflowSpec::from_json(&value.to_string())
                .unwrap()
                .resolve();
        let error = otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY
            .compile_initial_context(&resolved)
            .unwrap_err();
        assert!(error.to_string().contains(if collision {
            "conflicts"
        } else {
            "unknown transfer field"
        }));
    }
}

fn topic_transfer_case(explicit: bool, composite: bool) {
    let (rt, local_tasks) = setup_test_runtime();
    rt.block_on(local_tasks.run_until(async move {
        let broker = TopicBroker::<OtapPdata>::new();
        let topic_name = TopicName::parse("ingress").expect("topic name should parse");
        let handle = broker
            .create_in_memory_topic(
                topic_name.clone(),
                TopicOptions::Mixed {
                    balanced_capacity: 16,
                    broadcast_capacity: 16,
                    on_lag: TopicBroadcastOnLagPolicy::DropOldest,
                    ack_mode: TopicBroadcastAckMode::First,
                },
            )
            .expect("topic should be created");

        let mut schema_json = json!({
            "transfer": {"workspace": {"type": "transport_header"}}
        });
        if composite {
            schema_json["transfer"]["randomness"] = json!({"type": "randomness"});
        }
        let schema = explicit.then(|| {
            serde_json::from_value(schema_json.clone()).expect("valid transfer schema")
        });
        let binding = otel_arrow_dfe_engine::topic::PipelineTopicBinding::from(handle)
            .with_context_transfer(schema);
        let exporter_set = TopicSet::new("exporter-set");
        _ = exporter_set.insert(topic_name.clone(), binding.clone());
        let receiver_set = TopicSet::new("receiver-set");
        _ = receiver_set.insert(topic_name.clone(), binding);

        let mut exporter_ctx = create_test_pipeline_context();
        exporter_ctx.set_topic_set(exporter_set);
        let mut receiver_ctx = create_test_pipeline_context();
        receiver_ctx.set_topic_set(receiver_set);

        let exporter_node = test_node("topic_exporter");
        let receiver_node = test_node("topic_receiver");

        let mut exporter_user_cfg = NodeUserConfig::new_exporter_config(TOPIC_EXPORTER_URN);
        exporter_user_cfg.config = json!({
            "topic": "ingress",
            "queue_on_full": "block"
        });
        let mut receiver_user_cfg = NodeUserConfig::new_receiver_config(TOPIC_RECEIVER_URN);
        receiver_user_cfg.config = json!({
            "topic": "ingress",
            "subscription": {
                "mode": "balanced",
                "group": "sut-workers"
            }
        });

        if explicit {
            exporter_user_cfg.config["context"] = json!({"export": {
                "workspace": {"required": true, "selector": {"type": "named", "named": ["source"]}}
            }});
            receiver_user_cfg.config["context"] = json!({"import": {"workspace": {"store_as": "destination"}}});
        }
        if composite {
            exporter_user_cfg.config["context"]["export"]["randomness"] =
                json!({"required": true, "selector": {"type": "named", "named": ["delivery:id"]}});
            receiver_user_cfg.config["context"] = json!({"store_composite": "topic_exp", "conditions": {"workspace": "value"}});
        }
        let mut source_plan = None;
        for (pipeline, node, user) in [
            (&mut exporter_ctx, "topic_exporter", &exporter_user_cfg),
            (&mut receiver_ctx, "topic_receiver", &receiver_user_cfg),
        ] {
            let mut pipeline_config = json!({
                "nodes": {
                    node: user,
                    "source_receiver": {"type": "receiver:internal_telemetry", "config": {}}
                },
                "policies": {"transport_headers": {
                    "header_capture": {"headers": [{"match_names": ["X-Shared"], "store_as": "shared"}]}
                }}
            });
            if node == "topic_exporter" {
                pipeline_config["policies"]["transport_headers"]["header_capture"]["headers"]
                    .as_array_mut().expect("capture header array").push(json!({"match_names":["X-Private"],"store_as":"private"}));
                if composite {
                    pipeline_config["policies"]["context"] = json!({"entries": {
                        "delivery": [{"type": "randomness", "name": "id", "value": "uuid7"}]
                    }});
                }
            } else if composite {
                pipeline_config["nodes"]["qualified_reader"] = json!({
                    "type": "exporter:topic", "config": {"topic": "next", "context": {"export": {
                        "token": {"selector": {"type": "named", "named": ["topic_exp:randomness"]}}
                    }}}
                });
            }
            let mut spec = json!({
                "version": "otel_dataflow/v1",
                "topics": {"ingress": {}, "next": {}},
                "groups": {(pipeline.pipeline_group_id().to_string()): {
                    "pipelines": {(pipeline.pipeline_id().to_string()): pipeline_config}
                }}
            });
            if explicit { spec["topics"]["ingress"]["context"] = schema_json.clone(); }
            let resolved = otel_arrow_dfe_config::engine::OtelDataflowSpec::from_json(&spec.to_string())
                .expect("topic config").resolve();
            let prepared = otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY.compile_initial_context(&resolved)
                .expect("compiled topic context declarations");
            if node == "topic_exporter" {
                source_plan = prepared.bindings.context_materialization_plan(&pipeline.pipeline_key()).cloned();
            }
            pipeline.set_compiled_context_bindings(prepared.bindings);
            *pipeline = pipeline.clone().with_node_context(
                node.into(), user.r#type.clone(), user.kind(), Default::default(),
            );
        }

        let mut exporter = (TOPIC_EXPORTER.create)(
            exporter_ctx,
            exporter_node.clone(),
            Arc::new(exporter_user_cfg),
            &ExporterConfig::new("topic_exporter"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic exporter should be created");

        let mut receiver = (TOPIC_RECEIVER.create)(
            receiver_ctx,
            receiver_node.clone(),
            Arc::new(receiver_user_cfg),
            &ReceiverConfig::new("topic_receiver"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic receiver should be created");

        let (exporter_input_tx, exporter_input_rx) = create_not_send_channel::<OtapPdata>(8);
        exporter
            .set_pdata_receiver(
                exporter_node.clone(),
                PDataReceiver::Local(LocalReceiver::mpsc(exporter_input_rx)),
            )
            .expect("exporter input channel should be wired");

        let (receiver_output_tx, receiver_output_rx) = create_not_send_channel::<OtapPdata>(8);
        receiver
            .set_pdata_sender(
                receiver_node.clone(),
                "".into(),
                PDataSender::Local(LocalSender::mpsc(receiver_output_tx)),
            )
            .expect("receiver output channel should be wired");

        let exporter_ctrl = exporter.control_sender();
        let receiver_ctrl = receiver.control_sender();
        let (runtime_ctrl_tx, _runtime_ctrl_rx) = runtime_ctrl_msg_channel::<OtapPdata>(32);
        let (pipeline_completion_tx, _pipeline_completion_rx) =
            pipeline_completion_msg_channel::<OtapPdata>(32);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(64);
        let exporter_metrics = metrics_reporter.clone();
        let receiver_metrics = metrics_reporter.clone();
        let exporter_ctrl_tx = runtime_ctrl_tx.clone();
        let receiver_ctrl_tx = runtime_ctrl_tx.clone();
        let exporter_completion_tx = pipeline_completion_tx.clone();
        let receiver_completion_tx = pipeline_completion_tx.clone();
        let runtime_services = test_pipeline_runtime_services();
        let exporter_runtime_services = runtime_services.clone();

        let exporter_task = tokio::task::spawn_local(async move {
            exporter
                .start(
                    exporter_ctrl_tx,
                    exporter_completion_tx,
                    exporter_metrics,
                    Interests::empty(),
                    exporter_runtime_services,
                )
                .await
        });
        let receiver_task = tokio::task::spawn_local(async move {
            receiver
                .start(
                    receiver_ctrl_tx,
                    receiver_completion_tx,
                    receiver_metrics,
                    Interests::empty(),
                    runtime_services,
                )
                .await
        });

        let mut expected = make_test_pdata();
        let mut headers = otel_arrow_dfe_config::context::transport_headers::TransportHeaders::new();
        for name in ["source", "shared", "private"] {
            headers.push(otel_arrow_dfe_config::context::transport_headers::TransportHeader::text(
                name.try_into().expect("valid header name"), b"value".to_vec(),
            ));
        }
        expected.set_transport_headers(headers);
        let expected_id = source_plan.as_ref().map(|plan| {
            expected.context_mut().materialize_context(plan);
            expected.materialized_context().expect("source materialization")
                .entry(&"delivery".try_into().expect("composite name"))
                .expect("delivery composite")
                .member(&"id".try_into().expect("member name"))
                .expect("generated id").values().values()[0].bytes().to_vec()
        });
        exporter_input_tx
            .send(expected.clone())
            .expect("message should be sent to topic exporter");

        let mut delivered = tokio::time::timeout(Duration::from_secs(2), receiver_output_rx.recv())
            .await
            .expect("timed out waiting for topic receiver output")
            .expect("receiver output channel should stay open");

        assert_eq!(
            delivered.num_items(),
            expected.num_items(),
            "topic receiver should forward one pdata message"
        );
        if composite {
            assert!(delivered.transport_headers().is_none());
            let entry = delivered.materialized_context().expect("imported materialization")
                .entry(&"topic_exp".try_into().expect("composite name"))
                .expect("imported composite");
            assert_eq!(entry.members().len(), 2);
            assert_eq!(entry.member(&"randomness".try_into().expect("member name")).expect("randomness member").values().values()[0].bytes(), expected_id.as_ref().expect("source id"));
            assert_eq!(entry.member(&"workspace".try_into().expect("member name")).expect("workspace member").values().values()[0].bytes(), b"value");
        } else {
            let headers = delivered.transport_headers().expect("declared header crosses");
            assert_eq!(headers.len(), 1);
            assert_eq!(headers.iter().next().expect("imported header").name.as_str(), if explicit { "destination" } else { "shared" });
        }

        let deadline = Instant::now() + Duration::from_secs(1);
        exporter_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("exporter shutdown should be sent");
        receiver_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("receiver shutdown should be sent");

        let exporter_result = exporter_task.await.expect("exporter task should join");
        let receiver_result = receiver_task.await.expect("receiver task should join");
        assert!(exporter_result.is_ok(), "exporter should stop cleanly");
        assert!(receiver_result.is_ok(), "receiver should stop cleanly");
    }));
}

/// Scenario: a topic receiver is configured to tag incoming pdata with its source.
/// Guarantees: received pdata contains the configured source tag.
#[test]
fn topic_receiver_applies_source_tag_when_enabled() {
    let (rt, local_tasks) = setup_test_runtime();
    rt.block_on(local_tasks.run_until(async move {
        let broker = TopicBroker::<OtapPdata>::new();
        let topic_name = TopicName::parse("ingress").expect("topic name should parse");
        let handle = broker
            .create_in_memory_topic(
                topic_name.clone(),
                TopicOptions::Mixed {
                    balanced_capacity: 16,
                    broadcast_capacity: 16,
                    on_lag: TopicBroadcastOnLagPolicy::DropOldest,
                    ack_mode: TopicBroadcastAckMode::First,
                },
            )
            .expect("topic should be created");

        let exporter_set = TopicSet::new("exporter-set");
        _ = exporter_set.insert(topic_name.clone(), handle.clone());
        let receiver_set = TopicSet::new("receiver-set");
        _ = receiver_set.insert(topic_name.clone(), handle);

        let mut exporter_ctx = create_test_pipeline_context();
        exporter_ctx.set_topic_set(exporter_set);
        let mut receiver_ctx = create_test_pipeline_context();
        receiver_ctx.set_topic_set(receiver_set);

        let exporter_node = test_node("topic_exporter");
        let receiver_node = test_node("topic_receiver");

        let mut exporter_user_cfg = NodeUserConfig::new_exporter_config(TOPIC_EXPORTER_URN);
        exporter_user_cfg.config = json!({"topic": "ingress"});
        let mut receiver_user_cfg = NodeUserConfig::new_receiver_config(TOPIC_RECEIVER_URN);
        receiver_user_cfg.config = json!({
            "topic": "ingress",
            "subscription": {
                "mode": "balanced",
                "group": "sut-workers"
            }
        });

        let mut exporter = (TOPIC_EXPORTER.create)(
            exporter_ctx,
            exporter_node.clone(),
            Arc::new(exporter_user_cfg),
            &ExporterConfig::new("topic_exporter"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic exporter should be created");

        let mut receiver = (TOPIC_RECEIVER.create)(
            receiver_ctx,
            receiver_node.clone(),
            Arc::new(receiver_user_cfg),
            &ReceiverConfig::new("topic_receiver"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic receiver should be created");

        // Match runtime fan-in behavior where source tagging is enabled.
        receiver.set_source_tagging(SourceTagging::Enabled);

        let (exporter_input_tx, exporter_input_rx) = create_not_send_channel::<OtapPdata>(8);
        exporter
            .set_pdata_receiver(
                exporter_node.clone(),
                PDataReceiver::Local(LocalReceiver::mpsc(exporter_input_rx)),
            )
            .expect("exporter input channel should be wired");

        let (receiver_output_tx, receiver_output_rx) = create_not_send_channel::<OtapPdata>(8);
        receiver
            .set_pdata_sender(
                receiver_node.clone(),
                "".into(),
                PDataSender::Local(LocalSender::mpsc(receiver_output_tx)),
            )
            .expect("receiver output channel should be wired");

        let exporter_ctrl = exporter.control_sender();
        let receiver_ctrl = receiver.control_sender();
        let (runtime_ctrl_tx, _runtime_ctrl_rx) = runtime_ctrl_msg_channel::<OtapPdata>(32);
        let (pipeline_completion_tx, _pipeline_completion_rx) =
            pipeline_completion_msg_channel::<OtapPdata>(32);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(64);
        let exporter_metrics = metrics_reporter.clone();
        let receiver_metrics = metrics_reporter.clone();
        let exporter_ctrl_tx = runtime_ctrl_tx.clone();
        let receiver_ctrl_tx = runtime_ctrl_tx.clone();
        let exporter_completion_tx = pipeline_completion_tx.clone();
        let receiver_completion_tx = pipeline_completion_tx.clone();
        let runtime_services = test_pipeline_runtime_services();
        let exporter_runtime_services = runtime_services.clone();

        let exporter_task = tokio::task::spawn_local(async move {
            exporter
                .start(
                    exporter_ctrl_tx,
                    exporter_completion_tx,
                    exporter_metrics,
                    Interests::empty(),
                    exporter_runtime_services,
                )
                .await
        });
        let receiver_task = tokio::task::spawn_local(async move {
            receiver
                .start(
                    receiver_ctrl_tx,
                    receiver_completion_tx,
                    receiver_metrics,
                    Interests::empty(),
                    runtime_services,
                )
                .await
        });

        exporter_input_tx
            .send(make_test_pdata())
            .expect("message should be sent to topic exporter");

        let delivered = tokio::time::timeout(Duration::from_secs(2), receiver_output_rx.recv())
            .await
            .expect("timed out waiting for topic receiver output")
            .expect("receiver output channel should stay open");

        assert_eq!(
            delivered.get_source_node(),
            Some(receiver_node.index),
            "receiver should tag outgoing pdata with source node when enabled"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        exporter_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("exporter shutdown should be sent");
        receiver_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("receiver shutdown should be sent");

        let exporter_result = exporter_task.await.expect("exporter task should join");
        let receiver_result = receiver_task.await.expect("receiver task should join");
        assert!(exporter_result.is_ok(), "exporter should stop cleanly");
        assert!(receiver_result.is_ok(), "receiver should stop cleanly");
    }));
}
