#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:topic`` (TopicReceiver).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/topic_receiver/mod.rs
rust/otap-dataflow/crates/engine/src/topic.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="topic_receiver",
    urn="urn:otel:receiver:topic",
    subtitle="Subscribes to an in-process TopicBroker stream and forwards "
             "each delivery downstream as pdata, optionally bridging acks "
             "back to the topic runtime.",
    config_fields=[
        ("topic", "TopicName"),
        ("subscription.mode", "broadcast | balanced"),
        ("subscription.group", "SubscriptionGroupName"),
    ],
    state=[
        "topic subscription handle",
        "pending forward future",
        "tracked-message id table",
    ],
    effects=[
        "notify_receiver_drained",
        "send_message_with_source_node",
        "start_periodic_telemetry",
        "subscribe_to",
        "try_send_message_with_source_node",
    ],
    role="receiver",
    output_formats=["OTAP", "OTLP"],
    calldata=[
        ("topic_message_id", "u8 (Context8u8)"),
    ],
    control_msgs=[
        "Ack",
        "CollectTelemetry",
        "DrainIngress",
        "Nack",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="topic delivery",
    transport_status_label="topic ack/nack bridge",
    notes=[
        "broadcast mode: every subscriber sees every message; balanced "
        "mode shares the stream across a named consumer group.",
        "Pass-through for both pdata variants; the topic runtime moves "
        "OtapPdata reference-counted, no encoding.",
        "When the topic declares ack propagation, the receiver attaches "
        "the topic message_id as 1-byte calldata so downstream Ack/Nack "
        "control messages bridge back into the topic runtime.",
        "broadcast subscriptions can lag; the broadcast_on_lag policy "
        "(emit_lag_event vs disconnect) is taken from the topic "
        "declaration, not the receiver config.",
        "DrainIngress flushes the in-flight forward, releases the "
        "subscription, and notify_receiver_drained.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_topic_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
