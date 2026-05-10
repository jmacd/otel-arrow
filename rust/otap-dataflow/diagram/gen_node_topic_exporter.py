#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:exporter:topic`` (TopicExporter).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/exporters/topic_exporter/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="topic_exporter",
    urn="urn:otel:exporter:topic",
    subtitle="Publishes pdata into an in-process TopicBroker, optionally "
             "tracking each publish for end-to-end ack/nack bridging.",
    config_fields=[
        ("topic", "TopicName"),
        ("queue_on_full", "Option<TopicQueueOnFullPolicy>"),
    ],
    state=[
        "topic handle",
        "tracked publisher",
        "pending-message map",
        "blocked publish slot",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "start_periodic_telemetry",
    ],
    role="exporter",
    output_formats=[],
    input_formats=["OTAP", "OTLP"],
    control_msgs=[
        "CollectTelemetry",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="TopicBroker publish",
    transport_status_label="tracked outcome",
    notes=[
        "queue_on_full overrides the topic's declared queue policy: "
        "block (single in-flight blocked publish), drop_newest, or "
        "drop_oldest.",
        "When the topic declares Auto ack propagation, each publish is "
        "tracked; the resolved TrackedPublishOutcome (Ack / Nack / "
        "TimedOut / TopicClosed) bridges back as notify_ack / "
        "notify_nack to upstream.",
        "Pass-through for both pdata variants; payloads are not "
        "transformed before publishing.",
        "Blocked publishes switch the inbox to control-only reads to "
        "keep Shutdown responsive.",
        "Shutdown nacks any pending tracked messages with "
        "'shutdown_nacks' counter.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_topic_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
