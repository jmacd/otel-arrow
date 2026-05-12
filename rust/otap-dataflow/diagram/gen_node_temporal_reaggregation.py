#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:temporal_reaggregation`` (TemporalReaggregationProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/temporal_reaggregation_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/temporal_reaggregation_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="temporal_reaggregation_processor",
    urn="urn:otel:processor:temporal_reaggregation",
    subtitle="Reaggregates delta metrics over a configured period and flushes lower-frequency OTAP batches.",
    config_fields=[
        ("period",                  "Duration"),
        ("inbound_request_limit",   "NonZeroUsize"),
        ("outbound_request_limit",  "NonZeroUsize"),
        ("max_stream_cardinality",  "NonZeroU16"),
    ],
    state=[
        "identity dedup maps",
        "metric aggregation builder",
        "inbound ack/nack slots",
        "pending flush calldata",
        "outbound ack/nack slots",
        "local scheduler wakeups",
    ],
    effects=[
        "cancel_wakeup",
        "notify_ack",
        "notify_nack",
        "send_message_with_source_node",
        "set_wakeup",
        "subscribe_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[
        ("outbound", "SlotKey"),
    ],
    control_msgs=["CollectTelemetry", "Shutdown", "Wakeup"],
    notes=[
        "Only metrics are aggregated; logs and traces pass through unchanged.",
        "Aggregatable metric streams keep the newest data point seen within the period.",
        "Non-aggregatable metrics are emitted immediately as passthrough OTAP records.",
        "Flushes run on Wakeup, Shutdown, or when id or stream cardinality limits are hit.",
        "Inbound Ack waits until every associated passthrough and flushed outbound is Acked.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_temporal_reaggregation.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
