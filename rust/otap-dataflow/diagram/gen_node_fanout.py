#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:fanout`` (FanoutProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/fanout_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="fanout_processor",
    urn="urn:otel:processor:fanout",
    subtitle="Clones each pdata item to configured output ports with optional ack aggregation and fallbacks.",
    config_fields=[
        ("mode",                   "DeliveryMode"),
        ("await_ack",              "AwaitAck"),
        ("destinations",           "Vec<DestinationConfig>"),
        ("timeout_check_interval", "Duration"),
        ("max_inflight",           "usize"),
    ],
    state=[
        "in-flight destination state",
        "slim primary map",
        "timeout deadline heap",
        "request id counter",
        "timer started flag",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "send_message_to",
        "start_periodic_timer",
        "subscribe_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[
        ("request_id", "u64"),
        ("dest_index", "usize"),
    ],
    control_msgs=["CollectTelemetry", "TimerTick"],
    notes=[
        "parallel mode sends all active destinations immediately; sequential mode advances after Ack.",
        "await_ack none acks upstream immediately and does not subscribe to downstream responses.",
        "await_ack primary waits for the primary destination or its fallback chain.",
        "await_ack all waits for all non-fallback destinations and fail-fast nacks on failures.",
        "timeouts use TimerTick polling and can trigger configured fallback destinations.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_fanout.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
