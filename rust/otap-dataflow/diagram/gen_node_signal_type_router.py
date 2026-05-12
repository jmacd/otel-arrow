#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:type_router`` (SignalTypeRouter).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/signal_type_router/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="signal_type_router",
    urn="urn:otel:processor:type_router",
    subtitle="Routes pdata to logs, metrics, or traces output ports according to signal type.",
    config_fields=[
        ("admission_policy", "SelectedRouteAdmissionPolicy"),
    ],
    state=[
        "pause candidate ports",
        "parked route map",
        "local scheduler wakeups",
    ],
    effects=[
        "cancel_wakeup",
        "connected_ports",
        "default_port",
        "notify_nack",
        "send_message_with_source_node",
        "set_wakeup",
        "try_admit_message_with_source_node_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    named_outputs=["logs", "metrics", "traces", "default"],
    calldata=[],
    control_msgs=["CollectTelemetry", "Shutdown", "Wakeup"],
    notes=[
        "The router prefers named logs, metrics, and traces ports when they are connected.",
        "If the named port is not wired, the default output is used when present.",
        "Selected-route Full can be nacked immediately or parked by backpressure policy.",
        "Parked routes are retried through local Wakeup scheduling and nacked during shutdown.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_signal_type_router.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
