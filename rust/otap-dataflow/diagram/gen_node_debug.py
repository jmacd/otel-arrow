#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:debug`` (DebugProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/debug_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/debug_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="debug_processor",
    urn="urn:otel:processor:debug",
    subtitle="Forwards pdata while emitting decoded debug output for selected signals.",
    config_fields=[
        ("verbosity", "Verbosity"),
        ("mode",      "DisplayMode"),
        ("signals",   "HashSet<SignalActive>"),
        ("output",    "OutputMode"),
        ("filters",   "Vec<FilterRules>"),
        ("sampling",  "SamplingConfig"),
    ],
    state=[
        "sampling window counters",
    ],
    effects=[
        "connected_ports",
        "notify_ack",
        "notify_nack",
        "processor_id",
        "send_message_with_source_node",
        "send_message_with_source_node_to",
        "subscribe_to",
        "timed",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[("micros", "u128")],
    control_msgs=[
        "CollectTelemetry",
        "Config",
        "DrainIngress",
        "Shutdown",
        "TimerTick",
    ],
    notes=[
        "Pdata is forwarded before the processor decodes a cloned payload for debug output.",
        "Detailed verbosity subscribes to ACK/NACK and records elapsed time in calldata.",
        "Console and file outputs write locally; outports send debug output to configured named ports and forward pdata on remaining ports.",
        "The sampler keeps per-interval counters and suppresses debug output, not pdata forwarding.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_debug.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
