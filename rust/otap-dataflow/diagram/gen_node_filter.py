#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:filter`` (FilterProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/filter_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/filter_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="filter_processor",
    urn="urn:otel:processor:filter",
    subtitle="Applies configured log and trace filters after converting inputs to OTAP Arrow records.",
    config_fields=[
        ("logs",   "LogFilter"),
        ("traces", "TraceFilter"),
    ],
    state=[],
    effects=[
        "send_message_with_source_node",
        "timed",
    ],
    role="processor",
    output_formats=["OTAP"],
    calldata=[],
    control_msgs=["CollectTelemetry"],
    notes=[
        "Logs and traces are filtered with their configured OTAP filter definitions.",
        "Metrics currently pass through the converted OTAP Arrow records unchanged.",
        "Inputs are decoded from transport-optimized ids before filters run.",
        "Outputs are emitted as OTAP Arrow records even when the input arrived as OTLP bytes.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_filter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
