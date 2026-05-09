#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:delay`` (DelayProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/delay_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="delay_processor",
    urn="urn:otel:processor:delay",
    subtitle="Sleeps for the configured duration before forwarding each pdata message.",
    config_fields=[
        ("delay", "Duration"),
    ],
    state=[],
    effects=[
        "send_message_with_source_node",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    notes=[
        "The delay is applied only to pdata messages, not to control messages.",
        "Control variants CollectTelemetry, Config, and Shutdown are explicitly accepted with empty handlers.",
        "The processor preserves the original payload format and source node when forwarding.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_delay.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
