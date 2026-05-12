#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:attribute`` (AttributesProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/attributes_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="attributes_processor",
    urn="urn:otel:processor:attribute",
    subtitle="Applies configured attribute edits to selected OTAP attribute domains.",
    config_fields=[
        ("actions",  "Vec<Action>"),
        ("apply_to", "Option<Vec<String>>"),
    ],
    state=[],
    effects=[
        "send_message_with_source_node",
        "timed",
    ],
    role="processor",
    output_formats=["OTAP"],
    control_msgs=["CollectTelemetry"],
    notes=[
        "Supported actions are rename, delete, insert, and upsert; unsupported actions deserialize but are ignored.",
        "apply_to selects signal, resource, and/or scope attribute domains; omitted apply_to defaults to signal attributes.",
        "OTLP inputs are converted to OTAP records before transforms run, so non-noop outputs are OTAP.",
        "A no-action configuration forwards the original pdata unchanged as a fast path.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_attributes.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
