#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:condense_attributes`` (CondenseAttributesProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/contrib-nodes/src/processors/condense_attributes_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="condense_attributes_processor",
    urn="urn:otel:processor:condense_attributes",
    subtitle="Condenses selected log attributes into one destination "
             "attribute on OTAP output records.",
    config_fields=[
        ("destination_key", "String"),
        ("delimiter",       "char"),
        ("source_keys",     "Option<HashSet<String>>"),
        ("exclude_keys",    "Option<HashSet<String>>"),
    ],
    state=[
    ],
    effects=[
        "notify_nack",
        "send_message",
        "timed",
    ],
    role="processor",
    output_formats=["OTAP"],
    calldata=[
    ],
    control_msgs=["CollectTelemetry", "Config"],
    notes=[
        "Only log signals are transformed; other signal types fail with "
        "a Nack.",
        "OTLP inputs are converted to OTAP records before attributes are "
        "condensed.",
        "source_keys and exclude_keys are mutually exclusive; if neither "
        "is set, all attributes are condensed.",
        "Existing destination_key attributes are removed before the new "
        "condensed attribute is written.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_condense_attributes.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

