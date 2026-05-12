#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:resource_validator`` (ResourceValidatorProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/contrib-nodes/src/processors/resource_validator_processor/mod.rs
rust/otap-dataflow/crates/contrib-nodes/src/processors/resource_validator_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="resource_validator_processor",
    urn="urn:otel:processor:resource_validator",
    subtitle="Validates required resource attributes and permanently "
             "Nacks data that fails policy.",
    config_fields=[
        ("required_attribute_key", "String"),
        ("allowed_values",         "Vec<String>"),
        ("case_sensitive",         "bool"),
    ],
    state=[
    ],
    effects=[
        "notify_nack",
        "send_message",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[
    ],
    control_msgs=["CollectTelemetry"],
    notes=[
        "Each resource must contain required_attribute_key with a string "
        "value.",
        "The value must match allowed_values; case_sensitive only affects "
        "value matching.",
        "Validation failures produce permanent Nacks instead of forwarding "
        "the data.",
        "OTAP log records are validated directly; OTAP metrics and traces "
        "are converted to OTLP for validation.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_resource_validator.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

