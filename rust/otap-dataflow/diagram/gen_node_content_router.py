#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:content_router`` (ContentRouter).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/content_router/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="content_router",
    urn="urn:otel:processor:content_router",
    subtitle="Routes each batch to one named output port using a resource attribute value.",
    config_fields=[
        ("routing_key",      "RoutingKeyExpr"),
        ("routes",           "HashMap<String, String>"),
        ("default_output",   "Option<String>"),
        ("case_sensitive",   "bool"),
        ("admission_policy", "SelectedRouteAdmissionPolicy"),
    ],
    state=[
        "parked route messages",
        "local scheduler wakeups",
    ],
    effects=[
        "cancel_wakeup",
        "connected_ports",
        "notify_nack",
        "processor_id",
        "set_wakeup",
        "try_admit_message_with_source_node_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    named_outputs=["frontend", "backend", "default_output"],
    control_msgs=["CollectTelemetry", "Shutdown", "Wakeup"],
    notes=[
        "All resources in a batch must resolve to the same destination; mixed-route batches are permanently nacked.",
        "Unmatched or missing routing keys use default_output when configured, otherwise the input is permanently nacked.",
        "The router emits the original pdata format; Arrow metrics and traces may be converted only for route inspection.",
        "backpressure admission can park one message per blocked output port and retry it from a local wakeup.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_content_router.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
