#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:transform`` (TransformProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/transform_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/transform_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="transform_processor",
    urn="urn:otel:processor:transform",
    subtitle="Runs a query-engine pipeline over each OTAP batch; "
             "the query is written in one of several supported "
             "expression languages.",
    config_fields=[
        ("kql_query",                            "String"),
        ("opl_query",                            "String"),
        ("inbound_request_limit",                "NonZeroUsize"),
        ("outbound_request_limit",               "NonZeroUsize"),
        ("skip_sanitize_result",                 "bool"),
        ("filter_attribute_keys_case_sensitive", "bool"),
    ],
    state=[
        "parsed query pipeline",
        "query-engine execution state",
        "inbound ack/nack slots",
        "outbound ack/nack slots",
        "split-off batch buffer (route_to)",
    ],
    effects=[
        "connected_ports",
        "notify_ack",
        "notify_nack",
        "send_message",
        "send_message_to",
        "subscribe_to",
    ],
    role="processor",
    output_formats=["OTAP"],
    named_outputs=["main", "errors"],
    calldata=[("outbound", "SlotKey")],
    control_msgs=["CollectTelemetry"],
    notes=[
        "Exactly one of kql_query or opl_query must be set; the "
        "leading source token (logs / traces / metrics / signal) "
        "selects which signal type the pipeline applies to.",
        "Inputs whose signal type does not match the query are "
        "passed through unchanged.",
        "OTLP inputs are converted to OTAP records before the "
        "query runs; outputs are emitted as OTAP only.",
        "A route_to operator splits the batch onto a named output "
        "port; an inbound request is not Ack'd until every "
        "split-off outbound batch is Ack'd.",
        "skip_sanitize_result keeps removed/redacted bytes in the "
        "underlying Arrow buffers — leave it false for "
        "security-driven transforms.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_transform.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
