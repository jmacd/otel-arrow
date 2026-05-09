#!/usr/bin/env python3
"""Per-node slide for ``urn:microsoft:processor:recordset_kql`` (RecordsetKqlProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/contrib-nodes/src/processors/recordset_kql_processor/mod.rs
rust/otap-dataflow/crates/contrib-nodes/src/processors/recordset_kql_processor/config.rs
rust/otap-dataflow/crates/contrib-nodes/src/processors/recordset_kql_processor/processor.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="recordset_kql_processor",
    urn="urn:microsoft:processor:recordset_kql",
    subtitle="Runs a parsed RecordSet KQL pipeline over OTLP log "
             "records carried by OTAP pdata.",
    config_fields=[
        ("query",          "String"),
        ("bridge_options", "Option<serde_json::Value>"),
    ],
    state=[
        "parsed bridge pipeline",
    ],
    effects=[
        "notify_nack",
        "send_message",
        "timed",
    ],
    role="processor",
    output_formats=["OTLP"],
    calldata=[
    ],
    control_msgs=["CollectTelemetry", "Config"],
    notes=[
        "The query is parsed at startup and re-parsed on Config updates "
        "that change query or bridge_options.",
        "Inputs are converted to OTLP protobuf bytes before the KQL "
        "bridge runs.",
        "Only logs are implemented; metrics and traces return internal "
        "errors.",
        "Successful outputs are emitted as OTLP bytes in the original "
        "pdata context.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_recordset_kql.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

