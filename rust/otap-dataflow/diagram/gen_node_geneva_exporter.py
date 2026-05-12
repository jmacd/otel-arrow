#!/usr/bin/env python3
"""Per-node slide for ``urn:microsoft:exporter:geneva`` (GenevaExporter,
contrib).

Source of truth for the data:
rust/otap-dataflow/crates/contrib-nodes/src/exporters/geneva_exporter/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="geneva_exporter",
    urn="urn:microsoft:exporter:geneva",
    subtitle="Encodes and uploads OpenTelemetry logs and traces to "
             "Microsoft Geneva via the geneva-uploader client; metrics "
             "are not yet supported.",
    config_fields=[
        ("endpoint", "String"),
        ("environment", "String"),
        ("account", "String"),
        ("namespace", "String"),
        ("region", "String"),
        ("config_major_version", "u32"),
        ("tenant", "String"),
        ("role_name", "String"),
        ("role_instance", "String"),
        ("auth", "AuthConfig"),
        ("max_buffer_size", "usize"),
        ("max_concurrent_uploads", "usize"),
    ],
    state=[
        "geneva-uploader client",
        "concurrent upload futures",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "start_periodic_telemetry",
    ],
    role="exporter",
    output_formats=[],
    input_formats=["OTAP", "OTLP"],
    control_msgs=[
        "CollectTelemetry",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="HTTPS (Geneva ingestion)",
    transport_status_label="HTTP status",
    notes=[
        "Logs and traces only; metrics produce no upload (no encoder).",
        "Logs use a zero-copy fast path: OtapArrowRecords::Logs is "
        "encoded directly via OtapLogsView through encode_and_compress_"
        "logs. OTLP log bytes use RawLogsData over the same path.",
        "Traces fall back to the OTLP-bytes path: OTAP traces are "
        "transcoded to OTLP protobuf, then uploaded.",
        "Auth supports certificate (PKCS#12), system / user / "
        "ARM-resource-id managed identity, and workload identity.",
        "max_concurrent_uploads bounds in-flight uploads via a "
        "FuturesUnordered set; per-batch outcome resolves to ack or "
        "nack on the originating pdata.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_geneva_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
