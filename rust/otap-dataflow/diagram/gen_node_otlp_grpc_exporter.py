#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:exporter:otlp_grpc`` (OTLPExporter).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_grpc_exporter/mod.rs
rust/otap-dataflow/crates/otap/src/otap_grpc/client_settings.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="otlp_grpc_exporter",
    urn="urn:otel:exporter:otlp_grpc",
    subtitle="Sends OTLP/gRPC exports over a pooled tonic channel "
             "with bounded in-flight concurrency.",
    config_fields=[
        ("grpc",          "GrpcClientSettings"),
        ("max_in_flight", "usize"),
    ],
    state=[
        "gRPC channel + per-signal client pool",
        "in-flight export queue",
        "parked-message slot",
        "proto encoders + buffers",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "propagation_policy",
        "start_periodic_telemetry",
    ],
    role="exporter",
    output_formats=[],
    input_formats=["OTAP", "OTLP"],
    control_msgs=[
        "CollectTelemetry",
        "Shutdown",
    ],
    signal="otlp",
    transport_pdata_label="gRPC",
    transport_status_label="gRPC status",
    notes=[
        "max_in_flight bounds concurrent export RPCs; further pdata "
        "is parked until a completion frees a slot.",
        "OTAP inputs are encoded to OTLP protobuf bytes per signal "
        "before dispatch; OTLP inputs go straight to the wire.",
        "Successful RPC -> notify_ack with the saved payload; "
        "RPC error -> notify_nack with the error string.",
        "Transport headers from the pdata context are projected onto "
        "gRPC metadata via the configured propagation policy.",
        "Shutdown drains in-flight exports before the deadline before "
        "returning the terminal state.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_otlp_grpc_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
