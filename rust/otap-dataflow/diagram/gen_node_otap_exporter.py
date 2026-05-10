#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:exporter:otap`` (OTAPExporter).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/exporters/otap_exporter/mod.rs
rust/otap-dataflow/crates/core-nodes/src/exporters/otap_exporter/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="otap_exporter",
    urn="urn:otel:exporter:otap",
    subtitle="Streams OTAP record batches over per-signal Arrow gRPC "
             "streams (logs / metrics / traces) with response-correlated "
             "ack/nack.",
    config_fields=[
        ("grpc", "GrpcClientSettings"),
        ("compression_method", "Option<CompressionMethod>"),
        ("arrow", "ArrowConfig"),
        ("stream_queue_capacity", "usize"),
        ("streams_per_signal", "usize"),
        ("timeout", "Option<Duration>"),
    ],
    state=[
        "per-signal stream tasks",
        "stream queue (per-signal channel)",
        "in-flight response correlation map",
        "export latency window",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "start_periodic_telemetry",
    ],
    role="exporter",
    output_formats=[],
    input_formats=["OTAP"],
    control_msgs=[
        "CollectTelemetry",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="OTAP / gRPC",
    transport_status_label="BatchStatus / gRPC",
    notes=[
        "Each signal runs streams_per_signal independent Arrow gRPC "
        "streams (default 1) with stream_queue_capacity-bound enqueue.",
        "Server BatchStatus responses are matched back to the originating "
        "OtapPdata via a correlation queue; OK -> notify_ack, anything "
        "else -> notify_nack.",
        "arrow.compression independently zstd-compresses Arrow IPC frames "
        "in addition to gRPC compression (double compression by default).",
        "OTLP-only inputs are rejected (notify_nack 'payload conversion "
        "failed'); use otap_exporter only on the OTAP side of an ITR.",
        "Latency window publishes p50/p90/p99 export.rpc.duration to the "
        "self-telemetry async metrics.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_otap_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
