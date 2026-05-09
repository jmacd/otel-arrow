#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:otlp`` (OTLPReceiver).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/otlp_receiver/mod.rs
rust/otap-dataflow/crates/otap/src/otap_grpc/server_settings.rs
rust/otap-dataflow/crates/otap/src/otlp_http.rs
rust/otap-dataflow/crates/otap/src/otap_grpc/otlp/server_new.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="otlp_receiver",
    urn="urn:otel:receiver:otlp",
    subtitle="Ingests OTLP/gRPC and OTLP/HTTP exports as lazily-decoded "
             "OTLP byte payloads.",
    config_fields=[
        ("protocols.grpc", "Option<GrpcServerSettings>"),
        ("protocols.http", "Option<HttpServerSettings>"),
    ],
    state=[
        "ack subscription registry",
        "shared admission state",
    ],
    effects=[
        "notify_receiver_drained",
        "send_message_with_source_node",
        "subscribe_to",
        "tcp_listener",
    ],
    role="receiver",
    output_formats=["OTLP"],
    calldata=[
        ("slot", "SlotKey"),
    ],
    control_msgs=[
        "CollectTelemetry",
        "DrainIngress",
        "MemoryPressureChanged",
        "Shutdown",
    ],
    signal="otlp",
    transport_pdata_label="gRPC / HTTP",
    transport_status_label="gRPC / HTTP status",
    notes=[
        "At least one protocol (gRPC and/or HTTP) must be configured; "
        "default ports mirror the Go collector (4317 / 4318).",
        "Request bodies stay in serialized OTLP form via OtlpBytesCodec; "
        "downstream stages decode lazily.",
        "wait_for_result holds the response until the immediate "
        "downstream Acks/Nacks; otherwise responses return immediately.",
        "max_concurrent_requests defaults to 0 = adopt the downstream "
        "pdata channel capacity; non-zero values are clamped to it.",
        "MemoryPressureChanged feeds the shared admission state so "
        "ingress sheds load when the process is under pressure.",
        "DrainIngress closes both listeners and waits for in-flight "
        "wait_for_result requests to settle before notifying drained.",
        "TLS / mTLS are configured per protocol.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_otlp_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
