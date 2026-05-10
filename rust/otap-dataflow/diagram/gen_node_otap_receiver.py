#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:otap`` (OTAPReceiver).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/otap_receiver/mod.rs
rust/otap-dataflow/crates/otap/src/otap_grpc/mod.rs
rust/otap-dataflow/crates/otap/src/otap_grpc/otlp/server_new.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="otap_receiver",
    urn="urn:otel:receiver:otap",
    subtitle="Ingests OTel-Arrow gRPC streams (logs / metrics / traces) "
             "and emits decoded OTAP record batches.",
    config_fields=[
        ("listening_addr", "SocketAddr"),
        ("compression_method", "Option<CompressionMethod>"),
        ("response_stream_channel_size", "usize"),
        ("max_concurrent_requests", "usize"),
        ("max_concurrent_requests_per_stream", "NonZeroUsize"),
        ("wait_for_result", "bool"),
        ("timeout", "Option<Duration>"),
        ("tls", "Option<TlsServerConfig>"),
    ],
    state=[
        "ack subscription registry",
        "shared admission state",
        "memory-pressure rejection counters",
    ],
    effects=[
        "notify_receiver_drained",
        "send_message_with_source_node",
        "start_periodic_telemetry",
        "subscribe_to",
        "tcp_listener",
    ],
    role="receiver",
    output_formats=["OTAP"],
    shared=True,
    foreign_entities=[
        ("Tonic gRPC server", "Arrow services; Send handler clones."),
        ("ZstdRequestHeaderAdapter", "Decodes zstd request headers."),
    ],
    calldata=[
        ("slot", "SlotKey"),
    ],
    control_msgs=[
        "CollectTelemetry",
        "DrainIngress",
        "MemoryPressureChanged",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="gRPC",
    transport_status_label="gRPC status",
    notes=[
        "OTAP-only protocol: serves the three Arrow service variants "
        "(logs / metrics / traces) over a single gRPC endpoint; "
        "default port mirrors the OTel-Arrow Go reference (4317 by "
        "convention).",
        "Streams are zero-copy where possible; record batches arrive "
        "as Arrow IPC frames and are wrapped into OtapPdata without "
        "re-encoding.",
        "wait_for_result holds the per-message response on the gRPC "
        "stream until the immediate downstream Acks/Nacks; the "
        "default is false (matches the Go collector default).",
        "max_concurrent_requests is enforced by a shared semaphore at "
        "the Tower poll_ready layer; per-stream concurrency is "
        "additionally clamped by max_concurrent_requests_per_stream.",
        "MemoryPressureChanged feeds the shared admission state so "
        "ingress sheds load when the process is under pressure; "
        "rejected requests are counted in receiver metrics.",
        "DrainIngress cancels the gRPC listener and lets in-flight "
        "wait_for_result streams settle before notify_receiver_drained.",
        "TLS / mTLS are configured via the optional tls block; "
        "handshake_timeout is honored on a per-connection basis.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_otap_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
