#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:syslog_cef`` (SyslogCefReceiver).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/syslog_cef_receiver/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="syslog_cef_receiver",
    urn="urn:otel:receiver:syslog_cef",
    subtitle="Listens on TCP or UDP for RFC 5424 syslog and ArcSight CEF "
             "messages; emits accumulated OTAP log batches downstream.",
    config_fields=[
        ("protocol", "Protocol (Tcp|Udp)"),
        ("protocol.tcp.listening_addr", "SocketAddr"),
        ("protocol.tcp.tls", "Option<TlsServerConfig>"),
        ("protocol.udp.listening_addr", "SocketAddr"),
        ("batch.max_batch_duration_ms", "Option<NonZeroU64>"),
        ("batch.max_size", "Option<NonZeroU16>"),
    ],
    state=[
        "Arrow records builder",
        "per-connection read buffers",
        "batch flush timer",
        "local admission state",
    ],
    effects=[
        "notify_receiver_drained",
        "send_message_with_source_node",
        "start_periodic_telemetry",
        "tcp_listener",
        "try_send_message_with_source_node",
        "udp_socket",
    ],
    role="receiver",
    output_formats=["OTAP"],
    control_msgs=[
        "CollectTelemetry",
        "DrainIngress",
        "MemoryPressureChanged",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="syslog / CEF (TCP|UDP)",
    transport_status_label="(connection close)",
    notes=[
        "Exactly one transport: protocol.tcp or protocol.udp (Syslog over "
        "TLS on TCP, RFC 5425, when tls is set).",
        "TCP path bounds each line to MAX_MESSAGE_SIZE; over-length lines "
        "are truncated rather than blocking the read loop.",
        "Records are coalesced into Arrow log batches; the batch flushes "
        "on max_size or max_batch_duration_ms (defaults: 100 records / "
        "100 ms).",
        "MemoryPressureChanged feeds a local admission state used to "
        "drop new datagrams under pressure.",
        "DrainIngress closes listeners and waits up to 1s for spawned "
        "TCP tasks to drain before notify_receiver_drained.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_syslog_cef_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
