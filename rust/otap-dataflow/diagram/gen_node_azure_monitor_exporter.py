#!/usr/bin/env python3
"""Per-node slide for ``urn:microsoft:exporter:azure_monitor``
(``AzureMonitorExporter``, contrib).

Source of truth for the data:
rust/otap-dataflow/crates/contrib-nodes/src/exporters/azure_monitor_exporter/mod.rs
rust/otap-dataflow/crates/contrib-nodes/src/exporters/azure_monitor_exporter/config.rs
rust/otap-dataflow/crates/contrib-nodes/src/exporters/azure_monitor_exporter/exporter.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="azure_monitor_exporter",
    urn="urn:microsoft:exporter:azure_monitor",
    subtitle="Sends OpenTelemetry logs to Azure Monitor via the Data "
             "Collection Rules (DCR) Logs Ingestion API; metrics and "
             "traces are dropped with a warning.",
    config_fields=[
        ("api.dcr_endpoint", "String"),
        ("api.dcr", "String"),
        ("api.stream_name", "String"),
        ("api.schema", "SchemaConfig"),
        ("api.gzip_compression_level", "u32"),
        ("auth.method", "ManagedIdentity | Development"),
        ("auth.client_id", "Option<String>"),
        ("auth.scope", "String"),
        ("heartbeat", "HeartbeatConfig"),
    ],
    state=[
        "gzip batcher",
        "logs ingestion client pool",
        "in-flight exports map",
        "msg/batch correlation tables",
        "heartbeat timer",
        "OAuth token + refresh schedule",
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
        "TimerTick",
    ],
    signal="otap",
    transport_pdata_label="HTTPS (DCR Ingestion API)",
    transport_status_label="HTTP status",
    notes=[
        "Logs only: OTAP metrics/traces and OTLP "
        "ExportMetricsRequest/ExportTracesRequest are dropped with a "
        "warning.",
        "Two log paths: OTapArrowRecords::Logs through OtapLogsView, or "
        "OtlpProtoBytes::ExportLogsRequest through RawLogsData.",
        "MAX_IN_FLIGHT_EXPORTS = 16 concurrent HTTP POSTs; backpressure "
        "is enforced by the in-flight slot pool.",
        "Auth: managed identity (system or user-assigned via client_id) "
        "or developer tools (Azure CLI). Token refresh is scheduled 295s "
        "before expiry, throttled to >=10s between attempts.",
        "Periodic 3s timer drives gzip-batch flush + heartbeat; per-msg "
        "ack/nack reflects the final HTTP outcome.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_azure_monitor_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
