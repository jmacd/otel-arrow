#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:internal_telemetry``
(``InternalTelemetryReceiver``).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/internal_telemetry_receiver/mod.rs
rust/otap-dataflow/crates/telemetry/src/lib.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="internal_telemetry_receiver",
    urn="urn:otel:receiver:internal_telemetry",
    subtitle="Drains the in-process logging channel and emits each event as "
             "an OTLP ExportLogsRequest payload.",
    config_fields=[],
    state=[
        "logs receiver channel",
        "scope-to-bytes cache",
        "pre-encoded resource bytes",
    ],
    effects=[
        "notify_receiver_drained",
        "send_message",
        "start_periodic_telemetry",
    ],
    role="receiver",
    output_formats=["OTLP"],
    control_msgs=[
        "DrainIngress",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="logs channel",
    transport_status_label="(none)",
    notes=[
        "Requires the pipeline context to expose internal telemetry "
        "settings (logs_receiver, resource_bytes, registry); the factory "
        "errors out otherwise. Used in Internal Telemetry Service (ITS) "
        "mode of metrics provider.",
        "Each ObservedEvent::Log is encoded into an OTLP "
        "ExportLogsRequest via encode_export_logs_request and sent "
        "downstream. Engine events are not forwarded today.",
        "Resource attributes are pre-encoded once at startup; per-event "
        "scope encodings are memoized in a ScopeToBytesMap.",
        "DrainIngress drains the channel synchronously, then notifies "
        "the engine that ingress is drained.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_internal_telemetry_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
