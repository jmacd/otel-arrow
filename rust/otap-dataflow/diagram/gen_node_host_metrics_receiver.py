#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:receiver:host_metrics`` (HostMetricsReceiver).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/receivers/host_metrics_receiver/mod.rs
rust/otap-dataflow/crates/core-nodes/src/receivers/host_metrics_receiver/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="host_metrics_receiver",
    urn="urn:otel:receiver:host_metrics",
    subtitle="Periodically scrapes Linux /proc + /sys host metrics and "
             "emits OTAP record batches on a local schedule.",
    config_fields=[
        ("collection_interval", "Duration"),
        ("initial_delay", "Duration"),
        ("root_path", "Option<PathBuf>"),
        ("host_view", "HostViewConfig"),
        ("families", "FamiliesConfig"),
    ],
    state=[
        "host metrics lease",
        "family scheduler",
        "procfs source",
    ],
    effects=[
        "notify_receiver_drained",
        "start_periodic_telemetry",
        "try_send_message_with_source_node",
    ],
    role="receiver",
    output_formats=["OTAP"],
    control_msgs=[
        "CollectTelemetry",
        "DrainIngress",
        "Shutdown",
    ],
    signal="otap",
    transport_pdata_label="/proc + /sys",
    transport_status_label="(none)",
    notes=[
        "Linux only; the factory rejects the config on other platforms.",
        "Must run in a one-core source pipeline (host-wide collection); "
        "fan out via receiver:host_metrics -> exporter:topic and resume "
        "with multi-core processing downstream.",
        "FamilyScheduler picks the next due metric family; scrape_due "
        "returns a snapshot encoded directly to OTAP record batches.",
        "Backpressure on the downstream channel drops the current scrape "
        "(send_failures counter) instead of blocking the scheduler.",
        "host_view.validation chooses fail-startup vs warn-and-skip when "
        "selected procfs sources are unavailable at boot.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_host_metrics_receiver.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
