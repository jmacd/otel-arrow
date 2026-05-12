#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:exporter:parquet`` (ParquetExporter).

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/exporters/parquet_exporter/mod.rs
rust/otap-dataflow/crates/core-nodes/src/exporters/parquet_exporter/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="parquet_exporter",
    urn="urn:otel:exporter:parquet",
    subtitle="Writes OTAP Arrow record batches as partitioned Parquet "
             "files to a configured object-store backend.",
    config_fields=[
        ("storage", "StorageType"),
        ("partitioning_strategies", "Option<Vec<PartitioningStrategy>>"),
        ("writer_options.target_rows_per_file", "Option<usize>"),
        ("writer_options.flush_when_older_than", "Option<Duration>"),
    ],
    state=[
        "per-partition Parquet writer",
        "in-flight file age tracker",
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
        "TimerTick",
    ],
    signal="otap",
    transport_pdata_label="object_store (file/s3/...)",
    transport_status_label="(write OK | error)",
    notes=[
        "storage abstracts over object_store backends: local file, S3, "
        "Azure Blob, GCS — controlled by base_uri scheme.",
        "Files are flushed when target_rows_per_file is reached, when "
        "flush_when_older_than expires (driven by TimerTick), or at "
        "Shutdown.",
        "PartitioningStrategy::SchemaMetadata extracts named keys from "
        "Arrow schema metadata to choose the destination partition.",
        "Per-batch result feeds notify_ack on a successful write and "
        "notify_nack on writer error; flush errors fail the exporter.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_parquet_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
