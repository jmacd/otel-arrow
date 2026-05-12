#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:retry`` (RetryProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/retry_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="retry_processor",
    urn="urn:otel:processor:retry",
    subtitle="Inline retry with exponential backoff.",
    config_fields=[
        ("initial_interval", "Duration"),
        ("max_interval",     "Duration"),
        ("max_elapsed_time", "Duration"),
        ("multiplier",       "f64"),
    ],
    state=[
        "local timer wheel",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "requeue_later",
        "send_message_with_source_node",
        "subscribe_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[
        ("retries",   "u64"),
        ("deadline",  "f64"),
        ("num_items", "u64"),
    ],
    control_msgs=["CollectTelemetry", "Config", "DelayedData"],
    notes=[
        "First interval begins after the first Nack.",
        "Each interval grows by the multiplier, capped at "
        "max_interval.",
        "Retries cease after max_elapsed_time.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_retry.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
