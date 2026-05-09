#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:log_sampling`` (LogSamplingProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/log_sampling_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/log_sampling_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="log_sampling_processor",
    urn="urn:otel:processor:log_sampling",
    subtitle="Reduces log volume with either fixed-ratio sampling or a per-window item budget.",
    config_fields=[
        ("policy", "Policy"),
    ],
    state=[
        "sampler policy state",
        "id bitmap pool",
        "filter buffer",
    ],
    effects=[
        "notify_ack",
        "notify_nack",
        "send_message_with_source_node",
        "start_periodic_timer",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[],
    control_msgs=["CollectTelemetry", "TimerTick"],
    notes=[
        "Only log signals are sampled; metrics and traces pass through unchanged.",
        "ratio keeps emit out_of each cycle and carries cycle position across batches.",
        "zip keeps at most max_items per interval and resets its count on TimerTick.",
        "Empty sampled results are Acked upstream instead of sending an empty batch.",
        "Filtered log outputs are emitted as OTAP Arrow records.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_log_sampling.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
