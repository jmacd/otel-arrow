#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:batch`` (BatchProcessor).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/batch_processor/mod.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="batch_processor",
    urn="urn:otel:processor:batch",
    subtitle="Coalesces inputs into size-bound batches with a "
             "max-age timer.",
    config_fields=[
        ("otap",                   "FormatConfig"),
        ("otlp",                   "FormatConfig"),
        ("max_batch_duration",     "Duration"),
        ("inbound_request_limit",  "NonZeroUsize"),
        ("outbound_request_limit", "NonZeroUsize"),
        ("format",                 "BatchingFormat"),
    ],
    state=[
        "pending batch buffers",
        "inbound ack/nack slots",
        "outbound ack/nack slots",
        "local scheduler wakeups",
    ],
    effects=[
        "cancel_wakeup",
        "notify_ack",
        "notify_nack",
        "report_local_scheduler_metrics",
        "send_message",
        "set_wakeup",
        "subscribe_to",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[("slot", "SlotKey")],
    control_msgs=["CollectTelemetry", "DelayedData", "Shutdown",
                  "Wakeup"],
    notes=[
        "Timer starts when the first input lands in an empty batch; "
        "flush fires after max_batch_duration.",
        "Flushes also fire when the active sizer reaches min_size, "
        "on shutdown, or on a delayed-data wakeup.",
        "Inputs that exhaust inbound or outbound slots are nacked; "
        "a flush proceeds even when some outbound slots could not "
        "be allocated.",
        "BatchingFormat::Preserve batches OTAP and OTLP "
        "independently; Otap/Otlp force a single output format.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_batch.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
