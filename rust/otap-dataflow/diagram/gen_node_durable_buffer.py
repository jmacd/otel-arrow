#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:processor:durable_buffer`` (DurableBuffer).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/processors/durable_buffer_processor/mod.rs
rust/otap-dataflow/crates/core-nodes/src/processors/durable_buffer_processor/config.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="durable_buffer_processor",
    urn="urn:otel:processor:durable_buffer",
    subtitle="Persists pdata to per-core durable storage before forwarding finalized bundles downstream.",
    config_fields=[
        ("path",                      "PathBuf"),
        ("retention_size_cap",        "Byte"),
        ("max_age",                   "Option<Duration>"),
        ("size_cap_policy",           "SizeCapPolicy"),
        ("poll_interval",             "Duration"),
        ("otlp_handling",             "OtlpHandling"),
        ("max_segment_open_duration", "Duration"),
        ("initial_retry_interval",    "Duration"),
        ("max_retry_interval",        "Duration"),
        ("retry_multiplier",          "f64"),
        ("max_in_flight",             "usize"),
    ],
    state=[
        "quiver engine state",
        "in-flight bundle map",
        "deferred retry queue",
        "segment metrics cache",
        "warning rate-limit timestamps",
        "local scheduler wakeups",
    ],
    effects=[
        "cancel_wakeup",
        "notify_ack",
        "notify_nack",
        "set_wakeup",
        "start_periodic_timer",
        "subscribe_to",
        "try_send_message",
    ],
    role="processor",
    output_formats=["OTAP", "OTLP"],
    calldata=[
        ("segment_seq",   "u64"),
        ("bundle_index",  "u32"),
    ],
    control_msgs=[
        "CollectTelemetry",
        "Config",
        "DelayedData",
        "Shutdown",
        "TimerTick",
        "Wakeup",
    ],
    notes=[
        "Incoming pdata is acked only after a successful durable write to Quiver storage.",
        "Timer ticks flush old open segments, poll finalized bundles, and run storage maintenance.",
        "Transient downstream NACKs are retried with exponential backoff and jitter; permanent NACKs reject the bundle.",
        "OTLP pass-through preserves OTLP bytes, while convert_to_arrow stores and emits OTAP records.",
        "Each pipeline core uses a separate storage directory and a share of the configured retention size cap.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_durable_buffer.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
