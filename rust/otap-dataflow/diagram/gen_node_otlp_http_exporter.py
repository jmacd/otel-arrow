#!/usr/bin/env python3
"""Per-node slide for ``urn:otel:exporter:otlp_http`` (OtlpHttpExporter).

Pure data: the layout/grammar lives in :mod:`node_lib`. To tweak the
visual design, edit ``node_lib.render_node_slide`` (and the geometry
constants near the top of that module); to tweak the slide's content,
edit the SPEC below.

Source of truth for the data:
rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_http_exporter/mod.rs
rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_http_exporter/config.rs
rust/otap-dataflow/crates/otap/src/otlp_http/client_settings.rs
"""

from __future__ import annotations
import sys

from node_lib import NodeSlideSpec, render_node_slide


SPEC = NodeSlideSpec(
    name="otlp_http_exporter",
    urn="urn:otel:exporter:otlp_http",
    subtitle="Sends OTLP/HTTP exports over a pooled hyper client with "
             "bounded in-flight concurrency.",
    config_fields=[
        ("http",                     "HttpClientSettings"),
        ("endpoint",                 "String"),
        ("logs_endpoint",            "Option<String>"),
        ("metrics_endpoint",         "Option<String>"),
        ("traces_endpoint",          "Option<String>"),
        ("max_response_body_length", "usize"),
        ("client_pool_size",         "NonZeroUsize"),
        ("max_in_flight",            "usize"),
    ],
    state=[
        "HTTP client pool",
        "in-flight export queue",
        "proto encoder + buffer",
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
    ],
    signal="otlp",
    transport_pdata_label="HTTP",
    transport_status_label="HTTP status",
    notes=[
        "max_in_flight bounds concurrent export requests; further "
        "pdata waits for a completion before being dispatched.",
        "client_pool_size opens multiple hyper clients for "
        "SO_REUSEPORT load balancing across receiver instances.",
        "Per-signal endpoints (logs / metrics / traces) override "
        "the base endpoint when set; otherwise /v1/<signal> is "
        "appended automatically.",
        "OTAP inputs are encoded to OTLP protobuf bytes; OTLP inputs "
        "are sent as-is when the context allows the body to be reused.",
        "max_response_body_length caps response body decoding; "
        "oversize responses fail the batch.",
        "2xx -> notify_ack with the saved payload (partial-success "
        "fields are recorded in metrics); non-2xx -> notify_nack.",
    ],
)


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "node_otlp_http_exporter.svg"
    svg = render_node_slide(SPEC)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print(f"wrote {out} ({len(svg)} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
