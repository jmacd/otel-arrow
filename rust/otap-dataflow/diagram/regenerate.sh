#!/usr/bin/env bash
# regenerate.sh -- wipe and rebuild every SVG in ./out/.
#
# All slide generators are pure functions of their SPEC; this script is the
# single source of truth for *which* generators are wired up and where each
# one writes. Adding a new node slide is two lines below; renaming an output
# file is one edit here, no changes to the generator script. Run from any
# directory; the script cd's into its own location so the output path is
# stable.
#
# Usage:
#     ./regenerate.sh               # wipe out/*.svg and rebuild everything
#     ./regenerate.sh --no-clean    # rebuild without wiping (for fast iter)

set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")"

OUT_DIR="out"
mkdir -p "$OUT_DIR"

if [[ "${1:-}" != "--no-clean" ]]; then
    # Wipe stale outputs so a renamed slide can't leave its old name behind.
    rm -f "$OUT_DIR"/*.svg
fi

# --- generators ---------------------------------------------------------
# (script, output_filename) pairs. Output filenames are deliberately
# colocated here so renames are a one-line edit.
GENERATORS=(
    # Story slides (the original deck).
    "gen_diagram.py            experiments.svg"
    "gen_otlp_bytes.py         otlp_bytes.svg"
    "gen_otap_tables.py        otap_tables.svg"
    "gen_otap_pdata.py         otap_pdata.svg"

    # Engine architecture slides.
    "gen_engine_group.py       dataflow_engine.svg"
    "gen_engine_core.py        pipeline_engine.svg"
    "gen_memory_pressure.py    memory_pressure.svg"
    "gen_zero_copy.py          zero_copy.svg"

    # Per-node slides: receivers.
    "gen_node_otlp_receiver.py              node_otlp_receiver.svg"
    "gen_node_otap_receiver.py              node_otap_receiver.svg"
    "gen_node_host_metrics_receiver.py      node_host_metrics_receiver.svg"
    "gen_node_internal_telemetry_receiver.py node_internal_telemetry_receiver.svg"
    "gen_node_syslog_cef_receiver.py        node_syslog_cef_receiver.svg"
    "gen_node_topic_receiver.py             node_topic_receiver.svg"

    # Per-node slides: processors (core).
    "gen_node_attributes.py                 node_attributes.svg"
    "gen_node_batch.py                      node_batch.svg"
    "gen_node_content_router.py             node_content_router.svg"
    "gen_node_debug.py                      node_debug.svg"
    "gen_node_delay.py                      node_delay.svg"
    "gen_node_durable_buffer.py             node_durable_buffer.svg"
    "gen_node_fanout.py                     node_fanout.svg"
    "gen_node_filter.py                     node_filter.svg"
    "gen_node_log_sampling.py               node_log_sampling.svg"
    "gen_node_retry.py                      node_retry.svg"
    "gen_node_signal_type_router.py         node_signal_type_router.svg"
    "gen_node_temporal_reaggregation.py     node_temporal_reaggregation.svg"
    "gen_node_transform.py                  node_transform.svg"

    # Per-node slides: processors (contrib).
    "gen_node_condense_attributes.py        node_condense_attributes.svg"
    "gen_node_recordset_kql.py              node_recordset_kql.svg"
    "gen_node_resource_validator.py         node_resource_validator.svg"

    # Per-node slides: exporters (core).
    "gen_node_otlp_grpc_exporter.py         node_otlp_grpc_exporter.svg"
    "gen_node_otlp_http_exporter.py         node_otlp_http_exporter.svg"
    "gen_node_otap_exporter.py              node_otap_exporter.svg"
    "gen_node_parquet_exporter.py           node_parquet_exporter.svg"
    "gen_node_topic_exporter.py             node_topic_exporter.svg"

    # Per-node slides: exporters (contrib).
    "gen_node_azure_monitor_exporter.py     node_azure_monitor_exporter.svg"
    "gen_node_geneva_exporter.py            node_geneva_exporter.svg"
)

fail=0
for entry in "${GENERATORS[@]}"; do
    # shellcheck disable=SC2086
    set -- $entry
    script="$1"
    out_name="$2"
    if [[ ! -x "$script" && ! -f "$script" ]]; then
        echo "MISSING $script" >&2
        fail=1
        continue
    fi
    if python3 "$script" "$OUT_DIR/$out_name" >/dev/null; then
        printf '  OK  %s\n' "$out_name"
    else
        printf '  FAIL %s\n' "$out_name" >&2
        fail=1
    fi
done

# Sanity-check: every Python generator was listed above. A new gen_*.py
# without a matching row here would silently never run.
unlisted=()
while IFS= read -r script; do
    base="$(basename "$script")"
    if ! printf '%s\n' "${GENERATORS[@]}" | grep -q "^$base "; then
        unlisted+=("$base")
    fi
done < <(ls gen_*.py 2>/dev/null)

if (( ${#unlisted[@]} > 0 )); then
    echo >&2
    echo "Unlisted generators (add to GENERATORS in regenerate.sh):" >&2
    printf '  %s\n' "${unlisted[@]}" >&2
    fail=1
fi

if (( fail != 0 )); then
    exit 1
fi

count=$(ls "$OUT_DIR"/*.svg 2>/dev/null | wc -l)
echo "wrote $count SVGs to $OUT_DIR/"
