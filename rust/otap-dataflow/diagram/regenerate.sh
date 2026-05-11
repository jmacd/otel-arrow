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
    "gen_engine_group.py       01-dataflow-engine.svg"
    "gen_engine_core.py        02-pipeline-engine.svg"
    "gen_otap_pdata.py         03-otap-pdata.svg"
    "gen_otlp_bytes.py         04-otlp-bytes.svg"
    "gen_otap_tables.py        05-otap-tables.svg"
    "gen_otap_pdata.py         06-otap-pdata.svg"
    "gen_engine_core.py        07-pipeline-engine.svg"
    "gen_zero_copy.py          08-zero-copy.svg"
    "gen_memory_pressure.py    09-memory-pressure.svg"
    "gen_roadmap.py            09b-roadmap.svg"

    "gen_node_otlp_receiver.py               10-node-otlp-receiver.svg"
    "gen_node_syslog_cef_receiver.py         11-node-syslog-cef-receiver.svg"
    "gen_node_batch.py                       12-node-batch.svg"
    "gen_node_durable_buffer.py              13-node_durable_buffer.svg"
    "gen_node_fanout.py                      14-node_fanout.svg"
    "gen_node_transform.py                   15-node-transform.svg"
    "gen_node_attributes.py                  16-node-attributes.svg"
    "gen_node_filter.py                      17-node-filter.svg"
    "gen_node_azure_monitor_exporter.py      18-node_azure_monitor_exporter.svg"
    "gen_node_otap_exporter.py               19-node_otap_exporter.svg"
    "gen_node_parquet_exporter.py            20-node_parquet_exporter.svg"
    "gen_node_geneva_exporter.py             21-node_geneva_exporter.svg"
    "gen_node_topic_exporter.py              22-node_topic_exporter.svg"
    "gen_node_topic_receiver.py              23-node_topic_receiver.svg"
    "gen_node_internal_telemetry_receiver.py 24-node_internal_telemetry_receiver.svg"
    "gen_node_host_metrics_receiver.py       25-node_host_metrics_receiver.svg"
    "gen_node_otap_receiver.py               26-node_otap_receiver.svg"
    "gen_node_otlp_grpc_exporter.py          27-node_otlp_grpc_exporter.svg"
    "gen_node_otlp_http_exporter.py          28-node_otlp_http_exporter.svg"
    "gen_node_content_router.py              29-node_content_router.svg"
    "gen_node_log_sampling.py                30-node_log_sampling.svg"
    "gen_node_retry.py                       31-node_retry.svg"
    "gen_node_signal_type_router.py          32-node_signal_type_router.svg"
    "gen_node_temporal_reaggregation.py      33-node_temporal_reaggregation.svg"
    "gen_node_condense_attributes.py         34-node_condense_attributes.svg"
    "gen_node_recordset_kql.py               35-node_recordset_kql.svg"
    "gen_node_resource_validator.py          36-node_resource_validator.svg"
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
# unlisted=()
# while IFS= read -r script; do
#     base="$(basename "$script")"
#     if ! printf '%s\n' "${GENERATORS[@]}" | grep -q "^$base "; then
#         unlisted+=("$base")
#     fi
# done < <(ls gen_*.py 2>/dev/null)

# if (( ${#unlisted[@]} > 0 )); then
#     echo >&2
#     echo "Unlisted generators (add to GENERATORS in regenerate.sh):" >&2
#     printf '  %s\n' "${unlisted[@]}" >&2
#     fail=1
# fi

# if (( fail != 0 )); then
#     exit 1
# fi

count=$(ls "$OUT_DIR"/*.svg 2>/dev/null | wc -l)
echo "wrote $count SVGs to $OUT_DIR/"
