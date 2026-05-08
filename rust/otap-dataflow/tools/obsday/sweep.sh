#!/usr/bin/env bash
# obsday sweep harness: runs measure.sh over a (config, workers, rate) matrix
# and produces a single combined report (sweep_report.md) with one summary
# table plus the per-run top-20 CPU listings appended.
#
# Usage:
#   tools/obsday/sweep.sh [--outdir DIR] [--duration S] [--num-cores N]
#                         [--matrix FILE]
#
# Matrix file format (one run per line, blank/# lines skipped):
#   NAME  CONFIG  WORKERS  RATE
# Example:
#   otlp_w1_10k    configs/obsday-logger.yaml       1   10000
#   otlp_w2_10k    configs/obsday-logger.yaml       2   10000
#   otlp_w4_10k    configs/obsday-logger.yaml       4   10000
#   otap_w1_10k    configs/obsday-otap-logger.yaml  1   10000
#   otap_w2_10k    configs/obsday-otap-logger.yaml  2   10000
#   otap_w4_10k    configs/obsday-otap-logger.yaml  4   10000

set -euo pipefail
cd "$(dirname "$0")/../.."

OUTDIR="./obsday-out/sweep"
DURATION=60
NUM_CORES=1
MATRIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir)     OUTDIR="$2";     shift 2 ;;
    --duration)   DURATION="$2";   shift 2 ;;
    --num-cores)  NUM_CORES="$2";  shift 2 ;;
    --matrix)     MATRIX="$2";     shift 2 ;;
    -h|--help)    sed -n '1,20p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$MATRIX" ]]; then
  echo "[sweep] ERROR: --matrix required" >&2
  exit 2
fi
if [[ ! -f "$MATRIX" ]]; then
  echo "[sweep] ERROR: matrix file '$MATRIX' not found" >&2
  exit 2
fi

mkdir -p "$OUTDIR"

# Collect runs in declared order. Optional 5th column = collector config.
runs=()
while read -r name config workers rate collector _rest; do
  [[ -z "$name" || "$name" =~ ^# ]] && continue
  collector="${collector:-configs/obsday-collector.yaml}"
  runs+=("$name|$config|$workers|$rate|$collector")
done < "$MATRIX"

echo "[sweep] ${#runs[@]} runs into $OUTDIR (duration=${DURATION}s, num_cores=$NUM_CORES)"
echo

i=0
for entry in "${runs[@]}"; do
  i=$((i+1))
  IFS='|' read -r name config workers rate collector <<<"$entry"
  echo "==== [$i/${#runs[@]}] $name (config=$config collector=$collector workers=$workers rate=$rate) ===="
  rm -rf "$OUTDIR/$name"
  bash tools/obsday/measure.sh \
      --name "$name" \
      --rate "$rate" \
      --duration "$DURATION" \
      --workers "$workers" \
      --num-cores "$NUM_CORES" \
      --logger-config "$config" \
      --collector-config "$collector" \
      --outdir "$OUTDIR" \
    >"$OUTDIR/${name}.measure.log" 2>&1 || {
      echo "[sweep] WARN: measure.sh failed for $name (see $OUTDIR/${name}.measure.log)"
      continue
    }
  echo "[sweep:$name] done"
done

# Aggregate.
echo
echo "[sweep] aggregating -> $OUTDIR/sweep_report.md"
python3 tools/obsday/report_sweep.py "$OUTDIR" "$MATRIX" "$DURATION" "$NUM_CORES" \
  > "$OUTDIR/sweep_report.md"
echo "[sweep] done. Summary:"
sed -n '1,80p' "$OUTDIR/sweep_report.md"
