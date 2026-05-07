#!/usr/bin/env bash
# One-shot: record obsday logger+collector under samply, render flamegraph
# SVGs (all-threads / pipeline / workers), and open one in the browser.
#
# Usage:
#   tools/obsday/flame.sh [--rate N] [--duration S] [--workers N] \
#                         [--attrs N] \
#                         [--seed N] [--outdir DIR] [--thread NAME] \
#                         [--no-open]
#
# --thread selects which flamegraph to open in the browser. Choices:
#   all          (default)
#   pipeline     (the engine pipeline-system thread)
#   workers      (the obsday-w* worker threads emitting otel_info!)
#
# Requires: samply, inferno-flamegraph (cargo install samply inferno).

set -euo pipefail
cd "$(dirname "$0")/../.."

RATE=50000
DURATION=30
WORKERS=2
ATTRS=8
SEED=1
OUTDIR="./obsday-out"
THREAD="all"
OPEN=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate)     RATE="$2";     shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --workers)  WORKERS="$2";  shift 2 ;;
    --attrs)    ATTRS="$2";    shift 2 ;;
    --seed)     SEED="$2";     shift 2 ;;
    --outdir)   OUTDIR="$2";   shift 2 ;;
    --thread)   THREAD="$2";   shift 2 ;;
    --no-open)  OPEN=0;        shift ;;
    -h|--help)  sed -n '1,20p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

case "$THREAD" in all|pipeline|workers) ;; *)
  echo "--thread must be one of: all, pipeline, workers" >&2; exit 2 ;;
esac

export PATH="$HOME/.cargo/bin:$PATH"
for tool in samply inferno-flamegraph; do
  command -v "$tool" >/dev/null || { echo "missing $tool (cargo install ...)" >&2; exit 3; }
done

# 1. record
RUN_LOG="$OUTDIR/flame.run.log"
mkdir -p "$OUTDIR"
if ! bash tools/obsday/run.sh \
       --rate "$RATE" --duration "$DURATION" --workers "$WORKERS" \
       --attrs "$ATTRS"    \
       --seed "$SEED" --outdir "$OUTDIR" >"$RUN_LOG" 2>&1; then
  echo "[flame] ERROR: record step failed. Last lines of run.sh output:" >&2
  tail -n 30 "$RUN_LOG" | sed 's/^/    /' >&2
  echo "" >&2
  echo "[flame] Last lines of logger.log:" >&2
  tail -n 20 "$OUTDIR/logger.log" 2>/dev/null | sed 's/^/    /' >&2
  echo "" >&2
  echo "[flame] Last lines of collector.log:" >&2
  tail -n 20 "$OUTDIR/collector.log" 2>/dev/null | sed 's/^/    /' >&2
  exit 4
fi

PROF="$OUTDIR/logger.profile.json.gz"
SYMS="$OUTDIR/logger.profile.json.syms.json"

if [[ ! -s "$PROF" || ! -s "$SYMS" ]]; then
  echo "[flame] ERROR: expected profile artefacts not found:" >&2
  echo "    $PROF" >&2
  echo "    $SYMS" >&2
  echo "[flame] Last lines of logger.log:" >&2
  tail -n 20 "$OUTDIR/logger.log" 2>/dev/null | sed 's/^/    /' >&2
  exit 5
fi

# The logger may have exited early (e.g. invalid config). run.sh does not
# propagate its exit code, so detect this by looking at logger.log for an
# error marker AND for the expected "emission done" line.
if ! grep -q "emission done:" "$OUTDIR/logger.log" 2>/dev/null; then
  echo "[flame] ERROR: logger never reported 'emission done'." >&2
  if grep -E "^(Error|panicked|thread .* panicked)" "$OUTDIR/logger.log" \
       2>/dev/null >&2; then
    :  # error printed above
  fi
  echo "[flame] Last lines of logger.log:" >&2
  tail -n 20 "$OUTDIR/logger.log" 2>/dev/null | sed 's/^/    /' >&2
  exit 7
fi

# 2. fold + render the three views
TITLE="obsday logger ${RATE}/s attrs=${ATTRS} dur=${DURATION}s"
python3 tools/obsday/fold_samply.py "$PROF" "$SYMS" \
  > "$OUTDIR/logger.folded"
python3 tools/obsday/fold_samply.py "$PROF" "$SYMS" \
  --thread-substr pipeline-system > "$OUTDIR/logger.pipeline.folded"
python3 tools/obsday/fold_samply.py "$PROF" "$SYMS" \
  --thread-substr obsday-w > "$OUTDIR/logger.workers.folded"

# Sanity-check folded outputs. inferno-flamegraph emits an unhelpful
# "No stack counts found" error otherwise.
check_folded() {
  local label="$1" path="$2"
  if [[ ! -s "$path" ]]; then
    echo "[flame] ERROR: no stacks captured for $label ($path is empty)." >&2
    echo "[flame] The process may have exited before sampling started, or the" >&2
    echo "[flame] thread filter matched nothing. logger.log tail:" >&2
    tail -n 20 "$OUTDIR/logger.log" 2>/dev/null | sed 's/^/    /' >&2
    return 1
  fi
}
check_folded "all threads"       "$OUTDIR/logger.folded"          || exit 6
# Per-thread filters may legitimately be empty for very short runs;
# warn rather than fail so we still render the all-threads view.
[[ -s "$OUTDIR/logger.pipeline.folded" ]] || \
  echo "[flame] WARN: no samples on pipeline-system thread" >&2
[[ -s "$OUTDIR/logger.workers.folded" ]] || \
  echo "[flame] WARN: no samples on worker threads" >&2

inferno-flamegraph --title "$TITLE -- ALL threads" \
  "$OUTDIR/logger.folded" > "$OUTDIR/logger.flamegraph.svg"
if [[ -s "$OUTDIR/logger.pipeline.folded" ]]; then
  inferno-flamegraph --title "$TITLE -- pipeline-system thread" \
    "$OUTDIR/logger.pipeline.folded" > "$OUTDIR/logger.pipeline.flamegraph.svg"
fi
if [[ -s "$OUTDIR/logger.workers.folded" ]]; then
  inferno-flamegraph --title "$TITLE -- worker threads (otel_info! emit)" \
    --colors aqua \
    "$OUTDIR/logger.workers.folded" > "$OUTDIR/logger.workers.flamegraph.svg"
fi

case "$THREAD" in
  all)      SVG="$OUTDIR/logger.flamegraph.svg" ;;
  pipeline) SVG="$OUTDIR/logger.pipeline.flamegraph.svg" ;;
  workers)  SVG="$OUTDIR/logger.workers.flamegraph.svg" ;;
esac

echo "[flame] artefacts:"
echo "  $OUTDIR/logger.flamegraph.svg          (all threads)"
echo "  $OUTDIR/logger.pipeline.flamegraph.svg (pipeline-system)"
echo "  $OUTDIR/logger.workers.flamegraph.svg  (workers)"

if [[ "$OPEN" -eq 1 ]]; then
  if command -v xdg-open >/dev/null; then
    xdg-open "$SVG" >/dev/null 2>&1 &
  elif [[ -x /mnt/c/Windows/explorer.exe ]] && command -v wslpath >/dev/null; then
    /mnt/c/Windows/explorer.exe "$(wslpath -w "$SVG")" >/dev/null 2>&1 || true
  else
    echo "[flame] no browser opener found; open $SVG manually" >&2
  fi
fi
