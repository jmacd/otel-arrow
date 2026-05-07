#!/usr/bin/env bash
# Bench harness for the obsday logging-overhead experiment (Stack A).
# Unlike run.sh, this does NOT invoke samply; it wraps each binary in
# /usr/bin/time -v to capture CPU time, peak RSS, and wall time.
#
# Usage: tools/obsday/bench.sh --name LABEL [--rate N] [--duration S] \
#          [--workers N] [--attrs N] \
#          [--seed N] [--outdir DIR]

set -euo pipefail
cd "$(dirname "$0")/../.."

NAME=""
RATE=10000
DURATION=60
WORKERS=2
ATTRS=8
SEED=1
OUTDIR="./obsday-out/bench"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)     NAME="$2";     shift 2 ;;
    --rate)     RATE="$2";     shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --workers)  WORKERS="$2";  shift 2 ;;
    --attrs)    ATTRS="$2";    shift 2 ;;
    --seed)     SEED="$2";     shift 2 ;;
    --outdir)   OUTDIR="$2";   shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ -z "$NAME" ]] && { echo "--name required" >&2; exit 2; }
RUNDIR="$OUTDIR/$NAME"
mkdir -p "$RUNDIR"

DF_ENGINE="./target/profiling/df_engine"
LOGGER="./target/profiling/examples/obsday_logger"

if [[ ! -x "$DF_ENGINE" || ! -x "$LOGGER" ]]; then
  echo "[bench] building..."
  cargo build --profile profiling --bin df_engine --example obsday_logger >/dev/null
fi

for port in 4317 9876 9877; do
  if ss -ltn "( sport = :$port )" 2>/dev/null | tail -n +2 | grep -q .; then
    echo "[bench] ERROR: port $port in use" >&2; exit 3
  fi
done

COL_TIME="$RUNDIR/collector.time"
COL_LOG="$RUNDIR/collector.log"
LOG_TIME="$RUNDIR/logger.time"
LOG_LOG="$RUNDIR/logger.log"

echo "[bench:$NAME] starting collector..."
/usr/bin/time -v -o "$COL_TIME" \
  "$DF_ENGINE" --config configs/obsday-collector.yaml \
  >"$COL_LOG" 2>&1 &
COLLECTOR_PID=$!

cleanup() {
  # COLLECTOR_PID is /usr/bin/time; df_engine is its child. /usr/bin/time
  # does not always forward signals, so kill the child first.
  local CHILD
  CHILD=$(pgrep -P "$COLLECTOR_PID" 2>/dev/null | head -n 1 || true)
  curl -fsS -X POST \
    "http://127.0.0.1:9877/api/v1/groups/shutdown?wait=true&timeout_secs=15" \
    >/dev/null 2>&1 || true
  sleep 0.5
  if [[ -n "$CHILD" ]]; then
    kill "$CHILD" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "$CHILD" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$CHILD" 2>/dev/null || true
  fi
  kill "$COLLECTOR_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true
  for _ in $(seq 1 30); do
    ss -ltn "( sport = :4317 )" 2>/dev/null | tail -n +2 | grep -q . || break
    sleep 0.1
  done
}
trap cleanup EXIT

for _ in $(seq 1 100); do
  ss -ltn "( sport = :4317 )" 2>/dev/null | tail -n +2 | grep -q . \
    && ss -ltn "( sport = :9877 )" 2>/dev/null | tail -n +2 | grep -q . \
    && break
  sleep 0.1
done

echo "[bench:$NAME] starting logger (rate=$RATE dur=${DURATION}s attrs=$ATTRS)..."
/usr/bin/time -v -o "$LOG_TIME" \
  "$LOGGER" --config configs/obsday-logger.yaml \
       --rate "$RATE" --duration "$DURATION" --workers "$WORKERS" \
       --attrs "$ATTRS"  \
        --seed "$SEED" \
  >"$LOG_LOG" 2>&1

cleanup
trap - EXIT

echo "[bench:$NAME] done -> $RUNDIR"
