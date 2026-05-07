#!/usr/bin/env bash
# Profiling harness for the obsday logging-overhead experiment (Stack A:
# otap-dataflow). Builds release-with-debuginfo binaries, starts the
# collector under samply, then starts the logger under samply, waits for
# the logger to finish, and stops the collector.
#
# Usage:
#   tools/obsday/run.sh [--rate N] [--duration S] [--workers N] \
#                       [--attrs N] [--mean F] [--stddev F] [--min N] \
#                       [--seed N] [--outdir DIR]
#
# All flags have defaults. Profiles land in $OUTDIR (default: ./obsday-out).

set -euo pipefail

cd "$(dirname "$0")/../.."

RATE=10000
DURATION=30
WORKERS=2
ATTRS=8
MEAN=24
STDDEV=8
MIN=1
SEED=1
OUTDIR="./obsday-out"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rate)     RATE="$2";     shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --workers)  WORKERS="$2";  shift 2 ;;
    --attrs)    ATTRS="$2";    shift 2 ;;
    --mean)     MEAN="$2";     shift 2 ;;
    --stddev)   STDDEV="$2";   shift 2 ;;
    --min)      MIN="$2";      shift 2 ;;
    --seed)     SEED="$2";     shift 2 ;;
    --outdir)   OUTDIR="$2";   shift 2 ;;
    -h|--help)  sed -n '1,15p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

mkdir -p "$OUTDIR"

echo "[obsday] building (profile=profiling)..."
cargo build --profile profiling --bin df_engine --example obsday_logger >/dev/null

DF_ENGINE="./target/profiling/df_engine"
LOGGER="./target/profiling/examples/obsday_logger"

for port in 4317 9876 9877; do
  if ss -ltn "( sport = :$port )" 2>/dev/null | tail -n +2 | grep -q .; then
    echo "[obsday] ERROR: port $port is in use; aborting (kill stale df_engine?)" >&2
    exit 3
  fi
done

COLLECTOR_PROFILE="$OUTDIR/collector.profile.json.gz"
LOGGER_PROFILE="$OUTDIR/logger.profile.json.gz"
COLLECTOR_LOG="$OUTDIR/collector.log"
LOGGER_LOG="$OUTDIR/logger.log"

echo "[obsday] starting collector under samply..."
samply record --save-only --unstable-presymbolicate -o "$COLLECTOR_PROFILE" --rate 999 \
  -- "$DF_ENGINE" --config configs/obsday-collector.yaml \
  >"$COLLECTOR_LOG" 2>&1 &
COLLECTOR_PID=$!

cleanup() {
  if kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    curl -fsS -X POST \
      "http://127.0.0.1:9877/api/v1/groups/shutdown?wait=true&timeout_secs=15" \
      >/dev/null 2>&1 || true
    # df_engine's controller does not exit on admin shutdown, and does not
    # install a SIGINT handler. Send SIGTERM to df_engine (samply's child);
    # samply then writes the profile and exits when its child is gone.
    sleep 0.5
    local CHILD
    CHILD=$(pgrep -P "$COLLECTOR_PID" -f df_engine 2>/dev/null | head -n 1 || true)
    if [[ -n "$CHILD" ]]; then
      kill "$CHILD" 2>/dev/null || true
    fi
    for _ in $(seq 1 200); do
      if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "$COLLECTOR_PID" 2>/dev/null; then
      kill "$COLLECTOR_PID" 2>/dev/null || true
    fi
    wait "$COLLECTOR_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

COLLECTOR_READY=0
for _ in $(seq 1 100); do
  if ! kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    break
  fi
  if ss -ltn "( sport = :4317 )" 2>/dev/null | tail -n +2 | grep -q .; then
    if ss -ltn "( sport = :9877 )" 2>/dev/null | tail -n +2 | grep -q .; then
      COLLECTOR_READY=1
      break
    fi
  fi
  sleep 0.1
done

if [[ "$COLLECTOR_READY" -ne 1 ]]; then
  echo "[obsday] ERROR: collector did not bind 4317+9877; tail of log:" >&2
  tail -n 30 "$COLLECTOR_LOG" | sed 's/^/    /' >&2
  exit 4
fi

echo "[obsday] starting logger under samply..."
set +e
samply record --save-only --unstable-presymbolicate -o "$LOGGER_PROFILE" --rate 999 \
  -- "$LOGGER" --config configs/obsday-logger.yaml \
       --rate "$RATE" --duration "$DURATION" --workers "$WORKERS" \
       --attrs "$ATTRS" --attr-size-mean "$MEAN" --attr-size-stddev "$STDDEV" \
       --attr-size-min "$MIN" --seed "$SEED" \
  >"$LOGGER_LOG" 2>&1
LOGGER_RC=$?
set -e

echo "[obsday] logger exited with $LOGGER_RC"
echo "[obsday] tail of logger log:"
tail -n 12 "$LOGGER_LOG" | sed 's/^/    /'

cleanup
trap - EXIT
echo "[obsday] tail of collector log:"
tail -n 12 "$COLLECTOR_LOG" | sed 's/^/    /'

cat <<EOF

[obsday] artefacts:
  $LOGGER_PROFILE
  $LOGGER_LOG
  $COLLECTOR_PROFILE
  $COLLECTOR_LOG

To view either flame graph:
  samply load $LOGGER_PROFILE
  samply load $COLLECTOR_PROFILE
EOF
