#!/usr/bin/env bash
# obsday measurement harness.
#
# Runs a longer end-to-end test of the logger pipeline with samply CPU
# sampling. Scrapes the engine's Prometheus admin endpoint at 25% and 75%
# of the test duration, then computes effective throughput, CPU%, and peak
# memory from the two snapshots and emits a Markdown report.
#
# Usage:
#   tools/obsday/measure.sh --name LABEL [--rate N] [--duration S] \
#                           [--workers N] [--attrs N] [--mean F] \
#                           [--stddev F] [--min N] [--seed N] \
#                           [--outdir DIR] [--logger-config PATH] \
#                           [--collector-config PATH] [--no-flame]
#
# Output goes to $OUTDIR/$NAME/ and contains:
#   logger.profile.json.gz     samply CPU profile (use flame.sh later)
#   logger.profile.json.syms.json
#   logger.log, collector.log
#   prom-25.txt, prom-75.txt   raw Prometheus scrapes
#   stat-25.txt, stat-75.txt   /proc/<pid>/stat at scrape time
#   status-final.txt           /proc/<pid>/status post-run (VmHWM)
#   REPORT.md                  consolidated summary

set -euo pipefail
cd "$(dirname "$0")/../.."

NAME=""
RATE=50000
DURATION=120
WORKERS=2
ATTRS=8
MEAN=24
STDDEV=8
MIN=1
SEED=1
OUTDIR="./obsday-out/measure"
LOGGER_CFG="configs/obsday-logger.yaml"
COLLECTOR_CFG="configs/obsday-collector.yaml"
WANT_FLAME=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)             NAME="$2";          shift 2 ;;
    --rate)             RATE="$2";          shift 2 ;;
    --duration)         DURATION="$2";      shift 2 ;;
    --workers)          WORKERS="$2";       shift 2 ;;
    --attrs)            ATTRS="$2";         shift 2 ;;
    --mean)             MEAN="$2";          shift 2 ;;
    --stddev)           STDDEV="$2";        shift 2 ;;
    --min)              MIN="$2";           shift 2 ;;
    --seed)             SEED="$2";          shift 2 ;;
    --outdir)           OUTDIR="$2";        shift 2 ;;
    --logger-config)    LOGGER_CFG="$2";    shift 2 ;;
    --collector-config) COLLECTOR_CFG="$2"; shift 2 ;;
    --no-flame)         WANT_FLAME=0;       shift ;;
    -h|--help)          sed -n '1,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

[[ -z "$NAME" ]] && { echo "--name required" >&2; exit 2; }

export PATH="$HOME/.cargo/bin:$PATH"
if [[ "$WANT_FLAME" -eq 1 ]] && ! command -v samply >/dev/null; then
  echo "[measure] ERROR: samply not on PATH (cargo install samply --locked)" >&2
  exit 3
fi

RUNDIR="$OUTDIR/$NAME"
mkdir -p "$RUNDIR"

DF_ENGINE="./target/profiling/df_engine"
LOGGER="./target/profiling/examples/obsday_logger"
if [[ ! -x "$DF_ENGINE" || ! -x "$LOGGER" ]]; then
  echo "[measure] building..."
  cargo build --profile profiling --bin df_engine --example obsday_logger >/dev/null
fi

# Read admin bind addresses out of the YAML so we don't hardcode ports.
LOGGER_ADMIN=$(awk '/http_admin:/{f=1;next} f && /bind_address/{
  gsub(/"/,""); print $2; exit}' "$LOGGER_CFG")
COLLECTOR_ADMIN=$(awk '/http_admin:/{f=1;next} f && /bind_address/{
  gsub(/"/,""); print $2; exit}' "$COLLECTOR_CFG")
LOGGER_PORT="${LOGGER_ADMIN##*:}"
COLLECTOR_PORT="${COLLECTOR_ADMIN##*:}"

echo "[measure:$NAME] rate=$RATE duration=${DURATION}s workers=$WORKERS attrs=$ATTRS mean=$MEAN stddev=$STDDEV"
echo "[measure:$NAME] logger admin: $LOGGER_ADMIN; collector admin: $COLLECTOR_ADMIN"

for port in 4317 "$LOGGER_PORT" "$COLLECTOR_PORT"; do
  if ss -ltn "( sport = :$port )" 2>/dev/null | tail -n +2 | grep -q .; then
    echo "[measure] ERROR: port $port already in use" >&2; exit 4
  fi
done

# 1. Start collector (no samply; we only profile the logger).
"$DF_ENGINE" --config "$COLLECTOR_CFG" \
  >"$RUNDIR/collector.log" 2>&1 &
COLLECTOR_PID=$!

cleanup() {
  # Kill collector child of the bash-launched process.
  curl -fsS -X POST \
    "http://${COLLECTOR_ADMIN}/api/v1/groups/shutdown?wait=true&timeout_secs=15" \
    >/dev/null 2>&1 || true
  sleep 0.3
  kill "$COLLECTOR_PID" 2>/dev/null || true
  for _ in $(seq 1 30); do
    kill -0 "$COLLECTOR_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -9 "$COLLECTOR_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for collector to bind 4317 + its admin port.
for _ in $(seq 1 100); do
  if ss -ltn "( sport = :4317 )" 2>/dev/null | tail -n +2 | grep -q . \
     && ss -ltn "( sport = :$COLLECTOR_PORT )" 2>/dev/null | tail -n +2 | grep -q .; then
    break
  fi
  sleep 0.1
done

# 2. Start logger.
LOG_PROFILE="$RUNDIR/logger.profile.json.gz"
LOG_ARGS=(
  --config "$LOGGER_CFG"
  --rate "$RATE" --duration "$DURATION" --workers "$WORKERS"
  --attrs "$ATTRS" --attr-size-mean "$MEAN" --attr-size-stddev "$STDDEV"
  --attr-size-min "$MIN" --seed "$SEED"
)
if [[ "$WANT_FLAME" -eq 1 ]]; then
  samply record --save-only --unstable-presymbolicate -o "$LOG_PROFILE" \
    --rate 999 -- "$LOGGER" "${LOG_ARGS[@]}" \
    >"$RUNDIR/logger.log" 2>&1 &
else
  "$LOGGER" "${LOG_ARGS[@]}" >"$RUNDIR/logger.log" 2>&1 &
fi
SAMPLY_PID=$!

# Wait for logger admin to bind, identify the actual logger PID (samply child).
LOGGER_PID=""
for _ in $(seq 1 200); do
  if ss -ltn "( sport = :$LOGGER_PORT )" 2>/dev/null | tail -n +2 | grep -q .; then
    if [[ "$WANT_FLAME" -eq 1 ]]; then
      LOGGER_PID=$(pgrep -P "$SAMPLY_PID" -f obsday_logger 2>/dev/null \
                   | head -n 1 || true)
    else
      LOGGER_PID="$SAMPLY_PID"
    fi
    [[ -n "$LOGGER_PID" ]] && break
  fi
  sleep 0.1
done

if [[ -z "$LOGGER_PID" ]]; then
  echo "[measure] ERROR: logger never bound :$LOGGER_PORT" >&2
  echo "[measure] last logger.log:" >&2
  tail -n 30 "$RUNDIR/logger.log" | sed 's/^/    /' >&2
  exit 5
fi
echo "[measure:$NAME] logger PID $LOGGER_PID; sampling /proc..."

# Helper: snapshot Prometheus + /proc/<pid>/stat.
snapshot() {
  local label="$1"
  date +%s.%N > "$RUNDIR/wall-${label}.txt"
  cp "/proc/$LOGGER_PID/stat" "$RUNDIR/stat-${label}.txt" 2>/dev/null || true
  cp "/proc/$LOGGER_PID/status" "$RUNDIR/status-${label}.txt" 2>/dev/null || true
  curl -fsS "http://${LOGGER_ADMIN}/api/v1/telemetry/metrics?format=prometheus" \
    > "$RUNDIR/prom-${label}.txt" 2>/dev/null || true
}

# 3. Scrape at 25% and 75% of duration.
T25=$(awk -v d="$DURATION" 'BEGIN{printf "%.3f", d*0.25}')
T_MID=$(awk -v d="$DURATION" 'BEGIN{printf "%.3f", d*0.50}')

sleep "$T25"
snapshot 25
sleep "$T_MID"
snapshot 75

# 4. Wait for logger to finish.
wait "$SAMPLY_PID" || true

cleanup
trap - EXIT

# 5. Report.
python3 - "$RUNDIR" "$NAME" "$RATE" "$DURATION" "$ATTRS" "$MEAN" "$STDDEV" "$WORKERS" <<'PY'
import os, re, sys

run, name, rate, dur, attrs, mean, stddev, workers = sys.argv[1:9]

def read(path):
    try: return open(path).read()
    except FileNotFoundError: return ""

def read_float(path):
    s = read(path).strip()
    return float(s) if s else None

def parse_stat(path):
    # /proc/PID/stat fields: utime=14 stime=15 in clock ticks. (1-indexed)
    s = read(path)
    if not s: return None
    # comm field is parenthesized and may contain spaces -> split safely
    rp = s.rfind(")")
    rest = s[rp+2:].split()
    # rest[0] is field 3 (state); utime is field 14 -> rest[11]; stime is field 15 -> rest[12]
    utime = int(rest[11])
    stime = int(rest[12])
    return utime, stime

def parse_prom(path, name_substr):
    """Examine samples whose metric name contains the given substring.
    Returns (sum, max, dict-of-individual-samples). max is most useful when
    the same metric is emitted by multiple pipeline stages and you want the
    upstream-most count (records-into-stage-1 >= records-into-stage-2)."""
    total = 0.0
    mx = None
    samples = {}
    for line in read(path).splitlines():
        if not line or line.startswith("#"): continue
        m = re.match(r'([A-Za-z_:][A-Za-z0-9_:]*)(\{[^}]*\})?\s+([-0-9.eE+inf]+)', line)
        if not m: continue
        n, lbls, val = m.group(1), m.group(2) or "", m.group(3)
        if name_substr not in n: continue
        try: v = float(val)
        except ValueError: continue
        total += v
        mx = v if mx is None else max(mx, v)
        samples[n + lbls] = v
    return total, (mx or 0.0), samples

# Wall times
w25 = read_float(os.path.join(run, "wall-25.txt"))
w75 = read_float(os.path.join(run, "wall-75.txt"))

# /proc stats
s25 = parse_stat(os.path.join(run, "stat-25.txt"))
s75 = parse_stat(os.path.join(run, "stat-75.txt"))

clk = os.sysconf("SC_CLK_TCK")  # typically 100

# Prometheus counters
# In this experiment, the ITS receiver emits one record per input batch,
# so consumed_batches_logs == records consumed by the first batch processor.
# Sum across any batch-processor nodes to handle multi-stage pipelines.
# Use upstream-most batch processor's count (max across pipeline stages) for
# records, and sum (== max for a single stage) for bytes-on-the-wire is the
# bytes leaving the *last* batch processor before the OTLP exporter; pick max
# because adding multi-stage byte counts would double-count.
_, records25, _ = parse_prom(os.path.join(run, "prom-25.txt"),
                             "consumed_batches_logs")
_, records75, _ = parse_prom(os.path.join(run, "prom-75.txt"),
                             "consumed_batches_logs")
_, bytes25, _ = parse_prom(os.path.join(run, "prom-25.txt"),
                           "flush_output_bytes_sum")
_, bytes75, _ = parse_prom(os.path.join(run, "prom-75.txt"),
                           "flush_output_bytes_sum")

# Memory: VmHWM (peak RSS) and VmRSS (final). Take the latest snapshot
# captured while the logger was still alive (75% mark, fall back to 25%).
vm = {}
for cand in ("status-75.txt", "status-25.txt"):
    p = os.path.join(run, cand)
    if os.path.exists(p) and os.path.getsize(p) > 0:
        for line in read(p).splitlines():
            if line.startswith(("VmRSS:", "VmHWM:", "VmSize:")):
                k, v = line.split(":", 1)
                vm[k] = v.strip()
        break

# Compute window deltas (25% -> 75%).
def fmt(x, unit=""):
    return f"{x:,.2f}{unit}" if isinstance(x, float) else f"{x:,}{unit}"

dwall = (w75 - w25) if (w25 is not None and w75 is not None) else None
drec = records75 - records25
dbytes = bytes75 - bytes25

# CPU% over the window
cpu_pct = None
if s25 and s75 and dwall and dwall > 0:
    duser = (s75[0] - s25[0]) / clk
    dsys  = (s75[1] - s25[1]) / clk
    cpu_pct = 100.0 * (duser + dsys) / dwall

# Effective rate / bandwidth
recs_per_s = drec / dwall if dwall and dwall > 0 else None
bytes_per_s = dbytes / dwall if dwall and dwall > 0 else None

# Pull the obsday_logger's own self-reported emitted=N (final tally)
emitted = None
m = re.search(r"emission done: emitted=(\d+) wall=([\d.]+)s effective_rate=(\d+)/s",
              read(os.path.join(run, "logger.log")))
if m:
    emitted = int(m.group(1))
    overall_eff = int(m.group(3))
else:
    overall_eff = None

# Render report
lines = []
lines.append(f"# obsday measurement: `{name}`\n")
lines.append("## Configuration\n")
lines.append(f"- target rate: **{int(rate):,}/s**, duration: **{dur}s**, workers: **{workers}**")
lines.append(f"- attrs: **{attrs}**, attr size mean: **{mean}**, stddev: **{stddev}**")
lines.append("")
lines.append("## Window measurement (25% -> 75% of test)\n")
lines.append(f"- window wall time: **{fmt(dwall, ' s')}**")
lines.append(f"- records sent: **{int(drec):,}**  -> **{fmt(recs_per_s, ' rec/s') if recs_per_s else 'n/a'}**")
lines.append(f"- bytes sent (post-batch, OTLP wire): **{int(dbytes):,} B**  -> **{fmt(bytes_per_s, ' B/s') if bytes_per_s else 'n/a'}**")
lines.append(f"- logger CPU usage: **{fmt(cpu_pct, '%') if cpu_pct is not None else 'n/a'}**  (sum of user+sys, divided by wall)")
lines.append("")
lines.append("## Memory (logger process)\n")
for k in ("VmHWM", "VmRSS", "VmSize"):
    if k in vm:
        lines.append(f"- {k}: **{vm[k]}**")
lines.append("")
lines.append("## Logger self-report (full run)\n")
if emitted is not None:
    lines.append(f"- emitted: **{emitted:,}** records over {dur}s "
                 f"(effective {overall_eff:,}/s)")
else:
    lines.append("- (no `emission done:` line found in logger.log)")
lines.append("")
lines.append("## Raw artefacts\n")
lines.append(f"- profile (samply): `{os.path.relpath(os.path.join(run, 'logger.profile.json.gz'))}`")
lines.append(f"- profile syms:     `{os.path.relpath(os.path.join(run, 'logger.profile.json.syms.json'))}`")
lines.append(f"- prometheus dumps: `{os.path.relpath(os.path.join(run, 'prom-25.txt'))}`,"
             f" `{os.path.relpath(os.path.join(run, 'prom-75.txt'))}`")
lines.append(f"- /proc/<pid>/stat: `{os.path.relpath(os.path.join(run, 'stat-25.txt'))}`,"
             f" `{os.path.relpath(os.path.join(run, 'stat-75.txt'))}`")
lines.append(f"- /proc/<pid>/status: `{os.path.relpath(os.path.join(run, 'status-25.txt'))}`,"
             f" `{os.path.relpath(os.path.join(run, 'status-75.txt'))}`")
lines.append(f"- logger log: `{os.path.relpath(os.path.join(run, 'logger.log'))}`")
lines.append(f"- collector log: `{os.path.relpath(os.path.join(run, 'collector.log'))}`")
lines.append("")
lines.append("To render flamegraph from the saved profile:\n")
lines.append("```")
lines.append(f"export PATH=\"$HOME/.cargo/bin:$PATH\"")
lines.append(f"python3 tools/obsday/fold_samply.py {os.path.relpath(os.path.join(run, 'logger.profile.json.gz'))} \\")
lines.append(f"    {os.path.relpath(os.path.join(run, 'logger.profile.json.syms.json'))} \\")
lines.append(f"    --thread-substr pipeline-system \\")
lines.append(f"    | inferno-flamegraph > {os.path.relpath(os.path.join(run, 'logger.pipeline.flamegraph.svg'))}")
lines.append("```")

text = "\n".join(lines) + "\n"
with open(os.path.join(run, "REPORT.md"), "w") as f:
    f.write(text)
print(text)
PY

echo "[measure:$NAME] report -> $RUNDIR/REPORT.md"
