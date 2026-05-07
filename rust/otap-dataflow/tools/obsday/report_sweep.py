#!/usr/bin/env python3
"""Aggregate per-run REPORT.md files into a combined sweep report.

Outputs Markdown with a single summary table followed by the raw top-20
listings per run. Run rows are emitted in the order they appear in the
matrix file.
"""
import os, re, sys, subprocess

OUTDIR = sys.argv[1]
MATRIX = sys.argv[2]
DURATION = sys.argv[3]
NUM_CORES = sys.argv[4]

TOP_CPU = os.path.join(os.path.dirname(__file__), "top_cpu.py")


def parse_report_md(path):
    """Pull a few key numbers out of a REPORT.md."""
    out = {
        "wall": None, "records": None, "rec_per_s": None,
        "bytes": None, "bytes_per_s": None,
        "cpu_pct": None, "drop_pct": None,
        "vmhwm": None, "emitted": None, "effective": None,
    }
    if not os.path.exists(path):
        return out
    txt = open(path).read()
    def grab(pat, key, cast=float, group=1):
        m = re.search(pat, txt)
        if m:
            try:
                out[key] = cast(m.group(group).replace(",", ""))
            except Exception:
                pass
    grab(r"window wall time:\s*\*\*([\d.]+)\s*s\*\*", "wall")
    grab(r"records sent:\s*\*\*([\d,]+)\*\*", "records", int)
    grab(r"records sent:[^\n]*->\s*\*\*([\d,.]+)\s*rec/s\*\*", "rec_per_s")
    grab(r"bytes sent[^*]*\*\*([\d,]+)\s*B\*\*", "bytes", int)
    grab(r"bytes sent[^\n]*->\s*\*\*([\d,.]+)\s*B/s\*\*", "bytes_per_s")
    grab(r"logger CPU usage:\s*\*\*([\d.]+)%\*\*", "cpu_pct")
    grab(r"ingest drops:\s*\*\*([\d.]+)%\*\*", "drop_pct")
    grab(r"VmHWM:\s*\*\*([\d,]+)\s*kB\*\*", "vmhwm", int)
    grab(r"emitted:\s*\*\*([\d,]+)\*\*", "emitted", int)
    grab(r"effective\s+([\d,]+)/s", "effective", int)
    return out


def fmt_int(x):
    return f"{int(x):,}" if x is not None else "n/a"

def fmt_float(x, suffix="", prec=2):
    if x is None:
        return "n/a"
    return f"{x:,.{prec}f}{suffix}"

def fmt_kb(x):
    if x is None:
        return "n/a"
    if x >= 1024 * 1024:
        return f"{x/1024/1024:.1f} GB"
    if x >= 1024:
        return f"{x/1024:.1f} MB"
    return f"{x} kB"

def fmt_bytes_per_s(x):
    if x is None:
        return "n/a"
    if x >= 1e6:
        return f"{x/1e6:.1f} MB/s"
    if x >= 1e3:
        return f"{x/1e3:.1f} kB/s"
    return f"{x:.0f} B/s"


# Read matrix.
runs = []
for line in open(MATRIX):
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    parts = line.split()
    if len(parts) < 4:
        continue
    name, config, workers, rate = parts[0], parts[1], int(parts[2]), int(parts[3])
    runs.append((name, config, workers, rate))


# Header.
print("# obsday sweep report")
print()
print(f"- duration per run: **{DURATION}s**")
print(f"- engine cores per run: **{NUM_CORES}**")
print(f"- matrix: `{MATRIX}` ({len(runs)} runs)")
print()


# Summary table.
print("## Summary")
print()
hdr = ["run", "config", "workers", "target/s", "ingested/s",
       "drops%", "wire B/s", "process CPU%", "VmHWM",
       "worker emitted/s"]
print("| " + " | ".join(hdr) + " |")
print("|" + "|".join(["---"] * len(hdr)) + "|")
for name, config, workers, rate in runs:
    rep = parse_report_md(os.path.join(OUTDIR, name, "REPORT.md"))
    cfg_short = os.path.basename(config).replace("obsday-", "").replace(".yaml", "")
    ingested = rep["rec_per_s"]
    drops_pct = None
    if ingested is not None and rate > 0:
        drops_pct = max(0.0, 100.0 * (1.0 - ingested / rate))
    row = [
        f"`{name}`",
        cfg_short,
        str(workers),
        f"{rate:,}",
        fmt_float(ingested),
        fmt_float(drops_pct, "%") if drops_pct is not None else "n/a",
        fmt_bytes_per_s(rep["bytes_per_s"]),
        fmt_float(rep["cpu_pct"], "%"),
        fmt_kb(rep["vmhwm"]),
        fmt_int(rep["effective"]),
    ]
    print("| " + " | ".join(row) + " |")
print()


# Per-run top-20 listings.
print("## Top-20 CPU contributors per run")
print()
for name, config, workers, rate in runs:
    rundir = os.path.join(OUTDIR, name)
    profile = os.path.join(rundir, "logger.profile.json.gz")
    syms = os.path.join(rundir, "logger.profile.json.syms.json")
    print(f"### `{name}`  (config={os.path.basename(config)} workers={workers} target={rate:,}/s)")
    print()
    if not (os.path.exists(profile) and os.path.exists(syms)):
        print("> profile/syms missing")
        print()
        continue
    try:
        top = subprocess.run(
            ["python3", TOP_CPU, profile, syms, "--label", name, "--top", "20"],
            check=True, capture_output=True, text=True,
        ).stdout
    except subprocess.CalledProcessError as e:
        print("```")
        print(f"top_cpu.py failed: {e.stderr}")
        print("```")
        print()
        continue
    print("```")
    print(top.rstrip())
    print("```")
    print()
