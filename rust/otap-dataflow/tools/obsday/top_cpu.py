#!/usr/bin/env python3
"""Print top-N CPU contributors (self + inclusive) from a samply profile.

Usage:
  top_cpu.py PROFILE.json.gz SYMS.json [--thread-substr SUBSTR ...] [--top N]
"""
import argparse, sys, collections, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fold_samply import fold


def aggregate(counts):
    self_c = collections.Counter()
    incl_c = collections.Counter()
    total = 0
    for stack, c in counts.items():
        frames = stack.split(";")
        if not frames:
            continue
        total += c
        self_c[frames[-1]] += c
        for fn in set(frames):
            incl_c[fn] += c
    return self_c, incl_c, total


def fmt(name, n, total, width=80):
    pct = 100.0 * n / total if total else 0.0
    if len(name) > width:
        name = name[: width - 1] + "\u2026"
    return f"  {n:8d}  {pct:6.2f}%  {name}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("syms")
    ap.add_argument("--thread-substr", action="append", default=None)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    counts = fold(args.profile, args.syms, args.thread_substr)
    self_c, incl_c, total = aggregate(counts)

    label = f" ({args.label})" if args.label else ""
    threads = (",".join(args.thread_substr) if args.thread_substr else "all")
    print(f"# Top {args.top} CPU{label}")
    print(f"# profile: {args.profile}")
    print(f"# threads: {threads}; total samples: {total}")
    print()
    print(f"## Top {args.top} by SELF samples")
    print(f"  {'samples':>8}  {'pct':>6}  function")
    for name, n in self_c.most_common(args.top):
        print(fmt(name, n, total))
    print()
    print(f"## Top {args.top} by INCLUSIVE samples")
    print(f"  {'samples':>8}  {'pct':>6}  function")
    for name, n in incl_c.most_common(args.top):
        print(fmt(name, n, total))


if __name__ == "__main__":
    main()
