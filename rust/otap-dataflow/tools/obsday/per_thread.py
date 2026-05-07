#!/usr/bin/env python3
"""Print per-thread sample counts from a samply profile."""
import argparse, gzip, json, collections

ap = argparse.ArgumentParser()
ap.add_argument("profile")
args = ap.parse_args()

with gzip.open(args.profile) as f:
    prof = json.load(f)

per = collections.Counter()
for t in prof["threads"]:
    name = t.get("name", "") or "<unnamed>"
    n = t["samples"]["length"]
    per[name] += n

total = sum(per.values())
print(f"total samples: {total}")
print()
print(f"  {'samples':>8}  {'pct':>6}  thread")
for name, n in per.most_common():
    print(f"  {n:8d}  {100.0*n/total:6.2f}%  {name}")
