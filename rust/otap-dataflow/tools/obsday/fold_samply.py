#!/usr/bin/env python3
"""Convert a samply profile (.json.gz + .syms.json sidecar produced with
--unstable-presymbolicate) into folded stacks suitable for inferno-flamegraph.

Usage:
  fold_samply.py PROFILE.json.gz SYMS.json [--thread-substr SUBSTR ...] > out.folded
"""
import argparse, gzip, json, bisect, re, sys, collections


def build_lib_resolver(syms):
    out = {}
    for entry in syms["data"]:
        dn = entry["debug_name"].lower()
        did = entry["debug_id"].replace("-", "").lower()
        st = sorted(entry["symbol_table"], key=lambda s: s["rva"])
        out[(dn, did)] = st
    return out


def lookup(rva, table):
    rvas = [e["rva"] for e in table]
    i = bisect.bisect_right(rvas, rva) - 1
    if i < 0:
        return None
    e = table[i]
    if rva < e["rva"] + e["size"]:
        return e["symbol"]
    return None


def lib_key(lib):
    bp = lib["breakpadId"].lower()
    bp_hex = re.sub(r"[^0-9a-f]", "", bp)[:32]
    return (lib["debugName"].lower(), bp_hex)


def fold(profile_path, syms_path, thread_substrs):
    with gzip.open(profile_path) as f:
        prof = json.load(f)
    with open(syms_path) as f:
        syms = json.load(f)
    sstrs = syms["string_table"]
    libtbl = build_lib_resolver(syms)
    libs = prof["libs"]

    counts = collections.Counter()

    for t in prof["threads"]:
        tname = t.get("name", "") or ""
        if thread_substrs and not any(s in tname for s in thread_substrs):
            continue
        strings = t["stringArray"]
        funcs = t["funcTable"]
        frames = t["frameTable"]
        stacks = t["stackTable"]
        samples = t["samples"]
        n = samples["length"]

        # Resolve frames once.
        frame_names = []
        for fi in range(frames["length"]):
            addr = frames["address"][fi]
            func_idx = frames["func"][fi]
            res_idx = funcs["resource"][func_idx]
            name = strings[funcs["name"][func_idx]]
            if res_idx is not None and res_idx >= 0:
                lib_idx = t["resourceTable"]["lib"][res_idx]
                if lib_idx is not None and lib_idx >= 0 and addr is not None and addr >= 0:
                    lib = libs[lib_idx]
                    table = libtbl.get(lib_key(lib))
                    if table is None:
                        for k in libtbl:
                            if k[0] == lib_key(lib)[0]:
                                table = libtbl[k]
                                break
                    if table is not None:
                        sym_idx = lookup(addr, table)
                        if sym_idx is not None:
                            name = sstrs[sym_idx]
            # Folded stacks use ';' as separator; sanitize any in symbol names.
            name = name.replace(";", ":")
            frame_names.append(name)

        for i in range(n):
            s = samples["stack"][i]
            if s is None:
                continue
            stack = []
            cur = s
            while cur is not None:
                stack.append(frame_names[stacks["frame"][cur]])
                cur = stacks["prefix"][cur]
            stack.reverse()
            counts[";".join(stack)] += 1

    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("syms")
    ap.add_argument("--thread-substr", action="append", default=None,
                    help="Only fold threads whose name contains this substring "
                         "(may be repeated). Default: all threads.")
    args = ap.parse_args()
    counts = fold(args.profile, args.syms, args.thread_substr)
    for stack, c in counts.items():
        print(f"{stack} {c}")


if __name__ == "__main__":
    main()
