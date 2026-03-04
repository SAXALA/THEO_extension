#!/usr/bin/env python3
"""Compute GCCount statistics from ethstore replay logs.

Parses lines like:
    GCCount for folder 192218: 1

Aggregates counts per folder (sums if a folder appears multiple times), then prints:
- total GC count (sum over folders)
- min/max per-folder GC count
- average per-folder GC count

Usage:
    python3 gc_stats.py path/to/ethstoreLog_*.log
    python3 gc_stats.py path/to/*.log
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

LINE_RE = re.compile(r"^GCCount for folder\s+(\d+):\s+(\d+)\s*$")


def parse_file(path: Path) -> dict[int, int]:
    counts: dict[int, int] = {}
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = LINE_RE.match(line)
                if not m:
                    continue
                folder_id = int(m.group(1))
                value = int(m.group(2))
                counts[folder_id] = counts.get(folder_id, 0) + value
    except FileNotFoundError:
        raise
    return counts


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="GCCount stats from ethstore logs")
    ap.add_argument("files", nargs="+", help="log file(s) to parse")
    args = ap.parse_args(argv)

    merged: dict[int, int] = {}
    missing = 0
    for p in args.files:
        path = Path(p)
        try:
            per = parse_file(path)
        except FileNotFoundError:
            print(f"missing file: {path}", file=sys.stderr)
            missing += 1
            continue
        for k, v in per.items():
            merged[k] = merged.get(k, 0) + v

    if not merged:
        print("no GCCount lines found")
        return 2 if missing else 1

    values = list(merged.values())
    total = sum(values)
    folders = len(values)
    min_v = min(values)
    max_v = max(values)
    avg = total / folders if folders else 0.0

    # Also print which folder had min/max (ties pick one deterministically).
    min_folder = min((fid for fid, v in merged.items() if v == min_v))
    max_folder = min((fid for fid, v in merged.items() if v == max_v))

    print(f"folders: {folders}")
    print(f"total_gc: {total}")
    print(f"min_gc: {min_v} (folder {min_folder})")
    print(f"max_gc: {max_v} (folder {max_folder})")
    print(f"avg_gc: {avg:.6f}")

    return 0 if missing == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
