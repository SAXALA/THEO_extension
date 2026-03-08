#!/usr/bin/env python3
"""Extract replay metrics from ethstore/pebble replay logs.

Output is CSV only, one row per main replay log. For each main log, this script
auto-detects the paired "*_io_*.log" file and computes:
- avg CPU usage across all io samples
- avg RSS_KB across all io samples
- physical read/write bytes from the last valid sample row

From the main replay log, it extracts:
- replay summary: ops, total time, throughput, logical read/write
- GC summary: PrefixDB GC count and GC write bytes
- latency metrics for BlockData/OtherData/StateData and Global, op in
    {Get, Put, Delete, NewIterator}, fields {throughput, avg, p50, p99, p99.999}

Optional filtering:
- --only-replay: keep only replay logs (filename contains "_replay_")
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

REPLAY_FINISHED_RE = re.compile(
    r"Replay finished\.\s+ops=(\d+)\s+time=([0-9]*\.?[0-9]+)s\s+"
    r"throughput=([0-9]*\.?[0-9]+)\s*ops/s\s+read=(\d+)\s+write=(\d+)"
)

LATENCY_DATATYPE_RE = re.compile(
    r"\[Latency\]\s+dataType=(\w+)\s+op=(\w+)\s+count=(\d+)\s+"
    r"throughput=([0-9]*\.?[0-9]+)\s*([KMG]?)\s*ops/s\s+"
    r"avg=([^\s]+).*?p50=([^\s]+).*?p99=([^\s]+).*?p99\.999=([^\s]+)"
)

LATENCY_GLOBAL_RE = re.compile(
    r"\[Latency\]\[Global\]\s+op=(\w+)\s+count=(\d+)\s+"
    r"throughput=([0-9]*\.?[0-9]+)\s*([KMG]?)\s*ops/s\s+"
    r"avg=([^\s]+).*?p50=([^\s]+).*?p99=([^\s]+).*?p99\.999=([^\s]+)"
)

GC_RE = re.compile(r"PrefixDB GC stats:\s*count=(\d+)\s+writeBytes=(\d+)")


def parse_latency_to_us(token: str) -> float:
    m = re.fullmatch(r"([0-9]*\.?[0-9]+)(ns|us|ms|s)", token)
    if not m:
        raise ValueError(f"unsupported latency token: {token}")
    value = float(m.group(1))
    unit = m.group(2)
    if unit == "ns":
        return value / 1000.0
    if unit == "us":
        return value
    if unit == "ms":
        return value * 1000.0
    if unit == "s":
        return value * 1_000_000.0
    raise ValueError(f"unexpected latency unit: {unit}")


def parse_throughput_to_ops(rate: float, prefix: str) -> float:
    factor = {
        "": 1.0,
        "K": 1_000.0,
        "M": 1_000_000.0,
        "G": 1_000_000_000.0,
    }.get(prefix)
    if factor is None:
        raise ValueError(f"unsupported throughput prefix: {prefix}")
    return rate * factor


def empty_latency_tree() -> dict[str, dict[str, dict[str, Any]]]:
    groups = ["BlockData", "OtherData", "StateData", "Global"]
    ops = ["Get", "Put", "Delete", "NewIterator"]
    tree: dict[str, dict[str, dict[str, Any]]] = {}
    for g in groups:
        tree[g] = {}
        for op in ops:
            tree[g][op] = {
                "count": None,
                "throughput_ops_s": None,
                "avg_us": None,
                "p50_us": None,
                "p99_us": None,
                "p99999_us": None,
            }
    return tree


def parse_log(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "file": str(path),
        "backend": None,
        "summary": {
            "ops": None,
            "total_time_s": None,
            "throughput_ops_s": None,
            "logical_read": None,
            "logical_write": None,
        },
        "latency": empty_latency_tree(),
        "gc": {
            "count": None,
            "write_bytes": None,
        },
        "warnings": [],
    }

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        result["warnings"].append("file not found")
        return result

    for line in lines:
        if line.startswith("BACKEND="):
            result["backend"] = line.split("=", 1)[1].strip()
            break

    for line in lines:
        m = REPLAY_FINISHED_RE.search(line)
        if m:
            result["summary"] = {
                "ops": int(m.group(1)),
                "total_time_s": float(m.group(2)),
                "throughput_ops_s": float(m.group(3)),
                "logical_read": int(m.group(4)),
                "logical_write": int(m.group(5)),
            }

    for line in lines:
        m = GC_RE.search(line)
        if m:
            result["gc"]["count"] = int(m.group(1))
            result["gc"]["write_bytes"] = int(m.group(2))

    for line in lines:
        m = LATENCY_DATATYPE_RE.search(line)
        if not m:
            continue
        dtype, op = m.group(1), m.group(2)
        if dtype not in result["latency"] or op not in result["latency"][dtype]:
            continue
        rate = float(m.group(4))
        prefix = m.group(5)
        try:
            result["latency"][dtype][op] = {
                "count": int(m.group(3)),
                "throughput_ops_s": parse_throughput_to_ops(rate, prefix),
                "avg_us": parse_latency_to_us(m.group(6)),
                "p50_us": parse_latency_to_us(m.group(7)),
                "p99_us": parse_latency_to_us(m.group(8)),
                "p99999_us": parse_latency_to_us(m.group(9)),
            }
        except ValueError as e:
            result["warnings"].append(str(e))

    for line in lines:
        m = LATENCY_GLOBAL_RE.search(line)
        if not m:
            continue
        op = m.group(1)
        if op not in result["latency"]["Global"]:
            continue
        rate = float(m.group(3))
        prefix = m.group(4)
        try:
            result["latency"]["Global"][op] = {
                "count": int(m.group(2)),
                "throughput_ops_s": parse_throughput_to_ops(rate, prefix),
                "avg_us": parse_latency_to_us(m.group(5)),
                "p50_us": parse_latency_to_us(m.group(6)),
                "p99_us": parse_latency_to_us(m.group(7)),
                "p99999_us": parse_latency_to_us(m.group(8)),
            }
        except ValueError as e:
            result["warnings"].append(str(e))

    if result["summary"]["ops"] is None:
        result["warnings"].append("missing replay summary line")

    return result


def find_io_log(main_log: Path) -> Path:
    base = main_log.name
    if "_io_" in base:
        return main_log
    parts = base.rsplit("_", 1)
    if len(parts) != 2:
        return main_log.with_name(base.replace(".log", "_io.log"))
    return main_log.with_name(f"{parts[0]}_io_{parts[1]}")


def normalize_main_log(path: Path) -> Path:
    base = path.name
    if "_io_" not in base:
        return path
    # Convert e.g. "..._io_03-08-...log" -> "..._03-08-...log"
    return path.with_name(base.replace("_io_", "_", 1))


def is_replay_log(path: Path) -> bool:
    return "_replay_" in path.name.lower()


def safe_float(token: str) -> float | None:
    t = token.strip()
    if t == "":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def safe_int(token: str) -> int | None:
    t = token.strip()
    if t == "":
        return None
    try:
        return int(t)
    except ValueError:
        return None


def parse_io_log(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "io_file": str(path),
        "io_sample_count": 0,
        "io_avg_cpu_usage": None,
        "io_avg_rss_kb": None,
        "physical_read_bytes": None,
        "physical_write_bytes": None,
        "warnings": [],
    }

    if not path.exists():
        result["warnings"].append("io log not found")
        return result

    cpu_sum = 0.0
    cpu_n = 0
    rss_sum = 0.0
    rss_n = 0
    last_rchar: int | None = None
    last_wchar: int | None = None

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, skipinitialspace=True)
        header_seen = False
        for row in reader:
            if not row:
                continue
            if not header_seen:
                # Header starts with TIMESTAMP,CPU_USAGE,...
                if row[0].strip().upper() == "TIMESTAMP":
                    header_seen = True
                continue

            if len(row) < 9:
                continue

            cpu = safe_float(row[1])
            rss = safe_float(row[2])
            rchar = safe_int(row[7])
            wchar = safe_int(row[8])

            if cpu is not None:
                cpu_sum += cpu
                cpu_n += 1
            if rss is not None:
                rss_sum += rss
                rss_n += 1

            # Keep the last row with valid total read/write bytes.
            if rchar is not None and wchar is not None:
                last_rchar = rchar
                last_wchar = wchar

    result["io_sample_count"] = max(cpu_n, rss_n)
    if cpu_n > 0:
        result["io_avg_cpu_usage"] = cpu_sum / cpu_n
    if rss_n > 0:
        result["io_avg_rss_kb"] = rss_sum / rss_n
    result["physical_read_bytes"] = last_rchar
    result["physical_write_bytes"] = last_wchar

    if cpu_n == 0:
        result["warnings"].append("io cpu samples missing")
    if rss_n == 0:
        result["warnings"].append("io rss samples missing")
    if last_rchar is None or last_wchar is None:
        result["warnings"].append("io physical bytes missing")

    return result


def item_to_single_row(item: dict[str, Any], io: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "file": Path(item["file"]).name,
        "backend": item["backend"],
        "ops": item["summary"]["ops"],
        "total_time_s": item["summary"]["total_time_s"],
        "throughput_ops_s": item["summary"]["throughput_ops_s"],
        "logical_read": item["summary"]["logical_read"],
        "logical_write": item["summary"]["logical_write"],
        "gc_count": item["gc"]["count"],
        "gc_write_bytes": item["gc"]["write_bytes"],
        "io_sample_count": io["io_sample_count"],
        "io_avg_cpu_usage": io["io_avg_cpu_usage"],
        "io_avg_rss_kb": io["io_avg_rss_kb"],
        "physical_read_bytes": io["physical_read_bytes"],
        "physical_write_bytes": io["physical_write_bytes"],
    }

    groups = ["BlockData", "OtherData", "StateData", "Global"]
    ops = ["Get", "Put", "Delete", "NewIterator"]
    metric_keys = [
        "throughput_ops_s",
        "avg_us",
        "p50_us",
        "p99_us",
        "p99999_us",
    ]
    for g in groups:
        for op in ops:
            m = item["latency"][g][op]
            prefix = f"{g}_{op}"
            row[f"{prefix}_count"] = m["count"]
            for mk in metric_keys:
                row[f"{prefix}_{mk}"] = m[mk]

    warnings = []
    warnings.extend(item["warnings"])
    warnings.extend(io["warnings"])
    row["warnings"] = " | ".join(warnings)
    return row


def write_csv(rows: list[dict[str, Any]], output: Path | None) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    if output:
        with output.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
    else:
        w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Extract replay metrics to one-row-per-log CSV")
    ap.add_argument("files", nargs="+", help="log file path(s)")
    ap.add_argument("--format", choices=["csv"], default="csv", help=argparse.SUPPRESS)
    ap.add_argument("--output", help="write output to file instead of stdout")
    ap.add_argument(
        "--only-replay",
        action="store_true",
        help="only include replay logs (filename contains '_replay_')",
    )
    args = ap.parse_args(argv)

    # Normalize input so users can pass either main logs or *_io.log files.
    main_logs: list[Path] = []
    seen: set[str] = set()
    for p in args.files:
        normalized = normalize_main_log(Path(p))
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        main_logs.append(normalized)

    if args.only_replay:
        main_logs = [p for p in main_logs if is_replay_log(p)]

    items = [parse_log(p) for p in main_logs]
    out_path = Path(args.output) if args.output else None

    rows: list[dict[str, Any]] = []
    for item, p in zip(items, main_logs):
        io_metrics = parse_io_log(find_io_log(p))
        rows.append(item_to_single_row(item, io_metrics))

    write_csv(rows, out_path)

    # Do not fail on mixed log sets that include non-replay logs.
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
