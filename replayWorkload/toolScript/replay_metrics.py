#!/usr/bin/env python3
"""Extract replay metrics from ethstore/pebble replay logs.

Output is CSV only, one row per main replay log. For each main log, this script
auto-detects the paired "*_io_*.log" file and computes:
- avg CPU usage across all io samples
- avg RSS_KB across all io samples
- filesystem-level read/write op counts from /proc/PID/io syscr/syscw
- filesystem-level read/write bytes derived from TOTAL_RCHAR/TOTAL_WCHAR deltas
- NAND flash total read/write counts from the paired ".stat" file
- NAND flash total read/write sizes derived from 4 KiB pages

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
import math
import re
import sys
from pathlib import Path
from typing import Any

NAND_PAGE_BYTES = 4096

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
ROUND_TOKEN_RE = re.compile(r"(?:^|_)r_(\d+)(?:_|$)")
TRAILING_TIMESTAMP_RE = re.compile(r"_\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$")


T_CRITICAL_95: dict[int, float] = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}


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


def find_stat_log(main_log: Path) -> Path:
    return Path(f"{find_io_log(main_log)}.stat")


def normalize_main_log(path: Path) -> Path:
    base = path.name
    if "_io_" not in base:
        return path
    # Convert e.g. "..._io_03-08-...log" -> "..._03-08-...log"
    return path.with_name(base.replace("_io_", "_", 1))


def parse_round_from_name(path: Path) -> int | None:
    stem = path.stem
    m = ROUND_TOKEN_RE.search(stem)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def build_round_group_key(path: Path) -> str:
    stem = path.stem
    stem = TRAILING_TIMESTAMP_RE.sub("", stem)
    stem = re.sub(r"_r_\d+(?=_|$)", "", stem)
    stem = re.sub(r"__+", "_", stem).strip("_")
    return stem


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
        "fs_read_bytes_start": None,
        "fs_read_bytes_end": None,
        "fs_read_bytes_diff": None,
        "fs_write_bytes_start": None,
        "fs_write_bytes_end": None,
        "fs_write_bytes_diff": None,
        "warnings": [],
    }

    if not path.exists():
        result["warnings"].append("io log not found")
        return result

    cpu_sum = 0.0
    cpu_n = 0
    rss_sum = 0.0
    rss_n = 0
    first_rchar: int | None = None
    first_wchar: int | None = None
    last_rchar: int | None = None
    last_wchar: int | None = None

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            if not row:
                continue

            cpu = safe_float(row.get("CPU_USAGE", ""))
            rss = safe_float(row.get("RSS_KB", ""))
            rchar = safe_int(row.get("TOTAL_RCHAR", ""))
            wchar = safe_int(row.get("TOTAL_WCHAR", ""))

            if rchar is None:
                rchar = safe_int(row.get("TOTAL_READ_BYTES", ""))
            if wchar is None:
                wchar = safe_int(row.get("TOTAL_WRITE_BYTES", ""))

            if cpu is not None:
                cpu_sum += cpu
                cpu_n += 1
            if rss is not None:
                rss_sum += rss
                rss_n += 1

            # Keep the last row with valid total read/write bytes.
            if rchar is not None and wchar is not None:
                if first_rchar is None:
                    first_rchar = rchar
                if first_wchar is None:
                    first_wchar = wchar
                last_rchar = rchar
                last_wchar = wchar

    result["io_sample_count"] = max(cpu_n, rss_n)
    if cpu_n > 0:
        result["io_avg_cpu_usage"] = cpu_sum / cpu_n
    if rss_n > 0:
        result["io_avg_rss_kb"] = rss_sum / rss_n
    result["fs_read_bytes_start"] = first_rchar
    result["fs_read_bytes_end"] = last_rchar
    result["fs_write_bytes_start"] = first_wchar
    result["fs_write_bytes_end"] = last_wchar
    if first_rchar is not None and last_rchar is not None:
        result["fs_read_bytes_diff"] = last_rchar - first_rchar
    if first_wchar is not None and last_wchar is not None:
        result["fs_write_bytes_diff"] = last_wchar - first_wchar

    if cpu_n == 0:
        result["warnings"].append("io cpu samples missing")
    if rss_n == 0:
        result["warnings"].append("io rss samples missing")
    if last_rchar is None or last_wchar is None:
        result["warnings"].append("io physical bytes missing")

    return result


def parse_stat_log(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "stat_file": str(path),
        "fs_read_ops_start": None,
        "fs_read_ops_end": None,
        "fs_read_ops_diff": None,
        "fs_write_ops_start": None,
        "fs_write_ops_end": None,
        "fs_write_ops_diff": None,
        "nand_total_read_diff": None,
        "nand_total_write_diff": None,
        "nand_read_bytes": None,
        "nand_write_bytes": None,
        "nand_counter_source": None,
        "warnings": [],
    }

    if not path.exists():
        result["warnings"].append("stat file not found")
        return result

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        row = next(reader, None)

    if row is None:
        result["warnings"].append("stat row missing")
        return result

    fs_read_ops_start = safe_int(row.get("FS_READ_OPS_START", ""))
    fs_read_ops_end = safe_int(row.get("FS_READ_OPS_END", ""))
    fs_read_ops_diff = safe_int(row.get("FS_READ_OPS_DIFF", ""))
    fs_write_ops_start = safe_int(row.get("FS_WRITE_OPS_START", ""))
    fs_write_ops_end = safe_int(row.get("FS_WRITE_OPS_END", ""))
    fs_write_ops_diff = safe_int(row.get("FS_WRITE_OPS_DIFF", ""))
    nand_total_read_diff = safe_int(row.get("NAND_TOTAL_READ_DIFF", ""))
    nand_total_write_diff = safe_int(
        row.get("NAND_TOTAL_WRITE_DIFF", row.get("NAND_WRITE_BYTES_DIFF", ""))
    )
    nand_counter_source = (row.get("NAND_COUNTER_SOURCE") or "").strip() or None

    result["fs_read_ops_start"] = fs_read_ops_start
    result["fs_read_ops_end"] = fs_read_ops_end
    result["fs_read_ops_diff"] = fs_read_ops_diff
    result["fs_write_ops_start"] = fs_write_ops_start
    result["fs_write_ops_end"] = fs_write_ops_end
    result["fs_write_ops_diff"] = fs_write_ops_diff
    result["nand_total_read_diff"] = nand_total_read_diff
    result["nand_total_write_diff"] = nand_total_write_diff
    if nand_total_read_diff is not None:
        result["nand_read_bytes"] = nand_total_read_diff * NAND_PAGE_BYTES
    if nand_total_write_diff is not None:
        result["nand_write_bytes"] = nand_total_write_diff * NAND_PAGE_BYTES
    result["nand_counter_source"] = nand_counter_source
    if fs_read_ops_diff is None or fs_write_ops_diff is None:
        result["warnings"].append("stat filesystem op counts missing")
    if nand_total_read_diff is None:
        result["warnings"].append("stat nand total read diff missing")
    if nand_total_write_diff is None:
        result["warnings"].append("stat nand total write diff missing")

    return result


def item_to_single_row(
    item: dict[str, Any],
    io: dict[str, Any],
    stat: dict[str, Any],
) -> dict[str, Any]:
    file_name = Path(item["file"]).name
    round_idx = parse_round_from_name(Path(item["file"]))
    round_group = build_round_group_key(Path(item["file"]))

    row: dict[str, Any] = {
        "file": file_name,
        "round": round_idx,
        "round_group": round_group,
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
        "fs_read_ops": stat["fs_read_ops_diff"],
        "fs_write_ops": stat["fs_write_ops_diff"],
        "fs_read_bytes": io["fs_read_bytes_diff"],
        "fs_write_bytes": io["fs_write_bytes_diff"],
        "nand_read_ops": stat["nand_total_read_diff"],
        "nand_read_bytes": stat["nand_read_bytes"],
        "nand_write_ops": stat["nand_total_write_diff"],
        "nand_write_bytes": stat["nand_write_bytes"],
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
    warnings.extend(stat["warnings"])
    row["warnings"] = " | ".join(warnings)
    return row


def write_csv(rows: list[dict[str, Any]], output: Path | None) -> None:
    if not rows:
        return
    # Preserve first-row column order, then append any extra keys found later.
    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for row in rows[1:]:
        for k in row.keys():
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)
    if output:
        with output.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
    else:
        w = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def t_critical_95(df: int) -> float:
    if df <= 0:
        return 0.0
    if df in T_CRITICAL_95:
        return T_CRITICAL_95[df]
    if df <= 40:
        return 2.021
    if df <= 60:
        return 2.000
    if df <= 120:
        return 1.980
    return 1.960


def round_groups_to_merged_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(row.get("round_group") or "")
        if key == "":
            key = str(row.get("file") or "")
        grouped.setdefault(key, []).append(row)

    merged_rows: list[dict[str, Any]] = []
    for key in sorted(grouped.keys()):
        group_rows = grouped[key]
        if not group_rows:
            continue

        rounds = sorted(
            {
                int(r["round"])
                for r in group_rows
                if r.get("round") is not None and str(r.get("round")).isdigit()
            }
        )
        n = len(group_rows)

        merged: dict[str, Any] = {
            "round_group": key,
            "backend": group_rows[0].get("backend"),
            "sample_count": n,
            "rounds": "|".join(str(x) for x in rounds),
            "files": "|".join(str(r.get("file", "")) for r in group_rows),
        }

        candidate_columns = [
            c
            for c in group_rows[0].keys()
            if c not in {"file", "warnings", "backend", "round", "round_group"}
        ]

        for col in candidate_columns:
            vals = [to_float(r.get(col)) for r in group_rows]
            vals = [v for v in vals if v is not None]
            if not vals:
                continue

            mean_val = sum(vals) / len(vals)
            merged[f"{col}_mean"] = mean_val

            if len(vals) >= 2:
                ss = sum((v - mean_val) ** 2 for v in vals)
                sample_var = ss / (len(vals) - 1)
                sem = math.sqrt(sample_var) / math.sqrt(len(vals))
                delta = t_critical_95(len(vals) - 1) * sem
                merged[f"{col}_ci95_low"] = mean_val - delta
                merged[f"{col}_ci95_high"] = mean_val + delta
            else:
                merged[f"{col}_ci95_low"] = None
                merged[f"{col}_ci95_high"] = None

        merged_warnings = [str(r.get("warnings", "")).strip() for r in group_rows]
        merged_warnings = [w for w in merged_warnings if w]
        merged["warnings"] = " | ".join(merged_warnings)
        merged_rows.append(merged)

    return merged_rows


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Extract replay metrics to one-row-per-log CSV")
    ap.add_argument("files", nargs="+", help="log file path(s)")
    ap.add_argument("--format", choices=["csv"], default="csv", help=argparse.SUPPRESS)
    ap.add_argument("--output", help="write output to file instead of stdout")
    ap.add_argument(
        "--merged-output",
        help="write merged multi-round stats CSV (mean + 95%% t-CI)",
    )
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
        stat_metrics = parse_stat_log(find_stat_log(p))
        rows.append(item_to_single_row(item, io_metrics, stat_metrics))

    write_csv(rows, out_path)

    merged_rows = round_groups_to_merged_rows(rows)
    merged_path: Path | None = None
    if args.merged_output:
        merged_path = Path(args.merged_output)
    elif out_path is not None:
        suffix = out_path.suffix if out_path.suffix else ".csv"
        merged_path = out_path.with_name(f"{out_path.stem}_merged{suffix}")

    if merged_path is not None and merged_rows:
        write_csv(merged_rows, merged_path)
    elif out_path is None and merged_rows:
        print(
            "[replay_metrics] merged multi-round stats available; use --merged-output to export them.",
            file=sys.stderr,
        )

    # Do not fail on mixed log sets that include non-replay logs.
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
