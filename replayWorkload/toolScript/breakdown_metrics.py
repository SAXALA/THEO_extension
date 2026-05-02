#!/usr/bin/env python3
"""Extract BlockStore and PrefixDB breakdown metrics from replay logs.

Output is CSV only, one row per main replay log. For each main log, this script
extracts two breakdown groups from the log body:
- BlockStore get breakdown [...]
- PrefixDB TrieNodeStorage get breakdown [...]
- Theo TrieNodeStorage get breakdown [...]

Each breakdown line contributes six values:
- cacheCount
- cacheTotal
- cacheAvg
- noCacheCount
- noCacheTotal
- noCacheAvg

The script also derives per-step summary columns:
- total_count
- total_time_us
- step_cost_us
- cache_hit_ratio

Values are flattened into CSV columns using this scheme:
- block_store_<label>_<metric>
- state_store_<label>_<metric>

Duration totals and averages are normalized to microseconds for easier analysis.
Labels are sanitized to snake_case so they can be used as stable CSV headers.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path
from typing import Any

BACKEND_RE = re.compile(r"^BACKEND=(.+)$")
BREAKDOWN_RE = re.compile(
    r"^(BlockStore get breakdown|PrefixDB TrieNodeStorage get breakdown|Theo TrieNodeStorage get breakdown) "
    r"\[([^\]]+)\]:\s+"
    r"cacheCount=(\d+)\s+cacheTotal=([^\s]+)\s+cacheAvg=([^\s]+)\s+"
    r"noCacheCount=(\d+)\s+noCacheTotal=([^\s]+)\s+noCacheAvg=([^\s]+)\s*$"
)
ROUND_TOKEN_RE = re.compile(r"(?:^|_)r_(\d+)(?:_|$)")
TRAILING_TIMESTAMP_RE = re.compile(r"_\d{2}-\d{2}-\d{2}-\d{2}-\d{2}$")
GO_DURATION_PART_RE = re.compile(r"([+-]?[0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h)")


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


def parse_duration_to_us(token: str) -> float:
    text = token.strip()
    if text == "0" or text == "0s":
        return 0.0

    pos = 0
    total_us = 0.0
    for match in GO_DURATION_PART_RE.finditer(text):
        if match.start() != pos:
            raise ValueError(f"unsupported duration token: {token}")
        value = float(match.group(1))
        unit = match.group(2)
        factor = {
            "ns": 1.0 / 1000.0,
            "us": 1.0,
            "µs": 1.0,
            "ms": 1000.0,
            "s": 1_000_000.0,
            "m": 60.0 * 1_000_000.0,
            "h": 3600.0 * 1_000_000.0,
        }[unit]
        total_us += value * factor
        pos = match.end()

    if pos != len(text):
        raise ValueError(f"unsupported duration token: {token}")

    return total_us


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


def normalize_main_log(path: Path) -> Path:
    base = path.name
    if "_io_" not in base:
        return path
    return path.with_name(base.replace("_io_", "_", 1))


def is_replay_log(path: Path) -> bool:
    return "_replay_" in path.name.lower()


def sanitize_label(label: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", label.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unknown"


def empty_result(path: Path) -> dict[str, Any]:
    return {
        "file": str(path),
        "backend": None,
        "block_store": {},
        "state_store": {},
        "warnings": [],
    }


def derive_breakdown_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    cache_count = int(metrics["cache_count"])
    no_cache_count = int(metrics["no_cache_count"])
    cache_total_us = float(metrics["cache_total_us"])
    no_cache_total_us = float(metrics["no_cache_total_us"])

    total_count = cache_count + no_cache_count
    total_time_us = cache_total_us + no_cache_total_us
    step_cost_us = total_time_us / total_count if total_count > 0 else None
    cache_hit_ratio = cache_count / total_count if total_count > 0 else None

    derived = dict(metrics)
    derived["total_count"] = total_count
    derived["total_time_us"] = total_time_us
    derived["step_cost_us"] = step_cost_us
    derived["cache_hit_ratio"] = cache_hit_ratio
    return derived


def parse_log(path: Path) -> dict[str, Any]:
    result = empty_result(path)

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except FileNotFoundError:
        result["warnings"].append("file not found")
        return result

    for line in lines:
        m = BACKEND_RE.match(line)
        if m:
            result["backend"] = m.group(1).strip()
            break

    for line in lines:
        m = BREAKDOWN_RE.match(line)
        if not m:
            continue

        source_name = m.group(1)
        raw_label = m.group(2)
        label = sanitize_label(raw_label)

        try:
            metrics = derive_breakdown_metrics({
                "label": raw_label,
                "cache_count": int(m.group(3)),
                "cache_total_us": parse_duration_to_us(m.group(4)),
                "cache_avg_us": parse_duration_to_us(m.group(5)),
                "no_cache_count": int(m.group(6)),
                "no_cache_total_us": parse_duration_to_us(m.group(7)),
                "no_cache_avg_us": parse_duration_to_us(m.group(8)),
            })
        except ValueError as exc:
            result["warnings"].append(str(exc))
            continue

        target_key = "block_store" if source_name.startswith("BlockStore") else "state_store"
        if label in result[target_key]:
            result["warnings"].append(f"duplicate {target_key} label: {raw_label}")
        result[target_key][label] = metrics

    if not result["block_store"]:
        result["warnings"].append("missing BlockStore breakdown lines")
    if not result["state_store"]:
        result["warnings"].append(
            "missing PrefixDB/Theo TrieNodeStorage breakdown lines"
        )

    return result


def item_to_single_row(item: dict[str, Any]) -> dict[str, Any]:
    file_path = Path(item["file"])
    row: dict[str, Any] = {
        "file": file_path.name,
        "round": parse_round_from_name(file_path),
        "round_group": build_round_group_key(file_path),
        "backend": item["backend"],
    }

    for prefix, source_key in (("block_store", "block_store"), ("state_store", "state_store")):
        labels = item[source_key]
        for label in sorted(labels.keys()):
            metrics = labels[label]
            base = f"{prefix}_{label}"
            row[f"{base}_label"] = metrics["label"]
            row[f"{base}_cache_count"] = metrics["cache_count"]
            row[f"{base}_cache_total_us"] = metrics["cache_total_us"]
            row[f"{base}_cache_avg_us"] = metrics["cache_avg_us"]
            row[f"{base}_no_cache_count"] = metrics["no_cache_count"]
            row[f"{base}_no_cache_total_us"] = metrics["no_cache_total_us"]
            row[f"{base}_no_cache_avg_us"] = metrics["no_cache_avg_us"]
            row[f"{base}_total_count"] = metrics["total_count"]
            row[f"{base}_total_time_us"] = metrics["total_time_us"]
            row[f"{base}_step_cost_us"] = metrics["step_cost_us"]
            row[f"{base}_cache_hit_ratio"] = metrics["cache_hit_ratio"]

    row["warnings"] = " | ".join(item["warnings"])
    return row


def write_csv(rows: list[dict[str, Any]], output: Path | None) -> None:
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    seen = set(fieldnames)
    for row in rows[1:]:
        for key in row.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    if output:
        with output.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)


def build_step_cost_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    step_cost_rows: list[dict[str, Any]] = []
    for row in rows:
        step_cost_row: dict[str, Any] = {
            "file": row.get("file"),
            "round": row.get("round"),
            "round_group": row.get("round_group"),
            "backend": row.get("backend"),
            "warnings": row.get("warnings"),
        }
        for key, value in row.items():
            if key.endswith("_step_cost_us"):
                step_cost_row[key] = value
        step_cost_rows.append(step_cost_row)
    return step_cost_rows


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
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
        merged: dict[str, Any] = {
            "round_group": key,
            "backend": group_rows[0].get("backend"),
            "sample_count": len(group_rows),
            "rounds": "|".join(str(x) for x in rounds),
            "files": "|".join(str(r.get("file", "")) for r in group_rows),
        }

        candidate_columns = [
            col
            for col in group_rows[0].keys()
            if col not in {"file", "warnings", "backend", "round", "round_group"}
            and not col.endswith("_label")
        ]

        for col in candidate_columns:
            values = [to_float(r.get(col)) for r in group_rows]
            values = [value for value in values if value is not None]
            if not values:
                continue

            mean_val = sum(values) / len(values)
            merged[f"{col}_mean"] = mean_val

            if len(values) >= 2:
                ss = sum((value - mean_val) ** 2 for value in values)
                sample_var = ss / (len(values) - 1)
                sem = math.sqrt(sample_var) / math.sqrt(len(values))
                delta = t_critical_95(len(values) - 1) * sem
                merged[f"{col}_ci95_low"] = mean_val - delta
                merged[f"{col}_ci95_high"] = mean_val + delta
            else:
                merged[f"{col}_ci95_low"] = None
                merged[f"{col}_ci95_high"] = None

        warning_values = [str(r.get("warnings", "")).strip() for r in group_rows]
        warning_values = [warning for warning in warning_values if warning]
        merged["warnings"] = " | ".join(warning_values)
        merged_rows.append(merged)

    return merged_rows


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Extract BlockStore and PrefixDB TrieNodeStorage breakdown metrics"
    )
    parser.add_argument("files", nargs="+", help="log file path(s)")
    parser.add_argument("--output", help="write output to file instead of stdout")
    parser.add_argument(
        "--step-cost-output",
        help="write step_cost_us-only CSV to file",
    )
    parser.add_argument(
        "--merged-output",
        help="write merged multi-round stats CSV (mean + 95%% t-CI)",
    )
    parser.add_argument(
        "--only-replay",
        action="store_true",
        help="only include replay logs (filename contains '_replay_')",
    )
    args = parser.parse_args(argv)

    main_logs: list[Path] = []
    seen: set[str] = set()
    for raw_path in args.files:
        normalized = normalize_main_log(Path(raw_path))
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        main_logs.append(normalized)

    if args.only_replay:
        main_logs = [path for path in main_logs if is_replay_log(path)]

    rows = [item_to_single_row(parse_log(path)) for path in main_logs]

    out_path = Path(args.output) if args.output else None
    write_csv(rows, out_path)

    step_cost_rows = build_step_cost_rows(rows)
    step_cost_path: Path | None = None
    if args.step_cost_output:
        step_cost_path = Path(args.step_cost_output)
    elif out_path is not None:
        suffix = out_path.suffix if out_path.suffix else ".csv"
        step_cost_path = out_path.with_name(f"{out_path.stem}_step_cost{suffix}")

    if step_cost_path is not None and step_cost_rows:
        write_csv(step_cost_rows, step_cost_path)

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
            "[breakdown_metrics] merged multi-round stats available; use --merged-output to export them.",
            file=sys.stderr,
        )

    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))