#!/usr/bin/env python3
"""Summarize Celestia state-sync run homes for TreeDB readiness gates."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


FATAL_PATTERNS = [
    "valuelog: corrupt record",
    "state sync failed",
    "state sync aborted",
    "failed to restore snapshot",
    "IAVL node import failed",
    "IAVL commit failed",
    "panic:",
    "fatal error",
]


def parse_key_value_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line == "---" or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return default
        return int(value)
    try:
        raw = str(value).strip()
        if not raw:
            return default
        return int(raw)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    try:
        raw = str(value).strip()
        if not raw:
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def human_bytes(value: Any) -> str:
    n = float(safe_int(value, 0))
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while abs(n) >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.2f} {units[idx]}"


def human_seconds(value: Any) -> str:
    seconds = safe_float(value, 0.0)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60.0
    if minutes < 60:
        return f"{minutes:.2f}m"
    return f"{minutes / 60.0:.2f}h"


def backend_from_home(home: Path) -> str:
    match = re.match(r"\.celestia-app-mainnet-(.+)-[0-9]{14}$", home.name)
    if match:
        return match.group(1)
    return ""


def parse_disk_breakdown(path: Path, app_db: Path) -> dict[str, int]:
    if not path.is_file():
        return {}
    values: dict[str, int] = {}
    in_du_bytes = False
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if line == "du_bytes:":
            in_du_bytes = True
            continue
        if line.endswith(":") and line != "du_bytes:":
            in_du_bytes = False
        if not in_du_bytes:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            continue
        size_raw, path_raw = parts
        size = safe_int(size_raw, -1)
        if size < 0:
            continue
        item = Path(path_raw)
        try:
            rel = item.relative_to(app_db)
            key = str(rel)
        except ValueError:
            key = "application.db" if item == app_db else item.name
        values[key] = size
    return values


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def summarize_dwell_samples(dwell_dir: Path) -> dict[str, Any]:
    sample_paths = sorted(dwell_dir.glob("sample_*.json"))
    samples = [load_json(path) for path in sample_paths]
    samples = [sample for sample in samples if isinstance(sample, dict)]
    if not samples:
        return {"sample_count": 0}

    def max_field(name: str) -> int:
        return max(safe_int(sample.get(name), 0) for sample in samples)

    first = samples[0]
    last = samples[-1]
    return {
        "sample_count": len(samples),
        "first_timestamp": first.get("timestamp", ""),
        "last_timestamp": last.get("timestamp", ""),
        "first": first,
        "last": last,
        "max_vmrss_kb": max_field("vmrss_kb"),
        "max_vmhwm_kb": max_field("vmhwm_kb"),
        "last_home_apparent_bytes": safe_int(last.get("home_apparent_bytes"), 0),
        "last_home_physical_bytes": safe_int(last.get("home_physical_bytes"), 0),
        "last_app_db_apparent_bytes": safe_int(last.get("app_db_apparent_bytes"), 0),
        "last_app_db_physical_bytes": safe_int(last.get("app_db_physical_bytes"), 0),
        "last_maindb_apparent_bytes": safe_int(last.get("maindb_apparent_bytes"), 0),
        "last_wal_apparent_bytes": safe_int(last.get("wal_apparent_bytes"), 0),
    }


def count_fatal_matches(node_log: Path) -> dict[str, Any]:
    if not node_log.is_file():
        return {"count": 0, "matches": []}
    matches: list[dict[str, str]] = []
    for line_no, raw in enumerate(node_log.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        lowered = raw.lower()
        for pattern in FATAL_PATTERNS:
            if pattern.lower() in lowered:
                matches.append({"line": line_no, "pattern": pattern, "text": raw.strip()[:500]})
                break
    return {"count": len(matches), "matches": matches[:20]}


def summarize_home(home: Path) -> dict[str, Any]:
    home = home.expanduser().resolve()
    sync_dir = home / "sync"
    time_log = sync_dir / "sync-time.log"
    sync = parse_key_value_file(time_log)
    app_db = home / "data" / "application.db"
    disk = parse_disk_breakdown(sync_dir / "disk-breakdown.log", app_db)
    dwell = summarize_dwell_samples(sync_dir / "dwell-stats")
    fatal = count_fatal_matches(sync_dir / "node.log")

    db_backend = sync.get("db_backend") or backend_from_home(home)
    app_db_backend = sync.get("app_db_backend") or db_backend
    trust_height = safe_int(sync.get("trust_height"), 0)
    final_local_height = safe_int(sync.get("final_local_height"), 0)
    blocks_synced = max(final_local_height - trust_height, 0) if trust_height and final_local_height else 0
    sync_seconds = safe_int(sync.get("sync_duration_seconds", sync.get("duration_seconds")), 0)
    total_seconds = safe_int(sync.get("total_duration_seconds", sync.get("duration_seconds")), sync_seconds)
    end_app_bytes = safe_int(sync.get("end_app_bytes"), disk.get("application.db", 0))
    end_home_bytes = safe_int(sync.get("end_home_bytes"), 0)
    end_data_bytes = safe_int(sync.get("end_data_bytes"), 0)
    max_rss_kb = max(safe_int(sync.get("max_rss_kb"), 0), safe_int(dwell.get("max_vmrss_kb"), 0))
    max_hwm_kb = max(safe_int(sync.get("max_hwm_kb"), 0), safe_int(dwell.get("max_vmhwm_kb"), 0))

    return {
        "home": str(home),
        "db_backend": db_backend,
        "app_db_backend": app_db_backend,
        "trust_height": trust_height,
        "trust_hash": sync.get("trust_hash", ""),
        "final_local_height": final_local_height,
        "final_remote_height": safe_int(sync.get("final_remote_height"), 0),
        "final_remote_height_actual": safe_int(sync.get("final_remote_height_actual"), 0),
        "blocks_synced": blocks_synced,
        "sync_duration_seconds": sync_seconds,
        "total_duration_seconds": total_seconds,
        "post_sync_dwell_elapsed_seconds": safe_int(sync.get("post_sync_dwell_elapsed_seconds"), 0),
        "max_rss_kb": max_rss_kb,
        "max_hwm_kb": max_hwm_kb,
        "heap_capture_count": safe_int(sync.get("heap_capture_count"), 0),
        "end_home_bytes": end_home_bytes,
        "end_data_bytes": end_data_bytes,
        "end_app_bytes": end_app_bytes,
        "end_blockstore_bytes": safe_int(sync.get("end_blockstore_bytes"), 0),
        "app_bytes_per_block": (end_app_bytes / blocks_synced) if blocks_synced else 0.0,
        "sync_seconds_per_block": (sync_seconds / blocks_synced) if blocks_synced else 0.0,
        "disk_breakdown_bytes": disk,
        "dwell": dwell,
        "fatal_log_matches": fatal,
        "time_log": str(time_log) if time_log.exists() else "",
        "node_log": str(sync_dir / "node.log") if (sync_dir / "node.log").exists() else "",
        "disk_breakdown_log": str(sync_dir / "disk-breakdown.log") if (sync_dir / "disk-breakdown.log").exists() else "",
    }


def choose_baseline_and_candidate(runs: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    baseline_names = {"goleveldb", "leveldb"}
    baseline = next((run for run in runs if run.get("db_backend") in baseline_names), None)
    candidate = next((run for run in runs if run.get("db_backend") == "treedb"), None)
    if baseline is not None and candidate is not None:
        return baseline, candidate
    if len(runs) >= 2:
        return runs[0], runs[1]
    return None, None


def diff_metrics(runs: list[dict[str, Any]]) -> dict[str, Any]:
    baseline, candidate = choose_baseline_and_candidate(runs)
    if baseline is None or candidate is None:
        return {}

    def delta(field: str) -> float:
        return safe_float(candidate.get(field), 0.0) - safe_float(baseline.get(field), 0.0)

    def ratio(field: str) -> float:
        base = safe_float(baseline.get(field), 0.0)
        if base == 0:
            return 0.0
        return safe_float(candidate.get(field), 0.0) / base

    fields = [
        "sync_duration_seconds",
        "total_duration_seconds",
        "max_rss_kb",
        "max_hwm_kb",
        "end_home_bytes",
        "end_data_bytes",
        "end_app_bytes",
        "blocks_synced",
        "app_bytes_per_block",
        "sync_seconds_per_block",
    ]
    return {
        "baseline_home": baseline.get("home", ""),
        "baseline_backend": baseline.get("db_backend", ""),
        "candidate_home": candidate.get("home", ""),
        "candidate_backend": candidate.get("db_backend", ""),
        "deltas": {field: delta(field) for field in fields},
        "ratios": {field: ratio(field) for field in fields},
    }


def markdown_table(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    out: list[str] = []
    for idx, row in enumerate(rows):
        out.append("| " + " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) + " |")
        if idx == 0:
            out.append("| " + " | ".join("---" for _ in row) + " |")
    return "\n".join(out)


def render_markdown(payload: dict[str, Any]) -> str:
    runs = payload["runs"]
    lines = ["# Celestia Sync Run Summary", ""]
    rows = [[
        "backend",
        "app backend",
        "height",
        "blocks",
        "sync",
        "total",
        "dwell",
        "max RSS",
        "max HWM",
        "app bytes",
        "fatal",
        "home",
    ]]
    for run in runs:
        rows.append([
            str(run.get("db_backend", "")),
            str(run.get("app_db_backend", "")),
            str(run.get("final_local_height", 0)),
            str(run.get("blocks_synced", 0)),
            human_seconds(run.get("sync_duration_seconds", 0)),
            human_seconds(run.get("total_duration_seconds", 0)),
            human_seconds(run.get("post_sync_dwell_elapsed_seconds", 0)),
            human_bytes(safe_int(run.get("max_rss_kb"), 0) * 1024),
            human_bytes(safe_int(run.get("max_hwm_kb"), 0) * 1024),
            human_bytes(run.get("end_app_bytes", 0)),
            str((run.get("fatal_log_matches") or {}).get("count", 0)),
            str(run.get("home", "")),
        ])
    lines.append(markdown_table(rows))

    comparison = payload.get("comparison") or {}
    if comparison:
        lines.extend(["", "## Comparison", ""])
        delta_rows = [["metric", "delta", "ratio"]]
        deltas = comparison.get("deltas", {})
        ratios = comparison.get("ratios", {})
        for field in [
            "sync_duration_seconds",
            "total_duration_seconds",
            "max_rss_kb",
            "max_hwm_kb",
            "end_app_bytes",
            "app_bytes_per_block",
            "sync_seconds_per_block",
        ]:
            raw_delta = deltas.get(field, 0)
            if field.endswith("_bytes") or field == "app_bytes_per_block":
                delta_text = human_bytes(raw_delta)
            elif field.endswith("_kb"):
                delta_text = human_bytes(raw_delta * 1024)
            elif field.endswith("_seconds") or field == "sync_seconds_per_block":
                delta_text = human_seconds(raw_delta)
            else:
                delta_text = f"{raw_delta:.3f}"
            ratio = safe_float(ratios.get(field), 0.0)
            delta_rows.append([field, delta_text, f"{ratio:.3f}x" if ratio else "n/a"])
        lines.append(markdown_table(delta_rows))

    lines.extend(["", "## Artifacts", ""])
    for run in runs:
        lines.append(f"- `{run.get('db_backend')}` home: `{run.get('home')}`")
        for key in ["time_log", "node_log", "disk_breakdown_log"]:
            if run.get(key):
                lines.append(f"  - {key}: `{run[key]}`")
    return "\n".join(lines) + "\n"


def build_payload(homes: list[Path]) -> dict[str, Any]:
    runs = [summarize_home(home) for home in homes]
    return {
        "schema": "celestia_sync_run_summary.v1",
        "runs": runs,
        "comparison": diff_metrics(runs),
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("homes", nargs="+", type=Path, help="Celestia run home directories")
    parser.add_argument("--out-dir", type=Path, default=None, help="write summary JSON and Markdown to this directory")
    parser.add_argument("--json-name", default="celestia_sync_runs.json")
    parser.add_argument("--md-name", default="celestia_sync_runs.md")
    args = parser.parse_args(argv)

    missing = [str(home) for home in args.homes if not home.expanduser().is_dir()]
    if missing:
        for item in missing:
            print(f"run home does not exist: {item}", file=sys.stderr)
        return 2

    payload = build_payload(args.homes)
    markdown = render_markdown(payload)

    if args.out_dir is not None:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        (args.out_dir / args.json_name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (args.out_dir / args.md_name).write_text(markdown, encoding="utf-8")
        print(args.out_dir / args.md_name)
    else:
        print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
