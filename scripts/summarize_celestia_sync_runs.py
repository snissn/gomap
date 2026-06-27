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

from analyze_vlog_maintenance_capacity import build_summary as build_vlog_maintenance_summary
from analyze_vlog_maintenance_capacity import extract_stats as extract_treedb_stats
from analyze_vlog_maintenance_capacity import find_diagnostics_file


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

TREEDB_INSTANCE_PATTERN = "application.db"
TREEDB_MAINTENANCE_COUNTERS = [
    "maintenance_attempts",
    "maintenance_acquired",
    "maintenance_collisions",
    "maintenance_with_rewrite",
    "maintenance_with_gc",
    "maintenance_noop",
    "gc_runs",
    "gc_deleted_bytes",
    "gc_deleted_segments",
    "gc_last_eligible_bytes",
    "gc_last_pending_bytes",
    "gc_last_protected_retained_bytes",
    "retained_prune_runs",
    "vlog_retained_segments",
    "vlog_retained_bytes_estimate",
    "retained_prune_closed_bytes",
    "retained_prune_candidate_bytes",
    "retained_prune_removed_bytes",
    "retained_prune_removed_segments",
    "retained_prune_live_skipped_bytes",
    "retained_prune_in_use_skipped_bytes",
    "vlog_zombie_bytes",
    "vlog_zombie_segments",
    "rewrite_runs",
    "rewrite_reclaimed_bytes",
    "rewrite_processed_stale_bytes",
    "rewrite_queue_len",
    "rewrite_queue_progress_segments_net_drain_total",
    "rewrite_queue_progress_live_bytes_net_drain_total",
    "checkpoint_kick_runs",
    "checkpoint_kick_gc_runs",
    "checkpoint_kick_rewrite_runs",
    "checkpoint_kick_skipped_hot_no_debt",
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
        if item == app_db:
            key = "application.db"
        else:
            try:
                rel = item.relative_to(app_db)
                key = "application.db" if str(rel) == "." else str(rel)
            except ValueError:
                key = item.name
        values[key] = size
    return values


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def summarize_dwell_samples(dwell_dir: Path) -> dict[str, Any]:
    def sample_sort_key(path: Path) -> tuple[int, str]:
        match = re.fullmatch(r"sample_([0-9]+)\.json", path.name)
        if match:
            return (safe_int(match.group(1)), path.name)
        return (sys.maxsize, path.name)

    sample_paths = sorted(dwell_dir.glob("sample_*.json"), key=sample_sort_key)
    samples = [load_json(path) for path in sample_paths]
    samples = [sample for sample in samples if isinstance(sample, dict)]
    if not samples:
        return {"sample_count": 0}

    def max_field(name: str) -> int:
        return max(safe_int(sample.get(name), 0) for sample in samples)

    def min_field(name: str) -> int:
        return min(safe_int(sample.get(name), 0) for sample in samples)

    first = samples[0]
    last = samples[-1]
    first_app_db_apparent = safe_int(first.get("app_db_apparent_bytes"), 0)
    last_app_db_apparent = safe_int(last.get("app_db_apparent_bytes"), 0)
    max_app_db_apparent = max_field("app_db_apparent_bytes")
    min_app_db_apparent = min_field("app_db_apparent_bytes")
    first_app_db_physical = safe_int(first.get("app_db_physical_bytes"), 0)
    last_app_db_physical = safe_int(last.get("app_db_physical_bytes"), 0)
    max_app_db_physical = max_field("app_db_physical_bytes")
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
        "first_app_db_apparent_bytes": first_app_db_apparent,
        "last_app_db_apparent_bytes": last_app_db_apparent,
        "max_app_db_apparent_bytes": max_app_db_apparent,
        "min_app_db_apparent_bytes": min_app_db_apparent,
        "app_db_apparent_delta_bytes": last_app_db_apparent - first_app_db_apparent,
        "app_db_apparent_shrink_from_first_bytes": max(0, first_app_db_apparent - last_app_db_apparent),
        "app_db_apparent_shrink_from_peak_bytes": max(0, max_app_db_apparent - last_app_db_apparent),
        "first_app_db_physical_bytes": first_app_db_physical,
        "last_app_db_physical_bytes": last_app_db_physical,
        "max_app_db_physical_bytes": max_app_db_physical,
        "app_db_physical_delta_bytes": last_app_db_physical - first_app_db_physical,
        "app_db_physical_shrink_from_peak_bytes": max(0, max_app_db_physical - last_app_db_physical),
        "last_maindb_apparent_bytes": safe_int(last.get("maindb_apparent_bytes"), 0),
        "last_wal_apparent_bytes": safe_int(last.get("wal_apparent_bytes"), 0),
    }


def count_fatal_matches(node_log: Path) -> dict[str, Any]:
    if not node_log.is_file():
        return {"count": 0, "matches": []}
    matches: list[dict[str, str]] = []
    count = 0
    with node_log.open(encoding="utf-8", errors="replace") as handle:
        for line_no, raw in enumerate(handle, 1):
            lowered = raw.lower()
            for pattern in FATAL_PATTERNS:
                if pattern.lower() in lowered:
                    count += 1
                    if len(matches) < 20:
                        matches.append({"line": line_no, "pattern": pattern, "text": raw.strip()[:500]})
                    break
    return {"count": count, "matches": matches}


def summarize_treedb_maintenance(home: Path) -> dict[str, Any]:
    try:
        source = find_diagnostics_file(home)
    except OSError as exc:
        return {"available": False, "reason": f"diagnostics_error:{exc}"}
    if source is None:
        return {"available": False, "reason": "diagnostics_not_found"}

    payload = load_json(source)
    if not isinstance(payload, dict):
        return {
            "available": False,
            "reason": "diagnostics_json_invalid",
            "source_file": str(source),
        }

    stats, instance_name = extract_treedb_stats(payload, TREEDB_INSTANCE_PATTERN)
    if not stats:
        return {
            "available": False,
            "reason": "treedb_app_stats_not_found",
            "source_file": str(source),
        }

    summary = build_vlog_maintenance_summary(stats)
    counters = {key: summary.get(key, 0) for key in TREEDB_MAINTENANCE_COUNTERS}
    return {
        "available": True,
        "source_file": str(source),
        "instance": instance_name,
        "raw_stat_count": len(stats),
        "counters": counters,
    }


def summarize_treedb_disk_reclaim(dwell: dict[str, Any], maintenance: dict[str, Any]) -> dict[str, Any]:
    counters = maintenance.get("counters") if isinstance(maintenance, dict) else {}
    if not isinstance(counters, dict):
        counters = {}
    app_db_shrink = safe_int(dwell.get("app_db_apparent_shrink_from_peak_bytes"), 0)
    physical_shrink = safe_int(dwell.get("app_db_physical_shrink_from_peak_bytes"), 0)
    return {
        "dwell_app_db_apparent_peak_bytes": safe_int(dwell.get("max_app_db_apparent_bytes"), 0),
        "dwell_app_db_apparent_last_bytes": safe_int(dwell.get("last_app_db_apparent_bytes"), 0),
        "dwell_app_db_apparent_shrink_from_peak_bytes": app_db_shrink,
        "dwell_app_db_physical_peak_bytes": safe_int(dwell.get("max_app_db_physical_bytes"), 0),
        "dwell_app_db_physical_last_bytes": safe_int(dwell.get("last_app_db_physical_bytes"), 0),
        "dwell_app_db_physical_shrink_from_peak_bytes": physical_shrink,
        "gc_deleted_bytes": safe_int(counters.get("gc_deleted_bytes"), 0),
        "gc_deleted_segments": safe_int(counters.get("gc_deleted_segments"), 0),
        "retained_prune_closed_bytes": safe_int(counters.get("retained_prune_closed_bytes"), 0),
        "vlog_retained_bytes_estimate": safe_int(counters.get("vlog_retained_bytes_estimate"), 0),
        "vlog_retained_segments": safe_int(counters.get("vlog_retained_segments"), 0),
        "retained_prune_removed_bytes": safe_int(counters.get("retained_prune_removed_bytes"), 0),
        "retained_prune_removed_segments": safe_int(counters.get("retained_prune_removed_segments"), 0),
        "rewrite_reclaimed_bytes": safe_int(counters.get("rewrite_reclaimed_bytes"), 0),
        "vlog_zombie_bytes": safe_int(counters.get("vlog_zombie_bytes"), 0),
        "named_delete_or_remove_bytes": safe_int(counters.get("gc_deleted_bytes"), 0)
        + safe_int(counters.get("retained_prune_removed_bytes"), 0)
        + safe_int(counters.get("rewrite_reclaimed_bytes"), 0),
    }


def summarize_home(home: Path) -> dict[str, Any]:
    home = home.expanduser().resolve()
    sync_dir = home / "sync"
    time_log = sync_dir / "sync-time.log"
    sync = parse_key_value_file(time_log)
    app_db = home / "data" / "application.db"
    disk = parse_disk_breakdown(sync_dir / "disk-breakdown.log", app_db)
    dwell = summarize_dwell_samples(sync_dir / "dwell-stats")
    fatal = count_fatal_matches(sync_dir / "node.log")
    treedb_maintenance = summarize_treedb_maintenance(home)
    treedb_disk_reclaim = summarize_treedb_disk_reclaim(dwell, treedb_maintenance)

    db_backend = sync.get("db_backend") or backend_from_home(home)
    app_db_backend = sync.get("app_db_backend") or db_backend
    trust_height = safe_int(sync.get("trust_height"), 0)
    final_local_height = safe_int(sync.get("final_local_height"), 0)
    blocks_synced = max(final_local_height - trust_height, 0) if trust_height and final_local_height else 0
    sync_seconds = safe_int(sync.get("sync_duration_seconds", sync.get("duration_seconds")), 0)
    total_seconds = safe_int(sync.get("total_duration_seconds", sync.get("duration_seconds")), sync_seconds)
    disk_app_bytes = disk.get("application.db", 0)
    end_app_bytes = safe_int(sync.get("end_app_bytes"), disk_app_bytes)
    if end_app_bytes == 0 and disk_app_bytes > 0:
        end_app_bytes = disk_app_bytes
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
        "treedb_maintenance": treedb_maintenance,
        "treedb_disk_reclaim": treedb_disk_reclaim,
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

    window_fields = ["trust_height", "trust_hash", "final_local_height", "blocks_synced"]
    missing_fields = []
    for side, run in [("baseline", baseline), ("candidate", candidate)]:
        for field in window_fields:
            value = run.get(field, "")
            if value == "" or value == 0:
                missing_fields.append({"side": side, "field": field})
    if missing_fields:
        return {
            "valid": False,
            "reason": "missing_run_window_evidence",
            "baseline_home": baseline.get("home", ""),
            "baseline_backend": baseline.get("db_backend", ""),
            "candidate_home": candidate.get("home", ""),
            "candidate_backend": candidate.get("db_backend", ""),
            "missing_fields": missing_fields,
        }

    mismatches = [
        {
            "field": field,
            "baseline": baseline.get(field, ""),
            "candidate": candidate.get(field, ""),
        }
        for field in window_fields
        if baseline.get(field, "") != candidate.get(field, "")
    ]
    if mismatches:
        return {
            "valid": False,
            "reason": "mismatched_run_window",
            "baseline_home": baseline.get("home", ""),
            "baseline_backend": baseline.get("db_backend", ""),
            "candidate_home": candidate.get("home", ""),
            "candidate_backend": candidate.get("db_backend", ""),
            "mismatches": mismatches,
        }

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
        "valid": True,
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
        if comparison.get("valid") is False:
            lines.append(f"Comparison skipped: {comparison.get('reason', 'invalid_comparison')}.")
            if comparison.get("mismatches"):
                mismatch_rows = [["field", "baseline", "candidate"]]
                for mismatch in comparison.get("mismatches", []):
                    mismatch_rows.append([
                        str(mismatch.get("field", "")),
                        str(mismatch.get("baseline", "")),
                        str(mismatch.get("candidate", "")),
                    ])
                lines.extend(["", markdown_table(mismatch_rows)])
            if comparison.get("missing_fields"):
                missing_rows = [["side", "field"]]
                for missing in comparison.get("missing_fields", []):
                    missing_rows.append([str(missing.get("side", "")), str(missing.get("field", ""))])
                lines.extend(["", markdown_table(missing_rows)])
        else:
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

    maintenance_runs = [
        run
        for run in runs
        if isinstance(run.get("treedb_maintenance"), dict) and run["treedb_maintenance"].get("available")
    ]
    if maintenance_runs:
        lines.extend(["", "## TreeDB Maintenance", ""])
        maint_rows = [[
            "backend",
            "app shrink",
            "gc deleted",
            "retained closed",
            "retained estimate",
            "retained removed",
            "rewrite reclaimed",
            "maint",
            "gc runs",
            "checkpoint kick",
            "instance",
        ]]
        for run in maintenance_runs:
            maintenance = run.get("treedb_maintenance") or {}
            counters = maintenance.get("counters") or {}
            reclaim = run.get("treedb_disk_reclaim") or {}
            maint_rows.append([
                str(run.get("db_backend", "")),
                human_bytes(reclaim.get("dwell_app_db_apparent_shrink_from_peak_bytes", 0)),
                human_bytes(counters.get("gc_deleted_bytes", 0)),
                human_bytes(counters.get("retained_prune_closed_bytes", 0)),
                human_bytes(counters.get("vlog_retained_bytes_estimate", 0)),
                human_bytes(counters.get("retained_prune_removed_bytes", 0)),
                human_bytes(counters.get("rewrite_reclaimed_bytes", 0)),
                f"{counters.get('maintenance_acquired', 0)}/{counters.get('maintenance_attempts', 0)}",
                str(counters.get("gc_runs", 0)),
                (
                    f"{counters.get('checkpoint_kick_runs', 0)}"
                    f"/{counters.get('checkpoint_kick_gc_runs', 0)}"
                    f"/{counters.get('checkpoint_kick_rewrite_runs', 0)}"
                ),
                str(maintenance.get("instance", "")),
            ])
        lines.append(markdown_table(maint_rows))

    lines.extend(["", "## Artifacts", ""])
    for run in runs:
        lines.append(f"- `{run.get('db_backend')}` home: `{run.get('home')}`")
        maintenance = run.get("treedb_maintenance") or {}
        if maintenance.get("source_file"):
            lines.append(f"  - treedb_maintenance_source: `{maintenance['source_file']}`")
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
    missing_time_logs = [
        str(home.expanduser() / "sync" / "sync-time.log")
        for home in args.homes
        if not (home.expanduser() / "sync" / "sync-time.log").is_file()
    ]
    if missing_time_logs:
        for item in missing_time_logs:
            print(f"missing required sync-time.log: {item}", file=sys.stderr)
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
