#!/usr/bin/env python3
"""Summarize live TreeDB vlog maintenance capacity from run_celestia diagnostics.

Input can be:
- a run home dir (e.g. ~/.celestia-app-mainnet-treedb-YYYY...)
- a diagnostics dir
- a debug vars JSON file

By default, the script scans the newest ~/.celestia-app-mainnet-treedb-* home.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from pathlib import Path
from typing import Any


def human_bytes(value: float) -> str:
    if value is None:
        return "n/a"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if math.isnan(n):
        return "n/a"
    if n < 0:
        return f"-{human_bytes(-n)}"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while n >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.2f} {units[idx]}"


def pct(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return 100.0 * num / den


def safe_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return default
        if s == "true":
            return 1
        if s == "false":
            return 0
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return default
    return default


def safe_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return default
        if s == "true":
            return 1.0
        if s == "false":
            return 0.0
        try:
            return float(s)
        except ValueError:
            return default
    return default


def pick_latest(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(paths, key=lambda p: p.stat().st_mtime)


def find_latest_home() -> Path | None:
    homes: list[Path] = []
    for raw in glob.glob(os.path.expanduser("~/.celestia-app-mainnet-treedb-*")):
        p = Path(raw)
        if p.is_dir():
            homes.append(p)
    return pick_latest(homes)


def find_diagnostics_file(root: Path) -> Path | None:
    roots: list[Path] = []
    if (root / "sync" / "diagnostics").is_dir():
        roots.append(root / "sync" / "diagnostics")
    if (root / "diagnostics").is_dir():
        roots.append(root / "diagnostics")
    if root.is_dir() and root.name == "diagnostics":
        roots.append(root)

    patterns = ["*.debug_vars.json", "*.treedb_vars.json", "*.treedb_application_vars.json"]

    # Prefer richer payload shapes in order. Ignore obviously empty snapshots.
    for pat in patterns:
        candidates: list[Path] = []
        for diag in roots:
            candidates.extend(diag.glob(pat))
        # If caller passed a file-like path prefix directory with JSON files only.
        if root.is_dir() and not roots:
            candidates.extend(root.glob(pat))
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for cand in candidates:
            # "{}\n" snapshots are not useful for maintenance analysis.
            if cand.stat().st_size <= 4:
                continue
            return cand

    # Fallback: if all snapshots are tiny/empty, still return the newest one.
    fallback: list[Path] = []
    for pat in patterns:
        for diag in roots:
            fallback.extend(diag.glob(pat))
        if root.is_dir() and not roots:
            fallback.extend(root.glob(pat))
    return pick_latest(fallback)


def find_home_from_path(path: Path) -> str:
    for parent in [path] + list(path.parents):
        name = parent.name
        if name.startswith(".celestia-app-mainnet-"):
            return str(parent)
    return ""


def instance_matches_pattern(name: str, stats: dict[str, Any], pattern: str) -> bool:
    if not pattern:
        return False
    if pattern in name:
        return True
    for key in ("treedb.expvar.wal_dir", "treedb.process.identity.wal_dir"):
        value = stats.get(key)
        if isinstance(value, str) and pattern in value:
            return True
    return False


def choose_instance(instances: dict[str, Any], pattern: str) -> tuple[str, dict[str, Any]]:
    if not instances:
        return "", {}

    if pattern:
        matches = [
            (k, v)
            for k, v in instances.items()
            if isinstance(v, dict) and instance_matches_pattern(k, v, pattern)
        ]
        if matches:
            # Prefer the richest stats object among matches.
            matches.sort(key=lambda kv: len(kv[1]), reverse=True)
            return matches[0][0], matches[0][1]

    scored: list[tuple[int, int, str, dict[str, Any]]] = []
    for k, v in instances.items():
        if not isinstance(v, dict):
            continue
        vg_count = sum(1 for key in v.keys() if str(key).startswith("treedb.cache.vlog_generation."))
        scored.append((vg_count, len(v), k, v))
    if scored:
        scored.sort(reverse=True)
        _, _, k, v = scored[0]
        return k, v

    first_key = sorted(instances.keys())[0]
    val = instances[first_key]
    if isinstance(val, dict):
        return first_key, val
    return first_key, {}


def extract_stats(payload: Any, instance_pattern: str) -> tuple[dict[str, Any], str]:
    if not isinstance(payload, dict):
        return {}, ""

    # Most complete shape from debug vars snapshots:
    # { "treedb": { "instances": { "...": { stats... } } } }
    treedb = payload.get("treedb")
    if isinstance(treedb, dict):
        instances = treedb.get("instances")
        if isinstance(instances, dict):
            instance_name, stats = choose_instance(instances, instance_pattern)
            return stats, instance_name

    # Flat stats map shape.
    if any(str(k).startswith("treedb.cache.") for k in payload.keys()):
        return payload, ""

    # Other possible shape: top-level instances.
    instances = payload.get("instances")
    if isinstance(instances, dict):
        instance_name, stats = choose_instance(instances, instance_pattern)
        return stats, instance_name

    return {}, ""


def metric_int(stats: dict[str, Any], key: str) -> int:
    return safe_int(stats.get(key, 0), 0)


def metric_float(stats: dict[str, Any], key: str) -> float:
    return safe_float(stats.get(key, 0.0), 0.0)


def metric_str(stats: dict[str, Any], key: str, default: str = "") -> str:
    raw = stats.get(key, default)
    if raw is None:
        return default
    return str(raw)


def build_summary(stats: dict[str, Any]) -> dict[str, Any]:
    m = {
        "maintenance_attempts": metric_int(stats, "treedb.cache.vlog_generation.maintenance.attempts"),
        "maintenance_acquired": metric_int(stats, "treedb.cache.vlog_generation.maintenance.acquired"),
        "maintenance_collisions": metric_int(stats, "treedb.cache.vlog_generation.maintenance.collisions"),
        "maintenance_acquired_source_periodic": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.periodic",
        ),
        "maintenance_acquired_source_bypass": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.bypass",
        ),
        "maintenance_acquired_source_checkpoint_pending": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.checkpoint_pending",
        ),
        "maintenance_acquired_source_rewrite_age_blocked": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_age_blocked",
        ),
        "maintenance_acquired_source_rewrite_stage_confirm": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.rewrite_stage_confirm",
        ),
        "maintenance_acquired_source_other": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.acquired.source.other",
        ),
        "maintenance_with_rewrite_source_periodic": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.periodic",
        ),
        "maintenance_with_rewrite_source_bypass": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.bypass",
        ),
        "maintenance_with_rewrite_source_checkpoint_pending": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.checkpoint_pending",
        ),
        "maintenance_with_rewrite_source_rewrite_age_blocked": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.rewrite_age_blocked",
        ),
        "maintenance_with_rewrite_source_rewrite_stage_confirm": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.rewrite_stage_confirm",
        ),
        "maintenance_with_rewrite_source_other": metric_int(
            stats,
            "treedb.cache.vlog_generation.maintenance.passes.with_rewrite.source.other",
        ),
        "maintenance_noop": metric_int(stats, "treedb.cache.vlog_generation.maintenance.passes.noop"),
        "maintenance_with_rewrite": metric_int(stats, "treedb.cache.vlog_generation.maintenance.passes.with_rewrite"),
        "maintenance_with_gc": metric_int(stats, "treedb.cache.vlog_generation.maintenance.passes.with_gc"),
        "rewrite_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs"),
        "rewrite_runs_source_periodic": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.periodic"),
        "rewrite_runs_source_bypass": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.bypass"),
        "rewrite_runs_source_checkpoint_pending": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.checkpoint_pending"),
        "rewrite_runs_source_rewrite_age_blocked": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.rewrite_age_blocked"),
        "rewrite_runs_source_rewrite_stage_confirm": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.rewrite_stage_confirm"),
        "rewrite_runs_source_other": metric_int(stats, "treedb.cache.vlog_generation.rewrite.runs.source.other"),
        "rewrite_plan_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_runs"),
        "rewrite_plan_selected": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_selected"),
        "rewrite_plan_empty": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_empty"),
        "rewrite_plan_empty_no_selection": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_empty.no_selection"),
        "rewrite_plan_empty_age_blocked": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_empty.age_blocked"),
        "rewrite_plan_selected_segments_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_selected_segments_total"),
        "rewrite_plan_penalty_filter_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_penalty_filter.runs"),
        "rewrite_plan_penalty_filter_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_penalty_filter.segments"),
        "rewrite_plan_penalty_filter_to_empty_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_penalty_filter.to_empty_runs"),
        "rewrite_exec_source_segments_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_total"),
        "rewrite_exec_source_segments_requested_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_requested_total"),
        "rewrite_exec_source_segments_still_referenced_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_still_referenced_total"),
        "rewrite_exec_source_segments_unreferenced_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_unreferenced_total"),
        "rewrite_exec_source_segments_requested_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_requested_last"),
        "rewrite_exec_source_segments_still_referenced_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_still_referenced_last"),
        "rewrite_exec_source_segments_unreferenced_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_segments_unreferenced_last"),
        "rewrite_exec_source_bytes_requested_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total"),
        "rewrite_exec_source_bytes_still_referenced_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_total"),
        "rewrite_exec_source_bytes_unreferenced_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total"),
        "rewrite_exec_source_bytes_requested_total_source_periodic": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.periodic"),
        "rewrite_exec_source_bytes_requested_total_source_bypass": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.bypass"),
        "rewrite_exec_source_bytes_requested_total_source_checkpoint_pending": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.checkpoint_pending"),
        "rewrite_exec_source_bytes_requested_total_source_rewrite_age_blocked": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.rewrite_age_blocked"),
        "rewrite_exec_source_bytes_requested_total_source_rewrite_stage_confirm": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.rewrite_stage_confirm"),
        "rewrite_exec_source_bytes_requested_total_source_other": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_total.source.other"),
        "rewrite_exec_source_bytes_unreferenced_total_source_periodic": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.periodic"),
        "rewrite_exec_source_bytes_unreferenced_total_source_bypass": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.bypass"),
        "rewrite_exec_source_bytes_unreferenced_total_source_checkpoint_pending": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.checkpoint_pending"),
        "rewrite_exec_source_bytes_unreferenced_total_source_rewrite_age_blocked": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.rewrite_age_blocked"),
        "rewrite_exec_source_bytes_unreferenced_total_source_rewrite_stage_confirm": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.rewrite_stage_confirm"),
        "rewrite_exec_source_bytes_unreferenced_total_source_other": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_total.source.other"),
        "rewrite_exec_source_bytes_requested_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_requested_last"),
        "rewrite_exec_source_bytes_still_referenced_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_still_referenced_last"),
        "rewrite_exec_source_bytes_unreferenced_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.exec.source_bytes_unreferenced_last"),
        "rewrite_plan_selected_bytes_stale": metric_int(stats, "treedb.cache.vlog_generation.rewrite.plan_selected_bytes_stale"),
        "rewrite_processed_stale_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.processed_stale_bytes"),
        "rewrite_processed_live_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.processed_live_bytes"),
        "rewrite_bytes_in": metric_int(stats, "treedb.cache.vlog_generation.rewrite.bytes_in"),
        "rewrite_bytes_out": metric_int(stats, "treedb.cache.vlog_generation.rewrite.bytes_out"),
        "rewrite_reclaimed_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.reclaimed_bytes"),
        "rewrite_no_reclaim_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.no_reclaim_runs"),
        "rewrite_exec_total_ms": metric_float(stats, "treedb.cache.vlog_generation.rewrite.exec.total_ms"),
        "rewrite_exec_avg_ms": metric_float(stats, "treedb.cache.vlog_generation.rewrite.exec.avg_ms"),
        "rewrite_ledger_bytes_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.ledger_bytes_total"),
        "rewrite_ledger_bytes_stale": metric_int(stats, "treedb.cache.vlog_generation.rewrite.ledger_bytes_stale"),
        "rewrite_ledger_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.ledger_segments"),
        "rewrite_age_blocked_remaining_ms": metric_int(stats, "treedb.cache.vlog_generation.rewrite.age_blocked_remaining_ms"),
        "rewrite_penalties_active": metric_int(stats, "treedb.cache.vlog_generation.rewrite.penalties_active"),
        "rewrite_budget_consumed_bytes_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total"),
        "rewrite_budget_consumed_bytes_total_source_periodic": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.periodic"),
        "rewrite_budget_consumed_bytes_total_source_bypass": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.bypass"),
        "rewrite_budget_consumed_bytes_total_source_checkpoint_pending": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.checkpoint_pending"),
        "rewrite_budget_consumed_bytes_total_source_rewrite_age_blocked": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.rewrite_age_blocked"),
        "rewrite_budget_consumed_bytes_total_source_rewrite_stage_confirm": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.rewrite_stage_confirm"),
        "rewrite_budget_consumed_bytes_total_source_other": metric_int(stats, "treedb.cache.vlog_generation.rewrite_budget.consumed_bytes_total.source.other"),
        "rewrite_budget_tokens_utilization_pct": metric_float(stats, "treedb.cache.vlog_generation.rewrite_budget.tokens_utilization_pct"),
        "rewrite_queue_run_segment_cap": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap"),
        "rewrite_queue_run_segment_cap_limiter": metric_str(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter", "none"),
        "rewrite_queue_run_segment_cap_by_budget": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget"),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes"),
        "rewrite_queue_run_segment_cap_checkpoint_kick": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.checkpoint_kick"),
        "rewrite_queue_run_segment_cap_limiter_checkpoint_kick": metric_str(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.checkpoint_kick", "none"),
        "rewrite_queue_run_segment_cap_by_budget_checkpoint_kick": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.checkpoint_kick"),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_checkpoint_kick": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.checkpoint_kick"),
        "rewrite_queue_run_segment_cap_fresh_plan": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.fresh_plan"),
        "rewrite_queue_run_segment_cap_limiter_fresh_plan": metric_str(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter.fresh_plan", "none"),
        "rewrite_queue_run_segment_cap_by_budget_fresh_plan": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.by_budget.fresh_plan"),
        "rewrite_queue_run_segment_cap_per_segment_budget_bytes_fresh_plan": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.per_segment_budget_bytes.fresh_plan"),
        "rewrite_queue_run_segment_cap_decisions": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions"),
        "rewrite_queue_run_segment_cap_decisions_fresh_plan": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.decisions.fresh_plan"),
        "rewrite_queue_run_segment_cap_limiter_count_budget_tokens": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.budget_tokens"),
        "rewrite_queue_run_segment_cap_limiter_count_debt_drain_cap": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.debt_drain_cap"),
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_safety": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_safety"),
        "rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.checkpoint_kick_burst"),
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_queue_threshold": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_queue_threshold.fresh_plan"),
        "rewrite_queue_run_segment_cap_limiter_count_fresh_plan_cap": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_run_segment_cap.limiter_count.fresh_plan_cap.fresh_plan"),
        "rewrite_queue_config_resume_max_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_config.resume_max_segments"),
        "rewrite_queue_config_debt_drain_max_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_config.debt_drain_max_segments"),
        "rewrite_queue_config_fresh_plan_debt_drain_min_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_min_segments"),
        "rewrite_queue_config_fresh_plan_debt_drain_max_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_config.fresh_plan_debt_drain_max_segments"),
        "rewrite_queued_debt_passes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.passes"),
        "rewrite_queued_debt_rewrite_started": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.rewrite_started"),
        "rewrite_queued_debt_skip_quiet_window": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.quiet_window"),
        "rewrite_queued_debt_skip_cancel_backoff": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.cancel_backoff"),
        "rewrite_queued_debt_skip_ineffective_backoff": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.ineffective_backoff"),
        "rewrite_queued_debt_skip_min_interval": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.min_interval"),
        "rewrite_queued_debt_skip_budget_empty": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.budget_empty"),
        "rewrite_queued_debt_skip_no_chunk": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.skip.no_chunk"),
        "rewrite_queued_debt_exec_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.runs"),
        "rewrite_queued_debt_exec_segments": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.segments"),
        "rewrite_queued_debt_exec_plan_bytes_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_total"),
        "rewrite_queued_debt_exec_plan_bytes_live": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_live"),
        "rewrite_queued_debt_exec_plan_bytes_stale": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.plan_bytes_stale"),
        "rewrite_queued_debt_exec_effective_bytes_before": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_before"),
        "rewrite_queued_debt_exec_effective_bytes_after": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.effective_bytes_after"),
        "rewrite_queued_debt_exec_gc_bytes_deleted": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.gc_bytes_deleted"),
        "rewrite_queued_debt_exec_reclaimed_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.reclaimed_bytes"),
        "rewrite_queued_debt_exec_no_reclaim_runs": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.no_reclaim_runs"),
        "rewrite_queued_debt_exec_source_bytes_requested": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.source_bytes_requested"),
        "rewrite_queued_debt_exec_source_bytes_unreferenced": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queued_debt.exec.source_bytes_unreferenced"),
        "rewrite_queue_live_hint_known": metric_str(stats, "treedb.cache.vlog_generation.rewrite.queue_live_hint.known", "false"),
        "rewrite_queue_live_hint_ids_present": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_present"),
        "rewrite_queue_live_hint_ids_known": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_live_hint.ids_known"),
        "rewrite_queue_live_hint_coverage_pct": metric_float(stats, "treedb.cache.vlog_generation.rewrite.queue_live_hint.coverage_pct"),
        "rewrite_queue_live_hint_bytes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_live_hint.bytes"),
        "rewrite_queue_progress_passes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.passes"),
        "rewrite_queue_progress_snapshot_errors": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.snapshot_errors"),
        "rewrite_queue_progress_segments_before_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_before_total"),
        "rewrite_queue_progress_segments_after_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_after_total"),
        "rewrite_queue_progress_segments_drained_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_drained_total"),
        "rewrite_queue_progress_segments_grown_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_grown_total"),
        "rewrite_queue_progress_segments_before_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_before_last"),
        "rewrite_queue_progress_segments_after_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_after_last"),
        "rewrite_queue_progress_segments_delta_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.segments_delta_last"),
        "rewrite_queue_progress_live_bytes_known_passes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_known_passes"),
        "rewrite_queue_progress_live_bytes_unknown_passes": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_unknown_passes"),
        "rewrite_queue_progress_live_bytes_before_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_before_total"),
        "rewrite_queue_progress_live_bytes_after_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_after_total"),
        "rewrite_queue_progress_live_bytes_drained_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_drained_total"),
        "rewrite_queue_progress_live_bytes_grown_total": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_grown_total"),
        "rewrite_queue_progress_live_bytes_before_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_before_last"),
        "rewrite_queue_progress_live_bytes_after_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_after_last"),
        "rewrite_queue_progress_live_bytes_delta_last": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_progress.live_bytes_delta_last"),
        "rewrite_queue_len": metric_int(stats, "treedb.cache.vlog_generation.rewrite.queue_len"),
        "rewrite_queue_live_bytes_after_tokens": metric_int(
            stats,
            "treedb.cache.vlog_generation.rewrite.queue_live_bytes_after_tokens",
        ),
        "rewrite_queue_eta_seconds_budget": metric_float(
            stats,
            "treedb.cache.vlog_generation.rewrite.queue_eta_seconds.budget",
        ),
        "rewrite_queue_eta_seconds_recent_exec": metric_float(
            stats,
            "treedb.cache.vlog_generation.rewrite.queue_eta_seconds.recent_exec",
        ),
        "rewrite_exec_last_live_bytes": metric_int(
            stats,
            "treedb.cache.vlog_generation.rewrite.exec.last_live_bytes",
        ),
        "rewrite_exec_last_duration_ms": metric_float(
            stats,
            "treedb.cache.vlog_generation.rewrite.exec.last_duration_ms",
        ),
        "rewrite_exec_last_live_bytes_per_sec": metric_float(
            stats,
            "treedb.cache.vlog_generation.rewrite.exec.last_live_bytes_per_sec",
        ),
        "gc_runs": metric_int(stats, "treedb.cache.vlog_generation.gc.runs"),
        "gc_deleted_bytes": metric_int(stats, "treedb.cache.vlog_generation.gc.deleted_bytes"),
        "gc_deleted_segments": metric_int(stats, "treedb.cache.vlog_generation.gc.deleted_segments"),
        "gc_exec_total_ms": metric_float(stats, "treedb.cache.vlog_generation.gc.exec.total_ms"),
        "gc_exec_avg_ms": metric_float(stats, "treedb.cache.vlog_generation.gc.exec.avg_ms"),
        "gc_last_eligible_bytes": metric_int(stats, "treedb.cache.vlog_generation.gc.last_eligible_bytes"),
        "gc_last_pending_bytes": metric_int(stats, "treedb.cache.vlog_generation.gc.last_pending_bytes"),
        "gc_last_protected_retained_bytes": metric_int(stats, "treedb.cache.vlog_generation.gc.last_protected_retained_bytes"),
        "leaf_pack_gc_runs": metric_int(stats, "treedb.cache.vlog_generation.leaf_pack.gc.runs"),
        "leaf_pack_gc_deleted_bytes": metric_int(stats, "treedb.cache.vlog_generation.leaf_pack.gc.deleted_bytes"),
        "leaf_pack_gc_deleted_files": metric_int(stats, "treedb.cache.vlog_generation.leaf_pack.gc.deleted_files"),
        "leaf_pack_gc_deleted_generations": metric_int(stats, "treedb.cache.vlog_generation.leaf_pack.gc.deleted_generations"),
        "leaf_pack_gc_eligible_generations": metric_int(stats, "treedb.cache.vlog_generation.leaf_pack.gc.eligible_generations"),
        "retained_prune_closed_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.closed_bytes"),
        "vlog_retained_segments": metric_int(stats, "treedb.cache.vlog_retained_segments"),
        "vlog_retained_bytes_estimate": metric_int(stats, "treedb.cache.vlog_retained_bytes_estimate"),
        "retained_prune_runs": metric_int(stats, "treedb.cache.vlog_retained_prune.runs"),
        "retained_prune_forced_runs": metric_int(stats, "treedb.cache.vlog_retained_prune.forced_runs"),
        "retained_prune_candidate_segments": metric_int(stats, "treedb.cache.vlog_retained_prune.candidate_segments"),
        "retained_prune_candidate_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.candidate_bytes"),
        "retained_prune_removed_segments": metric_int(stats, "treedb.cache.vlog_retained_prune.removed_segments"),
        "retained_prune_removed_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.removed_bytes"),
        "retained_prune_in_use_skipped_segments": metric_int(stats, "treedb.cache.vlog_retained_prune.in_use_skipped_segments"),
        "retained_prune_in_use_skipped_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.in_use_skipped_bytes"),
        "retained_prune_live_skipped_segments": metric_int(stats, "treedb.cache.vlog_retained_prune.live_skipped_segments"),
        "retained_prune_live_skipped_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.live_skipped_bytes"),
        "retained_prune_zombie_marked_segments": metric_int(stats, "treedb.cache.vlog_retained_prune.zombie_marked_segments"),
        "retained_prune_zombie_marked_bytes": metric_int(stats, "treedb.cache.vlog_retained_prune.zombie_marked_bytes"),
        "vlog_zombie_segments": metric_int(stats, "treedb.cache.vlog_zombie.segments"),
        "vlog_zombie_bytes": metric_int(stats, "treedb.cache.vlog_zombie.bytes"),
        "vlog_zombie_pinned_segments": metric_int(stats, "treedb.cache.vlog_zombie.pinned_segments"),
        "vlog_zombie_pinned_bytes": metric_int(stats, "treedb.cache.vlog_zombie.pinned_bytes"),
        "vlog_zombie_unpinned_segments": metric_int(stats, "treedb.cache.vlog_zombie.unpinned_segments"),
        "vlog_zombie_unpinned_bytes": metric_int(stats, "treedb.cache.vlog_zombie.unpinned_bytes"),
        "retained_prune_observed_source_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_total"),
        "retained_prune_observed_source_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_total"),
        "retained_prune_observed_source_candidate_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_candidate_total"),
        "retained_prune_observed_source_candidate_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_candidate_total"),
        "retained_prune_observed_source_removed_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_removed_total"),
        "retained_prune_observed_source_removed_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_removed_total"),
        "retained_prune_observed_source_in_use_skipped_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_in_use_skipped_total"),
        "retained_prune_observed_source_in_use_skipped_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_in_use_skipped_total"),
        "retained_prune_observed_source_live_skipped_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_live_skipped_total"),
        "retained_prune_observed_source_live_skipped_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_live_skipped_total"),
        "retained_prune_observed_source_parse_skipped_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_parse_skipped_total"),
        "retained_prune_observed_source_parse_skipped_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_parse_skipped_total"),
        "retained_prune_observed_source_zombie_marked_segments_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.segments_zombie_marked_total"),
        "retained_prune_observed_source_zombie_marked_bytes_total": metric_int(stats, "treedb.cache.vlog_retained_prune.observed_source.bytes_zombie_marked_total"),
        "observed_gc_pending_ids": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.pending_ids"),
        "observed_gc_queued_ids": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.queued_ids"),
        "observed_gc_taken_ids": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.taken_ids"),
        "observed_gc_runs": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.runs"),
        "observed_gc_retry_queued": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.retry_queued"),
        "observed_gc_retry_dropped": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.retry_dropped"),
        "observed_gc_retry_max_attempts": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.retry_max_attempts"),
        "observed_gc_latency_completed_ids": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.latency.completed_ids"),
        "observed_gc_latency_dropped_ids": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.latency.dropped_ids"),
        "observed_gc_latency_total_ms": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.latency.total_ms"),
        "observed_gc_latency_max_ms": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.latency.max_ms"),
        "observed_gc_source_segments_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_total"),
        "observed_gc_source_segments_eligible_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_eligible_total"),
        "observed_gc_source_segments_deleted_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_deleted_total"),
        "observed_gc_source_segments_protected_in_use_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_protected_in_use_total"),
        "observed_gc_source_segments_protected_retained_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_protected_retained_total"),
        "observed_gc_source_segments_protected_overlap_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_protected_overlap_total"),
        "observed_gc_source_segments_protected_other_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_segments_protected_other_total"),
        "observed_gc_source_bytes_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_total"),
        "observed_gc_source_bytes_eligible_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_eligible_total"),
        "observed_gc_source_bytes_deleted_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_deleted_total"),
        "observed_gc_source_bytes_protected_in_use_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_protected_in_use_total"),
        "observed_gc_source_bytes_protected_retained_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_protected_retained_total"),
        "observed_gc_source_bytes_protected_overlap_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_protected_overlap_total"),
        "observed_gc_source_bytes_protected_other_total": metric_int(stats, "treedb.cache.vlog_generation.observed_gc.source_bytes_protected_other_total"),
        "checkpoint_kick_runs": metric_int(stats, "treedb.cache.vlog_generation.checkpoint_kick.runs"),
        "checkpoint_kick_gc_runs": metric_int(stats, "treedb.cache.vlog_generation.checkpoint_kick.gc_runs"),
        "checkpoint_kick_rewrite_runs": metric_int(stats, "treedb.cache.vlog_generation.checkpoint_kick.rewrite_runs"),
        "checkpoint_kick_skipped_hot_no_debt": metric_int(stats, "treedb.cache.vlog_generation.checkpoint_kick.skipped_hot_no_debt"),
        "checkpoint_kick_hot_no_debt_wake_runs": metric_int(
            stats,
            "treedb.cache.vlog_generation.checkpoint_kick.hot_no_debt_wake.runs",
        ),
    }

    skip_keys = [
        "treedb.cache.vlog_generation.maintenance.skip.wal_on_periodic",
        "treedb.cache.vlog_generation.maintenance.skip.maintenance_phase",
        "treedb.cache.vlog_generation.maintenance.skip.stage_gate",
        "treedb.cache.vlog_generation.maintenance.skip.stage_gate_not_due",
        "treedb.cache.vlog_generation.maintenance.skip.stage_gate_due_reserved",
        "treedb.cache.vlog_generation.maintenance.skip.age_blocked_gate",
        "treedb.cache.vlog_generation.maintenance.skip.priority_pending",
        "treedb.cache.vlog_generation.maintenance.skip.quiet_window",
        "treedb.cache.vlog_generation.maintenance.skip.before_first_checkpoint",
        "treedb.cache.vlog_generation.maintenance.skip.checkpoint_inflight",
    ]
    skip_map = {k.split(".")[-1]: metric_int(stats, k) for k in skip_keys}
    m["maintenance_skip"] = skip_map
    # stage_gate is an umbrella that is also reflected in *_not_due / *_due_reserved.
    m["maintenance_skip_total"] = (
        skip_map.get("wal_on_periodic", 0)
        + skip_map.get("maintenance_phase", 0)
        + skip_map.get("stage_gate_not_due", 0)
        + skip_map.get("stage_gate_due_reserved", 0)
        + skip_map.get("age_blocked_gate", 0)
        + skip_map.get("priority_pending", 0)
        + skip_map.get("quiet_window", 0)
        + skip_map.get("before_first_checkpoint", 0)
        + skip_map.get("checkpoint_inflight", 0)
    )

    # Maintenance pass counters are not mutually exclusive: a pass can run both
    # rewrite and GC. Prefer acquired as the canonical pass count.
    passes_total = m["maintenance_acquired"]
    m["maintenance_passes_total"] = passes_total
    m["maintenance_acquire_rate_pct"] = pct(m["maintenance_acquired"], m["maintenance_attempts"])
    m["maintenance_collision_rate_pct"] = pct(m["maintenance_collisions"], m["maintenance_attempts"])
    m["maintenance_rewrite_pass_share_pct"] = pct(m["maintenance_with_rewrite"], passes_total)
    m["maintenance_gc_pass_share_pct"] = pct(m["maintenance_with_gc"], passes_total)
    m["maintenance_acquired_source_periodic_pct"] = pct(
        m["maintenance_acquired_source_periodic"],
        passes_total,
    )
    m["maintenance_acquired_source_bypass_pct"] = pct(
        m["maintenance_acquired_source_bypass"],
        passes_total,
    )
    m["maintenance_acquired_source_checkpoint_pending_pct"] = pct(
        m["maintenance_acquired_source_checkpoint_pending"],
        passes_total,
    )
    m["maintenance_acquired_source_rewrite_age_blocked_pct"] = pct(
        m["maintenance_acquired_source_rewrite_age_blocked"],
        passes_total,
    )
    m["maintenance_acquired_source_rewrite_stage_confirm_pct"] = pct(
        m["maintenance_acquired_source_rewrite_stage_confirm"],
        passes_total,
    )
    m["maintenance_acquired_source_other_pct"] = pct(
        m["maintenance_acquired_source_other"],
        passes_total,
    )
    m["checkpoint_kick_rewrite_rate_pct"] = pct(
        m["checkpoint_kick_rewrite_runs"],
        m["checkpoint_kick_runs"],
    )
    m["rewrite_queued_debt_rewrite_start_rate_pct"] = pct(
        m["rewrite_queued_debt_rewrite_started"],
        m["rewrite_queued_debt_passes"],
    )
    m["rewrite_queued_debt_skip_total"] = (
        m["rewrite_queued_debt_skip_quiet_window"]
        + m["rewrite_queued_debt_skip_cancel_backoff"]
        + m["rewrite_queued_debt_skip_ineffective_backoff"]
        + m["rewrite_queued_debt_skip_min_interval"]
        + m["rewrite_queued_debt_skip_budget_empty"]
        + m["rewrite_queued_debt_skip_no_chunk"]
    )
    m["rewrite_queued_debt_exec_reclaim_ratio_pct"] = pct(
        m["rewrite_queued_debt_exec_reclaimed_bytes"],
        m["rewrite_queued_debt_exec_effective_bytes_before"],
    )
    m["rewrite_queued_debt_exec_no_reclaim_rate_pct"] = pct(
        m["rewrite_queued_debt_exec_no_reclaim_runs"],
        m["rewrite_queued_debt_exec_runs"],
    )
    m["rewrite_queued_debt_exec_source_unreferenced_bytes_pct"] = pct(
        m["rewrite_queued_debt_exec_source_bytes_unreferenced"],
        m["rewrite_queued_debt_exec_source_bytes_requested"],
    )

    m["rewrite_plan_select_rate_pct"] = pct(m["rewrite_plan_selected"], m["rewrite_plan_runs"])
    m["rewrite_segment_realization_pct"] = pct(
        m["rewrite_exec_source_segments_total"],
        m["rewrite_plan_selected_segments_total"],
    )
    m["rewrite_source_unreferenced_pct"] = pct(
        m["rewrite_exec_source_segments_unreferenced_total"],
        m["rewrite_exec_source_segments_requested_total"],
    )
    m["rewrite_source_still_referenced_pct"] = pct(
        m["rewrite_exec_source_segments_still_referenced_total"],
        m["rewrite_exec_source_segments_requested_total"],
    )
    m["rewrite_source_unreferenced_bytes_pct"] = pct(
        m["rewrite_exec_source_bytes_unreferenced_total"],
        m["rewrite_exec_source_bytes_requested_total"],
    )
    m["rewrite_source_still_referenced_bytes_pct"] = pct(
        m["rewrite_exec_source_bytes_still_referenced_total"],
        m["rewrite_exec_source_bytes_requested_total"],
    )
    m["rewrite_checkpoint_like_runs"] = (
        m["rewrite_runs_source_bypass"]
        + m["rewrite_runs_source_checkpoint_pending"]
    )
    m["rewrite_non_checkpoint_runs"] = max(
        0,
        m["rewrite_runs"] - m["rewrite_checkpoint_like_runs"],
    )
    m["rewrite_checkpoint_like_run_share_pct"] = pct(
        m["rewrite_checkpoint_like_runs"],
        m["rewrite_runs"],
    )
    m["rewrite_checkpoint_like_budget_consumed_bytes_total"] = (
        m["rewrite_budget_consumed_bytes_total_source_bypass"]
        + m["rewrite_budget_consumed_bytes_total_source_checkpoint_pending"]
    )
    m["rewrite_non_checkpoint_budget_consumed_bytes_total"] = max(
        0,
        m["rewrite_budget_consumed_bytes_total"]
        - m["rewrite_checkpoint_like_budget_consumed_bytes_total"],
    )
    m["rewrite_checkpoint_like_budget_share_pct"] = pct(
        m["rewrite_checkpoint_like_budget_consumed_bytes_total"],
        m["rewrite_budget_consumed_bytes_total"],
    )
    m["rewrite_checkpoint_like_source_bytes_requested_total"] = (
        m["rewrite_exec_source_bytes_requested_total_source_bypass"]
        + m["rewrite_exec_source_bytes_requested_total_source_checkpoint_pending"]
    )
    m["rewrite_checkpoint_like_source_bytes_unreferenced_total"] = (
        m["rewrite_exec_source_bytes_unreferenced_total_source_bypass"]
        + m["rewrite_exec_source_bytes_unreferenced_total_source_checkpoint_pending"]
    )
    m["rewrite_non_checkpoint_source_bytes_requested_total"] = max(
        0,
        m["rewrite_exec_source_bytes_requested_total"]
        - m["rewrite_checkpoint_like_source_bytes_requested_total"],
    )
    m["rewrite_non_checkpoint_source_bytes_unreferenced_total"] = max(
        0,
        m["rewrite_exec_source_bytes_unreferenced_total"]
        - m["rewrite_checkpoint_like_source_bytes_unreferenced_total"],
    )
    m["rewrite_checkpoint_like_source_unreferenced_bytes_pct"] = pct(
        m["rewrite_checkpoint_like_source_bytes_unreferenced_total"],
        m["rewrite_checkpoint_like_source_bytes_requested_total"],
    )
    m["rewrite_non_checkpoint_source_unreferenced_bytes_pct"] = pct(
        m["rewrite_non_checkpoint_source_bytes_unreferenced_total"],
        m["rewrite_non_checkpoint_source_bytes_requested_total"],
    )
    m["rewrite_stale_selection_coverage_pct"] = pct(
        m["rewrite_processed_stale_bytes"],
        m["rewrite_plan_selected_bytes_stale"],
    )
    m["rewrite_immediate_reclaim_pct"] = pct(
        m["rewrite_reclaimed_bytes"],
        m["rewrite_processed_stale_bytes"],
    )
    m["rewrite_queue_progress_segments_net_drain_total"] = (
        m["rewrite_queue_progress_segments_drained_total"]
        - m["rewrite_queue_progress_segments_grown_total"]
    )
    m["rewrite_queue_progress_live_bytes_net_drain_total"] = (
        m["rewrite_queue_progress_live_bytes_drained_total"]
        - m["rewrite_queue_progress_live_bytes_grown_total"]
    )
    m["rewrite_queue_progress_live_bytes_known_pct"] = pct(
        m["rewrite_queue_progress_live_bytes_known_passes"],
        m["rewrite_queue_progress_passes"],
    )
    m["rewrite_stale_not_reclaimed_bytes"] = max(
        0,
        m["rewrite_processed_stale_bytes"] - m["rewrite_reclaimed_bytes"],
    )
    rewrite_secs = m["rewrite_exec_total_ms"] / 1000.0
    m["rewrite_exec_throughput_bytes_per_sec"] = (
        (m["rewrite_bytes_in"] / rewrite_secs) if rewrite_secs > 0 else 0.0
    )

    gc_secs = m["gc_exec_total_ms"] / 1000.0
    m["gc_delete_throughput_bytes_per_sec"] = (
        (m["gc_deleted_bytes"] / gc_secs) if gc_secs > 0 else 0.0
    )

    m["observed_gc_drain_pct"] = pct(m["observed_gc_taken_ids"], m["observed_gc_queued_ids"])
    m["observed_gc_latency_finalized_ids"] = m["observed_gc_latency_completed_ids"] + m["observed_gc_latency_dropped_ids"]
    m["observed_gc_latency_avg_ms"] = (
        (float(m["observed_gc_latency_total_ms"]) / float(m["observed_gc_latency_finalized_ids"]))
        if m["observed_gc_latency_finalized_ids"] > 0
        else 0.0
    )
    m["observed_gc_source_segments_eligible_pct"] = pct(
        m["observed_gc_source_segments_eligible_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_segments_deleted_pct"] = pct(
        m["observed_gc_source_segments_deleted_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_bytes_eligible_pct"] = pct(
        m["observed_gc_source_bytes_eligible_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["observed_gc_source_bytes_deleted_pct"] = pct(
        m["observed_gc_source_bytes_deleted_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["observed_gc_source_bytes_deleted_of_eligible_pct"] = pct(
        m["observed_gc_source_bytes_deleted_total"],
        m["observed_gc_source_bytes_eligible_total"],
    )
    m["observed_gc_source_segments_protected_in_use_pct"] = pct(
        m["observed_gc_source_segments_protected_in_use_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_segments_protected_retained_pct"] = pct(
        m["observed_gc_source_segments_protected_retained_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_segments_protected_overlap_pct"] = pct(
        m["observed_gc_source_segments_protected_overlap_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_segments_protected_other_pct"] = pct(
        m["observed_gc_source_segments_protected_other_total"],
        m["observed_gc_source_segments_total"],
    )
    m["observed_gc_source_bytes_protected_in_use_pct"] = pct(
        m["observed_gc_source_bytes_protected_in_use_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["observed_gc_source_bytes_protected_retained_pct"] = pct(
        m["observed_gc_source_bytes_protected_retained_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["observed_gc_source_bytes_protected_overlap_pct"] = pct(
        m["observed_gc_source_bytes_protected_overlap_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["observed_gc_source_bytes_protected_other_pct"] = pct(
        m["observed_gc_source_bytes_protected_other_total"],
        m["observed_gc_source_bytes_total"],
    )
    m["retained_prune_removed_candidate_segments_pct"] = pct(
        m["retained_prune_removed_segments"],
        m["retained_prune_candidate_segments"],
    )
    m["retained_prune_removed_candidate_bytes_pct"] = pct(
        m["retained_prune_removed_bytes"],
        m["retained_prune_candidate_bytes"],
    )
    m["retained_prune_observed_removed_candidate_segments_pct"] = pct(
        m["retained_prune_observed_source_removed_segments_total"],
        m["retained_prune_observed_source_candidate_segments_total"],
    )
    m["retained_prune_observed_removed_candidate_bytes_pct"] = pct(
        m["retained_prune_observed_source_removed_bytes_total"],
        m["retained_prune_observed_source_candidate_bytes_total"],
    )
    m["retained_prune_observed_live_skipped_candidate_segments_pct"] = pct(
        m["retained_prune_observed_source_live_skipped_segments_total"],
        m["retained_prune_observed_source_candidate_segments_total"],
    )
    m["retained_prune_observed_live_skipped_candidate_bytes_pct"] = pct(
        m["retained_prune_observed_source_live_skipped_bytes_total"],
        m["retained_prune_observed_source_candidate_bytes_total"],
    )
    m["vlog_zombie_pinned_bytes_pct"] = pct(
        m["vlog_zombie_pinned_bytes"],
        m["vlog_zombie_bytes"],
    )

    return m


def print_report(summary: dict[str, Any], source_file: Path, run_home: str, instance_name: str) -> None:
    print(f"Source file: {source_file}")
    if run_home:
        print(f"Run home:    {run_home}")
    if instance_name:
        print(f"Instance:    {instance_name}")
    print("")

    print("Maintenance lane")
    print(
        "  attempts/acquired/collisions: "
        f"{summary['maintenance_attempts']} / {summary['maintenance_acquired']} / {summary['maintenance_collisions']} "
        f"(acquire={summary['maintenance_acquire_rate_pct']:.1f}%, collision={summary['maintenance_collision_rate_pct']:.1f}%)"
    )
    print(
        "  passes: "
        f"total={summary['maintenance_passes_total']} "
        f"noop={summary['maintenance_noop']} "
        f"rewrite={summary['maintenance_with_rewrite']} "
        f"gc={summary['maintenance_with_gc']} "
        f"(rewrite_share={summary['maintenance_rewrite_pass_share_pct']:.1f}%, gc_share={summary['maintenance_gc_pass_share_pct']:.1f}%)"
    )
    print(
        "  acquired by source: "
        f"periodic={summary['maintenance_acquired_source_periodic']} ({summary['maintenance_acquired_source_periodic_pct']:.1f}%) "
        f"bypass={summary['maintenance_acquired_source_bypass']} ({summary['maintenance_acquired_source_bypass_pct']:.1f}%) "
        f"checkpoint_pending={summary['maintenance_acquired_source_checkpoint_pending']} ({summary['maintenance_acquired_source_checkpoint_pending_pct']:.1f}%) "
        f"age_blocked={summary['maintenance_acquired_source_rewrite_age_blocked']} ({summary['maintenance_acquired_source_rewrite_age_blocked_pct']:.1f}%) "
        f"stage_confirm={summary['maintenance_acquired_source_rewrite_stage_confirm']} ({summary['maintenance_acquired_source_rewrite_stage_confirm_pct']:.1f}%) "
        f"other={summary['maintenance_acquired_source_other']} ({summary['maintenance_acquired_source_other_pct']:.1f}%)"
    )
    print(
        "  rewrite passes by source: "
        f"periodic={summary['maintenance_with_rewrite_source_periodic']} "
        f"bypass={summary['maintenance_with_rewrite_source_bypass']} "
        f"checkpoint_pending={summary['maintenance_with_rewrite_source_checkpoint_pending']} "
        f"age_blocked={summary['maintenance_with_rewrite_source_rewrite_age_blocked']} "
        f"stage_confirm={summary['maintenance_with_rewrite_source_rewrite_stage_confirm']} "
        f"other={summary['maintenance_with_rewrite_source_other']}"
    )
    skips = summary["maintenance_skip"]
    print(
        "  skip pressure: "
        f"total={summary['maintenance_skip_total']} "
        f"pre_checkpoint={skips['before_first_checkpoint']} "
        f"stage_gate={skips['stage_gate']} "
        f"stage_not_due={skips['stage_gate_not_due']} "
        f"age_blocked={skips['age_blocked_gate']} "
        f"quiet={skips['quiet_window']} "
        f"priority={skips['priority_pending']} "
        f"checkpoint={skips['checkpoint_inflight']}"
    )
    print(
        "  checkpoint-kick: "
        f"runs={summary['checkpoint_kick_runs']} "
        f"rewrite_runs={summary['checkpoint_kick_rewrite_runs']} "
        f"gc_runs={summary['checkpoint_kick_gc_runs']} "
        f"skipped_hot_no_debt={summary['checkpoint_kick_skipped_hot_no_debt']}"
    )
    print("")

    print("Rewrite economics")
    print(
        "  plan runs/selected/empty: "
        f"{summary['rewrite_plan_runs']} / {summary['rewrite_plan_selected']} / {summary['rewrite_plan_empty']} "
        f"(select_rate={summary['rewrite_plan_select_rate_pct']:.1f}%)"
    )
    print(
        "  plan-empty breakdown: "
        f"no_selection={summary['rewrite_plan_empty_no_selection']} "
        f"age_blocked={summary['rewrite_plan_empty_age_blocked']}"
    )
    print(
        "  plan penalty-filter: "
        f"runs={summary['rewrite_plan_penalty_filter_runs']} "
        f"segments={summary['rewrite_plan_penalty_filter_segments']} "
        f"to_empty_runs={summary['rewrite_plan_penalty_filter_to_empty_runs']}"
    )
    print(
        "  selected->executed segments: "
        f"{summary['rewrite_plan_selected_segments_total']} -> {summary['rewrite_exec_source_segments_total']} "
        f"(realization={summary['rewrite_segment_realization_pct']:.1f}%)"
    )
    print(
        "  source outcomes (exec): "
        f"requested_total={summary['rewrite_exec_source_segments_requested_total']} "
        f"unreferenced_total={summary['rewrite_exec_source_segments_unreferenced_total']} "
        f"still_referenced_total={summary['rewrite_exec_source_segments_still_referenced_total']} "
        f"(unref_pct={summary['rewrite_source_unreferenced_pct']:.1f}%, still_ref_pct={summary['rewrite_source_still_referenced_pct']:.1f}%) "
        f"last=requested:{summary['rewrite_exec_source_segments_requested_last']} "
        f"unref:{summary['rewrite_exec_source_segments_unreferenced_last']} "
        f"still_ref:{summary['rewrite_exec_source_segments_still_referenced_last']}"
    )
    print(
        "  source outcomes bytes (exec): "
        f"requested_total={human_bytes(summary['rewrite_exec_source_bytes_requested_total'])} "
        f"unreferenced_total={human_bytes(summary['rewrite_exec_source_bytes_unreferenced_total'])} "
        f"still_referenced_total={human_bytes(summary['rewrite_exec_source_bytes_still_referenced_total'])} "
        f"(unref_pct={summary['rewrite_source_unreferenced_bytes_pct']:.1f}%, "
        f"still_ref_pct={summary['rewrite_source_still_referenced_bytes_pct']:.1f}%) "
        f"last=requested:{human_bytes(summary['rewrite_exec_source_bytes_requested_last'])} "
        f"unref:{human_bytes(summary['rewrite_exec_source_bytes_unreferenced_last'])} "
        f"still_ref:{human_bytes(summary['rewrite_exec_source_bytes_still_referenced_last'])}"
    )
    print(
        "  source split (checkpoint-like vs non-checkpoint): "
        f"runs={summary['rewrite_checkpoint_like_runs']}/{summary['rewrite_non_checkpoint_runs']} "
        f"(ckpt_like_share={summary['rewrite_checkpoint_like_run_share_pct']:.1f}%) "
        f"budget={human_bytes(summary['rewrite_checkpoint_like_budget_consumed_bytes_total'])}/{human_bytes(summary['rewrite_non_checkpoint_budget_consumed_bytes_total'])} "
        f"(ckpt_like_budget_share={summary['rewrite_checkpoint_like_budget_share_pct']:.1f}%) "
        f"unref_pct={summary['rewrite_checkpoint_like_source_unreferenced_bytes_pct']:.1f}%/{summary['rewrite_non_checkpoint_source_unreferenced_bytes_pct']:.1f}%"
    )
    print(
        "  selected stale vs processed stale: "
        f"{human_bytes(summary['rewrite_plan_selected_bytes_stale'])} -> {human_bytes(summary['rewrite_processed_stale_bytes'])} "
        f"(coverage={summary['rewrite_stale_selection_coverage_pct']:.1f}%)"
    )
    print(
        "  bytes in/out/reclaimed: "
        f"{human_bytes(summary['rewrite_bytes_in'])} / {human_bytes(summary['rewrite_bytes_out'])} / {human_bytes(summary['rewrite_reclaimed_bytes'])}"
    )
    print(
        "  stale processed w/o immediate reclaim: "
        f"{human_bytes(summary['rewrite_stale_not_reclaimed_bytes'])} "
        f"(immediate_reclaim={summary['rewrite_immediate_reclaim_pct']:.2f}%, no_reclaim_runs={summary['rewrite_no_reclaim_runs']})"
    )
    print(
        "  exec: "
        f"runs={summary['rewrite_runs']} total_ms={summary['rewrite_exec_total_ms']:.3f} avg_ms={summary['rewrite_exec_avg_ms']:.3f} "
        f"throughput={human_bytes(summary['rewrite_exec_throughput_bytes_per_sec'])}/s"
    )
    print(
        "  debt/budget: "
        f"ledger={human_bytes(summary['rewrite_ledger_bytes_total'])} (stale={human_bytes(summary['rewrite_ledger_bytes_stale'])}, segs={summary['rewrite_ledger_segments']}) "
        f"age_blocked_ms={summary['rewrite_age_blocked_remaining_ms']} penalties={summary['rewrite_penalties_active']} "
        f"budget_consumed={human_bytes(summary['rewrite_budget_consumed_bytes_total'])} "
        f"budget_util={summary['rewrite_budget_tokens_utilization_pct']:.1f}%"
    )
    print(
        "  queue caps (effective): "
        f"resume={summary['rewrite_queue_config_resume_max_segments']} "
        f"debt_drain={summary['rewrite_queue_config_debt_drain_max_segments']} "
        f"fresh_plan_min={summary['rewrite_queue_config_fresh_plan_debt_drain_min_segments']} "
        f"fresh_plan_max={summary['rewrite_queue_config_fresh_plan_debt_drain_max_segments']}"
    )
    print(
        "  queue live-hint coverage: "
        f"known={summary['rewrite_queue_live_hint_known']} "
        f"ids={summary['rewrite_queue_live_hint_ids_known']}/{summary['rewrite_queue_live_hint_ids_present']} "
        f"coverage={summary['rewrite_queue_live_hint_coverage_pct']:.1f}% "
        f"bytes={human_bytes(summary['rewrite_queue_live_hint_bytes'])}"
    )
    print(
        "  queue segment-cap decisions: "
        f"run={summary['rewrite_queue_run_segment_cap']} ({summary['rewrite_queue_run_segment_cap_limiter']}, by_budget={summary['rewrite_queue_run_segment_cap_by_budget']}, "
        f"per_seg_budget={human_bytes(summary['rewrite_queue_run_segment_cap_per_segment_budget_bytes'])}) "
        f"checkpoint_kick={summary['rewrite_queue_run_segment_cap_checkpoint_kick']} ({summary['rewrite_queue_run_segment_cap_limiter_checkpoint_kick']}, "
        f"by_budget={summary['rewrite_queue_run_segment_cap_by_budget_checkpoint_kick']}, "
        f"per_seg_budget={human_bytes(summary['rewrite_queue_run_segment_cap_per_segment_budget_bytes_checkpoint_kick'])}) "
        f"fresh_plan={summary['rewrite_queue_run_segment_cap_fresh_plan']} ({summary['rewrite_queue_run_segment_cap_limiter_fresh_plan']}, "
        f"by_budget={summary['rewrite_queue_run_segment_cap_by_budget_fresh_plan']}, "
        f"per_seg_budget={human_bytes(summary['rewrite_queue_run_segment_cap_per_segment_budget_bytes_fresh_plan'])})"
    )
    print(
        "  queue segment-cap limiter counters: "
        f"run_decisions={summary['rewrite_queue_run_segment_cap_decisions']} "
        f"fresh_plan_decisions={summary['rewrite_queue_run_segment_cap_decisions_fresh_plan']} "
        f"budget_tokens={summary['rewrite_queue_run_segment_cap_limiter_count_budget_tokens']} "
        f"debt_drain_cap={summary['rewrite_queue_run_segment_cap_limiter_count_debt_drain_cap']} "
        f"checkpoint_kick_safety={summary['rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_safety']} "
        f"checkpoint_kick_burst={summary['rewrite_queue_run_segment_cap_limiter_count_checkpoint_kick_burst']} "
        f"fresh_plan_queue_threshold={summary['rewrite_queue_run_segment_cap_limiter_count_fresh_plan_queue_threshold']} "
        f"fresh_plan_cap={summary['rewrite_queue_run_segment_cap_limiter_count_fresh_plan_cap']}"
    )
    print(
        "  queue progress: "
        f"passes={summary['rewrite_queue_progress_passes']} "
        f"segments before/after={summary['rewrite_queue_progress_segments_before_total']}/{summary['rewrite_queue_progress_segments_after_total']} "
        f"drained={summary['rewrite_queue_progress_segments_drained_total']} "
        f"grown={summary['rewrite_queue_progress_segments_grown_total']} "
        f"net_drain={summary['rewrite_queue_progress_segments_net_drain_total']} "
        f"last_delta={summary['rewrite_queue_progress_segments_delta_last']} "
        f"live_known={summary['rewrite_queue_progress_live_bytes_known_pct']:.1f}% "
        f"live net_drain={human_bytes(summary['rewrite_queue_progress_live_bytes_net_drain_total'])} "
        f"snapshot_errors={summary['rewrite_queue_progress_snapshot_errors']}"
    )
    print("")

    print("GC economics")
    print(
        "  runs/deleted: "
        f"{summary['gc_runs']} / {summary['gc_deleted_segments']} segments, {human_bytes(summary['gc_deleted_bytes'])}"
    )
    print(
        "  leaf-pack deleted: "
        f"runs={summary['leaf_pack_gc_runs']} files={summary['leaf_pack_gc_deleted_files']} "
        f"generations={summary['leaf_pack_gc_deleted_generations']}/{summary['leaf_pack_gc_eligible_generations']} "
        f"bytes={human_bytes(summary['leaf_pack_gc_deleted_bytes'])}"
    )
    print(
        "  exec: "
        f"total_ms={summary['gc_exec_total_ms']:.3f} avg_ms={summary['gc_exec_avg_ms']:.3f} "
        f"delete_throughput={human_bytes(summary['gc_delete_throughput_bytes_per_sec'])}/s"
    )
    print(
        "  last eligibility/protection: "
        f"eligible={human_bytes(summary['gc_last_eligible_bytes'])} "
        f"pending={human_bytes(summary['gc_last_pending_bytes'])} "
        f"protected_retained={human_bytes(summary['gc_last_protected_retained_bytes'])}"
    )
    print(
        "  checkpoint-kick: "
        f"runs={summary['checkpoint_kick_runs']} rewrite_runs={summary['checkpoint_kick_rewrite_runs']} gc_runs={summary['checkpoint_kick_gc_runs']}"
    )
    print(
        "  retained-prune: "
        f"runs={summary['retained_prune_runs']} forced={summary['retained_prune_forced_runs']} closed={human_bytes(summary['retained_prune_closed_bytes'])} "
        f"candidates={summary['retained_prune_candidate_segments']} ({human_bytes(summary['retained_prune_candidate_bytes'])}) "
        f"removed={summary['retained_prune_removed_segments']} ({human_bytes(summary['retained_prune_removed_bytes'])}) "
        f"(seg_removed_pct={summary['retained_prune_removed_candidate_segments_pct']:.1f}%, bytes_removed_pct={summary['retained_prune_removed_candidate_bytes_pct']:.1f}%)"
    )
    print(
        "  retained-prune skips: "
        f"in_use={summary['retained_prune_in_use_skipped_segments']} ({human_bytes(summary['retained_prune_in_use_skipped_bytes'])}) "
        f"live={summary['retained_prune_live_skipped_segments']} ({human_bytes(summary['retained_prune_live_skipped_bytes'])}) "
        f"zombie_marked={summary['retained_prune_zombie_marked_segments']} ({human_bytes(summary['retained_prune_zombie_marked_bytes'])})"
    )
    print(
        "  zombie inventory: "
        f"total={summary['vlog_zombie_segments']} ({human_bytes(summary['vlog_zombie_bytes'])}) "
        f"pinned={summary['vlog_zombie_pinned_segments']} ({human_bytes(summary['vlog_zombie_pinned_bytes'])}) "
        f"unpinned={summary['vlog_zombie_unpinned_segments']} ({human_bytes(summary['vlog_zombie_unpinned_bytes'])}) "
        f"(pinned_bytes_pct={summary['vlog_zombie_pinned_bytes_pct']:.1f}%)"
    )
    print("")

    print("Observed-source replay")
    print(
        "  queued/taken/pending ids: "
        f"{summary['observed_gc_queued_ids']} / {summary['observed_gc_taken_ids']} / {summary['observed_gc_pending_ids']} "
        f"(drain={summary['observed_gc_drain_pct']:.1f}%, retries={summary['observed_gc_retry_queued']}, runs={summary['observed_gc_runs']})"
    )
    print(
        "  retry budget/latency: "
        f"max_attempts={summary['observed_gc_retry_max_attempts']} "
        f"retry_dropped={summary['observed_gc_retry_dropped']} "
        f"finalized_ids={summary['observed_gc_latency_finalized_ids']} "
        f"(completed={summary['observed_gc_latency_completed_ids']}, dropped={summary['observed_gc_latency_dropped_ids']}) "
        f"latency total_ms={summary['observed_gc_latency_total_ms']} "
        f"avg_ms={summary['observed_gc_latency_avg_ms']:.3f} "
        f"max_ms={summary['observed_gc_latency_max_ms']}"
    )
    print(
        "  observed-source totals: "
        f"segments total={summary['observed_gc_source_segments_total']} "
        f"eligible={summary['observed_gc_source_segments_eligible_total']} "
        f"deleted={summary['observed_gc_source_segments_deleted_total']} "
        f"(eligible_pct={summary['observed_gc_source_segments_eligible_pct']:.1f}%, deleted_pct={summary['observed_gc_source_segments_deleted_pct']:.1f}%)"
    )
    print(
        "  observed-source bytes: "
        f"total={human_bytes(summary['observed_gc_source_bytes_total'])} "
        f"eligible={human_bytes(summary['observed_gc_source_bytes_eligible_total'])} "
        f"deleted={human_bytes(summary['observed_gc_source_bytes_deleted_total'])} "
        f"protected_retained={human_bytes(summary['observed_gc_source_bytes_protected_retained_total'])} "
        f"(eligible_pct={summary['observed_gc_source_bytes_eligible_pct']:.1f}%, "
        f"deleted_pct={summary['observed_gc_source_bytes_deleted_pct']:.1f}%, "
        f"deleted_of_eligible={summary['observed_gc_source_bytes_deleted_of_eligible_pct']:.1f}%)"
    )
    print(
        "  observed-source protection mix: "
        f"segments in_use={summary['observed_gc_source_segments_protected_in_use_total']} "
        f"retained={summary['observed_gc_source_segments_protected_retained_total']} "
        f"overlap={summary['observed_gc_source_segments_protected_overlap_total']} "
        f"other={summary['observed_gc_source_segments_protected_other_total']} "
        f"(in_use={summary['observed_gc_source_segments_protected_in_use_pct']:.1f}%, "
        f"retained={summary['observed_gc_source_segments_protected_retained_pct']:.1f}%, "
        f"overlap={summary['observed_gc_source_segments_protected_overlap_pct']:.1f}%, "
        f"other={summary['observed_gc_source_segments_protected_other_pct']:.1f}%) "
        f"bytes in_use={human_bytes(summary['observed_gc_source_bytes_protected_in_use_total'])} "
        f"retained={human_bytes(summary['observed_gc_source_bytes_protected_retained_total'])} "
        f"overlap={human_bytes(summary['observed_gc_source_bytes_protected_overlap_total'])} "
        f"other={human_bytes(summary['observed_gc_source_bytes_protected_other_total'])} "
        f"(in_use={summary['observed_gc_source_bytes_protected_in_use_pct']:.1f}%, "
        f"retained={summary['observed_gc_source_bytes_protected_retained_pct']:.1f}%, "
        f"overlap={summary['observed_gc_source_bytes_protected_overlap_pct']:.1f}%, "
        f"other={summary['observed_gc_source_bytes_protected_other_pct']:.1f}%)"
    )
    print(
        "  observed-source retained-prune totals: "
        f"seen={summary['retained_prune_observed_source_segments_total']} ({human_bytes(summary['retained_prune_observed_source_bytes_total'])}) "
        f"candidate={summary['retained_prune_observed_source_candidate_segments_total']} ({human_bytes(summary['retained_prune_observed_source_candidate_bytes_total'])}) "
        f"removed={summary['retained_prune_observed_source_removed_segments_total']} ({human_bytes(summary['retained_prune_observed_source_removed_bytes_total'])}) "
        f"zombie_marked={summary['retained_prune_observed_source_zombie_marked_segments_total']} ({human_bytes(summary['retained_prune_observed_source_zombie_marked_bytes_total'])}) "
        f"live_skipped={summary['retained_prune_observed_source_live_skipped_segments_total']} ({human_bytes(summary['retained_prune_observed_source_live_skipped_bytes_total'])}) "
        f"in_use_skipped={summary['retained_prune_observed_source_in_use_skipped_segments_total']} ({human_bytes(summary['retained_prune_observed_source_in_use_skipped_bytes_total'])}) "
        f"(removed_of_candidate={summary['retained_prune_observed_removed_candidate_segments_pct']:.1f}% seg / "
        f"{summary['retained_prune_observed_removed_candidate_bytes_pct']:.1f}% bytes, "
        f"live_skip_of_candidate={summary['retained_prune_observed_live_skipped_candidate_segments_pct']:.1f}% seg / "
        f"{summary['retained_prune_observed_live_skipped_candidate_bytes_pct']:.1f}% bytes)"
    )

    print("")
    notes: list[str] = []
    if summary["rewrite_processed_stale_bytes"] > 0 and summary["rewrite_reclaimed_bytes"] == 0:
        notes.append("rewrite copied stale bytes but immediate reclaim is zero; inspect GC eligibility/protection and post-run rewrite window")
    if summary["observed_gc_pending_ids"] > 0:
        notes.append("observed-source GC backlog still pending; may need longer run window or higher checkpoint-kick pressure")
    if summary["observed_gc_retry_dropped"] > 0:
        notes.append("observed-source GC retries hit max-attempt budget for some IDs; inspect retained-prune throughput and checkpoint-kick cadence")
    if summary["maintenance_collision_rate_pct"] > 20.0:
        notes.append("maintenance collision rate is high; lane contention may be throttling rewrite/GC progress")
    if summary["rewrite_segment_realization_pct"] < 60.0 and summary["rewrite_plan_selected_segments_total"] > 0:
        notes.append("rewrite segment realization is low; staged debt is being selected faster than executed")
    if (
        summary["rewrite_exec_source_segments_unreferenced_total"] > 0
        and summary["retained_prune_observed_source_zombie_marked_segments_total"] > 0
        and summary["observed_gc_source_segments_deleted_total"] == 0
        and summary["vlog_zombie_segments"] == 0
    ):
        notes.append("rewrite-selected sources became unreferenced and were zombie-marked, but GC delete counters stayed zero; reclaim likely happened via zombie lifecycle outside GC byte accounting")
    if not notes:
        notes.append("no obvious maintenance-lane bottleneck signature in this snapshot")

    print("Signals")
    for note in notes:
        print(f"  - {note}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze TreeDB live vlog maintenance capacity from run_celestia diagnostics")
    p.add_argument(
        "input",
        nargs="?",
        help="run home dir, diagnostics dir, or debug vars JSON file (default: latest ~/.celestia-app-mainnet-treedb-*)",
    )
    p.add_argument(
        "--instance-pattern",
        default="application.db",
        help="prefer instance names containing this substring when debug_vars has multiple DB instances",
    )
    p.add_argument("--json", action="store_true", help="emit JSON summary instead of text report")
    return p.parse_args()


def resolve_source(input_arg: str | None) -> Path:
    if input_arg:
        p = Path(os.path.expanduser(input_arg)).resolve()
        if not p.exists():
            raise FileNotFoundError(f"input does not exist: {p}")
        if p.is_file():
            return p
        src = find_diagnostics_file(p)
        if src is None:
            raise FileNotFoundError(f"no diagnostics JSON found under: {p}")
        return src

    home = find_latest_home()
    if home is None:
        raise FileNotFoundError("no ~/.celestia-app-mainnet-treedb-* directories found")
    src = find_diagnostics_file(home)
    if src is None:
        raise FileNotFoundError(f"no diagnostics JSON found under: {home}")
    return src


def main() -> int:
    args = parse_args()
    try:
        source = resolve_source(args.input)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"error: failed to parse JSON from {source}: {exc}", file=sys.stderr)
        return 2

    prebuilt_summary: dict[str, Any] | None = None
    prebuilt_instance = ""
    prebuilt_run_home = ""
    if isinstance(payload, dict):
        maybe_summary = payload.get("summary")
        if isinstance(maybe_summary, dict) and "maintenance_attempts" in maybe_summary:
            prebuilt_summary = maybe_summary
            prebuilt_instance = str(payload.get("instance", ""))
            prebuilt_run_home = str(payload.get("run_home", ""))

    if prebuilt_summary is not None:
        summary = build_summary({})
        summary.update(prebuilt_summary)
        instance_name = prebuilt_instance
        run_home = prebuilt_run_home or find_home_from_path(source)
    else:
        stats, instance_name = extract_stats(payload, args.instance_pattern)
        if not stats:
            print(
                "error: could not extract treedb stats map from JSON (expected debug_vars shape, flat stats map, or maintenance summary JSON)",
                file=sys.stderr,
            )
            return 2
        summary = build_summary(stats)
        run_home = find_home_from_path(source)

    if args.json:
        out = {
            "source_file": str(source),
            "run_home": run_home,
            "instance": instance_name,
            "summary": summary,
        }
        print(json.dumps(out, indent=2, sort_keys=True))
    else:
        print_report(summary, source, run_home, instance_name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
