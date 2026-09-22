#!/usr/bin/env python3
"""Summarize TreeDB M14 final-gate unified-bench artifact matrices.

The script expects a matrix root with one subdirectory per row. Each row should
contain the standard `unified-bench -profile-dir` files:

  - benchprof_results.json
  - benchprof_results.md
  - insights.json

Rows may also contain `variant.env` metadata written by the M14 runbook script.
It writes `m14_matrix_summary.json` and `m14_matrix_summary.md` by default.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

TESTS = ("sequential_write", "batch_random", "random_write")
ROW_ORDER = (
    "default_unconfigured",
    "legacy_parallel_c4",
    "span_native_c1",
    "span_native_c2",
    "span_native_c4",
    "span_native_c8",
    "span_native_c16",
    "span_native_c4_no_backlog",
    "span_native_c4_cache_disabled",
)
CHECKPOINT_LABELS = ("Sequential Write", "Batch Random", "Random Write", "After Run")
DISK_KEYS = ("maindb/index.db", "maindb/wal", "maindb/value_vlog", "maindb/leaf_vlog")
COUNTER_KEYS = (
    "treedb.flush_apply.old_leaf_read_decode.bytes_per_op",
    "treedb.flush_apply.merge_build.leaf_merges_per_op",
    "treedb.cache.flush_apply.leaf_log_append_frames_per_op",
    "treedb.cache.flush_span_run.ops_per_span",
    "treedb.cache.flush_span_run.single_op_span_ratio",
    "treedb.cache.flush_span_run.target_leaves_split_across_chunks_total",
    "treedb.flush_apply.span_run.ops_per_span",
    "treedb.flush_apply.span_run.target_leaf_spans_total",
    "treedb.flush_apply.span_run.single_op_spans_total",
    "treedb.flush_apply.span_native.candidate_ops_total",
    "treedb.flush_apply.span_native.eligible_ops_total",
    "treedb.flush_apply.span_native.used_ops_total",
    "treedb.flush_apply.span_native.fallbacks_total",
    "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total",
    "treedb.flush_apply.span_native.fallback.reason.root_mismatch.ops_total",
    "treedb.flush_apply.span_native.fallback.reason.output_ownership_failure.ops_total",
    "treedb.flush_apply.span_native.fallback.reason.reducer_validation_failed.ops_total",
    "treedb.flush_apply.root_reduce.ns_total",
    "treedb.flush_apply.guarded_publish.ns_total",
    "treedb.flush_apply.retry_total",
    "treedb.flush_apply.mismatch_total",
    "treedb.cache.flush_apply.foreground_assist_wait_ns_total",
    "treedb.cache.flush_apply.coordinator.active_assist_skips_total",
    "treedb.cache.flush_apply.coordinator.progress_wait_ns_total",
    "treedb.cache.flush_apply.coordinator.stall_waits_total",
    "treedb.cache.flush_apply.coordinator.blocking_fallbacks_total",
    "treedb.cache.flush_apply.coordinator.hard_overload_fallbacks_total",
    "treedb.cache.checkpoint.active_background_flush_wait_ns_total",
    "treedb.cache.checkpoint.stage.value_log_flush.total_ns",
    "treedb.cache.checkpoint.stage.flush_all.total_ns",
    "treedb.cache.checkpoint.stage.leaf_value_log_sync.total_ns",
    "treedb.cache.checkpoint.stage.reducer_publish.total_ns",
    "treedb.cache.flush_backlog_coalescing.admitted_runs_total",
    "treedb.cache.flush_backlog_coalescing.admitted_extra_memtables_total",
    "treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total",
    "treedb.cache.flush_backlog_coalescing.selected_memtables_max",
    "treedb.cache.flush_backlog_coalescing.selected_ops_max",
)
PROFILE_KINDS = (
    "cpu_profiles",
    "alloc_space_profiles",
    "alloc_object_profiles",
    "block_profiles",
    "mutex_profiles",
)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _parse_env(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in _read_text(path).splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _extract_fenced_section(md: str, heading: str) -> str:
    idx = md.find(heading)
    if idx < 0:
        return ""
    first = md.find("```", idx)
    if first < 0:
        return ""
    first_end = md.find("\n", first)
    if first_end < 0:
        return ""
    second = md.find("```", first_end + 1)
    if second < 0:
        return ""
    return md[first_end + 1 : second]


def _parse_options(md: str) -> Dict[str, str]:
    section = _extract_fenced_section(md, "## Resolved TreeDB Options")
    opts: Dict[str, str] = {}
    for raw in section.splitlines():
        line = raw.strip()
        if not line or line == "notes:" or line.startswith("-") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        opts[key.strip()] = value.strip()
    return opts


def _effective_option(env: Mapping[str, str], opts: Mapping[str, str], env_key: str, option_key: str) -> str:
    """Return resolved option metadata when present, otherwise variant.env."""
    value = opts.get(option_key, "")
    if value != "":
        return value
    return env.get(env_key, "")


def _parse_checkpoint_table(md: str) -> Dict[str, str]:
    section = _extract_fenced_section(md, "## Checkpoint Time")
    checkpoints: Dict[str, str] = {}
    for raw in section.splitlines():
        line = raw.rstrip()
        if not line.strip() or set(line.strip()) <= {"-"} or "Before Test" in line:
            continue
        m = re.match(r"^\s*(Sequential Write|Batch Random|Random Write|After Run)\s+(.+?)\s*$", line)
        if m:
            checkpoints[m.group(1)] = m.group(2).strip()
    return checkpoints


def _parse_disk_usage(md: str) -> Dict[str, str]:
    section = _extract_fenced_section(md, "## Disk Usage")
    disks: Dict[str, str] = {}
    for raw in section.splitlines():
        line = raw.strip()
        if not line or line == "TreeDB:" or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        if key in DISK_KEYS:
            disks[key] = value.strip()
    return disks


def _parse_selected_stats(md: str) -> Dict[str, str]:
    section = _extract_fenced_section(md, "## TreeDB Selected Stats")
    stats: Dict[str, str] = {}
    for raw in section.splitlines():
        line = raw.strip()
        if not line or line == "TreeDB:" or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        stats[key] = value
        if key.startswith("flush_backlog_coalescing."):
            stats[f"treedb.cache.{key}"] = value
    return stats


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fmt_num(value: Any, digits: int = 3) -> str:
    num = _to_float(value)
    if num is None:
        return ""
    if abs(num) >= 1000 and num.is_integer():
        return f"{int(num):,}"
    if abs(num) >= 1000:
        return f"{num:,.0f}"
    if num.is_integer():
        return str(int(num))
    return f"{num:.{digits}f}"


def _first_top(entries: Iterable[Mapping[str, Any]], n: int = 3) -> str:
    parts: List[str] = []
    for entry in list(entries)[:n]:
        function = str(entry.get("function", "")).replace("|", "\\|")
        flat = entry.get("flat", "")
        flat_pct = entry.get("flat_pct", "")
        if flat_pct != "":
            parts.append(f"{function} ({flat}, {flat_pct}%)")
        else:
            parts.append(f"{function} ({flat})")
    return "<br>".join(parts)


def _profile_top(insights: Mapping[str, Any], kind_key: str, test: str, db_tag: Optional[str] = "treedb", n: int = 3) -> Dict[str, Any]:
    for prof in insights.get(kind_key, []) or []:
        if prof.get("test") != test:
            continue
        if db_tag is not None and prof.get("db_tag") != db_tag:
            continue
        return {
            "total": prof.get("total", ""),
            "top": list(prof.get("top_entries", []) or [])[:n],
            "top_text": _first_top(prof.get("top_entries", []) or [], n=n),
            "path": prof.get("path", ""),
        }
    return {"total": "", "top": [], "top_text": "", "path": ""}


@dataclass
class MatrixRow:
    label: str
    path: str
    commit: str
    concurrency: str
    span_native: str
    backlog_coalescing: str
    cache_mode: str
    note: str
    options: Dict[str, str]
    ops_per_sec: Dict[str, Optional[float]]
    checkpoints: Dict[str, str]
    disk_usage: Dict[str, str]
    counters: Dict[str, str]
    profiles: Dict[str, Dict[str, Any]]


def load_row(path: Path) -> MatrixRow:
    env = _parse_env(path / "variant.env")
    commit = _read_text(path / "COMMIT").strip() or env.get("commit", "")
    label = env.get("label") or path.name
    md = _read_text(path / "benchprof_results.md")
    with (path / "benchprof_results.json").open(encoding="utf-8") as f:
        bench = json.load(f)
    insights: Dict[str, Any] = {}
    if (path / "insights.json").exists():
        with (path / "insights.json").open(encoding="utf-8") as f:
            insights = json.load(f)
    run = bench.get("runs", [{}])[0]
    results = run.get("results", {}) or {}
    stats_by_db = run.get("treedb_stats", {}) or {}
    stats = stats_by_db.get("TreeDB", {}) if isinstance(stats_by_db, Mapping) else {}
    ops: Dict[str, Optional[float]] = {}
    for test in TESTS:
        test_results = results.get(test, {}) or {}
        ops[test] = _to_float(test_results.get("TreeDB"))
    options = _parse_options(md)
    counters = {k: str(stats.get(k, "")) for k in COUNTER_KEYS if k in stats}
    counters.update(_parse_selected_stats(md))
    single_op_spans = _to_float(counters.get("treedb.flush_apply.span_run.single_op_spans_total"))
    target_spans = _to_float(counters.get("treedb.flush_apply.span_run.target_leaf_spans_total"))
    if single_op_spans is not None and target_spans not in (None, 0):
        counters["derived.flush_apply.span_run.single_op_span_ratio"] = f"{single_op_spans / target_spans:.6f}"
    profiles = {
        "cpu_random_write": _profile_top(insights, "cpu_profiles", "random_write"),
        "cpu_batch_random": _profile_top(insights, "cpu_profiles", "batch_random"),
        "alloc_space_random_write": _profile_top(insights, "alloc_space_profiles", "random_write"),
        "alloc_objects_random_write": _profile_top(insights, "alloc_object_profiles", "random_write"),
        "block_random_write": _profile_top(insights, "block_profiles", "random_write"),
        "mutex_random_write": _profile_top(insights, "mutex_profiles", "random_write"),
        "checkpoint_post_run_cpu": _profile_top(insights, "cpu_profiles", "checkpoint/post", db_tag="run_treedb"),
    }
    return MatrixRow(
        label=label,
        path=str(path),
        commit=commit,
        concurrency=_effective_option(env, options, "concurrency", "flush_apply_concurrency"),
        span_native=_effective_option(env, options, "span_native", "flush_apply_span_native"),
        backlog_coalescing=_effective_option(env, options, "backlog_coalescing", "flush_backlog_coalescing"),
        cache_mode=env.get("cache_mode", ""),
        note=env.get("note", ""),
        options=options,
        ops_per_sec=ops,
        checkpoints=_parse_checkpoint_table(md),
        disk_usage=_parse_disk_usage(md),
        counters=counters,
        profiles=profiles,
    )


def load_matrix(root: Path) -> List[MatrixRow]:
    rows: List[MatrixRow] = []
    for child in sorted(root.iterdir()):
        if child.is_dir() and (child / "benchprof_results.json").exists():
            rows.append(load_row(child))
    order = {label: i for i, label in enumerate(ROW_ORDER)}
    rows.sort(key=lambda row: (order.get(row.label, len(order)), row.label))
    return rows


def _pct_delta(value: Optional[float], base: Optional[float]) -> str:
    if value is None or base in (None, 0):
        return ""
    return f"{((value - base) / base) * 100:+.2f}%"


def render_markdown(root: Path, rows: List[MatrixRow], baseline_label: str = "") -> str:
    by_label = {row.label: row for row in rows}
    baseline = by_label.get(baseline_label) if baseline_label else None
    lines: List[str] = []
    lines.append("# TreeDB M14 final-gate matrix summary")
    lines.append("")
    lines.append(f"- artifact root: `{root}`")
    if rows:
        commits = sorted({r.commit for r in rows if r.commit})
        lines.append("- commits: " + ", ".join(f"`{c}`" for c in commits))
    if baseline:
        lines.append(f"- delta baseline: `{baseline.label}`")
    lines.append("")
    lines.append("## Throughput and checkpoint summary")
    lines.append("")
    header = [
        "Row",
        "Concurrency",
        "Span-native",
        "Backlog",
        "Cache",
        "Seq ops/s",
        "Batch ops/s",
        "Random ops/s",
        "Δ random",
        "Checkpoint batch",
        "Checkpoint random",
        "Post-run checkpoint",
        "Artifact",
    ]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join(["---"] + ["---:"] * 11 + ["---"]) + "|")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row.label}`",
                    row.concurrency,
                    row.span_native,
                    row.backlog_coalescing,
                    row.cache_mode,
                    _fmt_num(row.ops_per_sec.get("sequential_write"), 0),
                    _fmt_num(row.ops_per_sec.get("batch_random"), 0),
                    _fmt_num(row.ops_per_sec.get("random_write"), 0),
                    _pct_delta(row.ops_per_sec.get("random_write"), baseline.ops_per_sec.get("random_write") if baseline else None),
                    row.checkpoints.get("Batch Random", ""),
                    row.checkpoints.get("Random Write", ""),
                    row.checkpoints.get("After Run", ""),
                    f"`{row.path}`",
                ]
            )
            + " |"
        )
    lines.append("")
    lines.append("## Rewrite-amplification, span, reducer, and stall counters")
    lines.append("")
    counter_cols = [
        ("old leaf B/op", "treedb.flush_apply.old_leaf_read_decode.bytes_per_op"),
        ("leaf merges/op", "treedb.flush_apply.merge_build.leaf_merges_per_op"),
        ("append frames/op", "treedb.cache.flush_apply.leaf_log_append_frames_per_op"),
        ("apply ops/span", "treedb.flush_apply.span_run.ops_per_span"),
        ("single-op span ratio", "derived.flush_apply.span_run.single_op_span_ratio"),
        ("target split leaves", "treedb.cache.flush_span_run.target_leaves_split_across_chunks_total"),
        ("span-native used ops", "treedb.flush_apply.span_native.used_ops_total"),
        ("fallbacks", "treedb.flush_apply.span_native.fallbacks_total"),
        ("close/chkp fallback ops", "treedb.flush_apply.span_native.fallback.reason.close_or_checkpoint.ops_total"),
        ("root reduce ns", "treedb.flush_apply.root_reduce.ns_total"),
        ("publish ns", "treedb.flush_apply.guarded_publish.ns_total"),
        ("fg assist wait ns", "treedb.cache.flush_apply.foreground_assist_wait_ns_total"),
        ("active assist skips", "treedb.cache.flush_apply.coordinator.active_assist_skips_total"),
        ("stall waits", "treedb.cache.flush_apply.coordinator.stall_waits_total"),
        ("checkpoint bg wait ns", "treedb.cache.checkpoint.active_background_flush_wait_ns_total"),
        ("checkpoint flush_all ns", "treedb.cache.checkpoint.stage.flush_all.total_ns"),
        ("checkpoint leaf sync ns", "treedb.cache.checkpoint.stage.leaf_value_log_sync.total_ns"),
        ("checkpoint reducer ns", "treedb.cache.checkpoint.stage.reducer_publish.total_ns"),
        ("coalesced extra ops", "treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total"),
    ]
    lines.append("| Row | " + " | ".join(c[0] for c in counter_cols) + " |")
    lines.append("|---|" + "|".join("---:" for _ in counter_cols) + "|")
    for row in rows:
        vals = [_fmt_num(row.counters.get(k, ""), 3) for _, k in counter_cols]
        lines.append("| `" + row.label + "` | " + " | ".join(vals) + " |")
    lines.append("")
    lines.append("## Disk usage")
    lines.append("")
    lines.append("| Row | index.db | WAL | value_vlog | leaf_vlog |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in rows:
        lines.append(
            f"| `{row.label}` | {row.disk_usage.get('maindb/index.db', '')} | {row.disk_usage.get('maindb/wal', '')} | {row.disk_usage.get('maindb/value_vlog', '')} | {row.disk_usage.get('maindb/leaf_vlog', '')} |"
        )
    lines.append("")
    lines.append("## Random-write top profile rows")
    lines.append("")
    lines.append("| Row | CPU total/top | alloc-space total/top | alloc-objects total/top | block total/top | mutex total/top | post-run checkpoint CPU total/top |")
    lines.append("|---|---|---|---|---|---|---|")
    for row in rows:
        def cell(name: str) -> str:
            p = row.profiles.get(name, {})
            total = p.get("total", "")
            top = p.get("top_text", "")
            if total and top:
                return f"{total}<br>{top}"
            return str(total or top or "")

        lines.append(
            f"| `{row.label}` | {cell('cpu_random_write')} | {cell('alloc_space_random_write')} | {cell('alloc_objects_random_write')} | {cell('block_random_write')} | {cell('mutex_random_write')} | {cell('checkpoint_post_run_cpu')} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_outputs(root: Path, rows: List[MatrixRow], baseline_label: str) -> None:
    summary_json = {
        "artifact_root": str(root),
        "baseline_label": baseline_label,
        "rows": [asdict(row) for row in rows],
    }
    (root / "m14_matrix_summary.json").write_text(json.dumps(summary_json, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (root / "m14_matrix_summary.md").write_text(render_markdown(root, rows, baseline_label), encoding="utf-8")


def _self_test() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        row = root / "default_unconfigured"
        row.mkdir()
        (row / "variant.env").write_text(
            "label=default_unconfigured\ncommit=abc123\nconcurrency=default\nspan_native=false\nbacklog_coalescing=false\ncache_mode=default\nnote=test row\n",
            encoding="utf-8",
        )
        (row / "COMMIT").write_text("abc123\n", encoding="utf-8")
        (row / "benchprof_results.json").write_text(
            json.dumps(
                {
                    "runs": [
                        {
                            "results": {
                                "sequential_write": {"TreeDB": 10.0},
                                "batch_random": {"TreeDB": 20.0},
                                "random_write": {"TreeDB": 30.0},
                            },
                            "treedb_stats": {
                                "TreeDB": {
                                    "treedb.flush_apply.old_leaf_read_decode.bytes_per_op": "1.5",
                                    "treedb.flush_apply.merge_build.leaf_merges_per_op": "0.25",
                                    "treedb.cache.flush_apply.leaf_log_append_frames_per_op": "0.25",
                                    "treedb.flush_apply.span_native.used_ops_total": "100",
                                    "treedb.flush_apply.span_native.fallbacks_total": "0",
                                    "treedb.cache.checkpoint.stage.flush_all.total_ns": "303",
                                }
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (row / "benchprof_results.md").write_text(
            """
## Resolved TreeDB Options

```text
flush_apply_concurrency=4
flush_apply_span_native=true
flush_backlog_coalescing=true
```

## Checkpoint Time (Between Tests + Post-run)

```text
     Before Test    TreeDB
----------------  --------
Sequential Write     100µs
    Batch Random  200.00ms
    Random Write     3.00s
       After Run     4.00s
```

## Disk Usage (End of Run)

```text
TreeDB:
  maindb/index.db: 1 MiB
  maindb/wal: total=12 B files=1 other=12 B
  maindb/value_vlog: total=0 B files=1
  maindb/leaf_vlog: total=2 MiB files=1 value=2 MiB
```

## TreeDB Selected Stats (End of Run)

```text
TreeDB:
  treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total: 42
```
""",
            encoding="utf-8",
        )
        (row / "insights.json").write_text(
            json.dumps(
                {
                    "cpu_profiles": [
                        {
                            "test": "random_write",
                            "db_tag": "treedb",
                            "total": "1s",
                            "top_entries": [{"function": "f", "flat": "1s", "flat_pct": 100}],
                        },
                        {
                            "test": "checkpoint/post",
                            "db_tag": "run_treedb",
                            "total": "2s",
                            "top_entries": [{"function": "ckpt", "flat": "2s", "flat_pct": 100}],
                        },
                    ],
                    "alloc_space_profiles": [],
                    "alloc_object_profiles": [],
                    "block_profiles": [],
                    "mutex_profiles": [],
                }
            ),
            encoding="utf-8",
        )
        rows = load_matrix(root)
        assert len(rows) == 1, rows
        got = rows[0]
        assert got.label == "default_unconfigured"
        assert got.concurrency == "4", got
        assert got.span_native == "true", got
        assert got.backlog_coalescing == "true", got
        assert got.ops_per_sec["random_write"] == 30.0
        assert got.checkpoints["After Run"] == "4.00s", got.checkpoints
        assert got.disk_usage["maindb/leaf_vlog"].startswith("total=2 MiB")
        assert got.counters["treedb.cache.flush_backlog_coalescing.admitted_extra_ops_total"] == "42"
        assert got.counters["treedb.cache.checkpoint.stage.flush_all.total_ns"] == "303"
        md = render_markdown(root, rows, "default_unconfigured")
        assert "TreeDB M14 final-gate matrix summary" in md
        assert "default_unconfigured" in md
        assert "| `default_unconfigured` | 4 | true | true |" in md
        assert "checkpoint flush_all ns" in md
        write_outputs(root, rows, "default_unconfigured")
        assert (root / "m14_matrix_summary.json").exists()
        assert (root / "m14_matrix_summary.md").exists()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, help="matrix artifact root")
    parser.add_argument("--baseline-label", default="", help="row label used for percent deltas")
    parser.add_argument("--self-test", action="store_true", help="run built-in parser smoke test")
    args = parser.parse_args(argv)
    if args.self_test:
        _self_test()
        print("self-test passed")
        return 0
    if args.root is None:
        parser.error("root is required unless --self-test is used")
    root = args.root.resolve()
    rows = load_matrix(root)
    if not rows:
        print(f"no benchmark rows found under {root}", file=sys.stderr)
        return 1
    write_outputs(root, rows, args.baseline_label)
    print(root / "m14_matrix_summary.md")
    print(root / "m14_matrix_summary.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
