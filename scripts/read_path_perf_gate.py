#!/usr/bin/env python3
"""
Read-path performance gate harness.

Method:
- Build unified-bench binaries for baseline and candidate refs.
- Run interleaved A/B rounds for valsize in {85,1}.
- Primary estimator: best-of-5 middle-3 effect.
- Auto-extend by +5 rounds up to max rounds when ambiguous.
- Report paired bootstrap CI95 over all collected rounds.
- Classify: approve / approve_with_revisions / reject.
- Optional microbench compare with benchstat.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union


RR_LINE = re.compile(r"^Random Read / TreeDB = ([0-9,]+)$")
RB_LINE = re.compile(r"^Random Read \(Batch\) / TreeDB = ([0-9,]+)$")
cleanup_stale_min_age_seconds = 30 * 60


@dataclass
class RunPoint:
    rr: int
    rb: int
    secs: float
    log_path: str


def run(
    cmd: List[str],
    cwd: Path,
    capture: bool = False,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    try:
        # capture=True mirrors stderr into stdout because callers parse a single
        # text stream from unified-bench output.
        kwargs: Dict[str, object] = {"text": True}
        if capture:
            kwargs["stdout"] = subprocess.PIPE
            kwargs["stderr"] = subprocess.STDOUT
        else:
            kwargs["stderr"] = subprocess.PIPE
        if env is not None:
            kwargs["env"] = env
        return subprocess.run(cmd, cwd=str(cwd), check=True, **kwargs)
    except subprocess.CalledProcessError as exc:
        print(f"command failed: {' '.join(cmd)}", file=sys.stderr)
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        raise


def ensure_ref(args: argparse.Namespace, ref: str) -> None:
    try:
        run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=args.repo, capture=True)
        return
    except subprocess.CalledProcessError:
        print(f"warning: reference '{ref}' not present locally; attempting to fetch it", file=sys.stderr)

    remote: Optional[str] = None
    if "/" in ref:
        candidate = ref.split("/", 1)[0]
        try:
            run(["git", "remote", "get-url", candidate], cwd=args.repo, capture=True)
            remote = candidate
        except subprocess.CalledProcessError:
            remote = None

    if remote is not None:
        try:
            print(f"info: fetching remote '{remote}' (targeted fetch)", file=sys.stderr)
            run(["git", "fetch", remote, "--prune"], cwd=args.repo)
            run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=args.repo, capture=True)
            return
        except subprocess.CalledProcessError:
            print(
                f"warning: targeted fetch from '{remote}' did not resolve '{ref}'; falling back to --all",
                file=sys.stderr,
            )

    print("warning: running 'git fetch --all --prune'; this may be expensive", file=sys.stderr)
    run(["git", "fetch", "--all", "--prune"], cwd=args.repo)
    run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=args.repo, capture=True)


def parse_table_last_numeric(prefix: str, line: str) -> Optional[int]:
    if not (line.startswith(prefix) or line.startswith("| " + prefix) or line.startswith("|" + prefix)):
        return None

    # Plain text line: take trailing number.
    plain = line.strip()
    if "|" not in plain:
        if not plain.startswith(prefix):
            return None
        m = re.search(r"([0-9][0-9,]*)\s*$", plain)
        if not m:
            return None
        value_str = m.group(1)
        value = int(value_str.replace(",", ""))
        return value if value > 0 else None

    # Pipe table-style row: parse the last non-empty cell (last cell may be empty due
    # to trailing pipe).
    cells = [part.strip() for part in plain.strip("|").split("|")]
    suffix = ""
    for part in reversed(cells):
        part = part.strip()
        if part:
            suffix = part
            break
    if not suffix:
        return None

    if not cells[0].startswith(prefix):
        return None

    m = re.search(r"([0-9][0-9,]*)\s*$", suffix)
    if not m:
        return None
    value_str = m.group(1)
    value = int(value_str.replace(",", ""))
    if value <= 0:
        return None
    return value


def parse_table_cell_int(raw: str) -> Optional[int]:
    m = re.search(r"([0-9][0-9,]*)\s*$", raw.strip())
    if not m:
        return None
    value = int(m.group(1).replace(",", ""))
    return value if value > 0 else None


def _parse_metric_from_row(
    metric_name: str,
    row: List[str],
    target_db_present: bool,
    tree_col_idx: Optional[int],
) -> Optional[int]:
    if len(row) < 2 or row[0].strip() != metric_name:
        return None

    if target_db_present:
        if tree_col_idx is None:
            return None
        idx = tree_col_idx
        if idx < 0 or idx >= len(row):
            return None
        return parse_table_cell_int(row[idx])

    return parse_table_cell_int(row[-1])


def _normalize_db_names(raw_dbs: Optional[Union[str, List[str]]]) -> List[str]:
    if raw_dbs is None:
        return ["treedb"]
    if isinstance(raw_dbs, str):
        parts = [d.strip() for d in raw_dbs.split(",") if d.strip()]
    else:
        parts = [d.strip() for d in raw_dbs if d.strip()]
    if not parts:
        return ["treedb"]
    return [p.lower() for p in parts]


def parse_metrics(text: str, dbs: Union[str, List[str]] = "treedb", warn_on_fallback: bool = False) -> Tuple[int, int]:
    rr = None
    rb = None
    rr_source: Optional[str] = None
    rb_source: Optional[str] = None
    target_db = "treedb"
    target_db_present = False
    tree_col_idx: Optional[int] = None
    normalized_dbs = _normalize_db_names(dbs)
    if "all" in normalized_dbs or target_db in normalized_dbs:
        target_db_present = True

    for raw in text.splitlines():
        line = raw.strip()
        m = RR_LINE.match(line)
        if m:
            rr = int(m.group(1).replace(",", ""))
            rr_source = "regex"
            continue
        m = RB_LINE.match(line)
        if m:
            rb = int(m.group(1).replace(",", ""))
            rb_source = "regex"
            continue

        # Fixed-width markdown tables in unified-bench output use repeated spaces.
        if "|" not in line:
            if line.startswith("```"):
                continue
            cols = [part.strip() for part in re.split(r"\s{2,}", raw.rstrip()) if part.strip()]
            if not cols:
                continue

            if cols[0].strip().lower() == "test" and len(cols) >= 2:
                lower_cols = [c.lower() for c in cols]
                if target_db in lower_cols:
                    tree_col_idx = lower_cols.index(target_db)
                continue

            metric = cols[0].strip()
            if rr is None:
                parsed = _parse_metric_from_row(
                    "Random Read",
                    cols,
                    target_db_present,
                    tree_col_idx,
                )
                if parsed is not None:
                    rr = parsed
                    rr_source = "table_whitespace"
                    continue

            if rb is None:
                parsed = _parse_metric_from_row(
                    "Random Read (Batch)",
                    cols,
                    target_db_present,
                    tree_col_idx,
                )
                if parsed is not None:
                    rb = parsed
                    rb_source = "table_whitespace"
                    continue

            if rr is None and metric == "Random Read":
                parsed = parse_table_last_numeric("Random Read", line)
                if parsed is not None:
                    rr = parsed
                    rr_source = "plain_fallback"
                    continue
            if rb is None and metric == "Random Read (Batch)":
                parsed = parse_table_last_numeric("Random Read (Batch)", line)
                if parsed is not None:
                    rb = parsed
                    rb_source = "plain_fallback"
                    continue
            continue

        if "|" in line:
            row = [part.strip() for part in line.strip("|").split("|") if part.strip()]
            if not row:
                continue

            if row[0].strip().lower() == "test" and len(row) >= 2:
                lower_cells = [c.lower() for c in row]
                if tree_col_idx is None and target_db in lower_cells:
                    tree_col_idx = lower_cells.index(target_db)
                elif tree_col_idx is None and len(row) == 2 and row[1].lower() == target_db:
                    tree_col_idx = 1

            if rr is None:
                parsed = _parse_metric_from_row(
                    "Random Read",
                    row,
                    target_db_present,
                    tree_col_idx,
                )
                if parsed is not None:
                    rr = parsed
                    rr_source = "table_pipe"

            if rb is None:
                parsed = _parse_metric_from_row(
                    "Random Read (Batch)",
                    row,
                    target_db_present,
                    tree_col_idx,
                )
                if parsed is not None:
                    rb = parsed
                    rb_source = "table_pipe"

    if rr is None or rb is None:
        preview = text[:200].replace("\n", "\\n")
        raise RuntimeError(
            "failed to parse Random Read metrics from unified-bench output "
            f"(text length={len(text)}, preview={preview!r})"
        )
    if rr <= 0 or rb <= 0:
        preview = text[:200].replace("\n", "\\n")
        raise RuntimeError(
            "parsed Random Read metrics must be positive "
            f"(rr={rr}, rb={rb}, text length={len(text)}, preview={preview!r})"
        )
    if warn_on_fallback:
        if rr_source != "regex":
            print(f"warning: parsed Random Read via fallback parser source='{rr_source}'", file=sys.stderr)
        if rb_source != "regex":
            print(f"warning: parsed Random Read (Batch) via fallback parser source='{rb_source}'", file=sys.stderr)
    return rr, rb


def middle3(values: List[int]) -> float:
    if len(values) < 5:
        raise ValueError(f"need at least 5 samples for middle-3 (got {len(values)})")
    # Best-of-5 middle-3 estimator: take top-5 values, then average their
    # middle 3 (higher is better).
    best_five = sorted(values, reverse=True)[:5]
    best_five.sort()
    return float(sum(best_five[1:4])) / 3.0


def bootstrap_ci95_effect_pct(base_vals: List[int], cand_vals: List[int], n_boot: int = 20000, seed: int = 1) -> Tuple[float, float]:
    if len(base_vals) != len(cand_vals):
        raise ValueError(f"paired sample count mismatch (base={len(base_vals)}, cand={len(cand_vals)})")
    if len(base_vals) < 5:
        raise ValueError(f"need at least 5 paired samples for bootstrap CI95 (got {len(base_vals)})")

    rng = random.Random(seed)
    paired = list(zip(base_vals, cand_vals))
    n = len(paired)
    boots: List[float] = []
    for _ in range(n_boot):
        sampled_base = []
        sampled_cand = []
        for __ in range(n):
            b, c = paired[rng.randrange(n)]
            sampled_base.append(b)
            sampled_cand.append(c)
        m3_base = middle3(sampled_base)
        m3_cand = middle3(sampled_cand)
        eff = (m3_cand - m3_base) / m3_base * 100.0 if m3_base > 0 else 0.0
        boots.append(eff)
    boots.sort()
    lo = boots[int(0.025 * n_boot)]
    hi = boots[int(0.975 * n_boot)]
    return lo, hi


def classify_metric(effect_middle3_pct: float, ci_lo: float, ci_hi: float, thresh: float = 1.0) -> str:
    if ci_lo > thresh:
        return "improve"
    if ci_hi < -thresh:
        return "regress"
    if abs(effect_middle3_pct) < thresh and ci_lo <= 0 <= ci_hi:
        return "neutral"
    return "uncertain"


def metric_summary(
    base_runs: List[RunPoint],
    cand_runs: List[RunPoint],
    metric: str,
    thresh: float = 1.0,
    seed: int = 1,
    n_boot: int = 20000,
) -> Dict[str, Union[float, str, List[float]]]:
    base_vals = [getattr(r, metric) for r in base_runs]
    cand_vals = [getattr(r, metric) for r in cand_runs]
    m3_base = middle3(base_vals)
    m3_cand = middle3(cand_vals)
    eff = (m3_cand - m3_base) / m3_base * 100.0 if m3_base > 0 else 0.0
    ci_lo, ci_hi = bootstrap_ci95_effect_pct(base_vals, cand_vals, n_boot=n_boot, seed=seed)
    cls = classify_metric(eff, ci_lo, ci_hi, thresh=thresh)
    return {
        "base_middle3": m3_base,
        "candidate_middle3": m3_cand,
        "effect_middle3_pct": eff,
        "ci95_pct": [ci_lo, ci_hi],
        "classification": cls,
        "base_mean_all": float(sum(base_vals)) / len(base_vals),
        "candidate_mean_all": float(sum(cand_vals)) / len(cand_vals),
        "base_stdev_all": statistics.pstdev(base_vals),
        "candidate_stdev_all": statistics.pstdev(cand_vals),
    }


def needs_more_rounds(
    base_runs: List[RunPoint],
    cand_runs: List[RunPoint],
    thresh: float,
    seed: int,
    quick_n_boot: int,
) -> bool:
    # Use a smaller bootstrap budget for intermediate stopping checks; final
    # reporting re-runs with the full bootstrap budget.
    rr = metric_summary(base_runs, cand_runs, "rr", thresh=thresh, seed=seed, n_boot=quick_n_boot)
    rb = metric_summary(base_runs, cand_runs, "rb", thresh=thresh, seed=seed, n_boot=quick_n_boot)
    return rr["classification"] == "uncertain" or rb["classification"] == "uncertain"


def run_unified_once(
    bin_path: Path,
    valsize: int,
    tests: str,
    args: argparse.Namespace,
    log_file: Path,
    run_seed: int,
) -> RunPoint:
    cmd = [
        str(bin_path),
        "-dbs",
        args.dbs,
        "-profile",
        args.profile,
        "-keys",
        str(args.keys),
        "-format",
        "markdown",
        "-checkpoint-between-tests",
        "-treedb-force-value-pointers=false",
        "-test",
        tests,
        "-seed",
        str(run_seed),
        "-progress=false",
        "-valsize",
        str(valsize),
    ]
    t0 = time.time()
    p = run(cmd, cwd=args.repo, capture=True)
    log_file.write_text(p.stdout)
    rr, rb = parse_metrics(p.stdout, args.dbs, warn_on_fallback=True)
    return RunPoint(rr=rr, rb=rb, secs=time.time() - t0, log_path=str(log_file))


def build_binaries(args: argparse.Namespace, out_dir: Path) -> Tuple[Path, Path, Dict[str, str]]:
    wt_root = out_dir / "worktrees"
    wt_root.mkdir(parents=True, exist_ok=True)
    # Recover from stale worktrees left behind by an interrupted earlier run.
    cleanup_worktrees(args, out_dir)
    ensure_ref(args, args.baseline_ref)
    ensure_ref(args, args.candidate_ref)
    baseline_wt = wt_root / "baseline"
    candidate_wt = wt_root / "candidate"

    run(["git", "worktree", "add", "--detach", str(baseline_wt), args.baseline_ref], cwd=args.repo)
    run(["git", "worktree", "add", "--detach", str(candidate_wt), args.candidate_ref], cwd=args.repo)

    bins = out_dir / "bin"
    bins.mkdir(parents=True, exist_ok=True)
    base_bin = bins / "unified-bench-baseline"
    cand_bin = bins / "unified-bench-candidate"

    run(["go", "build", "-o", str(base_bin), "./cmd/unified_bench"], cwd=baseline_wt)
    run(["go", "build", "-o", str(cand_bin), "./cmd/unified_bench"], cwd=candidate_wt)

    hashes = {
        "baseline": run(["git", "rev-parse", "HEAD"], cwd=baseline_wt, capture=True).stdout.strip(),
        "candidate": run(["git", "rev-parse", "HEAD"], cwd=candidate_wt, capture=True).stdout.strip(),
    }
    return base_bin, cand_bin, hashes


def cleanup_worktrees(args: argparse.Namespace, out_dir: Path) -> None:
    wt_root = out_dir / "worktrees"
    for name in ("baseline", "candidate"):
        wt = wt_root / name
        if wt.exists():
            try:
                run(["git", "worktree", "remove", "--force", str(wt)], cwd=args.repo)
            except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
                print(f"Warning: failed to remove worktree {wt}: {exc!r}", file=sys.stderr)


def cleanup_stale_gate_worktrees(args: argparse.Namespace, keep_out_dir: Path) -> None:
    root = args.repo / "artifacts" / "read_gate"
    if not root.exists():
        return
    for wt_root in root.glob("gate_*/worktrees"):
        gate_dir = wt_root.parent
        if gate_dir == keep_out_dir:
            continue
        try:
            age_seconds = max(0.0, time.time() - gate_dir.stat().st_mtime)
        except (FileNotFoundError, OSError):
            continue
        if age_seconds < cleanup_stale_min_age_seconds:
            print(
                f"info: skipping stale cleanup for {gate_dir} (age={age_seconds:.0f}s < {cleanup_stale_min_age_seconds}s)",
                file=sys.stderr,
            )
            continue
        for name in ("baseline", "candidate"):
            wt = wt_root / name
            if not wt.exists():
                continue
            try:
                run(["git", "worktree", "remove", "--force", str(wt)], cwd=args.repo)
            except (subprocess.CalledProcessError, FileNotFoundError, OSError) as exc:
                print(f"warning: stale worktree cleanup failed for {wt}: {exc!r}", file=sys.stderr)


def overall_decision(per_valsize: Dict[str, Dict], max_rounds: int) -> str:
    # "approve_with_revisions" means inconclusive signal (not a regression).
    any_improve = False
    any_regress = False
    any_uncertain = False

    for _, vs in per_valsize.items():
        rr_cls = vs["metrics"]["rr"]["classification"]
        rb_cls = vs["metrics"]["rb"]["classification"]

        if rr_cls == "improve" or rb_cls == "improve":
            any_improve = True
        if rr_cls == "regress" or rb_cls == "regress":
            any_regress = True
        if rr_cls == "uncertain" or rb_cls == "uncertain":
            any_uncertain = True

    maxed_without_upside = (
        any_uncertain and not any_improve and all(vs["n_per_side"] >= max_rounds for vs in per_valsize.values())
    )

    if any_regress:
        return "reject"
    if maxed_without_upside:
        return "reject"
    if any_uncertain:
        return "approve_with_revisions"
    if any_improve:
        return "approve"
    return "approve_with_revisions"


def run_broad_sanity(bin_path: Path, args: argparse.Namespace, out_file: Path) -> Dict[str, Union[int, str]]:
    # Broad sanity intentionally records only TreeDB metrics for decision context.
    dbs = args.sanity_dbs
    cmd = [
        str(bin_path),
        "-dbs",
        dbs,
        "-profile",
        args.profile,
        "-keys",
        str(args.keys),
        "-format",
        "markdown",
        "-checkpoint-between-tests",
        "-treedb-force-value-pointers=false",
        "-test",
        "all",
        "-seed",
        str(args.seed),
        "-progress=false",
        "-valsize",
        str(args.sanity_valsize),
    ]
    p = run(cmd, cwd=args.repo, capture=True)
    out_file.write_text(p.stdout)
    rr_treedb, rb_treedb = parse_metrics(p.stdout, dbs)

    return {
        "random_read_treedb": rr_treedb,
        "random_read_batch_treedb": rb_treedb,
        "log": str(out_file),
    }


def run_microbench_compare(args: argparse.Namespace, out_dir: Path) -> Optional[Dict[str, str]]:
    if not args.microbench:
        return None

    bench_pattern = "|".join(args.microbench)
    wt_root = out_dir / "worktrees"
    baseline_wt = wt_root / "baseline"
    candidate_wt = wt_root / "candidate"
    micro_dir = out_dir / "microbench"
    micro_dir.mkdir(parents=True, exist_ok=True)

    base_txt = micro_dir / "baseline.txt"
    cand_txt = micro_dir / "candidate.txt"
    stat_txt = micro_dir / "benchstat.txt"

    cmd = [
        "go",
        "test",
        "./TreeDB/node",
        "-run",
        "^$",
        "-bench",
        bench_pattern,
        "-benchmem",
        "-count",
        str(args.microbench_count),
    ]

    env = os.environ.copy()
    env["GOMAXPROCS"] = str(args.microbench_gomaxprocs)

    p1 = run(cmd, cwd=baseline_wt, capture=True, env=env)
    base_txt.write_text(p1.stdout)
    p2 = run(cmd, cwd=candidate_wt, capture=True, env=env)
    cand_txt.write_text(p2.stdout)

    try:
        p3 = run(["benchstat", str(base_txt), str(cand_txt)], cwd=args.repo, capture=True)
    except FileNotFoundError:
        error_message = (
            "Error: 'benchstat' command not found. Install it with: "
            "go install golang.org/x/perf/cmd/benchstat@latest"
        )
        print(error_message, file=sys.stderr)
        stat_txt.write_text(error_message + "\n")
        return {
            "pattern": bench_pattern,
            "baseline_output": str(base_txt),
            "candidate_output": str(cand_txt),
            "benchstat": str(stat_txt),
            "status": "error",
            "error_message": error_message,
        }
    except subprocess.CalledProcessError as exc:
        error_message = f"Error: benchstat failed: {exc}"
        print(error_message, file=sys.stderr)
        stat_txt.write_text(error_message + "\n")
        return {
            "pattern": bench_pattern,
            "baseline_output": str(base_txt),
            "candidate_output": str(cand_txt),
            "benchstat": str(stat_txt),
            "status": "error",
            "error_message": error_message,
        }
    stat_txt.write_text(p3.stdout)

    return {
        "pattern": bench_pattern,
        "baseline_output": str(base_txt),
        "candidate_output": str(cand_txt),
        "benchstat": str(stat_txt),
        "status": "ok",
    }


def _warn_if_repo_dirty(repo: Path) -> None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "status", "--porcelain"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError:
        return

    if proc.returncode != 0:
        return
    if proc.stdout.strip():
        print(
            "warning: repository has uncommitted changes; benchmark results may be less reproducible",
            file=sys.stderr,
        )


def _next_log_file(vdir: Path, side: str, run_index_hint: int) -> Path:
    run_index = max(1, run_index_hint)
    while True:
        path = vdir / f"run_{side}_{run_index:02d}.md"
        if not path.exists():
            return path
        run_index += 1


def compose_run_seed(base_seed: int, valsize: int, turn: int, side: str) -> int:
    side_bit = 0 if side == "base" else 1
    raw = f"{base_seed}:{valsize}:{turn}:{side_bit}".encode("utf-8")
    digest = hashlib.blake2b(raw, digest_size=8).digest()
    seed = int.from_bytes(digest, byteorder="big", signed=False) & ((1 << 63) - 1)
    return seed if seed > 0 else 1


def _rebalance_pairs(base_runs: List[RunPoint], cand_runs: List[RunPoint]) -> None:
    while len(base_runs) > len(cand_runs):
        base_runs.pop()
    while len(cand_runs) > len(base_runs):
        cand_runs.pop()


def _run_side_once(
    side: str,
    turn: int,
    valsize: int,
    vdir: Path,
    base_runs: List[RunPoint],
    cand_runs: List[RunPoint],
    base_bin: Path,
    cand_bin: Path,
    args: argparse.Namespace,
) -> None:
    hint = (len(base_runs) + 1) if side == "base" else (len(cand_runs) + 1)
    log_file = _next_log_file(vdir, side, hint)
    try:
        rec = run_unified_once(
            base_bin if side == "base" else cand_bin,
            valsize,
            args.tests,
            args,
            log_file,
            compose_run_seed(args.seed, valsize, turn, side),
        )
    except Exception:
        _rebalance_pairs(base_runs, cand_runs)
        raise

    if side == "base":
        base_runs.append(rec)
        print(f"  r{len(base_runs):02d} base rr={rec.rr} rb={rec.rb} t={rec.secs:.2f}s", flush=True)
    else:
        cand_runs.append(rec)
        print(f"  r{len(cand_runs):02d} cand rr={rec.rr} rb={rec.rb} t={rec.secs:.2f}s", flush=True)


def render_summary_md(result: Dict, path: Path) -> None:
    lines: List[str] = []
    lines.append("# Read Path Perf Gate")
    lines.append("")
    lines.append(f"- baseline: `{result['meta']['baseline_ref']}` (`{result['meta']['baseline_hash'][:12]}`)")
    lines.append(f"- candidate: `{result['meta']['candidate_ref']}` (`{result['meta']['candidate_hash'][:12]}`)")
    lines.append(f"- decision: **{result['decision']}**")
    lines.append("")
    lines.append("## Primary Gate")
    lines.append("")
    lines.append("| ValSize | n/side | Metric | Base middle-3 | Cand middle-3 | Effect | CI95 | Class |")
    lines.append("|---:|---:|---|---:|---:|---:|---:|---|")
    for valsize in result["valsize_order"]:
        vs = result["valsizes"][str(valsize)]
        n = vs["n_per_side"]
        for metric_key, metric_name in (("rr", "Random Read"), ("rb", "Random Read (Batch)")):
            m = vs["metrics"][metric_key]
            lo, hi = m["ci95_pct"]
            effect = m["effect_middle3_pct"]
            effect_str = f"{effect:+.2f}%" if math.isfinite(effect) else "N/A"
            lines.append(
                f"| {valsize} | {n} | {metric_name} | {m['base_middle3']:,.0f} | {m['candidate_middle3']:,.0f} | {effect_str} | [{lo:+.2f}, {hi:+.2f}] | {m['classification']} |"
            )
    lines.append("")
    lines.append("## Broad Sanity (Single Run)")
    lines.append("")
    b = result["broad_sanity"]["baseline"]
    c = result["broad_sanity"]["candidate"]
    lines.append("| Side | RR(TreeDB) | RR Batch(TreeDB) | Log |")
    lines.append("|---|---:|---:|---|")
    lines.append(f"| baseline | {b['random_read_treedb']:,} | {b['random_read_batch_treedb']:,} | `{b['log']}` |")
    lines.append(f"| candidate | {c['random_read_treedb']:,} | {c['random_read_batch_treedb']:,} | `{c['log']}` |")

    microbench = result.get("microbench")
    if isinstance(microbench, dict) and "pattern" in microbench:
        lines.append("")
        lines.append("## Microbench")
        lines.append("")
        lines.append(f"- bench pattern: `{microbench['pattern']}`")
        if microbench.get("status") == "ok":
            lines.append(f"- benchstat: `{microbench['benchstat']}`")
        else:
            lines.append(f"- status: **error**")
            lines.append(f"- error: `{microbench.get('error_message', 'unknown error')}`")
            lines.append(f"- baseline output: `{microbench['baseline_output']}`")
            lines.append(f"- candidate output: `{microbench['candidate_output']}`")

    path.write_text("\n".join(lines) + "\n")


def validate_re2_compatible(pattern: str) -> None:
    # Go test -bench uses RE2 syntax. Reject common constructs that Python
    # accepts but RE2 does not.
    unsupported = (
        ("(?<=", "lookbehind"),
        ("(?<!", "lookbehind"),
        ("(?=", "lookahead"),
        ("(?!", "lookahead"),
        ("(?P=", "backreference"),
    )
    for token, label in unsupported:
        if token in pattern:
            raise SystemExit(
                f"--microbench pattern uses {label}, unsupported by Go RE2 engine: {pattern!r}"
            )
    if re.search(r"\\[1-9]", pattern):
        raise SystemExit(
            f"--microbench pattern uses numeric backreferences, unsupported by Go RE2 engine: {pattern!r}"
        )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read-path performance gate harness")
    ap.add_argument("--repo", type=Path, default=Path.cwd())
    ap.add_argument("--baseline-ref", default="origin/main")
    ap.add_argument("--candidate-ref", default="HEAD")
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--keys", type=int, default=500000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dbs", default="treedb")
    ap.add_argument("--profile", default="fast")
    ap.add_argument("--tests", default="random_read,random_read_batch")
    ap.add_argument("--sanity-dbs", default="leveldb,treedb")
    ap.add_argument("--valsizes", default="85,1")
    ap.add_argument("--initial-rounds", type=int, default=5)
    ap.add_argument("--step-rounds", type=int, default=5)
    ap.add_argument("--max-rounds", type=int, default=25)
    ap.add_argument("--practical-threshold-pct", type=float, default=1.0)
    ap.add_argument("--sanity-valsize", type=int, default=85)
    ap.add_argument("--bootstrap-samples", type=int, default=20000)
    ap.add_argument("--bootstrap-samples-quick", type=int, default=2000)
    ap.add_argument("--cleanup-stale-worktrees", action="store_true")
    ap.add_argument("--microbench", action="append", default=[])
    ap.add_argument("--microbench-count", type=int, default=20)
    ap.add_argument("--microbench-gomaxprocs", type=int, default=1)
    args = ap.parse_args()
    if args.initial_rounds <= 0:
        raise SystemExit("--initial-rounds must be > 0")
    if args.initial_rounds < 5:
        raise SystemExit("--initial-rounds must be >= 5 to support middle-3 on 5 samples")
    if args.step_rounds <= 0:
        raise SystemExit("--step-rounds must be > 0")
    if args.max_rounds <= 0:
        raise SystemExit("--max-rounds must be > 0")
    if args.initial_rounds > args.max_rounds:
        raise SystemExit("--initial-rounds must be <= --max-rounds")
    if args.keys <= 0:
        raise SystemExit("--keys must be > 0")
    if args.sanity_valsize <= 0:
        raise SystemExit("--sanity-valsize must be > 0")
    if args.microbench_count <= 0:
        raise SystemExit("--microbench-count must be > 0")
    if args.microbench_gomaxprocs <= 0:
        raise SystemExit("--microbench-gomaxprocs must be > 0")
    if args.practical_threshold_pct <= 0:
        raise SystemExit("--practical-threshold-pct must be > 0")
    if args.bootstrap_samples <= 0:
        raise SystemExit("--bootstrap-samples must be > 0")
    if args.bootstrap_samples_quick <= 0:
        raise SystemExit("--bootstrap-samples-quick must be > 0")
    if args.bootstrap_samples_quick > args.bootstrap_samples:
        raise SystemExit("--bootstrap-samples-quick must be <= --bootstrap-samples")
    for pattern in args.microbench:
        p = pattern.strip()
        if not p:
            raise SystemExit("--microbench patterns must be non-empty")
        if "\n" in p or "\r" in p:
            raise SystemExit("--microbench patterns must be single-line regexes")
        if len(p) > 256:
            raise SystemExit("--microbench pattern too long (max 256 chars)")
        try:
            re.compile(p)
        except re.error as exc:
            raise SystemExit(f"--microbench pattern is not a valid regular expression: {p!r}: {exc}") from exc
        validate_re2_compatible(p)
    return args


def main() -> int:
    args = parse_args()
    args.repo = args.repo.resolve()
    _warn_if_repo_dirty(args.repo)
    ts = time.strftime("%Y%m%d%H%M%S")
    out_dir = args.out_dir.resolve() if args.out_dir else (args.repo / "artifacts" / "read_gate" / f"gate_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.cleanup_stale_worktrees:
        cleanup_stale_gate_worktrees(args, out_dir)

    valsizes: List[int] = []
    for token in [v.strip() for v in args.valsizes.split(",") if v.strip()]:
        try:
            value = int(token)
        except ValueError as exc:
            raise SystemExit(f"invalid --valsizes entry {token!r}: {exc}") from exc
        if value <= 0:
            raise SystemExit(f"--valsizes entries must be > 0 (got {value})")
        valsizes.append(value)
    if not valsizes:
        raise SystemExit("--valsizes produced empty list")

    result: Dict = {
        "meta": {
            "timestamp": ts,
            "repo": str(args.repo),
            "baseline_ref": args.baseline_ref,
            "candidate_ref": args.candidate_ref,
            "keys": args.keys,
            "seed": args.seed,
            "tests": args.tests,
            "profile": args.profile,
            "dbs": args.dbs,
            "sanity_dbs": args.sanity_dbs,
            "initial_rounds": args.initial_rounds,
            "step_rounds": args.step_rounds,
            "max_rounds": args.max_rounds,
            "practical_threshold_pct": args.practical_threshold_pct,
            "bootstrap_samples": args.bootstrap_samples,
            "bootstrap_samples_quick": args.bootstrap_samples_quick,
        },
        "valsizes": {},
        "valsize_order": valsizes,
    }

    base_bin = cand_bin = None
    hashes = {}
    try:
        base_bin, cand_bin, hashes = build_binaries(args, out_dir)
        result["meta"]["baseline_hash"] = hashes["baseline"]
        result["meta"]["candidate_hash"] = hashes["candidate"]

        for valsize in valsizes:
            print(f"== valsize={valsize} ==", flush=True)
            vdir = out_dir / f"valsize_{valsize}"
            vdir.mkdir(parents=True, exist_ok=True)

            base_runs: List[RunPoint] = []
            cand_runs: List[RunPoint] = []

            while True:
                add_n = args.initial_rounds if len(base_runs) == 0 else args.step_rounds
                remaining = args.max_rounds - len(base_runs)
                if remaining <= 0:
                    break
                add_n = min(add_n, remaining)
                start_turn = len(base_runs)
                for i in range(add_n):
                    turn = start_turn + i
                    order = ("base", "cand") if (turn % 2 == 0) else ("cand", "base")
                    for side in order:
                        _run_side_once(
                            side,
                            turn,
                            valsize,
                            vdir,
                            base_runs,
                            cand_runs,
                            base_bin,
                            cand_bin,
                            args,
                        )

                if len(base_runs) < 5:
                    continue
                if not needs_more_rounds(
                    base_runs,
                    cand_runs,
                    args.practical_threshold_pct,
                    args.seed,
                    args.bootstrap_samples_quick,
                ):
                    break
                if len(base_runs) >= args.max_rounds:
                    break
                print(f"  ambiguous at n={len(base_runs)}; extending +{args.step_rounds}", flush=True)

            vs = {
                "n_per_side": len(base_runs),
                "runs": {
                    "base": [r.__dict__ for r in base_runs],
                    "candidate": [r.__dict__ for r in cand_runs],
                },
                "metrics": {
                    "rr": metric_summary(
                        base_runs,
                        cand_runs,
                        "rr",
                        thresh=args.practical_threshold_pct,
                        seed=args.seed,
                        n_boot=args.bootstrap_samples,
                    ),
                    "rb": metric_summary(
                        base_runs,
                        cand_runs,
                        "rb",
                        thresh=args.practical_threshold_pct,
                        seed=args.seed,
                        n_boot=args.bootstrap_samples,
                    ),
                },
            }
            result["valsizes"][str(valsize)] = vs

        decision = overall_decision(result["valsizes"], args.max_rounds)
        result["decision"] = decision

        sanity_dir = out_dir / "broad_sanity"
        sanity_dir.mkdir(parents=True, exist_ok=True)
        result["broad_sanity"] = {
            "baseline": run_broad_sanity(base_bin, args, sanity_dir / "baseline.md"),
            "candidate": run_broad_sanity(cand_bin, args, sanity_dir / "candidate.md"),
        }

        result["microbench"] = run_microbench_compare(args, out_dir)

        js_path = out_dir / "results.json"
        md_path = out_dir / "summary.md"
        js_path.write_text(json.dumps(result, indent=2))
        render_summary_md(result, md_path)

        print(f"wrote {js_path}")
        print(f"wrote {md_path}")
        print(f"decision: {decision}")
        return 0
    finally:
        cleanup_worktrees(args, out_dir)


if __name__ == "__main__":
    sys.exit(main())
