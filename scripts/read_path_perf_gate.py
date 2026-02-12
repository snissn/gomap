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
from typing import Dict, List, Tuple


RR_LINE = re.compile(r"^Random Read / TreeDB = ([0-9,]+)$")
RB_LINE = re.compile(r"^Random Read \(Batch\) / TreeDB = ([0-9,]+)$")


@dataclass
class RunPoint:
    rr: int
    rb: int
    secs: float
    log_path: str


def run(cmd: List[str], cwd: Path, capture: bool = False) -> subprocess.CompletedProcess:
    if capture:
        return subprocess.run(cmd, cwd=str(cwd), check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return subprocess.run(cmd, cwd=str(cwd), check=True)


def parse_metrics(text: str) -> Tuple[int, int]:
    rr = None
    rb = None

    def parse_table_last_numeric(prefix: str, line: str) -> int | None:
        if not line.startswith(prefix):
            return None
        nums = re.findall(r"[0-9][0-9,]*", line)
        if not nums:
            return None
        return int(nums[-1].replace(",", ""))

    for raw in text.splitlines():
        line = raw.strip()
        m = RR_LINE.match(line)
        if m:
            rr = int(m.group(1).replace(",", ""))
            continue
        m = RB_LINE.match(line)
        if m:
            rb = int(m.group(1).replace(",", ""))
            continue
        if rr is None and "(Batch)" not in line and "/ TreeDB" not in line:
            parsed = parse_table_last_numeric("Random Read", line)
            if parsed is not None:
                rr = parsed
                continue
        if rb is None and "/ TreeDB" not in line:
            parsed = parse_table_last_numeric("Random Read (Batch)", line)
            if parsed is not None:
                rb = parsed
                continue
    if rr is None or rb is None:
        raise RuntimeError("failed to parse Random Read metrics from unified-bench output")
    return rr, rb


def middle3(values: List[int]) -> float:
    if len(values) < 5:
        raise ValueError("need at least 5 samples for middle-3")
    # Best-of-5 middle-3 estimator from the full sample set (higher is better).
    best_five = sorted(values, reverse=True)[:5]
    best_five.sort()
    return float(sum(best_five[1:4])) / 3.0


def bootstrap_ci95_effect_pct(base_vals: List[int], cand_vals: List[int], n_boot: int = 20000, seed: int = 1) -> Tuple[float, float]:
    if len(base_vals) != len(cand_vals):
        raise ValueError("paired sample count mismatch")
    if not base_vals:
        return (0.0, 0.0)
    mean_base = sum(base_vals) / len(base_vals)
    if mean_base <= 0:
        return (0.0, 0.0)

    diffs = [c - b for b, c in zip(base_vals, cand_vals)]
    rng = random.Random(seed)
    n = len(diffs)
    boots: List[float] = []
    for _ in range(n_boot):
        sm = 0.0
        for __ in range(n):
            sm += diffs[rng.randrange(n)]
        boots.append((sm / n) / mean_base * 100.0)
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


def metric_summary(base_runs: List[RunPoint], cand_runs: List[RunPoint], metric: str, thresh: float = 1.0) -> Dict[str, float | str | List[float]]:
    base_vals = [getattr(r, metric) for r in base_runs]
    cand_vals = [getattr(r, metric) for r in cand_runs]
    m3_base = middle3(base_vals)
    m3_cand = middle3(cand_vals)
    eff = (m3_cand - m3_base) / m3_base * 100.0 if m3_base > 0 else math.nan
    ci_lo, ci_hi = bootstrap_ci95_effect_pct(base_vals, cand_vals, seed=17)
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


def needs_more_rounds(base_runs: List[RunPoint], cand_runs: List[RunPoint], thresh: float) -> bool:
    rr = metric_summary(base_runs, cand_runs, "rr", thresh=thresh)
    rb = metric_summary(base_runs, cand_runs, "rb", thresh=thresh)
    return rr["classification"] == "uncertain" or rb["classification"] == "uncertain"


def run_unified_once(
    bin_path: Path,
    valsize: int,
    tests: str,
    args: argparse.Namespace,
    log_file: Path,
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
        str(args.seed),
        "-progress=false",
        "-valsize",
        str(valsize),
    ]
    t0 = time.time()
    p = run(cmd, cwd=args.repo, capture=True)
    rr, rb = parse_metrics(p.stdout)
    log_file.write_text(p.stdout)
    return RunPoint(rr=rr, rb=rb, secs=time.time() - t0, log_path=str(log_file))


def build_binaries(args: argparse.Namespace, out_dir: Path) -> Tuple[Path, Path, Dict[str, str]]:
    wt_root = out_dir / "worktrees"
    wt_root.mkdir(parents=True, exist_ok=True)
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
            except Exception as exc:
                print(f"Warning: failed to remove worktree {wt}: {exc}", file=sys.stderr)


def overall_decision(per_valsize: Dict[str, Dict], max_rounds: int) -> str:
    any_improve = False
    any_regress = False
    any_uncertain = False
    maxed_without_upside = False

    for _, vs in per_valsize.items():
        rr_cls = vs["metrics"]["rr"]["classification"]
        rb_cls = vs["metrics"]["rb"]["classification"]
        n = vs["n_per_side"]

        if rr_cls == "improve" or rb_cls == "improve":
            any_improve = True
        if rr_cls == "regress" or rb_cls == "regress":
            any_regress = True
        if rr_cls == "uncertain" or rb_cls == "uncertain":
            any_uncertain = True
            if n >= max_rounds and not any_improve:
                maxed_without_upside = True

    if any_regress:
        return "reject"
    if any_improve and not any_regress:
        return "approve"
    if maxed_without_upside:
        return "reject"
    if any_uncertain:
        return "approve_with_revisions"
    return "approve_with_revisions"


def run_broad_sanity(bin_path: Path, args: argparse.Namespace, out_file: Path) -> Dict[str, int | str]:
    cmd = [
        str(bin_path),
        "-dbs",
        "leveldb,treedb",
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

    rr_treedb = None
    rb_treedb = None
    for ln in p.stdout.splitlines():
        ln = ln.strip()
        m = RR_LINE.match(ln)
        if m:
            rr_treedb = int(m.group(1).replace(",", ""))
            continue
        m = RB_LINE.match(ln)
        if m:
            rb_treedb = int(m.group(1).replace(",", ""))
            continue
    return {
        "random_read_treedb": rr_treedb if rr_treedb is not None else 0,
        "random_read_batch_treedb": rb_treedb if rb_treedb is not None else 0,
        "log": str(out_file),
    }


def run_microbench_compare(args: argparse.Namespace, out_dir: Path) -> Dict[str, str] | None:
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

    p1 = subprocess.run(cmd, cwd=str(baseline_wt), check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    base_txt.write_text(p1.stdout)
    p2 = subprocess.run(cmd, cwd=str(candidate_wt), check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    cand_txt.write_text(p2.stdout)

    p3 = subprocess.run(["benchstat", str(base_txt), str(cand_txt)], cwd=str(args.repo), check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stat_txt.write_text(p3.stdout)

    return {
        "pattern": bench_pattern,
        "baseline_output": str(base_txt),
        "candidate_output": str(cand_txt),
        "benchstat": str(stat_txt),
    }


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
            lines.append(
                f"| {valsize} | {n} | {metric_name} | {m['base_middle3']:,.0f} | {m['candidate_middle3']:,.0f} | {m['effect_middle3_pct']:+.2f}% | [{lo:+.2f}, {hi:+.2f}] | {m['classification']} |"
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

    if result.get("microbench"):
        lines.append("")
        lines.append("## Microbench")
        lines.append("")
        lines.append(f"- bench pattern: `{result['microbench']['pattern']}`")
        lines.append(f"- benchstat: `{result['microbench']['benchstat']}`")

    path.write_text("\n".join(lines) + "\n")


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
    ap.add_argument("--valsizes", default="85,1")
    ap.add_argument("--initial-rounds", type=int, default=5)
    ap.add_argument("--step-rounds", type=int, default=5)
    ap.add_argument("--max-rounds", type=int, default=25)
    ap.add_argument("--practical-threshold-pct", type=float, default=1.0)
    ap.add_argument("--sanity-valsize", type=int, default=85)
    ap.add_argument("--microbench", action="append", default=[])
    ap.add_argument("--microbench-count", type=int, default=20)
    ap.add_argument("--microbench-gomaxprocs", type=int, default=1)
    args = ap.parse_args()
    if args.initial_rounds <= 0:
        raise SystemExit("--initial-rounds must be > 0")
    if args.step_rounds <= 0:
        raise SystemExit("--step-rounds must be > 0")
    if args.max_rounds <= 0:
        raise SystemExit("--max-rounds must be > 0")
    if args.initial_rounds > args.max_rounds:
        raise SystemExit("--initial-rounds must be <= --max-rounds")
    return args


def main() -> int:
    args = parse_args()
    args.repo = args.repo.resolve()
    ts = time.strftime("%Y%m%d%H%M%S")
    out_dir = args.out_dir.resolve() if args.out_dir else (args.repo / "artifacts" / "read_gate" / f"gate_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    valsizes = [int(v.strip()) for v in args.valsizes.split(",") if v.strip()]
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
            "initial_rounds": args.initial_rounds,
            "step_rounds": args.step_rounds,
            "max_rounds": args.max_rounds,
            "practical_threshold_pct": args.practical_threshold_pct,
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
                        side_idx = len(base_runs) + 1 if side == "base" else len(cand_runs) + 1
                        log_file = vdir / f"run_{side}_{side_idx:02d}.md"
                        rec = run_unified_once(
                            base_bin if side == "base" else cand_bin,
                            valsize,
                            args.tests,
                            args,
                            log_file,
                        )
                        if side == "base":
                            base_runs.append(rec)
                            print(f"  r{len(base_runs):02d} base rr={rec.rr} rb={rec.rb} t={rec.secs:.2f}s", flush=True)
                        else:
                            cand_runs.append(rec)
                            print(f"  r{len(cand_runs):02d} cand rr={rec.rr} rb={rec.rb} t={rec.secs:.2f}s", flush=True)

                if len(base_runs) < 5:
                    continue
                if not needs_more_rounds(base_runs, cand_runs, args.practical_threshold_pct):
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
                    "rr": metric_summary(base_runs, cand_runs, "rr", thresh=args.practical_threshold_pct),
                    "rb": metric_summary(base_runs, cand_runs, "rb", thresh=args.practical_threshold_pct),
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
