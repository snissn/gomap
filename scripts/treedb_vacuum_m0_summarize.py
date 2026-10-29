#!/usr/bin/env python3
"""Summarize and gate TreeDB index-vacuum M0 benchmark captures."""

from __future__ import annotations

import argparse
import json
import math
import re
import os
import statistics
import subprocess
from pathlib import Path


REQUIRED_CV_METRICS = (
    "vacuum-total-ns/op",
    "max-writer-pause-ns",
    "foreground-p99-ns/op",
)
PRODUCT_V3_CV_METRICS = ("vacuum-total-ns/op", "foreground-p99-ns/op")


def cv_metrics(stability_arm: str, acceptance_contract: str) -> tuple[str, ...]:
    if acceptance_contract not in ("v2", "v3"):
        raise ValueError("unknown acceptance contract")
    if acceptance_contract == "v3":
        if stability_arm != "production":
            raise ValueError("v3 requires the production stability arm")
        return PRODUCT_V3_CV_METRICS
    return REQUIRED_CV_METRICS


def validate_qualification(profile: str, arm: str, contract: str) -> None:
    if profile not in ("controlled-performance", "hosted-semantic"):
        raise ValueError("unknown qualification profile")
    if profile == "hosted-semantic" and (arm, contract) != ("production", "v3"):
        raise ValueError("hosted-semantic requires prospective production v3 capture")


def capture_passes(gates: dict[str, bool], profile: str) -> bool:
    validate_qualification(profile, "production", "v3")
    # Keep the strict predicate, including its failed value, in every packet.
    # Only the explicitly named timing predicate differs in hosted CI's verdict.
    return all(value for name, value in gates.items()
               if profile != "hosted-semantic" or name != "production_cv_at_most_10_percent")


def pause_diagnostic(metrics: dict[str, dict[str, object]]) -> dict[str, object]:
    pause = metrics["max-writer-pause-ns"]
    return {
        **pause,
        "maximum": max(pause["samples"]),
        "cv_at_most_10_percent": pause["cv"] <= 0.10,
        "merge_predicate": False,
        "owner_review_required": True,
        "owner_review": None,
    }


STABILITY_BENCHMARKS = {
    "legacy": "BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x",
    "production": "BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x",
}


def command(*args: str, cwd: Path) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def parse_benchmark(path: Path, expected_benchmark: str | None = None) -> dict[str, float]:
    rows = []
    lines = path.read_text().splitlines()
    if expected_benchmark is not None and ("PASS" not in lines or "FAIL" in lines):
        raise ValueError(f"capture did not complete successfully: {path}")
    for line in lines:
        if not line.startswith("Benchmark"):
            continue
        fields = line.split()
        if len(fields) < 4 or len(fields[2:]) % 2:
            raise ValueError(f"malformed benchmark row in {path}: {line}")
        if expected_benchmark is not None:
            if re.fullmatch(re.escape(expected_benchmark) + r"-[1-9][0-9]*", fields[0]) is None:
                raise ValueError(f"benchmark identity differs from selected arm: {path}")
            if fields[1] != "1":
                raise ValueError(f"expected exactly one fixed-work iteration: {path}")
        metrics = {}
        values = fields[2::2]
        units = fields[3::2]
        for index, value in enumerate(values):
            unit = units[index]
            number = float(value)
            if unit in metrics or not math.isfinite(number) or number < 0:
                raise ValueError(f"invalid/duplicate metric {unit}: {path}")
            metrics[unit] = number
        rows.append(metrics)
    if len(rows) != 1:
        raise ValueError(f"expected one benchmark row in {path}, found {len(rows)}")
    return rows[0]


def summarize(paths: list[Path], expected_benchmark: str | None = None) -> dict[str, dict[str, object]]:
    samples = [parse_benchmark(path, expected_benchmark) for path in paths]
    if len(samples) != 10:
        raise ValueError(f"expected 10 samples, found {len(samples)}")
    names = set(samples[0])
    if any(set(sample) != names for sample in samples[1:]):
        raise ValueError("benchmark metric set drifted between repetitions")
    result = {}
    for name in sorted(names):
        values = [sample[name] for sample in samples]
        mean = statistics.fmean(values)
        result[name] = {
            "samples": values,
            "mean": mean,
            "median": statistics.median(values),
            "cv": statistics.stdev(values) / mean if mean else 0.0,
        }
    return result


def classify_public_status(public: dict[str, dict[str, object]]) -> str:
    unsupported = public.get("vacuum-unsupported/op", {}).get("samples", [])
    retries = public.get("vacuum-concurrent-retries/op", {}).get("samples", [])
    unexpected = public.get("vacuum-unexpected-errors/op", {}).get("samples", [])
    misses = public.get("foreground-exposure-misses/op", {}).get("samples", [])
    overlap = public.get("foreground-overlap-samples/op", {}).get("samples", [])
    if all(len(samples) == 10 for samples in (unsupported, retries, unexpected, misses, overlap)):
        if (
            all(value == 1 for value in unsupported)
            and all(value == 0 for value in retries)
            and all(value == 0 for value in unexpected)
            and all(value == 1 for value in misses)
            and all(value == 0 for value in overlap)
        ):
            return "production-index-vacuum-unavailable"
        if (
            all(value == 0 for value in unsupported)
            and all(value in (0, 1) for value in retries)
            and any(value == 0 for value in retries)
            and all(value == 0 for value in unexpected)
            and all(value == 0 for value in misses)
            and all(value > 0 for value in overlap)
        ):
            return "production-index-vacuum-available"
    return "production-index-vacuum-ambiguous"


def evaluate_gates(
    legacy: dict[str, dict[str, object]], public: dict[str, dict[str, object]],
    stability_arm: str = "legacy",
    acceptance_contract: str = "v2",
) -> dict[str, bool]:
    if stability_arm not in STABILITY_BENCHMARKS:
        raise ValueError("unknown stability arm")
    required_cv = cv_metrics(stability_arm, acceptance_contract)
    missing = [metric for metric in REQUIRED_CV_METRICS if metric not in legacy]
    if missing:
        raise ValueError(f"{stability_arm} samples missing stability metrics: {missing}")
    gates = {
        f"{stability_arm}_cv_at_most_10_percent": all(
            legacy[metric]["cv"] <= 0.10 for metric in required_cv
        ),
        f"{stability_arm}_completed_without_abort": (
            "concurrent-aborts/op" in legacy
            and all(value == 0 for value in legacy["concurrent-aborts/op"]["samples"])
        ),
        "public_status_explicit": classify_public_status(public)
        != "production-index-vacuum-ambiguous",
    }

    if stability_arm == "production":
        required_counts = {
            "foreground-overlap-samples/op": 4096,
            "foreground-points/op": 2048,
            "foreground-ranges/op": 2048,
            "foreground-exposure-misses/op": 0,
            "vacuum-errors/op": 0,
            "concurrent-aborts/op": 0,
        }
        gates["production_fixed_work_complete"] = all(
            name in legacy
            and len(legacy[name].get("samples", [])) == 10
            and all(value == expected for value in legacy[name]["samples"])
            for name, expected in required_counts.items()
        )
        gates["production_stability_observations_complete"] = all(
            len(legacy[name].get("samples", [])) == 10 for name in REQUIRED_CV_METRICS
        )
    return gates


def environment(repo: Path, run_dir: Path, stability_arm: str) -> dict[str, object]:
    status = command("git", "status", "--porcelain", cwd=repo)
    df = command("df", "-PT", str(run_dir), cwd=repo).splitlines()[-1].split()
    return {
        "git_sha": command("git", "rev-parse", "HEAD", cwd=repo),
        "dirty_state": "dirty" if status else "clean",
        "go_version": command("go", "version", cwd=repo),
        "goos": command("go", "env", "GOOS", cwd=repo),
        "goarch": command("go", "env", "GOARCH", cwd=repo),
        "device": df[0],
        "filesystem": df[1],
        "cpu_set": os.environ.get("CPU_SET", "unknown"),
        "gomaxprocs": os.environ.get("GOMAXPROCS", "unknown"),
        "gomemlimit": os.environ.get("GOMEMLIMIT", "unknown"),
        "repetitions": 10,
        "commands": (run_dir / "commands.txt").read_text().splitlines(),
        "artifact_paths": [
            "fixture.json",
            "results.json",
            "summary.md",
            "commands.txt",
            f"{stability_arm}-benchstat-input.txt",
            f"{stability_arm}-benchstat.txt (when benchstat is installed)",
            f"raw/{stability_arm}-*.txt",
            "raw/public-*.txt",
        ],
    }


def render_markdown(result: dict[str, object]) -> str:
    env = result["environment"]
    fixture = result["fixture"]["fixture"]
    stability_arm = result["stability_arm"]
    contract = result.get("acceptance_contract", "v2")
    required_cv = cv_metrics(stability_arm, contract)
    qualification = result.get("qualification_profile", "controlled-performance")
    legacy = result["metrics"][stability_arm]
    public = result["metrics"]["public"]
    lines = [
        "# TreeDB Vacuum M0 Capture",
        "",
        f"- SHA: `{env['git_sha']}` ({env['dirty_state']})",
        f"- Go: `{env['go_version']}`; `{env['goos']}/{env['goarch']}`",
        f"- Storage: `{env['device']}` (`{env['filesystem']}`)",
        f"- Timing boundary: `{result['fixture']['timing_boundary']}`",
        f"- Repetitions: `10` interleaved {stability_arm}/public runs",
        f"- Stability benchmark: `{STABILITY_BENCHMARKS[stability_arm]}`",
        f"- Acceptance contract: `{contract}`; no historical result reinterpreted",
        "- Stability predicates: " + ", ".join(f"`{name}` CV <= 0.10" for name in required_cv) + "; no discarded samples",
        f"- Status: `{result['public_status']}`",
        "",
        f"- Qualification profile: `{qualification}`",
        "- Hosted semantic success certifies completed fixed work and classification only; timing acceptance and product performance approval remain separate.",
        "- Strict predicates below retain their original verdicts, including failures.",
        "",
        "## Fixture",
        "",
        f"- Digest: `{fixture['logical_digest']}`",
        f"- Index bytes: `{fixture['index_bytes']}` -> `{fixture['offline_index_bytes']}`",
        f"- Reclaimable pages: `{fixture['reclaimable_pages']}` (`{fixture['reclaimable_page_percent']:.2f}%`)",
        f"- Value-log bytes: `{fixture['value_log_bytes']}` -> `{fixture['offline_value_log_bytes']}`",
        f"- Leaf-log bytes: `{fixture['leaf_log_bytes']}` -> `{fixture.get('offline_leaf_log_bytes', 0)}`",
        f"- Parameters: `{json.dumps(fixture['parameters'], sort_keys=True)}`",
        "",
        "## Stability Gates",
        "",
        "| Metric | Median | CV | Gate |",
        "| --- | ---: | ---: | --- |",
    ]
    for metric in REQUIRED_CV_METRICS:
        entry = legacy[metric]
        classification = 'pass' if entry['cv'] <= 0.10 else 'fail'
        if metric not in required_cv:
            classification += ' (diagnostic only; owner review required)'
        lines.append(f"| `{metric}` | {entry['median']:.3f} | {entry['cv']:.4f} | {classification} |")
    if contract == "v3":
        pause = result["pause_diagnostic"]
        lines.extend(["", f"- Pause maximum: `{pause['maximum']:.0f} ns`; all ten raw maxima and CV remain above/in results.json.",
                      "- Pause distribution and source phase evidence require separate outcome-owner review; this capture does not grant merge approval."])
    lines.extend(
        [
            "",
            f"## {stability_arm.title()} Completion",
            "",
            f"- `concurrent-aborts/op`: median `{legacy['concurrent-aborts/op']['median']:.3f}`",
            "",
            "## Public Classification",
            "",
            f"- `vacuum-unsupported/op`: median `{public['vacuum-unsupported/op']['median']:.3f}`",
            f"- `vacuum-unexpected-errors/op`: median `{public['vacuum-unexpected-errors/op']['median']:.3f}`",
            "- Unavailable status requires one unsupported result and one exposure miss with zero retries, unexpected errors, and foreground overlap in every sample.",
            "- Available status requires at least one successful vacuum, only typed transient retries, zero unexpected errors/exposure misses, and positive foreground overlap in every sample.",
            "",
            "## Commands",
            "",
            "```sh",
            *env["commands"],
            "```",
            "",
            "Raw inputs are under `raw/`; the complete machine-readable summary is `results.json`.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--stability-arm", choices=tuple(STABILITY_BENCHMARKS), default="legacy",
                        help="legacy retains historical interpretation; production is the current capture contract")
    parser.add_argument("--acceptance-contract", choices=("v2", "v3"), default="v2",
                        help="v2 retains all three CV gates; v3 reports pause CV diagnostically with mandatory owner review")
    parser.add_argument("--qualification-profile", choices=("controlled-performance", "hosted-semantic"),
                        default="controlled-performance")
    args = parser.parse_args()
    validate_qualification(args.qualification_profile, args.stability_arm, args.acceptance_contract)
    repo = args.repo_root.resolve()
    run_dir = args.run_dir.resolve()
    fixture = json.loads((run_dir / "fixture.json").read_text())
    arm = args.stability_arm
    required_cv = cv_metrics(arm, args.acceptance_contract)
    legacy = summarize(sorted((run_dir / "raw").glob(f"{arm}-*.txt")), STABILITY_BENCHMARKS[arm])
    public = summarize(sorted((run_dir / "raw").glob("public-*.txt")))

    gates = evaluate_gates(legacy, public, arm, args.acceptance_contract)
    public_status = classify_public_status(public)
    result = {
        "schema_version": 3 if args.acceptance_contract == "v3" else 2,
        "acceptance_contract": args.acceptance_contract,
        "qualification_profile": args.qualification_profile,
        "capture_passed": capture_passes(gates, args.qualification_profile),
        "strict_performance_predicates_passed": all(gates.values()),
        "product_performance_approval": None,
        "stability_arm": arm,
        "stability_contract": {"benchmark": STABILITY_BENCHMARKS[arm], "repetitions": 10,
                               "cv_limit": 0.10, "fixed_foreground_operations": 4096,
                               "required_cv_metrics": list(required_cv),
                               "historical_results_reinterpreted": False},
        "environment": environment(repo, run_dir, arm),
        "fixture": fixture,
        "metrics": {arm: legacy, "public": public},
        "public_status": public_status,
        "gates": gates,
    }
    if args.acceptance_contract == "v3":
        result["pause_diagnostic"] = pause_diagnostic(legacy)
    (run_dir / "results.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (run_dir / "summary.md").write_text(render_markdown(result))
    return 0 if capture_passes(gates, args.qualification_profile) else 1


if __name__ == "__main__":
    raise SystemExit(main())
