#!/usr/bin/env python3
"""Summarize and gate TreeDB index-vacuum M0 benchmark captures."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
from pathlib import Path


REQUIRED_CV_METRICS = (
    "vacuum-total-ns/op",
    "max-writer-pause-ns",
    "foreground-p99-ns/op",
)


def command(*args: str, cwd: Path) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def parse_benchmark(path: Path) -> dict[str, float]:
    rows = []
    for line in path.read_text().splitlines():
        if not line.startswith("Benchmark"):
            continue
        fields = line.split()
        if len(fields) < 4 or len(fields[2:]) % 2:
            raise ValueError(f"malformed benchmark row in {path}: {line}")
        metrics = {}
        values = fields[2::2]
        units = fields[3::2]
        for index, value in enumerate(values):
            unit = units[index]
            metrics[unit] = float(value)
        rows.append(metrics)
    if len(rows) != 1:
        raise ValueError(f"expected one benchmark row in {path}, found {len(rows)}")
    return rows[0]


def summarize(paths: list[Path]) -> dict[str, dict[str, object]]:
    samples = [parse_benchmark(path) for path in paths]
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
    legacy: dict[str, dict[str, object]], public: dict[str, dict[str, object]]
) -> dict[str, bool]:
    missing = [metric for metric in REQUIRED_CV_METRICS if metric not in legacy]
    if missing:
        raise ValueError(f"legacy samples missing stability metrics: {missing}")
    return {
        "legacy_cv_at_most_10_percent": all(
            legacy[metric]["cv"] <= 0.10 for metric in REQUIRED_CV_METRICS
        ),
        "legacy_completed_without_abort": (
            "concurrent-aborts/op" in legacy
            and all(value == 0 for value in legacy["concurrent-aborts/op"]["samples"])
        ),
        "public_status_explicit": classify_public_status(public)
        != "production-index-vacuum-ambiguous",
    }


def environment(repo: Path, run_dir: Path) -> dict[str, object]:
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
            "legacy-benchstat-input.txt",
            "legacy-benchstat.txt (when benchstat is installed)",
            "raw/legacy-*.txt",
            "raw/public-*.txt",
        ],
    }


def render_markdown(result: dict[str, object]) -> str:
    env = result["environment"]
    fixture = result["fixture"]["fixture"]
    legacy = result["metrics"]["legacy"]
    public = result["metrics"]["public"]
    lines = [
        "# TreeDB Vacuum M0 Capture",
        "",
        f"- SHA: `{env['git_sha']}` ({env['dirty_state']})",
        f"- Go: `{env['go_version']}`; `{env['goos']}/{env['goarch']}`",
        f"- Storage: `{env['device']}` (`{env['filesystem']}`)",
        f"- Timing boundary: `{result['fixture']['timing_boundary']}`",
        "- Repetitions: `10` interleaved legacy/public runs",
        f"- Status: `{result['public_status']}`",
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
        lines.append(f"| `{metric}` | {entry['median']:.3f} | {entry['cv']:.4f} | {'pass' if entry['cv'] <= 0.10 else 'fail'} |")
    lines.extend(
        [
            "",
            "## Legacy Completion",
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
    args = parser.parse_args()
    repo = args.repo_root.resolve()
    run_dir = args.run_dir.resolve()
    fixture = json.loads((run_dir / "fixture.json").read_text())
    legacy = summarize(sorted((run_dir / "raw").glob("legacy-*.txt")))
    public = summarize(sorted((run_dir / "raw").glob("public-*.txt")))

    gates = evaluate_gates(legacy, public)
    public_status = classify_public_status(public)
    result = {
        "schema_version": 1,
        "environment": environment(repo, run_dir),
        "fixture": fixture,
        "metrics": {"legacy": legacy, "public": public},
        "public_status": public_status,
        "gates": gates,
    }
    (run_dir / "results.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (run_dir / "summary.md").write_text(render_markdown(result))
    return 0 if all(gates.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
