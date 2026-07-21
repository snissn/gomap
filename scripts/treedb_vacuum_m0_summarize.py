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
        for value, unit in zip(fields[2::2], fields[3::2]):
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
        "- Status: `production-index-vacuum-unavailable`",
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
            "## Public Classification",
            "",
            f"- `vacuum-unsupported/op`: median `{public['vacuum-unsupported/op']['median']:.3f}`",
            f"- `vacuum-unexpected-errors/op`: median `{public['vacuum-unexpected-errors/op']['median']:.3f}`",
            "- Unsupported samples are classification evidence, not successful vacuum measurements.",
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

    missing = [metric for metric in REQUIRED_CV_METRICS if metric not in legacy]
    if missing:
        raise ValueError(f"legacy samples missing stability metrics: {missing}")
    cv_pass = all(legacy[metric]["cv"] <= 0.10 for metric in REQUIRED_CV_METRICS)
    public_pass = (
        "vacuum-unsupported/op" in public
        and "vacuum-unexpected-errors/op" in public
        and all(value == 1 for value in public["vacuum-unsupported/op"]["samples"])
        and all(value == 0 for value in public["vacuum-unexpected-errors/op"]["samples"])
    )
    result = {
        "schema_version": 1,
        "environment": environment(repo, run_dir),
        "fixture": fixture,
        "metrics": {"legacy": legacy, "public": public},
        "gates": {"legacy_cv_at_most_10_percent": cv_pass, "public_status_explicit": public_pass},
    }
    (run_dir / "results.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (run_dir / "summary.md").write_text(render_markdown(result))
    return 0 if cv_pass and public_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
