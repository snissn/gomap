#!/usr/bin/env python3
"""Summarize the pinned TreeDB MVCC closeout benchmark matrix."""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path


BENCH_RE = re.compile(
    r"^(BenchmarkDgraphMVCCCloseout/.+?)(?:-\d+)?\s+\d+\s+"
    r"([0-9.]+)\s+ns/op\s+(.*)$"
)
METRIC_RE = re.compile(r"([0-9.eE+-]+)\s+([^\s]+)")
RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s+(\d+)")
USER_RE = re.compile(r"User time \(seconds\):\s+([0-9.]+)")
SYSTEM_RE = re.compile(r"System time \(seconds\):\s+([0-9.]+)")


def parse_benchmarks(path: Path, expected: int) -> dict[str, dict[str, float]]:
    samples: dict[str, list[dict[str, float]]] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = BENCH_RE.match(line.strip())
        if match is None:
            continue
        name, ns_per_op, tail = match.groups()
        metrics = {unit: float(value) for value, unit in METRIC_RE.findall(tail)}
        metrics["ns/op"] = float(ns_per_op)
        samples.setdefault(name, []).append(metrics)
    if not samples:
        raise ValueError(f"{path}: no closeout benchmarks found")
    medians: dict[str, dict[str, float]] = {}
    for name, rows in samples.items():
        if len(rows) != expected:
            raise ValueError(f"{path}: {name}: expected {expected} samples, got {len(rows)}")
        units = set.intersection(*(set(row) for row in rows))
        medians[name] = {
            unit: statistics.median(row[unit] for row in rows) for unit in sorted(units)
        }
    return medians


def parse_resources(path: Path) -> dict[str, float]:
    text = path.read_text(encoding="utf-8", errors="replace")
    rss = [int(value) for value in RSS_RE.findall(text)]
    user = [float(value) for value in USER_RE.findall(text)]
    system = [float(value) for value in SYSTEM_RE.findall(text)]
    if not rss or not user or not system:
        raise ValueError(f"{path}: incomplete /usr/bin/time evidence")
    return {
        "invocations": float(len(rss)),
        "max_rss_kib": float(max(rss)),
        "total_user_seconds": sum(user),
        "total_system_seconds": sum(system),
    }


def throughput(metrics: dict[str, float]) -> tuple[float, str]:
    for unit in ("mutations/s", "lookups/s", "versions/s", "pruned_versions/s"):
        if unit in metrics:
            return metrics[unit], unit
    return 0.0, "-"


def render_markdown(
    candidate_sha: str,
    expected: int,
    medians: dict[str, dict[str, float]],
    resources: dict[str, float],
) -> str:
    lines = [
        "## TreeDB Dgraph MVCC closeout matrix",
        "",
        f"- candidate: `{candidate_sha}`",
        f"- samples: {expected}",
        f"- measured benchmark invocations: {resources['invocations']:.0f}",
        f"- maximum benchmark-process RSS: {resources['max_rss_kib']:.0f} KiB",
        f"- aggregate process CPU: user {resources['total_user_seconds']:.2f}s, "
        f"system {resources['total_system_seconds']:.2f}s",
        "- durability classes are separate rows; relaxed rows are not durability-equivalent to durable sync",
        "",
        "| Benchmark | ns/op | Throughput | B/op | allocs/op | storage bytes/op | durable bytes/op | delete write amp |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in sorted(medians):
        metrics = medians[name]
        rate, unit = throughput(metrics)
        rate_text = f"{rate:.3f} {unit}" if unit != "-" else "-"
        lines.append(
            f"| `{name}` | {metrics['ns/op']:.3f} | {rate_text} | "
            f"{metrics.get('B/op', 0):.3f} | {metrics.get('allocs/op', 0):.3f} | "
            f"{metrics.get('storage_bytes/op', 0):.3f} | "
            f"{metrics.get('durable_bytes/op', 0):.3f} | "
            f"{metrics.get('delete_write_amplification', 0):.3f} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench", type=Path, required=True)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--expected-samples", type=int, required=True)
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    args = parser.parse_args()

    medians = parse_benchmarks(args.bench, args.expected_samples)
    resources = parse_resources(args.resources)
    payload = {
        "schema": "treedb-dgraph-mvcc-closeout-v1",
        "candidate_sha": args.candidate_sha,
        "samples": args.expected_samples,
        "resources": resources,
        "benchmarks": medians,
    }
    args.json_output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown_output.write_text(
        render_markdown(args.candidate_sha, args.expected_samples, medians, resources),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
