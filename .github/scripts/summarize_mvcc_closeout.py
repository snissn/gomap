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

PROFILES = ("durable_sync", "wal_on_relaxed", "wal_off_relaxed")
EXPECTED_BENCHMARKS = frozenset(
    name
    for profile in PROFILES
    for name in (
        f"BenchmarkDgraphMVCCCloseout/CommitAt/{profile}/batch=1",
        f"BenchmarkDgraphMVCCCloseout/CommitAt/{profile}/batch=32",
        f"BenchmarkDgraphMVCCCloseout/GetAt/{profile}/depth=1",
        f"BenchmarkDgraphMVCCCloseout/GetAt/{profile}/depth=64",
        f"BenchmarkDgraphMVCCCloseout/AllVersions/{profile}/keys=64/depth=1",
        f"BenchmarkDgraphMVCCCloseout/AllVersions/{profile}/keys=64/depth=32",
        f"BenchmarkDgraphMVCCCloseout/Prune/{profile}/keys=64/depth=16/floor=4",
        f"BenchmarkDgraphMVCCCloseout/Prune/{profile}/keys=64/depth=16/floor=12",
    )
)


def required_metrics(name: str) -> set[str]:
    required = {"ns/op", "B/op", "allocs/op"}
    if "/CommitAt/" in name:
        required.update(("mutations/s", "storage_bytes/op"))
    elif "/GetAt/" in name:
        required.add("lookups/s")
    elif "/AllVersions/" in name:
        required.add("versions/s")
    elif "/Prune/" in name:
        required.update(
            ("pruned_versions/s", "storage_bytes/op", "delete_write_amplification")
        )
    if "/durable_sync/" in name and ("/CommitAt/" in name or "/Prune/" in name):
        required.add("durable_footprint_bytes/op")
    return required


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
    names = set(samples)
    if names != EXPECTED_BENCHMARKS:
        missing = sorted(EXPECTED_BENCHMARKS - names)
        extra = sorted(names - EXPECTED_BENCHMARKS)
        raise ValueError(f"{path}: benchmark set mismatch; missing={missing}; extra={extra}")
    medians: dict[str, dict[str, float]] = {}
    for name, rows in samples.items():
        if len(rows) != expected:
            raise ValueError(f"{path}: {name}: expected {expected} samples, got {len(rows)}")
        required = required_metrics(name)
        for index, row in enumerate(rows, 1):
            missing = sorted(required - set(row))
            if missing:
                raise ValueError(f"{path}: {name}: sample {index} missing metrics {missing}")
        units = set.intersection(*(set(row) for row in rows))
        medians[name] = {
            unit: statistics.median(row[unit] for row in rows) for unit in sorted(units)
        }
    return medians


def parse_resources(path: Path, expected_samples: int) -> dict[str, float]:
    text = path.read_text(encoding="utf-8", errors="replace")
    rss = [int(value) for value in RSS_RE.findall(text)]
    user = [float(value) for value in USER_RE.findall(text)]
    system = [float(value) for value in SYSTEM_RE.findall(text)]
    expected_invocations = expected_samples * 2
    counts = (len(rss), len(user), len(system))
    if counts != (expected_invocations,) * 3:
        raise ValueError(
            f"{path}: expected {expected_invocations} complete /usr/bin/time "
            f"invocations, got rss/user/system={counts}"
        )
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


def metric_text(metrics: dict[str, float], unit: str) -> str:
    value = metrics.get(unit)
    return "-" if value is None else f"{value:.3f}"


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
        "| Benchmark | ns/op | Throughput | B/op | allocs/op | storage bytes/op | durable footprint bytes/op | delete write amp |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in sorted(medians):
        metrics = medians[name]
        rate, unit = throughput(metrics)
        rate_text = f"{rate:.3f} {unit}" if unit != "-" else "-"
        lines.append(
            f"| `{name}` | {metrics['ns/op']:.3f} | {rate_text} | "
            f"{metric_text(metrics, 'B/op')} | {metric_text(metrics, 'allocs/op')} | "
            f"{metric_text(metrics, 'storage_bytes/op')} | "
            f"{metric_text(metrics, 'durable_footprint_bytes/op')} | "
            f"{metric_text(metrics, 'delete_write_amplification')} |"
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
    resources = parse_resources(args.resources, args.expected_samples)
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
