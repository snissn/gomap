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
RESOURCE_DELIMITER_RE = re.compile(
    r"--- sample=(\d+) group=(regular|prune) benchtime=([^\s]+) ---"
)
RESOURCE_FIELDS = (
    ("command", re.compile(r'Command being timed:\s+".+"')),
    ("user", re.compile(r"User time \(seconds\):\s+([0-9.]+)")),
    ("system", re.compile(r"System time \(seconds\):\s+([0-9.]+)")),
    ("cpu", re.compile(r"Percent of CPU this job got:\s+\d+%")),
    (
        "elapsed",
        re.compile(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s+[0-9:.]+"),
    ),
    ("average_shared_text", re.compile(r"Average shared text size \(kbytes\):\s+\d+")),
    ("average_unshared_data", re.compile(r"Average unshared data size \(kbytes\):\s+\d+")),
    ("average_stack", re.compile(r"Average stack size \(kbytes\):\s+\d+")),
    ("average_total", re.compile(r"Average total size \(kbytes\):\s+\d+")),
    ("rss", re.compile(r"Maximum resident set size \(kbytes\):\s+(\d+)")),
    ("average_rss", re.compile(r"Average resident set size \(kbytes\):\s+\d+")),
    ("major_faults", re.compile(r"Major \(requiring I/O\) page faults:\s+\d+")),
    ("minor_faults", re.compile(r"Minor \(reclaiming a frame\) page faults:\s+\d+")),
    ("voluntary_switches", re.compile(r"Voluntary context switches:\s+\d+")),
    ("involuntary_switches", re.compile(r"Involuntary context switches:\s+\d+")),
    ("swaps", re.compile(r"Swaps:\s+\d+")),
    ("filesystem_inputs", re.compile(r"File system inputs:\s+\d+")),
    ("filesystem_outputs", re.compile(r"File system outputs:\s+\d+")),
    ("socket_sent", re.compile(r"Socket messages sent:\s+\d+")),
    ("socket_received", re.compile(r"Socket messages received:\s+\d+")),
    ("signals", re.compile(r"Signals delivered:\s+\d+")),
    ("page_size", re.compile(r"Page size \(bytes\):\s+\d+")),
    ("exit_status", re.compile(r"Exit status:\s+(\d+)")),
)

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
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
    ):
        match = BENCH_RE.match(line.strip())
        if match is None:
            continue
        name, ns_per_op, tail = match.groups()
        metrics = {"ns/op": float(ns_per_op)}
        for value, unit in METRIC_RE.findall(tail):
            if unit in metrics:
                raise ValueError(
                    f"{path}:{line_number}: {name}: duplicate metric unit {unit}"
                )
            metrics[unit] = float(value)
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
    lines = [
        line.strip()
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines()
        if line.strip()
    ]
    cursor = 0
    records: list[dict[str, str]] = []
    for sample in range(1, expected_samples + 1):
        for group in ("regular", "prune"):
            if cursor >= len(lines):
                raise ValueError(f"{path}: missing resource record for sample={sample} group={group}")
            delimiter = RESOURCE_DELIMITER_RE.fullmatch(lines[cursor])
            if delimiter is None or (int(delimiter.group(1)), delimiter.group(2)) != (
                sample,
                group,
            ):
                raise ValueError(
                    f"{path}: expected resource delimiter for sample={sample} "
                    f"group={group}, got {lines[cursor]!r}"
                )
            cursor += 1
            record: dict[str, str] = {}
            for field, pattern in RESOURCE_FIELDS:
                if cursor >= len(lines):
                    raise ValueError(
                        f"{path}: sample={sample} group={group}: missing {field} field"
                    )
                match = pattern.fullmatch(lines[cursor])
                if match is None:
                    raise ValueError(
                        f"{path}: sample={sample} group={group}: expected {field} "
                        f"field, got {lines[cursor]!r}"
                    )
                if match.lastindex:
                    record[field] = match.group(1)
                cursor += 1
            if record["exit_status"] != "0":
                raise ValueError(
                    f"{path}: sample={sample} group={group}: /usr/bin/time "
                    f"exit status {record['exit_status']}"
                )
            records.append(record)
    if cursor != len(lines):
        raise ValueError(f"{path}: unexpected extra resource content {lines[cursor]!r}")
    rss = [int(record["rss"]) for record in records]
    user = [float(record["user"]) for record in records]
    system = [float(record["system"]) for record in records]
    return {
        "invocations": float(len(records)),
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
