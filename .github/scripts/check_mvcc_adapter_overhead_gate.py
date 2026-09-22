#!/usr/bin/env python3
"""Check MVCC adapter overhead and base/head regressions without dependencies."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import tempfile
from pathlib import Path


BENCHMARKS = (
    "BenchmarkCommitAt/DirectTreeDB/1",
    "BenchmarkCommitAt/MVCC/1",
    "BenchmarkGetAt/DirectSeek/64",
    "BenchmarkGetAt/MVCC/64",
    "BenchmarkVersionIteration/Physical/keys=64/depth=32/reverse=false",
    "BenchmarkVersionIteration/MVCC/keys=64/depth=32/reverse=false",
)
PAIRS = (
    ("CommitAt", BENCHMARKS[0], BENCHMARKS[1]),
    ("GetAt", BENCHMARKS[2], BENCHMARKS[3]),
    ("VersionIteration", BENCHMARKS[4], BENCHMARKS[5]),
)
BENCH_RE = re.compile(
    r"^(Benchmark.+?)(?:-\d+)?\s+\d+\s+([0-9.]+)\s+ns/op\s+(.*)$"
)
METRIC_RE = re.compile(r"([0-9.eE+-]+)\s+(B/op|allocs/op)(?:\s|$)")


def parse(path: Path, expected: int) -> dict[str, list[tuple[float, float, float]]]:
    rows = {name: [] for name in BENCHMARKS}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
    ):
        match = BENCH_RE.match(line.strip())
        if match is None:
            continue
        name, ns, tail = match.groups()
        if name in rows:
            metrics = {}
            for value, unit in METRIC_RE.findall(tail):
                if unit in metrics:
                    raise ValueError(
                        f"{path}:{line_number}: {name}: duplicate metric unit {unit}"
                    )
                metrics[unit] = float(value)
            missing = {"B/op", "allocs/op"} - set(metrics)
            if missing:
                raise ValueError(f"{path}: {name}: missing metrics {sorted(missing)}")
            rows[name].append((float(ns), metrics["B/op"], metrics["allocs/op"]))
    for name, samples in rows.items():
        if len(samples) != expected:
            raise ValueError(f"{path}: expected {expected} samples for {name}, got {len(samples)}")
    return rows


def medians(rows: dict[str, list[tuple[float, float, float]]]) -> dict[str, tuple[float, float, float]]:
    return {
        name: tuple(statistics.median(sample[index] for sample in samples) for index in range(3))
        for name, samples in rows.items()
    }


def evaluate(
    baseline_rows,
    candidate_rows,
    max_regression: float,
    max_bytes_percent: float,
    max_bytes_absolute: float,
    max_ratio: float,
):
    baseline = medians(baseline_rows)
    candidate = medians(candidate_rows)
    results = []
    passed = True
    for name in BENCHMARKS:
        base_ns, base_bytes, base_allocs = baseline[name]
        head_ns, head_bytes, head_allocs = candidate[name]
        delta = (head_ns / base_ns - 1) * 100
        bytes_tolerance = min(base_bytes * max_bytes_percent / 100, max_bytes_absolute)
        row_pass = (
            delta <= max_regression
            and head_bytes - base_bytes <= bytes_tolerance
            and head_allocs <= base_allocs
        )
        passed &= row_pass
        results.append(
            {
                "benchmark": name,
                "baseline": {"ns_per_op": base_ns, "bytes_per_op": base_bytes, "allocs_per_op": base_allocs},
                "candidate": {"ns_per_op": head_ns, "bytes_per_op": head_bytes, "allocs_per_op": head_allocs},
                "ns_delta_percent": delta,
                "bytes_tolerance": bytes_tolerance,
                "pass": row_pass,
            }
        )
    ratios = []
    for label, direct, adapter in PAIRS:
        base_ratio = baseline[adapter][0] / baseline[direct][0]
        head_ratio = candidate[adapter][0] / candidate[direct][0]
        ratio_pass = head_ratio <= max_ratio
        passed &= ratio_pass
        ratios.append(
            {"pair": label, "baseline_ratio": base_ratio, "candidate_ratio": head_ratio, "pass": ratio_pass}
        )
    return passed, results, ratios


def render(baseline_sha, candidate_sha, samples, max_regression, max_ratio, passed, results, ratios):
    lines = [
        "# TreeDB MVCC adapter-overhead gate", "",
        f"- result: **{'PASS' if passed else 'FAIL'}**",
        f"- baseline: `{baseline_sha}`", f"- candidate: `{candidate_sha}`",
        f"- samples: {samples} per revision, benchmark-group-paired alternating AB/BA order",
        f"- base/head timing threshold: +{max_regression:g}%",
        f"- candidate MVCC/direct ratio threshold: {max_ratio:g}x", "",
        "| Benchmark | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | Base allocs/op | Head allocs/op | Result |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in results:
        base, head = row["baseline"], row["candidate"]
        lines.append(
            f"| {row['benchmark']} | {base['ns_per_op']:.3f} | {head['ns_per_op']:.3f} | "
            f"{row['ns_delta_percent']:+.2f}% | {base['bytes_per_op']:.3f} | {head['bytes_per_op']:.3f} | "
            f"{base['allocs_per_op']:.3f} | {head['allocs_per_op']:.3f} | {'PASS' if row['pass'] else 'FAIL'} |"
        )
    lines.extend(["", "| Pair | Base ratio | Head ratio | Result |", "| --- | ---: | ---: | --- |"])
    for row in ratios:
        lines.append(
            f"| {row['pair']} | {row['baseline_ratio']:.3f}x | {row['candidate_ratio']:.3f}x | "
            f"{'PASS' if row['pass'] else 'FAIL'} |"
        )
    lines.append("")
    return "\n".join(lines)


def synthetic(scale=1.0, adapter_ratio=1.5, bytes_delta=0.0, alloc_delta=0.0, runs=8):
    lines = []
    adapter_names = {adapter for _, _, adapter in PAIRS}
    for run in range(runs):
        for index, name in enumerate(BENCHMARKS):
            ns = (1000 + index + run) * scale * (adapter_ratio if name in adapter_names else 1)
            lines.append(
                f"{name}-1 10 {ns} ns/op 7 custom_metric/op "
                f"{100 + bytes_delta} B/op {2 + alloc_delta} allocs/op"
            )
    return "\n".join(lines) + "\n"


def self_test():
    with tempfile.TemporaryDirectory() as tmp:
        base_path, head_path = Path(tmp) / "base", Path(tmp) / "head"
        base_path.write_text(synthetic(), encoding="utf-8")
        head_path.write_text(synthetic(scale=1.04), encoding="utf-8")
        base, head = parse(base_path, 8), parse(head_path, 8)
        assert evaluate(base, head, 5, 1, 64, 2)[0]
        head_path.write_text(synthetic(scale=1.06), encoding="utf-8")
        assert not evaluate(base, parse(head_path, 8), 5, 1, 64, 2)[0]
        head_path.write_text(synthetic(bytes_delta=1.01), encoding="utf-8")
        assert not evaluate(base, parse(head_path, 8), 5, 1, 64, 2)[0]
        head_path.write_text(synthetic(alloc_delta=1), encoding="utf-8")
        assert not evaluate(base, parse(head_path, 8), 5, 1, 64, 2)[0]
        head_path.write_text(synthetic(adapter_ratio=2.01), encoding="utf-8")
        assert not evaluate(base, parse(head_path, 8), 5, 1, 64, 2)[0]
        head_path.write_text(synthetic(runs=7), encoding="utf-8")
        try:
            parse(head_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("wrong sample count should fail")
        head_path.write_text(synthetic().replace(BENCHMARKS[0] + "-1", "ignored-1"), encoding="utf-8")
        try:
            parse(head_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("missing row should fail")
        head_path.write_text(
            synthetic().replace("100.0 B/op", "100.0 B/op 101.0 B/op", 1),
            encoding="utf-8",
        )
        try:
            parse(head_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("duplicate metric unit should fail")
    print("check_mvcc_adapter_overhead_gate.py self-test: PASS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline")
    parser.add_argument("--candidate")
    parser.add_argument("--baseline-sha")
    parser.add_argument("--candidate-sha")
    parser.add_argument("--expected-samples", type=int, default=8)
    parser.add_argument("--max-regression-percent", type=float, default=5)
    parser.add_argument("--max-bytes-regression-percent", type=float, default=1)
    parser.add_argument("--max-bytes-regression-absolute", type=float, default=64)
    parser.add_argument("--max-adapter-ratio", type=float, default=2)
    parser.add_argument("--json-output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    required = ("baseline", "candidate", "baseline_sha", "candidate_sha", "json_output", "markdown_output")
    missing = [name for name in required if not getattr(args, name)]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))
    if args.expected_samples < 1 or args.expected_samples % 2 != 0 or min(
        args.max_regression_percent,
        args.max_bytes_regression_percent,
        args.max_bytes_regression_absolute,
        args.max_adapter_ratio,
    ) < 0:
        parser.error("sample count must be positive/even and thresholds non-negative")
    baseline = parse(Path(args.baseline), args.expected_samples)
    candidate = parse(Path(args.candidate), args.expected_samples)
    passed, results, ratios = evaluate(
        baseline, candidate, args.max_regression_percent,
        args.max_bytes_regression_percent, args.max_bytes_regression_absolute,
        args.max_adapter_ratio,
    )
    payload = {
        "pass": passed, "baseline_sha": args.baseline_sha, "candidate_sha": args.candidate_sha,
        "expected_samples": args.expected_samples, "results": results, "ratios": ratios,
    }
    Path(args.json_output).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = render(
        args.baseline_sha, args.candidate_sha, args.expected_samples,
        args.max_regression_percent, args.max_adapter_ratio, passed, results, ratios,
    )
    Path(args.markdown_output).write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
