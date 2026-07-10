#!/usr/bin/env python3
"""Check paired TreeDB raw-path benchmark medians without third-party modules."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path


BENCHMARKS = (
    "BenchmarkGetVersioned",
    "BenchmarkConditionalTxnBaselineBatchWrite",
)
BENCH_RE = re.compile(
    r"^(Benchmark(?:GetVersioned|ConditionalTxnBaselineBatchWrite))(?:-\d+)?\s+"
    r"\d+\s+([0-9.]+)\s+ns/op\s+([0-9.]+)\s+B/op\s+"
    r"([0-9.]+)\s+allocs/op\s*$"
)


@dataclass(frozen=True)
class Sample:
    ns_per_op: float
    bytes_per_op: float
    allocs_per_op: float


def parse_benchmarks(path: Path, expected_samples: int) -> dict[str, list[Sample]]:
    samples = {name: [] for name in BENCHMARKS}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = BENCH_RE.match(line.strip())
        if match is None:
            continue
        name, ns_per_op, bytes_per_op, allocs_per_op = match.groups()
        samples[name].append(
            Sample(float(ns_per_op), float(bytes_per_op), float(allocs_per_op))
        )

    for name, rows in samples.items():
        if len(rows) != expected_samples:
            raise ValueError(
                f"{path}: expected {expected_samples} samples for {name}, got {len(rows)}"
            )
    return samples


def median_sample(samples: list[Sample]) -> Sample:
    return Sample(
        statistics.median(sample.ns_per_op for sample in samples),
        statistics.median(sample.bytes_per_op for sample in samples),
        statistics.median(sample.allocs_per_op for sample in samples),
    )


def evaluate(
    baseline: dict[str, list[Sample]],
    candidate: dict[str, list[Sample]],
    max_regression_percent: float,
) -> tuple[bool, list[dict[str, object]]]:
    passed = True
    results: list[dict[str, object]] = []
    for name in BENCHMARKS:
        base = median_sample(baseline[name])
        head = median_sample(candidate[name])
        ns_delta_percent = ((head.ns_per_op / base.ns_per_op) - 1.0) * 100.0
        timing_pass = ns_delta_percent <= max_regression_percent
        bytes_pass = head.bytes_per_op <= base.bytes_per_op
        allocs_pass = head.allocs_per_op <= base.allocs_per_op
        row_pass = timing_pass and bytes_pass and allocs_pass
        passed = passed and row_pass
        results.append(
            {
                "benchmark": name,
                "baseline": {
                    "ns_per_op": base.ns_per_op,
                    "bytes_per_op": base.bytes_per_op,
                    "allocs_per_op": base.allocs_per_op,
                },
                "candidate": {
                    "ns_per_op": head.ns_per_op,
                    "bytes_per_op": head.bytes_per_op,
                    "allocs_per_op": head.allocs_per_op,
                },
                "ns_delta_percent": ns_delta_percent,
                "timing_pass": timing_pass,
                "bytes_pass": bytes_pass,
                "allocs_pass": allocs_pass,
                "pass": row_pass,
            }
        )
    return passed, results


def render_markdown(
    baseline_sha: str,
    candidate_sha: str,
    expected_samples: int,
    max_regression_percent: float,
    passed: bool,
    results: list[dict[str, object]],
) -> str:
    lines = [
        "## TreeDB MVCC raw-path gate",
        "",
        f"- result: **{'PASS' if passed else 'FAIL'}**",
        f"- baseline: `{baseline_sha}`",
        f"- candidate: `{candidate_sha}`",
        f"- samples: {expected_samples} per revision, alternating sequential order",
        f"- timing threshold: candidate median <= baseline median + {max_regression_percent:g}%",
        "- allocation threshold: candidate median B/op and allocs/op must not increase",
        "",
        "| Benchmark | Base ns/op | Head ns/op | Delta | Base B/op | Head B/op | Base allocs/op | Head allocs/op | Result |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in results:
        base = row["baseline"]
        head = row["candidate"]
        assert isinstance(base, dict)
        assert isinstance(head, dict)
        lines.append(
            "| {benchmark} | {base_ns:.3f} | {head_ns:.3f} | {delta:+.2f}% | "
            "{base_bytes:.3f} | {head_bytes:.3f} | {base_allocs:.3f} | "
            "{head_allocs:.3f} | {status} |".format(
                benchmark=row["benchmark"],
                base_ns=base["ns_per_op"],
                head_ns=head["ns_per_op"],
                delta=row["ns_delta_percent"],
                base_bytes=base["bytes_per_op"],
                head_bytes=head["bytes_per_op"],
                base_allocs=base["allocs_per_op"],
                head_allocs=head["allocs_per_op"],
                status="PASS" if row["pass"] else "FAIL",
            )
        )
    lines.append("")
    return "\n".join(lines)


def synthetic_log(
    ns_scale: float = 1.0,
    bytes_delta: float = 0.0,
    alloc_delta: float = 0.0,
) -> str:
    lines: list[str] = []
    for index in range(7):
        for bench_index, name in enumerate(BENCHMARKS):
            ns_per_op = (1000.0 + bench_index * 9000.0 + index) * ns_scale
            bytes_per_op = 128.0 + bench_index * 128.0 + bytes_delta
            allocs_per_op = 2.0 + bench_index + alloc_delta
            lines.append(
                f"{name}-1 1000 {ns_per_op:.3f} ns/op "
                f"{bytes_per_op:.3f} B/op {allocs_per_op:.3f} allocs/op"
            )
    return "\n".join(lines) + "\n"


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        baseline_path = root / "baseline.txt"
        candidate_path = root / "candidate.txt"
        baseline_path.write_text(synthetic_log(), encoding="utf-8")
        candidate_path.write_text(synthetic_log(ns_scale=1.04), encoding="utf-8")
        baseline = parse_benchmarks(baseline_path, 7)
        candidate = parse_benchmarks(candidate_path, 7)
        passed, _ = evaluate(baseline, candidate, 5.0)
        if not passed:
            raise AssertionError("4% timing change should pass")

        candidate_path.write_text(synthetic_log(ns_scale=1.06), encoding="utf-8")
        candidate = parse_benchmarks(candidate_path, 7)
        passed, _ = evaluate(baseline, candidate, 5.0)
        if passed:
            raise AssertionError("6% timing regression should fail")

        candidate_path.write_text(synthetic_log(alloc_delta=1.0), encoding="utf-8")
        candidate = parse_benchmarks(candidate_path, 7)
        passed, _ = evaluate(baseline, candidate, 5.0)
        if passed:
            raise AssertionError("allocation regression should fail")

        candidate_path.write_text(synthetic_log(bytes_delta=1.0), encoding="utf-8")
        candidate = parse_benchmarks(candidate_path, 7)
        passed, _ = evaluate(baseline, candidate, 5.0)
        if passed:
            raise AssertionError("byte allocation regression should fail")

        candidate_path.write_text("", encoding="utf-8")
        try:
            parse_benchmarks(candidate_path, 7)
        except ValueError:
            pass
        else:
            raise AssertionError("missing benchmark rows should fail")
    print("check_mvcc_raw_path_gate.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline")
    parser.add_argument("--candidate")
    parser.add_argument("--baseline-sha")
    parser.add_argument("--candidate-sha")
    parser.add_argument("--expected-samples", type=int, default=7)
    parser.add_argument("--max-regression-percent", type=float, default=5.0)
    parser.add_argument("--json-output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    required = (
        "baseline",
        "candidate",
        "baseline_sha",
        "candidate_sha",
        "json_output",
        "markdown_output",
    )
    missing = [name for name in required if getattr(args, name) in (None, "")]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))

    baseline = parse_benchmarks(Path(args.baseline), args.expected_samples)
    candidate = parse_benchmarks(Path(args.candidate), args.expected_samples)
    passed, results = evaluate(
        baseline, candidate, args.max_regression_percent
    )
    payload = {
        "pass": passed,
        "baseline_sha": args.baseline_sha,
        "candidate_sha": args.candidate_sha,
        "expected_samples": args.expected_samples,
        "max_regression_percent": args.max_regression_percent,
        "results": results,
    }
    Path(args.json_output).write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    markdown = render_markdown(
        args.baseline_sha,
        args.candidate_sha,
        args.expected_samples,
        args.max_regression_percent,
        passed,
        results,
    )
    Path(args.markdown_output).write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
