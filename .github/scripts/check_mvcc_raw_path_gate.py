#!/usr/bin/env python3
"""Check paired TreeDB raw-path medians and benchmark-binary equivalence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path


BENCHMARKS = (
    "BenchmarkGetVersioned",
    "BenchmarkConditionalTxnBaselineBatchWrite",
    "BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek",
    "BenchmarkRepeatedIterator",
    "BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1",
)
BINARY_PACKAGE_BY_BENCHMARK = {
    "BenchmarkGetVersioned": "db",
    "BenchmarkConditionalTxnBaselineBatchWrite": "db",
    "BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek": "treedb",
    "BenchmarkRepeatedIterator": "caching",
    "BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1": "treedb",
}
BINARY_PACKAGES = ("db", "caching", "treedb")
BENCH_RE = re.compile(
    r"^(Benchmark.+?)(?:-\d+)?\s+\d+\s+([0-9.]+)\s+ns/op\s+(.*)$"
)
METRIC_RE = re.compile(r"([0-9.eE+-]+)\s+(B/op|allocs/op)(?:\s|$)")


@dataclass(frozen=True)
class Sample:
    ns_per_op: float
    bytes_per_op: float
    allocs_per_op: float


def sha256_file(path: Path) -> tuple[str, tuple[int, int]]:
    if path.is_symlink():
        raise ValueError(f"benchmark binary evidence must not be a symlink: {path}")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            metadata = os.fstat(source.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(
                    f"benchmark binary evidence must be a regular file: {path}"
                )
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ValueError(f"missing benchmark binary evidence: {path}: {error}") from error
    return digest.hexdigest(), (metadata.st_dev, metadata.st_ino)


def compute_binary_digests(
    binary_paths: dict[str, dict[str, Path]],
) -> dict[str, dict[str, str]]:
    if set(binary_paths) != set(BINARY_PACKAGES):
        raise ValueError("binary path mapping must contain exactly db, caching, and treedb")
    digests: dict[str, dict[str, str]] = {}
    identities: set[tuple[int, int]] = set()
    for package in BINARY_PACKAGES:
        revisions = binary_paths[package]
        if set(revisions) != {"baseline", "candidate"}:
            raise ValueError(
                f"binary path mapping for {package} must contain exactly baseline and candidate"
            )
        digests[package] = {}
        for revision in ("baseline", "candidate"):
            path = revisions[revision]
            expected_name = f"{revision}-{package}.test"
            if path.name != expected_name:
                raise ValueError(
                    f"{revision} {package} binary must be named {expected_name}: {path}"
                )
            digest, identity = sha256_file(path)
            if identity in identities:
                raise ValueError(f"benchmark binary paths must identify six files: {path}")
            identities.add(identity)
            digests[package][revision] = digest
    return digests


def parse_benchmarks(path: Path, expected_samples: int) -> dict[str, list[Sample]]:
    samples = {name: [] for name in BENCHMARKS}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8", errors="replace").splitlines(), 1
    ):
        match = BENCH_RE.match(line.strip())
        if match is None:
            continue
        name, ns_per_op, tail = match.groups()
        if name not in samples:
            continue
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
        samples[name].append(
            Sample(float(ns_per_op), metrics["B/op"], metrics["allocs/op"])
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


def median_paired_ns_delta_percent(
    baseline: list[Sample], candidate: list[Sample]
) -> float:
    if len(baseline) != len(candidate):
        raise ValueError("baseline and candidate timing sample counts must match")
    deltas: list[float] = []
    for index, (base, head) in enumerate(zip(baseline, candidate), 1):
        if not math.isfinite(base.ns_per_op) or base.ns_per_op <= 0.0:
            raise ValueError(
                f"paired sample {index}: baseline ns/op must be positive, got {base.ns_per_op}"
            )
        if not math.isfinite(head.ns_per_op) or head.ns_per_op <= 0.0:
            raise ValueError(
                f"paired sample {index}: candidate ns/op must be positive, got {head.ns_per_op}"
            )
        deltas.append(((head.ns_per_op / base.ns_per_op) - 1.0) * 100.0)
    return statistics.median(deltas)


def evaluate(
    baseline: dict[str, list[Sample]],
    candidate: dict[str, list[Sample]],
    max_regression_percent: float,
    max_bytes_regression_percent: float,
    max_bytes_regression_absolute: float,
) -> tuple[bool, list[dict[str, object]]]:
    passed = True
    results: list[dict[str, object]] = []
    for name in BENCHMARKS:
        base = median_sample(baseline[name])
        head = median_sample(candidate[name])
        paired_ns_delta_percent = median_paired_ns_delta_percent(
            baseline[name], candidate[name]
        )
        ns_delta_percent = ((head.ns_per_op / base.ns_per_op) - 1.0) * 100.0
        timing_pass = paired_ns_delta_percent <= max_regression_percent
        bytes_delta = head.bytes_per_op - base.bytes_per_op
        bytes_tolerance = min(
            base.bytes_per_op * max_bytes_regression_percent / 100.0,
            max_bytes_regression_absolute,
        )
        bytes_pass = bytes_delta <= bytes_tolerance
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
                "paired_ns_delta_percent": paired_ns_delta_percent,
                "bytes_delta": bytes_delta,
                "bytes_tolerance": bytes_tolerance,
                "timing_pass": timing_pass,
                "bytes_pass": bytes_pass,
                "allocs_pass": allocs_pass,
                "measurement_pass": row_pass,
            }
        )
    return passed, results


def annotate_binary_equivalence(
    results: list[dict[str, object]],
    binary_digests: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    annotated: list[dict[str, object]] = []
    for result in results:
        benchmark = result["benchmark"]
        assert isinstance(benchmark, str)
        measurement_pass = result["measurement_pass"]
        assert isinstance(measurement_pass, bool)
        package = BINARY_PACKAGE_BY_BENCHMARK[benchmark]
        package_digests = binary_digests[package]
        equivalent = package_digests["baseline"] == package_digests["candidate"]
        annotated.append(
            {
                **result,
                "binary_package": package,
                "binary_equivalent": equivalent,
                "attribution": (
                    "NON_ATTRIBUTABLE" if equivalent else "CANDIDATE"
                ),
                "acceptance_pass": measurement_pass or equivalent,
                "acceptance_verdict": (
                    "PASS"
                    if measurement_pass
                    else "EQUIVALENT"
                    if equivalent
                    else "FAIL"
                ),
            }
        )
    return annotated


def acceptance_verdict(results: list[dict[str, object]]) -> str:
    if all(row["measurement_pass"] for row in results):
        return "PASS"
    return "EQUIVALENT" if all(row["acceptance_pass"] for row in results) else "FAIL"


def render_markdown(
    baseline_sha: str,
    candidate_sha: str,
    expected_samples: int,
    max_regression_percent: float,
    max_bytes_regression_percent: float,
    max_bytes_regression_absolute: float,
    measurement_pass: bool,
    verdict: str,
    binary_digests: dict[str, dict[str, str]],
    results: list[dict[str, object]],
) -> str:
    lines = [
        "## TreeDB MVCC raw-path gate",
        "",
        f"- verdict: **{verdict}**",
        f"- measured threshold observation: **{'PASS' if measurement_pass else 'FAIL'}**",
        f"- baseline: `{baseline_sha}`",
        f"- candidate: `{candidate_sha}`",
        f"- samples: {expected_samples} per revision, benchmark-group-paired alternating AB/BA order",
        "- timing acceptance: median paired candidate/base relative delta <= "
        f"{max_regression_percent:g}% (base/head medians remain reported for context)",
        "- allocs/op threshold: candidate median must not increase",
        "- B/op jitter threshold: candidate median may increase by at most the smaller of "
        f"{max_bytes_regression_percent:g}% or {max_bytes_regression_absolute:g} B; "
        "zero-B baselines remain strict",
    ]
    if verdict == "EQUIVALENT":
        lines.extend(
            [
                "- equivalence acceptance: failed rows whose owning base/head benchmark "
                "binary is byte-identical remain measured and reported, but are not "
                "attributable to the candidate revision; rows with changed binaries "
                "remain threshold-enforced",
            ]
        )
    lines.extend(
        [
            "",
            "| Package | Baseline SHA-256 | Candidate SHA-256 | Relation |",
            "| --- | --- | --- | --- |",
        ]
    )
    for package in BINARY_PACKAGES:
        base_digest = binary_digests[package]["baseline"]
        candidate_digest = binary_digests[package]["candidate"]
        relation = "EQUIVALENT" if base_digest == candidate_digest else "DIFFERENT"
        lines.append(
            f"| {package} | `{base_digest}` | `{candidate_digest}` | {relation} |"
        )
    lines.extend(
        [
            "",
            "| Benchmark | Binary | Base ns/op | Head ns/op | Median delta | Paired delta | Base B/op | Head B/op | B tolerance | Base allocs/op | Head allocs/op | Measured | Attribution | Acceptance |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in results:
        base = row["baseline"]
        head = row["candidate"]
        assert isinstance(base, dict)
        assert isinstance(head, dict)
        lines.append(
            "| {benchmark} | {binary} | {base_ns:.3f} | {head_ns:.3f} | {delta:+.2f}% | {paired_delta:+.2f}% | "
            "{base_bytes:.3f} | {head_bytes:.3f} | {bytes_tolerance:.3f} | {base_allocs:.3f} | "
            "{head_allocs:.3f} | {status} | {attribution} | {acceptance} |".format(
                benchmark=row["benchmark"],
                binary=(
                    f"{row['binary_package']} EQUIVALENT"
                    if row["binary_equivalent"]
                    else f"{row['binary_package']} DIFFERENT"
                ),
                base_ns=base["ns_per_op"],
                head_ns=head["ns_per_op"],
                delta=row["ns_delta_percent"],
                paired_delta=row["paired_ns_delta_percent"],
                base_bytes=base["bytes_per_op"],
                head_bytes=head["bytes_per_op"],
                bytes_tolerance=row["bytes_tolerance"],
                base_allocs=base["allocs_per_op"],
                head_allocs=head["allocs_per_op"],
                status="PASS" if row["measurement_pass"] else "FAIL",
                attribution=row["attribution"],
                acceptance=row["acceptance_verdict"],
            )
        )
    lines.append("")
    return "\n".join(lines)


def synthetic_log(
    ns_scale: float = 1.0,
    bytes_base: float = 128.0,
    bytes_delta: float = 0.0,
    alloc_delta: float = 0.0,
    runs: int = 8,
) -> str:
    lines: list[str] = []
    for index in range(runs):
        for bench_index, name in enumerate(BENCHMARKS):
            ns_per_op = (1000.0 + bench_index * 9000.0 + index) * ns_scale
            bytes_per_op = bytes_base + bench_index * bytes_base + bytes_delta
            allocs_per_op = 2.0 + bench_index + alloc_delta
            lines.append(
                f"{name}-1 1000 {ns_per_op:.3f} ns/op "
                f"7.000 custom_metric/op "
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
        baseline = parse_benchmarks(baseline_path, 8)
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if not passed:
            raise AssertionError("4% timing change should pass")

        candidate_path.write_text(synthetic_log(ns_scale=1.06), encoding="utf-8")
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if passed:
            raise AssertionError("6% timing regression should fail")

        candidate_path.write_text(synthetic_log(alloc_delta=1.0), encoding="utf-8")
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if passed:
            raise AssertionError("allocation regression should fail")

        baseline_path.write_text(synthetic_log(bytes_base=100.0), encoding="utf-8")
        candidate_path.write_text(
            synthetic_log(bytes_base=100.0, bytes_delta=1.0), encoding="utf-8"
        )
        baseline = parse_benchmarks(baseline_path, 8)
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if not passed:
            raise AssertionError("B/op increase at the 1% boundary should pass")

        candidate_path.write_text(
            synthetic_log(bytes_base=100.0, bytes_delta=1.001), encoding="utf-8"
        )
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if passed:
            raise AssertionError("B/op increase above the 1% boundary should fail")

        baseline_path.write_text(synthetic_log(bytes_base=10000.0), encoding="utf-8")
        candidate_path.write_text(
            synthetic_log(bytes_base=10000.0, bytes_delta=64.0), encoding="utf-8"
        )
        baseline = parse_benchmarks(baseline_path, 8)
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if not passed:
            raise AssertionError("B/op increase at the 64 B boundary should pass")

        candidate_path.write_text(
            synthetic_log(bytes_base=10000.0, bytes_delta=64.001), encoding="utf-8"
        )
        candidate = parse_benchmarks(candidate_path, 8)
        passed, _ = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if passed:
            raise AssertionError("B/op increase above the 64 B boundary should fail")

        observed_baseline = [
            1302945, 1176097, 1679767, 1962560, 1050465, 4438267, 1170681, 660182,
        ]
        observed_candidate = [
            2491471, 839760, 1502818, 743114, 1287476, 4460418, 915485, 1558013,
        ]
        baseline = {
            name: [Sample(ns_per_op, 128.0, 2.0) for ns_per_op in observed_baseline]
            for name in BENCHMARKS
        }
        candidate = {
            name: [Sample(ns_per_op, 128.0, 2.0) for ns_per_op in observed_candidate]
            for name in BENCHMARKS
        }
        passed, results = evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        if not passed or results[0]["paired_ns_delta_percent"] >= 0.0:
            raise AssertionError("paired samples should accept the observed false failure")

        candidate_path.write_text(synthetic_log(runs=7), encoding="utf-8")
        try:
            parse_benchmarks(candidate_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("wrong sample count should fail")

        candidate_path.write_text(
            synthetic_log().replace(BENCHMARKS[0] + "-1", "IgnoredBenchmark-1"),
            encoding="utf-8",
        )
        try:
            parse_benchmarks(candidate_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("missing required row should fail")
        candidate_path.write_text(
            synthetic_log().replace(
                "128.000 B/op", "128.000 B/op 129.000 B/op", 1
            ),
            encoding="utf-8",
        )
        try:
            parse_benchmarks(candidate_path, 8)
        except ValueError:
            pass
        else:
            raise AssertionError("duplicate metric unit should fail")
    print("check_mvcc_raw_path_gate.py self-test: PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline")
    parser.add_argument("--candidate")
    parser.add_argument("--baseline-sha")
    parser.add_argument("--candidate-sha")
    for package in BINARY_PACKAGES:
        parser.add_argument(f"--baseline-{package}-binary")
        parser.add_argument(f"--candidate-{package}-binary")
    parser.add_argument("--expected-samples", type=int, default=8)
    parser.add_argument("--max-regression-percent", type=float, default=5.0)
    parser.add_argument("--max-bytes-regression-percent", type=float, default=1.0)
    parser.add_argument("--max-bytes-regression-absolute", type=float, default=64.0)
    parser.add_argument("--json-output")
    parser.add_argument("--markdown-output")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    for name in (
        "max_regression_percent",
        "max_bytes_regression_percent",
        "max_bytes_regression_absolute",
    ):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} must be non-negative")

    required = (
        "baseline",
        "candidate",
        "baseline_sha",
        "candidate_sha",
        "json_output",
        "markdown_output",
    )
    required += tuple(
        f"{revision}_{package}_binary"
        for package in BINARY_PACKAGES
        for revision in ("baseline", "candidate")
    )
    missing = [name for name in required if getattr(args, name) in (None, "")]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))
    if args.expected_samples <= 0 or args.expected_samples % 2 != 0:
        parser.error("--expected-samples must be a positive even integer")

    baseline = parse_benchmarks(Path(args.baseline), args.expected_samples)
    candidate = parse_benchmarks(Path(args.candidate), args.expected_samples)
    binary_digests = compute_binary_digests(
        {
            package: {
                revision: Path(getattr(args, f"{revision}_{package}_binary"))
                for revision in ("baseline", "candidate")
            }
            for package in BINARY_PACKAGES
        }
    )
    measurement_pass, results = evaluate(
        baseline,
        candidate,
        args.max_regression_percent,
        args.max_bytes_regression_percent,
        args.max_bytes_regression_absolute,
    )
    results = annotate_binary_equivalence(results, binary_digests)
    verdict = acceptance_verdict(results)
    accepted = verdict in {"PASS", "EQUIVALENT"}
    payload = {
        "accepted": accepted,
        "no_attributable_regression": accepted,
        "verdict": verdict,
        "measurement_pass": measurement_pass,
        "attributable_measurement_pass": all(
            row["measurement_pass"]
            for row in results
            if not row["binary_equivalent"]
        ),
        "all_row_binaries_equivalent": all(
            row["binary_equivalent"] for row in results
        ),
        "binary_digests": binary_digests,
        "baseline_sha": args.baseline_sha,
        "candidate_sha": args.candidate_sha,
        "expected_samples": args.expected_samples,
        "max_regression_percent": args.max_regression_percent,
        "max_bytes_regression_percent": args.max_bytes_regression_percent,
        "max_bytes_regression_absolute": args.max_bytes_regression_absolute,
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
        args.max_bytes_regression_percent,
        args.max_bytes_regression_absolute,
        measurement_pass,
        verdict,
        binary_digests,
        results,
    )
    Path(args.markdown_output).write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
