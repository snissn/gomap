#!/usr/bin/env python3
"""Tests fail-closed binary equivalence acceptance for the MVCC raw gate."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHECKER_PATH = ROOT / ".github" / "scripts" / "check_mvcc_raw_path_gate.py"
SPEC = importlib.util.spec_from_file_location("check_mvcc_raw_path_gate", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECKER
SPEC.loader.exec_module(CHECKER)


SYNTHETIC_RUNS = 8
SYNTHETIC_NS_PER_OP = 100.0
SYNTHETIC_BASE_BYTES_PER_OP = 10000.0
SYNTHETIC_FAILED_BYTES_PER_OP = 10065.0
SYNTHETIC_ALLOCS_PER_OP = 2.0


class RawPathEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def results_with_byte_regressions(
        self, failed_benchmarks: set[str]
    ) -> list[dict[str, object]]:
        baseline = {
            benchmark: [
                CHECKER.Sample(
                    SYNTHETIC_NS_PER_OP,
                    SYNTHETIC_BASE_BYTES_PER_OP,
                    SYNTHETIC_ALLOCS_PER_OP,
                )
                for _ in range(SYNTHETIC_RUNS)
            ]
            for benchmark in CHECKER.BENCHMARKS
        }
        candidate = {
            benchmark: [
                CHECKER.Sample(
                    SYNTHETIC_NS_PER_OP,
                    (
                        SYNTHETIC_FAILED_BYTES_PER_OP
                        if benchmark in failed_benchmarks
                        else SYNTHETIC_BASE_BYTES_PER_OP
                    ),
                    SYNTHETIC_ALLOCS_PER_OP,
                )
                for _ in range(SYNTHETIC_RUNS)
            ]
            for benchmark in CHECKER.BENCHMARKS
        }
        measurement_pass, rows = CHECKER.evaluate(
            baseline, candidate, 5.0, 1.0, 64.0
        )
        self.assertEqual(measurement_pass, not failed_benchmarks)
        return rows

    @staticmethod
    def log_with_byte_regressions(failed_benchmarks: set[str]) -> str:
        lines: list[str] = []
        for _ in range(SYNTHETIC_RUNS):
            for benchmark in CHECKER.BENCHMARKS:
                bytes_per_op = (
                    SYNTHETIC_FAILED_BYTES_PER_OP
                    if benchmark in failed_benchmarks
                    else SYNTHETIC_BASE_BYTES_PER_OP
                )
                lines.append(
                    f"{benchmark}-1 1000 {SYNTHETIC_NS_PER_OP:.3f} ns/op "
                    f"{bytes_per_op:.3f} B/op "
                    f"{SYNTHETIC_ALLOCS_PER_OP:.3f} allocs/op"
                )
        return "\n".join(lines) + "\n"

    @staticmethod
    def row_for(
        rows: list[dict[str, object]], benchmark: str
    ) -> dict[str, object]:
        return next(row for row in rows if row["benchmark"] == benchmark)

    @staticmethod
    def samples(timings: list[float]) -> list[CHECKER.Sample]:
        return [CHECKER.Sample(timing, 128.0, 2.0) for timing in timings]

    def benchmark_sets(
        self, baseline_timings: list[float], candidate_timings: list[float]
    ) -> tuple[dict[str, list[CHECKER.Sample]], dict[str, list[CHECKER.Sample]]]:
        return (
            {benchmark: self.samples(baseline_timings) for benchmark in CHECKER.BENCHMARKS},
            {benchmark: self.samples(candidate_timings) for benchmark in CHECKER.BENCHMARKS},
        )

    def test_paired_timing_accepts_observed_independent_median_false_failure(self) -> None:
        baseline, candidate = self.benchmark_sets(
            [
                1302945,
                1176097,
                1679767,
                1962560,
                1050465,
                4438267,
                1170681,
                660182,
            ],
            [
                2491471,
                839760,
                1502818,
                743114,
                1287476,
                4460418,
                915485,
                1558013,
            ],
        )
        passed, results = CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        self.assertTrue(passed)
        self.assertGreater(results[0]["ns_delta_percent"], 5.0)
        self.assertLess(results[0]["paired_ns_delta_percent"], 0.0)
        self.assertTrue(results[0]["timing_pass"])

    def test_paired_timing_rejects_real_regression(self) -> None:
        baseline, candidate = self.benchmark_sets(
            [100.0] * 8,
            [106.0] * 8,
        )
        passed, results = CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)
        self.assertFalse(passed)
        self.assertAlmostEqual(results[0]["paired_ns_delta_percent"], 6.0)
        self.assertFalse(results[0]["timing_pass"])

    def test_paired_timing_rejects_non_positive_baseline(self) -> None:
        baseline, candidate = self.benchmark_sets(
            [100.0, 100.0, 100.0, 0.0, 100.0, 100.0, 100.0, 100.0],
            [100.0] * 8,
        )
        with self.assertRaisesRegex(ValueError, "baseline ns/op must be positive"):
            CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)

        baseline, candidate = self.benchmark_sets([0.0] * 8, [100.0] * 8)
        with self.assertRaisesRegex(ValueError, "baseline ns/op must be positive"):
            CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)

    def test_paired_timing_rejects_non_positive_candidate(self) -> None:
        baseline, candidate = self.benchmark_sets(
            [100.0] * 8,
            [100.0, 100.0, 100.0, 0.0, 100.0, 100.0, 100.0, 100.0],
        )
        with self.assertRaisesRegex(ValueError, "candidate ns/op must be positive"):
            CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)

        baseline, candidate = self.benchmark_sets([100.0] * 8, [0.0] * 8)
        with self.assertRaisesRegex(ValueError, "candidate ns/op must be positive"):
            CHECKER.evaluate(baseline, candidate, 5.0, 1.0, 64.0)

    def binary_paths(
        self, equivalent_packages: set[str]
    ) -> dict[str, dict[str, Path]]:
        paths: dict[str, dict[str, Path]] = {}
        for package in CHECKER.BINARY_PACKAGES:
            baseline = self.root / f"baseline-{package}.test"
            candidate = self.root / f"candidate-{package}.test"
            baseline.write_bytes(f"{package}-baseline".encode())
            candidate.write_bytes(
                baseline.read_bytes()
                if package in equivalent_packages
                else f"{package}-candidate".encode()
            )
            paths[package] = {"baseline": baseline, "candidate": candidate}
        return paths

    def test_equivalent_binary_row_accepts_failed_measurement_despite_changed_other_binary(self) -> None:
        parsed = CHECKER.compute_binary_digests(
            self.binary_paths({"db", "treedb"})
        )
        rows = CHECKER.annotate_binary_equivalence(
            self.results_with_byte_regressions(
                {"BenchmarkConditionalTxnBaselineBatchWrite"}
            ),
            parsed,
        )
        batch_write = self.row_for(
            rows, "BenchmarkConditionalTxnBaselineBatchWrite"
        )
        self.assertTrue(batch_write["binary_equivalent"])
        self.assertFalse(batch_write["measurement_pass"])
        self.assertFalse(batch_write["bytes_pass"])
        self.assertEqual(batch_write["bytes_delta"], 65.0)
        self.assertEqual(batch_write["attribution"], "NON_ATTRIBUTABLE")
        self.assertEqual(batch_write["acceptance_verdict"], "EQUIVALENT")
        self.assertEqual(CHECKER.acceptance_verdict(rows), "EQUIVALENT")

    def test_changed_binary_failed_row_remains_attributable(self) -> None:
        parsed = CHECKER.compute_binary_digests(
            self.binary_paths({"caching", "treedb"})
        )
        rows = CHECKER.annotate_binary_equivalence(
            self.results_with_byte_regressions(
                {"BenchmarkConditionalTxnBaselineBatchWrite"}
            ),
            parsed,
        )
        batch_write = self.row_for(
            rows, "BenchmarkConditionalTxnBaselineBatchWrite"
        )
        self.assertFalse(batch_write["binary_equivalent"])
        self.assertFalse(batch_write["measurement_pass"])
        self.assertEqual(batch_write["attribution"], "CANDIDATE")
        self.assertEqual(batch_write["acceptance_verdict"], "FAIL")
        self.assertEqual(CHECKER.acceptance_verdict(rows), "FAIL")

    def test_mixed_rows_cannot_hide_failed_changed_binary_row(self) -> None:
        parsed = CHECKER.compute_binary_digests(
            self.binary_paths({"db", "treedb"})
        )
        rows = CHECKER.annotate_binary_equivalence(
            self.results_with_byte_regressions(
                {
                    "BenchmarkConditionalTxnBaselineBatchWrite",
                    "BenchmarkRepeatedIterator",
                }
            ),
            parsed,
        )
        self.assertTrue(
            self.row_for(rows, "BenchmarkConditionalTxnBaselineBatchWrite")[
                "binary_equivalent"
            ]
        )
        self.assertFalse(
            self.row_for(rows, "BenchmarkRepeatedIterator")["binary_equivalent"]
        )
        self.assertEqual(CHECKER.acceptance_verdict(rows), "FAIL")

    def test_malformed_or_missing_binary_evidence_fails_closed(self) -> None:
        valid = self.binary_paths(set(CHECKER.BINARY_PACKAGES))
        duplicate = self.root / "duplicate" / "candidate-db.test"
        duplicate.parent.mkdir()
        duplicate.symlink_to(valid["db"]["baseline"])
        cases = {
            "missing package": {key: value for key, value in valid.items() if key != "db"},
            "unexpected package": {**valid, "other": valid["db"]},
            "missing revision": {
                **valid,
                "db": {"baseline": valid["db"]["baseline"]},
            },
            "missing file": {
                **valid,
                "db": {
                    **valid["db"],
                    "candidate": self.root / "missing" / "candidate-db.test",
                },
            },
            "symlinked duplicate": {
                **valid,
                "db": {**valid["db"], "candidate": duplicate},
            },
        }
        for name, paths in cases.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                CHECKER.compute_binary_digests(paths)

    def test_cli_emits_distinct_equivalent_verdict_and_failed_observation(self) -> None:
        baseline_log = self.root / "baseline.txt"
        candidate_log = self.root / "candidate.txt"
        baseline_log.write_text(CHECKER.synthetic_log(), encoding="utf-8")
        candidate_log.write_text(
            CHECKER.synthetic_log(ns_scale=1.20), encoding="utf-8"
        )
        paths = self.binary_paths(set(CHECKER.BINARY_PACKAGES))
        json_output = self.root / "summary.json"
        markdown_output = self.root / "summary.md"
        command = [
            sys.executable,
            str(CHECKER_PATH),
            "--baseline",
            str(baseline_log),
            "--candidate",
            str(candidate_log),
            "--baseline-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--json-output",
            str(json_output),
            "--markdown-output",
            str(markdown_output),
        ]
        for package in CHECKER.BINARY_PACKAGES:
            for revision in ("baseline", "candidate"):
                command.extend(
                    [
                        f"--{revision}-{package}-binary",
                        str(paths[package][revision]),
                    ]
                )
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(json_output.read_text(encoding="utf-8"))
        self.assertEqual(payload["verdict"], "EQUIVALENT")
        self.assertTrue(payload["accepted"])
        self.assertTrue(payload["no_attributable_regression"])
        self.assertFalse(payload["measurement_pass"])
        self.assertTrue(payload["attributable_measurement_pass"])
        self.assertFalse(payload["results"][0]["measurement_pass"])
        self.assertEqual(payload["results"][0]["attribution"], "NON_ATTRIBUTABLE")
        self.assertEqual(payload["results"][0]["acceptance_verdict"], "EQUIVALENT")
        self.assertAlmostEqual(payload["results"][0]["paired_ns_delta_percent"], 20.0)
        self.assertNotIn("pass", payload)
        markdown = markdown_output.read_text(encoding="utf-8")
        self.assertIn("- verdict: **EQUIVALENT**", markdown)
        self.assertIn("- measured threshold observation: **FAIL**", markdown)
        self.assertIn("timing acceptance: median paired candidate/base", markdown)
        self.assertIn("| Median delta | Paired delta |", markdown)
        self.assertIn("| Measured | Attribution | Acceptance |", markdown)

    def test_cli_accepts_only_equivalent_failed_row_in_mixed_digest_evidence(self) -> None:
        baseline_log = self.root / "baseline.txt"
        candidate_log = self.root / "candidate.txt"
        baseline_log.write_text(self.log_with_byte_regressions(set()), encoding="utf-8")
        candidate_log.write_text(
            self.log_with_byte_regressions(
                {"BenchmarkConditionalTxnBaselineBatchWrite"}
            ),
            encoding="utf-8",
        )
        paths = self.binary_paths({"db", "treedb"})
        json_output = self.root / "summary.json"
        markdown_output = self.root / "summary.md"
        command = [
            sys.executable,
            str(CHECKER_PATH),
            "--baseline",
            str(baseline_log),
            "--candidate",
            str(candidate_log),
            "--baseline-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--json-output",
            str(json_output),
            "--markdown-output",
            str(markdown_output),
        ]
        for package in CHECKER.BINARY_PACKAGES:
            for revision in ("baseline", "candidate"):
                command.extend(
                    [
                        f"--{revision}-{package}-binary",
                        str(paths[package][revision]),
                    ]
                )
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(json_output.read_text(encoding="utf-8"))
        self.assertEqual(payload["verdict"], "EQUIVALENT")
        self.assertTrue(payload["accepted"])
        self.assertFalse(payload["measurement_pass"])
        self.assertTrue(payload["attributable_measurement_pass"])
        db_row = self.row_for(
            payload["results"], "BenchmarkConditionalTxnBaselineBatchWrite"
        )
        caching_row = self.row_for(payload["results"], "BenchmarkRepeatedIterator")
        self.assertTrue(db_row["binary_equivalent"])
        self.assertFalse(db_row["measurement_pass"])
        self.assertEqual(db_row["attribution"], "NON_ATTRIBUTABLE")
        self.assertEqual(db_row["acceptance_verdict"], "EQUIVALENT")
        self.assertFalse(caching_row["binary_equivalent"])
        self.assertTrue(caching_row["measurement_pass"])
        self.assertEqual(caching_row["attribution"], "CANDIDATE")
        self.assertEqual(caching_row["acceptance_verdict"], "PASS")
        markdown = markdown_output.read_text(encoding="utf-8")
        self.assertIn("- measured threshold observation: **FAIL**", markdown)
        self.assertIn("NON_ATTRIBUTABLE | EQUIVALENT", markdown)
        self.assertIn("CANDIDATE | PASS", markdown)


if __name__ == "__main__":
    unittest.main()
