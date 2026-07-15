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


class RawPathEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def results(self, passed: bool) -> list[dict[str, object]]:
        return [
            {"benchmark": benchmark, "measurement_pass": passed}
            for benchmark in CHECKER.BENCHMARKS
        ]

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

    def test_identical_package_digests_accept_equivalent_despite_failed_measurement(self) -> None:
        parsed = CHECKER.compute_binary_digests(
            self.binary_paths(set(CHECKER.BINARY_PACKAGES))
        )
        all_equivalent, rows = CHECKER.annotate_binary_equivalence(
            self.results(False), parsed
        )
        self.assertTrue(all_equivalent)
        self.assertTrue(all(row["binary_equivalent"] for row in rows))
        self.assertFalse(all(row["measurement_pass"] for row in rows))
        self.assertEqual(
            CHECKER.acceptance_verdict(False, all_equivalent), "EQUIVALENT"
        )

    def test_verdict_truth_table(self) -> None:
        cases = (
            (True, True, "PASS"),
            (True, False, "PASS"),
            (False, True, "EQUIVALENT"),
            (False, False, "FAIL"),
        )
        for measurement_pass, all_equivalent, expected in cases:
            with self.subTest(
                measurement_pass=measurement_pass,
                all_equivalent=all_equivalent,
            ):
                self.assertEqual(
                    CHECKER.acceptance_verdict(measurement_pass, all_equivalent),
                    expected,
                )

    def test_different_package_digests_use_measured_pass(self) -> None:
        parsed = CHECKER.compute_binary_digests(self.binary_paths(set()))
        all_equivalent, _ = CHECKER.annotate_binary_equivalence(
            self.results(True), parsed
        )
        self.assertFalse(all_equivalent)
        self.assertEqual(CHECKER.acceptance_verdict(True, all_equivalent), "PASS")

    def test_mixed_digests_cannot_mask_failed_measurement(self) -> None:
        parsed = CHECKER.compute_binary_digests(
            self.binary_paths({"db", "treedb"})
        )
        all_equivalent, rows = CHECKER.annotate_binary_equivalence(
            self.results(False), parsed
        )
        self.assertFalse(all_equivalent)
        self.assertTrue(rows[0]["binary_equivalent"])
        self.assertFalse(rows[3]["binary_equivalent"])
        self.assertEqual(CHECKER.acceptance_verdict(False, all_equivalent), "FAIL")

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
        self.assertFalse(payload["results"][0]["measurement_pass"])
        self.assertAlmostEqual(payload["results"][0]["paired_ns_delta_percent"], 20.0)
        self.assertNotIn("pass", payload)
        markdown = markdown_output.read_text(encoding="utf-8")
        self.assertIn("- verdict: **EQUIVALENT**", markdown)
        self.assertIn("- measured threshold observation: **FAIL**", markdown)
        self.assertIn("timing acceptance: median paired candidate/base", markdown)
        self.assertIn("| Median delta | Paired delta |", markdown)


if __name__ == "__main__":
    unittest.main()
