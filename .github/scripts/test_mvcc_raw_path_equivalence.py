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
        self.assertFalse(payload["measurement_pass"])
        self.assertFalse(payload["results"][0]["measurement_pass"])
        self.assertNotIn("pass", payload)
        markdown = markdown_output.read_text(encoding="utf-8")
        self.assertIn("- verdict: **EQUIVALENT**", markdown)
        self.assertIn("- measured threshold observation: **FAIL**", markdown)


if __name__ == "__main__":
    unittest.main()
