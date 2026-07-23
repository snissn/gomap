#!/usr/bin/env python3

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("treedb_vacuum_m0_summarize.py")
sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("vacuum_m0_summarize", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class VacuumM0SummarizeTest(unittest.TestCase):
    def test_summarize_computes_stable_required_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for sample in range(10):
                path = Path(directory) / f"legacy-{sample:02d}.txt"
                path.write_text(
                    "BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x-24 "
                    f"1 {1000 + sample} ns/op {2000 + sample} vacuum-total-ns/op "
                    f"{3000 + sample} max-writer-pause-ns {4000 + sample} foreground-p99-ns/op\n"
                )
                paths.append(path)
            result = MODULE.summarize(paths)
            self.assertEqual(result["vacuum-total-ns/op"]["median"], 2004.5)
            self.assertLess(result["foreground-p99-ns/op"]["cv"], 0.10)

    def test_parse_rejects_more_than_one_benchmark_row(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.txt"
            row = "BenchmarkOne-24 1 10 ns/op\n"
            path.write_text(row + row)
            with self.assertRaisesRegex(ValueError, "expected one benchmark row"):
                MODULE.parse_benchmark(path)

    def test_evaluate_gates_requires_completed_legacy_samples(self):
        legacy = {
            metric: {"cv": 0.01, "samples": [1] * 10}
            for metric in MODULE.REQUIRED_CV_METRICS
        }
        legacy["concurrent-aborts/op"] = {"samples": [0] * 10}
        public = {
            "vacuum-unsupported/op": {"samples": [1] * 10},
            "vacuum-concurrent-retries/op": {"samples": [0] * 10},
            "vacuum-unexpected-errors/op": {"samples": [0] * 10},
            "foreground-exposure-misses/op": {"samples": [1] * 10},
            "foreground-overlap-samples/op": {"samples": [0] * 10},
        }
        self.assertTrue(all(MODULE.evaluate_gates(legacy, public).values()))

        legacy["concurrent-aborts/op"]["samples"][4] = 1
        gates = MODULE.evaluate_gates(legacy, public)
        self.assertFalse(gates["legacy_completed_without_abort"])
        self.assertTrue(gates["legacy_cv_at_most_10_percent"])
        self.assertTrue(gates["public_status_explicit"])

        public["vacuum-unsupported/op"]["samples"] = [0] * 10
        public["foreground-exposure-misses/op"]["samples"] = [0] * 10
        public["foreground-overlap-samples/op"]["samples"] = [160] * 10
        public["vacuum-concurrent-retries/op"]["samples"][4] = 1
        gates = MODULE.evaluate_gates(legacy, public)
        self.assertTrue(gates["public_status_explicit"])
        self.assertEqual(MODULE.classify_public_status(public), "production-index-vacuum-available")

        public["vacuum-concurrent-retries/op"]["samples"] = [1] * 10
        gates = MODULE.evaluate_gates(legacy, public)
        self.assertFalse(gates["public_status_explicit"])
        self.assertEqual(MODULE.classify_public_status(public), "production-index-vacuum-ambiguous")

    def test_evaluate_gates_rejects_unsupported_with_foreground_overlap(self):
        legacy = {
            metric: {"cv": 0.01, "samples": [1] * 10}
            for metric in MODULE.REQUIRED_CV_METRICS
        }
        legacy["concurrent-aborts/op"] = {"samples": [0] * 10}
        public = {
            "vacuum-unsupported/op": {"samples": [1] * 10},
            "vacuum-concurrent-retries/op": {"samples": [0] * 10},
            "vacuum-unexpected-errors/op": {"samples": [0] * 10},
            "foreground-exposure-misses/op": {"samples": [1] * 10},
            "foreground-overlap-samples/op": {"samples": [0] * 9 + [1]},
        }

        gates = MODULE.evaluate_gates(legacy, public)
        self.assertFalse(gates["public_status_explicit"])
        self.assertEqual(MODULE.classify_public_status(public), "production-index-vacuum-ambiguous")

    def test_evaluate_gates_accepts_verified_production_success(self):
        legacy = {
            metric: {"cv": 0.01, "samples": [1] * 10}
            for metric in MODULE.REQUIRED_CV_METRICS
        }
        legacy["concurrent-aborts/op"] = {"samples": [0] * 10}
        public = {
            "vacuum-unsupported/op": {"samples": [0] * 10},
            "vacuum-concurrent-retries/op": {"samples": [0] * 10},
            "vacuum-unexpected-errors/op": {"samples": [0] * 10},
            "foreground-exposure-misses/op": {"samples": [0] * 10},
            "foreground-overlap-samples/op": {"samples": [160] * 10},
        }

        gates = MODULE.evaluate_gates(legacy, public)
        self.assertTrue(gates["public_status_explicit"])
        self.assertEqual(MODULE.classify_public_status(public), "production-index-vacuum-available")

    def test_evaluate_gates_rejects_mixed_public_status(self):
        legacy = {
            metric: {"cv": 0.01, "samples": [1] * 10}
            for metric in MODULE.REQUIRED_CV_METRICS
        }
        legacy["concurrent-aborts/op"] = {"samples": [0] * 10}
        public = {
            "vacuum-unsupported/op": {"samples": [1, 0] * 5},
            "vacuum-concurrent-retries/op": {"samples": [0] * 10},
            "vacuum-unexpected-errors/op": {"samples": [0] * 10},
            "foreground-exposure-misses/op": {"samples": [0] * 10},
            "foreground-overlap-samples/op": {"samples": [0, 160] * 5},
        }

        gates = MODULE.evaluate_gates(legacy, public)
        self.assertFalse(gates["public_status_explicit"])
        self.assertEqual(MODULE.classify_public_status(public), "production-index-vacuum-ambiguous")


if __name__ == "__main__":
    unittest.main()
