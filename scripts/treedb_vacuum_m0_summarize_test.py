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


    @staticmethod
    def production_observations():
        production = {
            metric: {"cv": 0.01, "samples": [1] * 10}
            for metric in MODULE.REQUIRED_CV_METRICS
        }
        for metric, value in {
            "foreground-overlap-samples/op": 4096,
            "foreground-points/op": 2048,
            "foreground-ranges/op": 2048,
            "foreground-exposure-misses/op": 0,
            "vacuum-errors/op": 0,
            "concurrent-aborts/op": 0,
        }.items():
            production[metric] = {"samples": [value] * 10}
        public = {
            "vacuum-unsupported/op": {"samples": [0] * 10},
            "vacuum-concurrent-retries/op": {"samples": [0] * 10},
            "vacuum-unexpected-errors/op": {"samples": [0] * 10},
            "foreground-exposure-misses/op": {"samples": [0] * 10},
            "foreground-overlap-samples/op": {"samples": [160] * 10},
        }
        return production, public

    def test_production_preserves_all_three_cv_limits(self):
        for metric in MODULE.REQUIRED_CV_METRICS:
            with self.subTest(metric=metric):
                production, public = self.production_observations()
                production[metric]["cv"] = 0.10
                self.assertTrue(all(MODULE.evaluate_gates(production, public, "production").values()))
                production[metric]["cv"] = 0.100001
                self.assertFalse(MODULE.evaluate_gates(production, public, "production")["production_cv_at_most_10_percent"])

    def test_production_rejects_missing_or_changed_work(self):
        for metric in ("foreground-overlap-samples/op", "foreground-points/op", "foreground-ranges/op",
                       "foreground-exposure-misses/op", "vacuum-errors/op", "concurrent-aborts/op"):
            with self.subTest(metric=metric):
                production, public = self.production_observations()
                production[metric]["samples"][4] += 1
                self.assertFalse(MODULE.evaluate_gates(production, public, "production")["production_fixed_work_complete"])
                production[metric]["samples"].pop()
                self.assertFalse(MODULE.evaluate_gates(production, public, "production")["production_fixed_work_complete"])
        production, public = self.production_observations()
        production[MODULE.REQUIRED_CV_METRICS[0]]["samples"].pop()
        self.assertFalse(MODULE.evaluate_gates(production, public, "production")["production_stability_observations_complete"])

    def test_production_does_not_accept_renamed_legacy_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "production-1.txt"
            path.write_text(MODULE.STABILITY_BENCHMARKS["legacy"] + "-2 1 10 ns/op\nPASS\n")
            with self.assertRaisesRegex(ValueError, "identity differs"):
                MODULE.parse_benchmark(path, MODULE.STABILITY_BENCHMARKS["production"])
            path.write_text(MODULE.STABILITY_BENCHMARKS["production"] + "-2 1 10 ns/op\nPASS\n")
            self.assertEqual(MODULE.parse_benchmark(path, MODULE.STABILITY_BENCHMARKS["production"]), {"ns/op": 10})
            path.write_text(MODULE.STABILITY_BENCHMARKS["production"] + "-2 2 10 ns/op\nPASS\n")
            with self.assertRaisesRegex(ValueError, "fixed-work iteration"):
                MODULE.parse_benchmark(path, MODULE.STABILITY_BENCHMARKS["production"])
            path.write_text(MODULE.STABILITY_BENCHMARKS["production"] + "-2 1 10 ns/op\n")
            with self.assertRaisesRegex(ValueError, "did not complete"):
                MODULE.parse_benchmark(path, MODULE.STABILITY_BENCHMARKS["production"])

    def test_legacy_predicate_is_not_relabelled(self):
        legacy, public = self.production_observations()
        legacy["foreground-p99-ns/op"]["cv"] = 0.451596031730326
        gates = MODULE.evaluate_gates(legacy, public)
        self.assertFalse(gates["legacy_cv_at_most_10_percent"])
        self.assertNotIn("production_cv_at_most_10_percent", gates)


if __name__ == "__main__":
    unittest.main()
