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


if __name__ == "__main__":
    unittest.main()
