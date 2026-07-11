#!/usr/bin/env python3
"""Unit tests for the exact MVCC closeout evidence contract."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("summarize_mvcc_closeout.py")
SPEC = importlib.util.spec_from_file_location("summarize_mvcc_closeout", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
closeout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(closeout)


def benchmark_line(name: str, omit: str | None = None) -> str:
    metrics = {"B/op": 32, "allocs/op": 1}
    if "/CommitAt/" in name:
        metrics.update({"mutations/s": 10, "storage_bytes/op": 20})
    elif "/GetAt/" in name:
        metrics["lookups/s"] = 10
    elif "/AllVersions/" in name:
        metrics["versions/s"] = 10
    else:
        metrics.update(
            {
                "pruned_versions/s": 10,
                "storage_bytes/op": 20,
                "delete_write_amplification": 1.25,
            }
        )
    if "/durable_sync/" in name and ("/CommitAt/" in name or "/Prune/" in name):
        metrics["durable_footprint_bytes/op"] = 20
    metrics.pop(omit, None)
    tail = " ".join(f"{value} {unit}" for unit, value in metrics.items())
    return f"{name}-1 1 100 ns/op {tail}"


class CloseoutSummaryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_bench(self, names=None, runs: int = 2, omit: str | None = None) -> Path:
        names = sorted(closeout.EXPECTED_BENCHMARKS if names is None else names)
        path = self.root / "bench.txt"
        path.write_text(
            "\n".join(benchmark_line(name, omit) for _ in range(runs) for name in names)
            + "\n",
            encoding="utf-8",
        )
        return path

    def write_resources(self, invocations: int) -> Path:
        path = self.root / "resources.txt"
        row = (
            "Maximum resident set size (kbytes): 123\n"
            "User time (seconds): 1.5\n"
            "System time (seconds): 0.5\n"
        )
        path.write_text(row * invocations, encoding="utf-8")
        return path

    def test_success_and_absent_rendering(self) -> None:
        medians = closeout.parse_benchmarks(self.write_bench(), 2)
        resources = closeout.parse_resources(self.write_resources(4), 2)
        markdown = closeout.render_markdown("abc", 2, medians, resources)
        relaxed_get = next(
            line for line in markdown.splitlines() if "/GetAt/wal_on_relaxed/" in line
        )
        self.assertIn(" | - | - | - |", relaxed_get)

    def test_missing_required_row_rejected(self) -> None:
        names = set(closeout.EXPECTED_BENCHMARKS)
        names.remove(next(iter(names)))
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(names), 2)

    def test_missing_durability_class_rejected(self) -> None:
        names = {name for name in closeout.EXPECTED_BENCHMARKS if "/wal_on_relaxed/" not in name}
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(names), 2)

    def test_missing_group_rejected(self) -> None:
        names = {name for name in closeout.EXPECTED_BENCHMARKS if "/Prune/" not in name}
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(names), 2)

    def test_missing_required_metric_rejected(self) -> None:
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(omit="allocs/op"), 2)

    def test_wrong_sample_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(runs=1), 2)

    def test_wrong_resource_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            closeout.parse_resources(self.write_resources(3), 2)


if __name__ == "__main__":
    unittest.main()
