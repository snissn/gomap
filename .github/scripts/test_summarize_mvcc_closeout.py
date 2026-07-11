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

    @staticmethod
    def resource_record(sample: int, group: str) -> str:
        benchtime = "750ms" if group == "regular" else "1x"
        return (
            f"--- sample={sample} group={group} benchtime={benchtime} ---\n"
            'Command being timed: "benchmark command"\n'
            "User time (seconds): 1.5\n"
            "System time (seconds): 0.5\n"
            "Percent of CPU this job got: 75%\n"
            "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:02.00\n"
            "Average shared text size (kbytes): 0\n"
            "Average unshared data size (kbytes): 0\n"
            "Average stack size (kbytes): 0\n"
            "Average total size (kbytes): 0\n"
            "Maximum resident set size (kbytes): 123\n"
            "Average resident set size (kbytes): 0\n"
            "Major (requiring I/O) page faults: 0\n"
            "Minor (reclaiming a frame) page faults: 1\n"
            "Voluntary context switches: 2\n"
            "Involuntary context switches: 3\n"
            "Swaps: 0\n"
            "File system inputs: 0\n"
            "File system outputs: 4\n"
            "Socket messages sent: 0\n"
            "Socket messages received: 0\n"
            "Signals delivered: 0\n"
            "Page size (bytes): 4096\n"
            "Exit status: 0\n"
        )

    def write_resources(self, samples: int) -> Path:
        path = self.root / "resources.txt"
        path.write_text(
            "".join(
                self.resource_record(sample, group)
                for sample in range(1, samples + 1)
                for group in ("regular", "prune")
            ),
            encoding="utf-8",
        )
        return path

    def test_success_and_absent_rendering(self) -> None:
        medians = closeout.parse_benchmarks(self.write_bench(), 2)
        resources = closeout.parse_resources(self.write_resources(2), 2)
        durable_commit = medians[
            "BenchmarkDgraphMVCCCloseout/CommitAt/durable_sync/batch=1"
        ]
        self.assertEqual(
            durable_commit,
            {
                "B/op": 32.0,
                "allocs/op": 1.0,
                "durable_footprint_bytes/op": 20.0,
                "mutations/s": 10.0,
                "ns/op": 100.0,
                "storage_bytes/op": 20.0,
            },
        )
        self.assertEqual(
            resources,
            {
                "invocations": 4.0,
                "max_rss_kib": 123.0,
                "total_user_seconds": 6.0,
                "total_system_seconds": 2.0,
            },
        )
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

    def test_duplicate_required_metric_rejected(self) -> None:
        path = self.write_bench()
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "32 B/op", "32 B/op 64 B/op", 1
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "duplicate metric unit B/op"):
            closeout.parse_benchmarks(path, 2)

    def test_wrong_sample_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            closeout.parse_benchmarks(self.write_bench(runs=1), 2)

    def test_wrong_resource_count_rejected(self) -> None:
        with self.assertRaises(ValueError):
            closeout.parse_resources(self.write_resources(1), 2)

    def test_missing_resource_field_rejected(self) -> None:
        path = self.write_resources(2)
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "Page size (bytes): 4096\n", "", 1
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ValueError):
            closeout.parse_resources(path, 2)

    def test_extra_resource_field_rejected(self) -> None:
        path = self.write_resources(2)
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "Exit status: 0\n", "Exit status: 0\nUnexpected field: 1\n", 1
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ValueError):
            closeout.parse_resources(path, 2)

    def test_malformed_resource_field_rejected(self) -> None:
        path = self.write_resources(2)
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "System time (seconds): 0.5",
                "System time (seconds): malformed",
                1,
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ValueError):
            closeout.parse_resources(path, 2)

    def test_interleaved_resource_record_rejected(self) -> None:
        path = self.write_resources(2)
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "Maximum resident set size (kbytes): 123",
                "--- sample=1 group=prune benchtime=1x ---\n"
                "Maximum resident set size (kbytes): 123",
                1,
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ValueError):
            closeout.parse_resources(path, 2)

    def test_nonzero_resource_exit_rejected(self) -> None:
        path = self.write_resources(2)
        path.write_text(
            path.read_text(encoding="utf-8").replace(
                "Exit status: 0", "Exit status: 1", 1
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "exit status 1"):
            closeout.parse_resources(path, 2)


if __name__ == "__main__":
    unittest.main()
