#!/usr/bin/env python3
"""Dry-run contract tests for treedb_rabitq_1bit_profile_gate.sh."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("treedb_rabitq_1bit_profile_gate.sh")
ROOT = SCRIPT.parents[1]
TARGET_ROW = "rabitq_collection_quantized_only_c1"


class RabitQProfileGateScriptTest(unittest.TestCase):
    def run_dry_run(self, *, profile_scope: str) -> Path:
        tmpdir = tempfile.TemporaryDirectory(prefix="gomap_rabitq_profile_gate_test_")
        self.addCleanup(tmpdir.cleanup)
        run_dir = Path(tmpdir.name)
        env = os.environ.copy()
        env.update(
            {
                "RUN_DIR": str(run_dir),
                "DRY_RUN": "true",
                "PROFILE_SCOPE": profile_scope,
                "SHAPE": "10k_x_1536",
                "ROWS": TARGET_ROW,
                "PROFILE_ROWS": TARGET_ROW,
                "TIMING_COUNT": "1",
                "PROFILE_COUNT": "1",
                "BENCHTIME": "7x",
                "GOMAXPROCS": "2",
                "GOWORK": "off",
                "BENCHMARK_LOCK": "/tmp/gomap_2538_benchmark.lock",
            }
        )
        result = subprocess.run(
            [str(SCRIPT)],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            self.fail(f"dry run failed with {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
        return run_dir

    def test_search_loop_dry_run_uses_hot_profile_env_and_no_go_test_profile_flags(self) -> None:
        run_dir = self.run_dry_run(profile_scope="search_loop")
        row_dir = run_dir / TARGET_ROW

        command = (row_dir / "command_profile.txt").read_text(encoding="utf-8")
        self.assertIn("TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH=", command)
        self.assertIn("TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_PROFILE_PATH=", command)
        self.assertIn("TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_ALLOCS_BASE_PROFILE_PATH=", command)
        self.assertIn("TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_MEM_PROFILE_RATE=", command)
        self.assertNotIn("-cpuprofile", command)
        self.assertNotIn("-memprofile", command)
        self.assertIn("-o", command)

        row_env = (row_dir / "row.env").read_text(encoding="utf-8")
        self.assertIn("profile_scope=search_loop", row_env)
        self.assertIn("shape=10k_x_1536", row_env)
        self.assertIn("fixture_rows=10000", row_env)
        self.assertIn("fixture_dims=1536", row_env)

        row_readme = (row_dir / "README.md").read_text(encoding="utf-8")
        self.assertIn("PROFILE_SCOPE=search_loop", row_readme)
        self.assertIn("start after fixture", row_readme)
        self.assertIn("allocs_diff_raw.pprof", row_readme)
        self.assertIn("pprof-writer noise", row_readme)
        self.assertIn("HOT_MEM_PROFILE_RATE", row_readme)
        self.assertIn("scales sampled allocations", row_readme)

        context = (run_dir / "context.txt").read_text(encoding="utf-8")
        self.assertIn("profile_scope: search_loop", context)
        self.assertIn("alloc_profile_ignore:", context)
        self.assertIn("benchmark_lock: /tmp/gomap_2538_benchmark.lock", context)

        summary_header = (run_dir / "summary.tsv").read_text(encoding="utf-8").splitlines()[0]
        self.assertIn("profile_scope", summary_header.split("\t"))

    def test_go_test_scope_dry_run_keeps_legacy_profile_flags(self) -> None:
        run_dir = self.run_dry_run(profile_scope="go_test")
        row_dir = run_dir / TARGET_ROW

        command = (row_dir / "command_profile.txt").read_text(encoding="utf-8")
        self.assertIn("-cpuprofile", command)
        self.assertIn("-memprofile", command)
        self.assertIn("-blockprofile", command)
        self.assertIn("-mutexprofile", command)
        self.assertNotIn("TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH=", command)

        row_env = (row_dir / "row.env").read_text(encoding="utf-8")
        self.assertIn("profile_scope=go_test", row_env)

    def test_search_loop_rejects_scalar_profile_rows_before_work(self) -> None:
        with tempfile.TemporaryDirectory(prefix="gomap_rabitq_profile_gate_test_") as tmp:
            env = os.environ.copy()
            env.update(
                {
                    "RUN_DIR": tmp,
                    "DRY_RUN": "true",
                    "PROFILE_SCOPE": "search_loop",
                    "ROWS": "scalar_collection_quantized_only_c1",
                    "PROFILE_ROWS": "scalar_collection_quantized_only_c1",
                    "GOWORK": "off",
                }
            )
            result = subprocess.run(
                [str(SCRIPT)],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("PROFILE_SCOPE=search_loop only supports rabitq_1bit", result.stderr)


if __name__ == "__main__":
    unittest.main()
