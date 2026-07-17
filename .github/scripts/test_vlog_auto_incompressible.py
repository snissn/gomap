#!/usr/bin/env python3
"""Tests for the paired incompressible value-log performance gate."""

from pathlib import Path
import math
import re
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKER = REPO_ROOT / ".github" / "scripts" / "check_vlog_auto_incompressible.go"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "treedb-tests.yml"


def normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def perf_job_script() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    match = re.search(
        r"      - name: Incompressible auto-vs-off gate\n"
        r"        run: \|\n(?P<body>.*?)"
        r"(?=^  snapshot-iterator-perf:)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError("missing incompressible auto-vs-off workflow step")
    return match.group("body")


class VLogAutoIncompressibleCheckerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._build_dir = tempfile.TemporaryDirectory()
        cls.checker_bin = Path(cls._build_dir.name) / "check-vlog-auto-incompressible"
        subprocess.run(
            ["go", "build", "-o", str(cls.checker_bin), str(CHECKER)],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._build_dir.cleanup()

    def run_checker(
        self,
        throughput_ratios: list[float],
        size_ratios: list[float] | None = None,
        *,
        omit_last_auto: bool = False,
        off_ops_per_sec: float = 1_000_000,
        corrupt_last_auto: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if size_ratios is None:
            size_ratios = [0.996] * len(throughput_ratios)
        self.assertEqual(len(throughput_ratios), len(size_ratios))

        with tempfile.TemporaryDirectory() as temp_dir:
            args = [str(self.checker_bin)]
            for index, (throughput_ratio, size_ratio) in enumerate(
                zip(throughput_ratios, size_ratios, strict=True), start=1
            ):
                off_log = Path(temp_dir) / f"off-{index}.txt"
                auto_log = Path(temp_dir) / f"auto-{index}.txt"
                off_log.write_text(
                    f"Batch Write / fixture = {off_ops_per_sec:.3f}\n"
                    "maindb/value_vlog: fixture value=100 MB\n",
                    encoding="utf-8",
                )
                if corrupt_last_auto and index == len(throughput_ratios):
                    auto_log.write_text("missing benchmark rows\n", encoding="utf-8")
                else:
                    auto_log.write_text(
                        f"Batch Write / fixture = {throughput_ratio * off_ops_per_sec:.3f}\n"
                        f"maindb/value_vlog: fixture value={size_ratio * 100:.3f} MB\n",
                        encoding="utf-8",
                    )
                args.extend(["-off-log", str(off_log)])
                if not (omit_last_auto and index == len(throughput_ratios)):
                    args.extend(["-auto-log", str(auto_log)])
            args.extend(
                [
                    "-min-samples",
                    str(len(throughput_ratios)),
                    "-min-throughput-frac",
                    "1.01",
                    "-max-size-frac",
                    "1.02",
                ]
            )
            return subprocess.run(args, capture_output=True, text=True, check=False)

    def test_one_lucky_sample_cannot_override_mostly_failing_pairs(self) -> None:
        ratios = [0.9955, 0.9988, 0.9896, 0.9593, 0.9807, 0.9763, 0.9890, 0.99, 1.0, 1.0186]
        result = self.run_checker(ratios)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        expected = math.prod(ratios) ** (1 / len(ratios))
        self.assertIn(f"throughput_geomean={expected:.4f}", result.stdout)
        self.assertIn("FAIL aggregate throughput gate", result.stderr)

    def test_complete_paired_aggregate_above_strict_boundary_passes(self) -> None:
        result = self.run_checker([1.02, 1.03, 1.04, 1.05])

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS incompressible auto-vs-off aggregate gate", result.stdout)

    def test_aggregate_equal_to_boundary_fails(self) -> None:
        result = self.run_checker([1.01, 1.01])

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("<= 1.0100", result.stderr)

    def test_any_sample_over_size_bound_fails(self) -> None:
        result = self.run_checker([1.02, 1.03], [0.996, 1.021])

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("FAIL size gate sample 2", result.stderr)

    def test_mismatched_pair_counts_fail_closed(self) -> None:
        result = self.run_checker([1.02, 1.03], omit_last_auto=True)

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("paired sample count mismatch", result.stderr)

    def test_non_positive_metrics_fail_closed(self) -> None:
        result = self.run_checker([1.02, 1.03], off_ops_per_sec=0)

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("invalid off metrics sample 1", result.stderr)

    def test_missing_benchmark_rows_fail_closed(self) -> None:
        result = self.run_checker([1.02, 1.03], corrupt_last_auto=True)

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("parse auto log sample 2", result.stderr)


class VLogAutoIncompressibleWorkflowContractTest(unittest.TestCase):
    def test_perf_job_runs_aggregate_checker_self_tests(self) -> None:
        workflow = normalized(WORKFLOW.read_text(encoding="utf-8"))

        self.assertIn(
            "name: Aggregate perf gate self-tests env: PYTHONDONTWRITEBYTECODE: \"1\" run: python3 .github/scripts/test_vlog_auto_incompressible.py",
            workflow,
        )

    def test_workflow_uses_full_balanced_sample_set_without_early_success(self) -> None:
        script = normalized(perf_job_script())

        self.assertIn("sample_count=6", script)
        self.assertIn("if ((sample_count % 2 != 0)); then", script)
        self.assertIn("sample_count must be even to balance off/auto row order", script)
        self.assertNotIn("max_attempts", script)
        self.assertNotIn("exit 0", script)
        self.assertIn(
            'if ((sample % 2 == 1)); then echo "sample ${sample} order=off-auto" run_vlog_row treedb_vlog_off "${off_log}" run_vlog_row treedb_vlog_auto "${auto_log}" else echo "sample ${sample} order=auto-off" run_vlog_row treedb_vlog_auto "${auto_log}" run_vlog_row treedb_vlog_off "${off_log}"',
            script,
        )

    def test_workflow_aggregates_every_pair_at_strict_existing_bounds(self) -> None:
        script = normalized(perf_job_script())

        self.assertIn(
            'checker_args+=( -off-log "${off_logs[$sample]}" -auto-log "${auto_logs[$sample]}" )',
            script,
        )
        self.assertIn('-min-samples "${sample_count}"', script)
        self.assertIn("-min-throughput-frac 1.01", script)
        self.assertIn("-max-size-frac 1.02", script)
        self.assertIn("geometric mean of every balanced pair must exceed 1.01x", script)
        self.assertIn("Upload value-log performance evidence", script)
        self.assertIn("if: always()", script)
        self.assertIn('2>&1 | tee "${summary_log}"', script)
        self.assertIn('checker_status=${PIPESTATUS[0]}', script)
        self.assertIn('>> "${GITHUB_STEP_SUMMARY}"', script)
        self.assertIn("vlog_auto_incompressible_summary.txt", script)
        self.assertIn("vlog_auto_incompressible_off_sample*.txt", script)
        self.assertIn("vlog_auto_incompressible_auto_sample*.txt", script)


if __name__ == "__main__":
    unittest.main()
