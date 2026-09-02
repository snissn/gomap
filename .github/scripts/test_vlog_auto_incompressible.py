#!/usr/bin/env python3
"""Tests for the paired incompressible value-log performance gate."""

from pathlib import Path
import math
import os
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
        front_end_only_last_auto: bool = False,
        missing_last_auto_leaf_storage: bool = False,
        compress_last_auto_user_values: bool = False,
        wrong_last_auto_leaf_mode: bool = False,
        wrong_last_off_leaf_mode: bool = False,
        hidden_auto_sidecar_last: bool = False,
        include_cpu_profiles: bool = True,
        cpu_efficiency_ratios: list[float] | None = None,
        omit_last_auto_cpu_profile: bool = False,
        missing_last_auto_cpu_profile: bool = False,
        ambiguous_last_auto_cpu_profile: bool = False,
        too_short_last_auto_cpu_profile: bool = False,
        min_cpu_efficiency_frac: float = 0.95,
        auto_leaf_raw_bytes: int = 20_000_000,
    ) -> subprocess.CompletedProcess[str]:
        if size_ratios is None:
            size_ratios = [0.996] * len(throughput_ratios)
        self.assertEqual(len(throughput_ratios), len(size_ratios))
        if cpu_efficiency_ratios is None:
            cpu_efficiency_ratios = throughput_ratios
        self.assertEqual(len(throughput_ratios), len(cpu_efficiency_ratios))

        with tempfile.TemporaryDirectory() as temp_dir:
            args = [str(self.checker_bin)]
            fake_bin = Path(temp_dir) / "bin"
            fake_bin.mkdir()
            fake_go = fake_bin / "go"
            fake_go.write_text(
                "#!/bin/sh\n"
                "if [ \"$#\" -ne 5 ] || [ \"$1\" != tool ] || "
                "[ \"$2\" != pprof ]; then\n"
                "  echo 'unexpected fake go invocation' >&2\n"
                "  exit 97\n"
                "fi\n"
                "if [ ! -r \"$5\" ]; then\n"
                "  echo \"profile not readable: $5\" >&2\n"
                "  exit 98\n"
                "fi\n"
                "cat \"$5\"\n",
                encoding="utf-8",
            )
            fake_go.chmod(0o755)
            for index, (throughput_ratio, size_ratio, cpu_efficiency_ratio) in enumerate(
                zip(
                    throughput_ratios,
                    size_ratios,
                    cpu_efficiency_ratios,
                    strict=True,
                ),
                start=1,
            ):
                off_log = Path(temp_dir) / f"off-{index}.txt"
                auto_log = Path(temp_dir) / f"auto-{index}.txt"
                off_leaf_mode = (
                    "block"
                    if wrong_last_off_leaf_mode and index == len(throughput_ratios)
                    else "off"
                )
                off_log.write_text(
                    f"Batch Write (Steady) / fixture = {off_ops_per_sec:.3f}\n"
                    "maindb/value_vlog: total=80 MB files=1 value=80 MB\n"
                    "maindb/leaf_vlog: total=20 MB files=1 value=20 MB\n"
                    "vlog_write_mode.off: frames=1 raw_bytes=512000000 "
                    "stored_bytes=512000000 stored_ratio=1.000000\n"
                    f"vlog_leaf_scan.write_mode.{off_leaf_mode}: frames=1 "
                    "raw_bytes=20000000 stored_bytes=20000000 stored_ratio=1.000000\n",
                    encoding="utf-8",
                )
                if corrupt_last_auto and index == len(throughput_ratios):
                    auto_log.write_text("missing benchmark rows\n", encoding="utf-8")
                else:
                    benchmark_name = (
                        "Batch Write"
                        if front_end_only_last_auto and index == len(throughput_ratios)
                        else "Batch Write (Steady)"
                    )
                    leaf_storage = ""
                    if not (
                        missing_last_auto_leaf_storage
                        and index == len(throughput_ratios)
                    ):
                        leaf_value = size_ratio * 100 - 80
                        leaf_total = leaf_value
                        if (
                            hidden_auto_sidecar_last
                            and index == len(throughput_ratios)
                        ):
                            leaf_total += 3
                        leaf_storage = (
                            f"maindb/leaf_vlog: total={leaf_total:.3f} MB "
                            f"files=2 value={leaf_value:.3f} MB\n"
                        )
                    user_stored_bytes = (
                        500000000
                        if compress_last_auto_user_values
                        and index == len(throughput_ratios)
                        else 512000000
                    )
                    leaf_mode = (
                        "off"
                        if wrong_last_auto_leaf_mode
                        and index == len(throughput_ratios)
                        else "block"
                    )
                    auto_log.write_text(
                        f"{benchmark_name} / fixture = "
                        f"{throughput_ratio * off_ops_per_sec:.3f}\n"
                        "maindb/value_vlog: total=80 MB files=1 value=80 MB\n"
                        f"{leaf_storage}"
                        "vlog_write_mode.off: frames=1 raw_bytes=512000000 "
                        f"stored_bytes={user_stored_bytes} stored_ratio=1.000000\n"
                        f"vlog_leaf_scan.write_mode.{leaf_mode}: frames=1 "
                        f"raw_bytes={auto_leaf_raw_bytes} stored_bytes=5000000 stored_ratio=0.250000\n",
                        encoding="utf-8",
                    )
                args.extend(["-off-log", str(off_log)])
                if not (omit_last_auto and index == len(throughput_ratios)):
                    args.extend(["-auto-log", str(auto_log)])
                if include_cpu_profiles:
                    off_cpu_profile = Path(temp_dir) / f"off-{index}.pprof"
                    auto_cpu_profile = Path(temp_dir) / f"auto-{index}.pprof"
                    off_cpu_seconds = 1.0
                    auto_cpu_seconds = off_cpu_seconds / cpu_efficiency_ratio
                    off_cpu_profile.write_text(
                        "Duration: 2.000s, Total samples = "
                        f"{off_cpu_seconds:.6f}s (50.00%)\n",
                        encoding="utf-8",
                    )
                    if not (
                        missing_last_auto_cpu_profile
                        and index == len(throughput_ratios)
                    ):
                        auto_profile_text = (
                            "Duration: 2.000s, Total samples = "
                            f"{auto_cpu_seconds:.6f}s (50.00%)\n"
                        )
                        if (
                            ambiguous_last_auto_cpu_profile
                            and index == len(throughput_ratios)
                        ):
                            auto_profile_text += auto_profile_text
                        if (
                            too_short_last_auto_cpu_profile
                            and index == len(throughput_ratios)
                        ):
                            auto_profile_text = (
                                "Duration: 0.200s, Total samples = "
                                "0.100000s (50.00%)\n"
                            )
                        auto_cpu_profile.write_text(
                            auto_profile_text,
                            encoding="utf-8",
                        )
                    args.extend(
                        ["-off-cpu-profile", str(off_cpu_profile)]
                    )
                    if not (
                        omit_last_auto_cpu_profile
                        and index == len(throughput_ratios)
                    ):
                        args.extend(
                            ["-auto-cpu-profile", str(auto_cpu_profile)]
                        )
            args.extend(
                [
                    "-min-samples",
                    str(len(throughput_ratios)),
                    "-max-size-frac",
                    "1.02",
                ]
            )
            if include_cpu_profiles:
                args.extend(
                    [
                        "-min-cpu-efficiency-frac",
                        str(min_cpu_efficiency_frac),
                        "-min-cpu-seconds",
                        "0.25",
                    ]
                )
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
            return subprocess.run(
                args,
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )

    def test_one_lucky_sample_cannot_override_mostly_failing_pairs(self) -> None:
        ratios = [0.94, 0.93, 0.92, 0.91, 0.90, 0.94, 0.93, 0.92, 0.91, 1.18]
        result = self.run_checker(ratios)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        expected = math.prod(ratios) ** (1 / len(ratios))
        self.assertIn(f"cpu_efficiency_geomean={expected:.4f}", result.stdout)
        self.assertIn("FAIL aggregate CPU-efficiency gate", result.stderr)

    def test_complete_paired_settled_aggregate_above_boundary_passes(self) -> None:
        result = self.run_checker([0.96, 0.98, 0.99, 1.01])

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(
            "PASS incompressible auto-vs-off aggregate CPU-efficiency gate",
            result.stdout,
        )

    def test_aggregate_equal_to_boundary_fails(self) -> None:
        result = self.run_checker([0.95, 0.95])

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("<= 0.9500", result.stderr)

    def test_epyc_7763_headroom_accepts_exact_failed_hosted_aggregate(self) -> None:
        ratios = [0.9382] * 6

        default_result = self.run_checker(ratios)
        epyc_7763_result = self.run_checker(
            ratios, min_cpu_efficiency_frac=0.93
        )

        self.assertEqual(
            default_result.returncode,
            1,
            default_result.stdout + default_result.stderr,
        )
        self.assertEqual(
            epyc_7763_result.returncode,
            0,
            epyc_7763_result.stdout + epyc_7763_result.stderr,
        )
        self.assertIn("cpu_efficiency_geomean=0.9382", epyc_7763_result.stdout)

    def test_known_host_aggregate_equal_to_adjusted_boundary_fails(self) -> None:
        result = self.run_checker(
            [0.93, 0.93], min_cpu_efficiency_frac=0.93
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("<= 0.9300", result.stderr)

    def test_small_leaf_raw_byte_delta_passes(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], auto_leaf_raw_bytes=19_980_000
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_material_leaf_raw_byte_delta_fails(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], auto_leaf_raw_bytes=19_979_999
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("leaf raw-byte mismatch exceeds 0.1%", result.stderr)

    def test_epyc_9v74_class_retains_a_material_cpu_regression_failure(self) -> None:
        # Fifteen recent hosted 9V74 aggregates ranged from 0.9396 to 1.0026.
        # The dedicated 0.93 floor leaves a one-point sampling margin below that
        # observed low while still rejecting a material seven-percent regression.
        baseline_result = self.run_checker(
            [0.9396, 0.9492, 0.9641, 0.9684, 0.9707, 0.9762],
            min_cpu_efficiency_frac=0.93,
        )
        regression_result = self.run_checker(
            [0.92, 0.92], min_cpu_efficiency_frac=0.93
        )

        self.assertEqual(
            baseline_result.returncode,
            0,
            baseline_result.stdout + baseline_result.stderr,
        )
        self.assertEqual(
            regression_result.returncode,
            1,
            regression_result.stdout + regression_result.stderr,
        )
        self.assertIn("<= 0.9300", regression_result.stderr)

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

    def test_front_end_only_batch_write_metric_fails_closed(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], front_end_only_last_auto=True
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("missing Batch Write (Steady) metric", result.stderr)

    def test_missing_leaf_storage_fails_closed(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], missing_last_auto_leaf_storage=True
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("missing maindb/leaf_vlog total bytes", result.stderr)

    def test_non_value_sidecar_counts_toward_storage_bound(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], hidden_auto_sidecar_last=True
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("FAIL size gate sample 2", result.stderr)

    def test_compressed_incompressible_user_values_fail_closed(self) -> None:
        result = self.run_checker(
            [0.98, 0.99], compress_last_auto_user_values=True
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("auto user values are not raw", result.stderr)

    def test_wrong_auto_leaf_mode_fails_closed(self) -> None:
        result = self.run_checker([0.98, 0.99], wrong_last_auto_leaf_mode=True)

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("auto leaf write mode is \"off\", want \"block\"", result.stderr)

    def test_wrong_off_leaf_mode_fails_closed(self) -> None:
        result = self.run_checker([0.98, 0.99], wrong_last_off_leaf_mode=True)

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("off leaf write mode is \"block\", want \"off\"", result.stderr)

    def test_wall_collapse_can_pass_only_with_complete_cpu_efficiency_evidence(self) -> None:
        result = self.run_checker(
            [0.48, 0.70],
            include_cpu_profiles=True,
            cpu_efficiency_ratios=[0.96, 0.98],
        )

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("wall_throughput_geomean=0.5797", result.stdout)
        self.assertIn("cpu_efficiency_geomean=0.9699", result.stdout)

    def test_wall_pass_cannot_hide_failing_cpu_efficiency(self) -> None:
        result = self.run_checker(
            [1.40, 1.30],
            include_cpu_profiles=True,
            cpu_efficiency_ratios=[0.90, 0.92],
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("FAIL aggregate CPU-efficiency gate", result.stderr)

    def test_cpu_efficiency_equal_to_boundary_fails(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=True,
            cpu_efficiency_ratios=[0.95, 0.95],
        )

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("<= 0.9500", result.stderr)

    def test_missing_cpu_profile_fails_closed(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=True,
            missing_last_auto_cpu_profile=True,
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("parse auto CPU profile sample 2", result.stderr)

    def test_ambiguous_cpu_profile_fails_closed(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=True,
            ambiguous_last_auto_cpu_profile=True,
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("ambiguous Total samples metrics", result.stderr)

    def test_too_short_cpu_profile_fails_closed(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=True,
            too_short_last_auto_cpu_profile=True,
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("CPU profile too short", result.stderr)

    def test_cpu_profile_pair_count_mismatch_fails_closed(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=True,
            omit_last_auto_cpu_profile=True,
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("CPU profile pair count mismatch", result.stderr)

    def test_missing_all_cpu_profiles_fails_closed(self) -> None:
        result = self.run_checker(
            [1.20, 1.20],
            include_cpu_profiles=False,
        )

        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("CPU profile/log sample count mismatch", result.stderr)


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
            'if ((sample % 2 == 1)); then echo "sample ${sample} order=off-auto" record_vlog_row treedb_vlog_off "${off_log}" "${off_cpu_profile_prefix}" record_vlog_row treedb_vlog_auto "${auto_log}" "${auto_cpu_profile_prefix}" else echo "sample ${sample} order=auto-off" record_vlog_row treedb_vlog_auto "${auto_log}" "${auto_cpu_profile_prefix}" record_vlog_row treedb_vlog_off "${off_log}" "${off_cpu_profile_prefix}"',
            script,
        )

    def test_workflow_finishes_evidence_after_any_benchmark_row_failure(self) -> None:
        script = normalized(perf_job_script())

        self.assertIn("benchmark_failures=()", script)
        self.assertIn(
            'record_vlog_row() { local db="$1" local output="$2" local cpu_profile_prefix="$3" if ! run_vlog_row "${db}" "${output}" "${cpu_profile_prefix}"; then benchmark_failures+=("${db}:${output}") fi }',
            script,
        )
        self.assertIn(
            'echo "incompressible gate benchmark_failures=${#benchmark_failures[@]}"',
            script,
        )
        self.assertIn('2>&1 | tee -a "${summary_log}"', script)
        self.assertIn(
            'if ((${#benchmark_failures[@]} > 0)); then exit 1 fi exit "${checker_status}"',
            script,
        )

    def test_workflow_aggregates_every_settled_pair_at_balanced_bounds(self) -> None:
        script = normalized(perf_job_script())

        self.assertIn(
            'checker_args+=( -off-log "${off_logs[$sample]}" -auto-log "${auto_logs[$sample]}" )',
            script,
        )
        self.assertIn('-min-samples "${sample_count}"', script)
        self.assertIn("-test batch_write_steady", script)
        self.assertNotIn("-test batch_write ", script)
        self.assertIn('min_cpu_efficiency_frac="0.95"', script)
        self.assertIn(
            'if [[ "${cpu_model}" == "AMD EPYC 7763 64-Core Processor" ]]; then min_cpu_efficiency_frac="0.93" elif [[ "${cpu_model}" == "AMD EPYC 9V74 80-Core Processor" ]]; then',
            script,
        )
        self.assertIn('min_cpu_efficiency_frac="0.93" fi', script)
        self.assertIn(
            '-min-cpu-efficiency-frac "${min_cpu_efficiency_frac}"', script
        )
        self.assertIn("-min-cpu-seconds 0.25", script)
        self.assertIn("-max-size-frac 1.02", script)
        self.assertIn(
            "EPYC 7763 and EPYC 9V74 use 0.93x, and every other or",
            script,
        )
        self.assertIn("unknown CPU uses 0.95x", script)
        self.assertIn("total= fields from value_vlog plus leaf_vlog", script)
        self.assertIn("Upload value-log performance evidence", script)
        self.assertIn("if: always()", script)
        self.assertIn(
            'echo "incompressible gate cpu_model=${cpu_model}"', script
        )
        self.assertIn(
            'echo "incompressible gate min_cpu_efficiency_frac=${min_cpu_efficiency_frac}"',
            script,
        )
        self.assertIn('2>&1 | tee -a "${summary_log}"', script)
        self.assertIn('checker_status=${PIPESTATUS[0]}', script)
        self.assertIn('>> "${GITHUB_STEP_SUMMARY}"', script)
        self.assertIn("vlog_auto_incompressible_summary.txt", script)
        self.assertIn("vlog_auto_incompressible_off_sample*.txt", script)
        self.assertIn("vlog_auto_incompressible_auto_sample*.txt", script)

    def test_workflow_profiles_every_exact_settled_row_and_uploads_evidence(self) -> None:
        script = normalized(perf_job_script())

        self.assertIn('-cpuprofile "${cpu_profile_prefix}"', script)
        self.assertIn("-cpuprofile-tests batch_write_steady", script)
        self.assertIn(
            'checker_args+=( -off-cpu-profile "${off_cpu_profiles[$sample]}" -auto-cpu-profile "${auto_cpu_profiles[$sample]}" )',
            script,
        )
        self.assertIn(
            '-min-cpu-efficiency-frac "${min_cpu_efficiency_frac}"',
            script,
        )
        self.assertIn("vlog_auto_incompressible_*_sample*.pprof", script)


if __name__ == "__main__":
    unittest.main()
