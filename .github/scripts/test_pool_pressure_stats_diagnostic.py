#!/usr/bin/env python3
"""Failure controls for the scoped public Stats diagnostic (no benchmarks)."""

import copy
import json
import math
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import pool_pressure_stats_diagnostic as diagnostic


class PublicStatsDiagnosticTests(unittest.TestCase):
    def byte_result(self, baseline=100000, growth=1, **kwargs):
        rows = self.rows()
        for row in rows:
            row["metrics"]["B/op"] = baseline + (growth if row["revision"] == "candidate" else 0)
        raw = diagnostic.evaluate(rows)
        original = copy.deepcopy(raw)
        result = diagnostic.qualify_measurement(raw, quiet=True, **kwargs)
        self.assertEqual(raw, original)
        return result

    def test_v1_default_preserves_strict_raw_byte_failure(self):
        for options in ({}, {"acceptance_contract": "v1"}):
            result = self.byte_result(**options)
            self.assertEqual(result["acceptance_contract"], "v1")
            self.assertFalse(result["accepted"])
            self.assertFalse(result["measurement_pass"])
            self.assertEqual(result["acceptance_gates"], result["gates"])
            self.assertEqual(result["bytes_disposition"], "FAIL")

    def test_v2_byte_boundaries_preserve_raw_results(self):
        cases = [(100000, 1, True), (100000, 8, True),
                 (0, 0, True), (0, math.nextafter(0, math.inf), False),
                 (100000, 0, True), (100000, -1, True),
                 (10000, 1, True)]
        # Use nextafter on the candidate value, so addition cannot round the
        # immediately-above control back to the exact boundary.
        cases += [(100000, math.nextafter(100008, math.inf) - 100000, False),
                  (10000, math.nextafter(10001, math.inf) - 10000, False)]
        for baseline, growth, accepted in cases:
            with self.subTest(baseline=baseline, growth=growth):
                result = self.byte_result(baseline, growth, acceptance_contract="v2")
                self.assertEqual(result["accepted"], accepted)
                self.assertEqual(result["gates"]["bytes"], growth <= 0)
                self.assertEqual(result["measurement_pass"], growth <= 0)
                self.assertEqual(result["byte_budget"], {
                    "absolute_limit_B_per_op": 8.0, "relative_limit_percent": 0.01,
                    "allowance_B_per_op": min(8, baseline * 0.0001), "growth_B_per_op": growth})
                self.assertEqual(result["bytes_disposition"], "PASS" if growth <= 0 else
                                 "WITHIN_ACCEPTED_BYTE_BUDGET" if accepted else "FAIL")

    def test_v2_preserves_strict_timing_allocations_and_quietness(self):
        for metric, value, gate in (("ns/op", 106, "timing"), ("allocs/op", 4, "allocations")):
            rows = self.rows()
            for row in rows:
                if row["revision"] == "candidate":
                    row["metrics"][metric] = value
            result = diagnostic.qualify_measurement(diagnostic.evaluate(rows), quiet=True,
                                                    acceptance_contract="v2")
            self.assertFalse(result["acceptance_gates"][gate])
            self.assertFalse(result["accepted"])
        self.assertFalse(diagnostic.qualify_measurement(diagnostic.evaluate(self.rows()),
                         quiet=False, acceptance_contract="v2")["accepted"])

    def test_unknown_contract_fails_before_output_or_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "absent"
            with mock.patch.object(diagnostic.subprocess, "Popen") as process:
                with self.assertRaisesRegex(ValueError, "unknown acceptance contract"):
                    diagnostic.run(ROOT, output, "1" * 40, "2" * 40, acceptance_contract="unknown")
                process.assert_not_called()
            self.assertFalse(output.exists())
        with self.assertRaisesRegex(ValueError, "unknown acceptance contract"):
            diagnostic.qualify_measurement({}, quiet=True, acceptance_contract="unknown")

    def test_cli_contract_handoff_and_unknown_choice(self):
        argv = ["stats", "--root", "/source", "--output", "/output",
                "--baseline", "1" * 40, "--candidate", "2" * 40]
        for extra, contract in (([], "v1"), (["--acceptance-contract", "v2"], "v2")):
            with mock.patch.object(sys, "argv", argv + extra), mock.patch.object(diagnostic, "run", return_value=0) as run:
                self.assertEqual(diagnostic.main(), 0)
                self.assertEqual(run.call_args.kwargs["acceptance_contract"], contract)
        with mock.patch.object(sys, "argv", argv + ["--acceptance-contract", "unknown"]), \
                mock.patch.object(diagnostic, "run") as run, mock.patch.object(sys, "stderr"):
            with self.assertRaises(SystemExit) as error:
                diagnostic.main()
            self.assertEqual(error.exception.code, 2)
            run.assert_not_called()

    def complete_run(self, *, contract="v2", failure=None, timing=100, allocations=3):
        """Run real orchestration against temporary sources and stubbed commands."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "source"
            for name in (diagnostic.HARNESS, "scripts/pool_pressure_stats_diagnostic.py",
                         "scripts/mvcc_primary_cost_gate.py", "scripts/mvcc_candidate_checkout_guard.sh"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("temporary source fixture: " + name)
            output = Path(directory) / "evidence"
            calls = []

            class Process:
                def __init__(self, argv, *, stdout, **kwargs):
                    calls.append(argv)
                    self.status = 0
                    if argv[:3] == ["git", "worktree", "add"]:
                        Path(argv[-2]).mkdir(parents=True)
                    elif argv[:3] == ["git", "worktree", "remove"]:
                        if failure == "cleanup":
                            self.status = 1
                        else:
                            Path(argv[-1]).rmdir()
                    elif argv[:3] == ["go", "test", "-c"]:
                        Path(argv[argv.index("-o") + 1]).write_bytes(b"same bound binary")
                    elif argv[0] == "taskset":
                        candidate = "candidate" in Path(argv[3]).name
                        measured_count = sum(call[0] == "taskset" for call in calls)
                        iterations = 511 if failure == "iterations" and measured_count == 6 else 512
                        text = (f"{diagnostic.BENCHMARK} {iterations} "
                                f"{timing if candidate else 100} ns/op "
                                f"{100001 if candidate else 100000} B/op "
                                f"{allocations if candidate else 3} allocs/op\nPASS\n")
                        stdout.write(text.encode())
                    elif argv[0] == "bash" and str(argv[-1]) == "2" * 40:
                        # Only the after guard occurs after all six samples.
                        if failure == "source" and sum(call[0] == "taskset" for call in calls) == 6:
                            (root / "scripts/pool_pressure_stats_diagnostic.py").write_text("drift")

                def wait(self, **kwargs):
                    return self.status

            def runtime_binding(binary, command, label, environment):
                if failure == "binary" and label.startswith("after"):
                    binary.write_bytes(b"changed binary")
                identity = diagnostic.maintained.file_identity(binary)
                identity.pop("path")
                binding = {"executable": identity, "linkage": "static", "interpreter": None, "libraries": []}
                if failure == "runtime" and label.startswith("after"):
                    binding["libraries"] = [{"sha256": "drifting runtime"}]
                return binding

            observer = mock.Mock()
            observer.finish.return_value = {"busy": failure == "busy"}
            with mock.patch.object(diagnostic, "__file__", str(root / "scripts/pool_pressure_stats_diagnostic.py")), \
                    mock.patch.object(diagnostic.maintained, "__file__", str(root / "scripts/mvcc_primary_cost_gate.py")), \
                    mock.patch.object(diagnostic.subprocess, "Popen", Process), \
                    mock.patch.object(diagnostic.maintained, "runtime_binding", side_effect=runtime_binding), \
                    mock.patch.object(diagnostic.maintained, "acquire_quiet_window"), \
                    mock.patch.object(diagnostic.maintained, "ProcessObservation", return_value=observer):
                status = diagnostic.run(root, output, "1" * 40, "2" * 40, acceptance_contract=contract)
            summary = json.loads((output / "summary.json").read_text())
            bindings = json.loads((output / "bindings.json").read_text())
            self.assertEqual(summary["retained_samples"], 5 if failure == "iterations" else 6)
            measured = [call for call in calls if call[0] == "taskset"]
            self.assertEqual(len(measured), 6)
            for call in measured:
                self.assertIn("-test.benchtime=512x", call)
            for record in (summary, bindings):
                self.assertEqual(record["fixture_version"], "public-stats-v3-fixed512-close-excluded")
                self.assertEqual(record["iterations_per_sample"], 512)
                self.assertEqual(record["benchtime"], "512x")
            if failure == "iterations":
                receipts = json.loads((output / "commands.json").read_text())
                samples = [r for r in receipts if r["argv"][0] == "taskset"]
                self.assertEqual(len(samples), 6)
                self.assertTrue(all(Path(record["stdout"]["path"]).is_file() for record in samples))
                self.assertIn(" 511 ", Path(samples[-1]["stdout"]["path"]).read_text())
                self.assertTrue(all(row["iterations"] == 512 for row in
                                    json.loads((output / "samples.json").read_text())))
                self.assertEqual(receipts[-1]["label"], "baseline-worktree-remove")
                self.assertEqual(receipts[-1]["exit"], 0)
            self.assertEqual([json.loads((output / "runtime-inputs-before.json").read_text())[revision]["executable"]
                              for revision in ("baseline", "candidate")],
                             [dict(sha256=bindings["binaries"]["baseline"]["sha256"],
                                   bytes=bindings["binaries"]["baseline"]["bytes"])] * 2)
            return status, summary, bindings, (output / "summary.md").read_text()

    def test_complete_run_records_contract_budget_and_raw_failure(self):
        status, summary, bindings, markdown = self.complete_run()
        self.assertEqual(status, 0)
        self.assertEqual(summary["verdict"], "PASS")
        self.assertTrue(summary["accepted"])
        self.assertTrue(summary["product_guard_available"])
        self.assertFalse(summary["measurement_pass"])
        self.assertFalse(summary["gates"]["bytes"])
        self.assertEqual(summary["acceptance_contract"], "v2")
        self.assertEqual(bindings["acceptance_contract"], "v2")
        self.assertEqual(bindings["byte_budget"], summary["byte_budget"])
        self.assertEqual(bindings["byte_limits"], {"absolute_limit_B_per_op": 8.0, "relative_limit_percent": 0.01})
        self.assertEqual(summary["bytes_disposition"], "WITHIN_ACCEPTED_BYTE_BUDGET")
        self.assertIn("v2", markdown)
        status, summary, _, _ = self.complete_run(contract="v1")
        self.assertEqual(status, 1)
        self.assertEqual(summary["verdict"], "FAIL")
        self.assertFalse(summary["accepted"])

    def test_complete_run_wrong_count_retains_raw_failure_and_cleanup(self):
        status, summary, _, _ = self.complete_run(failure="iterations")
        self.assertEqual(status, 1)
        self.assertEqual(summary["verdict"], "NULL")
        self.assertFalse(summary["accepted"])
        self.assertFalse(summary["product_guard_available"])
        self.assertIn("exactly 512 iterations", summary["error"])

    def test_complete_run_drift_cleanup_and_busy_refuse_acceptance(self):
        for failure in ("runtime", "source", "binary", "cleanup", "busy"):
            with self.subTest(failure=failure):
                status, summary, _, _ = self.complete_run(failure=failure)
                self.assertEqual(status, 1)
                self.assertEqual(summary["verdict"], "NULL")
                self.assertFalse(summary["accepted"])
                self.assertFalse(summary["product_guard_available"])
                if failure != "busy":
                    self.assertIn("cleanup failed" if failure == "cleanup" else "drift", summary["error"])

    def test_matching_binaries_never_bypass_timing_or_allocation_failure(self):
        for options, gate in (({"timing": 106}, "timing"), ({"allocations": 4}, "allocations")):
            with self.subTest(gate=gate):
                status, summary, _, _ = self.complete_run(**options)
                self.assertEqual(status, 1)
                self.assertEqual(summary["verdict"], "FAIL")
                self.assertFalse(summary["accepted"])
                self.assertFalse(summary["acceptance_gates"][gate])
                self.assertFalse(summary["product_guard_available"])

    def test_command_failure_retains_null_not_acceptance(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with mock.patch.object(diagnostic.subprocess, "Popen", side_effect=OSError("injected command failure")):
                self.assertEqual(diagnostic.run(ROOT, output, "1" * 40, "2" * 40), 1)
            result = json.loads((output / "summary.json").read_text())
            self.assertEqual(result["verdict"], "NULL")
            self.assertFalse(result["product_guard_available"])
            self.assertEqual(result["retained_samples"], 0)
            self.assertIn("injected command failure", result["error"])

    def sample(self):
        return f"{diagnostic.BENCHMARK} 512 100 ns/op 200 B/op 3 allocs/op\nPASS\n"

    def rows(self):
        return [dict(pair=pair, revision=revision, **diagnostic.parse_sample(self.sample()))
                for pair in range(3) for revision in
                (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]

    def test_fixed_plan_and_independent_strict_limits(self):
        self.assertTrue(diagnostic.evaluate(self.rows())["measurement_pass"])
        for metric, value, name in (("ns/op", 106, "timing"), ("B/op", 201, "bytes"),
                                    ("allocs/op", 4, "allocations")):
            rows = self.rows()
            for row in rows:
                if row["revision"] == "candidate":
                    row["metrics"][metric] = value
            self.assertFalse(diagnostic.evaluate(rows)["gates"][name])
            self.assertFalse(diagnostic.evaluate(rows)["measurement_pass"])
        # Pairing must preserve chronological AB/BA/AB, not sort results by cost.
        rows = self.rows()
        for invalid in (rows[:-1], rows + rows[:1], list(reversed(rows)), rows[1:] + rows[:1]):
            with self.assertRaises(ValueError):
                diagnostic.evaluate(invalid)
        for row in rows:
            row["metrics"]["ns/op"] = (row["pair"] + 1) * 100 * (1.02 if row["revision"] == "candidate" else 1)
        self.assertAlmostEqual(diagnostic.evaluate(rows)["median_paired_delta_percent"], 2)

    def test_only_predeclared_iteration_count_is_accepted(self):
        self.assertEqual(diagnostic.parse_sample(self.sample())["iterations"], 512)
        for count in (1, 511, 513, 1024):
            with self.subTest(count=count):
                with self.assertRaisesRegex(ValueError, "exactly 512 iterations"):
                    diagnostic.parse_sample(self.sample().replace(" 512 ", f" {count} "))
                rows = self.rows()
                rows[-1]["iterations"] = count
                with self.assertRaisesRegex(ValueError, "exactly 512 iterations"):
                    diagnostic.evaluate(rows)

    def test_incomplete_malformed_and_failed_samples_refused(self):
        good = self.sample()
        for bad in (good + good, good.replace("512", "0"), good.replace("PASS", "FAIL"),
                    good.replace("100 ns/op", "nan ns/op"), good.replace("100 ns/op", "0 ns/op"),
                    good.replace("200 B/op", "-1 B/op"), good.replace("3 allocs/op", ""),
                    good.replace("3 allocs/op", "3 allocs/op 3 allocs/op"),
                    good.replace("3 allocs/op", "3 unknown/op"), good + "panic: broken\n"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                diagnostic.parse_sample(bad)

    def test_scope_is_identical_in_workflow_and_excludes_unrelated_changes(self):
        workflow = (ROOT / ".github/workflows/treedb-tests.yml").read_text()
        expression = re.search(r"(?m)^      PRESSURE_STATS_PATH_RE: '([^']+)'$", workflow)[1]
        scope = re.compile(expression)
        included = ["TreeDB/caching/db.go", "TreeDB/caching/pool_pressure_snapshot.go",
                    diagnostic.HARNESS, "TreeDB/caching/pool_pressure_publication_bench_test.go",
                    "scripts/pool_pressure_stats_diagnostic.py", "scripts/mvcc_primary_cost_gate.py",
                    ".github/scripts/test_pool_pressure_stats_diagnostic.py"]
        excluded = ["TreeDB/db/db.go", "TreeDB/mvcc/primary_generation_routing_v6_test.go",
                    "TreeDB/caching/iterator.go", "docs/benchmarks/pool-pressure-publication.md"]
        for path in included + excluded:
            with self.subTest(path=path):
                self.assertEqual(bool(scope.fullmatch(path)), path in included)
                self.assertEqual(bool(diagnostic.SCOPE.fullmatch(path)), path in included)
        start = workflow.index("      - name: Compare public Stats diagnostic")
        step = workflow[start:workflow.index("      - name: Add benchmark summary", start)]
        self.assertIn("steps.paths.outputs.pressure_stats == 'true'", step)
        self.assertNotIn("continue-on-error:", step)
        self.assertIn("--acceptance-contract v2", step)
        self.assertIn('"$RUNNER_TEMP/mvcc-raw-path-gate/pressure-stats"', step)
        self.assertIn("scripts/mvcc_primary_cost_gate.py", workflow)

    def test_overlay_fixture_has_only_public_stats_and_preserves_real_samplers(self):
        fixture = (ROOT / diagnostic.HARNESS).read_text()
        self.assertEqual(re.findall(r"func (Benchmark\w+)\(", fixture), [diagnostic.BENCHMARK])
        for call in ("backenddb.Open(", "SetSync(", "cached.Stats()", "b.ReportAllocs()", "b.ResetTimer()", "b.StopTimer()"):
            self.assertIn(call, fixture)
        self.assertGreater(fixture.index("b.StopTimer()"), fixture.index("runtime.KeepAlive(cached.Stats())"))
        for private in ("poolPressureNow", "publishPoolPressureSnapshot", "samplePoolPressureSnapshot"):
            self.assertNotIn(private, fixture)
        self.assertNotIn("func " + diagnostic.BENCHMARK, (ROOT / "TreeDB/caching/pool_pressure_publication_bench_test.go").read_text())


if __name__ == "__main__":
    unittest.main()
