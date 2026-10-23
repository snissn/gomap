#!/usr/bin/env python3
"""Pure Python failure controls for the frozen #5214 discriminator."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import cow_successor_workspace_diagnostic as diagnostic


class COWSuccessorDiagnosticTests(unittest.TestCase):
    def text(self):
        return "\n".join(f"{cell}-8 1024 100 ns/op 200 B/op 3 allocs/op"
                         for cell in diagnostic.CELLS) + "\nPASS\n"

    def rows(self):
        return [dict(pair=pair, revision=revision, **row) for pair, revision in diagnostic.ORDER
                for row in diagnostic.parse_samples(self.text())]

    def improving_rows(self):
        rows = self.rows()
        for row in rows:
            if row["revision"] == "candidate" and row["cell"].endswith("depth=8/concurrency=1") \
                    and "/GetAt/" in row["cell"]:
                row["metrics"]["allocs/op"] = 2
        return rows

    def test_exact_plan_and_visible_per_cell_gates(self):
        self.assertEqual(len(self.rows()), 60)
        self.assertEqual(len(diagnostic.CELLS), 10)
        self.assertEqual(len(diagnostic.CELLS) - sum("/Prefix/" in c for c in diagnostic.CELLS), 8)
        self.assertTrue(diagnostic.evaluate(self.improving_rows())["measurement_pass"])
        self.assertFalse(diagnostic.evaluate(self.rows())["measurement_pass"])
        boundary = self.improving_rows()
        for row in boundary:
            if row["revision"] == "candidate":
                row["metrics"]["ns/op"] = 105
        self.assertTrue(diagnostic.evaluate(boundary)["measurement_pass"])
        for cell in diagnostic.CELLS:
            for metric, value, gate in (("ns/op", 106, "time"), ("B/op", 201, "bytes"),
                                        ("allocs/op", 4, "allocations")):
                rows = self.improving_rows()
                for row in rows:
                    if row["revision"] == "candidate" and row["cell"] == cell:
                        row["metrics"][metric] = value
                result = diagnostic.evaluate(rows)
                self.assertFalse(result["cells"][cell]["gates"][gate])
                self.assertFalse(result["measurement_pass"])
        # Both serial layouts must improve independently, not an average gain.
        for pointer in ("false", "true"):
            rows = self.improving_rows()
            for row in rows:
                if row["revision"] == "candidate" and row["cell"] == diagnostic.BENCHMARK + \
                        f"/GetAt/pointer={pointer}/depth=8/concurrency=1":
                    row["metrics"]["allocs/op"] = 3
            self.assertFalse(diagnostic.evaluate(rows)["measurement_pass"])
        rows = self.improving_rows()
        for invalid in (rows[:-1], rows + rows[:1], list(reversed(rows)), rows[10:] + rows[:10]):
            with self.assertRaises(ValueError):
                diagnostic.evaluate(invalid)

    def test_invalid_partial_duplicate_and_split_logical_rows(self):
        good = self.text()
        bad_texts = [good + good, good.replace("1024", "1023", 1), good.replace("PASS", "FAIL"),
                     good.replace("100 ns/op", "nan ns/op", 1), good.replace("100 ns/op", "0 ns/op", 1),
                     good.replace("200 B/op", "-1 B/op", 1), good.replace("3 allocs/op", "", 1),
                     good.replace("3 allocs/op", "3 allocs/op 3 allocs/op", 1),
                     good.replace("3 allocs/op", "3 unknown/op", 1), good + "panic: failure\n",
                     good.replace("pointer=false", "pointer=unknown", 1)]
        for bad in bad_texts:
            with self.subTest(text=bad[:100]), self.assertRaises(ValueError):
                diagnostic.parse_samples(bad)
        partial = "\n".join(good.splitlines()[:4])
        self.assertEqual(len(diagnostic.parse_samples(partial, complete=False)), 4)
        with self.assertRaises(ValueError):
            diagnostic.parse_samples(partial)
        split = good.replace("-8 1024", "-8\tsetup log\nanother setup log\n1024", 1)
        self.assertEqual(diagnostic.parse_samples(split), diagnostic.parse_samples(good))

    def test_command_failure_is_null_and_source_is_frozen(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            with mock.patch.object(diagnostic.subprocess, "Popen", side_effect=OSError("injected failure")):
                self.assertEqual(diagnostic.run(ROOT, output, diagnostic.BASELINE, "1" * 40), 1)
            result = json.loads((output / "summary.json").read_text())
            self.assertEqual(result["verdict"], "NULL")
            self.assertIsNone(result["final_acceptance"])
            self.assertFalse(result["product_guard_available"])
            self.assertEqual(result["retained_rows"], 0)
        self.assertEqual(hashlib.sha256((ROOT / diagnostic.HARNESS).read_bytes()).hexdigest(),
                         diagnostic.HARNESS_SHA256)
        self.assertEqual(diagnostic.ALLOWED_CHANGES, {
            "scripts/cow_successor_workspace_diagnostic.py",
            ".github/scripts/test_cow_successor_workspace_diagnostic.py",
            ".github/workflows/treedb-tests.yml", ".github/ci/ci_impact.json",
            "TreeDB/docs/benchmarks/cow-successor-workspace-5214.md"})
        self.assertFalse(any(path.endswith(".go") for path in diagnostic.ALLOWED_CHANGES))
        for path in ("TreeDB/caching/cow_read.go", "TreeDB/caching/cow_cut.go", diagnostic.HARNESS,
                     "scripts/mvcc_primary_cost_gate.py"):
            actual = (ROOT / path).read_bytes()
            frozen = subprocess.check_output(["git", "show", diagnostic.PRODUCT + ":" + path], cwd=ROOT)
            self.assertEqual(actual, frozen, path)
        with self.assertRaises(ValueError):
            diagnostic.run(ROOT, Path("unused"), "0" * 40, "1" * 40)

    def test_total_deadline_kills_and_joins_child_process_group(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            children = []
            actual_popen = subprocess.Popen
            def start(*args, **kwargs):
                process = actual_popen(*args, **kwargs)
                children.append(process)
                self.assertTrue(kwargs["start_new_session"])
                return process
            with mock.patch.object(diagnostic.subprocess, "Popen", side_effect=start), \
                    mock.patch.object(diagnostic.os, "killpg", wraps=os.killpg) as kill:
                with self.assertRaises(TimeoutError):
                    diagnostic.execute([sys.executable, "-c", "import time; time.sleep(60)"],
                        path, dict(os.environ), path / "stdout", path / "stderr", time.monotonic() + 0.1)
                kill.assert_called_once_with(children[0].pid, diagnostic.signal.SIGKILL)
                self.assertIsNotNone(children[0].returncode)
            with mock.patch.object(diagnostic.subprocess, "Popen") as popen:
                with self.assertRaises(TimeoutError):
                    diagnostic.execute(["unused"], path, {}, path / "out", path / "err", time.monotonic() - 1)
                popen.assert_not_called()

    def test_synthetic_collection_missing_rows_and_continuous_noise_are_null(self):
        # This substitutes command execution and observation, never invokes Go.
        # It checks orchestration/qualification, not product performance.
        for variant in ("complete", "noise", "partial", "quiet_failure"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "evidence"
                observer = mock.Mock()
                observer.finish.return_value = {"busy": variant == "noise"}
                def fake_execute(argv, cwd, environment, stdout, stderr, deadline,
                                 observation=None, on_start=None):
                    argv = [str(arg) for arg in argv]
                    if on_start:
                        on_start(123)
                    if observation is not None:
                        observation.observe()
                    text, status = "", 0
                    if argv[:3] == ["git", "worktree", "add"]:
                        Path(argv[-2]).mkdir(parents=True)
                    elif argv[:3] == ["git", "worktree", "remove"]:
                        shutil.rmtree(argv[-1])
                    elif argv[:3] == ["go", "test", "-c"]:
                        Path(argv[argv.index("-o") + 1]).write_bytes(b"synthetic binary")
                    elif argv[0].endswith("-successor.test"):
                        lines = []
                        for cell in diagnostic.CELLS:
                            improves = ("candidate-" in argv[0] and "/GetAt/" in cell
                                        and cell.endswith("depth=8/concurrency=1"))
                            lines.append(f"{cell}-8 1024 100 ns/op 200 B/op {2 if improves else 3} allocs/op")
                        if variant == "partial":
                            lines, status = lines[:4], 1
                        text = "\n".join(lines) + ("\nPASS\n" if status == 0 else "\nFAIL\n")
                    stdout.write_text(text)
                    stderr.write_text("")
                    return status
                def binding(binary, *unused):
                    identity = diagnostic.maintained.file_identity(binary)
                    identity.pop("path")
                    return dict(executable=identity, linkage="static", interpreter=None, libraries=[])
                with mock.patch.object(diagnostic, "execute", side_effect=fake_execute), \
                        mock.patch.object(diagnostic.os, "sched_getaffinity", return_value={0, 1}), \
                        mock.patch.object(diagnostic.maintained, "runtime_binding", side_effect=binding), \
                        mock.patch.object(diagnostic.maintained, "ProcessObservation", return_value=observer), \
                        mock.patch.object(diagnostic.maintained, "acquire_quiet_window",
                            side_effect=RuntimeError("quiet unavailable") if variant == "quiet_failure" else None):
                    status = diagnostic.run(ROOT, output, diagnostic.BASELINE, "1" * 40)
                result = json.loads((output / "summary.json").read_text())
                expected_rows = 0 if variant == "quiet_failure" else 4 if variant == "partial" else 60
                self.assertEqual(result["retained_rows"], expected_rows)
                self.assertEqual(result["verdict"], "PASS" if variant == "complete" else "NULL")
                self.assertEqual(status, 0 if variant == "complete" else 1)
                self.assertEqual(result["product_guard_available"], variant == "complete")
                self.assertEqual(observer.observe.call_count, 0 if variant == "quiet_failure" else 1 if variant == "partial" else 6)
                cpu = json.loads((output / "cpu.json").read_text())
                self.assertEqual(cpu["allowed_cpus"], [0, 1])
                self.assertTrue(cpu["oversubscribed"])
                overlay = json.loads((output / "baseline-overlay.json").read_text())["Replace"]
                self.assertEqual(len(overlay), 1)
                self.assertTrue(next(iter(overlay)).endswith(diagnostic.HARNESS))

    def test_scoped_hosted_step_preserves_primary_and_all_original_controls(self):
        workflow = (ROOT / ".github/workflows/treedb-tests.yml").read_text()
        expression = re.search(r"(?m)^      COW_SUCCESSOR_PATH_RE: '([^']+)'$", workflow)[1]
        scope = re.compile(expression)
        included = ["TreeDB/caching/cow_read.go", "TreeDB/caching/cow_cut.go",
                    "TreeDB/caching/cow_successor_workspace_test.go", diagnostic.HARNESS,
                    "scripts/cow_successor_workspace_diagnostic.py", "scripts/mvcc_primary_cost_gate.py",
                    ".github/scripts/test_cow_successor_workspace_diagnostic.py"]
        excluded = ["TreeDB/caching/db.go", "TreeDB/db/db.go", "TreeDB/caching/pool_pressure_snapshot.go",
                    "TreeDB/mvcc/primary_generation_routing_v6_test.go"]
        for path in included + excluded:
            self.assertEqual(bool(scope.fullmatch(path)), path in included, path)
            self.assertEqual(bool(diagnostic.SCOPE.fullmatch(path)), path in included, path)
            matched = subprocess.run(["grep", "-Eq", expression], input=path + "\n", text=True)
            self.assertEqual(matched.returncode, 0 if path in included else 1, path)
        broad = re.search(r"(?m)^      MVCC_GATE_PATH_RE: '([^']+)'$", workflow)[1]
        for path in included:
            self.assertEqual(subprocess.run(["grep", "-Eq", broad], input=path + "\n", text=True).returncode, 0, path)
        start = workflow.index("      - name: Compare frozen COW successor diagnostic")
        step = workflow[start:workflow.index("      - name: Add benchmark summary", start)]
        self.assertIn("steps.paths.outputs.cow_successor == 'true'", step)
        self.assertNotIn("continue-on-error:", step)
        self.assertIn(diagnostic.BASELINE, step)
        self.assertIn('"$RUNNER_TEMP/mvcc-raw-path-gate/cow-successor"', step)
        self.assertIn("timeout-minutes: 15", workflow)
        self.assertIn('PR_HEAD_REF: ${{ github.event.pull_request.head.ref }}', workflow)
        self.assertIn('if [[ "$PR_HEAD_REF" == "codex/5214-cow-successor-workspace" ]] &&', workflow)
        self.assertIn("scripts/mvcc_primary_cost_gate.py", workflow)
        self.assertEqual(diagnostic.GOMAXPROCS, 8)
        self.assertEqual(diagnostic.DEADLINE_SECONDS, 360)
        # No changed benchmark fixture or disabled runtime GC/samplers.
        source = (ROOT / "scripts/cow_successor_workspace_diagnostic.py").read_text()
        self.assertNotIn("GOGC=", source)
        self.assertNotRegex(source, r"environment\[['\"]GOGC['\"]\]\s*=")
        self.assertIn("runtime_environment=", source)
        self.assertNotIn('"taskset"', source)
        self.assertIn("maintained.acquire_quiet_window(output, clock=clock)", source)


if __name__ == "__main__":
    unittest.main()
