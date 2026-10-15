#!/usr/bin/env python3
"""Pure harness controls. Never executes a Go fixture or benchmark."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

sys.dont_write_bytecode = True

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("observation", HERE / "mvcc_m0_observation.py")
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)


class HarnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        product = os.environ.get("M0_TEST_PRODUCT")
        cls.source = ((Path(product) / h.BENCH_SOURCE).read_bytes() if product else
            subprocess.check_output(["git", "show", h.CANDIDATE + ":" + h.BENCH_SOURCE], cwd=HERE))

    def test_exact_schedule(self):
        rows = h.schedule()
        self.assertEqual(len(rows), 22)
        self.assertEqual([name for name, _ in rows[2:]],
                         [f"{arm}-{i}" for i in range(1, 11) for arm in ("legacy", "public")])
        self.assertEqual([args[1] for _, args in rows[:2]], ["^" + n + "$" for n in h.FIXTURES])
        for _, args in rows[2:]:
            self.assertIn("-benchtime=1x", args)
            self.assertIn("-count=1", args)

    def test_overlay_preserves_timed_body_and_writer(self):
        before = self.source.decode()
        after = h.checked_overlay(self.source).decode()
        name = "func benchmarkVacuumIndexOnlineCollectionForegroundChurn("
        original = before[before.index(name):]
        changed = after[after.index(name):]
        timed = lambda value: value[value.index("b.ResetTimer()"):value.index("b.StopTimer()")]
        self.assertEqual(timed(original), timed(changed))
        writer = "func vacuumCollectionForegroundWriter("
        self.assertEqual(before[before.index(writer):], after[after.index(writer):])
        # Exactly two non-timed call-site insertions, no other source edit.
        restored = after.replace("\t\t\tobservation := m0ObservationBegin()\n", "").replace(
            "\n\t\t\tm0ObservationEnd(b, observation, foregroundLatencies)", "")
        self.assertEqual(restored, before)

    def test_preimage_drift_refuses(self):
        with self.assertRaisesRegex(ValueError, "preimage"):
            h.checked_overlay(self.source + b"\n")

    def test_operation_export_completeness(self):
        record = {"WallStart": 1, "WallEnd": 2, "Before": {}, "After": {},
                  "Operations": [{"ordinal": i, "type": "range" if i % 2 else "point", "duration_ns": i}
                                 for i in range(4096)]}
        for side in ("Before", "After"):
            record[side] = {key: 0 for key in ("NumGC", "PauseTotalNs", "PauseNs", "PauseEnd")}
        h.validate_operations(record)
        record["Operations"][31]["type"] = "point"
        with self.assertRaisesRegex(ValueError, "order/type"):
            h.validate_operations(record)
        record["Operations"].pop()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            h.validate_operations(record)

    def test_exact_candidate_observer_python_compatibility(self):
        product = os.environ.get("M0_TEST_PRODUCT")
        source = ((Path(product) / h.OBSERVER_SOURCE).read_bytes() if product else
            subprocess.check_output(["git", "show", h.CANDIDATE + ":" + h.OBSERVER_SOURCE], cwd=HERE))
        self.assertEqual(h.hashlib.sha256(source).hexdigest(), h.OBSERVER_SHA)
        namespace = {"__name__": "fixed_candidate_observer"}
        exec(compile(source, h.OBSERVER_SOURCE, "exec"), namespace)
        with tempfile.TemporaryDirectory() as directory:
            observation = namespace["ProcessObservation"](Path(directory) / "raw.jsonl")
            result = observation.finish()
            self.assertEqual(result["cpu_threshold_percent"], 2)
            self.assertEqual(result["sampling_interval_s"], 0.25)
            self.assertIn("busy", result)

    def test_missing_telemetry_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            result = h.host_observation(Path(directory), Path(directory))
            self.assertFalse(result["stat"]["available"])
            self.assertIn("reason", result["stat"])
            self.assertFalse(result["cgroups_v2"]["available"])

    def test_leaf_and_parent_quota_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "proc/self").mkdir(parents=True)
            (root / "proc/self/cgroup").write_text("0::/leaf\n")
            (root / "cg/leaf").mkdir(parents=True)
            (root / "cg/leaf/cpu.max").write_text("max 100000\n")
            (root / "cg/cpu.max").write_text("200000 100000\n")
            groups = h.host_observation(root / "proc", root / "cg")["cgroups_v2"]["groups"]
            self.assertEqual([g["files"]["cpu.max"]["raw"] for g in groups],
                             ["max 100000\n", "200000 100000\n"])

    def test_timeout_kills_and_joins_group(self):
        # A toy Python group is a harness unit control, never product execution.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner = h.Runner(root, root, root, time.time() + 30)
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command("negative-timeout", [sys.executable, "-c",
                    "import os,time; os.fork(); time.sleep(30)"], timeout=0.2)
            receipt = json.loads((root / "commands.json").read_text())[0]
            self.assertTrue(receipt["group_killed"])
            self.assertTrue(receipt["joined"])
            self.assertTrue(receipt["adopted_group_children_joined"])

    def test_exec_retains_actual_binary_and_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "exec"
            result = subprocess.run([sys.executable, HERE / "mvcc_m0_observation_exec.py",
                sys.executable, "-c", "import sys; sys.exit(7)"],
                env=dict(os.environ, MVCC_M0_EXEC_DIR=str(output)), capture_output=True)
            self.assertEqual(result.returncode, 7, result.stderr)
            self.assertEqual(h.digest(output / "actual.test"), h.digest(sys.executable))
            self.assertEqual(json.loads((output / "exec.json").read_text())["sha256"], h.digest(sys.executable))

    def test_workflow_trust_boundary(self):
        text = (HERE.parent / "workflows/treedb-vacuum-m0-observation.yml").read_text()
        self.assertIn("  workflow_dispatch:\n", text)
        for forbidden in ("pull_request:", "push:", "self-hosted", "secrets.", "workflow_call:"):
            self.assertNotIn(forbidden, text)
        self.assertIn("    runs-on: ubuntu-24.04", text)
        self.assertIn("    timeout-minutes: 20", text)
        self.assertIn("  contents: read", text)
        self.assertIn("ref: " + h.CANDIDATE, text)
        self.assertEqual(text.count("if: always()"), 2)


if __name__ == "__main__":
    unittest.main()
