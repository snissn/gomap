"""Exercise fixed-plan failure custody without running a benchmark."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "qualification", Path(__file__).with_name("run_shared_cow_qualification.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class LaunchFailureTests(unittest.TestCase):
    def run_plan(self, mode, missing_cwd=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            refused = root / "not-executable"
            refused.write_text("not an executable\n")
            executable = root / "exit-seven"
            executable.write_text("#!/bin/sh\nexit 7\n")
            executable.chmod(0o700)
            sides = {}
            for name, binary in (("baseline", refused), ("candidate", executable)):
                if missing_cwd:
                    binary = executable
                sides[name] = dict(binary=str(binary), sha256=driver.digest(binary),
                    cwd=str(root / "missing") if missing_cwd else str(root))
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(dict(tmpdir=str(root), **sides)))
            output = root / "output"
            # The actual OS launches exercise EACCES and ENOENT; process
            # inventory is unrelated to these controlled failures.
            with patch.object(sys, "argv", ["qualification", str(manifest), mode, str(output)]), \
                    patch.object(driver, "host_inventory", lambda path: path.write_text("test inventory\n")), \
                    contextlib.redirect_stdout(io.StringIO()):
                driver.main()
            receipts = json.loads((output / "receipts.json").read_text())
            plan = json.loads((output / "plan.json").read_text())
            self.assertEqual(len(receipts), 64 if mode == "primary" else 24)
            self.assertEqual([row["label"] for row in receipts], [row["label"] for row in plan])
            for row in receipts:
                self.assertEqual(row["status"], "failed")
                self.assertIsNone(row["metrics"])
                self.assertIn("started_unix", row)
                self.assertGreaterEqual(row["wall_seconds"], 0)
                if missing_cwd or row["route"] == "baseline":
                    self.assertIsNone(row["returncode"])
                    failure = row["infrastructure_failure"]
                    self.assertEqual(failure["kind"], "INFRASTRUCTURE_UNAVAILABLE")
                    self.assertEqual(failure["stage"], "launch")
                    self.assertIn(failure["errno"], (2, 13))
                    self.assertTrue(failure["message"])
                else:
                    self.assertEqual(row["returncode"], 7)
                    self.assertNotIn("infrastructure_failure", row)
            if mode == "primary":
                summary = json.loads((output / "summary.json").read_text())
                self.assertTrue(all(not cell["numeric_target_pass"] and cell["median_ratios"] is None
                    for cell in summary.values()))

    def test_primary_launch_refusal_preserves_remaining_attempts(self):
        self.run_plan("primary")

    def test_resource_missing_working_directory_preserves_fixed_plan(self):
        self.run_plan("resources", missing_cwd=True)


if __name__ == "__main__":
    unittest.main()
