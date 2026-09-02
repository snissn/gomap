#!/usr/bin/env python3
"""Tests the fail-closed guard used by all live-checkout MVCC measurements."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "scripts" / "mvcc_candidate_checkout_guard.sh"
MEASUREMENT_SCRIPTS = (
    ROOT / "scripts" / "mvcc_raw_path_gate.sh",
    ROOT / "scripts" / "mvcc_adapter_overhead_gate.sh",
    ROOT / "scripts" / "mvcc_closeout_matrix.sh",
)


class CandidateCheckoutGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self.tmp.name)
        self.git("init", "-q")
        self.git("config", "user.email", "mvcctest@example.invalid")
        self.git("config", "user.name", "MVCC Test")
        (self.repo / ".gitignore").write_text("artifacts/\n", encoding="utf-8")
        (self.repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
        self.git("add", ".gitignore", "tracked.txt")
        self.git("commit", "-qm", "fixture")
        self.sha = self.git("rev-parse", "HEAD").stdout.strip()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ("git", "-C", str(self.repo), *args),
            check=True,
            text=True,
            capture_output=True,
        )

    def guard(self, sha: str | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            (str(GUARD), str(self.repo), self.sha if sha is None else sha),
            check=False,
            text=True,
            capture_output=True,
        )

    def test_clean_checkout_passes(self) -> None:
        self.assertEqual(self.guard().returncode, 0)

    def test_ignored_artifacts_do_not_block(self) -> None:
        artifact = self.repo / "artifacts" / "summary.json"
        artifact.parent.mkdir()
        artifact.write_text("{}\n", encoding="utf-8")
        self.assertEqual(self.guard().returncode, 0)

    def test_tracked_edit_fails_closed(self) -> None:
        (self.repo / "tracked.txt").write_text("dirty\n", encoding="utf-8")
        result = self.guard()
        self.assertEqual(result.returncode, 2)
        self.assertIn("candidate worktree is dirty", result.stderr)
        self.assertIn("tracked.txt", result.stderr)

    def test_untracked_file_fails_closed(self) -> None:
        (self.repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
        result = self.guard()
        self.assertEqual(result.returncode, 2)
        self.assertIn("?? untracked.txt", result.stderr)

    def test_sha_mismatch_fails_before_measurement(self) -> None:
        result = self.guard("0" * 40)
        self.assertEqual(result.returncode, 2)
        self.assertIn("candidate mismatch", result.stderr)

    def test_all_live_checkout_scripts_guard_before_output_cleanup(self) -> None:
        invocation = '"$ROOT/scripts/mvcc_candidate_checkout_guard.sh" "$ROOT" "$CANDIDATE_SHA"'
        for script in MEASUREMENT_SCRIPTS:
            text = script.read_text(encoding="utf-8")
            with self.subTest(script=script.name):
                self.assertEqual(text.count(invocation), 1)
                self.assertLess(text.index(invocation), text.index('rm -rf "$OUT_DIR"'))


if __name__ == "__main__":
    unittest.main()
