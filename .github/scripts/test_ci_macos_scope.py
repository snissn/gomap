#!/usr/bin/env python3
"""Executable immutable-tree contract for the macOS PR classifier."""

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().with_name("ci_macos_scope.py")


class MacScope(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name)
        self.git("init", "-q")
        self.git("config", "user.name", "CI test")
        self.git("config", "user.email", "ci@example.invalid")
        self.git("config", "core.filemode", "true")
        self.write("CHANGELOG.md")
        self.write("TODO.md")
        self.write("TreeDB/docs/spec/README.md")
        self.write("TreeDB/docs/spec/backup-restore.md")
        self.write("TreeDB/docs/spec/no-wal-fast-audit.md")
        self.write("docs/benchmarks/ycsb_mongodb_treedb_current.md")
        self.write("TreeDB/nativewire/lifecycle_test.go")
        self.write("HashDB/stress/compact_test.go")
        self.write("TreeDB/integration/gethethdb/go.mod")
        self.write("TreeDB/pkg/nested/go.mod")
        for path in (
            "TreeDB/collections/testdata/example_test.go",
            "TreeDB/collections/.hidden/example_test.go",
            "TreeDB/collections/_hidden/example_test.go",
            "TreeDB/collections/vendor/example_test.go",
            "TreeDB/collections/tree_fixture_test.go",
            "TreeDB/collections/fixtures/example_test.go",
            "TreeDB/docs/evidence/example_test.go",
            "TreeDB/integration/gethethdb/example_test.go",
            "TreeDB/pkg/nested/sub/example_test.go",
        ):
            self.write(path)
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True).strip()

    def write(self, name, text="first\n"):
        path = self.repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def commit(self):
        self.git("add", "-A")
        self.git("commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def classify(self, head, base=None, lane="root"):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--base", base or self.base, "--head", head, "--lane", lane],
            cwd=self.repo, capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()

    def test_exact_prose_only(self):
        self.write("CHANGELOG.md", "new\n")
        self.write("TODO.md", "new\n")
        self.assertEqual(self.classify(self.commit()), "skip")

    def test_audited_unified_markdown_does_not_skip_root_docs_checks(self):
        for path in (
            "TreeDB/docs/spec/backup-restore.md",
            "TreeDB/docs/spec/no-wal-fast-audit.md",
            "docs/benchmarks/ycsb_mongodb_treedb_current.md",
        ):
            self.write(path, "changed prose\n")
        head = self.commit()
        self.assertEqual(self.classify(head, lane="root"), "full")
        self.assertEqual(self.classify(head, lane="unified"), "skip")
        self.write("TreeDB/docs/spec/README.md", "also changed\n")
        self.assertEqual(self.classify(self.commit(), lane="unified"), "full")

    def test_uncommitted_files_are_not_used(self):
        self.write("CHANGELOG.md", "new\n")
        head = self.commit()
        self.write("TreeDB/go_test.go")
        self.assertEqual(self.classify(head), "skip")

    def test_unknown_changes_are_full_even_with_prose(self):
        for name in ("TreeDB/go_test.go", "TreeDB/nativewire/lifecycle.go", "go.sum", "docs/README.md",
                     "TreeDB/docs/spec/README.md", "README.md", "pkg/c.h",
                     ".github/scripts/ci_macos_scope.py",
                     ".github/workflows/root.yml", "nested/CHANGELOG.md",
                     "CHANGELOG.md\n", ".github/CI_POLICY.md", "fixtures/answer.json"):
            with self.subTest(name=name):
                self.git("reset", "--hard", self.base)
                self.write("CHANGELOG.md", "new\n")
                self.write(name, "new\n")
                self.assertEqual(self.classify(self.commit()), "full")

    def test_add_delete_rename_mode_change_and_symlink_are_full(self):
        for op in ("add", "delete", "rename", "mode", "symlink"):
            with self.subTest(op=op):
                self.git("reset", "--hard", self.base)
                path = self.repo / "CHANGELOG.md"
                if op == "add":
                    self.write("TENTATIVE_DELETIONS.md")
                elif op == "delete":
                    path.unlink()
                elif op == "rename":
                    path.rename(self.repo / "TENTATIVE_DELETIONS.md")
                elif op == "mode":
                    os.chmod(path, 0o755)
                else:
                    path.unlink()
                    path.symlink_to("TODO.md")
                self.assertEqual(self.classify(self.commit()), "full")

    def test_separate_native_owner_runs_existing_treedb_tests(self):
        self.write("TreeDB/nativewire/lifecycle_test.go", "changed\n")
        head = self.commit()
        self.assertEqual(self.classify(head, lane="root"), "skip")
        self.assertEqual(self.classify(head, lane="unified"), "skip")

    def test_hashdb_test_has_root_but_not_unified_ownership(self):
        self.write("HashDB/stress/compact_test.go", "changed\n")
        head = self.commit()
        self.assertEqual(self.classify(head, lane="root"), "full")
        self.assertEqual(self.classify(head, lane="unified"), "skip")
        self.assertEqual(self.classify(head, lane="unknown"), "full")
        self.write("HashDB/stress/compact.go", "changed\n")
        self.assertEqual(self.classify(self.commit(), lane="unified"), "full")

    def test_existing_test_fixtures_evidence_and_nested_modules_stay_full(self):
        paths = self.git("ls-files", "TreeDB").splitlines()
        for path in paths:
            if path.endswith("_test.go") and "nativewire" not in path:
                with self.subTest(path=path):
                    self.git("reset", "--hard", self.base)
                    self.write(path, "changed\n")
                    head = self.commit()
                    self.assertEqual(self.classify(head, lane="root"), "full")
                    self.assertEqual(self.classify(head, lane="unified"), "full")

    def test_unverifiable_and_empty_ranges_are_full(self):
        self.assertEqual(self.classify(self.base), "full")
        self.assertEqual(self.classify("HEAD"), "full")
        self.assertEqual(self.classify("0" * 40), "full")
        self.assertEqual(self.classify(self.git("rev-parse", "HEAD:TODO.md")), "full")


if __name__ == "__main__":
    unittest.main()
