#!/usr/bin/env python3
"""Exercise Makefile command wiring without downloading Go dependencies.

A recording Go executable checks orchestration, not storage correctness. The
real native-wire tests are run separately by make check-nativewire.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PACKAGES = ["./TreeDB/nativewire", "./cmd/treedb-native-server"]
NATIVE_BUILD = ["build", "-o", "bin/treedb-native-server", "./cmd/treedb-native-server"]


class MakefileWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copyfile(ROOT / "Makefile", self.root / "Makefile")
        for directory in (
            "HashDB", "TreeDB", "cmd/unified_bench", "cmd/benchprof",
            "cmd/collection_load_fixture", "cmd/collection_bench_matrix",
            "cmd/collection_canonical_bench", "cmd/treedb_out_of_core_smoke",
            "tools",
        ):
            (self.root / directory).mkdir(parents=True, exist_ok=True)
        self.log = self.root / "go-calls.jsonl"
        go = self.root / "tools" / "go"
        go.write_text(
            '#!/usr/bin/env python3\n'
            'import json, os, sys\n'
            'with open(os.environ["GO_CALL_LOG"], "a", encoding="utf-8") as log:\n'
            '    log.write(json.dumps([os.getcwd(), sys.argv[1:]]) + "\\n")\n'
            'sys.exit(42 if sys.argv[1] == os.environ.get("FAIL_GO_COMMAND") else 0)\n',
            encoding="utf-8",
        )
        go.chmod(0o755)
        self.env = dict(os.environ)
        for key in ("MAKEFLAGS", "MFLAGS", "MAKEFILES", "FAIL_GO_COMMAND"):
            self.env.pop(key, None)
        self.env.update(
            PATH=str(self.root / "tools") + os.pathsep + os.environ.get("PATH", ""),
            GO_CALL_LOG=str(self.log),
        )

    def run_make(self, target: str, *, fail: str = "") -> tuple[int, list[list]]:
        self.log.unlink(missing_ok=True)
        result = subprocess.run(
            ["make", "--no-print-directory", target], cwd=self.root,
            env={**self.env, "FAIL_GO_COMMAND": fail},
            text=True, capture_output=True, timeout=30,
        )
        calls = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        if not fail:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.returncode, calls

    def test_full_test_and_vet_visit_the_root_once(self) -> None:
        for target in ("test", "vet"):
            with self.subTest(target=target):
                _, calls = self.run_make(target)
                self.assertEqual(calls, [[str(self.root), [target, "./..."]]])

    def test_focused_targets_remain_available(self) -> None:
        for name, directory in (("hashdb", "HashDB"), ("treedb", "TreeDB"), ("unified-bench", "cmd/unified_bench")):
            for command in ("test", "vet"):
                with self.subTest(name=name, command=command):
                    _, calls = self.run_make(f"{command}-{name}")
                    self.assertEqual(calls, [[str(self.root / directory), [command, "./..."]]])

    def test_build_includes_native_server_once(self) -> None:
        _, calls = self.run_make("build")
        self.assertEqual([args for _, args in calls].count(NATIVE_BUILD), 1)

    def test_native_server_target_is_phony(self) -> None:
        (self.root / "build-native-server").touch()
        _, calls = self.run_make("build-native-server")
        self.assertEqual(calls, [[str(self.root), NATIVE_BUILD]])

    def test_nativewire_check_builds_vets_and_runs_uncached_tests(self) -> None:
        _, calls = self.run_make("check-nativewire")
        self.assertEqual(calls, [
            [str(self.root), NATIVE_BUILD],
            [str(self.root), ["vet", *PACKAGES]],
            [str(self.root), ["test", "-count=1", *PACKAGES]],
        ])

    def test_failures_are_not_reported_as_success(self) -> None:
        for target, command in (("test", "test"), ("vet", "vet"), ("build-native-server", "build")):
            with self.subTest(target=target):
                rc, calls = self.run_make(target, fail=command)
                self.assertNotEqual(rc, 0)
                self.assertEqual(len(calls), 1)

    def test_nativewire_check_stops_at_the_first_failure(self) -> None:
        for index, command in enumerate(("build", "vet", "test"), start=1):
            with self.subTest(command=command):
                rc, calls = self.run_make("check-nativewire", fail=command)
                self.assertNotEqual(rc, 0)
                self.assertEqual(len(calls), index)


if __name__ == "__main__":
    unittest.main()
