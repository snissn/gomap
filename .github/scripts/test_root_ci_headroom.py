#!/usr/bin/env python3
"""Contract tests for complete, bounded Root CI sharding."""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "root-tests.yml"


def matrix_entries(source: str) -> dict[str, dict[str, str]]:
    matrix = source.split("        include:\n", 1)[1].split(
        "    timeout-minutes:", 1
    )[0]
    entries: dict[str, dict[str, str]] = {}
    current: dict[str, str] | None = None
    for line in matrix.splitlines():
        name_match = re.fullmatch(r"          - name: (\S+)", line)
        if name_match is not None:
            name = name_match.group(1)
            current = {"name": name}
            entries[name] = current
            continue
        field_match = re.fullmatch(r"            ([a-z_]+): (\S+)", line)
        if current is not None and field_match is not None:
            current[field_match.group(1)] = field_match.group(2)
    return entries


def command_lines(*args: str) -> list[str]:
    result = subprocess.run(
        list(args),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def modulo_shards(items: list[str], count: int) -> list[list[str]]:
    return [[item for index, item in enumerate(items) if index % count == shard]
            for shard in range(count)]


class RootCIHeadroomContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = WORKFLOW.read_text(encoding="utf-8")
        cls.entries = matrix_entries(cls.source)

    def test_existing_ubuntu_matrix_is_unchanged(self) -> None:
        self.assertEqual(
            self.entries["ubuntu-latest-1"],
            {
                "name": "ubuntu-latest-1",
                "os": "ubuntu-latest",
                "run_docs_check": "true",
                "run_vet": "true",
                "test_shard_index": "0",
                "test_shard_count": "2",
            },
        )
        self.assertEqual(
            self.entries["ubuntu-latest-2"],
            {
                "name": "ubuntu-latest-2",
                "os": "ubuntu-latest",
                "run_docs_check": "false",
                "run_vet": "false",
                "test_shard_index": "1",
                "test_shard_count": "2",
            },
        )

    def test_macos_matrix_has_two_complete_disjoint_shards(self) -> None:
        macos = {
            name: entry
            for name, entry in self.entries.items()
            if entry.get("os") == "macos-latest"
        }
        self.assertEqual(
            macos,
            {
                "macos-latest-1": {
                    "name": "macos-latest-1",
                    "os": "macos-latest",
                    "run_docs_check": "true",
                    "run_vet": "true",
                    "test_shard_index": "0",
                    "test_shard_count": "2",
                },
                "macos-latest-2": {
                    "name": "macos-latest-2",
                    "os": "macos-latest",
                    "run_docs_check": "false",
                    "run_vet": "false",
                    "test_shard_index": "1",
                    "test_shard_count": "2",
                },
            },
        )

    def test_timeout_and_shard_enumeration_contract_are_preserved(self) -> None:
        self.assertIn("    timeout-minutes: 15", self.source)
        self.assertIn(
            "go list ./... | grep -v "
            "'^github.com/snissn/gomap/TreeDB/db$'",
            self.source,
        )
        self.assertIn(
            "go test ./TreeDB/db -list 'Test.*' | grep '^Test'",
            self.source,
        )
        self.assertEqual(self.source.count("if ((c % n) == idx) print"), 2)

    def test_contract_runs_on_one_existing_ubuntu_shard(self) -> None:
        self.assertIn("name: Root CI headroom contract", self.source)
        self.assertIn(
            "if: ${{ matrix.name == 'ubuntu-latest-1' }}",
            self.source,
        )
        self.assertIn(
            "python3 .github/scripts/test_root_ci_headroom.py",
            self.source,
        )

    def test_current_package_and_db_test_domains_partition_exactly_once(self) -> None:
        packages = [
            package
            for package in command_lines("go", "list", "./...")
            if package != "github.com/snissn/gomap/TreeDB/db"
        ]
        db_tests = [
            name
            for name in command_lines(
                "go", "test", "./TreeDB/db", "-list", "Test.*"
            )
            if name.startswith("Test")
        ]
        for label, domain in (("packages", packages), ("TreeDB/db tests", db_tests)):
            with self.subTest(domain=label):
                self.assertTrue(domain)
                self.assertEqual(len(domain), len(set(domain)))
                shards = modulo_shards(domain, 2)
                self.assertFalse(set(shards[0]) & set(shards[1]))
                self.assertEqual(set(shards[0]) | set(shards[1]), set(domain))
                self.assertEqual(sum(map(len, shards)), len(domain))


if __name__ == "__main__":
    unittest.main()
