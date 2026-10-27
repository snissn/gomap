#!/usr/bin/env python3
"""Contract tests for complete, bounded Root CI sharding."""

from __future__ import annotations

import os
import re
import subprocess
import unittest
import traceback
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "root-tests.yml"
SHARD_AWK = "BEGIN { c = 0 } { if ((c % n) == idx) print; c++ }"


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


def redact_command_diagnostic(value: str) -> str:
    """Keep useful command output without URL credentials or known secret values."""
    value = re.sub(r"([A-Za-z][A-Za-z0-9+.-]*://)[^/\s]*@", r"\1[REDACTED]@", value)
    credential_name = re.compile(
        r"(?:^|_)(?:TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIALS?|PRIVATE_KEY|ACCESS_KEY(?:_ID)?)(?:$|_)",
        re.IGNORECASE,
    )
    secrets = {secret for name, secret in os.environ.items()
               if secret and credential_name.search(name)}
    for secret in sorted(secrets, key=len, reverse=True):
        value = value.replace(secret, "[REDACTED]")
    return value


def command_lines(*args: str) -> list[str]:
    try:
        result = subprocess.run(
            list(args),
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        # Suppress the original exception's rendering: its command can contain
        # credentials even after the diagnostic below has been sanitized.
        raise RuntimeError(
            f"command {[redact_command_diagnostic(arg) for arg in args]!r} "
            f"in {redact_command_diagnostic(str(ROOT))} "
            f"failed with exit status {error.returncode}\n"
            f"stdout:\n{redact_command_diagnostic(error.stdout or '')}\n"
            f"stderr:\n{redact_command_diagnostic(error.stderr or '')}"
        ) from None
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def workflow_steps(source: str) -> list[dict[str, str]]:
    lines = source.splitlines()
    steps: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    index = 0
    while index < len(lines):
        line = lines[index]
        name_match = re.fullmatch(r"      - name: (.+)", line)
        if name_match is not None:
            current = {"name": name_match.group(1)}
            steps.append(current)
            index += 1
            continue
        field_match = re.fullmatch(r"        (if|run): (.+)", line)
        if current is not None and field_match is not None:
            field, value = field_match.groups()
            if field == "run" and value == "|":
                block: list[str] = []
                index += 1
                while index < len(lines):
                    block_line = lines[index]
                    if block_line.startswith("          "):
                        block.append(block_line[10:])
                        index += 1
                        continue
                    if not block_line:
                        block.append("")
                        index += 1
                        continue
                    break
                current[field] = "\n".join(block)
                continue
            current[field] = value
        index += 1
    return steps


def awk_shards(items: list[str], count: int) -> list[list[str]]:
    input_text = "".join(f"{item}\n" for item in items)
    shards: list[list[str]] = []
    for shard in range(count):
        result = subprocess.run(
            ["awk", "-v", f"idx={shard}", "-v", f"n={count}", SHARD_AWK],
            check=True,
            capture_output=True,
            input=input_text,
            text=True,
        )
        shards.append(
            [line.strip() for line in result.stdout.splitlines() if line.strip()]
        )
    return shards


class CommandLinesTests(unittest.TestCase):
    def test_success_preserves_existing_nonempty_stripped_lines_and_runs_once(self) -> None:
        completed = subprocess.CompletedProcess(['go', 'list', './...'], 0,
                                                stdout='  first/package  \n\n second/package\n',
                                                stderr='unused success diagnostic')
        with mock.patch('subprocess.run', return_value=completed) as run:
            self.assertEqual(command_lines('go', 'list', './...'),
                             ['first/package', 'second/package'])
        run.assert_called_once_with(['go', 'list', './...'], cwd=ROOT, check=True,
                                    capture_output=True, text=True)

    def test_failure_exposes_both_streams_context_and_status_without_environment(self) -> None:
        error = subprocess.CalledProcessError(17, ['go', 'list', './...'],
                                             output='  partial package output\n',
                                             stderr='original dependency failure\n')
        with mock.patch.dict(os.environ, {'UNRELATED_CI_SECRET': 'must-never-be-printed'}), \
             mock.patch('subprocess.run', side_effect=error) as run:
            with self.assertRaises(RuntimeError) as raised:
                command_lines('go', 'list', './...')
        message = str(raised.exception)
        for value in ("['go', 'list', './...']", str(ROOT), '17', error.stdout, error.stderr):
            self.assertIn(value, message)
        self.assertNotIn('UNRELATED_CI_SECRET', message)
        self.assertNotIn('must-never-be-printed', message)
        self.assertIs(raised.exception.__context__, error)
        self.assertTrue(raised.exception.__suppress_context__)
        run.assert_called_once_with(['go', 'list', './...'], cwd=ROOT, check=True,
                                    capture_output=True, text=True)

    def test_failure_redacts_url_userinfo_and_environment_credentials_in_traceback(self) -> None:
        args = ['go', 'list', 'https://cli-user:cli-password@packages.example/module']
        stdout = 'partial output https://other-user:other-password@proxy.example/path\n'
        stderr = 'https://error-user:error-password@host.example/path?token=environment-token-value\n'
        error = subprocess.CalledProcessError(1, args, output=stdout, stderr=stderr)
        environment = {'GITHUB_TOKEN': 'environment-token-value',
                       'SERVICE_PASSWORD': 'environment-password-value',
                       'UNRELATED_SETTING': 'must-not-dump-unrelated-setting'}
        error.stderr += 'credential=' + environment['SERVICE_PASSWORD'] + '\n'
        with mock.patch.dict(os.environ, environment), mock.patch('subprocess.run', side_effect=error) as run:
            with self.assertRaises(RuntimeError) as raised:
                command_lines(*args)
        rendered = ''.join(traceback.format_exception(raised.exception))
        for secret in ('cli-user', 'cli-password', 'other-user', 'other-password',
                       'error-user', 'error-password', *environment.values()):
            self.assertNotIn(secret, rendered)
        for useful in ('packages.example/module', 'proxy.example/path', 'host.example/path',
                       'partial output', 'exit status 1', '[REDACTED]'):
            self.assertIn(useful, rendered)
        run.assert_called_once()

    def test_nonzero_exit_with_no_captured_output_still_fails_once(self) -> None:
        error = subprocess.CalledProcessError(1, ['go', 'list', './...'])
        with mock.patch('subprocess.run', side_effect=error) as run:
            with self.assertRaisesRegex(RuntimeError, 'exit status 1') as raised:
                command_lines('go', 'list', './...')
        self.assertIn('stdout:\n\nstderr:\n', str(raised.exception))
        self.assertIs(raised.exception.__context__, error)
        self.assertTrue(raised.exception.__suppress_context__)
        run.assert_called_once()


class RootCIHeadroomContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = WORKFLOW.read_text(encoding="utf-8")
        cls.entries = matrix_entries(cls.source)
        cls.steps = workflow_steps(cls.source)

    def test_existing_ubuntu_matrix_is_unchanged(self) -> None:
        self.assertEqual(
            set(self.entries),
            {
                "ubuntu-latest-1",
                "ubuntu-latest-2",
                "macos-latest-1",
                "macos-latest-2",
            },
        )
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

    def test_timeout_and_non_treedb_shard_contract_are_preserved(self) -> None:
        self.assertIn("    timeout-minutes: 40", self.source)
        self.assertIn(
            "go list ./... | grep -Ev "
            "'^github.com/snissn/gomap/TreeDB($|/)'",
            self.source,
        )
        self.assertNotIn("go test ./TreeDB/db", self.source)
        self.assertEqual(self.source.count("if ((c % n) == idx) print"), 1)

    def test_vet_excludes_the_dedicated_treedb_domain(self) -> None:
        vet_steps = [step for step in self.steps if step["name"] == "Vet"]
        self.assertEqual(len(vet_steps), 1)
        self.assertIn(
            "go list ./... | grep -Ev "
            "'^github.com/snissn/gomap/TreeDB($|/)' | xargs go vet",
            vet_steps[0]["run"],
        )

    def test_contract_runs_on_one_existing_ubuntu_shard(self) -> None:
        contract_steps = [
            step
            for step in self.steps
            if step["name"] == "Root CI headroom contract"
        ]
        self.assertEqual(
            contract_steps,
            [
                {
                    "name": "Root CI headroom contract",
                    "if": "${{ matrix.name == 'ubuntu-latest-1' }}",
                    "run": "python3 .github/scripts/test_root_ci_headroom.py",
                }
            ],
        )

    def test_current_non_treedb_package_domain_partitions_exactly_once(self) -> None:
        test_steps = [step for step in self.steps if step["name"] == "Test"]
        self.assertEqual(len(test_steps), 1)
        test_command = test_steps[0]["run"]
        package_pipeline = (
            "go list ./... | grep -Ev "
            "'^github.com/snissn/gomap/TreeDB($|/)' | "
            'awk -v idx="$shard_index" -v n="$shard_count" '
            f"'{SHARD_AWK}' > \"$package_file\""
        )
        self.assertEqual(test_command.count(package_pipeline), 1)
        packages = [
            package
            for package in command_lines("go", "list", "./...")
            if package != "github.com/snissn/gomap/TreeDB"
            and not package.startswith("github.com/snissn/gomap/TreeDB/")
        ]
        self.assertTrue(packages)
        self.assertEqual(len(packages), len(set(packages)))
        shards = awk_shards(packages, 2)
        self.assertFalse(set(shards[0]) & set(shards[1]))
        self.assertEqual(set(shards[0]) | set(shards[1]), set(packages))
        self.assertEqual(sum(map(len, shards)), len(packages))


if __name__ == "__main__":
    unittest.main()
