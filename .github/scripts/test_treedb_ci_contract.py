#!/usr/bin/env python3
"""Structural contract tests for the required TreeDB CI merge gate."""

from __future__ import annotations

import re
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "treedb-tests.yml"


def block(source: str, start: str, end: str | None = None) -> str:
    begin = source.index(start)
    if end is None:
        return source[begin:]
    finish = source.index(end, begin + len(start))
    return source[begin:finish]


class TreeDBCIContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = WORKFLOW.read_text(encoding="utf-8")
        cls.m0 = (ROOT / ".github/workflows/treedb-vacuum-m0.yml").read_text(
            encoding="utf-8"
        )

    def test_vet_platform_budgets_and_macos_diagnostics(self) -> None:
        vet = block(self.source, "  vet:\n", "  test:\n")
        for platform, timeout in (("ubuntu", 15), ("macos", 30), ("windows", 15)):
            self.assertRegex(vet, rf"name: {platform}\n\s+os: [^\n]+\n\s+timeout_minutes: {timeout}\n")
        self.assertIn("timeout-minutes: ${{ matrix.timeout_minutes }}", vet)
        self.assertIn("if: ${{ matrix.name != 'macos' }}", vet)
        self.assertIn("if: ${{ matrix.name == 'macos' }}", vet)
        self.assertIn("/usr/bin/time -l go vet -x ./...", vet)
        self.assertIn("run: go vet ./...", vet)
        self.assertIn("cache: false", vet)
        self.assertNotIn("continue-on-error", vet)

    def test_events_cover_merge_results_manual_certification_and_main_push(self) -> None:
        events = block(self.source, "on:\n", "\npermissions:")
        self.assertRegex(events, r"(?m)^  pull_request:\s*$")
        self.assertRegex(events, r"(?m)^  merge_group:\s*$")
        self.assertRegex(events, r"(?m)^  workflow_dispatch:\s*$")
        self.assertRegex(
            events,
            r"(?ms)^  push:\s*\n    branches:\s*\n      - main\s*$",
        )

    def test_only_superseded_pull_request_runs_share_a_concurrency_group(self) -> None:
        concurrency = block(self.source, "concurrency:\n", "\npermissions:")
        self.assertIn(
            "${{ github.event.pull_request.number || github.run_id }}",
            concurrency,
        )
        self.assertIn(
            "cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
            concurrency,
        )
        self.assertNotIn("github.ref", concurrency)

    def test_required_test_jobs_checkout_the_event_sha(self) -> None:
        required_jobs = (
            ("vet", "test"),
            ("test", "race"),
            ("race", "perf"),
            ("perf", "snapshot-iterator-perf"),
            ("snapshot-iterator-perf", "required"),
        )
        for job, next_job in required_jobs:
            with self.subTest(job=job):
                job_source = block(
                    self.source,
                    f"  {job}:\n",
                    f"\n  {next_job}:\n",
                )
                checkout = block(
                    job_source,
                    "      - name: Checkout\n",
                    "\n      - name: Setup Go\n",
                )
                self.assertIn("uses: actions/checkout@v4", checkout)
                self.assertNotIn("ref:", checkout)

    def test_mvcc_runner_override_is_opt_in_and_exact_same_repository_pr_only(self) -> None:
        mvcc = block(self.source, "  mvcc-raw-path-gate:\n", "\n  test:\n")
        match = re.search(r"(?m)^    runs-on: \$\{\{ (.+) \}\}$", mvcc)
        self.assertIsNotNone(match)
        expression = match.group(1)
        self.assertEqual(
            expression,
            "github.event_name == 'pull_request' && "
            "github.event.pull_request.head.repo.full_name == github.repository && "
            "vars.TREEDB_PERF_RUNNER_LABEL != '' && "
            "vars.TREEDB_PERF_RUNNER_PR != '' && "
            "vars.TREEDB_PERF_RUNNER_PR == format('{0}', github.event.pull_request.number) && "
            "vars.TREEDB_PERF_RUNNER_LABEL || 'ubuntu-24.04'",
        )
        self.assertEqual(self.source.count("vars.TREEDB_PERF_RUNNER_LABEL"), 2)
        self.assertEqual(self.source.count("vars.TREEDB_PERF_RUNNER_PR"), 2)

    def test_m0_runner_override_is_exact_pr_only_with_a_distinct_job_label(self) -> None:
        match = re.search(r"(?m)^    runs-on: \$\{\{ (.+) \}\}$", self.m0)
        self.assertIsNotNone(match)
        expression = match.group(1)
        self.assertEqual(
            expression,
            "github.event_name == 'pull_request' && "
            "github.event.pull_request.head.repo.full_name == github.repository && "
            "vars.TREEDB_PERF_RUNNER_LABEL != '' && "
            "vars.TREEDB_PERF_RUNNER_PR != '' && "
            "vars.TREEDB_PERF_RUNNER_PR == format('{0}', github.event.pull_request.number) && "
            "format('{0}-m0', vars.TREEDB_PERF_RUNNER_LABEL) || 'ubuntu-24.04'",
        )
        self.assertEqual(self.m0.count("vars.TREEDB_PERF_RUNNER_LABEL"), 2)
        self.assertEqual(self.m0.count("vars.TREEDB_PERF_RUNNER_PR"), 2)

    def test_m0_routing_preserves_original_enforced_capture(self) -> None:
        self.assertIn("    timeout-minutes: 20\n", self.m0)
        self.assertNotIn("continue-on-error", self.m0)
        self.assertNotIn("fromJSON", self.m0)
        self.assertNotIn("inputs:", self.m0)
        self.assertIn("ref: ${{ github.event.pull_request.head.sha || github.sha }}", self.m0)
        self.assertIn(
            'run: RUN_DIR="$RUNNER_TEMP/treedb-vacuum-m0" scripts/treedb_vacuum_m0_capture.sh',
            self.m0,
        )
        self.assertIn("      - name: Upload complete evidence packet\n        if: always()", self.m0)
        self.assertIn("          if-no-files-found: error", self.m0)

    def test_mvcc_attribution_uses_pull_request_base_and_head(self) -> None:
        mvcc = block(self.source, "  mvcc-raw-path-gate:\n", "\n  test:\n")
        self.assertIn("EVENT_NAME: ${{ github.event_name }}", mvcc)
        self.assertIn("PR_NUMBER: ${{ github.event.pull_request.number || '' }}", mvcc)
        self.assertIn("BASE_SHA: ${{ github.event.pull_request.base.sha || '' }}", mvcc)
        self.assertIn("HEAD_SHA: ${{ github.event.pull_request.head.sha || github.sha }}", mvcc)
        self.assertIn("ref: ${{ steps.scope.outputs.head_sha }}", mvcc)
        self.assertIn('if [[ "$EVENT_NAME" != "pull_request" ]]', mvcc)
        self.assertIn('git diff --name-only "$BASE_SHA...$HEAD_SHA"', mvcc)
        self.assertNotIn('/commits/${HEAD_SHA}/pulls', mvcc)
        self.assertNotIn('/commits/${head_sha}/pulls', mvcc)
        self.assertNotIn('/pulls/${PR_NUMBER}/files', mvcc)
        self.assertNotIn('/pulls/${pr_number}/files', mvcc)

    def test_mvcc_observation_only_label_does_not_change_attribution(self) -> None:
        mvcc = block(self.source, "  mvcc-raw-path-gate:\n", "\n  test:\n")
        self.assertIn('/pulls/${PR_NUMBER}', mvcc)
        self.assertIn('performance-observation-only', mvcc)
        self.assertIn(
            "continue-on-error: ${{ steps.scope.outputs.enforce != 'true' }}",
            mvcc,
        )
        self.assertIn("if: steps.paths.outputs.run == 'true'", mvcc)

    def test_required_gate_aggregates_every_tree_job(self) -> None:
        required = block(self.source, "  required:\n")
        self.assertIn("name: TreeDB required gate", required)
        self.assertIn("if: ${{ always() }}", required)
        for job in (
            "mvcc-raw-path-gate",
            "getversioned-bindings-qualification",
            "getversioned-timing-acceptance",
            "vet",
            "test",
            "race",
            "perf",
            "snapshot-iterator-perf",
        ):
            self.assertRegex(required, rf"(?m)^      - {re.escape(job)}\s*$")
            self.assertIn(f"needs.{job}.result", required)
        self.assertIn('[[ "$result" == "success" ]]', required)

        bindings = dict(re.findall(r"(?m)^          ([A-Z_]+): \$\{\{ needs\.([\w-]+)\.result \}\}$", required))
        expected = {"mvcc-raw-path-gate", "getversioned-bindings-qualification", "getversioned-timing-acceptance", "vet", "test", "race", "perf", "snapshot-iterator-perf"}
        self.assertEqual(set(bindings.values()), expected)
        self.assertEqual(len(bindings), len(expected))
        script = textwrap.dedent(required.split("        run: |\n", 1)[1])
        env = {name: "success" for name in bindings}
        def run(values: dict[str, str]) -> int:
            return subprocess.run(["bash", "-c", script], env=values, capture_output=True, check=False).returncode
        self.assertEqual(run(env), 0)
        for name in bindings:
            for bad in ("failure", "cancelled", "skipped", ""):
                values = dict(env)
                values[name] = bad
                with self.subTest(name=name, result=bad):
                    self.assertNotEqual(run(values), 0)


    def test_scoped_timing_route_preserves_normal_comparisons(self) -> None:
        mvcc = block(self.source, "  mvcc-raw-path-gate:\n", "\n  vet:\n")
        raw = block(mvcc, "      - name: Compare exact base and head\n", "      - name: Compare public PRIMARY")
        primary = block(mvcc, "      - name: Compare public PRIMARY", "      - name: Authenticate original evidence")
        for branch in (raw, primary):
            self.assertIn("steps.acceptance_scope.outputs.scoped != 'true'", branch)
        self.assertIn('MAX_REGRESSION_PERCENT: "5"', raw)
        self.assertIn('RUNS: "8"', raw)
        self.assertIn('./scripts/mvcc_raw_path_gate.sh', raw)
        self.assertIn('python3 scripts/mvcc_primary_cost_gate.py', primary)
        preparation = block(mvcc, "      - name: Authenticate original evidence", "      - name: Compare frozen COW")
        self.assertNotIn("continue-on-error", preparation)
        self.assertIn("getversioned_scoped_acceptance.py prepare", preparation)
        self.assertNotIn("-test.bench", preparation)
        self.assertNotIn("checks: write", mvcc)
        self.assertNotIn("statuses: write", mvcc)

    def test_scoped_timing_cannot_accept_non_timing_failure(self) -> None:
        job = block(self.source, "  getversioned-timing-acceptance:\n", "\n  required:\n")
        self.assertIn("if: ${{ always() }}", job)
        self.assertNotIn("continue-on-error", job)
        for needed in ("mvcc-raw-path-gate", "getversioned-bindings-qualification", "vet", "test", "race", "perf", "snapshot-iterator-perf"):
            self.assertIn("      - " + needed + "\n", job)
        step = block(job, "      - name: Require original non-timing", "      - name: Upload distinct")
        bindings = dict(re.findall(r"(?m)^          ([A-Z_]+): \$\{\{ needs\.([\w-]+)\.result \}\}$", step))
        self.assertEqual(len(bindings), 7)
        self.assertIn("test", bindings.values())  # Includes Windows core-6.
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            calls = directory / "called.txt"
            stub = directory / "python3"
            stub.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$CALLS\"\nprintf '{}\\n' > \"$RUNNER_TEMP/getversioned-acceptance-decision.json\"\n")
            stub.chmod(0o755)
            success = {name: "success" for name in bindings}
            success.update(SCOPED_ACCEPTANCE="true", PATH=temporary + os.pathsep + os.defpath,
                           CALLS=str(calls), GITHUB_EVENT_PATH="event.json", RUNNER_TEMP=temporary,
                           GITHUB_STEP_SUMMARY=str(directory / "summary.md"))
            def run(values: dict[str, str]) -> tuple[int, bool]:
                calls.unlink(missing_ok=True)
                result = subprocess.run(["bash", "-c", script], env=values, capture_output=True, check=False)
                return result.returncode, calls.exists()
            self.assertEqual(run(success), (0, True))
            self.assertIn("accept\n--event\nevent.json", calls.read_text())
            for name in bindings:
                for bad in ("failure", "cancelled", "skipped", ""):
                    values = dict(success, **{name: bad})
                    with self.subTest(name=name, value=bad):
                        code, invoked = run(values)
                        self.assertNotEqual(code, 0)
                        self.assertFalse(invoked)
            for unrelated in ("false", ""):
                self.assertEqual(run(dict(success, SCOPED_ACCEPTANCE=unrelated)), (0, False))


if __name__ == "__main__":
    unittest.main()
