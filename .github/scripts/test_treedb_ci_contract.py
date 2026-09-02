#!/usr/bin/env python3
"""Structural contract tests for the required TreeDB CI merge gate."""

from __future__ import annotations

import re
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
            "vet",
            "test",
            "race",
            "perf",
            "snapshot-iterator-perf",
        ):
            self.assertRegex(required, rf"(?m)^      - {re.escape(job)}\s*$")
            self.assertIn(f"needs.{job}.result", required)
        self.assertIn('[[ "$result" == "success" ]]', required)


if __name__ == "__main__":
    unittest.main()
