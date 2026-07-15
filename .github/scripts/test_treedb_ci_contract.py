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

    def test_normal_tests_checkout_the_event_sha(self) -> None:
        test_job = block(self.source, "  test:\n", "\n  race:\n")
        checkout = block(test_job, "      - name: Checkout\n", "\n      - name: Setup Go\n")
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
        self.assertNotIn('/pulls/${PR_NUMBER}', mvcc)

    def test_required_gate_aggregates_every_tree_job(self) -> None:
        required = block(self.source, "  required:\n")
        self.assertIn("name: TreeDB required gate", required)
        self.assertIn("if: ${{ always() }}", required)
        for job in (
            "mvcc-raw-path-gate",
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
