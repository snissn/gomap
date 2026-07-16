#!/usr/bin/env python3
"""Contract checks for bounded TreeDB CI timeout headroom."""

from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "treedb-tests.yml"


def matrix_timeout(workflow: str, job_name: str) -> int:
    match = re.search(
        rf"- name: {re.escape(job_name)}\s+"
        r"os: windows-latest\s+"
        r"timeout: (?P<timeout>\d+)",
        workflow,
    )
    if match is None:
        raise AssertionError(f"missing Windows matrix entry for {job_name}")
    return int(match.group("timeout"))


def workflow_job(workflow: str, job_name: str) -> str:
    match = re.search(
        rf"^  {re.escape(job_name)}:\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"missing workflow job {job_name}")
    return match.group("body")


class TreeDBWindowsCoreHeadroomTest(unittest.TestCase):
    def test_core_shards_have_equal_bounded_thirty_minute_caps(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")

        # Hosted evidence reached 24m48s after every selected test passed. A
        # 30-minute cap preserves the bounded job while restoring about 20%
        # wall-clock headroom on the slow observation.
        self.assertEqual(matrix_timeout(workflow, "windows-core-1"), 30)
        self.assertEqual(matrix_timeout(workflow, "windows-core-2"), 30)
        self.assertIn("timeout-minutes: ${{ matrix.timeout }}", workflow)

    def test_caching_shards_keep_their_existing_bounded_caps(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")

        for job_name in (
            "windows-caching-heavy-1",
            "windows-caching-heavy-2",
            "windows-caching-rest-1",
            "windows-caching-rest-2",
            "windows-caching-rest-3",
        ):
            with self.subTest(job_name=job_name):
                self.assertEqual(matrix_timeout(workflow, job_name), 25)


class TreeDBLinuxRaceHeadroomTest(unittest.TestCase):
    def test_db_package_timeout_is_bounded_inside_the_job_cap(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        race_job = workflow_job(workflow, "race")

        outer_match = re.search(r"^    timeout-minutes: (?P<timeout>\d+)$", race_job, re.MULTILINE)
        self.assertIsNotNone(outer_match, "race job must keep a bounded outer timeout")
        outer_minutes = int(outer_match.group("timeout"))

        command_match = re.search(
            r"^(?P<command>\s*GOMEMLIMIT=.*go test -json -race -p 1 "
            r"-timeout (?P<timeout>\d+)m ./db -run \"\$db_regex\".*)$",
            race_job,
            re.MULTILINE,
        )
        self.assertIsNotNone(
            command_match,
            "TreeDB/db race invocation must keep coverage and an explicit package timeout",
        )
        inner_minutes = int(command_match.group("timeout"))

        # The failed hosted run spent about 11m15s before TreeDB/db and then
        # exhausted Go's 10-minute default. Twelve minutes restores 20%
        # package headroom while preserving time for fail-closed artifacts
        # under the existing 25-minute job cap.
        self.assertEqual(outer_minutes, 25)
        self.assertEqual(inner_minutes, 12)
        self.assertLess(inner_minutes, outer_minutes)


if __name__ == "__main__":
    unittest.main()
