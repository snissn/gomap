#!/usr/bin/env python3
"""Contract checks for bounded TreeDB Windows core-shard headroom."""

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


if __name__ == "__main__":
    unittest.main()
