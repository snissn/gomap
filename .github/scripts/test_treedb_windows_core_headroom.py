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


def core_matrix_partition(workflow: str, job_name: str) -> tuple[int, int]:
    match = re.search(
        rf"- name: {re.escape(job_name)}\s+"
        r"os: windows-latest\s+"
        r"timeout: \d+\s+"
        r"shard_kind: windows-core\s+"
        r"package_shard_index: (?P<index>\d+)\s+"
        r"package_shard_count: (?P<count>\d+)",
        workflow,
    )
    if match is None:
        raise AssertionError(f"missing Windows core partition for {job_name}")
    return int(match.group("index")), int(match.group("count"))


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
    def test_core_shards_use_equal_bounded_five_way_partition(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")

        # Five deterministic shards twice exhausted 30 minutes with healthy
        # tests incomplete. Keep bounded hosted-runner variability headroom.
        self.assertEqual(
            len(
                re.findall(
                    r"^          - name: windows-core-\d+$",
                    workflow,
                    re.MULTILINE,
                )
            ),
            5,
        )
        for shard in range(5):
            job_name = f"windows-core-{shard + 1}"
            with self.subTest(job_name=job_name):
                self.assertEqual(matrix_timeout(workflow, job_name), 40)
                self.assertEqual(core_matrix_partition(workflow, job_name), (shard, 5))
        self.assertIn("timeout-minutes: ${{ matrix.timeout }}", workflow)

    def test_core_selection_has_no_two_way_pin_complement_special_case(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        test_job = workflow_job(workflow, "test")
        core_case = re.search(
            r"^            windows-core\)\n(?P<body>.*?)(?=^              ;;$)",
            test_job,
            re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(core_case, "missing windows-core shell case")
        body = core_case.group("body")

        self.assertNotIn('if [ "$package_shard_count" -eq 2 ]', body)
        self.assertNotIn("weighted_shard0_tests.txt", body)
        for tests_file in ("package_file", "root_all_file", "db_all_file"):
            with self.subTest(tests_file=tests_file):
                self.assertRegex(
                    body,
                    rf"awk -v idx=\"\$package_shard_index\" -v n=\"\$package_shard_count\" .* "
                    rf'\"\${tests_file}\"',
                )

    def test_collections_runnables_are_split_across_core_shards(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        test_job = workflow_job(workflow, "test")
        core_case = re.search(
            r"^            windows-core\)\n(?P<body>.*?)(?=^              ;;$)",
            test_job,
            re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(core_case, "missing windows-core shell case")
        body = core_case.group("body")

        self.assertIn(
            "grep -v '^github.com/snissn/gomap/TreeDB/collections$'",
            body,
            "collections must not remain an indivisible package-modulo assignment",
        )
        self.assertIn(
            r"go test ./collections -list '^(Test|Example|Fuzz).*' | tr -d '\r' | "
            r"grep -E '^(Test|Example|Fuzz)' > "
            '"$collections_all_file"',
            body,
            "collections enumeration must retain ordinary fuzz-seed coverage",
        )
        self.assertRegex(
            body,
            r'awk -v idx="\$package_shard_index" -v n="\$package_shard_count" .* '
            r'"\$collections_all_file" > "\$collections_test_file"',
        )
        self.assertIn(
            'run_named_test_file ./collections "TreeDB/collections test '
            'shard=${package_shard_index}/${package_shard_count}" '
            '"$collections_test_file" treedb-test.jsonl',
            body,
        )

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


class TreeDBLinuxAllPackagesHeadroomTest(unittest.TestCase):
    def test_ubuntu_keeps_complete_serial_coverage_inside_a_bounded_cap(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        matrix_match = re.search(
            r"- name: ubuntu-latest\s+"
            r"os: ubuntu-latest\s+"
            r"timeout: (?P<timeout>\d+)\s+"
            r"shard_kind: (?P<shard_kind>\S+)\s+"
            r"run_vet: (?P<run_vet>\S+)",
            workflow,
        )
        self.assertIsNotNone(matrix_match, "missing Ubuntu all-package matrix entry")

        # The failed hosted run consumed about 14m13s in vet plus the four
        # dominant packages before the fixed 15-minute cap canceled it. Keep
        # a bounded cap with meaningful runner-variability and artifact margin.
        self.assertEqual(int(matrix_match.group("timeout")), 25)
        self.assertEqual(matrix_match.group("shard_kind"), "all")
        self.assertEqual(matrix_match.group("run_vet"), "true")

        test_job = workflow_job(workflow, "test")
        self.assertIn("GOMAXPROCS: 2", test_job)
        self.assertIn(
            "go test -json -timeout 30m -p 1 ./... | tee treedb-test.jsonl",
            test_job,
        )


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
