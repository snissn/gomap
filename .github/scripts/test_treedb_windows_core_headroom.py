#!/usr/bin/env python3
"""Contract checks for complete, bounded, weighted TreeDB CI sharding."""

from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "treedb-tests.yml"
CI_DIR = REPO_ROOT / ".github" / "ci"


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


def shell_case(job: str, name: str) -> str:
    match = re.search(
        rf"^            {re.escape(name)}\)\n(?P<body>.*?)(?=^              ;;$)",
        job,
        re.MULTILINE | re.DOTALL,
    )
    if match is None:
        raise AssertionError(f"missing {name} shell case")
    return match.group("body")


def read_weights(
    filename: str, shard_count: int, allowed_kinds: set[str]
) -> dict[tuple[str, str], int]:
    pins: dict[tuple[str, str], int] = {}
    for line_number, raw in enumerate(
        (CI_DIR / filename).read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not raw or raw.startswith("#"):
            continue
        fields = raw.split("\t")
        if len(fields) != 4:
            raise AssertionError(f"{filename}:{line_number}: expected four TSV fields")
        kind, shard_text, item, seconds_text = fields
        if kind not in allowed_kinds:
            raise AssertionError(f"{filename}:{line_number}: unexpected kind {kind}")
        shard = int(shard_text)
        if not 0 <= shard < shard_count:
            raise AssertionError(f"{filename}:{line_number}: invalid shard {shard}")
        if float(seconds_text) <= 0:
            raise AssertionError(f"{filename}:{line_number}: non-positive timing")
        key = (kind, item)
        if key in pins:
            raise AssertionError(f"{filename}:{line_number}: duplicate pin {key}")
        pins[key] = shard
    if not pins:
        raise AssertionError(f"{filename}: no weighted pins")
    return pins


def weighted_shards(
    items: list[str], kind: str, shard_count: int, pins: dict[tuple[str, str], int]
) -> list[list[str]]:
    shards = [[] for _ in range(shard_count)]
    fallback = 0
    for item in items:
        shard = pins.get((kind, item))
        if shard is None:
            shard = fallback % shard_count
            fallback += 1
        shards[shard].append(item)
    return shards


class TreeDBWeightedManifestTest(unittest.TestCase):
    def test_manifests_are_valid_and_partition_pins_and_new_items_once(self) -> None:
        manifests = (
            (
                "treedb_windows_core_weighted_shards.tsv",
                8,
                {"package", "root", "db", "collections"},
            ),
            (
                "treedb_race_weighted_shards.tsv",
                3,
                {"package", "root", "caching", "db"},
            ),
            ("treedb_unix_weighted_shards.tsv", 3, {"package"}),
        )
        for filename, count, kinds in manifests:
            with self.subTest(filename=filename):
                pins = read_weights(filename, count, kinds)
                for kind in kinds:
                    domain = [item for (item_kind, item) in pins if item_kind == kind]
                    domain.extend(("new-item-a", "new-item-b", "new-item-c"))
                    shards = weighted_shards(domain, kind, count, pins)
                    flattened = [item for shard in shards for item in shard]
                    self.assertEqual(len(flattened), len(domain))
                    self.assertEqual(set(flattened), set(domain))

    def test_workflow_uses_one_complete_weighted_selector_per_test_job(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(workflow.count("weighted_shard_file() {"), 2)
        self.assertIn("NR == FNR {", workflow)
        self.assertIn("pinned[$3] = $2", workflow)
        self.assertIn("(fallback % n) == idx { print }", workflow)


class TreeDBWindowsCoreHeadroomTest(unittest.TestCase):
    def test_core_shards_use_bounded_weighted_eight_way_partition(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(
            len(re.findall(r"^          - name: windows-core-\d+$", workflow, re.MULTILINE)),
            8,
        )
        for shard in range(8):
            job_name = f"windows-core-{shard + 1}"
            with self.subTest(job_name=job_name):
                self.assertEqual(matrix_timeout(workflow, job_name), 40)
                self.assertEqual(core_matrix_partition(workflow, job_name), (shard, 8))

    def test_core_selector_weights_every_split_domain(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        body = shell_case(workflow_job(workflow, "test"), "windows-core")
        self.assertIn("treedb_windows_core_weighted_shards.tsv", body)
        for kind in ("package", "root", "db", "collections"):
            with self.subTest(kind=kind):
                self.assertIn(f'weighted_shard_file {kind} "$package_shard_index"', body)

    def test_collections_runnables_remain_individually_sharded(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        body = shell_case(workflow_job(workflow, "test"), "windows-core")
        self.assertIn("grep -v '^github.com/snissn/gomap/TreeDB/collections$'", body)
        self.assertIn(
            r"go test ./collections -list '^(Test|Example|Fuzz).*' | tr -d '\r' | "
            r"grep -E '^(Test|Example|Fuzz)' > "
            '"$collections_all_file"',
            body,
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


class TreeDBUnixHeadroomTest(unittest.TestCase):
    def test_ubuntu_and_macos_use_three_weighted_test_shards(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        for os_name, timeout in (("ubuntu", 25), ("macos", 30)):
            for shard in range(3):
                name = f"{os_name}-latest-{shard + 1}"
                match = re.search(
                    rf"- name: {re.escape(name)}\s+"
                    rf"os: {os_name}-latest\s+"
                    rf"timeout: {timeout}\s+"
                    r"shard_kind: all\s+"
                    rf"package_shard_index: {shard}\s+"
                    r"package_shard_count: 3",
                    workflow,
                )
                self.assertIsNotNone(match, f"missing weighted Unix shard {name}")
        all_case = shell_case(workflow_job(workflow, "test"), "all")
        self.assertIn("treedb_unix_weighted_shards.tsv", all_case)
        self.assertIn("xargs go test -json -timeout 30m -p 1", all_case)

    def test_vet_is_a_separate_three_os_matrix_and_required_gate_dependency(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        vet_job = workflow_job(workflow, "vet")
        test_job = workflow_job(workflow, "test")
        self.assertEqual(vet_job.count("            os: "), 3)
        for os_name in ("ubuntu-latest", "macos-latest", "windows-latest"):
            self.assertIn(f"            os: {os_name}", vet_job)
        self.assertIn("run: go vet ./...", vet_job)
        self.assertNotIn("      - name: Vet", test_job)
        required = workflow_job(workflow, "required")
        self.assertIn("      - vet", required)
        self.assertIn("needs.vet.result", required)


class TreeDBLinuxRaceHeadroomTest(unittest.TestCase):
    def test_three_weighted_core_shards_and_dedicated_collections_shard_are_bounded(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        race_job = workflow_job(workflow, "race")
        matrix = re.findall(
            r"- shard: (\d+)\s+shard_kind: ([a-z-]+)\s+shard_index: (\d+)\s+shard_count: (\d+)",
            race_job,
        )
        self.assertEqual(
            matrix,
            [
                ("1", "core", "0", "3"),
                ("2", "core", "1", "3"),
                ("3", "core", "2", "3"),
                ("4", "collections", "0", "1"),
            ],
        )
        self.assertIn("treedb_race_weighted_shards.tsv", race_job)
        self.assertIn("grep -v '^github.com/snissn/gomap/TreeDB/collections$'", race_job)
        self.assertIn(
            "go test -json -race -p 1 -timeout 12m ./collections",
            race_job,
        )
        for kind in ("package", "root", "caching", "db"):
            self.assertIn(f'weighted_shard_file {kind} "$shard_index"', race_job)

        outer_match = re.search(r"^    timeout-minutes: (?P<timeout>\d+)$", race_job, re.MULTILINE)
        self.assertIsNotNone(outer_match, "race job must keep a bounded outer timeout")
        outer_minutes = int(outer_match.group("timeout"))
        self.assertEqual(outer_minutes, 40)
        for command in (
            "xargs go test -json -race -p 1 -timeout 12m",
            'go test -json -race -p 1 -timeout 12m . -run "$root_regex"',
            'go test -json -race -p 1 -timeout 12m ./caching -run "$caching_regex"',
            'go test -json -race -p 1 -timeout 12m ./db -run "$db_regex"',
        ):
            with self.subTest(command=command):
                self.assertIn(command, race_job)
        self.assertLess(12, outer_minutes)


if __name__ == "__main__":
    unittest.main()
