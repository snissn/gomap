#!/usr/bin/env python3
"""Contract & routing tests for the 3-tier pre-alpha CI architecture."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
sys.path.insert(0, str(REPO_ROOT / ".github" / "scripts"))

import ci_full_windows_shard  # noqa: E402
import ci_scope  # noqa: E402


SAMPLE_PKG_TO_DIR = {
    "github.com/snissn/gomap/HashDB": "HashDB",
    "github.com/snissn/gomap/TreeDB": "TreeDB",
    "github.com/snissn/gomap/TreeDB/caching": "TreeDB/caching",
    "github.com/snissn/gomap/TreeDB/collections": "TreeDB/collections",
    "github.com/snissn/gomap/TreeDB/db": "TreeDB/db",
    "github.com/snissn/gomap/TreeDB/docs": "TreeDB/docs",
    "github.com/snissn/gomap/TreeDB/internal/atomicfile": "TreeDB/internal/atomicfile",
    "github.com/snissn/gomap/TreeDB/internal/raftcluster": "TreeDB/internal/raftcluster",
    "github.com/snissn/gomap/TreeDB/internal/raftplacement": "TreeDB/internal/raftplacement",
    "github.com/snissn/gomap/TreeDB/internal/stableio": "TreeDB/internal/stableio",
    "github.com/snissn/gomap/TreeDB/mongo_gateway": "TreeDB/mongo_gateway",
    "github.com/snissn/gomap/TreeDB/nativewire": "TreeDB/nativewire",
    "github.com/snissn/gomap/TreeDB/pager": "TreeDB/pager",
    "github.com/snissn/gomap/cmd/mongo_gateway_bench": "cmd/mongo_gateway_bench",
    "github.com/snissn/gomap/cmd/treedb_vector_partition_bench": "cmd/treedb_vector_partition_bench",
    "github.com/snissn/gomap/cmd/unified_bench": "cmd/unified_bench",
}

SAMPLE_PKG_TO_IMPORTS = {
    "github.com/snissn/gomap/HashDB": [],
    "github.com/snissn/gomap/TreeDB/internal/atomicfile": [],
    "github.com/snissn/gomap/TreeDB/internal/stableio": [],
    "github.com/snissn/gomap/TreeDB/pager": ["github.com/snissn/gomap/TreeDB/internal/stableio"],
    "github.com/snissn/gomap/TreeDB/db": ["github.com/snissn/gomap/TreeDB/pager"],
    "github.com/snissn/gomap/TreeDB/caching": ["github.com/snissn/gomap/TreeDB/db"],
    "github.com/snissn/gomap/TreeDB": ["github.com/snissn/gomap/TreeDB/caching", "github.com/snissn/gomap/TreeDB/db"],
    "github.com/snissn/gomap/TreeDB/collections": ["github.com/snissn/gomap/TreeDB/db", "github.com/snissn/gomap/TreeDB"],
    "github.com/snissn/gomap/TreeDB/mongo_gateway": ["github.com/snissn/gomap/TreeDB/collections"],
    "github.com/snissn/gomap/cmd/mongo_gateway_bench": ["github.com/snissn/gomap/TreeDB/mongo_gateway"],
    "github.com/snissn/gomap/TreeDB/internal/raftcluster": ["github.com/snissn/gomap/TreeDB"],
    "github.com/snissn/gomap/TreeDB/internal/raftplacement": ["github.com/snissn/gomap/TreeDB/internal/raftcluster"],
    "github.com/snissn/gomap/TreeDB/nativewire": ["github.com/snissn/gomap/TreeDB/collections", "github.com/snissn/gomap/TreeDB/internal/raftplacement"],
    "github.com/snissn/gomap/cmd/treedb_vector_partition_bench": ["github.com/snissn/gomap/TreeDB/collections"],
    "github.com/snissn/gomap/cmd/unified_bench": ["github.com/snissn/gomap/TreeDB", "github.com/snissn/gomap/HashDB"],
    "github.com/snissn/gomap/TreeDB/docs": [],
}


def load_yaml(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    # PyYAML parses unquoted 'on:' key as boolean True under YAML 1.1
    if True in data and "on" not in data:
        data["on"] = data.pop(True)
    return data


class TestWorkflowYAMLContracts(unittest.TestCase):
    def test_all_workflows_are_valid_yaml_and_only_tier_workflows_auto_trigger(self) -> None:
        workflow_files = sorted(WORKFLOWS.glob("*.yml"))
        self.assertGreaterEqual(len(workflow_files), 10)
        retired = {
            "format.yml",
            "hashdb-tests.yml",
            "root-tests.yml",
            "treedb-tests.yml",
            "unified-bench-tests.yml",
        }
        for wf in workflow_files:
            self.assertNotIn(wf.name, retired, f"Superseded workflow {wf.name} must not remain")
            doc = load_yaml(wf)
            self.assertIsInstance(doc, dict, wf.name)
            triggers = doc.get("on", {})
            if isinstance(triggers, list):
                trigger_keys = set(triggers)
            elif isinstance(triggers, dict):
                trigger_keys = set(triggers.keys())
            else:
                trigger_keys = {triggers}
            if wf.name == "ci.yml":
                self.assertEqual(trigger_keys, {"pull_request", "push", "merge_group", "workflow_dispatch"})
                pr_types = triggers["pull_request"]["types"]
                self.assertNotIn("edited", pr_types, "PR body edits must not trigger CI")
                self.assertIn("ready_for_review", pr_types)
                self.assertEqual(triggers["push"]["branches"], ["main"])
            elif wf.name == "ci-full-sweep.yml":
                self.assertEqual(trigger_keys, {"schedule", "workflow_call", "workflow_dispatch"})
                self.assertNotIn("push", trigger_keys)
                self.assertNotIn("pull_request", trigger_keys)
            else:
                self.assertEqual(
                    trigger_keys,
                    {"workflow_dispatch"},
                    f"Legacy/manual workflow {wf.name} must be workflow_dispatch-only, got {trigger_keys}",
                )

    def test_ci_yml_guardrails_and_caching(self) -> None:
        raw = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
        doc = load_yaml(WORKFLOWS / "ci.yml")
        # Tier 0 runs docs-check, TreeDB/docs, and TestTypedStorage*
        docs_job = doc["jobs"]["docs-checks"]
        docs_script = "\n".join(str(s.get("run", "")) for s in docs_job["steps"])
        self.assertIn("make docs-check", docs_script)
        self.assertIn("go test -count=1 ./TreeDB/docs", docs_script)
        self.assertIn("TestTypedStorage", docs_script)
        # No setup-go cache: false anywhere in ci.yml
        self.assertNotIn("cache: false", raw)
        # Single fail-closed aggregator includes opted-in full-sweep
        self.assertIn("full-sweep", doc["jobs"])
        self.assertEqual(doc["jobs"]["full-sweep"]["uses"], "./.github/workflows/ci-full-sweep.yml")
        self.assertIn("ci-gate", doc["jobs"])
        self.assertEqual(doc["jobs"]["ci-gate"]["name"], "CI Gate")
        self.assertIn("full-sweep", doc["jobs"]["ci-gate"]["needs"])
        # Producer artifact identity is exported by preflight so selective reruns succeed
        self.assertIn("plan_artifact", doc["jobs"]["preflight"]["outputs"])
        download_steps = [
            step
            for job in doc["jobs"].values()
            if isinstance(job, dict)
            for step in job.get("steps", [])
            if str(step.get("uses", "")).startswith("actions/download-artifact@")
        ]
        self.assertEqual(len(download_steps), 5)
        for step in download_steps:
            self.assertEqual(step["with"]["name"], "${{ needs.preflight.outputs.plan_artifact }}")
        # Cross-compile steps and retained script suites exist
        vet_script = "\n".join(str(s.get("run", "")) for s in doc["jobs"]["vet-and-cross-compile"]["steps"])
        self.assertIn("--kind cross-darwin", vet_script)
        self.assertIn("--kind cross-windows", vet_script)
        self.assertIn("treedb_peer_*test.py", vet_script)
        self.assertIn("treedb_vacuum_m0_summarize_test.py", vet_script)

    def test_ci_full_sweep_guardrails_and_7_windows_shards(self) -> None:
        raw = (WORKFLOWS / "ci-full-sweep.yml").read_text(encoding="utf-8")
        doc = load_yaml(WORKFLOWS / "ci-full-sweep.yml")
        self.assertNotIn("cache: false", raw)
        self.assertNotIn("go vet -p=1 -x", raw)
        self.assertNotIn("windows-caching-heavy-1", raw)
        self.assertEqual(doc["jobs"]["treedb-windows"]["strategy"]["matrix"]["shard"], [1, 2, 3, 4, 5, 6, 7])
        # Schedule guard checks last passing scheduled SHA
        guard_run = "\n".join(str(s.get("run", "")) for s in doc["jobs"]["guard"]["steps"])
        self.assertIn("event=schedule", guard_run)
        self.assertIn("should_run=false", guard_run)
        # Snapshot iterator retains summary publication and artifact upload on always()
        snap_steps = doc["jobs"]["snapshot-iterator-perf"]["steps"]
        snap_names = [s.get("name", "") for s in snap_steps]
        self.assertIn("Publish snapshot iterator summary", snap_names)
        self.assertIn("Upload snapshot iterator performance evidence", snap_names)
        upload_step = next(s for s in snap_steps if s.get("name") == "Upload snapshot iterator performance evidence")
        self.assertIn("always()", str(upload_step.get("if", "")))
        self.assertEqual(upload_step["with"]["path"], "artifacts/snapshot_iterator_perf/")


class TestCIScopeRouter(unittest.TestCase):
    def _plan(self, **kwargs):
        defaults = dict(
            event_name="pull_request",
            changed_files=[],
            is_draft=False,
            pr_title="test PR",
            pr_labels=[],
            dispatch_mode="",
            pkg_to_dir=SAMPLE_PKG_TO_DIR,
            pkg_to_imports=SAMPLE_PKG_TO_IMPORTS,
        )
        defaults.update(kwargs)
        return ci_scope.build_plan(**defaults)

    def test_tier0_docs_only_pr(self) -> None:
        plan = self._plan(changed_files=["README.md", "docs/benchmarks/ycsb_2026.md", "TreeDB/docs/spec/recovery.md"])
        self.assertEqual(plan["tier"], "docs_only")
        self.assertTrue(plan["docs_only"])
        self.assertTrue(plan["run_docs_check"])
        self.assertEqual(plan["test_packages"], [])
        self.assertEqual(plan["race_packages"], [])
        self.assertFalse(plan["escalate_os"])

    def test_docs_plus_go_bench_is_not_docs_only(self) -> None:
        plan = self._plan(changed_files=["README.md", "TreeDB/collections/vector_usearch_bench_test.go"])
        self.assertEqual(plan["tier"], "surgical")
        self.assertFalse(plan["docs_only"])
        self.assertTrue(plan["run_docs_check"])
        self.assertIn("github.com/snissn/gomap/TreeDB/collections", plan["touched_packages"])

    def test_tier05_draft_pr_preflight_only(self) -> None:
        plan = self._plan(
            changed_files=["TreeDB/db/db.go"],
            is_draft=True,
        )
        self.assertEqual(plan["tier"], "draft")
        self.assertEqual(plan["vet_packages"], ["github.com/snissn/gomap/TreeDB/db"])
        self.assertEqual(plan["test_packages"], [])
        self.assertEqual(plan["race_packages"], [])
        self.assertFalse(plan["escalate_os"])

    def test_draft_pr_with_ci_full_runs_tier1(self) -> None:
        plan = self._plan(
            changed_files=["TreeDB/mongo_gateway/server.go"],
            is_draft=True,
            pr_labels=["ci:full"],
        )
        self.assertEqual(plan["tier"], "surgical")
        self.assertTrue(plan["ci_full"])
        self.assertIn("github.com/snissn/gomap/TreeDB/mongo_gateway", plan["test_packages"])

    def test_surgical_mongo_gateway_is_narrow_no_os_escalation(self) -> None:
        plan = self._plan(changed_files=["TreeDB/mongo_gateway/server.go"])
        self.assertEqual(plan["tier"], "surgical")
        self.assertEqual(plan["touched_packages"], ["github.com/snissn/gomap/TreeDB/mongo_gateway"])
        self.assertEqual(
            plan["test_packages"],
            ["github.com/snissn/gomap/TreeDB/mongo_gateway", "github.com/snissn/gomap/cmd/mongo_gateway_bench"],
        )
        self.assertEqual(plan["race_packages"], ["github.com/snissn/gomap/TreeDB/mongo_gateway"])
        self.assertFalse(plan["escalate_os"])
        self.assertNotIn("github.com/snissn/gomap/HashDB", plan["test_packages"])
        self.assertEqual(plan["windows_cross_packages"], ["github.com/snissn/gomap/TreeDB/mongo_gateway"])

    def test_surgical_db_vets_nativewire_but_skips_indirect_heavy_runtime_suites(self) -> None:
        plan = self._plan(changed_files=["TreeDB/db/db.go"])
        self.assertEqual(plan["tier"], "surgical")
        # Full production cone is vetted & cross-compiled
        self.assertIn("github.com/snissn/gomap/TreeDB/nativewire", plan["vet_packages"])
        self.assertIn("github.com/snissn/gomap/cmd/treedb_vector_partition_bench", plan["vet_packages"])
        # Heavy indirect suites are omitted from surgical runtime tests
        self.assertNotIn("github.com/snissn/gomap/TreeDB/nativewire", plan["test_packages"])
        self.assertNotIn("github.com/snissn/gomap/cmd/treedb_vector_partition_bench", plan["test_packages"])
        self.assertNotIn("github.com/snissn/gomap/cmd/unified_bench", plan["test_packages"])
        # Directly touched & core dependents ARE tested
        self.assertIn("github.com/snissn/gomap/TreeDB/db", plan["test_packages"])
        self.assertIn("github.com/snissn/gomap/TreeDB/caching", plan["test_packages"])
        self.assertIn("github.com/snissn/gomap/TreeDB", plan["test_packages"])
        # Race runs ONLY on directly touched package
        self.assertEqual(plan["race_packages"], ["github.com/snissn/gomap/TreeDB/db"])

    def test_heavy_touched_package_splits_race_across_3_shards(self) -> None:
        plan = self._plan(changed_files=["TreeDB/collections/store.go"])
        self.assertEqual(len(plan["race_shards"]), 3)
        for idx, shard in enumerate(plan["race_shards"]):
            self.assertEqual(shard["split_packages"], ["github.com/snissn/gomap/TreeDB/collections"])
            self.assertEqual(shard["split_index"], idx)
            self.assertEqual(shard["split_count"], 3)

    def test_os_escalation_for_platform_and_durability_paths(self) -> None:
        for path, want_win in (
            ("TreeDB/pager/mmap_windows.go", True),
            ("TreeDB/internal/stableio/sync_file_darwin.go", True),
            ("HashDB/replacefile_windows.go", True),
            ("cmd/unified_bench/hostinfo_darwin.go", False),
            ("TreeDB/nativewire/vector_partition_production_listener_socket_windows_v1.go", True),
            ("TreeDB/nativewire/vector_partition_production_listener_socket_unix_v1.go", True),
            ("TreeDB/nativewire/foo_linux_v2_test.go", True),
            ("TreeDB/db/durable_root_runtime.go", True),
            ("TreeDB/db/root_publication_v1.go", True),
            ("TreeDB/db/native_command_wal_completion.go", True),
            ("TreeDB/db/leaf_generation_manifest.go", True),
            ("TreeDB/db/leaf_manifest_revision_gc.go", True),
            ("TreeDB/db/command_wal_publish.go", True),
            ("TreeDB/internal/atomicfile/atomicfile.go", True),
            ("TreeDB/caching/stable_leaf_resource.go", True),
        ):
            plan = self._plan(changed_files=[path])
            self.assertTrue(plan["escalate_os"], path)
            self.assertTrue(plan["os_macos_packages"], path)
            self.assertEqual(bool(plan["os_windows_packages"]), want_win, path)

        non_durability = self._plan(changed_files=["TreeDB/db/vacuum_collection_publication_guard_test.go"])
        self.assertFalse(non_durability["escalate_os"])

        # Also detect OS build constraints in arbitrarily named .go files.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            pkg_dir = root / "TreeDB" / "mongo_gateway"
            pkg_dir.mkdir(parents=True)
            (pkg_dir / "custom_socket.go").write_text("//go:build windows\n\npackage mongogateway\n", encoding="utf-8")
            plan = self._plan(changed_files=["TreeDB/mongo_gateway/custom_socket.go"], repo=root)
            self.assertTrue(plan["escalate_os"])
            self.assertTrue(plan["os_windows_packages"])

    def test_main_push_is_main_fast_and_fail_closed_on_unknown_diff(self) -> None:
        p_main = self._plan(event_name="push", changed_files=[])
        self.assertEqual(p_main["tier"], "main_fast")
        self.assertEqual(len(p_main["test_shards"]), 4)
        self.assertEqual(p_main["race_shards"], [])

        p_err = self._plan(event_name="pull_request", changed_files=None)
        self.assertEqual(p_err["tier"], "main_fast")

    def test_local_replace_module_edits_fail_closed(self) -> None:
        replace_dirs = ci_scope.discover_local_replace_dirs(REPO_ROOT)
        self.assertIn("third_party/hashicorp_raft_v1.7.3/", replace_dirs)
        plan = self._plan(
            changed_files=["third_party/hashicorp_raft_v1.7.3/raft.go"],
            local_replace_dirs=replace_dirs,
        )
        self.assertEqual(plan["tier"], "main_fast")
        self.assertEqual(len(plan["vet_packages"]), len(SAMPLE_PKG_TO_DIR))
        self.assertEqual(len(plan["test_packages"]), len(SAMPLE_PKG_TO_DIR))
        self.assertTrue(plan["escalate_os"])

        draft_plan = self._plan(
            changed_files=["third_party/hashicorp_raft_v1.7.3/raft.go"],
            is_draft=True,
            local_replace_dirs=replace_dirs,
        )
        self.assertEqual(draft_plan["tier"], "draft")
        self.assertEqual(len(draft_plan["vet_packages"]), len(SAMPLE_PKG_TO_DIR))

    def test_fuzz_seeds_included_in_windows_and_split_race_discovery(self) -> None:
        win_src = (REPO_ROOT / ".github" / "scripts" / "ci_full_windows_shard.py").read_text(encoding="utf-8")
        scope_src = (REPO_ROOT / ".github" / "scripts" / "ci_scope.py").read_text(encoding="utf-8")
        self.assertIn("^(Test|Example|Fuzz).*", win_src)
        self.assertIn('("Test", "Example", "Fuzz")', win_src)
        self.assertIn("^(Test|Example|Fuzz).*", scope_src)
        self.assertIn('("Test", "Example", "Fuzz")', scope_src)

    def test_github_outputs_never_emit_empty_matrix_vectors(self) -> None:
        plan = self._plan(changed_files=["README.md"])
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "out.txt"
            ci_scope.write_github_outputs(plan, out)
            kv = dict(line.split("=", 1) for line in out.read_text().splitlines() if "=" in line)
            self.assertEqual(kv["run_test"], "false")
            self.assertEqual(kv["run_race"], "false")
            self.assertEqual(kv["escalate_os"], "false")
            self.assertTrue(json.loads(kv["test_shards_json"]))
            self.assertTrue(json.loads(kv["race_shards_json"]))
            self.assertTrue(json.loads(kv["os_runners_json"]))


class TestWindowsFullShardPartition(unittest.TestCase):
    def test_partition_tests_is_complete_and_disjoint(self) -> None:
        tests = [
            "TestA",
            "TestB",
            "TestVectorPartitionSourceImportDependencyEncodingCoalescedV2",
            "TestVectorPartitionSourceImportDependencyEncodingContinuousV2",
            "TestVectorPartitionSourceImportDependencyEncodingReopenV2",
            "TestC",
            "TestD",
        ]
        s0 = ci_full_windows_shard.partition_tests(tests, 0, 2, ci_full_windows_shard.COLLECTIONS_PIN_SHARD)
        s1 = ci_full_windows_shard.partition_tests(tests, 1, 2, ci_full_windows_shard.COLLECTIONS_PIN_SHARD)
        self.assertEqual(sorted(s0 + s1), sorted(tests))
        self.assertEqual(set(s0) & set(s1), set())
        self.assertIn("TestVectorPartitionSourceImportDependencyEncodingCoalescedV2", s0)
        self.assertIn("TestVectorPartitionSourceImportDependencyEncodingContinuousV2", s1)


if __name__ == "__main__":
    unittest.main()
