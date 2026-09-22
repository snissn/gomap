#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from lexical_common import (
    RESULT_SCHEMA,
    ValidationError,
    consolidate,
    corpus_bytes,
    eligible_result_ids,
    load_manifest,
    manifest_sha256,
    normalize,
    read_corpus,
    reference_results,
    result_digest,
    sha256_bytes,
    tokenize,
    validate_result,
)
import run_lexical_comparison as lexical_runner

HERE = Path(__file__).resolve().parent


class LexicalComparisonTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = load_manifest(HERE / "lexical_manifest.json")
        cls.documents = [dict(zip(("id", "title", "body", "tenant"), line.split("\t"), strict=True)) for line in corpus_bytes(10_000).decode().splitlines()]
        cls.expected = reference_results(cls.manifest, cls.documents)
        cls.eligible = eligible_result_ids(cls.manifest, cls.documents)
        cls.corpus_ids = {doc["id"] for doc in cls.documents}
    def context(self, modified: bool = False) -> dict:
        return {
            "source": {
                "commit": "candidate-commit",
                "tree_oid": "candidate-tree",
                "treedb_subtree_oid": "treedb-tree",
                "harness_subtree_oid": "harness-tree",
                "tracked_diff_sha256": "d" * 64 if modified else sha256_bytes(b""),
                "untracked_sources": [],
                "post_run_reverified": True,
                "vcs_modified": modified,
                "qualification_eligible": not modified,
            },
            "environment_contract": copy.deepcopy(self.manifest["environment"]),
            "detected_address_space_limit": "unlimited",
            "runner_filesystem_device_id": "1",
            "enforced_execution": {"query_concurrency": 1, "engine_process_concurrency": 1, "runtime_cpu_parallelism": 1},
        }


    def artifact(self, engine_id: str = "treedb_text_v2", repetition: int = 1) -> dict:
        cases = []
        for query in self.manifest["queries"]:
            ids = self.expected[query["id"]]
            if engine_id == "treedb_text_v2":
                route_proof = ({"text_index_epoch": 0, "scalar_filter_strategy": "prefilter", "text_candidates": 10, "documents_fetched": 0, "fail_closed": 0} if query["semantic"] == "term_scalar" else {"index_version": "v2", "active_roots": ["lexical"], "postings_scanned": 10, "documents_fetched": 0, "fail_closed": 0})
            else:
                route_proof = {
                    "lucene": {"query_class": "TermQuery", "reader_documents": 10_000},
                    "bleve": {"index_type": "scorch", "query_type": query["semantic"]},
                    "sqlite_fts5": [[0, 0, 0, "SCAN docs_fts VIRTUAL TABLE INDEX 0:M1"]],
                }[engine_id]
            route_name = "text_v2_blockmax_scalar_prefilter" if engine_id == "treedb_text_v2" and query["semantic"] == "term_scalar" else "test_inverted_index"
            cases.append({
                "id": query["id"], "status": "ok", "equivalent": True,
                "samples_nanos": [100] * self.manifest["execution"]["measured_queries_per_case"],
                "result_ids": ids, "result_digest": result_digest(ids),
                "reopen_result_ids": ids, "reopen_result_digest": result_digest(ids),
                "route": {"intended": True, "name": route_name, "fallback": False, "proof": copy.deepcopy(route_proof)},
                "timed_out": False,
            })
        if engine_id in {"bleve", "sqlite_fts5"}:
            for query, case in zip(self.manifest["queries"], cases, strict=True):
                if engine_id == "bleve" and query["semantic"] == "term_scalar":
                    case.clear()
                    case.update({"id": query["id"], "status": "unsupported", "equivalent": False, "unsupported_reason": "non-scoring scalar filter unavailable"})
                    continue
                case["status"] = "directional"
                case["equivalent"] = False
                case["non_equivalent_reason"] = "native scorer differs from pinned BM25F"
        config = {"top_k": 10, "stable_setting": "base", "build_timing_boundary": self.manifest["execution"]["build_timing_boundary"], "stored_source_fields": ["id", "title", "body", "tenant"]}
        if engine_id == "treedb_text_v2":
            config.update(weights={"title": 3, "body": 1}, bm25f={"k1": 1.2, "b": 0.75}, scoring_contract="pinned_bm25f")
        elif engine_id == "lucene":
            config.update(tie_break="score,id", scoring_contract="pinned_bm25f", weighted_field_materialization="title repeated 3x then body for term scoring, including phrase-qualified rows", phrase_filter_fields=["title", "body"], phrase_scoring="weighted terms scored; title/body phrase OR applied as FILTER")
        elif engine_id == "bleve":
            config.update(tie_break="score,id", scoring_contract="native_directional", weighted_field_materialization="title repeated 3x then body for non-phrase native scoring", phrase_fields=["title", "body"], phrase_scoring="native TF-IDF title boost 3, body boost 1")
        else:
            config.update(tie_break="score,id", scoring_contract="native_directional", source_table="docs", fts_content_mode="contentless", generated_weighted_field_storage="FTS index only", weighted_field_materialization="title repeated 3x then body for non-phrase native scoring", phrase_fields=["title", "body"], phrase_scoring="native bm25 title weight 3, body weight 1")
        command = ["python", "-E", "-s", "-B", "sqlite_fts5_bench.py"] if engine_id == "sqlite_fts5" else ["adapter"]
        return {
            "schema_version": RESULT_SCHEMA, "status": "ok",
            "engine": {"id": engine_id, "family": "test", "name": engine_id, "version": "candidate-commit" if engine_id == "treedb_text_v2" else "pinned"},
            "repetition": repetition, "manifest_sha256": manifest_sha256(self.manifest),
            "corpus": {"document_count": 10_000, "sha256": self.manifest["corpus"]["sha256"]},
            "command": command, "versions": {"adapter": "pinned"}, "config": config,
            "environment": {
                "contract": copy.deepcopy(self.manifest["environment"]),
                "filesystem": {"runner_device_id": "1", "corpus_store_id": "1", "index_store_id": "1", "result_store_id": "1", "same_filesystem": True},
                "memory": {"detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": True, "adapter_changed_limit": False},
                "execution": {"query_concurrency": 1, "engine_process_concurrency": 1, "runtime_cpu_parallelism": 1},
            },
            "build": {"elapsed_nanos": 1, "docs_per_second": 1.0, "cpu": {"status": "ok", "value": 1, "unit": "nanoseconds"}, "peak_rss": {"status": "unsupported", "reason": "test runtime has no RSS API"}, "checkpointed": True},
            "storage": {"durable_bytes": 1, "wal_bytes": 0, "transient_bytes": 0},
            "reopen": {"performed": True, "verified": True, "result_digest": result_digest(case.get("reopen_result_digest", "") for case in cases)} | ({"query_connection_reopened": True, "durability_connection_reopened": True} if engine_id == "sqlite_fts5" else {}),
            "cases": cases,
        }

    def test_frozen_corpus_and_normalization_contract(self) -> None:
        payload = corpus_bytes(self.manifest["corpus"]["document_count"])
        self.assertEqual(sha256_bytes(payload), self.manifest["corpus"]["sha256"])
        self.assertEqual(normalize("ＲＥＦＵＮＤ Policy"), "refund policy")
        self.assertEqual(tokenize("Refund—POLICY_42"), ["refund", "policy", "42"])

    def test_manifest_rejects_scoring_drift_from_adapter_contract(self) -> None:
        mutated = copy.deepcopy(self.manifest)
        mutated["analysis"]["bm25f"]["k1"] = 2.0
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text(json.dumps(mutated), encoding="utf-8")
            with self.assertRaisesRegex(ValidationError, "adapter-bound frozen scoring contract"):
                load_manifest(path)

    def test_untracked_source_identity_excludes_only_exact_output_subtree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            out_dir = root / "run"
            out_dir.mkdir()
            source = root / "adapter.py"
            generated = out_dir / "raw.json"
            source.write_text("first", encoding="utf-8")
            generated.write_text("generated", encoding="utf-8")
            listed = b"adapter.py\x00run/raw.json\x00"
            with patch.object(lexical_runner, "ROOT", root), patch.object(lexical_runner, "git_bytes", return_value=listed):
                first = lexical_runner.untracked_source_identity(out_dir)
            self.assertEqual([item["path"] for item in first], ["adapter.py"])
            source.write_text("second", encoding="utf-8")
            with patch.object(lexical_runner, "ROOT", root), patch.object(lexical_runner, "git_bytes", return_value=listed):
                changed = lexical_runner.untracked_source_identity(out_dir)
            self.assertNotEqual(first, changed)
            source.unlink()
            with patch.object(lexical_runner, "ROOT", root), patch.object(lexical_runner, "git_bytes", return_value=b"run/raw.json\x00"):
                removed = lexical_runner.untracked_source_identity(out_dir)
            self.assertEqual(removed, [])

    def test_untracked_source_identity_includes_ignored_build_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ignored = root / "ignored.go"
            ignored.write_text("package ignored", encoding="utf-8")
            with patch.object(lexical_runner, "ROOT", root), patch.object(lexical_runner, "git_bytes", side_effect=[b"", b"ignored.go\x00"]):
                records = lexical_runner.untracked_source_identity(root / "run")
            self.assertEqual([record["path"] for record in records], ["ignored.go"])

    def test_hidden_tracked_paths_rejects_index_flags(self) -> None:
        listed = b"H ordinary.py\x00h assumed.py\x00S skipped.py\x00"
        with patch.object(lexical_runner, "git_bytes", return_value=listed):
            self.assertEqual(lexical_runner.hidden_tracked_paths(), ["assumed.py", "skipped.py"])

    def test_retained_logs_replace_host_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            out_dir = Path(temporary)
            logs = out_dir / "logs"
            logs.mkdir()
            retained = logs / "engine-r1.log"
            retained.write_text("cwd=/private/work/repo\nout=/private/work/run/raw.json\n", encoding="utf-8")
            lexical_runner.sanitize_retained_logs(out_dir, [("/private/work/run", "$RUN"), ("/private/work/repo", "$REPO")])
            self.assertEqual(retained.read_text(encoding="utf-8"), "cwd=$REPO\nout=$RUN/raw.json\n")

    def test_lucene_jvm_execution_is_frozen_not_inherited(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}):
            environment = lexical_runner.benchmark_environment(Path(temporary), self.manifest)
        self.assertEqual(environment["MAVEN_OPTS"], lexical_runner.LUCENE_JVM_EXECUTION)
        self.assertEqual(environment["LEXICAL_JVM_EXECUTION"], lexical_runner.LUCENE_JVM_EXECUTION)
        self.assertEqual(environment["PYTHONDONTWRITEBYTECODE"], "1")
        self.assertNotIn("-Xmx1g", environment["MAVEN_OPTS"])

    def test_child_environment_drops_unrecorded_toolchain_overrides(self) -> None:
        inherited = {"PATH": "/tools", "HOME": "/home/bench", "GOFLAGS": "-race", "JAVA_TOOL_OPTIONS": "-Xmx4g", "PYTHONPATH": "/override"}
        with patch.dict(os.environ, inherited, clear=True):
            environment = lexical_runner.child_environment({"GOWORK": "off"})
        self.assertEqual(environment, {"PATH": "/tools", "HOME": "/home/bench", "GOWORK": "off"})

    def test_adapter_environment_suppresses_child_bytecode(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "fixture_module.py").write_text("VALUE = 1\n", encoding="utf-8")
            (root / "adapter.py").write_text("import fixture_module\n", encoding="utf-8")
            environment = os.environ.copy()
            environment.update(lexical_runner.benchmark_environment(root, self.manifest))
            subprocess.run([sys.executable, str(root / "adapter.py")], cwd=root, env=environment, check=True)
            self.assertFalse((root / "__pycache__").exists())

    def test_parent_runner_requires_user_site_isolation(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Python -E -s"):
            lexical_runner.require_isolated_parent(1, 0)
        with self.assertRaisesRegex(RuntimeError, "Python -E -s"):
            lexical_runner.require_isolated_parent(0, 1)
        lexical_runner.require_isolated_parent(1, 1)

    def test_selected_go_binary_reaches_setup_and_adapter_commands(self) -> None:
        selected = "/toolchains/go 1.26/bin/go"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            completed = subprocess.CompletedProcess([], 0, "", "")
            with patch.object(lexical_runner, "run_command", return_value=completed) as run:
                setup_ok, setup_command, _ = lexical_runner.setup_engine("bleve", root, 1, selected)
            self.assertTrue(setup_ok)
            self.assertEqual(setup_command[0], selected)
            self.assertEqual(run.call_args.args[4], {"GOWORK": "off", "GOENV": "off"})
            with patch.object(lexical_runner, "run_command", return_value=completed) as run:
                lexical_runner.setup_engine("lucene", root, 1, selected)
            self.assertEqual(run.call_args.args[4], {"MAVEN_SKIP_RC": "1"})
            treedb_command, _, treedb_env = lexical_runner.adapter_command("treedb_text_v2", 1, root, HERE / "lexical_manifest.json", root / "corpus.tsv", "source", selected)
            bleve_command, _, bleve_env = lexical_runner.adapter_command("bleve", 1, root, HERE / "lexical_manifest.json", root / "corpus.tsv", "source", selected)
        self.assertEqual(treedb_command[0], selected)
        self.assertEqual(bleve_command[0], selected)
        self.assertEqual(treedb_env["LEXICAL_GO_EXECUTABLE"], selected)
        self.assertEqual(bleve_env["LEXICAL_GO_EXECUTABLE"], selected)
        _, _, lucene_env = lexical_runner.adapter_command("lucene", 1, root, HERE / "lexical_manifest.json", root / "corpus.tsv", "source", selected)
        self.assertEqual(lucene_env["MAVEN_SKIP_RC"], "1")
        sqlite_command, _, sqlite_env = lexical_runner.adapter_command("sqlite_fts5", 1, root, HERE / "lexical_manifest.json", root / "corpus.tsv", "source", selected)
        self.assertEqual(sqlite_command[1:4], ["-E", "-s", "-B"])
        self.assertEqual(sqlite_env["PYTHONNOUSERSITE"], "1")
        self.assertEqual(lexical_runner.normalize_executable("./sdk/go/bin/go", Path("/workspace")), "/workspace/sdk/go/bin/go")
        self.assertEqual(lexical_runner.normalize_executable("go", Path("/workspace")), "go")

    def test_reference_interprets_every_manifest_shape(self) -> None:
        self.assertEqual(self.expected["common"], [f"doc-{i:06d}" for i in (0, 1, 2, 3, 4, 5, 6, 7, 9, 8)])
        self.assertEqual(self.expected["rare"], [f"doc-{i:06d}" for i in range(10, 20)])
        self.assertEqual(self.expected["and"], [f"doc-{i:06d}" for i in range(20, 30)])
        self.assertEqual(self.expected["or"], [f"doc-{i:06d}" for i in range(70, 80)])
        self.assertEqual(self.expected["phrase"], [f"doc-{i:06d}" for i in range(90, 100)])
        self.assertEqual(self.expected["scalar_filtered"], [f"doc-{i:06d}" for i in range(100, 110)])

    def test_common_ranking_depends_on_weight_tf_and_length_normalization(self) -> None:
        ranked = self.expected["common"]
        self.assertLess(ranked.index("doc-000000"), ranked.index("doc-000001"))
        self.assertLess(ranked.index("doc-000006"), ranked.index("doc-000007"))
        equal_weights = reference_results(self.manifest, self.documents, weight_overrides={"title": 1.0}, verify_evidence=False)["common"]
        self.assertLess(equal_weights.index("doc-000007"), equal_weights.index("doc-000006"))
        no_length_normalization = reference_results(self.manifest, self.documents, b_override=0.0, verify_evidence=False)["common"]
        self.assertLess(ranked.index("doc-000009"), ranked.index("doc-000008"))
        self.assertLess(no_length_normalization.index("doc-000008"), no_length_normalization.index("doc-000009"))

    def test_validator_rejects_drift_mismatch_leakage_and_missing_proof(self) -> None:
        mutations = {
            "document count drift": lambda a: a["corpus"].update(document_count=9_999),
            "content drift": lambda a: a["corpus"].update(sha256="0" * 64),
            "result mismatch": lambda a: a["cases"][0].update(result_ids=list(reversed(a["cases"][0]["result_ids"]))),
            "duplicate": lambda a: a["cases"][0].update(result_ids=[a["cases"][0]["result_ids"][0]] * 10),
            "leakage": lambda a: a["cases"][0].update(result_ids=["not-in-corpus"]),
            "fallback": lambda a: a["cases"][0]["route"].update(fallback=True),
            "route": lambda a: a["cases"][0]["route"].update(intended=False),
            "missing route proof": lambda a: a["cases"][0]["route"].pop("proof"),
            "empty route proof": lambda a: a["cases"][0]["route"].update(proof={}),
            "invalid ordinary TreeDB proof": lambda a: a["cases"][0]["route"]["proof"].update(postings_scanned=0),
            "invalid scalar TreeDB proof": lambda a: a["cases"][5]["route"]["proof"].update(scalar_filter_strategy="fallback"),
            "reopen": lambda a: a["reopen"].update(verified=False),
            "timeout": lambda a: a["cases"][0].update(timed_out=True),
            "malformed reopen digest": lambda a: a["reopen"].update(result_digest="proof"),
            "untyped resource": lambda a: a["build"].update(peak_rss=None),
            "filesystem mismatch": lambda a: a["environment"]["filesystem"].update(same_filesystem=False),
            "manifest environment mismatch": lambda a: a["environment"]["contract"].update(query_concurrency=2),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                artifact = self.artifact()
                mutate(artifact)
                with self.assertRaises(ValidationError):
                    validate_result(artifact, self.manifest, self.expected, self.eligible, self.corpus_ids)

    def test_validator_rejects_sqlite_storage_contract_drift(self) -> None:
        mutations = {
            "source table": lambda config: config.update(source_table="generated_content"),
            "FTS content mode": lambda config: config.update(fts_content_mode="contentful"),
            "generated field storage": lambda config: config.update(generated_weighted_field_storage="persisted"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                artifact = self.artifact("sqlite_fts5")
                mutate(artifact["config"])
                with self.assertRaisesRegex(ValidationError, "SQLite"):
                    validate_result(artifact, self.manifest, self.expected, self.eligible, self.corpus_ids)
        artifact = self.artifact("sqlite_fts5")
        artifact["command"] = ["python", "sqlite_fts5_bench.py"]
        with self.assertRaisesRegex(ValidationError, "Python isolation command"):
            validate_result(artifact, self.manifest, self.expected, self.eligible, self.corpus_ids)

    def test_directional_rows_reject_filter_leakage_and_truncation(self) -> None:
        leaked = self.artifact("sqlite_fts5")
        scalar = leaked["cases"][-1]
        scalar["result_ids"] = scalar["reopen_result_ids"] = [f"doc-{index:06d}" for index in range(10)]
        scalar["result_digest"] = scalar["reopen_result_digest"] = result_digest(scalar["result_ids"])
        leaked["reopen"]["result_digest"] = result_digest(case.get("reopen_result_digest", "") for case in leaked["cases"])
        with self.assertRaisesRegex(ValidationError, "eligibility"):
            validate_result(leaked, self.manifest, self.expected, self.eligible, self.corpus_ids)

        truncated = self.artifact("sqlite_fts5")
        common = truncated["cases"][0]
        common["result_ids"] = common["reopen_result_ids"] = common["result_ids"][:-1]
        common["result_digest"] = common["reopen_result_digest"] = result_digest(common["result_ids"])
        truncated["reopen"]["result_digest"] = result_digest(case.get("reopen_result_digest", "") for case in truncated["cases"])
        with self.assertRaisesRegex(ValidationError, "truncated"):
            validate_result(truncated, self.manifest, self.expected, self.eligible, self.corpus_ids)

    def test_typed_unsupported_is_accepted_but_not_headline_eligible(self) -> None:
        artifact = self.artifact()
        artifact["cases"][4] = {"id": "phrase", "status": "unsupported", "equivalent": False, "unsupported_reason": "positions disabled"}
        artifact["reopen"]["result_digest"] = result_digest(case.get("reopen_result_digest", "") for case in artifact["cases"])
        validate_result(artifact, self.manifest, self.expected, self.eligible, self.corpus_ids)
        artifacts = []
        for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5"):
            for repetition in range(1, 4):
                candidate = self.artifact(engine, repetition)
                if engine == "bleve":
                    candidate["cases"][4] = copy.deepcopy(artifact["cases"][4])
                    candidate["reopen"]["result_digest"] = result_digest(case.get("reopen_result_digest", "") for case in candidate["cases"])
                artifacts.append(candidate)
        report = consolidate(artifacts, self.manifest, self.documents, 3, self.context())
        self.assertFalse(any(row["engine"]["id"] == "bleve" and row["case"] == "phrase" for row in report["headline_rows"]))
        self.assertTrue(any(item["engine"]["id"] == "bleve" and item.get("case") == "phrase" and item["status"] == "unsupported" for item in report["equivalence_ledger"]))
        self.assertTrue(all(row["engine"]["id"] in {"treedb_text_v2", "lucene"} for row in report["headline_rows"]))
        self.assertTrue(any(row["engine"]["id"] == "bleve" for row in report["directional_rows"]))
        self.assertTrue(any(row["engine"]["id"] == "sqlite_fts5" for row in report["directional_rows"]))
        self.assertIn("bleve", report["engines_partial"])
        self.assertNotIn("bleve", report["engines_completed"])

    def test_dirty_source_never_enters_headline(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        context = self.context(modified=True)
        report = consolidate(artifacts, self.manifest, self.documents, 3, context)
        self.assertEqual(report["headline_rows"], [])
        self.assertFalse(report["qualification_eligible"])

    def test_consolidation_requires_treedb_and_two_external_engines(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene") for repetition in range(1, 4)]
        with self.assertRaisesRegex(ValidationError, "at least two"):
            consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_consolidation_rejects_repetition_metadata_drift(self) -> None:
        mutations = {
            "engine metadata": lambda artifact: artifact["engine"].update(name="different"),
            "versions": lambda artifact: artifact["versions"].update(adapter="different"),
            "benchmark config": lambda artifact: artifact["config"].update(stable_setting="different"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
                mutate(artifacts[1])
                with self.assertRaises(ValidationError):
                    consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_consolidation_rejects_directional_result_drift(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        changed = artifacts[7]["cases"][0]
        changed["result_ids"] = changed["reopen_result_ids"] = list(reversed(changed["result_ids"]))
        changed["result_digest"] = changed["reopen_result_digest"] = result_digest(changed["result_ids"])
        artifacts[7]["reopen"]["result_digest"] = result_digest(case.get("reopen_result_digest", "") for case in artifacts[7]["cases"])
        with self.assertRaisesRegex(ValidationError, "result IDs differ across repetitions"):
            consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_environment_policy_and_cross_repetition_detection_are_fail_closed(self) -> None:
        artifact = self.artifact()
        artifact["environment"]["execution"]["query_concurrency"] = 2
        with self.assertRaisesRegex(ValidationError, "concurrency"):
            validate_result(artifact, self.manifest, self.expected, self.eligible, self.corpus_ids)
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        artifacts[1]["environment"]["memory"]["runtime_specific_limit"] = "different"
        with self.assertRaisesRegex(ValidationError, "environment differs"):
            consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_consolidation_binds_artifact_resources_to_runner_context(self) -> None:
        mutations = {
            "filesystem": lambda artifact: artifact["environment"]["filesystem"].update(runner_device_id="2", corpus_store_id="2", index_store_id="2", result_store_id="2"),
            "memory": lambda artifact: artifact["environment"]["memory"].update(detected_address_space_limit="1073741824"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
                mutate(artifacts[0])
                with self.assertRaises(ValidationError):
                    consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_consolidated_builds_retain_typed_cpu_and_rss_per_repetition(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        report = consolidate(artifacts, self.manifest, self.documents, 3, self.context())
        for build in report["builds"]:
            self.assertEqual(len(build["cpu"]), 3)
            self.assertEqual(len(build["peak_rss"]), 3)
            self.assertEqual(build["environment"]["execution"]["query_concurrency"], 1)
            self.assertTrue(all(resource["status"] in {"ok", "unsupported"} for resource in build["cpu"] + build["peak_rss"]))

    def test_dirty_source_is_explicitly_ineligible(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "lucene", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        report = consolidate(artifacts, self.manifest, self.documents, 3, self.context(modified=True))
        self.assertFalse(report["qualification_eligible"])
        self.assertFalse(any(row["headline_eligible"] for row in report["headline_rows"]))
        context = self.context()
        context["source"]["post_run_reverified"] = False
        with self.assertRaisesRegex(ValidationError, "end-of-run recheck"):
            consolidate(artifacts, self.manifest, self.documents, 3, context)


if __name__ == "__main__":
    unittest.main()
