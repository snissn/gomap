#!/usr/bin/env python3

from __future__ import annotations

import copy
import unittest
from pathlib import Path

from lexical_common import (
    RESULT_SCHEMA,
    ValidationError,
    consolidate,
    corpus_bytes,
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

HERE = Path(__file__).resolve().parent


class LexicalComparisonTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = load_manifest(HERE / "lexical_manifest.json")
        cls.documents = [dict(zip(("id", "title", "body", "tenant"), line.split("\t"), strict=True)) for line in corpus_bytes(10_000).decode().splitlines()]
        cls.expected = reference_results(cls.manifest, cls.documents)
        cls.corpus_ids = {doc["id"] for doc in cls.documents}
    def context(self, modified: bool = False) -> dict:
        return {
            "source": {
                "commit": "candidate-commit",
                "tree_oid": "candidate-tree",
                "treedb_subtree_oid": "treedb-tree",
                "harness_subtree_oid": "harness-tree",
                "vcs_modified": modified,
                "qualification_eligible": not modified,
            }
        }


    def artifact(self, engine_id: str = "treedb_text_v2", repetition: int = 1) -> dict:
        cases = []
        for query in self.manifest["queries"]:
            ids = self.expected[query["id"]]
            cases.append({
                "id": query["id"], "status": "ok", "equivalent": True,
                "samples_nanos": [100] * self.manifest["execution"]["measured_queries_per_case"],
                "result_ids": ids, "result_digest": result_digest(ids),
                "reopen_result_ids": ids, "reopen_result_digest": result_digest(ids),
                "route": {"intended": True, "name": "test_inverted_index", "fallback": False},
                "timed_out": False,
            })
        return {
            "schema_version": RESULT_SCHEMA, "status": "ok",
            "engine": {"id": engine_id, "family": "test", "name": engine_id, "version": "candidate-commit" if engine_id == "treedb_text_v2" else "pinned"},
            "repetition": repetition, "manifest_sha256": manifest_sha256(self.manifest),
            "corpus": {"document_count": 10_000, "sha256": self.manifest["corpus"]["sha256"]},
            "command": ["adapter"], "versions": {"adapter": "pinned"}, "config": {"top_k": 10},
            "build": {"elapsed_nanos": 1, "docs_per_second": 1.0, "cpu_nanos": 1, "peak_rss_bytes": None, "checkpointed": True},
            "storage": {"durable_bytes": 1, "wal_bytes": 0, "transient_bytes": 0},
            "reopen": {"performed": True, "verified": True, "result_digest": "proof"},
            "cases": cases,
        }

    def test_frozen_corpus_and_normalization_contract(self) -> None:
        payload = corpus_bytes(self.manifest["corpus"]["document_count"])
        self.assertEqual(sha256_bytes(payload), self.manifest["corpus"]["sha256"])
        self.assertEqual(normalize("ＲＥＦＵＮＤ Policy"), "refund policy")
        self.assertEqual(tokenize("Refund—POLICY_42"), ["refund", "policy", "42"])

    def test_reference_interprets_every_manifest_shape(self) -> None:
        self.assertEqual(self.expected["common"], [f"doc-{i:06d}" for i in range(10)])
        self.assertEqual(self.expected["rare"], [f"doc-{i:06d}" for i in range(10)])
        self.assertEqual(self.expected["and"], [f"doc-{i:06d}" for i in range(20, 30)])
        self.assertEqual(self.expected["or"], [f"doc-{i:06d}" for i in range(70, 80)])
        self.assertEqual(self.expected["phrase"], [f"doc-{i:06d}" for i in range(80, 90)])
        self.assertEqual(self.expected["scalar_filtered"], [f"doc-{i:06d}" for i in range(100, 110)])

    def test_validator_rejects_drift_mismatch_leakage_and_missing_proof(self) -> None:
        mutations = {
            "document count drift": lambda a: a["corpus"].update(document_count=9_999),
            "content drift": lambda a: a["corpus"].update(sha256="0" * 64),
            "result mismatch": lambda a: a["cases"][0].update(result_ids=list(reversed(a["cases"][0]["result_ids"]))),
            "duplicate": lambda a: a["cases"][0].update(result_ids=[a["cases"][0]["result_ids"][0]] * 10),
            "leakage": lambda a: a["cases"][0].update(result_ids=["not-in-corpus"]),
            "fallback": lambda a: a["cases"][0]["route"].update(fallback=True),
            "route": lambda a: a["cases"][0]["route"].update(intended=False),
            "reopen": lambda a: a["reopen"].update(verified=False),
            "timeout": lambda a: a["cases"][0].update(timed_out=True),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                artifact = self.artifact()
                mutate(artifact)
                with self.assertRaises(ValidationError):
                    validate_result(artifact, self.manifest, self.expected, self.corpus_ids)

    def test_typed_unsupported_is_accepted_but_not_headline_eligible(self) -> None:
        artifact = self.artifact()
        artifact["cases"][4] = {"id": "phrase", "status": "unsupported", "equivalent": False, "unsupported_reason": "positions disabled"}
        validate_result(artifact, self.manifest, self.expected, self.corpus_ids)
        artifacts = []
        for engine in ("treedb_text_v2", "bleve", "sqlite_fts5"):
            for repetition in range(1, 4):
                candidate = self.artifact(engine, repetition)
                if engine == "bleve":
                    candidate["cases"][4] = copy.deepcopy(artifact["cases"][4])
                artifacts.append(candidate)
        report = consolidate(artifacts, self.manifest, self.documents, 3, self.context())
        self.assertFalse(any(row["engine"]["id"] == "bleve" and row["case"] == "phrase" for row in report["headline_rows"]))
        self.assertTrue(any(item["engine"]["id"] == "bleve" and item.get("case") == "phrase" and item["status"] == "unsupported" for item in report["equivalence_ledger"]))

    def test_consolidation_requires_treedb_and_two_external_engines(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "sqlite_fts5") for repetition in range(1, 4)]
        with self.assertRaisesRegex(ValidationError, "at least two"):
            consolidate(artifacts, self.manifest, self.documents, 3, self.context())

    def test_dirty_source_is_explicitly_ineligible(self) -> None:
        artifacts = [self.artifact(engine, repetition) for engine in ("treedb_text_v2", "bleve", "sqlite_fts5") for repetition in range(1, 4)]
        report = consolidate(artifacts, self.manifest, self.documents, 3, self.context(modified=True))
        self.assertFalse(report["qualification_eligible"])
        self.assertFalse(any(row["headline_eligible"] for row in report["headline_rows"]))


if __name__ == "__main__":
    unittest.main()
