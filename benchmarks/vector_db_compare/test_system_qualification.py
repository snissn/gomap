#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from system_qualification import ContractError, _load, matched_recall_buckets, validate_plan, validate_result


ROOT = Path(__file__).parents[2]
PLAN_PATH = ROOT / "TreeDB/docs/spec/artifacts/vector-partition-local-system-qualification-4019-plan.json"


class SystemQualificationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(PLAN_PATH.read_text(encoding="utf-8"))
        cls.plan_sha256 = hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest()

    def result(self) -> dict:
        source_head = "1" * 40
        corpora = [
            {key: corpus[key] for key in ("id", "fixture_checksum", "manifest_sha256", "truth_identity", "truth_artifact_sha256", "truth_sha256")}
            for corpus in self.plan["accepted_inputs"]["corpora"]
        ]
        rows = []
        for row_index, contract in enumerate(self.plan["rows"]):
            identity = {
                key: (value.replace("<source_head_sha>", source_head) if isinstance(value, str) else value)
                for key, value in contract["pinned_identity"].items()
            }
            for key in contract["required_identity_fields"]:
                identity.setdefault(key, "a" * 64)
            identity["topology_identity_sha256"] = f"{row_index + 1:064x}"
            budgets = self.plan["workload"][contract["search_budget"]]["search_budgets"]
            corpus_runs = []
            for corpus in corpora:
                repetitions = []
                for repetition in range(1, 4):
                    searches = []
                    ordered_budgets = budgets if repetition == 1 else list(reversed(budgets)) if repetition == 2 else budgets[1:] + budgets[:1]
                    for budget in ordered_budgets:
                        for concurrency in self.plan["workload"]["concurrency"]:
                            cell = {
                                "budget": budget,
                                "concurrency": concurrency,
                                "status": "valid",
                                "metrics": {
                                    "queries": 1000,
                                    "completed_queries": 1000,
                                    "result_count": 10000,
                                    "errors": 0,
                                    "timeouts": 0,
                                    "recall_at_10": 0.95,
                                    "qps": 1.0,
                                    "p50_nanos": 1,
                                    "p95_nanos": 2,
                                    "p99_nanos": 3,
                                },
                            }
                            if contract["system"] == "treedb":
                                cell["counters"] = {key: 0 for key in self.plan["artifact_contract"]["tree_db_counters"]}
                                for key in self.plan["artifact_contract"]["tree_db_positive_counters"]:
                                    cell["counters"][key] = 1
                                cell["timings"] = {key: 0 for key in self.plan["artifact_contract"]["tree_db_timings"]}
                                for key in self.plan["artifact_contract"]["tree_db_positive_timings"]:
                                    cell["timings"][key] = 1
                                cell["generation"] = {"Index": "embedding_graph", "Generation": 7}
                                cell["elapsed_nanos"] = 1_000_000_000_000
                                cell["total_nanos"] = [1, 2, 3] + [3] * 997
                                cell["metrics"].update({"qps": 1.0, "p50_nanos": 3, "p95_nanos": 3, "p99_nanos": 3})
                            searches.append(cell)
                    repetitions.append(
                        {
                            "repetition": repetition,
                            "status": "valid",
                            "input_identity": copy.deepcopy(corpus),
                            "noise": {"valid": True, "load_1": 0.1, "load_5": 0.1, "load_15": 0.1},
                            "warmup_queries_per_cell": 1000,
                            "budget_order": ordered_budgets,
                            "phases": {key: 1 for key in self.plan["artifact_contract"]["required_phases"]},
                            "resources": {key: (0 if key == "swap_bytes" else 1) for key in self.plan["artifact_contract"]["required_resources"]},
                            "searches": searches,
                        }
                    )
                    if contract["id"] == "treedb_container_multi_daemon":
                        repetitions[-1]["resources"]["container_allocations"] = [
                            {"cpuset_cpus": cpus, "memory_bytes": 6 * 1024**3, "memory_swap_bytes": 6 * 1024**3, "pids_limit": 768}
                            for cpus in ("0-2", "3-5", "6-8", "9-11")
                        ]
                corpus_runs.append({"id": corpus["id"], "repetitions": repetitions})
            rows.append({"id": contract["id"], "status": "valid", "boundary": copy.deepcopy(contract["boundary"]), "identity": identity, "corpora": corpus_runs})
        return {
            "schema_version": 1,
            "result_kind": "vector_partition_local_system_qualification_v1",
            "status": "complete",
            "plan_sha256": self.plan_sha256,
            "resource_envelope": copy.deepcopy(self.plan["resource_envelope"]),
            "source_head_sha": source_head,
            "host": {
                "cpu_model": "11th Gen Intel(R) Core(TM) i5-11400F @ 2.60GHz",
                "logical_cpus": 12,
                "memory_bytes": 33512759296,
                "kernel": "test kernel",
                "storage_filesystem": "ext4",
                "storage_root": "/mnt/fast4tb/test",
                "storage_free_bytes": 107374182400,
                "docker_version": "test docker",
                "go_version": "go1.26.0",
                "python_version": "3.10.12",
            },
            "provenance": {
                "artifact_root": "/mnt/fast4tb/test",
                "commands_path": "commands.json",
                "commands_sha256": "b" * 64,
                "environment_path": "environment.json",
                "environment_sha256": "c" * 64,
                "started_at": "2026-08-05T00:00:00Z",
                "completed_at": "2026-08-05T01:00:00Z",
            },
            "corpora": corpora,
            "rows": rows,
        }

    def test_frozen_plan_and_complete_result_validate(self) -> None:
        validate_plan(self.plan)
        milvus = next(row for row in self.plan["rows"] if row["id"] == "milvus_standalone")
        pgvector = next(row for row in self.plan["rows"] if row["id"] == "postgres_pgvector")
        container = next(row for row in self.plan["rows"] if row["id"] == "treedb_container_multi_daemon")
        self.assertEqual(milvus["pinned_identity"]["server_version"], "2.6.20")
        self.assertIn("@sha256:", pgvector["pinned_identity"]["image"])
        self.assertEqual(container["pinned_identity"]["volume_policy"], "one_fresh_host_root_and_unique_in_container_destination_per_container")
        self.assertEqual(self.plan["accepted_inputs"]["campaign_index_sha256"], "c20f11bb38898fd0d5907330bec3df80db29df14e44962a8766502e757849aa2")
        self.assertEqual(self.plan["accepted_inputs"]["corpora"][0]["fixture_checksum"], "ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95")
        self.assertEqual(self.plan["accepted_inputs"]["corpora"][1]["fixture_checksum"], "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69")
        self.assertTrue(validate_result(self.plan, self.plan_sha256, self.result(), require_complete=True))
        self.assertEqual(len(matched_recall_buckets(self.plan, self.result())), 5 * 2 * 3)

        changed_plan = copy.deepcopy(self.plan)
        changed_plan["resource_envelope"]["memory_limit_bytes"] -= 1
        with self.assertRaisesRegex(ContractError, "frozen plan content"):
            validate_plan(changed_plan)

    def test_missing_identity_or_changed_boundary_fails_closed(self) -> None:
        missing = self.result()
        del missing["rows"][0]["identity"]["binary_sha256"]
        with self.assertRaisesRegex(ContractError, "binary_sha256"):
            validate_result(self.plan, self.plan_sha256, missing)

        changed = self.result()
        changed["rows"][1]["boundary"]["client"] = "in_process_call"
        with self.assertRaisesRegex(ContractError, "boundary"):
            validate_result(self.plan, self.plan_sha256, changed)

        mixed_binary = self.result()
        mixed_binary["rows"][1]["identity"]["binary_sha256"] = "f" * 64
        with self.assertRaisesRegex(ContractError, "one benchmark binary"):
            validate_result(self.plan, self.plan_sha256, mixed_binary)

        wrong_host = self.result()
        wrong_host["host"]["cpu_model"] = "different cpu"
        with self.assertRaisesRegex(ContractError, "host CPU"):
            validate_result(self.plan, self.plan_sha256, wrong_host)

        wrong_storage = self.result()
        wrong_storage["host"]["storage_root"] = wrong_storage["provenance"]["artifact_root"] = "/tmp/test"
        with self.assertRaisesRegex(ContractError, "host storage"):
            validate_result(self.plan, self.plan_sha256, wrong_storage)

        placeholder = self.result()
        placeholder["host"]["storage_root"] = placeholder["provenance"]["artifact_root"] = self.plan["resource_envelope"]["storage_root"]
        with self.assertRaisesRegex(ContractError, "host storage"):
            validate_result(self.plan, self.plan_sha256, placeholder)

        reused_topology = self.result()
        reused_topology["rows"][4]["identity"]["topology_identity_sha256"] = reused_topology["rows"][0]["identity"]["topology_identity_sha256"]
        with self.assertRaisesRegex(ContractError, "topology identities"):
            validate_result(self.plan, self.plan_sha256, reused_topology)

        wrong_input = self.result()
        wrong_input["rows"][3]["corpora"][0]["repetitions"][0]["input_identity"]["fixture_checksum"] = "f" * 64
        with self.assertRaisesRegex(ContractError, "accepted corpus"):
            validate_result(self.plan, self.plan_sha256, wrong_input)

        wrong_allocation = self.result()
        wrong_allocation["rows"][2]["corpora"][0]["repetitions"][0]["resources"]["container_allocations"][0]["cpuset_cpus"] = "0-11"
        with self.assertRaisesRegex(ContractError, "resource allocation"):
            validate_result(self.plan, self.plan_sha256, wrong_allocation)

    def test_incomplete_row_is_preserved_but_cannot_qualify(self) -> None:
        result = self.result()
        result["status"] = "incomplete"
        result["rows"][3] = {
            "id": "milvus_standalone",
            "status": "unsupported",
            "reason": "official deployment unavailable",
            "boundary": self.plan["rows"][3]["boundary"],
            "identity": result["rows"][3]["identity"],
        }
        self.assertFalse(validate_result(self.plan, self.plan_sha256, result))
        with self.assertRaisesRegex(ContractError, "every row"):
            validate_result(self.plan, self.plan_sha256, result, require_complete=True)

    def test_search_matrix_and_metrics_fail_closed(self) -> None:
        corpus_local_generation = self.result()
        for repetition in corpus_local_generation["rows"][0]["corpora"][1]["repetitions"]:
            for search in repetition["searches"]:
                search["generation"]["Generation"] = 8
        self.assertTrue(validate_result(self.plan, self.plan_sha256, corpus_local_generation, require_complete=True))

        missing_cell = self.result()
        missing_cell["rows"][4]["corpora"][0]["repetitions"][0]["searches"].pop()
        with self.assertRaisesRegex(ContractError, "search matrix"):
            validate_result(self.plan, self.plan_sha256, missing_cell)

        reordered = self.result()
        searches = reordered["rows"][0]["corpora"][0]["repetitions"][0]["searches"]
        searches[0], searches[1] = searches[1], searches[0]
        with self.assertRaisesRegex(ContractError, "search matrix order"):
            validate_result(self.plan, self.plan_sha256, reordered)

        bad_metrics = copy.deepcopy(self.result())
        bad_metrics["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["metrics"]["p95_nanos"] = 0
        with self.assertRaisesRegex(ContractError, "search metrics"):
            validate_result(self.plan, self.plan_sha256, bad_metrics)

        malformed = self.result()
        malformed["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["metrics"]["qps"] = "fast"
        with self.assertRaisesRegex(ContractError, "search metrics"):
            validate_result(self.plan, self.plan_sha256, malformed)

        nonfinite = self.result()
        nonfinite["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["metrics"]["qps"] = float("nan")
        with self.assertRaisesRegex(ContractError, "search metrics"):
            validate_result(self.plan, self.plan_sha256, nonfinite)

        infinite_resource = self.result()
        infinite_resource["rows"][0]["corpora"][0]["repetitions"][0]["resources"]["cpu_seconds"] = float("inf")
        with self.assertRaisesRegex(ContractError, "total-system resources"):
            validate_result(self.plan, self.plan_sha256, infinite_resource)

        swapped = self.result()
        swapped["rows"][0]["corpora"][0]["repetitions"][0]["resources"]["swap_bytes"] = 1
        with self.assertRaisesRegex(ContractError, "swap limit"):
            validate_result(self.plan, self.plan_sha256, swapped)

        missing_path_proof = self.result()
        missing_path_proof["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["counters"] = {
            key: 0 for key in self.plan["artifact_contract"]["tree_db_counters"]
        }
        with self.assertRaisesRegex(ContractError, "nonzero TreeDB path proof"):
            validate_result(self.plan, self.plan_sha256, missing_path_proof)

        missing_timing = self.result()
        missing_timing["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["timings"]["shard_search"] = 0
        with self.assertRaisesRegex(ContractError, "nonzero TreeDB timing attribution"):
            validate_result(self.plan, self.plan_sha256, missing_timing)

        changed_generation = self.result()
        changed_generation["rows"][0]["corpora"][0]["repetitions"][0]["searches"][1]["generation"]["Generation"] += 1
        with self.assertRaisesRegex(ContractError, "changes TreeDB generation identity"):
            validate_result(self.plan, self.plan_sha256, changed_generation)

        changed_latency = self.result()
        changed_latency["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["metrics"].update({"p50_nanos": 4, "p95_nanos": 4, "p99_nanos": 4})
        with self.assertRaisesRegex(ContractError, "latency percentiles"):
            validate_result(self.plan, self.plan_sha256, changed_latency)

        shortened = self.result()
        metrics = shortened["rows"][0]["corpora"][0]["repetitions"][0]["searches"][0]["metrics"]
        metrics["queries"] = metrics["completed_queries"] = 999
        metrics["result_count"] = 9990
        with self.assertRaisesRegex(ContractError, "search metrics"):
            validate_result(self.plan, self.plan_sha256, shortened)

        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "nonfinite.json"
            artifact.write_text('{"qps": NaN}', encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "non-finite JSON number"):
                _load(artifact)
            with self.assertRaisesRegex(ContractError, "exceeds 1 bytes"):
                _load(artifact, limit=1)

    def test_matched_recall_bucketing_rejects_missing_floor(self) -> None:
        missing = self.result()
        missing["rows"][3]["corpora"][0]["repetitions"][0]["searches"].pop(0)
        with self.assertRaisesRegex(ContractError, "lacks metrics"):
            matched_recall_buckets(self.plan, missing)

        result = self.result()
        milvus = result["rows"][3]
        for corpus in milvus["corpora"]:
            for repetition in corpus["repetitions"]:
                for cell in repetition["searches"]:
                    if cell["budget"] == {"ef": 16}:
                        cell["metrics"]["recall_at_10"] = 0.5
        selected = [bucket for bucket in matched_recall_buckets(self.plan, result) if bucket["row"] == "milvus_standalone"]
        self.assertTrue(selected)
        self.assertTrue(all(bucket["budget"] == {"ef": 32} for bucket in selected))

        result = self.result()
        milvus = result["rows"][3]
        for corpus in milvus["corpora"]:
            for repetition in corpus["repetitions"]:
                for cell in repetition["searches"]:
                    cell["metrics"]["recall_at_10"] = 0.5
        with self.assertRaisesRegex(ContractError, "no matched-recall budget"):
            validate_result(self.plan, self.plan_sha256, result)


if __name__ == "__main__":
    unittest.main()
