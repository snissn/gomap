#!/usr/bin/env python3
"""Focused mutation checks for the uncommitted #4587 C0 validator draft."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("policy4587", HERE / "treedb_construction_policy_4587.py")
assert SPEC and SPEC.loader
policy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(policy)
CONTRACT_PATH = HERE.parent / "docs/benchmarks/treedb_construction_policy_c0_4587.json"
COMMIT = "1" * 40


class DecisionFixture:
    def __init__(self, root: Path):
        self.root = root
        self.contract = json.loads(CONTRACT_PATH.read_text())
        self.counter = 0

    @staticmethod
    def _write(path: Path, value: object) -> dict[str, str]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        return {"path": path.name, "sha256": policy.sha256_file(path)}

    def _result(self, root: Path, ef: int, kind: str, route: str, timestamp: float,
                dataset: dict) -> tuple[dict, dict, dict]:
        index_name = f"index-ef{ef}"
        diagnostic = kind == "diagnostic"
        exact_route = route == "exact"
        metadata = {
            "name": index_name,
            "dimension": 768,
            "metric": "cosine",
            "generation": 1,
            "vector_strategy": "column_graph",
            "vector_m": 16,
            "vector_ef_construction": ef,
            "vector_ef_search": 192,
            "quantized_indexes": [{"name": "embedding.scalar_u8.fast"}],
        }
        db_config = {
            "index_name": index_name,
            "stats_mode": "full_diagnostics" if diagnostic else "production",
            "response_format": "full" if diagnostic else "ids",
            "query_embedding_encoding": "f32_le",
        }
        case_config = {
            "metric_type": "COSINE",
            "m": 16,
            "ef_construction": ef,
            "ef_search": 192,
            "strategy": "column_graph",
            "use_vector_index": True,
            "query_mode": "exact" if exact_route else "quantized_rerank",
            "quantized_index_name": "" if exact_route else "embedding.scalar_u8.fast",
            "quantized_rerank_candidates": 0 if exact_route else 400,
            "require_vector_index_guards": diagnostic,
        }
        result = {
            "timestamp": timestamp,
            "results": [{
                "label": ":)",
                "task_config": {
                    "db_config": db_config,
                    "db_case_config": case_config,
                    "case_config": {
                        "k": 100,
                        "custom_case": {
                            "dataset_config": {
                                "name": dataset["name"],
                                "dir": dataset["directory"],
                                "size": str(dataset["vectors"]),
                                "dim": str(dataset["dimensions"]),
                                "metric_type": "COSINE",
                                "file_count": "1",
                                "use_shuffled": False,
                                "with_gt": True,
                            },
                        },
                        "concurrency_search_config": {
                            "num_concurrency": [32],
                            "concurrency_duration": 30,
                        },
                    },
                    "stages": ["search_serial", "search_concurrent"],
                },
                "metrics": {
                    "recall": 0.94,
                    "ndcg": 0.95,
                    "qps": 1000.0,
                    "conc_latency_p99_list": [0.002],
                    "conc_num_list": [32],
                    "payload_profile": "full" if diagnostic else "ids_only",
                },
            }],
        }
        if diagnostic:
            stats = {"documents_fetched": 0, "document_bytes": 0}
            if not exact_route:
                stats["quantized_rerank_exact_score_calls"] = 192
            response = {
                "no_documents": True,
                "query_mode": case_config["query_mode"],
                "results": [{"id": str(i)} for i in range(100)],
                "diagnostics": {
                    "fallback_reason": "none",
                    "route": "exact_hnsw_search_pack_v1" if exact_route else "quantized_rerank",
                },
                "stats": stats,
            }
        else:
            response = {"response_format": "ids", "ids": [str(i) for i in range(100)]}
        tag = f"{kind}-{route}"
        return (
            self._write(root / f"result-{tag}.json", result),
            self._write(root / f"response-{tag}.json", response),
            self._write(root / "index-metadata.json", metadata),
        )

    def run(self, ef: int, scale: int, role: str, partition: str, *, adjacency: float,
            persisted: float = 100.0, allocated: float = 100.0, projection: float | None = None) -> dict:
        self.counter += 1
        root = self.root / f"run-{self.counter}-{scale}-ef{ef}"
        root.mkdir(parents=True)
        dataset = policy.dataset_expected(self.contract, scale, partition)
        config = {
            "dimensions": 768,
            "metric": "cosine",
            "m": 16,
            "ef_construction": ef,
            "ef_search": 192,
            "configured_rerank_candidates": 400,
            "effective_rerank_candidates": 192,
            "top_k": 100,
        }
        manifest = {
            "schema_version": "treedb-vectordbbench-artifact/v1",
            "artifact_root": str(root.resolve()),
            "context": {
                "gomap": {"commit": COMMIT, "dirty": False},
                "vectordbbench": {
                    "commit": self.contract["source_identity"]["vectordbbench"]["commit"],
                    "dirty": False,
                },
            },
            "service": {
                "binary": {"sha256": "2" * 64},
                "data_dir": str((root / "treedb-data").resolve()),
            },
            "lifecycle": {
                "result_status": "completed",
                "expected_rows": scale,
                "identity": {
                    "gomap_commit": COMMIT,
                    "service_binary_sha256": "2" * 64,
                    "vectordbbench_commit": self.contract["source_identity"]["vectordbbench"]["commit"],
                },
                "dataset": {
                    "name": dataset["name"],
                    "vectors": dataset["vectors"],
                    "dimensions": dataset["dimensions"],
                    "sha256": dataset["train_sha256"],
                },
            },
            "harness": {"m": 16, "ef_construction": ef, "ef_search": 192, "k": 100,
                        "rerank_candidates": 400, "rows": "scalar"},
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        isolation = {
            "schema_version": policy.ISOLATION_SCHEMA,
            "artifact_root": str(root.resolve()),
            "lock_path": self.contract["experiment"]["isolation_and_noise"]["lock_path"],
            "gomaxprocs": 12,
            "competing_processes": [],
            "swap": {"before_used_bytes": 0, "after_used_bytes": 0, "peak_used_bytes": 0},
        }
        work = {key: 1 for key in policy.WORK_KEYS}
        work["search_visited_candidates_by_layer"] = {"0": 1}
        work["diversity_rejection_position_distribution"] = {"0": 1}
        resources = {key: 100.0 for key in policy.RESOURCE_KEYS}
        resources["persisted_bytes"] = persisted
        resources["cumulative_allocated_bytes"] = allocated
        measurements = {
            "schema_version": policy.MEASUREMENT_SCHEMA,
            "phase_seconds": {"adjacency": adjacency, "optimize": adjacency + 10},
            "cpu_utilization_logical_cores": 6.0,
            "determinism": {
                "graph_config_checksum": policy.canonical_sha256(config),
                "small_repeat_checksum_a": "4" * 64,
                "small_repeat_checksum_b": "4" * 64,
                "tie_row_order_digest_a": "5" * 64,
                "tie_row_order_digest_b": "5" * 64,
            },
            "diagnostic_work_profile": work,
            "resources": resources,
            "projected_10m_adjacency_reduction_fraction": projection,
        }
        search = []
        for position, (kind, route) in enumerate(policy.SEARCH_ORDER):
            result, response, metadata = self._result(
                root, ef, kind, route, float(position + 1), dataset)
            search.append({"kind": kind, "route": route, "result": result,
                           "response": response, "index_metadata": metadata})
        return {
            "run_id": f"run-{self.counter}",
            "scale": scale,
            "role": role,
            "partition": partition,
            "ef_construction": ef,
            "execution_commit": COMMIT,
            "dataset": dataset,
            "configuration": config,
            "artifact": {"root": str(root.resolve()), "manifest_sha256": policy.sha256_file(manifest_path)},
            "isolation_evidence": self._write(root / "isolation.json", isolation),
            "measurement_evidence": self._write(root / "measurements.json", measurements),
            "search_evidence": search,
        }

    def no_go_packet(self) -> dict:
        runs = [
            self.run(128, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(192, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(256, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(300, 250000, "screening_control", "selection", adjacency=100),
        ]
        return {"schema_version": policy.RESULT_SCHEMA, "execution_commit": COMMIT,
                "contract_sha256": policy.canonical_sha256(self.contract), "verdict": "C0_NO_GO", "runs": runs}

    def go_packet(self) -> dict:
        runs = [
            self.run(128, 250000, "screening_candidate", "selection", adjacency=60),
            self.run(192, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(256, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(300, 250000, "screening_control", "selection", adjacency=100),
            self.run(300, 1000000, "decision_control", "holdout", adjacency=100, projection=0.0),
            self.run(128, 1000000, "decision_candidate", "holdout", adjacency=60, projection=0.4),
        ]
        return {"schema_version": policy.RESULT_SCHEMA, "execution_commit": COMMIT,
                "contract_sha256": policy.canonical_sha256(self.contract), "verdict": "GO", "runs": runs}

    def validate(self, packet: dict) -> dict:
        return policy.validate_decision(packet, self.contract, run_base_validator=False, require_clean_head=False)

    def rewrite_bound(self, run: dict, binding: dict, mutate) -> None:
        path = Path(run["artifact"]["root"]) / binding["path"]
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        binding["sha256"] = policy.sha256_file(path)


class ValidatorMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.fixture = DecisionFixture(Path(self.temp.name))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def assert_invalid(self, packet: dict, pattern: str) -> None:
        with self.assertRaisesRegex(ValueError, pattern):
            self.fixture.validate(packet)

    def test_valid_no_go(self) -> None:
        self.assertEqual(self.fixture.validate(self.fixture.no_go_packet())["verdict"], "C0_NO_GO")

    def test_wrong_source_and_dirty_identity(self) -> None:
        packet = self.fixture.no_go_packet()
        packet["runs"][0]["execution_commit"] = "3" * 40
        self.assert_invalid(packet, "execution commit")
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        manifest = Path(run["artifact"]["root"]) / "manifest.json"
        value = json.loads(manifest.read_text())
        value["context"]["gomap"]["dirty"] = True
        manifest.write_text(json.dumps(value, sort_keys=True) + "\n")
        run["artifact"]["manifest_sha256"] = policy.sha256_file(manifest)
        self.assert_invalid(packet, "artifact gomap identity")

    def test_extra_row_and_holdout_misuse(self) -> None:
        packet = self.fixture.no_go_packet()
        packet["runs"].append(copy.deepcopy(packet["runs"][0]))
        self.assert_invalid(packet, "unique|cardinality")
        packet = self.fixture.no_go_packet()
        packet["runs"].append(self.fixture.run(300, 1000000, "decision_control", "holdout", adjacency=100))
        self.assert_invalid(packet, "no-winner run cardinality")

    def test_wrong_dataset_and_configuration(self) -> None:
        packet = self.fixture.no_go_packet()
        packet["runs"][0]["dataset"]["train_sha256"] = "0" * 64
        self.assert_invalid(packet, "dataset binding")
        packet = self.fixture.no_go_packet()
        packet["runs"][0]["configuration"]["ef_search"] = 191
        self.assert_invalid(packet, "configuration")
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["search_evidence"][0]["result"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value["results"][0]["task_config"]["case_config"]["custom_case"][
                "dataset_config"
            ].update({"name": "cohere-medium-1m-holdout-v1", "size": "1000000"}),
        )
        self.assert_invalid(packet, "canonical dataset config")

    def test_mixed_diagnostic_and_production_evidence(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["search_evidence"][0]["result"]
        self.fixture.rewrite_bound(run, binding, lambda value: value["results"][0]["task_config"]["db_config"].update(
            {"stats_mode": "production", "response_format": "ids"}))
        self.assert_invalid(packet, "diagnostic transport")

    def test_resource_regression_cannot_be_go(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][5]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(run, binding,
                                   lambda value: value["resources"].update({"persisted_bytes": 101.0}))
        self.assert_invalid(packet, "computed verdict")

    def test_fallback_and_result_count(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["search_evidence"][0]["response"]
        self.fixture.rewrite_bound(run, binding,
                                   lambda value: value["diagnostics"].update({"fallback_reason": "exact_scan"}))
        self.assert_invalid(packet, "fallback reason")
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["search_evidence"][1]["response"]
        self.fixture.rewrite_bound(run, binding, lambda value: value["ids"].pop())
        self.assert_invalid(packet, "production result count")

    def test_cpu_and_determinism_evidence(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value.update({"cpu_utilization_logical_cores": 13.0}),
        )
        self.assert_invalid(packet, "cpu_utilization_logical_cores")
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value["determinism"].update({"small_repeat_checksum_b": "6" * 64}),
        )
        self.assert_invalid(packet, "small-repeat graph determinism")

    def test_graph_config_checksum_binding(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value["determinism"].update({"graph_config_checksum": "6" * 64}),
        )
        self.assert_invalid(packet, "graph/config checksum binding")

    def test_winner_tie_break_uses_both_routes(self) -> None:
        runs = [
            self.fixture.run(128, 250000, "screening_candidate", "selection", adjacency=60),
            self.fixture.run(192, 250000, "screening_candidate", "selection", adjacency=60),
            self.fixture.run(256, 250000, "screening_candidate", "selection", adjacency=95),
            self.fixture.run(300, 250000, "screening_control", "selection", adjacency=100),
            self.fixture.run(300, 1000000, "decision_control", "holdout", adjacency=100, projection=0.0),
            self.fixture.run(192, 1000000, "decision_candidate", "holdout", adjacency=60, projection=0.4),
        ]
        binding = runs[0]["search_evidence"][3]["result"]
        self.fixture.rewrite_bound(
            runs[0],
            binding,
            lambda value: value["results"][0]["metrics"].update({"recall": 0.9385, "ndcg": 0.9485}),
        )
        binding = runs[1]["search_evidence"][1]["result"]
        self.fixture.rewrite_bound(
            runs[1],
            binding,
            lambda value: value["results"][0]["metrics"].update({"recall": 0.939, "ndcg": 0.949}),
        )
        packet = {
            "schema_version": policy.RESULT_SCHEMA,
            "execution_commit": COMMIT,
            "contract_sha256": policy.canonical_sha256(self.fixture.contract),
            "verdict": "GO",
            "runs": runs,
        }
        self.assertEqual(self.fixture.validate(packet)["winner"], 192)

    def test_holdout_wrong_winner(self) -> None:
        packet = self.fixture.go_packet()
        packet["runs"][5]["ef_construction"] = 192
        self.assert_invalid(packet, "configuration|holdout cardinality")


if __name__ == "__main__":
    unittest.main()
