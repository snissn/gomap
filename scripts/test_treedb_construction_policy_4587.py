#!/usr/bin/env python3
"""Focused mutation checks for the #4587 C0 authorization and decision validator."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("policy4587", HERE / "treedb_construction_policy_4587.py")
assert SPEC and SPEC.loader
policy = importlib.util.module_from_spec(SPEC)
SEARCH_SPEC = importlib.util.spec_from_file_location(
    "search_existing_index", HERE / "treedb_vdbbench_search_existing_index.py")
assert SEARCH_SPEC and SEARCH_SPEC.loader
search_existing_index = importlib.util.module_from_spec(SEARCH_SPEC)
SEARCH_SPEC.loader.exec_module(search_existing_index)
SPEC.loader.exec_module(policy)
CONTRACT_PATH = HERE.parent / "docs/benchmarks/treedb_construction_policy_c0_4587.json"
COMMIT = "1" * 40


class DecisionFixture:
    def __init__(self, root: Path):
        self.root = root
        self.contract = json.loads(CONTRACT_PATH.read_text())
        self.contract["commands"]["artifact_root"] = str(root.resolve())
        self.contract["source_identity"]["gomap_root"] = str(HERE.parent)
        self.counter = 0
        self.service_binary = root / "treedb-document-service"
        self.service_binary.write_bytes(b"fixture service binary\n")
        self.binary_sha = policy.sha256_file(self.service_binary)
        self.authorization_path = root / "execution-authorization.json"
        self.reset_authorization()

    @staticmethod
    def _write(path: Path, value: object) -> dict[str, str]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        return {"path": path.name, "sha256": policy.sha256_file(path)}

    def reset_authorization(self) -> dict[str, str]:
        source_root = HERE.parent
        authorization = {
            "schema_version": policy.AUTHORIZATION_SCHEMA,
            "authorization_kind": "COORDINATOR_REVIEW_PROVENANCE",
            "artifact_root": str(Path(self.contract["commands"]["artifact_root"]).resolve()),
            "execution_commit": COMMIT,
            "contract_sha256": policy.canonical_sha256(self.contract),
            "protocol_files": {
                path: policy.sha256_file(source_root / path) for path in policy.PROTOCOL_PATHS
            },
            "source_identity": policy.authorization_source_identity(self.contract),
            "service_binary": {
                "path": str(self.service_binary.resolve()),
                "sha256": self.binary_sha,
            },
        }
        self.authorization_path.write_text(json.dumps(authorization, sort_keys=True) + "\n")
        return {
            "path": str(self.authorization_path.resolve()),
            "sha256": policy.sha256_file(self.authorization_path),
        }

    def mutate_authorization(self, mutate) -> dict[str, str]:
        authorization = json.loads(self.authorization_path.read_text())
        mutate(authorization)
        self.authorization_path.write_text(json.dumps(authorization, sort_keys=True) + "\n")
        return {
            "path": str(self.authorization_path.resolve()),
            "sha256": policy.sha256_file(self.authorization_path),
        }

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
            persisted: float = 100.0, allocated: float = 100.0, projection: float | None = None,
            binary_sha: str | None = None) -> dict:
        self.counter += 1
        root = self.root / f"run-{self.counter}-{scale}-ef{ef}"
        root.mkdir(parents=True)
        started = datetime(2026, 9, 2, tzinfo=timezone.utc) + timedelta(minutes=10 * self.counter)
        completed = started + timedelta(minutes=5)
        lifecycle_path = root / "lifecycle.jsonl"
        lifecycle_events = [
            {
                "schema_version": "treedb-vectordbbench-lifecycle-event/v1",
                "sequence": 0,
                "stage": "startup",
                "timestamp": started.isoformat(),
                "state": {},
            },
            {
                "schema_version": "treedb-vectordbbench-lifecycle-event/v1",
                "sequence": 1,
                "stage": "teardown",
                "timestamp": completed.isoformat(),
                "state": {},
            },
        ]
        lifecycle_path.write_text("".join(
            json.dumps(event, sort_keys=True) + "\n" for event in lifecycle_events))
        binary_sha = binary_sha or self.binary_sha
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
                "binary": {"sha256": binary_sha},
                "data_dir": str((root / "treedb-data").resolve()),
            },
            "lifecycle": {
                "schema_version": "treedb-vectordbbench-lifecycle/v1",
                "file": lifecycle_path.name,
                "sha256": policy.sha256_file(lifecycle_path),
                "result_status": "completed",
                "expected_rows": scale,
                "identity": {
                    "gomap_commit": COMMIT,
                    "service_binary_sha256": binary_sha,
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
            "origin": {
                "run_id": f"run-{self.counter}",
                "artifact_root": str(root.resolve()),
                "execution_commit": COMMIT,
                "dataset_sha256": policy.canonical_sha256(dataset),
                "scale": scale,
                "role": role,
                "partition": partition,
                "ef_construction": ef,
                "lifecycle_sha256": policy.sha256_file(lifecycle_path),
                "lifecycle_started_at": started.isoformat(),
                "lifecycle_completed_at": completed.isoformat(),
            },
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
            origin = {
                "schema_version": policy.SEARCH_ORIGIN_SCHEMA,
                "run_id": f"run-{self.counter}",
                "artifact_root": str(root.resolve()),
                "manifest_sha256": policy.sha256_file(manifest_path),
                "execution_commit": COMMIT,
                "dataset_sha256": policy.canonical_sha256(dataset),
                "scale": scale,
                "role": role,
                "partition": partition,
                "ef_construction": ef,
                "lifecycle_sha256": policy.sha256_file(lifecycle_path),
                "lifecycle_started_at": started.isoformat(),
                "lifecycle_completed_at": completed.isoformat(),
                "kind": kind,
                "route": route,
                "result_sha256": result["sha256"],
                "response_sha256": response["sha256"],
                "index_metadata_sha256": metadata["sha256"],
            }
            search.append({
                "kind": kind,
                "route": route,
                "origin": self._write(root / f"search-origin-{position}.json", origin),
                "result": result,
                "response": response,
                "index_metadata": metadata,
            })
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

    def winner_selection(self, runs: list[dict], winner: int, authorization_sha256: str,
                         selected_at: datetime | None = None) -> dict[str, str]:
        screening_rows = []
        completed = []
        for run in runs[:4]:
            measurement_path = Path(run["artifact"]["root"]) / run["measurement_evidence"]["path"]
            origin = json.loads(measurement_path.read_text())["origin"]
            completed.append(policy.utc_timestamp(origin["lifecycle_completed_at"], "fixture completion"))
            screening_rows.append({
                "run_id": run["run_id"],
                "artifact_root": run["artifact"]["root"],
                "measurement_sha256": run["measurement_evidence"]["sha256"],
                "lifecycle_sha256": origin["lifecycle_sha256"],
                "completed_at": origin["lifecycle_completed_at"],
            })
        selected_at = selected_at or max(completed) + timedelta(minutes=1)
        event = {
            "schema_version": policy.WINNER_SELECTION_SCHEMA,
            "execution_commit": COMMIT,
            "contract_sha256": policy.canonical_sha256(self.contract),
            "authorization_sha256": authorization_sha256,
            "screening_runs": screening_rows,
            "selected_ef_construction": winner,
            "selected_at": selected_at.isoformat(),
        }
        return self._write(self.root / "winner-selection.json", event)

    def no_go_packet(self) -> dict:
        runs = [
            self.run(128, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(192, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(256, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(300, 250000, "screening_control", "selection", adjacency=100),
        ]
        return {"schema_version": policy.RESULT_SCHEMA, "execution_commit": COMMIT,
                "contract_sha256": policy.canonical_sha256(self.contract),
                "authorization": self.reset_authorization(), "winner_selection": None,
                "verdict": "C0_NO_GO", "runs": runs}

    def go_packet(self) -> dict:
        runs = [
            self.run(128, 250000, "screening_candidate", "selection", adjacency=60),
            self.run(192, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(256, 250000, "screening_candidate", "selection", adjacency=95),
            self.run(300, 250000, "screening_control", "selection", adjacency=100),
            self.run(300, 1000000, "decision_control", "holdout", adjacency=100, projection=0.0),
            self.run(128, 1000000, "decision_candidate", "holdout", adjacency=60, projection=0.4),
        ]
        authorization = self.reset_authorization()
        return {"schema_version": policy.RESULT_SCHEMA, "execution_commit": COMMIT,
                "contract_sha256": policy.canonical_sha256(self.contract),
                "authorization": authorization,
                "winner_selection": self.winner_selection(runs, 128, authorization["sha256"]),
                "verdict": "GO", "runs": runs}

    def validate(self, packet: dict) -> dict:
        return policy.validate_decision(packet, self.contract, run_base_validator=False, require_clean_head=False)



    def rewrite_winner_selection(self, packet: dict, mutate) -> None:
        binding = packet["winner_selection"]
        path = self.root / binding["path"]
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        binding["sha256"] = policy.sha256_file(path)

    def rewrite_bound(self, run: dict, binding: dict, mutate) -> None:
        linked = [
            (entry, key)
            for entry in run["search_evidence"]
            for key in ("result", "response", "index_metadata")
            if entry[key] is binding
        ]
        path = Path(run["artifact"]["root"]) / binding["path"]
        value = json.loads(path.read_text())
        mutate(value)
        path.write_text(json.dumps(value, sort_keys=True) + "\n")
        binding["sha256"] = policy.sha256_file(path)
        for entry, key in linked:
            origin_binding = entry["origin"]
            origin_path = Path(run["artifact"]["root"]) / origin_binding["path"]
            origin = json.loads(origin_path.read_text())
            origin[f"{key}_sha256"] = binding["sha256"]
            origin_path.write_text(json.dumps(origin, sort_keys=True) + "\n")
            origin_binding["sha256"] = policy.sha256_file(origin_path)


    def retime_run(self, run: dict, started: str, completed: str) -> None:
        root = Path(run["artifact"]["root"])
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        lifecycle_path = root / manifest["lifecycle"]["file"]
        events = [json.loads(line) for line in lifecycle_path.read_text().splitlines()]
        events[0]["timestamp"] = started
        events[-1]["timestamp"] = completed
        lifecycle_path.write_text("".join(
            json.dumps(event, sort_keys=True) + "\n" for event in events))
        lifecycle_sha = policy.sha256_file(lifecycle_path)
        manifest["lifecycle"]["sha256"] = lifecycle_sha
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        manifest_sha = policy.sha256_file(manifest_path)
        run["artifact"]["manifest_sha256"] = manifest_sha
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        measurement["origin"].update({
            "lifecycle_sha256": lifecycle_sha,
            "lifecycle_started_at": started,
            "lifecycle_completed_at": completed,
        })
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        for entry in run["search_evidence"]:
            origin_path = root / entry["origin"]["path"]
            origin = json.loads(origin_path.read_text())
            origin.update({
                "manifest_sha256": manifest_sha,
                "lifecycle_sha256": lifecycle_sha,
                "lifecycle_started_at": started,
                "lifecycle_completed_at": completed,
            })
            origin_path.write_text(json.dumps(origin, sort_keys=True) + "\n")
            entry["origin"]["sha256"] = policy.sha256_file(origin_path)


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

    def test_authorization_required_and_checksum_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        packet.pop("authorization")
        self.assert_invalid(packet, "decision keys")
        packet = self.fixture.no_go_packet()
        packet["authorization"]["sha256"] = "0" * 64
        self.assert_invalid(packet, "authorization SHA-256")
        packet = self.fixture.no_go_packet()
        packet["authorization"] = self.fixture.mutate_authorization(
            lambda value: value.update({"unexpected": True}))
        self.assert_invalid(packet, "execution authorization keys")

    def test_authorization_rejects_post_review_protocol_file_drift(self) -> None:
        source_root = HERE.parent
        original_sha256_file = policy.sha256_file
        for path in policy.PROTOCOL_PATHS:
            with self.subTest(path=path):
                packet = self.fixture.no_go_packet()
                changed_path = (source_root / path).resolve()

                def changed_digest(candidate: Path) -> str:
                    if candidate.resolve() == changed_path:
                        return "0" * 64
                    return original_sha256_file(candidate)

                with mock.patch.object(policy, "sha256_file", side_effect=changed_digest):
                    self.assert_invalid(packet, "authorized protocol file")

    def test_authorization_binds_commit_contract_and_source_identities(self) -> None:
        mutations = [
            ("authorized execution commit", lambda value: value.update({"execution_commit": "3" * 40})),
            ("contract SHA-256", lambda value: value.update({"contract_sha256": "0" * 64})),
            ("source identity", lambda value: value["source_identity"]["runtime"].update({"logical_cpus": 11})),
            ("source identity", lambda value: value["source_identity"]["harness_blobs"].update(
                {"scripts/treedb_vectordbbench_artifact.py": "0" * 40})),
            ("source identity", lambda value: value["source_identity"]["vectordbbench"].update(
                {"commit": "0" * 40})),
        ]
        for pattern, mutate in mutations:
            with self.subTest(pattern=pattern):
                packet = self.fixture.no_go_packet()
                packet["authorization"] = self.fixture.mutate_authorization(mutate)
                self.assert_invalid(packet, pattern)

    def test_mixed_service_binaries_fail_even_when_each_manifest_is_self_consistent(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][1]
        manifest_path = Path(run["artifact"]["root"]) / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["service"]["binary"]["sha256"] = "3" * 64
        manifest["lifecycle"]["identity"]["service_binary_sha256"] = "3" * 64
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        run["artifact"]["manifest_sha256"] = policy.sha256_file(manifest_path)
        for entry in run["search_evidence"]:
            self.fixture.rewrite_bound(
                run, entry["origin"],
                lambda value: value.update({"manifest_sha256": run["artifact"]["manifest_sha256"]}),
            )
        self.assert_invalid(packet, "one authorized service binary")

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

    def test_measurement_origin_binding(self) -> None:
        mutations = [
            {"run_id": "other-run"},
            {"artifact_root": "/tmp/wrong-root"},
            {"execution_commit": "2" * 40},
            {"dataset_sha256": "0" * 64},
            {"scale": 1000000},
            {"role": "decision_candidate"},
            {"partition": "holdout"},
            {"ef_construction": 192},
            {"lifecycle_sha256": "0" * 64},
            {"lifecycle_started_at": "2026-09-02T00:00:00+00:00"},
            {"lifecycle_completed_at": "2026-09-02T00:01:00+00:00"},
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                packet = self.fixture.no_go_packet()
                run = packet["runs"][0]
                self.fixture.rewrite_bound(
                    run, run["measurement_evidence"],
                    lambda value, mutation=mutation: value["origin"].update(mutation),
                )
                self.assert_invalid(packet, "measurement originating run binding")

    def test_winner_selection_chronology(self) -> None:
        packet = self.fixture.go_packet()
        screening_measurement = Path(packet["runs"][3]["artifact"]["root"]) / "measurements.json"
        screening_completed = json.loads(screening_measurement.read_text())["origin"]["lifecycle_completed_at"]
        self.fixture.rewrite_winner_selection(
            packet, lambda value: value.update({"selected_at": screening_completed}))
        self.assert_invalid(packet, "after all screening lifecycles")

        packet = self.fixture.go_packet()
        holdout_measurement = Path(packet["runs"][4]["artifact"]["root"]) / "measurements.json"
        holdout_started = json.loads(holdout_measurement.read_text())["origin"]["lifecycle_started_at"]
        self.fixture.rewrite_winner_selection(
            packet, lambda value: value.update({"selected_at": holdout_started}))
        self.assert_invalid(packet, "holdout lifecycle must start after")

    def test_zero_production_qps_and_p99_are_invalid(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["search_evidence"][1]["result"],
            lambda value: value["results"][0]["metrics"].update({"qps": 0.0}),
        )
        self.assert_invalid(packet, "qps must be a finite positive number")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["search_evidence"][1]["result"],
            lambda value: value["results"][0]["metrics"].update({"conc_latency_p99_list": [0.0]}),
        )
        self.assert_invalid(packet, "p99 must be a finite positive number")

    def test_missing_document_counters_are_invalid_at_both_sites(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["search_evidence"][0]["response"],
            lambda value: value["stats"].pop("documents_fetched"),
        )
        self.assert_invalid(packet, "document counters are required")

        response = {
            "no_documents": True,
            "query_mode": "exact",
            "results": [{"id": "0"}],
            "diagnostics": {
                "fallback_reason": "none",
                "route": "exact_hnsw_search_pack_v1",
            },
            "stats": {"document_bytes": 0},
        }
        args = search_existing_index.argparse.Namespace(
            route="exact", top_k=1, effective_rerank_candidates=192)
        with self.assertRaisesRegex(ValueError, "missing document counters"):
            search_existing_index.validate_diagnostic(response, args)

    def test_frozen_go_gate_policy_drift_is_invalid(self) -> None:
        packet = self.fixture.no_go_packet()
        self.fixture.contract["experiment"]["go_gates"]["minimum_production_qps_ratio"] = 0.90
        self.assert_invalid(packet, "frozen GO gate policy")

    def test_search_evidence_origin_binding(self) -> None:
        mutations = [
            {"run_id": "other-run"},
            {"manifest_sha256": "0" * 64},
            {"result_sha256": "0" * 64},
            {"response_sha256": "0" * 64},
            {"index_metadata_sha256": "0" * 64},
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                packet = self.fixture.no_go_packet()
                run = packet["runs"][0]
                self.fixture.rewrite_bound(
                    run, run["search_evidence"][0]["origin"],
                    lambda value, mutation=mutation: value.update(mutation),
                )
                self.assert_invalid(packet, "originating run binding")

    def test_decision_control_completes_before_candidate_starts(self) -> None:
        packet = self.fixture.go_packet()
        control_measurement = Path(packet["runs"][4]["artifact"]["root"]) / "measurements.json"
        candidate_measurement = Path(packet["runs"][5]["artifact"]["root"]) / "measurements.json"
        control_completed = json.loads(control_measurement.read_text())["origin"]["lifecycle_completed_at"]
        candidate_completed = json.loads(candidate_measurement.read_text())["origin"]["lifecycle_completed_at"]
        self.fixture.retime_run(packet["runs"][5], control_completed, candidate_completed)
        self.assert_invalid(packet, "decision control lifecycle must complete before candidate")

    def test_recall_and_ndcg_must_be_finite_unit_interval(self) -> None:
        for metric, value in (("recall", 1.01), ("ndcg", 1.01), ("recall", float("inf"))):
            with self.subTest(metric=metric, value=value):
                packet = self.fixture.no_go_packet()
                run = packet["runs"][0]
                self.fixture.rewrite_bound(
                    run, run["search_evidence"][1]["result"],
                    lambda result, metric=metric, value=value:
                    result["results"][0]["metrics"].update({metric: value}),
                )
                self.assert_invalid(packet, "finite")

    def test_decision_gate_resources_must_be_positive(self) -> None:
        for position in (4, 5):
            for key in ("peak_rss_bytes", "persisted_bytes", "cumulative_allocated_bytes"):
                with self.subTest(position=position, key=key):
                    packet = self.fixture.go_packet()
                    run = packet["runs"][position]
                    self.fixture.rewrite_bound(
                        run, run["measurement_evidence"],
                        lambda value, key=key: value["resources"].update({key: 0.0}),
                    )
                    self.assert_invalid(packet, rf"decision .* resources\.{key} must be a finite positive")

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
        authorization = self.fixture.reset_authorization()
        packet = {
            "schema_version": policy.RESULT_SCHEMA,
            "execution_commit": COMMIT,
            "contract_sha256": policy.canonical_sha256(self.fixture.contract),
            "authorization": authorization,
            "winner_selection": self.fixture.winner_selection(runs, 192, authorization["sha256"]),
            "verdict": "GO",
            "runs": runs,
        }
        self.assertEqual(self.fixture.validate(packet)["winner"], 192)

    def test_holdout_wrong_winner(self) -> None:
        packet = self.fixture.go_packet()
        packet["runs"][5] = self.fixture.run(
            192, 1000000, "decision_candidate", "holdout", adjacency=60, projection=0.4)
        self.assert_invalid(packet, "holdout cardinality and order")


if __name__ == "__main__":
    unittest.main()
