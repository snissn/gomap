#!/usr/bin/env python3
"""Focused mutation checks for the #4587 C0 authorization and decision validator."""

from __future__ import annotations

import copy
import functools
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

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

@functools.cache
def fixture_go_runtime() -> dict[str, str]:
    values = json.loads(policy.run(
        "go", "env", "-json", "GOROOT", "GOPATH", "GOMODCACHE"))
    go_root = Path(values["GOROOT"]).resolve()
    go_executable = (go_root / "bin/go").resolve()
    version = policy.run(str(go_executable), "version")
    if not version.startswith("go version "):
        raise RuntimeError(f"unexpected Go version output: {version}")
    return {
        "go": version.removeprefix("go version "),
        "go_executable": str(go_executable),
        "go_sha256": policy.sha256_file(go_executable),
        "go_root": str(go_root),
        "go_path": str(Path(values["GOPATH"]).resolve()),
        "go_mod_cache": str(Path(values["GOMODCACHE"]).resolve()),
    }


class DecisionFixture:
    def __init__(self, root: Path):
        self.root = root
        self.contract = json.loads(CONTRACT_PATH.read_text())
        self.contract["commands"]["artifact_root"] = str(root.resolve())
        self.contract["source_identity"]["gomap_root"] = str(HERE.parent)
        runtime = self.contract["source_identity"]["runtime"]
        python_executable = Path(sys.executable).resolve()
        runtime["python_executable"] = str(python_executable)
        runtime["python_sha256"] = policy.sha256_file(python_executable)
        runtime.update(fixture_go_runtime())
        commands = self.contract["commands"]
        commands["lifecycle_vdbbench_environment_template"]["PYTHONPATH"] = (
            policy.os.pathsep.join((
                self.contract["source_identity"]["vectordbbench"]["root"],
                str(HERE.parent / "clients/python/treedb_client/src"),
            ))
        )
        for name in (
            "authorization_generate_argv_template",
            "authorized_preflight_argv",
            "winner_selection_generate_argv_template",
            "lifecycle_harness_argv_template",
            "decision_validation_argv",
        ):
            commands[name][0] = str(python_executable)
        search_commands = commands["existing_index_search"]
        search_commands["probe_argv_template"][0] = str(python_executable)
        search_commands["vectordbbench_common_argv_template"][0] = str(python_executable)
        lifecycle_argv = commands["lifecycle_harness_argv_template"]
        lifecycle_argv[lifecycle_argv.index("--python") + 1] = str(python_executable)
        self.counter = 0
        self.service_binary = root / "treedb-document-service"
        self.service_binary.write_bytes(b"fixture service binary\n")
        self.binary_sha = policy.sha256_file(self.service_binary)
        commands["binary"] = str(self.service_binary.resolve())
        commands["build_argv"] = [
            self.contract["source_identity"]["runtime"]["go_executable"],
            "build", "-trimpath", "-buildvcs=false", "-o",
            commands["binary"], "./cmd/treedb-document-service",
        ]
        self.storage_audit_binary = root / "treedb-column-section-audit"
        self.storage_audit_binary.write_bytes(b"fixture storage audit binary\n")
        self.storage_audit_binary_sha = policy.sha256_file(self.storage_audit_binary)
        commands["storage_audit_binary"] = str(self.storage_audit_binary.resolve())
        commands["storage_audit_build_argv"] = [
            self.contract["source_identity"]["runtime"]["go_executable"],
            "build", "-trimpath", "-buildvcs=false", "-o",
            commands["storage_audit_binary"], "./cmd/treedb_column_section_audit",
        ]
        authorization_argv = commands["authorization_generate_argv_template"]
        authorization_argv[authorization_argv.index("--service-binary") + 1] = commands["binary"]
        authorization_argv[authorization_argv.index("--storage-audit-binary") + 1] = (
            commands["storage_audit_binary"])
        lifecycle_argv[lifecycle_argv.index("--service-bin") + 1] = commands["binary"]
        lifecycle_argv[lifecycle_argv.index("--storage-audit-bin") + 1] = (
            commands["storage_audit_binary"])
        commands["build_environment"] = policy.expected_build_environment(self.contract)
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
                "build_argv": self.contract["commands"]["build_argv"],
                "build_environment": self.contract["commands"]["build_environment"],
                "go_version": f"go version {self.contract['source_identity']['runtime']['go']}",
            },
            "storage_audit_binary": {
                "path": str(self.storage_audit_binary.resolve()),
                "sha256": self.storage_audit_binary_sha,
                "build_argv": self.contract["commands"]["storage_audit_build_argv"],
                "build_environment": self.contract["commands"]["build_environment"],
                "go_version": f"go version {self.contract['source_identity']['runtime']['go']}",
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
        planning = {key: 0 for key in policy.OBSERVER_PHASE_KEYS}
        reciprocal = {key: 0 for key in policy.OBSERVER_PHASE_KEYS}
        for phase in (planning, reciprocal):
            phase["saturated"] = False
            for key in policy.OBSERVER_PHASE_KEYS:
                if key.endswith("_histogram"):
                    phase[key] = [0] * 16
        planning["decisions"] = 1
        reciprocal["decisions"] = 1
        construction_decisions = {"planning": planning, "reciprocal": reciprocal}
        optimize_start_ns = int((started + timedelta(seconds=30)).timestamp() * 1_000_000_000)
        optimize_end_ns = optimize_start_ns + int((adjacency + 10) * 1_000_000_000)
        adapter_path = root / "adapter-lifecycle.jsonl"
        adapter_path.write_text("".join(
            json.dumps(record, sort_keys=True) + "\n"
            for record in (
                {"event": "optimize_start", "timestamp_ns": optimize_start_ns},
                {
                    "event": "optimize_end",
                    "timestamp_ns": optimize_end_ns,
                    "response": {
                        "status": {
                            "column_graph_build": {
                                "adjacency_build_nanos": int(adjacency * 1_000_000_000),
                                "construction_decisions": construction_decisions,
                            },
                        },
                    },
                },
            )
        ))
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
                "host": {
                    "go": f"go version {self.contract['source_identity']['runtime']['go']}",
                    "python": self.contract["source_identity"]["runtime"]["python"] + " fixture",
                    "pyarrow": self.contract["source_identity"]["runtime"]["pyarrow"],
                    "cpu_brand": self.contract["source_identity"]["runtime"]["host_cpu"],
                    "physical_cpu_count": self.contract["source_identity"]["runtime"]["physical_cores"],
                    "logical_cpu_count": self.contract["source_identity"]["runtime"]["logical_cpus"],
                    "gomaxprocs": str(self.contract["source_identity"]["runtime"]["gomaxprocs"]),
                    "storage": {
                        "mount": self.contract["source_identity"]["runtime"]["storage_mount"],
                        "filesystem": self.contract["source_identity"]["runtime"]["filesystem"],
                    },
                },
                "gomap": {"commit": COMMIT, "dirty": False},
                "vectordbbench": {
                    "commit": self.contract["source_identity"]["vectordbbench"]["commit"],
                    "dirty": False,
                },
            },
            "service": {
                "base_url": "http://127.0.0.1:6060",
                "binary": {"sha256": binary_sha},
                "data_dir": str((root / "treedb-data").resolve()),
            },
            "lifecycle": {
                "schema_version": "treedb-vectordbbench-lifecycle/v1",
                "file": lifecycle_path.name,
                "sha256": policy.sha256_file(lifecycle_path),
                "result_status": "completed",
                "raw_artifacts": [{
                    "path": "adapter-lifecycle.jsonl",
                    "sha256": policy.sha256_file(adapter_path),
                }],
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
                    "sha256_after": dataset["train_sha256"],
                },
            },
            "harness": {
                "m": 16, "ef_construction": ef, "ef_search": 192, "k": 100,
                "rerank_candidates": 400, "rows": "scalar",
                "quantized_index_name": "fixture-index",
                "construction_decision_diagnostics": True,
                "python_executable": self.contract["source_identity"]["runtime"]["python_executable"],
                "python_sha256": self.contract["source_identity"]["runtime"]["python_sha256"],
                "use_uv": "off",
                "storage_audit_binary": {
                    "path": str(self.storage_audit_binary.resolve()),
                    "bytes": self.storage_audit_binary.stat().st_size,
                    "sha256": self.storage_audit_binary_sha,
                },
                "service_environment": self.contract[
                    "commands"]["lifecycle_service_environment"],
                "vdbbench_environments": {
                    "scalar": {
                        key: value.format(artifact_root=str(root), row="scalar")
                        for key, value in self.contract["commands"][
                            "lifecycle_vdbbench_environment_template"].items()
                    },
                },
            },
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        isolation = {
            "schema_version": policy.ISOLATION_SCHEMA,
            "artifact_root": str(root.resolve()),
            "lock_path": self.contract["experiment"]["isolation_and_noise"]["lock_path"],
            "lock_acquired_at": (started - timedelta(seconds=1)).isoformat(),
            "lock_held_through_evidence": True,
            "coverage_completed_at": (completed + timedelta(seconds=1)).isoformat(),
            "gomaxprocs": 12,
            "competing_processes": [],
            "peak_swap_used_bytes": 0,
            "samples": [
                {
                    "timestamp": (started + timedelta(seconds=offset)).isoformat(),
                    "swap_used_bytes": 0,
                    "competing_processes": [],
                }
                for offset in range(-1, 302, 5)
            ] + [{
                "timestamp": (completed + timedelta(seconds=1)).isoformat(),
                "swap_used_bytes": 0,
                "competing_processes": [],
            }],
        }
        isolation_binding = self._write(root / "isolation.json", isolation)
        data_root = root / "treedb-data"
        (data_root / "maindb").mkdir(parents=True)
        data_file = data_root / "maindb" / "index.db"
        data_file.write_bytes(b"x" * int(persisted))
        audit = {
            "schema_version": "treedb-column-section-audit/v2",
            "status": "passed",
            "collection": "fixture-index",
            "detailed_sections": False,
            "read_integrity": "verify",
            "owned_files": [{
                "path": str(data_file.resolve()),
                "bytes": data_file.stat().st_size,
                "domain": "index",
            }],
            "physical_accounting": {
                "complete": True,
                "collection": "fixture-index",
                "manifest_generation": 1,
                "recovery_manifest_generation": 1,
                "manifest_checksum": 123,
                "recovery_manifest_checksum": 123,
            },
            "storage_plan": {
                "before": [
                    {
                        "name": name,
                        "path": str((data_root / path).resolve()),
                        "bytes": data_file.stat().st_size if name in {"index", "total"} else 0,
                        "files": 1 if name in {"index", "total"} else 0,
                        "zero_byte_files": 0,
                    }
                    for name, path in (
                        ("index", "maindb/index.db"),
                        ("wal", "maindb/wal"),
                        ("value_vlog", "maindb/value_vlog"),
                        ("leaf_vlog", "maindb/leaf_vlog"),
                        ("total", "maindb"),
                    )
                ],
                "value_log_gc": {
                    "BytesTotal": 0, "SegmentsTotal": 0,
                    "BytesReferenced": 0, "SegmentsReferenced": 0,
                },
                "leaf_generation_plan": {"Generations": []},
            },
            "asset_lifecycle": {
                "reachability_complete": True,
                "reachability": {"Complete": True, "SegmentEntries": []},
            },
        }
        audit_path = root / "storage-ownership-audit.json"
        audit_path.write_text(json.dumps(audit, sort_keys=True) + "\n")
        manifest["lifecycle"]["storage_ownership_audit"] = audit_path.name
        manifest["lifecycle"]["raw_artifacts"].append({
            "path": audit_path.name,
            "sha256": policy.sha256_file(audit_path),
        })
        diagnostics_path = root / "diagnostics.jsonl"
        diagnostics = [
            {
                "boundary": "optimize_start",
                "timestamp_ns": optimize_start_ns,
                "process": {"cpu_nanoseconds": 1_000_000_000, "peak_rss_bytes": 100},
                "snapshot": {"treedb.process.memory.total_alloc_bytes": allocated},
            },
            {
                "boundary": "optimize_end",
                "timestamp_ns": optimize_end_ns,
                "process": {
                    "cpu_nanoseconds": 1_000_000_000 + int((adjacency + 10) * 6 * 1_000_000_000),
                    "peak_rss_bytes": 100,
                },
                "snapshot": {"treedb.process.memory.total_alloc_bytes": allocated},
            },
        ]
        diagnostics_path.write_text("".join(
            json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
            for record in diagnostics))
        manifest["lifecycle"]["raw_artifacts"].append({
            "path": diagnostics_path.name,
            "sha256": policy.sha256_file(diagnostics_path),
        })
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        data_files = [{
            "path": data_file.relative_to(data_root).as_posix(),
            "size": data_file.stat().st_size,
            "sha256": policy.sha256_file(data_file),
        }]
        measurement_source = {
            "schema_version": "treedb-construction-policy-4587-measurement-source/v4",
            "adapter_lifecycle": {
                "path": adapter_path.name, "sha256": policy.sha256_file(adapter_path),
            },
            "diagnostics": {
                "path": diagnostics_path.name, "sha256": policy.sha256_file(diagnostics_path),
            },
            "isolation": isolation_binding,
            "storage_ownership_audit": {
                "path": audit_path.name, "sha256": policy.sha256_file(audit_path),
            },
            "data_root": str(data_root.resolve()),
            "data_files": data_files,
            "audited_data_files": data_files,
        }
        measurement_source_binding = self._write(
            root / "measurement-source.json", measurement_source)
        work = construction_decisions
        resources = {
            "peak_rss_bytes": 100.0,
            "persisted_bytes": int(persisted),
            "cumulative_allocated_bytes": allocated,
        }
        measurements = {
            "schema_version": policy.MEASUREMENT_SCHEMA,
            "source": measurement_source_binding,
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
                "persisted_data_ledger_checksum": policy.canonical_sha256(data_files),
                "adapter_lifecycle_checksum": policy.sha256_file(adapter_path),
            },
            "diagnostic_work_profile": work,
            "resources": resources,
            "projected_10m_adjacency_reduction_fraction": None,
        }
        query_path = root / "query.json"
        query_path.write_text(json.dumps([0.0] * 768) + "\n")
        raw_search = [
            (*self._result(root, ef, kind, route, float(position + 1), dataset), kind, route)
            for position, (kind, route) in enumerate(policy.SEARCH_ORDER)
        ]
        command_records = []
        for position, (result, response, metadata, kind, route) in enumerate(raw_search):
            group_start = (position // 2) * 2
            diagnostic, production = raw_search[group_start:group_start + 2]
            probe_started = completed + timedelta(seconds=position * 4 + 1)
            probe_completed = probe_started + timedelta(seconds=1)
            command_started = probe_completed
            command_completed = command_started + timedelta(seconds=1)
            argv = [
                str(Path(self.contract["source_identity"]["gomap_root"]) /
                    policy.SEARCH_HELPER_PATH),
                "--base-url", "http://127.0.0.1:6060",
                "--index-name", f"index-ef{ef}",
                "--query-json", str(query_path),
                "--metadata-out", str(root / diagnostic[2]["path"]),
                "--diagnostic-response-out", str(root / diagnostic[1]["path"]),
                "--production-response-out", str(root / production[1]["path"]),
                "--diagnostic-result", str(root / diagnostic[0]["path"]),
                "--production-result", str(root / production[0]["path"]),
                "--diagnostic-origin-out", str(root / f"search-origin-{group_start}.json"),
                "--production-origin-out", str(root / f"search-origin-{group_start + 1}.json"),
                "--command-ledger", str(root / "probe-command-ledger.jsonl"),
                "--run-id", f"run-{self.counter}", "--artifact-root", str(root.resolve()),
                "--manifest-sha256", policy.sha256_file(manifest_path),
                "--execution-commit", COMMIT,
                "--dataset-sha256", policy.canonical_sha256(dataset),
                "--scale", str(scale), "--dataset-name", dataset["name"],
                "--dataset-dir", dataset["directory"],
                "--vectordbbench-dir", self.contract["source_identity"]["vectordbbench"]["root"],
                "--role", role, "--partition", partition,
                "--lifecycle-sha256", policy.sha256_file(lifecycle_path),
                "--lifecycle-started-at", started.isoformat(),
                "--lifecycle-completed-at", completed.isoformat(),
                "--ef-construction", str(ef), "--expected-generation", "1", "--route", route,
                "--service-bin", self.contract["commands"]["binary"],
                "--service-binary-sha256", binary_sha,
                "--python-executable", self.contract["source_identity"]["runtime"]["python_executable"],
                "--python-sha256", self.contract["source_identity"]["runtime"]["python_sha256"],
                "--search-isolation-out", str(root / f"search-isolation-{route}.json"),
                "--exclusive-lock", self.contract["experiment"]["isolation_and_noise"]["lock_path"],
                "--diagnostics-interval", "5", "--service-health-timeout", "60",
            ]
            command_records.append({
                "schema_version": "treedb-construction-policy-4587-probe-command/v5",
                "sequence": position,
                "argv": argv,
                "helper_sha256": policy.sha256_file(
                    HERE.parent / policy.SEARCH_HELPER_PATH),
                "python_executable": str(Path(
                    self.contract["source_identity"]["runtime"]["python_executable"]
                ).resolve()),
                "python_sha256": self.contract["source_identity"]["runtime"]["python_sha256"],
                "vdbbench_argv": [
                    self.contract["source_identity"]["runtime"]["python_executable"],
                ] + policy.expected_vdbbench_argv(
                    {"scale": scale, "role": role, "dataset": dataset, "configuration": config},
                    f"index-ef{ef}", kind, route, "http://127.0.0.1:6060"),
                "vdbbench_env": {
                    "GOMAXPROCS": "12",
                    "RESULTS_LOCAL_DIR": str(root / f"vdbbench-results-{route}-{kind}"),
                    "PYTHONPATH": search_existing_index.os.pathsep.join((
                        self.contract["source_identity"]["vectordbbench"]["root"],
                        str(HERE.parent / "clients/python/treedb_client/src"),
                    )),
                    "LOG_FILE": str(root / f"vdbbench-{route}-{kind}.log"),
                    "NUM_PER_BATCH": "500",
                },
                "kind": kind,
                "started_at": command_started.isoformat(),
                "completed_at": command_completed.isoformat(),
                "probe_started_at": probe_started.isoformat(),
                "probe_completed_at": probe_completed.isoformat(),
                "query_sha256": policy.sha256_file(query_path),
                "dataset_files_before_sha256": {
                    "test.parquet": dataset["test_sha256"],
                    "neighbors.parquet": dataset["neighbors_sha256"],
                },
                "dataset_files_after_sha256": {
                    "test.parquet": dataset["test_sha256"],
                    "neighbors.parquet": dataset["neighbors_sha256"],
                },
                "exit_code": 0,
                "run_id": f"run-{self.counter}",
                "route": route,
                "result_sha256": result["sha256"],
                "response_sha256": response["sha256"],
                "index_metadata_sha256": metadata["sha256"],
            })
        ledger_path = root / "probe-command-ledger.jsonl"
        ledger_path.write_text("".join(
            json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
            for record in command_records))
        service_argv = [
            self.contract["commands"]["binary"],
            "-dir", str(root / "treedb-data"), "-addr", "127.0.0.1:6060",
            "-profile", "command_wal_durable",
        ]
        route_isolation = {}
        for group, route in enumerate(("exact", "scalar_u8_rerank")):
            envelope_start = completed + timedelta(seconds=group * 8 + 0.5)
            envelope_completed = completed + timedelta(seconds=group * 8 + 8)
            samples = [
                {"timestamp": envelope_start.isoformat(), "swap_used_bytes": 0,
                 "competing_processes": []},
                {"timestamp": (envelope_start + timedelta(seconds=5)).isoformat(),
                 "swap_used_bytes": 0, "competing_processes": []},
                {"timestamp": envelope_completed.isoformat(), "swap_used_bytes": 0,
                 "competing_processes": []},
            ]
            route_isolation[route] = self._write(
                root / f"search-isolation-{route}.json",
                {
                    "schema_version": policy.SEARCH_ISOLATION_SCHEMA,
                    "artifact_root": str(root.resolve()),
                    "lock_path": self.contract["experiment"]["isolation_and_noise"]["lock_path"],
                    "lock_acquired_at": (envelope_start - timedelta(seconds=0.1)).isoformat(),
                    "coverage_completed_at": envelope_completed.isoformat(),
                    "gomaxprocs": 12,
                    "service_environment": self.contract[
                        "commands"]["existing_index_search"]["service_environment"],
                    "service_binary_sha256": binary_sha,
                    "service_argv": service_argv,
                    "service_started_at": (envelope_start + timedelta(seconds=0.1)).isoformat(),
                    "service_completed_at": (envelope_completed - timedelta(seconds=0.1)).isoformat(),
                    "service_exit_code": 0,
                    "samples": samples,
                })
        search = []
        for position, (result, response, metadata, kind, route) in enumerate(raw_search):
            command = command_records[position]
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
                "command_ledger_path": ledger_path.name,
                "command_sequence": command["sequence"],
                "command_record_sha256": policy.canonical_sha256(command),
                "probe_started_at": command["probe_started_at"],
                "probe_completed_at": command["probe_completed_at"],
                "service_binary_sha256": binary_sha,
                "service_argv": service_argv,
                "isolation": route_isolation[route],
                "search_started_at": (
                    completed + timedelta(seconds=(0 if route == "exact" else 8) + 0.5)
                ).isoformat(),
                "search_completed_at": (
                    completed + timedelta(seconds=(0 if route == "exact" else 8) + 8)
                ).isoformat(),
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
            "isolation_evidence": isolation_binding,
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
            search_origin_path = (
                Path(run["artifact"]["root"]) / run["search_evidence"][-1]["origin"]["path"]
            )
            search_completed_at = json.loads(search_origin_path.read_text())["search_completed_at"]
            completed.append(policy.utc_timestamp(search_completed_at, "fixture search completion"))
            screening_rows.append({
                "run_id": run["run_id"],
                "artifact_root": run["artifact"]["root"],
                "measurement_sha256": run["measurement_evidence"]["sha256"],
                "lifecycle_sha256": origin["lifecycle_sha256"],
                "completed_at": origin["lifecycle_completed_at"],
                "search_completed_at": search_completed_at,
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
        if linked:
            ledger_path = Path(run["artifact"]["root"]) / "probe-command-ledger.jsonl"
            records = [json.loads(line) for line in ledger_path.read_text().splitlines()]
            for position, record in enumerate(records):
                entry = run["search_evidence"][position]
                record["result_sha256"] = entry["result"]["sha256"]
                record["response_sha256"] = entry["response"]["sha256"]
                record["index_metadata_sha256"] = entry["index_metadata"]["sha256"]
            ledger_path.write_text("".join(
                json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
                for record in records))
            for position, entry in enumerate(run["search_evidence"]):
                origin_binding = entry["origin"]
                origin_path = Path(run["artifact"]["root"]) / origin_binding["path"]
                origin = json.loads(origin_path.read_text())
                for key in ("result", "response", "index_metadata"):
                    origin[f"{key}_sha256"] = entry[key]["sha256"]
                origin["command_record_sha256"] = policy.canonical_sha256(records[position])
                origin_path.write_text(json.dumps(origin, sort_keys=True) + "\n")
                origin_binding["sha256"] = policy.sha256_file(origin_path)


    def rewrite_command(self, run: dict, sequence: int, mutate) -> None:
        root = Path(run["artifact"]["root"])
        path = root / "probe-command-ledger.jsonl"
        records = [json.loads(line) for line in path.read_text().splitlines()]
        mutate(records[sequence])
        path.write_text("".join(
            json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
            for record in records))
        binding = run["search_evidence"][sequence]["origin"]
        origin_path = root / binding["path"]
        origin = json.loads(origin_path.read_text())
        origin["command_record_sha256"] = policy.canonical_sha256(records[sequence])
        origin["probe_started_at"] = records[sequence]["probe_started_at"]
        origin["probe_completed_at"] = records[sequence]["probe_completed_at"]
        origin_path.write_text(json.dumps(origin, sort_keys=True) + "\n")
        binding["sha256"] = policy.sha256_file(origin_path)


    def rewrite_search_isolation(self, run: dict, route: str, mutate) -> None:
        root = Path(run["artifact"]["root"])
        entries = [entry for entry in run["search_evidence"] if entry["route"] == route]
        first_origin_path = root / entries[0]["origin"]["path"]
        first_origin = json.loads(first_origin_path.read_text())
        isolation_path = root / first_origin["isolation"]["path"]
        isolation = json.loads(isolation_path.read_text())
        mutate(isolation)
        isolation_path.write_text(json.dumps(isolation, sort_keys=True) + "\n")
        isolation_sha = policy.sha256_file(isolation_path)
        for entry in entries:
            origin_path = root / entry["origin"]["path"]
            origin = json.loads(origin_path.read_text())
            origin["isolation"]["sha256"] = isolation_sha
            origin_path.write_text(json.dumps(origin, sort_keys=True) + "\n")
            entry["origin"]["sha256"] = policy.sha256_file(origin_path)


    def rewrite_adapter(self, run: dict, mutate) -> None:
        root = Path(run["artifact"]["root"])
        path = root / "adapter-lifecycle.jsonl"
        records = [json.loads(line) for line in path.read_text().splitlines()]
        mutate(records)
        path.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        binding = next(
            item for item in manifest["lifecycle"]["raw_artifacts"]
            if item["path"] == "adapter-lifecycle.jsonl"
        )
        binding["sha256"] = policy.sha256_file(path)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        run["artifact"]["manifest_sha256"] = policy.sha256_file(manifest_path)
        source_binding = run["measurement_evidence"]
        measurement_path = root / source_binding["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        source["adapter_lifecycle"]["sha256"] = policy.sha256_file(path)
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["determinism"]["adapter_lifecycle_checksum"] = policy.sha256_file(path)
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        source_binding["sha256"] = policy.sha256_file(measurement_path)


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
        started_at = policy.utc_timestamp(started, "fixture lifecycle start")
        completed_at = policy.utc_timestamp(completed, "fixture lifecycle completion")
        isolation_path = root / run["isolation_evidence"]["path"]
        isolation = json.loads(isolation_path.read_text())
        isolation["lock_acquired_at"] = (started_at - timedelta(seconds=1)).isoformat()
        isolation["coverage_completed_at"] = (completed_at + timedelta(seconds=1)).isoformat()
        isolation["samples"] = [
            {
                "timestamp": (started_at + timedelta(seconds=offset)).isoformat(),
                "swap_used_bytes": 0,
                "competing_processes": [],
            }
            for offset in range(-1, int((completed_at - started_at).total_seconds()) + 2, 5)
        ]
        if isolation["samples"][-1]["timestamp"] != isolation["coverage_completed_at"]:
            isolation["samples"].append({
                "timestamp": isolation["coverage_completed_at"],
                "swap_used_bytes": 0,
                "competing_processes": [],
            })
        isolation_path.write_text(json.dumps(isolation, sort_keys=True) + "\n")
        run["isolation_evidence"]["sha256"] = policy.sha256_file(isolation_path)
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        source["isolation"] = run["isolation_evidence"]
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
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
        for sequence in range(4):
            probe_started = completed_at + timedelta(seconds=sequence * 4 + 1)
            def mutate_command(record, probe_started=probe_started):
                record["probe_started_at"] = probe_started.isoformat()
                record["probe_completed_at"] = (probe_started + timedelta(seconds=1)).isoformat()
                record["started_at"] = record["probe_completed_at"]
                record["completed_at"] = (probe_started + timedelta(seconds=2)).isoformat()
                values = {
                    "--manifest-sha256": manifest_sha,
                    "--lifecycle-sha256": lifecycle_sha,
                    "--lifecycle-started-at": started,
                    "--lifecycle-completed-at": completed,
                }
                for flag, value in values.items():
                    record["argv"][record["argv"].index(flag) + 1] = value
            self.rewrite_command(run, sequence, mutate_command)


class ValidatorMutations(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self._fixture: DecisionFixture | None = None

    @property
    def fixture(self) -> DecisionFixture:
        if self._fixture is None:
            self._fixture = DecisionFixture(Path(self.temp.name))
        return self._fixture

    def tearDown(self) -> None:
        self.temp.cleanup()

    def assert_invalid(self, packet: dict, pattern: str) -> None:
        with self.assertRaisesRegex(ValueError, pattern):
            self.fixture.validate(packet)

    def storage_audit_inputs(self) -> tuple[Path, dict, dict, Path, list[dict]]:
        run = self.fixture.no_go_packet()["runs"][0]
        root = Path(run["artifact"]["root"])
        manifest = json.loads((root / "manifest.json").read_text())
        measurements = json.loads(
            (root / run["measurement_evidence"]["path"]).read_text())
        source = json.loads((root / measurements["source"]["path"]).read_text())
        return (
            root,
            source["storage_ownership_audit"],
            manifest,
            Path(source["data_root"]),
            source["data_files"],
        )

    def test_storage_ownership_audit_must_pass(self) -> None:
        root, binding, manifest, data_root, data_files = self.storage_audit_inputs()
        audit_path = root / binding["path"]
        audit = json.loads(audit_path.read_text())
        audit["status"] = "failed"
        audit_path.write_text(json.dumps(audit, sort_keys=True) + "\n")
        binding["sha256"] = policy.sha256_file(audit_path)
        next(item for item in manifest["lifecycle"]["raw_artifacts"]
             if item["path"] == binding["path"])["sha256"] = binding["sha256"]
        with self.assertRaisesRegex(ValueError, "audit status"):
            policy.validate_storage_ownership_audit(
                root, binding, manifest, data_root, data_files)

    def test_storage_ownership_audit_requires_every_column_segment(self) -> None:
        root, binding, manifest, data_root, data_files = self.storage_audit_inputs()
        segment = (
            data_root / "maindb" / "column_assets" / "fixture-index"
            / "column-assets" / "assets" / "segments" / "segment-000001.tca"
        )
        segment.parent.mkdir(parents=True)
        segment.write_bytes(b"owned")
        data_files.append({
            "path": segment.relative_to(data_root).as_posix(),
            "size": segment.stat().st_size,
            "sha256": policy.sha256_file(segment),
        })
        with self.assertRaisesRegex(ValueError, "complete column segment ledger"):
            policy.validate_storage_ownership_audit(
                root, binding, manifest, data_root, data_files)

    def test_valid_no_go(self) -> None:
        self.assertEqual(self.fixture.validate(self.fixture.no_go_packet())["verdict"], "C0_NO_GO")

    def test_authorization_service_build_argv_is_frozen(self) -> None:
        contract = copy.deepcopy(self.fixture.contract)
        contract["commands"]["build_argv"][2] = "-race"
        with self.assertRaisesRegex(ValueError, "authorization service build argv"):
            policy.validate_python_command_contract(contract)

        for variable, value in (
            ("GOFLAGS", "-overlay=/tmp/unreviewed.json"),
            ("GOEXPERIMENT", "arenas"),
            ("GOTOOLCHAIN", "auto"),
            ("GOWORK", "/tmp/unreviewed.work"),
            ("CGO_ENABLED", "1"),
        ):
            with self.subTest(variable=variable):
                contract = copy.deepcopy(self.fixture.contract)
                contract["commands"]["build_environment"][variable] = value
                with self.assertRaisesRegex(
                    ValueError, "authorization service build environment"
                ):
                    policy.validate_python_command_contract(contract)
    def test_authorization_builds_service_from_reviewed_checkout(self) -> None:
        contract = copy.deepcopy(self.fixture.contract)
        service_binary = self.fixture.root / "built-service"
        contract["commands"]["binary"] = str(service_binary)
        contract["commands"]["build_argv"] = [
            contract["source_identity"]["runtime"]["go_executable"],
            "build", "-trimpath", "-buildvcs=false", "-o", str(service_binary),
            "./cmd/treedb-document-service",
        ]
        storage_audit_binary = self.fixture.root / "built-storage-audit"
        contract["commands"]["storage_audit_binary"] = str(storage_audit_binary)
        contract["commands"]["storage_audit_build_argv"] = [
            contract["source_identity"]["runtime"]["go_executable"],
            "build", "-trimpath", "-buildvcs=false", "-o", str(storage_audit_binary),
            "./cmd/treedb_column_section_audit",
        ]
        contract["commands"]["build_environment"] = policy.expected_build_environment(contract)
        calls = []

        def execute(*argv: str, cwd: Path,
                    env: dict[str, str] | None = None) -> str:
            calls.append((argv, env))
            if argv == ("git", "rev-parse", "HEAD"):
                return COMMIT
            if argv == ("git", "status", "--porcelain=v1"):
                return ""
            if argv == (contract["commands"]["build_argv"][0], "version"):
                self.assertEqual(env, contract["commands"]["build_environment"])
                return f"go version {contract['source_identity']['runtime']['go']}"
            if argv == (contract["commands"]["build_argv"][0], "mod", "verify"):
                self.assertEqual(env, contract["commands"]["build_environment"])
                return "all modules verified"
            if argv == tuple(contract["commands"]["build_argv"]):
                self.assertEqual(env, contract["commands"]["build_environment"])
                service_binary.write_bytes(b"binary built by frozen command\n")
                return ""
            if argv == tuple(contract["commands"]["storage_audit_build_argv"]):
                self.assertEqual(env, contract["commands"]["build_environment"])
                storage_audit_binary.write_bytes(b"audit binary built by frozen command\n")
                return ""
            raise AssertionError(f"unexpected command: {argv}")

        authorization_path = self.fixture.root / "generated-authorization.json"
        with mock.patch.object(policy, "validate_contract"), mock.patch.object(
            policy, "run", side_effect=execute
        ):
            generated = policy.generate_authorization(
                contract, authorization_path, service_binary, storage_audit_binary, COMMIT)
        self.assertIn(
            (tuple(contract["commands"]["build_argv"]),
             contract["commands"]["build_environment"]),
            calls)
        self.assertIn(
            (tuple(contract["commands"]["storage_audit_build_argv"]),
             contract["commands"]["build_environment"]),
            calls)
        verify_call = (
            (contract["commands"]["build_argv"][0], "mod", "verify"),
            contract["commands"]["build_environment"],
        )
        self.assertIn(verify_call, calls)
        self.assertLess(calls.index(verify_call), calls.index(
            (tuple(contract["commands"]["build_argv"]),
             contract["commands"]["build_environment"])))
        self.assertEqual(generated["execution_commit"], COMMIT)
        self.assertEqual(
            generated["service_binary_sha256"], policy.sha256_file(service_binary))
        self.assertEqual(
            generated["storage_audit_binary_sha256"],
            policy.sha256_file(storage_audit_binary))

    def test_winner_generation_binds_authorized_storage_audit(self) -> None:
        packet = self.fixture.go_packet()
        rows = packet["runs"][:4]
        authorization = policy.validate_authorization(
            self.fixture.contract, self.fixture.authorization_path,
            require_clean_head=False,
        )
        validated = [
            policy.validate_run(
                row, self.fixture.contract, COMMIT, False,
                authorization["protocol_files"][policy.SEARCH_HELPER_PATH],
                authorization["storage_audit_binary_sha256"],
            )
            for row in rows
        ]
        runs_path = self.fixture.root / "screening-runs-source.json"
        runs_path.write_text(json.dumps(rows, sort_keys=True) + "\n")
        output_path = self.fixture.root / "generated-winner-selection.json"
        with mock.patch.object(
            policy, "validate_authorization", return_value=authorization,
        ), mock.patch.object(policy, "validate_run", side_effect=validated) as validate_run:
            binding = policy.generate_winner_selection(
                self.fixture.contract, runs_path, output_path,
                self.fixture.authorization_path,
            )
        event = json.loads(output_path.read_text())
        self.assertEqual(event["selected_ef_construction"], 128)
        self.assertEqual(binding["sha256"], policy.sha256_file(output_path))
        self.assertEqual(validate_run.call_count, 4)
        for call in validate_run.call_args_list:
            self.assertEqual(
                call.args[5], authorization["storage_audit_binary_sha256"])

    def test_partition_rows_must_match_canonical_ordinals(self) -> None:
        import pyarrow as arrow
        import pyarrow.parquet as parquet

        source = self.fixture.contract["datasets"]["canonical_query_source"]
        canonical_test = parquet.read_table(source["test_path"])
        canonical_neighbors = parquet.read_table(source["neighbors_path"])
        ids = canonical_test.column("id").to_pylist()
        selection, holdout = policy.partition_ordinals(
            ids, self.fixture.contract["datasets"]["partition"]["seed"])
        for corrupted_file in ("test.parquet", "neighbors.parquet"):
            with self.subTest(corrupted_file=corrupted_file):
                contract = copy.deepcopy(self.fixture.contract)
                for name, ordinals in (("screening", selection), ("decision", holdout)):
                    directory = self.fixture.root / f"{corrupted_file}-{name}"
                    directory.mkdir(parents=True)
                    test_rows = canonical_test.take(arrow.array(ordinals))
                    neighbor_rows = canonical_neighbors.take(arrow.array(ordinals))
                    if name == "screening":
                        wrong_ordinals = list(ordinals)
                        wrong_ordinals[0] = holdout[0]
                        if corrupted_file == "test.parquet":
                            wrong = canonical_test.take(arrow.array(wrong_ordinals))
                            id_index = wrong.schema.get_field_index("id")
                            test_rows = wrong.set_column(
                                id_index, wrong.schema.field(id_index),
                                test_rows.column("id"))
                        else:
                            wrong = canonical_neighbors.take(arrow.array(wrong_ordinals))
                            id_index = wrong.schema.get_field_index("id")
                            neighbor_rows = wrong.set_column(
                                id_index, wrong.schema.field(id_index),
                                neighbor_rows.column("id"))
                    parquet.write_table(test_rows, directory / "test.parquet")
                    parquet.write_table(neighbor_rows, directory / "neighbors.parquet")
                    config = contract["datasets"][name]
                    config["directory"] = str(directory)
                    config["files"] = {
                        filename: policy.sha256_file(directory / filename)
                        for filename in ("test.parquet", "neighbors.parquet")
                    }

                with self.assertRaisesRegex(ValueError, "rows do not match canonical"):
                    policy.verify_datasets(contract)

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

    def test_authorization_schema_matches_protocol_files(self) -> None:
        protocol_schema = self.fixture.contract[
            "execution_authorization_schema"]["properties"]["protocol_files"]
        self.assertEqual(protocol_schema["required"], list(policy.PROTOCOL_PATHS))
        self.assertEqual(set(protocol_schema["properties"]), set(policy.PROTOCOL_PATHS))

    def test_authorization_binds_commit_contract_and_source_identities(self) -> None:
        mutations = [
            ("authorized execution commit", lambda value: value.update({"execution_commit": "3" * 40})),
            ("contract SHA-256", lambda value: value.update({"contract_sha256": "0" * 64})),
            ("source identity", lambda value: value["source_identity"]["runtime"].update({"logical_cpus": 11})),
            ("source identity", lambda value: value["source_identity"]["harness_blobs"].update(
                {"scripts/treedb_vectordbbench_artifact.py": "0" * 40})),
            ("source identity", lambda value: value["source_identity"]["vectordbbench"].update(
                {"commit": "0" * 40})),
            ("authorization service build argv", lambda value: value["service_binary"][
                "build_argv"].append("-race")),
            ("authorization service build environment", lambda value: value["service_binary"][
                "build_environment"].update({"GOFLAGS": "-overlay=/tmp/unreviewed.json"})),
            ("authorization Go toolchain version", lambda value: value["service_binary"].update(
                {"go_version": "go version go1.25.0 linux/amd64"})),
            ("authorized storage-audit binary SHA-256", lambda value: value["storage_audit_binary"].update(
                {"sha256": "0" * 64})),
            ("authorization storage-audit build argv", lambda value: value["storage_audit_binary"][
                "build_argv"].append("-race")),
            ("authorization storage-audit build environment", lambda value: value["storage_audit_binary"][
                "build_environment"].update({"GOFLAGS": "-overlay=/tmp/unreviewed.json"})),
            ("authorization storage-audit Go toolchain version",
             lambda value: value["storage_audit_binary"].update(
                 {"go_version": "go version go1.25.0 linux/amd64"})),
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
        for sequence in range(4):
            self.fixture.rewrite_command(
                run, sequence, lambda value: value["argv"].__setitem__(
                    value["argv"].index("--manifest-sha256") + 1,
                    run["artifact"]["manifest_sha256"],
                ))
        self.assert_invalid(packet, "authorized service binary")

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
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        measurement["resources"]["persisted_bytes"] = 101
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "resources raw source binding")

    def test_duplicate_persisted_data_paths_are_rejected(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        source["data_files"].append(copy.deepcopy(source["data_files"][0]))
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["resources"]["persisted_bytes"] *= 2
        measurement["determinism"]["persisted_data_ledger_checksum"] = policy.canonical_sha256(
            source["data_files"])
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "data file paths must be unique")

    def test_persisted_data_symlink_cannot_escape_data_root(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        data_root = Path(source["data_root"])
        external = root / "external-data"
        external.write_bytes(b"unrelated external storage")
        link = data_root / "maindb" / "format.json"
        link.symlink_to(external)
        source["data_files"].append({
            "path": link.relative_to(data_root).as_posix(),
            "size": external.stat().st_size,
            "sha256": policy.sha256_file(external),
        })
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["resources"]["persisted_bytes"] += external.stat().st_size
        measurement["determinism"]["persisted_data_ledger_checksum"] = policy.canonical_sha256(
            source["data_files"])
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "data file path must remain inside data root")

    def test_persisted_data_hard_links_cannot_double_count_storage(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        data_root = Path(source["data_root"])
        original = data_root / source["data_files"][0]["path"]
        hard_link = data_root / "maindb" / "format.json"
        hard_link.hardlink_to(original)
        source["data_files"].append({
            "path": hard_link.relative_to(data_root).as_posix(),
            "size": original.stat().st_size,
            "sha256": policy.sha256_file(original),
        })
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["resources"]["persisted_bytes"] += original.stat().st_size
        measurement["determinism"]["persisted_data_ledger_checksum"] = policy.canonical_sha256(
            source["data_files"])
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "data file link count")

    def test_persisted_data_hard_link_outside_root_is_rejected(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        data_root = Path(source["data_root"])
        external = root / "external-owned-data"
        external.write_bytes(b"unrelated external storage")
        hard_link = data_root / "maindb" / "format.json"
        hard_link.hardlink_to(external)
        source["data_files"].append({
            "path": hard_link.relative_to(data_root).as_posix(),
            "size": external.stat().st_size,
            "sha256": policy.sha256_file(external),
        })
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["resources"]["persisted_bytes"] += external.stat().st_size
        measurement["determinism"]["persisted_data_ledger_checksum"] = policy.canonical_sha256(
            source["data_files"])
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "data file link count")

    def test_unrelated_regular_file_cannot_inflate_persisted_storage(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        data_root = Path(source["data_root"])
        padding = data_root / "unrelated-padding.bin"
        padding.write_bytes(b"x" * 100)
        source["data_files"].append({
            "path": padding.name,
            "size": padding.stat().st_size,
            "sha256": policy.sha256_file(padding),
        })
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement["resources"]["persisted_bytes"] += padding.stat().st_size
        measurement["determinism"]["persisted_data_ledger_checksum"] = policy.canonical_sha256(
            source["data_files"])
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "unexpected TreeDB data file path")

    def test_allowed_control_padding_is_not_counted_as_persisted_data(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][4]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        data_root = Path(source["data_root"])
        padding = data_root / "maindb" / "LOCK"
        padding.write_bytes(b"x" * 1000)
        source["data_files"].append({
            "path": padding.relative_to(data_root).as_posix(),
            "size": padding.stat().st_size,
            "sha256": policy.sha256_file(padding),
        })
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)

        self.assertEqual(self.fixture.validate(packet)["verdict"], "GO")

    def test_treedb_data_file_allowlist_matches_canonical_layout(self) -> None:
        allowed = (
            "maindb/index.db",
            "dictdb/format.json",
            "maindb/vlog_health.json",
            "maindb/wal/commit-l0-000001.log",
            "maindb/wal/command-wal-journal-owner.lock",
            "maindb/value_vlog/value-l0-000001.log",
            "maindb/leaf_vlog/value-l255-000018.log.lenidx",
            "maindb/leaf_vlog/manifest.durable.0000000000000005.json",
            "maindb/column_assets/example/column-assets/assets/segments/segment-1048579.tca",
        )
        self.assertTrue(all(policy.is_treedb_data_file(Path(path)) for path in allowed))
        self.assertFalse(policy.is_treedb_data_file(Path("unrelated-padding.bin")))

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
        self.assert_invalid(packet, "raw source binding")
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value["determinism"].update({"persisted_data_ledger_checksum": "6" * 64}),
        )
        self.assert_invalid(packet, "raw source binding")

    def test_graph_config_checksum_binding(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["measurement_evidence"]
        self.fixture.rewrite_bound(
            run,
            binding,
            lambda value: value["determinism"].update({"graph_config_checksum": "6" * 64}),
        )
        self.assert_invalid(packet, "raw source binding")

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
        self.assert_invalid(packet, "after all screening search envelopes")
        packet = self.fixture.go_packet()
        screening_measurement = Path(packet["runs"][3]["artifact"]["root"]) / "measurements.json"
        screening_completed = policy.utc_timestamp(
            json.loads(screening_measurement.read_text())["origin"]["lifecycle_completed_at"],
            "screening completion")
        self.fixture.rewrite_winner_selection(
            packet, lambda value: value.update({
                "selected_at": (screening_completed + timedelta(seconds=1)).isoformat(),
            }))
        self.assert_invalid(packet, "after all screening search envelopes")

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

    def test_existing_index_helper_emits_bound_search_origin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            result = root / "result.json"
            response = root / "response.json"
            metadata = root / "metadata.json"
            output = root / "origin.json"
            for path, value in (
                (result, {"result": 1}), (response, {"response": 1}), (metadata, {"metadata": 1}),
            ):
                path.write_text(json.dumps(value, sort_keys=True) + "\n")
            command = {
                "sequence": 0,
                "probe_started_at": "2026-09-02T00:02:00+00:00",
                "probe_completed_at": "2026-09-02T00:02:01+00:00",
            }
            args = search_existing_index.argparse.Namespace(
                run_id="run-1", artifact_root=root, manifest_sha256="1" * 64,
                execution_commit=COMMIT, dataset_sha256="2" * 64, scale=250000,
                role="screening_candidate", partition="selection", ef_construction=128,
                lifecycle_sha256="3" * 64, lifecycle_started_at="2026-09-02T00:00:00+00:00",
                lifecycle_completed_at="2026-09-02T00:01:00+00:00", route="exact",
                metadata_out=metadata, command_ledger=root / "probe-command-ledger.jsonl",
                service_bin=Path("/authorized/service"), service_binary_sha256="4" * 64,
                service_addr="127.0.0.1:6060",
            )
            search_existing_index.write_origin(
                args, "production", result, response, output, command,
                {"path": "search-isolation-exact.json", "sha256": "5" * 64},
                "2026-09-02T00:01:01+00:00", "2026-09-02T00:03:00+00:00")
            origin = json.loads(output.read_text())
            self.assertEqual(origin["result_sha256"], search_existing_index.sha256_file(result))
            self.assertEqual(origin["response_sha256"], search_existing_index.sha256_file(response))
            self.assertEqual(origin["index_metadata_sha256"], search_existing_index.sha256_file(metadata))
            self.assertEqual(
                set(origin),
                policy.SEARCH_ORIGIN_KEYS,
            )

    def test_probe_command_ledger_and_chronology_are_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["argv"].__setitem__(
                value["argv"].index("--run-id") + 1, "other-run"))
        self.assert_invalid(packet, "executed command identity")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["argv"].__setitem__(
                0, str(Path(run["artifact"]["root"]) /
                       policy.SEARCH_HELPER_PATH)))
        self.assert_invalid(packet, "authorized existing-index helper")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value.update({"helper_sha256": "0" * 64}))
        self.assert_invalid(packet, "authorized helper SHA-256")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value.update({
                "python_executable": "/tmp/unreviewed-python"}))
        self.assert_invalid(packet, "frozen helper Python interpreter")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value.update({"python_sha256": "0" * 64}))
        self.assert_invalid(packet, "frozen helper Python checksum")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["dataset_files_before_sha256"].update({
                "test.parquet": "0" * 64}))
        self.assert_invalid(packet, "pre-command dataset checksums")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["dataset_files_after_sha256"].update({
                "neighbors.parquet": "0" * 64}))
        self.assert_invalid(packet, "post-command dataset checksums")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["vdbbench_env"].update({"PYTHONHASHSEED": "1"}))
        self.assert_invalid(packet, "canonical VectorDBBench environment")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        lifecycle_completed = json.loads(
            (Path(run["artifact"]["root"]) / "measurements.json").read_text()
        )["origin"]["lifecycle_completed_at"]
        self.fixture.rewrite_command(
            run, 0, lambda value: value.update({
                "probe_started_at": lifecycle_completed,
                "probe_completed_at": lifecycle_completed,
                "started_at": lifecycle_completed,
                "completed_at": lifecycle_completed,
            }))
        self.assert_invalid(packet, "immediately precede")

    def test_existing_index_vdbbench_environment_is_non_inherited(self) -> None:
        args = SimpleNamespace(
            gomaxprocs=12,
            vectordbbench_dir=Path("/vectordbbench"),
            artifact_root=Path("/artifact"),
            route="exact",
        )
        with mock.patch.dict(
            search_existing_index.os.environ,
            {"PYTHONHASHSEED": "random", "HTTPS_PROXY": "http://proxy"},
            clear=False,
        ):
            environment = search_existing_index.vdbbench_environment(
                args, "production", Path("/artifact/results"))
        self.assertEqual(set(environment), {
            "GOMAXPROCS", "RESULTS_LOCAL_DIR", "PYTHONPATH", "LOG_FILE", "NUM_PER_BATCH",
        })
    def test_canonical_digest_matches_existing_index_producer(self) -> None:
        value = {"a": 1}
        self.assertEqual(policy.canonical_sha256(value), search_existing_index.canonical_sha256(value))

    def test_existing_index_helper_requires_frozen_gomaxprocs(self) -> None:
        with mock.patch.dict(search_existing_index.os.environ, {"GOMAXPROCS": "12"}):
            self.assertEqual(search_existing_index.frozen_gomaxprocs(), 12)
        with mock.patch.dict(search_existing_index.os.environ, {"GOMAXPROCS": "8"}):
            with self.assertRaisesRegex(ValueError, "frozen value 12"):
                search_existing_index.frozen_gomaxprocs()

    def test_existing_index_helper_requires_clean_service_teardown(self) -> None:
        process = mock.Mock(pid=123, returncode=None)
        process.poll.return_value = None
        process.wait.return_value = 0
        with mock.patch.object(search_existing_index.os, "killpg"):
            self.assertEqual(search_existing_index.stop_service(process), 0)

        process = mock.Mock(pid=123, returncode=2)
        process.poll.return_value = None
        process.wait.return_value = 2
        with mock.patch.object(search_existing_index.os, "killpg"):
            with self.assertRaisesRegex(ValueError, "did not close cleanly"):
                search_existing_index.stop_service(process)

        process = mock.Mock(pid=123, returncode=-9)
        process.poll.return_value = None
        process.wait.side_effect = [
            search_existing_index.subprocess.TimeoutExpired("service", 30),
            -9,
        ]
        with mock.patch.object(search_existing_index.os, "killpg"):
            with self.assertRaisesRegex(RuntimeError, "did not close within"):
                search_existing_index.stop_service(process)


    def test_existing_index_helper_executes_and_records_canonical_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            query = root / "query.json"
            response = root / "response.json"
            metadata = root / "metadata.json"
            dataset_dir = root / "dataset"
            dataset_dir.mkdir()
            (dataset_dir / "test.parquet").write_bytes(b"test")
            (dataset_dir / "neighbors.parquet").write_bytes(b"neighbors")
            for path in (query, response, metadata):
                path.write_text("{}\n")
            args = search_existing_index.argparse.Namespace(
                route="exact",
                gomaxprocs=12,
                base_url="http://127.0.0.1:6060",
                index_name="index-ef128",
                m=16,
                ef_construction=128,
                ef_search=192,
                top_k=100,
                scale=250000,
                role="screening_candidate",
                dataset_name="cohere-medium-250k-selection-v1",
                dataset_dir=dataset_dir,
                dimensions=768,
                quantized_index_name="embedding.scalar_u8.fast",
                rerank_candidates=400,
                artifact_root=root,
                vectordbbench_dir=root,
                diagnostic_result=root / "diagnostic-result.json",
                production_result=root / "production-result.json",
                command_ledger=root / "probe-command-ledger.jsonl",
                query_json=query,
                run_id="screening-ef128",
                metadata_out=metadata,
                python_executable=Path("/usr/bin/python3"),
            )

            def execute(command, *, cwd, env):
                result_root = Path(env["RESULTS_LOCAL_DIR"])
                result_root.mkdir()
                (result_root / "result_generated.json").write_text('{"result":true}\n')
                return search_existing_index.argparse.Namespace(returncode=0)

            with mock.patch.object(
                search_existing_index.subprocess, "run", side_effect=execute
            ) as invoked:
                result, record = search_existing_index.run_vdbbench_command(
                    args,
                    "diagnostic",
                    response,
                    "2026-09-02T00:00:00Z",
                    "2026-09-02T00:00:01Z",
                )

            self.assertEqual(invoked.call_count, 1)
            self.assertEqual(record["dataset_files_before_sha256"], {
                "test.parquet": search_existing_index.sha256_file(
                    dataset_dir / "test.parquet"),
                "neighbors.parquet": search_existing_index.sha256_file(
                    dataset_dir / "neighbors.parquet"),
            })
            self.assertEqual(
                record["dataset_files_after_sha256"],
                record["dataset_files_before_sha256"])

            self.assertEqual(result, args.diagnostic_result)
            self.assertEqual(record["result_sha256"], search_existing_index.sha256_file(result))
            self.assertEqual(
                record["argv"][0], str(Path(search_existing_index.__file__).resolve()))
            self.assertEqual(
                record["helper_sha256"],
                search_existing_index.sha256_file(Path(search_existing_index.__file__).resolve()))
            self.assertEqual(
                record["python_executable"], str(Path(sys.executable).resolve()))
            self.assertEqual(
                record["python_sha256"],
                search_existing_index.sha256_file(Path(sys.executable).resolve()))
            self.assertEqual(
                json.loads(args.command_ledger.read_text()),
                record,
            )

            def mutate_dataset(command, *, cwd, env):
                (dataset_dir / "test.parquet").write_bytes(b"substituted")
                return search_existing_index.argparse.Namespace(returncode=0)

            with mock.patch.object(
                search_existing_index.subprocess, "run", side_effect=mutate_dataset
            ):
                with self.assertRaisesRegex(ValueError, "changed its search dataset files"):
                    search_existing_index.run_vdbbench_command(
                        args,
                        "production",
                        response,
                        "2026-09-02T00:00:02Z",
                        "2026-09-02T00:00:03Z",
                    )


    def test_canonical_vdbbench_interpreter_is_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["vdbbench_argv"].__setitem__(
                0, "/usr/local/bin/python3"))
        self.assert_invalid(packet, "frozen Python interpreter")

    def test_lifecycle_interpreter_and_environment_are_frozen(self) -> None:
        base = copy.deepcopy(self.fixture.contract)
        mutations = (
            (lambda argv: argv.__setitem__(0, "/tmp/python3"), "frozen Python launcher"),
            (lambda argv: argv.__setitem__(
                argv.index("--python") + 1, "/tmp/python3"), "lifecycle harness --python binding"),
            (lambda argv: argv.__setitem__(
                argv.index("--use-uv") + 1, "auto"), "lifecycle harness --use-uv binding"),
        )
        for mutate, error in mutations:
            with self.subTest(error=error):
                self.fixture.contract = copy.deepcopy(base)
                mutate(self.fixture.contract["commands"]["lifecycle_harness_argv_template"])
                self.assert_invalid(self.fixture.no_go_packet(), error)
        self.fixture.contract = base

    def test_screening_commands_bind_normalized_dataset(self) -> None:
        expected = policy.canonical_sha256(
            policy.dataset_expected(self.fixture.contract, 250000, "selection"))
        raw = policy.canonical_sha256(self.fixture.contract["datasets"]["screening"])
        self.assertNotEqual(expected, raw)
        with mock.patch("builtins.print") as emitted:
            policy.print_screening_commands(self.fixture.contract)
        self.assertEqual(emitted.call_count, 4)
        for call in emitted.call_args_list:
            command = call.args[0]
            self.assertIn(f"--measurement-dataset-sha256 {expected}", command)
            self.assertNotIn(raw, command)

    def test_canonical_vdbbench_command_is_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_command(
            run, 0, lambda value: value["vdbbench_argv"].append("--stale-result"))
        self.assert_invalid(packet, "canonical VectorDBBench argv")

    def test_measurement_source_is_manifest_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        source["diagnostics"]["path"] = "adapter-lifecycle.jsonl"
        source["diagnostics"]["sha256"] = policy.sha256_file(root / "adapter-lifecycle.jsonl")
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "diagnostics manifest binding")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        root = Path(run["artifact"]["root"])
        measurement_path = root / run["measurement_evidence"]["path"]
        measurement = json.loads(measurement_path.read_text())
        source_path = root / measurement["source"]["path"]
        source = json.loads(source_path.read_text())
        source["data_root"] = str(root)
        source_path.write_text(json.dumps(source, sort_keys=True) + "\n")
        measurement["source"]["sha256"] = policy.sha256_file(source_path)
        measurement_path.write_text(json.dumps(measurement, sort_keys=True) + "\n")
        run["measurement_evidence"]["sha256"] = policy.sha256_file(measurement_path)
        self.assert_invalid(packet, "manifest data root binding")


    def test_frozen_go_gate_policy_drift_is_invalid(self) -> None:
        packet = self.fixture.no_go_packet()
        self.fixture.contract["experiment"]["go_gates"]["minimum_production_qps_ratio"] = 0.90
        self.assert_invalid(packet, "frozen GO gate policy")
        self.fixture.contract["experiment"]["go_gates"]["minimum_production_qps_ratio"] = 0.95
        packet = self.fixture.no_go_packet()
        self.fixture.contract["experiment"]["projection_model"]["target_scale"] = 9_000_000
        self.assert_invalid(packet, "10M projection model")

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
        self.assert_invalid(
            packet, "each lifecycle and search envelope must complete before the next lifecycle starts")

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
            for key in policy.RESOURCE_KEYS:
                with self.subTest(position=position, key=key):
                    packet = self.fixture.go_packet()
                    run = packet["runs"][position]
                    self.fixture.rewrite_bound(
                        run, run["measurement_evidence"],
                        lambda value, key=key: value["resources"].update({key: 0.0}),
                    )
                    self.assert_invalid(packet, "raw source binding")

    def test_construction_observer_evidence_is_required_and_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_adapter(
            run,
            lambda records: records[-1]["response"]["status"]["column_graph_build"].pop(
                "construction_decisions"),
        )
        self.assert_invalid(packet, "raw construction decisions")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_adapter(
            run,
            lambda records: records[-1]["response"]["status"]["column_graph_build"][
                "construction_decisions"]["planning"].update({"saturated": True}),
        )
        self.assert_invalid(packet, "saturated")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_adapter(
            run,
            lambda records: records[-1]["response"]["status"]["column_graph_build"][
                "construction_decisions"]["reciprocal"].update({"decisions": 0}),
        )
        self.assert_invalid(packet, "raw construction decisions.reciprocal.decisions")

        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["measurement_evidence"],
            lambda value: value["diagnostic_work_profile"]["planning"].update({"decisions": 2}),
        )
        self.assert_invalid(packet, "raw source binding")

    def test_phase_timings_are_bound_to_raw_optimize_evidence(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["measurement_evidence"],
            lambda value: value["phase_seconds"].update({"adjacency": 96}),
        )
        self.assert_invalid(packet, "raw source binding")

    def test_screening_lifecycles_must_not_overlap(self) -> None:
        packet = self.fixture.no_go_packet()
        first_measurement = Path(packet["runs"][0]["artifact"]["root"]) / "measurements.json"
        first_completed = json.loads(first_measurement.read_text())["origin"]["lifecycle_completed_at"]
        second_measurement = Path(packet["runs"][1]["artifact"]["root"]) / "measurements.json"
        second_completed = json.loads(second_measurement.read_text())["origin"]["lifecycle_completed_at"]
        self.fixture.retime_run(packet["runs"][1], first_completed, second_completed)
        self.assert_invalid(
            packet, "each lifecycle and search envelope must complete before the next lifecycle starts")

    def test_projected_reduction_is_a_fraction(self) -> None:
        packet = self.fixture.go_packet()
        run = packet["runs"][5]
        self.fixture.rewrite_bound(
            run, run["measurement_evidence"],
            lambda value: value.update({"projected_10m_adjacency_reduction_fraction": 1.01}),
        )
        self.assert_invalid(packet, "raw producer projection field")

    def test_runtime_identity_is_bound(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        manifest_path = Path(run["artifact"]["root"]) / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["context"]["host"]["pyarrow"] = "0.0.0"
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
        run["artifact"]["manifest_sha256"] = policy.sha256_file(manifest_path)
        self.assert_invalid(packet, "artifact PyArrow runtime")

    def test_isolation_series_is_fail_closed(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        binding = run["isolation_evidence"]
        self.fixture.rewrite_bound(
            run, binding, lambda value: value["samples"][0].update({"swap_used_bytes": 1}),
        )
        self.assert_invalid(packet, "swap_used_bytes")

        mutations = [
            ("lock coverage", lambda value: value.update({"lock_held_through_evidence": False})),
            ("competing_processes", lambda value: value["samples"][0].update({
                "competing_processes": [{"pid": 1, "command": "vectordbbench"}],
            })),
            ("at least start and completion", lambda value: value.update({
                "samples": value["samples"][:1],
            })),
            ("sampling gap", lambda value: value["samples"][1].update({
                "timestamp": (
                    policy.utc_timestamp(value["samples"][0]["timestamp"], "sample")
                    + timedelta(seconds=7)
                ).isoformat(),
            })),
        ]
        for pattern, mutate in mutations:
            with self.subTest(pattern=pattern):
                packet = self.fixture.no_go_packet()
                run = packet["runs"][0]
                self.fixture.rewrite_bound(run, run["isolation_evidence"], mutate)
                self.assert_invalid(packet, pattern)

    def test_lifecycle_subprocess_environments_are_fail_closed(self) -> None:
        for field, mutation in (
            ("service_environment", {"GOGC": "off"}),
            ("vdbbench_environments", {"scalar": {"GOMAXPROCS": "8"}}),
        ):
            with self.subTest(field=field):
                packet = self.fixture.no_go_packet()
                run = packet["runs"][0]
                manifest_path = Path(run["artifact"]["root"]) / "manifest.json"
                manifest = json.loads(manifest_path.read_text())
                manifest["harness"][field] = mutation
                manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n")
                run["artifact"]["manifest_sha256"] = policy.sha256_file(manifest_path)
                self.assert_invalid(packet, "subprocess environment")

    def test_search_envelope_is_fail_closed(self) -> None:
        mutations = [
            ("swap", lambda value: value["samples"][0].update({"swap_used_bytes": 1})),
            ("competitors", lambda value: value["samples"][0].update({
                "competing_processes": [{"pid": 9, "command": "vectordbbench"}],
            })),
            ("sampling gap", lambda value: value["samples"][1].update({
                "timestamp": (
                    policy.utc_timestamp(value["samples"][0]["timestamp"], "search sample")
                    + timedelta(seconds=7)
                ).isoformat(),
            })),
            ("service_argv", lambda value: value["service_argv"].__setitem__(2, "/tmp/wrong")),
            ("gomaxprocs", lambda value: value.update({"gomaxprocs": 1})),
            ("service_environment", lambda value: value["service_environment"].update({
                "GOGC": "off",
            })),
            ("service_exit_code", lambda value: value.update({"service_exit_code": -9})),
        ]
        for pattern, mutate in mutations:
            with self.subTest(pattern=pattern):
                packet = self.fixture.no_go_packet()
                self.fixture.rewrite_search_isolation(packet["runs"][0], "exact", mutate)
                self.assert_invalid(packet, pattern)

    def test_search_origin_binds_authorized_service(self) -> None:
        packet = self.fixture.no_go_packet()
        run = packet["runs"][0]
        self.fixture.rewrite_bound(
            run, run["search_evidence"][0]["origin"],
            lambda value: value.update({"service_binary_sha256": "0" * 64}),
        )
        self.assert_invalid(packet, "authorized service binary")

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


class SearchEnvelopeHelperTest(unittest.TestCase):
    def parse_args_fixture(
        self, root: Path, *, base_url: str = "http://127.0.0.1:6060",
    ) -> search_existing_index.argparse.Namespace:
        root.mkdir(parents=True, exist_ok=True)
        dataset_dir = root / "dataset"
        dataset_dir.mkdir()
        vectordbbench_dir = root / "vectordbbench"
        vectordbbench_dir.mkdir()
        service = root / "service"
        service.write_bytes(b"service")
        query = root / "query.json"
        query.write_text("{}\n")
        lifecycle = root / "lifecycle.jsonl"
        lifecycle.write_text(
            '{"stage":"startup","timestamp":"2026-09-02T00:00:00+00:00"}\n'
            '{"stage":"teardown","timestamp":"2026-09-02T00:01:00+00:00"}\n'
        )
        service_sha = search_existing_index.sha256_file(service)
        lifecycle_sha = search_existing_index.sha256_file(lifecycle)
        manifest = {
            "artifact_root": str(root),
            "service": {
                "data_dir": str(root / "treedb-data"),
                "base_url": base_url,
                "profile": "command_wal_durable",
                "binary": {"sha256": service_sha},
            },
            "lifecycle": {"file": lifecycle.name, "sha256": lifecycle_sha},
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest) + "\n")
        alias = root / "alias"
        alias.symlink_to(root, target_is_directory=True)
        python = Path(sys.executable).resolve()
        argv = [
            "--base-url", base_url,
            "--index-name", "index-ef128",
            "--query-json", str(alias / query.name),
            "--metadata-out", str(alias / "metadata.json"),
            "--diagnostic-response-out", str(alias / "diagnostic-response.json"),
            "--production-response-out", str(alias / "production-response.json"),
            "--diagnostic-result", str(alias / "diagnostic-result.json"),
            "--production-result", str(alias / "production-result.json"),
            "--diagnostic-origin-out", str(alias / "diagnostic-origin.json"),
            "--production-origin-out", str(alias / "production-origin.json"),
            "--command-ledger", str(alias / "command-ledger.jsonl"),
            "--run-id", "screening-ef128",
            "--artifact-root", str(root),
            "--manifest-sha256", search_existing_index.sha256_file(manifest_path),
            "--execution-commit", COMMIT,
            "--dataset-sha256", "d" * 64,
            "--scale", "250000",
            "--dataset-name", "cohere-medium-250k-selection-v1",
            "--dataset-dir", str(dataset_dir),
            "--vectordbbench-dir", str(vectordbbench_dir),
            "--role", "screening_candidate",
            "--partition", "selection",
            "--lifecycle-sha256", lifecycle_sha,
            "--lifecycle-started-at", "2026-09-02T00:00:00+00:00",
            "--lifecycle-completed-at", "2026-09-02T00:01:00+00:00",
            "--ef-construction", "128",
            "--expected-generation", "1",
            "--route", "exact",
            "--service-bin", str(service),
            "--service-binary-sha256", service_sha,
            "--python-executable", str(python),
            "--python-sha256", search_existing_index.sha256_file(python),
            "--search-isolation-out", str(alias / "search-isolation.json"),
            "--exclusive-lock", str(root / "shared.lock"),
        ]
        with mock.patch.dict(search_existing_index.os.environ, {"GOMAXPROCS": "12"}), \
                mock.patch.object(search_existing_index.sys, "argv", ["helper", *argv]):
            return search_existing_index.parse_args()

    def test_parse_args_brackets_ipv6_and_retains_resolved_evidence_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "artifact"
            args = self.parse_args_fixture(root, base_url="http://[::1]:6060")

        self.assertEqual(args.service_addr, "[::1]:6060")
        self.assertEqual(args.command_ledger, root / "command-ledger.jsonl")
        self.assertEqual(args.search_isolation_out, root / "search-isolation.json")

    def test_parse_args_rejects_preexisting_vdbbench_logs(self) -> None:
        for kind in ("diagnostic", "production"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "artifact"
                root.mkdir()
                (root / f"vdbbench-exact-{kind}.log").write_text("stale\n")
                with self.assertRaisesRegex(ValueError, "output already exists"):
                    self.parse_args_fixture(root)

    def test_service_argv_is_exact_and_retained(self) -> None:
        args = SimpleNamespace(
            service_bin=Path("/authorized/service"),
            artifact_root=Path("/artifact"),
            service_addr="127.0.0.1:6060",
        )
        self.assertEqual(search_existing_index.service_argv(args), [
            "/authorized/service", "-dir", "/artifact/treedb-data",
            "-addr", "127.0.0.1:6060", "-profile", "command_wal_durable",
        ])

    def test_service_environment_is_complete_and_non_inherited(self) -> None:
        with mock.patch.dict(
            search_existing_index.os.environ,
            {"GOGC": "off", "GOMEMLIMIT": "1"},
            clear=False,
        ):
            self.assertEqual(
                search_existing_index.service_environment(SimpleNamespace(gomaxprocs=12)),
                {"GOMAXPROCS": "12"},
            )

    def test_isolation_monitor_rethrows_sampling_failure(self) -> None:
        monitor = object.__new__(search_existing_index.IsolationMonitor)
        monitor.stop_event = mock.Mock()
        monitor.thread = mock.Mock()
        monitor.error = OSError("sample failed")
        monitor.samples = []
        with self.assertRaisesRegex(RuntimeError, "isolation monitor failed"):
            monitor.stop()
        monitor.thread.join.assert_called_once_with()

    def test_stop_service_rejects_early_exit(self) -> None:
        process = mock.Mock()
        process.poll.return_value = 2
        process.returncode = 2
        with self.assertRaisesRegex(ValueError, "exited unexpectedly"):
            search_existing_index.stop_service(process)


if __name__ == "__main__":
    unittest.main()
