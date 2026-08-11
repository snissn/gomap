#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from system_qualification import ContractError
from topology_tax import _listener_matches, _topology_identity, summarize


SOURCE_REVISION = "d" * 40
EXECUTABLE_PATH = "/expected/treedb_vector_partition_bench"
EXECUTABLE_SHA256 = "e" * 64


def summarize_checked(single: list[Path], native: list[Path]) -> dict:
    return summarize(single, native, SOURCE_REVISION, EXECUTABLE_PATH, EXECUTABLE_SHA256)


class TopologyTaxTest(unittest.TestCase):
    def test_listener_accepts_only_exact_or_equivalent_wildcard(self) -> None:
        self.assertTrue(_listener_matches("[::]:47101", "0.0.0.0:47101"))
        self.assertFalse(_listener_matches("127.0.0.1:47101", "0.0.0.0:47101"))
        self.assertFalse(_listener_matches("[::]:47102", "0.0.0.0:47101"))

    def test_retained_runner_preserves_timing_wrapper_after_readiness(self) -> None:
        script = Path(__file__).parents[2] / "TreeDB/docs/evidence/vector-partition-local-system-qualification-4019/m2-95c60cbe/run_m2.py"
        spec = importlib.util.spec_from_file_location("m2_retained_runner", script)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        process = mock.Mock(pid=101)
        process.poll.return_value = None
        process.wait.return_value = 0
        with mock.patch.object(module.os, "kill") as kill, mock.patch.object(module.os, "killpg") as killpg:
            module.stop_nodes([{"pid": 202}], [process])
        kill.assert_called_once_with(202, module.signal.SIGTERM)
        killpg.assert_not_called()

    @staticmethod
    def node_config_sha256(topology: dict, node: dict) -> str:
        config = {
            "schema_version": 1, "result_kind": "vector_partition_system_node_config_v1", "assembly": "production_public_v1", "topology": topology["topology"],
            "node_id": node["node_id"], "dataset_directory": topology["dataset_directory"], "database_directory": node["database_directory"],
            "state_directory": node["state_directory"],
        }
        if "capability_key_path" in node:
            config["capability_key_path"] = node["capability_key_path"]
        if "public_listen" in node:
            config["public_listen"] = node["public_listen"]
        config["ready_path"] = node["ready_path"]
        if "profile_directory" in node:
            config["profile_directory"] = node["profile_directory"]
        config.update({
            "local_groups": node["local_groups"],
            "endpoints": {key: topology["endpoints"][key] for key in sorted(topology["endpoints"])},
            "group_applied_indexes": {key: topology["group_applied_indexes"][key] for key in sorted(topology["group_applied_indexes"])},
        })
        if "runtime_ownership" in node:
            config["runtime_ownership"] = node["runtime_ownership"]
        return hashlib.sha256(json.dumps(config, separators=(",", ":")).encode()).hexdigest()

    def run_value(self, topology: str) -> dict:
        cells = []
        for probes in (2, 16):
            for concurrency in (1, 8):
                samples = [1, 2, 3] + [3] * 997
                cells.append({
                    "status": "valid", "budget": {"probes": probes}, "concurrency": concurrency,
                    "generation": {"Index": "embedding_graph", "Generation": 7},
                    "metrics": {"queries": 1000, "completed_queries": 1000, "result_count": 10000, "errors": 0, "timeouts": 0, "recall_at_10": .95, "qps": 1.0, "p50_nanos": 3, "p95_nanos": 3, "p99_nanos": 3},
                    "counters": {key: (0 if key in ("retries", "redirects") else 1) for key in ("selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects", "candidates", "edges", "query_bytes", "request_bytes", "candidate_bytes", "response_bytes")},
                    "timings": {key: (0 if key in ("router_open", "placement", "queue", "network", "generation_open", "response", "dedupe", "merge") else 1) for key in ("router_open", "router_search", "placement", "queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search", "response", "dedupe", "merge", "total")},
                    "elapsed_nanos": 1_000_000_000_000, "total_nanos": samples,
                })
        return {"schema_version": 1, "result_kind": "vector_partition_system_bench_v1", "topology": topology, "topology_identity_sha256": "a" * 64, "dataset_checksum": "b" * 64, "truth_artifact_sha256": "c" * 64, "top_k": 10, "ef_search": 128, "warmup_queries": 1000, "cells": cells}

    def files(self, root: Path) -> tuple[list[Path], list[Path]]:
        values: list[list[Path]] = [[], []]
        for topology_index, topology in enumerate(("single_daemon_four_group", "native_four_daemon_four_group")):
            for repetition in range(3):
                value = self.run_value(topology)
                run_root = root / topology / f"repeat-{repetition + 1}"
                run_root.mkdir(parents=True)
                path = run_root / "search.json"
                node_count = 1 if topology_index == 0 else 4
                endpoints = {f"group-{group}": f"127.0.0.1:{10000 + topology_index * 1000 + repetition * 100 + group}" for group in range(4)}
                nodes = []
                for node in range(node_count):
                    owned = range(4) if node_count == 1 else (node,)
                    node_value = {
                        "node_id": f"node-{node}", "node_config_sha256": "",
                        "database_directory": str(run_root / f"database-{node}"), "state_directory": str(run_root / f"state-{node}"),
                        "ready_path": str(run_root / f"state-{node}/ready.json"),
                    }
                    if node == 0:
                        node_value["public_listen"] = f"127.0.0.1:{20000 + topology_index * 100 + repetition}"
                    node_value["local_groups"] = [{"group_id": f"group-{group}", "listen": endpoints[f"group-{group}"]} for group in owned]
                    if node_count == 4:
                        node_value["runtime_ownership"] = {"cpu_set": str(node), "gomaxprocs": 1, "go_memory_limit_bytes": 1 << 30}
                    nodes.append(node_value)
                topology_value = {
                    "schema_version": 1, "result_kind": "vector_partition_system_topology_v1", "assembly": "production_public_v1", "topology": topology,
                    "nodes": nodes, "dataset_directory": str(root / "dataset"), "endpoints": endpoints,
                    "group_applied_indexes": {group: 1 for group in endpoints}, "public_route": "vectorpartition.OperationsV1.Search", "m8_loopback": False,
                    "topology_identity_sha256": "",
                }
                for node_value in nodes:
                    node_value["node_config_sha256"] = self.node_config_sha256(topology_value, node_value)
                topology_value["topology_identity_sha256"] = hashlib.sha256(json.dumps(topology_value, separators=(",", ":")).encode()).hexdigest()
                value["topology_identity_sha256"] = topology_value["topology_identity_sha256"]
                value["endpoint"] = nodes[0]["public_listen"]
                path.write_text(json.dumps(value), encoding="utf-8")
                (run_root / "topology.json").write_text(json.dumps(topology_value), encoding="utf-8")
                for node_value in nodes:
                    ready = {
                        "schema_version": 1, "result_kind": "vector_partition_system_node_ready_v1", "assembly": "production_public_v1", "topology": topology,
                        "node_id": node_value["node_id"], "pid": 100 + node_count, "public_route": "vectorpartition.OperationsV1.Search", "production_topology": True,
                        "m8_loopback": False, "database_directory": node_value["database_directory"], "state_directory": node_value["state_directory"],
                        "source_revision": SOURCE_REVISION, "vcs_modified": False, "executable_sha256": EXECUTABLE_SHA256,
                        "node_config_sha256": node_value["node_config_sha256"], "lifecycle_state": "active",
                        "groups": [{"group_id": group["group_id"], "endpoint": group["listen"], "leader_id": "leader", "applied_index": 2, "proves_production_consensus": True} for group in node_value["local_groups"]],
                    }
                    if "public_listen" in node_value:
                        ready["public_endpoint"] = node_value["public_listen"]
                    if "runtime_ownership" in node_value:
                        ready.update({
                            "logical_cpus": 4, "gomaxprocs": 1, "go_memory_limit": 1 << 30,
                            "effective_cpu_set": str(node_value["runtime_ownership"]["cpu_set"]),
                            "runtime_ownership": node_value["runtime_ownership"],
                        })
                    ready_path = run_root / Path(node_value["state_directory"]).name / "ready.json"
                    ready_path.parent.mkdir()
                    ready_path.write_text(json.dumps(ready), encoding="utf-8")
                command = [
                    "/usr/bin/time", "-v", "-o", str(run_root / "bench.time"), EXECUTABLE_PATH, "system-bench",
                    "-endpoint", value["endpoint"], "-topology", str(run_root / "topology.json"), "-dataset", topology_value["dataset_directory"],
                    "-truth-cache", "/truth", "-truth-cache-sha256", value["truth_artifact_sha256"], "-probes", "2,16", "-concurrency", "1,8",
                    "-top-k", "10", "-ef-search", "128", "-warmup", "1000", "-out", str(path),
                ]
                (run_root / "bench.command.json").write_text(json.dumps(command), encoding="utf-8")
                (run_root / "bench.time").write_text('\tCommand being timed: "' + " ".join(command[4:]) + '"\n', encoding="utf-8")
                (run_root / "bench.rc").write_text("0\n", encoding="utf-8")
                values[topology_index].append(path)
        return values[0], values[1]

    def test_three_repetition_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = summarize_checked(*self.files(Path(directory)))
        self.assertEqual(result["status"], "valid_baseline")
        self.assertEqual(len(result["inputs"]), 6)
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(result["rows"][0]["native_over_single_qps"], 1)
        self.assertEqual(result["rows"][0]["topologies"]["single_daemon_four_group"]["qps_min"], 1)
        self.assertEqual(result["rows"][0]["topologies"]["single_daemon_four_group"]["qps_max"], 1)

    def test_capability_key_path_is_bound_to_topology_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, _ = self.files(Path(directory))
            topology = json.loads(single[0].with_name("topology.json").read_text(encoding="utf-8"))
            topology["nodes"][0]["capability_key_path"] = "/strict/capability.key"
            topology["nodes"][0]["node_config_sha256"] = self.node_config_sha256(topology, topology["nodes"][0])
            topology["topology_identity_sha256"] = ""
            computed, _ = _topology_identity(topology, "single_daemon_four_group", 1)
            topology["topology_identity_sha256"] = computed
            self.assertEqual(computed, topology["topology_identity_sha256"])
            topology["nodes"][0]["capability_key_path"] += ".forged"
            with self.assertRaisesRegex(ContractError, "node config identity digest mismatch"):
                _topology_identity(topology, "single_daemon_four_group", 1)

    def test_reused_repetition_artifacts_reject(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            with self.assertRaisesRegex(ContractError, "repetition artifacts must be distinct"):
                summarize_checked([single[0]] * 3, native)
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            shutil.copyfile(single[0], single[1])
            with self.assertRaisesRegex(ContractError, "repetition artifacts must be distinct"):
                summarize_checked(single, native)

    def test_reused_persistent_database_root_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            first = json.loads(single[0].with_name("topology.json").read_text(encoding="utf-8"))
            repeated = json.loads(single[1].with_name("topology.json").read_text(encoding="utf-8"))
            repeated["nodes"][0]["database_directory"] = first["nodes"][0]["database_directory"]
            repeated["nodes"][0]["node_config_sha256"] = self.node_config_sha256(repeated, repeated["nodes"][0])
            ready_path = single[1].parent / Path(repeated["nodes"][0]["state_directory"]).name / "ready.json"
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            ready["database_directory"] = repeated["nodes"][0]["database_directory"]
            ready["node_config_sha256"] = repeated["nodes"][0]["node_config_sha256"]
            ready_path.write_text(json.dumps(ready), encoding="utf-8")
            repeated["topology_identity_sha256"] = ""
            repeated["topology_identity_sha256"] = hashlib.sha256(json.dumps(repeated, separators=(",", ":")).encode()).hexdigest()
            single[1].with_name("topology.json").write_text(json.dumps(repeated), encoding="utf-8")
            search = json.loads(single[1].read_text(encoding="utf-8"))
            search["topology_identity_sha256"] = repeated["topology_identity_sha256"]
            single[1].write_text(json.dumps(search), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "distinct persistent database roots"):
                summarize_checked(single, native)

    def test_nested_persistent_database_root_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            topology = json.loads(native[0].with_name("topology.json").read_text(encoding="utf-8"))
            topology["nodes"][1]["database_directory"] = str(Path(topology["nodes"][0]["database_directory"]) / "nested")
            topology["nodes"][1]["node_config_sha256"] = self.node_config_sha256(topology, topology["nodes"][1])
            ready_path = native[0].parent / Path(topology["nodes"][1]["state_directory"]).name / "ready.json"
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            ready["database_directory"] = topology["nodes"][1]["database_directory"]
            ready["node_config_sha256"] = topology["nodes"][1]["node_config_sha256"]
            ready_path.write_text(json.dumps(ready), encoding="utf-8")
            topology["topology_identity_sha256"] = ""
            topology["topology_identity_sha256"] = hashlib.sha256(json.dumps(topology, separators=(",", ":")).encode()).hexdigest()
            search = json.loads(native[0].read_text(encoding="utf-8"))
            search["topology_identity_sha256"] = topology["topology_identity_sha256"]
            native[0].with_name("topology.json").write_text(json.dumps(topology), encoding="utf-8")
            native[0].write_text(json.dumps(search), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "database roots are invalid"):
                summarize_checked(single, native)

    def test_forged_topology_identity_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            topology = json.loads(single[1].with_name("topology.json").read_text(encoding="utf-8"))
            search = json.loads(single[1].read_text(encoding="utf-8"))
            topology["topology_identity_sha256"] = search["topology_identity_sha256"] = "f" * 64
            single[1].with_name("topology.json").write_text(json.dumps(topology), encoding="utf-8")
            single[1].write_text(json.dumps(search), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "identity digest mismatch"):
                summarize_checked(single, native)

    def test_forged_node_config_roots_reject(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            topology = json.loads(single[1].with_name("topology.json").read_text(encoding="utf-8"))
            search = json.loads(single[1].read_text(encoding="utf-8"))
            topology["nodes"][0]["database_directory"] += "-forged"
            topology["topology_identity_sha256"] = ""
            topology["topology_identity_sha256"] = hashlib.sha256(json.dumps(topology, separators=(",", ":")).encode()).hexdigest()
            search["topology_identity_sha256"] = topology["topology_identity_sha256"]
            single[1].with_name("topology.json").write_text(json.dumps(topology), encoding="utf-8")
            single[1].write_text(json.dumps(search), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "node config identity digest mismatch"):
                summarize_checked(single, native)

    def test_changed_generation_or_percentile_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            value["cells"][1]["generation"]["Generation"] = 8
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "generation changed"):
                summarize_checked(single, native)
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            value["cells"][0]["metrics"]["p95_nanos"] = 4
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "percentiles changed"):
                summarize_checked(single, native)

    def test_readiness_provenance_or_node_binding_rejects(self) -> None:
        for field, value, error in (
            ("source_revision", "f" * 40, "executable provenance"),
            ("executable_sha256", "f" * 64, "executable provenance"),
            ("database_directory", "/forged/database", "node config"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                single, native = self.files(Path(directory))
                topology = json.loads(native[1].with_name("topology.json").read_text(encoding="utf-8"))
                ready_path = native[1].parent / Path(topology["nodes"][0]["state_directory"]).name / "ready.json"
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
                ready[field] = value
                ready_path.write_text(json.dumps(ready), encoding="utf-8")
                with self.assertRaisesRegex(ContractError, error):
                    summarize_checked(single, native)

    def test_client_executable_attestation_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            path = native[1].with_name("bench.command.json")
            command = json.loads(path.read_text(encoding="utf-8"))
            command[4] = "/stale/treedb_vector_partition_bench"
            path.write_text(json.dumps(command), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "client executable"):
                summarize_checked(single, native)

    def test_recall_below_frozen_floor_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            value["cells"][0]["metrics"]["recall_at_10"] = .899
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "below the frozen floor"):
                summarize_checked(single, native)

    def test_elapsed_shorter_than_serialized_worker_lane_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            single, native = self.files(Path(directory))
            value = json.loads(native[2].read_text(encoding="utf-8"))
            cell = next(cell for cell in value["cells"] if cell["concurrency"] == 8)
            samples = [100 if index % 8 == 0 else 1 for index in range(1000)]
            cell["total_nanos"] = samples
            cell["elapsed_nanos"] = (sum(samples) + 7) // 8
            cell["metrics"]["qps"] = 1_000_000_000_000 / cell["elapsed_nanos"]
            cell["metrics"]["p50_nanos"] = 1
            cell["metrics"]["p95_nanos"] = 100
            cell["metrics"]["p99_nanos"] = 100
            native[2].write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "elapsed time is invalid"):
                summarize_checked(single, native)

    def test_committed_m2_baseline_reduces_from_raw_rows(self) -> None:
        evidence = Path(__file__).parents[2] / "TreeDB/docs/evidence/vector-partition-local-system-qualification-4019/m2-95c60cbe"
        single = [evidence / f"runs/single_daemon_four_group/repeat-{repetition}/search.json" for repetition in range(1, 4)]
        native = [evidence / f"runs/native_four_daemon_four_group/repeat-{repetition}/search.json" for repetition in range(1, 4)]
        expected = json.loads((evidence / "topology-tax.json").read_text(encoding="utf-8"))
        result = summarize(single, native, "95c60cbef0b0cb824a74a29e9304784e76745d9d", "/mnt/fast4tb/gomap-4019-m2-topology-tax-evidence-95c60cbe/bin/treedb_vector_partition_bench.vcs", "b8d12f98778698ed74db4a905e2bb6b2925840702664beb6fab9e402c4f913d1")
        self.assertEqual(result["status"], "valid_baseline")
        self.assertEqual(result["execution_identity"], expected["execution_identity"])
        self.assertEqual(result["fixture_truth_identity"], expected["fixture_truth_identity"])
        self.assertEqual(result["rows"], expected["rows"])
        self.assertEqual(
            [{key: entry[key] for key in ("topology", "repetition", "sha256", "topology_identity_sha256", "ready_sha256", "client_attestation_sha256")} for entry in result["inputs"]],
            [{key: entry[key] for key in ("topology", "repetition", "sha256", "topology_identity_sha256", "ready_sha256", "client_attestation_sha256")} for entry in expected["inputs"]],
        )


if __name__ == "__main__":
    unittest.main()
