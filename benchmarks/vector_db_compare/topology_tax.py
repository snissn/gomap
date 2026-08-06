#!/usr/bin/env python3
"""Validate and summarize the bounded #4019 TreeDB topology-tax baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

from system_qualification import ContractError, _is_sha256, _load, _percentile, _require


TOPOLOGIES = ("single_daemon_four_group", "native_four_daemon_four_group")
TIMINGS = ("router_open", "router_search", "placement", "queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search", "response", "dedupe", "merge", "total")
POSITIVE_TIMINGS = ("router_search", "rpc", "read_index_apply", "shard_search", "total")
COUNTERS = ("selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects", "candidates", "edges", "query_bytes", "request_bytes", "candidate_bytes", "response_bytes")
POSITIVE_COUNTERS = tuple(value for value in COUNTERS if value not in ("retries", "redirects"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _uint(value: Any, *, positive: bool = False) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= (1 if positive else 0)


def _go_json_sha256(value: dict[str, Any]) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029").encode()
    return hashlib.sha256(raw).hexdigest()


def _node_config_identity(value: dict[str, Any], node: dict[str, Any]) -> str:
    config = {
        "schema_version": 1, "result_kind": "vector_partition_system_node_config_v1", "assembly": "production_public_v1", "topology": value["topology"],
        "node_id": node["node_id"], "dataset_directory": value["dataset_directory"], "database_directory": node["database_directory"],
        "state_directory": node["state_directory"],
    }
    if "public_listen" in node:
        config["public_listen"] = node["public_listen"]
    config.update({
        "ready_path": node["ready_path"], "local_groups": node["local_groups"],
        "endpoints": {key: value["endpoints"][key] for key in sorted(value["endpoints"])},
        "group_applied_indexes": {key: value["group_applied_indexes"][key] for key in sorted(value["group_applied_indexes"])},
    })
    return _go_json_sha256(config)


def _topology_identity(value: dict[str, Any], topology: str, want_nodes: int) -> tuple[str, list[str]]:
    expected_keys = {"schema_version", "result_kind", "assembly", "topology", "nodes", "dataset_directory", "endpoints", "group_applied_indexes", "public_route", "m8_loopback", "topology_identity_sha256"}
    _require(set(value) == expected_keys and value.get("schema_version") == 1 and value.get("result_kind") == "vector_partition_system_topology_v1" and value.get("assembly") == "production_public_v1" and value.get("topology") == topology and value.get("public_route") == "vectorpartition.OperationsV1.Search" and value.get("m8_loopback") is False, "system-bench topology artifact structure is invalid")
    endpoints, applied, nodes = value.get("endpoints"), value.get("group_applied_indexes"), value.get("nodes")
    _require(isinstance(value.get("dataset_directory"), str) and value["dataset_directory"], "system-bench topology dataset directory is invalid")
    _require(isinstance(endpoints, dict) and len(endpoints) == 4 and all(isinstance(key, str) and key and isinstance(endpoint, str) and endpoint for key, endpoint in endpoints.items()), "system-bench topology endpoints are invalid")
    _require(isinstance(applied, dict) and set(applied) == set(endpoints) and all(_uint(index, positive=True) for index in applied.values()), "system-bench topology applied indexes are invalid")
    _require(isinstance(nodes, list) and len(nodes) == want_nodes, "system-bench topology node set changed")
    canonical_nodes: list[dict[str, Any]] = []
    database_roots: list[str] = []
    node_ids: list[str] = []
    owned_groups: set[str] = set()
    public_nodes = 0
    for node in nodes:
        _require(isinstance(node, dict), "system-bench topology node is invalid")
        required = {"node_id", "node_config_sha256", "database_directory", "state_directory", "ready_path", "local_groups"}
        _require(set(node) in (required, required | {"public_listen"}), "system-bench topology node structure is invalid")
        _require(isinstance(node.get("node_id"), str) and node["node_id"] and _is_sha256(node.get("node_config_sha256")), "system-bench topology node identity is invalid")
        _require(all(isinstance(node.get(key), str) and node[key] for key in ("database_directory", "state_directory", "ready_path")), "system-bench topology persistent roots are invalid")
        groups = node.get("local_groups")
        _require(isinstance(groups, list) and groups and all(isinstance(group, dict) and set(group) == {"group_id", "listen"} and isinstance(group["group_id"], str) and group["group_id"] in endpoints and group["listen"] == endpoints[group["group_id"]] for group in groups), "system-bench topology group ownership is invalid")
        group_ids = {group["group_id"] for group in groups}
        _require(owned_groups.isdisjoint(group_ids), "system-bench topology group ownership is invalid")
        owned_groups.update(group_ids)
        _require(node["node_config_sha256"] == _node_config_identity(value, node), "system-bench node config identity digest mismatch")
        canonical_node = {key: node[key] for key in ("node_id", "node_config_sha256", "database_directory", "state_directory", "ready_path")}
        if "public_listen" in node:
            _require(isinstance(node["public_listen"], str) and node["public_listen"], "system-bench topology public endpoint is invalid")
            canonical_node["public_listen"] = node["public_listen"]
            public_nodes += 1
        canonical_node["local_groups"] = [{"group_id": group["group_id"], "listen": group["listen"]} for group in groups]
        canonical_nodes.append(canonical_node)
        database_roots.append(node["database_directory"])
        node_ids.append(node["node_id"])
    _require(public_nodes == 1 and owned_groups == set(endpoints) and node_ids == sorted(node_ids), "system-bench topology node ordering or ownership is invalid")
    canonical = {
        "schema_version": 1, "result_kind": "vector_partition_system_topology_v1", "assembly": "production_public_v1", "topology": topology,
        "nodes": canonical_nodes, "dataset_directory": value["dataset_directory"], "endpoints": {key: endpoints[key] for key in sorted(endpoints)},
        "group_applied_indexes": {key: applied[key] for key in sorted(applied)}, "public_route": "vectorpartition.OperationsV1.Search", "m8_loopback": False,
        "topology_identity_sha256": "",
    }
    return _go_json_sha256(canonical), database_roots


def validate_run(value: dict[str, Any], topology: str) -> None:
    _require(value.get("schema_version") == 1 and value.get("result_kind") == "vector_partition_system_bench_v1", "unexpected system-bench identity")
    _require(value.get("topology") == topology and _is_sha256(value.get("topology_identity_sha256")), "system-bench topology identity mismatch")
    _require(value.get("top_k") == 10 and value.get("ef_search") == 128 and value.get("warmup_queries") == 1000, "system-bench configuration changed")
    _require(_is_sha256(value.get("dataset_checksum")) and _is_sha256(value.get("truth_artifact_sha256")), "system-bench fixture or truth identity is invalid")
    cells = value.get("cells")
    _require(isinstance(cells, list) and len(cells) == 4, "system-bench must contain p2/p16 x c1/c8")
    keys = [(cell.get("budget", {}).get("probes"), cell.get("concurrency")) for cell in cells if isinstance(cell, dict)]
    _require(set(keys) == {(2, 1), (2, 8), (16, 1), (16, 8)} and len(set(keys)) == 4, "system-bench matrix changed")
    generation = None
    for cell in cells:
        _require(cell.get("status") == "valid" and not cell.get("error"), "system-bench contains a failed cell")
        metrics, counters, timings = cell.get("metrics"), cell.get("counters"), cell.get("timings")
        _require(isinstance(metrics, dict) and metrics.get("queries") == 1000 and metrics.get("completed_queries") == 1000 and metrics.get("result_count") == 10000 and metrics.get("errors") == 0 and metrics.get("timeouts") == 0, "system-bench metrics are incomplete")
        _require(isinstance(metrics.get("recall_at_10"), (int, float)) and not isinstance(metrics["recall_at_10"], bool) and math.isfinite(metrics["recall_at_10"]) and .90 <= metrics["recall_at_10"] <= 1, "system-bench recall is below the frozen floor")
        _require(isinstance(counters, dict) and all(_uint(counters.get(key)) for key in COUNTERS) and all(counters[key] > 0 for key in POSITIVE_COUNTERS), "system-bench counters are invalid")
        _require(isinstance(timings, dict) and all(_uint(timings.get(key)) for key in TIMINGS) and all(timings[key] > 0 for key in POSITIVE_TIMINGS), "system-bench timing attribution is invalid")
        observed_generation = cell.get("generation")
        _require(isinstance(observed_generation, dict) and isinstance(observed_generation.get("Index"), str) and observed_generation["Index"] and _uint(observed_generation.get("Generation"), positive=True), "system-bench generation is invalid")
        generation = observed_generation if generation is None else generation
        _require(observed_generation == generation, "system-bench generation changed")
        samples, elapsed = cell.get("total_nanos"), cell.get("elapsed_nanos")
        _require(isinstance(samples, list) and len(samples) == 1000 and all(_uint(sample, positive=True) for sample in samples), "system-bench raw samples are invalid")
        workers = cell["concurrency"]
        worker_elapsed = max(sum(samples[worker::workers]) for worker in range(workers))
        _require(_uint(elapsed, positive=True) and elapsed >= worker_elapsed, "system-bench elapsed time is invalid")
        _require((metrics.get("p50_nanos"), metrics.get("p95_nanos"), metrics.get("p99_nanos")) == (_percentile(samples, 50), _percentile(samples, 95), _percentile(samples, 99)), "system-bench percentiles changed")
        _require(math.isclose(metrics.get("qps", 0), 1_000_000_000_000 / elapsed, rel_tol=1e-12), "system-bench QPS changed")


def summarize(single_paths: list[Path], native_paths: list[Path]) -> dict[str, Any]:
    _require(len(single_paths) == len(native_paths) == 3, "topology-tax baseline requires three repetitions per topology")
    paths = dict(zip(TOPOLOGIES, (single_paths, native_paths), strict=True))
    runs: dict[str, list[dict[str, Any]]] = {}
    inputs: list[dict[str, Any]] = []
    input_paths: set[Path] = set()
    input_digests: set[str] = set()
    topology_identities: set[str] = set()
    database_directories: set[str] = set()
    for topology, topology_paths in paths.items():
        runs[topology] = []
        for repetition, path in enumerate(topology_paths, 1):
            canonical = path.resolve(strict=True)
            digest = _sha256(canonical)
            _require(canonical not in input_paths and digest not in input_digests, "topology-tax repetition artifacts must be distinct")
            input_paths.add(canonical)
            input_digests.add(digest)
            value = _load(path)
            validate_run(value, topology)
            topology_identity = value["topology_identity_sha256"]
            topology_artifact = _load(canonical.with_name("topology.json"))
            computed_identity, roots = _topology_identity(topology_artifact, topology, 1 if topology == TOPOLOGIES[0] else 4)
            _require(topology_artifact.get("topology_identity_sha256") == topology_identity == computed_identity, "system-bench topology artifact identity digest mismatch")
            _require(len(set(roots)) == len(roots), "system-bench topology database roots are invalid")
            _require(topology_identity not in topology_identities and database_directories.isdisjoint(roots), "topology-tax repetitions must use distinct persistent database roots")
            topology_identities.add(topology_identity)
            database_directories.update(roots)
            runs[topology].append(value)
            inputs.append({"topology": topology, "repetition": repetition, "path": str(path), "sha256": digest, "topology_identity_sha256": topology_identity})
    identities = {(run["dataset_checksum"], run["truth_artifact_sha256"]) for values in runs.values() for run in values}
    _require(len(identities) == 1, "topology-tax rows do not share fixture and truth identities")
    generations = {json.dumps(cell["generation"], sort_keys=True) for values in runs.values() for run in values for cell in run["cells"]}
    _require(len(generations) == 1, "topology-tax rows do not share one generation")
    rows: list[dict[str, Any]] = []
    for probes in (2, 16):
        for concurrency in (1, 8):
            row: dict[str, Any] = {"probes": probes, "concurrency": concurrency, "topologies": {}}
            for topology in TOPOLOGIES:
                cells = [next(cell for cell in run["cells"] if cell["budget"]["probes"] == probes and cell["concurrency"] == concurrency) for run in runs[topology]]
                recalls = [cell["metrics"]["recall_at_10"] for cell in cells]
                qps = [cell["metrics"]["qps"] for cell in cells]
                p95 = [cell["metrics"]["p95_nanos"] for cell in cells]
                row["topologies"][topology] = {
                    "recall_at_10_min": min(recalls),
                    "recall_at_10_median": median(recalls),
                    "recall_at_10_max": max(recalls),
                    "qps_min": min(qps),
                    "qps_median": median(qps),
                    "qps_max": max(qps),
                    "p95_nanos_min": min(p95),
                    "p95_nanos_median": median(p95),
                    "p95_nanos_max": max(p95),
                    "timing_nanos_per_query_median": {key: median(cell["timings"][key] / 1000 for cell in cells) for key in TIMINGS},
                    "counters_per_query_median": {key: median(cell["counters"][key] / 1000 for cell in cells) for key in COUNTERS},
                }
            single = row["topologies"][TOPOLOGIES[0]]
            native = row["topologies"][TOPOLOGIES[1]]
            row["native_over_single_qps"] = native["qps_median"] / single["qps_median"]
            row["native_over_single_p95"] = native["p95_nanos_median"] / single["p95_nanos_median"]
            rows.append(row)
    return {"schema_version": 1, "result_kind": "vector_partition_topology_tax_v1", "status": "valid_baseline", "inputs": inputs, "fixture_truth_identity": {"dataset_checksum": next(iter(identities))[0], "truth_artifact_sha256": next(iter(identities))[1]}, "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--single", type=Path, action="append", required=True)
    parser.add_argument("--native", type=Path, action="append", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.single, args.native)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
