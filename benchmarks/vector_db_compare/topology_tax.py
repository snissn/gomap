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


def _read(path: Path, limit: int) -> bytes:
    _require(path.is_file(), f"{path.name} is missing")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    _require(len(raw) <= limit, f"{path.name} exceeds {limit} bytes")
    return raw


def _uint(value: Any, *, positive: bool = False) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= (1 if positive else 0)


def _git_sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 40 and all(character in "0123456789abcdef" for character in value)


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _listener_matches(actual: str, configured: str) -> bool:
    if actual == configured:
        return True

    def split(value: str) -> tuple[str, str]:
        if value.startswith("[") and "]:" in value:
            host, port = value[1:].rsplit("]:", 1)
            return host, port
        return value.rsplit(":", 1) if ":" in value else ("", "")

    actual_host, actual_port = split(actual)
    configured_host, configured_port = split(configured)
    return actual_port == configured_port and actual_host in ("0.0.0.0", "::") and configured_host in ("0.0.0.0", "::")


def _go_json_sha256(value: dict[str, Any]) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029").encode()
    return hashlib.sha256(raw).hexdigest()


def _node_config_identity(value: dict[str, Any], node: dict[str, Any]) -> str:
    config = {
        "schema_version": 1, "result_kind": "vector_partition_system_node_config_v1", "assembly": "production_public_v1", "topology": value["topology"],
        "node_id": node["node_id"], "dataset_directory": value["dataset_directory"], "database_directory": node["database_directory"],
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
        "endpoints": {key: value["endpoints"][key] for key in sorted(value["endpoints"])},
        "group_applied_indexes": {key: value["group_applied_indexes"][key] for key in sorted(value["group_applied_indexes"])},
    })
    if "runtime_ownership" in node:
        config["runtime_ownership"] = node["runtime_ownership"]
    return _go_json_sha256(config)


def _topology_identity(value: dict[str, Any], topology: str, want_nodes: int, public_route: str = "vectorpartition.OperationsV1.Search") -> tuple[str, list[str]]:
    expected_keys = {"schema_version", "result_kind", "assembly", "topology", "nodes", "dataset_directory", "endpoints", "group_applied_indexes", "public_route", "m8_loopback", "topology_identity_sha256"}
    _require(set(value) == expected_keys and value.get("schema_version") == 1 and value.get("result_kind") == "vector_partition_system_topology_v1" and value.get("assembly") == "production_public_v1" and value.get("topology") == topology and value.get("public_route") == public_route and value.get("m8_loopback") is False, "system-bench topology artifact structure is invalid")
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
        optional = {"capability_key_path", "public_listen", "profile_directory", "runtime_ownership"}
        _require(required <= set(node) <= required | optional, "system-bench topology node structure is invalid")
        _require(isinstance(node.get("node_id"), str) and node["node_id"] and _is_sha256(node.get("node_config_sha256")), "system-bench topology node identity is invalid")
        _require(all(isinstance(node.get(key), str) and node[key] for key in ("database_directory", "state_directory", "ready_path")), "system-bench topology persistent roots are invalid")
        groups = node.get("local_groups")
        _require(isinstance(groups, list) and groups and all(isinstance(group, dict) and set(group) == {"group_id", "listen"} and isinstance(group["group_id"], str) and group["group_id"] in endpoints and group["listen"] == endpoints[group["group_id"]] for group in groups), "system-bench topology group ownership is invalid")
        group_ids = {group["group_id"] for group in groups}
        _require(owned_groups.isdisjoint(group_ids), "system-bench topology group ownership is invalid")
        owned_groups.update(group_ids)
        _require(node["node_config_sha256"] == _node_config_identity(value, node), "system-bench node config identity digest mismatch")
        canonical_node = {key: node[key] for key in ("node_id", "node_config_sha256", "database_directory", "state_directory")}
        if "capability_key_path" in node:
            _require(isinstance(node["capability_key_path"], str) and node["capability_key_path"], "system-bench topology capability key path is invalid")
            canonical_node["capability_key_path"] = node["capability_key_path"]
        canonical_node["ready_path"] = node["ready_path"]
        if "profile_directory" in node:
            _require(isinstance(node["profile_directory"], str) and node["profile_directory"], "system-bench topology profile directory is invalid")
            canonical_node["profile_directory"] = node["profile_directory"]
        if "public_listen" in node:
            _require(isinstance(node["public_listen"], str) and node["public_listen"], "system-bench topology public endpoint is invalid")
            canonical_node["public_listen"] = node["public_listen"]
            public_nodes += 1
        canonical_node["local_groups"] = [{"group_id": group["group_id"], "listen": group["listen"]} for group in groups]
        if "runtime_ownership" in node:
            ownership = node["runtime_ownership"]
            _require(
                isinstance(ownership, dict) and set(ownership) == {"cpu_set", "gomaxprocs", "go_memory_limit_bytes"} and
                isinstance(ownership["cpu_set"], str) and ownership["cpu_set"] and
                _uint(ownership["gomaxprocs"], positive=True) and _uint(ownership["go_memory_limit_bytes"], positive=True),
                "system-bench topology runtime ownership is invalid",
            )
            canonical_node["runtime_ownership"] = ownership
        canonical_nodes.append(canonical_node)
        database_roots.append(str(Path(node["database_directory"]).resolve()))
        node_ids.append(node["node_id"])
    _require(public_nodes == 1 and owned_groups == set(endpoints) and node_ids == sorted(node_ids), "system-bench topology node ordering or ownership is invalid")
    canonical = {
        "schema_version": 1, "result_kind": "vector_partition_system_topology_v1", "assembly": "production_public_v1", "topology": topology,
        "nodes": canonical_nodes, "dataset_directory": value["dataset_directory"], "endpoints": {key: endpoints[key] for key in sorted(endpoints)},
        "group_applied_indexes": {key: applied[key] for key in sorted(applied)}, "public_route": public_route, "m8_loopback": False,
        "topology_identity_sha256": "",
    }
    return _go_json_sha256(canonical), database_roots


def _ready_identity(topology_path: Path, topology_value: dict[str, Any], node: dict[str, Any], source_revision: str, executable_sha256: str, public_route: str = "vectorpartition.OperationsV1.Search") -> str:
    recorded = Path(node["ready_path"])
    _require(recorded.name == "ready.json" and recorded.parent == Path(node["state_directory"]), "system-bench readiness path is invalid")
    ready_path = topology_path.parent / Path(node["state_directory"]).name / recorded.name
    _require(ready_path.is_file(), "system-bench readiness artifact is missing")
    ready_path = ready_path.resolve()
    ready = _load(ready_path, 1 << 20)
    required = {
        "schema_version", "result_kind", "assembly", "topology", "node_id", "pid", "public_route", "production_topology", "m8_loopback",
        "database_directory", "state_directory", "source_revision", "vcs_modified", "executable_sha256", "node_config_sha256", "lifecycle_state", "groups",
    }
    runtime_stats = {"logical_cpus", "gomaxprocs", "go_memory_limit", "effective_cpu_set"}
    runtime = runtime_stats | {"runtime_ownership"}
    if "public_listen" in node:
        required.add("public_endpoint")
    if "profile_directory" in node:
        required.add("profile_directory")
    _require(set(ready) in (required, required | runtime_stats, required | runtime), "system-bench readiness structure is invalid")
    _require(
        ready.get("schema_version") == 1 and ready.get("result_kind") == "vector_partition_system_node_ready_v1" and
        ready.get("assembly") == "production_public_v1" and ready.get("topology") == topology_value["topology"] and
        ready.get("node_id") == node["node_id"] and _uint(ready.get("pid"), positive=True),
        "system-bench readiness identity is invalid",
    )
    _require(
        ready.get("public_route") == public_route and ready.get("production_topology") is True and
        ready.get("m8_loopback") is False and ready.get("lifecycle_state") == "active",
        "system-bench readiness production route is invalid",
    )
    _require(
        ready.get("database_directory") == node["database_directory"] and ready.get("state_directory") == node["state_directory"] and
        ready.get("node_config_sha256") == node["node_config_sha256"],
        "system-bench readiness node config is invalid",
    )
    _require(
        ready.get("source_revision") == source_revision and ready.get("vcs_modified") is False and ready.get("executable_sha256") == executable_sha256,
        "system-bench readiness executable provenance is invalid",
    )
    _require(_listener_matches(ready.get("public_endpoint", ""), node.get("public_listen", "")), "system-bench readiness public endpoint is invalid")
    _require(ready.get("profile_directory", "") == node.get("profile_directory", ""), "system-bench readiness profile directory is invalid")
    if "runtime_ownership" in node:
        ownership = node["runtime_ownership"]
        _require(
            runtime <= set(ready) and ready["runtime_ownership"] == ownership and
            _uint(ready["logical_cpus"], positive=True) and ready["gomaxprocs"] == ownership["gomaxprocs"] and
            ready["go_memory_limit"] == ownership["go_memory_limit_bytes"] and ready["effective_cpu_set"] == ownership["cpu_set"],
            "system-bench readiness runtime budget is invalid",
        )
    else:
        _require("runtime_ownership" not in ready, "system-bench readiness has undeclared runtime ownership")
        if runtime_stats <= set(ready):
            _require(
                _uint(ready["logical_cpus"], positive=True) and _uint(ready["gomaxprocs"], positive=True) and
                isinstance(ready["go_memory_limit"], int) and ready["go_memory_limit"] > 0 and isinstance(ready["effective_cpu_set"], str),
                "system-bench readiness runtime budget is invalid",
            )
    groups = ready.get("groups")
    _require(isinstance(groups, list) and len(groups) == len(node["local_groups"]), "system-bench readiness group set is invalid")
    for group, configured in zip(groups, node["local_groups"], strict=True):
        _require(
            isinstance(group, dict) and set(group) == {"group_id", "endpoint", "leader_id", "applied_index", "proves_production_consensus"} and
            group.get("group_id") == configured["group_id"] and group.get("endpoint") == configured["listen"] and
            isinstance(group.get("leader_id"), str) and bool(group["leader_id"]) and
            _uint(group.get("applied_index"), positive=True) and group["applied_index"] >= topology_value["group_applied_indexes"][group["group_id"]] and
            group.get("proves_production_consensus") is True,
            "system-bench readiness group evidence is invalid",
        )
    return _sha256(ready_path)


def _client_attestation(search_path: Path, value: dict[str, Any], topology_value: dict[str, Any], executable_path: str) -> dict[str, str]:
    command_path, time_path, rc_path = (search_path.with_name(name) for name in ("bench.command.json", "bench.time", "bench.rc"))
    command = json.loads(_read(command_path, 1 << 20))
    _require(isinstance(command, list) and all(isinstance(part, str) and part for part in command), "system-bench client command is invalid")
    _require(len(command) >= 8 and command[:2] == ["/usr/bin/time", "-v"] and command[2] == "-o" and command[4:6] == [executable_path, "system-bench"], "system-bench client executable is invalid")
    _require(len(command[6:]) % 2 == 0, "system-bench client arguments are invalid")
    flags = dict(zip(command[6::2], command[7::2], strict=True))
    expected_flags = {"-endpoint", "-topology", "-dataset", "-truth-cache", "-truth-cache-sha256", "-probes", "-concurrency", "-top-k", "-ef-search", "-warmup", "-out"}
    _require(set(flags) == expected_flags and len(flags) * 2 == len(command[6:]), "system-bench client arguments are invalid")
    output = Path(flags["-out"])
    _require(Path(command[3]) == output.with_name("bench.time") and Path(flags["-topology"]) == output.with_name("topology.json") and output.name == "search.json", "system-bench client output paths are invalid")
    probes = list(dict.fromkeys(cell["budget"]["probes"] for cell in value["cells"]))
    concurrency = list(dict.fromkeys(cell["concurrency"] for cell in value["cells"]))
    _require(
        flags["-endpoint"] == value.get("endpoint") and flags["-dataset"] == topology_value["dataset_directory"] and
        flags["-truth-cache"] and flags["-truth-cache-sha256"] == value["truth_artifact_sha256"] and
        flags["-probes"] == ",".join(map(str, probes)) and flags["-concurrency"] == ",".join(map(str, concurrency)) and
        flags["-top-k"] == str(value["top_k"]) and flags["-ef-search"] == str(value["ef_search"]) and flags["-warmup"] == str(value["warmup_queries"]),
        "system-bench client command does not match retained result",
    )
    time_raw, rc_raw = _read(time_path, 1 << 20), _read(rc_path, 32)
    expected_time = ('\tCommand being timed: "' + " ".join(command[4:]) + '"\n').encode()
    _require(time_raw.startswith(expected_time) and rc_raw == b"0\n", "system-bench client process attestation is invalid")
    return {"command": _sha256(command_path), "time": _sha256(time_path), "rc": _sha256(rc_path)}


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


def summarize(single_paths: list[Path], native_paths: list[Path], source_revision: str, executable_path: str, executable_sha256: str) -> dict[str, Any]:
    _require(len(single_paths) == len(native_paths) == 3, "topology-tax baseline requires three repetitions per topology")
    _require(_git_sha(source_revision) and Path(executable_path).is_absolute() and _is_sha256(executable_sha256), "topology-tax expected executable provenance is invalid")
    paths = dict(zip(TOPOLOGIES, (single_paths, native_paths), strict=True))
    runs: dict[str, list[dict[str, Any]]] = {}
    inputs: list[dict[str, Any]] = []
    input_paths: set[Path] = set()
    input_digests: set[str] = set()
    topology_identities: set[str] = set()
    database_directories: list[Path] = []
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
            ready_digests = {node["node_id"]: _ready_identity(canonical.with_name("topology.json"), topology_artifact, node, source_revision, executable_sha256) for node in topology_artifact["nodes"]}
            client_attestation = _client_attestation(canonical, value, topology_artifact, executable_path)
            canonical_roots = [Path(root) for root in roots]
            _require(all(not _paths_overlap(root, other) for index, root in enumerate(canonical_roots) for other in canonical_roots[index + 1:]), "system-bench topology database roots are invalid")
            _require(topology_identity not in topology_identities and all(not _paths_overlap(root, other) for root in canonical_roots for other in database_directories), "topology-tax repetitions must use distinct persistent database roots")
            topology_identities.add(topology_identity)
            database_directories.extend(canonical_roots)
            runs[topology].append(value)
            inputs.append({"topology": topology, "repetition": repetition, "path": str(path), "sha256": digest, "topology_identity_sha256": topology_identity, "ready_sha256": ready_digests, "client_attestation_sha256": client_attestation})
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
    return {"schema_version": 1, "result_kind": "vector_partition_topology_tax_v1", "status": "valid_baseline", "execution_identity": {"source_revision": source_revision, "vcs_modified": False, "executable_path": executable_path, "executable_sha256": executable_sha256}, "inputs": inputs, "fixture_truth_identity": {"dataset_checksum": next(iter(identities))[0], "truth_artifact_sha256": next(iter(identities))[1]}, "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--single", type=Path, action="append", required=True)
    parser.add_argument("--native", type=Path, action="append", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--executable-path", required=True)
    parser.add_argument("--executable-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.single, args.native, args.source_revision, args.executable_path, args.executable_sha256)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
