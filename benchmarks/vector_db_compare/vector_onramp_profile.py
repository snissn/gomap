#!/usr/bin/env python3
"""Validate and reduce the bounded #4090 TreeDB vector on-ramp profile."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

from system_qualification import _is_sha256, _load, _percentile, _require
from topology_tax import _git_sha, _paths_overlap, _ready_identity, _sha256, _topology_identity, _uint


CONTROLS = ("single", "native-default", "native-budgeted", "container")
TOPOLOGIES = {
    "single": ("single_daemon_four_group", 1),
    "native-default": ("native_four_daemon_four_group", 4),
    "native-budgeted": ("native_four_daemon_four_group", 4),
    "container": ("container_four_daemon_four_group", 4),
}
CONCURRENCIES = (1, 32)
COUNTERS = {
    "selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects", "candidates", "edges",
    "query_bytes", "request_bytes", "candidate_bytes", "response_bytes", "public_request_frame_bytes", "public_response_frame_bytes",
}
TIMINGS = {
    "admission", "operations_health", "service_adapter", "public_adapter", "router_open", "router_search", "placement",
    "coordinator_lifecycle", "dispatch", "queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search",
    "response", "dedupe", "merge", "coordinator_total", "total", "client_encode", "client_write", "client_response_read",
    "client_decode", "client_total",
}
STAGE_FIELDS = {
    "reads", "successes", "failures", "verify_leader_calls", "log_barriers", "total_nanos", "admission_nanos",
    "verify_leader_nanos", "barrier_nanos", "applied_read_nanos",
}
CATALOG_SOURCES = ("operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")
RUNTIME_MONOTONIC = (
    "cpu_time_nanos", "voluntary_context_switches", "nonvoluntary_context_switches",
    "peak_rss_bytes", "total_alloc_bytes", "mallocs", "frees", "num_gc", "pause_total_nanos",
)
RUNTIME_DIAGNOSTIC = ("run_queue_delay_nanos", "timeslices")
RUNTIME_FIELDS = {
    "sample_unix_nano", *RUNTIME_MONOTONIC, *RUNTIME_DIAGNOSTIC, "rss_bytes", "heap_alloc_bytes", "heap_objects", "goroutines",
}


def _subtract(after: dict[str, Any], before: dict[str, Any], fields: set[str], label: str) -> dict[str, int]:
    _require(set(after) == set(before) == fields and all(_uint(after[key]) and _uint(before[key]) and after[key] >= before[key] for key in fields), f"{label} is non-monotonic")
    return {key: after[key] - before[key] for key in fields}


def _validate_catalog(cell: dict[str, Any], node_ids: set[str]) -> dict[str, Any]:
    evidence = cell.get("catalog_reads")
    _require(isinstance(evidence, dict) and set(evidence) == {"nodes", "total"}, "catalog evidence structure is invalid")
    nodes = evidence["nodes"]
    _require(isinstance(nodes, list) and len(nodes) == len(node_ids), "catalog evidence node set changed")
    _require({node.get("node_config_sha256") for node in nodes if isinstance(node, dict)} == node_ids, "catalog evidence node identity changed")
    for node in nodes:
        _require(set(node) == {"node_config_sha256", "before", "after", "delta"}, "catalog node evidence is invalid")
        for source in ("total", *CATALOG_SOURCES):
            want = _subtract(node["after"][source], node["before"][source], STAGE_FIELDS, "catalog read stage")
            _require(node["delta"][source] == want, "catalog node delta changed")
        for key in ("last_term", "last_catalog_applied_index", "last_raft_applied_index", "last_raft_log_index"):
            _require(_uint(node["after"].get(key), positive=True) and node["delta"].get(key) == node["after"][key], "catalog proof identity is invalid")
        _require(node["after"]["last_raft_log_index"] - node["before"]["last_raft_log_index"] >= node["delta"]["total"]["log_barriers"], "catalog log amplification is under-reported")
    total = evidence["total"]
    expected_reads = 2000 + cell["counters"]["selected_groups"]
    expected = {"operations_health": 1000, "coordinator_lifecycle": 1000, "shard_lifecycle": cell["counters"]["selected_groups"], "unknown": 0}
    for source, reads in expected.items():
        stage = total.get(source)
        _require(isinstance(stage, dict) and set(stage) == STAGE_FIELDS and stage["reads"] == reads and stage["successes"] == reads and stage["failures"] == 0 and stage["verify_leader_calls"] == reads and stage["log_barriers"] == reads, "catalog source count changed")
        nested = stage["admission_nanos"] + stage["verify_leader_nanos"] + stage["barrier_nanos"] + stage["applied_read_nanos"]
        _require(stage["total_nanos"] >= nested, "catalog stage timing is invalid")
    summed = {key: sum(total[source][key] for source in CATALOG_SOURCES) for key in STAGE_FIELDS}
    _require(total.get("total") == summed and summed["reads"] == expected_reads, "catalog totals do not match search work")
    return {
        "reads_per_query": summed["reads"] / 1000,
        "log_barriers_per_query": summed["log_barriers"] / 1000,
        "raft_log_entries_per_query": sum(node["after"]["last_raft_log_index"] - node["before"]["last_raft_log_index"] for node in nodes) / 1000,
        "work_nanos_per_query": {source: total[source]["total_nanos"] / 1000 for source in ("total", *CATALOG_SOURCES)},
        "barrier_nanos_per_query": {source: total[source]["barrier_nanos"] / 1000 for source in ("total", *CATALOG_SOURCES)},
    }


def _validate_runtime(cell: dict[str, Any], node_ids: set[str]) -> dict[str, float]:
    nodes = cell.get("runtime")
    _require(isinstance(nodes, list) and len(nodes) == len(node_ids), "runtime evidence node set changed")
    _require({node.get("node_config_sha256") for node in nodes if isinstance(node, dict)} == node_ids, "runtime evidence node identity changed")
    totals = {key: 0 for key in RUNTIME_MONOTONIC}
    for node in nodes:
        _require(set(node) == {"node_config_sha256", "before", "after"} and set(node["before"]) == set(node["after"]) == RUNTIME_FIELDS, "runtime evidence structure is invalid")
        before, after = node["before"], node["after"]
        _require(_uint(before["sample_unix_nano"], positive=True) and after["sample_unix_nano"] > before["sample_unix_nano"] and _uint(after["goroutines"], positive=True), "runtime sample identity is invalid")
        for key in RUNTIME_MONOTONIC:
            _require(_uint(before[key]) and _uint(after[key]) and after[key] >= before[key], "runtime evidence is non-monotonic")
            totals[key] += after[key] - before[key]
        _require(all(_uint(before[key]) and _uint(after[key]) for key in RUNTIME_DIAGNOSTIC), "runtime diagnostic fields are invalid")
    _require(totals["cpu_time_nanos"] > 0 and totals["total_alloc_bytes"] > 0 and totals["mallocs"] > 0, "runtime evidence did not cover process worker threads")
    return {key + "_per_query": value / 1000 for key, value in totals.items()}


def _wall(cell: dict[str, Any]) -> dict[str, Any]:
    timing = cell["timings"]
    public_parts = ("admission", "operations_health", "service_adapter", "public_adapter", "coordinator_total")
    coordinator_parts = ("router_open", "coordinator_lifecycle", "router_search", "placement", "dispatch", "dedupe", "merge")
    transport_parts = ("client_encode", "client_write", "client_response_read", "client_decode")
    public_residual = timing["total"] - sum(timing[key] for key in public_parts)
    coordinator_residual = timing["coordinator_total"] - sum(timing[key] for key in coordinator_parts)
    transport_residual = timing["client_total"] - sum(timing[key] for key in transport_parts)
    serial = cell["concurrency"] == 1
    scheduling_residual = cell["elapsed_nanos"] - timing["client_total"] if serial else 0
    _require(min(public_residual, coordinator_residual, transport_residual, scheduling_residual) >= 0, "exclusive wall-time attribution is invalid")
    unexplained = public_residual + coordinator_residual + transport_residual + scheduling_residual
    return {
        "accounting_kind": "serial_wall" if serial else "aggregate_request_work",
        "elapsed_nanos_per_query": cell["elapsed_nanos"] / 1000,
        "public_exclusive_nanos_per_query": {key: timing[key] / 1000 for key in public_parts} | {"unexplained": public_residual / 1000},
        "coordinator_exclusive_nanos_per_query": {key: timing[key] / 1000 for key in coordinator_parts} | {"unexplained": coordinator_residual / 1000},
        "client_exclusive_nanos_per_query": {key: timing[key] / 1000 for key in transport_parts} | {"unexplained": transport_residual / 1000},
        "serial_scheduling_unexplained_nanos_per_query": scheduling_residual / 1000,
        "shard_work_nanos_per_query": {key: timing[key] / 1000 for key in ("queue", "rpc", "network", "read_index_apply", "generation_open", "shard_search", "response")},
        "unexplained_fraction": unexplained / (cell["elapsed_nanos"] if serial else timing["client_total"]),
        "unexplained_defect": serial and unexplained * 20 > cell["elapsed_nanos"],
    }


def validate_cell(cell: dict[str, Any], node_ids: set[str]) -> dict[str, Any]:
    _require(isinstance(cell, dict) and cell.get("status") == "valid" and not cell.get("error") and cell.get("budget") == {"probes": 2} and cell.get("concurrency") in CONCURRENCIES, "profile cell identity changed")
    metrics, counters, timings = cell.get("metrics"), cell.get("counters"), cell.get("timings")
    _require(isinstance(metrics, dict) and metrics.get("queries") == metrics.get("completed_queries") == 1000 and metrics.get("result_count") == 10000 and metrics.get("errors") == metrics.get("timeouts") == 0, "profile cell is incomplete")
    recall = metrics.get("recall_at_10")
    _require(isinstance(recall, (int, float)) and not isinstance(recall, bool) and math.isfinite(recall) and .90 <= recall <= 1, "profile recall is below the frozen floor")
    _require(isinstance(counters, dict) and set(counters) == COUNTERS and all(_uint(value) for value in counters.values()) and all(counters[key] > 0 for key in COUNTERS - {"retries", "redirects"}), "profile counters are invalid")
    _require(isinstance(timings, dict) and set(timings) == TIMINGS and all(_uint(value) for value in timings.values()) and all(timings[key] > 0 for key in TIMINGS - {"admission"}), "profile timings are invalid")
    samples, elapsed, workers = cell.get("total_nanos"), cell.get("elapsed_nanos"), cell["concurrency"]
    _require(isinstance(samples, list) and len(samples) == 1000 and all(_uint(sample, positive=True) for sample in samples), "profile raw samples are invalid")
    lane_elapsed = max(sum(samples[worker::workers]) for worker in range(workers))
    _require(_uint(elapsed, positive=True) and elapsed >= lane_elapsed, "profile elapsed time is invalid")
    _require((metrics.get("p50_nanos"), metrics.get("p95_nanos"), metrics.get("p99_nanos")) == (_percentile(samples, 50), _percentile(samples, 95), _percentile(samples, 99)), "profile percentiles changed")
    _require(math.isclose(metrics.get("qps", 0), 1_000_000_000_000 / elapsed, rel_tol=1e-12), "profile QPS changed")
    generation = cell.get("generation")
    _require(isinstance(generation, dict) and isinstance(generation.get("Index"), str) and generation["Index"] and _uint(generation.get("Generation"), positive=True), "profile generation is invalid")
    return {"catalog": _validate_catalog(cell, node_ids), "runtime": _validate_runtime(cell, node_ids), "wall": _wall(cell)}


def _run_root(path: Path) -> Path:
    return path.parent.parent if path.parent.name == "client" else path.parent


def _median_tree(values: list[Any]) -> Any:
    if isinstance(values[0], dict):
        return {key: _median_tree([value[key] for value in values]) for key in values[0]}
    if isinstance(values[0], bool):
        return any(values)
    return median(values)


def _validate_command(path: Path, value: dict[str, Any], topology: dict[str, Any], control: str, executable_path: str) -> dict[str, str]:
    root = _run_root(path)
    command_path, time_path, rc_path = (path.with_name(name) for name in ("bench.command.json", "bench.time", "bench.rc"))
    command = json.loads(command_path.read_text(encoding="utf-8"))
    _require(isinstance(command, list) and all(isinstance(part, str) and part for part in command) and rc_path.read_bytes() == b"0\n", "profile client process attestation is invalid")
    if control == "container":
        start = 5
        _require(command[:4] == ["docker", "exec", "-e", "GOMAXPROCS=3"] and command[start:start + 2] == ["/treedb_vector_partition_bench", "system-bench"], "container profile client command is invalid")
        flags = dict(zip(command[start + 2::2], command[start + 3::2], strict=True))
    else:
        offset = 4
        _require(command[:3] == ["/usr/bin/time", "-v", "-o"], "native profile timing command is invalid")
        if control == "native-budgeted":
            _require(command[4:9] == ["taskset", "--cpu-list", "0-2", "env", "GOMAXPROCS=3"], "budgeted client CPU ownership changed")
            offset = 9
        _require(command[offset:offset + 2] == [executable_path, "system-bench"], "native profile client executable changed")
        flags = dict(zip(command[offset + 2::2], command[offset + 3::2], strict=True))
        expected_time = ('\tCommand being timed: "' + " ".join(command[4:]) + '"\n').encode()
        _require(time_path.read_bytes().startswith(expected_time), "native profile timing attestation is invalid")
    expected_flags = {"-endpoint", "-topology", "-dataset", "-truth-cache", "-truth-cache-sha256", "-probes", "-concurrency", "-top-k", "-ef-search", "-warmup", "-out"}
    output = Path(flags.get("-out", ""))
    _require(set(flags) == expected_flags and output.name == path.name == "search.json" and Path(flags["-topology"]).name == "topology.json", "profile client flags changed")
    _require(flags["-endpoint"] == value["endpoint"] and flags["-dataset"] == topology["dataset_directory"] and flags["-truth-cache"] and flags["-probes"] == "2" and flags["-concurrency"] == "1,32" and flags["-top-k"] == "10" and flags["-ef-search"] == "128" and flags["-warmup"] == "1000" and flags["-truth-cache-sha256"] == value["truth_artifact_sha256"], "profile client flags changed")
    result = {"command": _sha256(command_path), "rc": _sha256(rc_path)}
    if control != "container":
        result["time"] = _sha256(time_path)
    return result


def summarize(paths: dict[str, list[Path]], source_revision: str, executable_path: str, executable_sha256: str, container_image_sha256: str) -> dict[str, Any]:
    _require(set(paths) == set(CONTROLS) and all(len(paths[control]) == 3 for control in CONTROLS), "profile requires four controls with three repetitions")
    _require(_git_sha(source_revision) and Path(executable_path).is_absolute() and _is_sha256(executable_sha256) and _is_sha256(container_image_sha256.removeprefix("sha256:")), "profile execution identity is invalid")
    seen_paths: set[Path] = set()
    seen_digests: set[str] = set()
    seen_topologies: set[str] = set()
    roots: list[Path] = []
    runs: dict[str, list[dict[str, Any]]] = {control: [] for control in CONTROLS}
    inputs: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    generations: set[str] = set()
    for control in CONTROLS:
        topology, node_count = TOPOLOGIES[control]
        for repetition, path in enumerate(paths[control], 1):
            canonical = path.resolve(strict=True)
            digest = _sha256(canonical)
            _require(canonical not in seen_paths and digest not in seen_digests, "profile repetition artifacts must be distinct")
            seen_paths.add(canonical)
            seen_digests.add(digest)
            value = _load(canonical)
            _require(value.get("schema_version") == 1 and value.get("result_kind") == "vector_partition_system_bench_v1" and value.get("topology") == topology and value.get("top_k") == 10 and value.get("ef_search") == 128 and value.get("warmup_queries") == 1000 and _is_sha256(value.get("dataset_checksum")) and _is_sha256(value.get("truth_artifact_sha256")), "profile result identity changed")
            cells = value.get("cells")
            _require(isinstance(cells, list) and len(cells) == 2 and {cell.get("concurrency") for cell in cells if isinstance(cell, dict)} == set(CONCURRENCIES), "profile cell matrix changed")
            topology_path = canonical.with_name("topology.json")
            topology_value = _load(topology_path)
            computed, database_roots = _topology_identity(topology_value, topology, node_count)
            _require(value.get("topology_identity_sha256") == topology_value.get("topology_identity_sha256") == computed and computed not in seen_topologies, "profile topology identity changed or was reused")
            seen_topologies.add(computed)
            current_roots = [Path(root) for root in database_roots]
            _require(all(not _paths_overlap(root, other) for index, root in enumerate(current_roots) for other in current_roots[index + 1:]) and all(not _paths_overlap(root, other) for root in current_roots for other in roots), "profile database roots overlap")
            roots.extend(current_roots)
            node_ids = {node["node_config_sha256"] for node in topology_value["nodes"]}
            ready_root = _run_root(canonical)
            ready = {node["node_id"]: _ready_identity(ready_root / "topology.json", topology_value, node, source_revision, executable_sha256) for node in topology_value["nodes"]}
            cell_evidence = {str(cell["concurrency"]): validate_cell(cell, node_ids) for cell in cells}
            runner_path = ready_root / "runner.json"
            runner = _load(runner_path)
            _require(set(runner) == {"phases", "resources", "started_monotonic_nanos", "completed_monotonic_nanos"} and runner["completed_monotonic_nanos"] > runner["started_monotonic_nanos"], "profile runner evidence is invalid")
            resources = runner.get("resources")
            _require(isinstance(resources, dict) and resources.get("profile_concurrency") == 0 and resources.get("cpu_seconds", 0) > 0 and resources.get("peak_rss_bytes", 0) > 0 and resources.get("persistent_bytes", 0) > 0, "profile resource evidence is invalid")
            if control == "native-budgeted":
                _require(resources.get("server_cpu_sets") == ["0-2", "3-5", "6-8", "9-11"] and resources.get("client_cpu_set") == "0-2", "budgeted native CPU ownership changed")
            if control == "container":
                _require(resources.get("image_sha256") == container_image_sha256 and [item.get("cpuset_cpus") for item in resources.get("allocations", [])] == ["0-2", "3-5", "6-8", "9-11"], "container CPU or image ownership changed")
            attestation = _validate_command(canonical, value, topology_value, control, executable_path)
            identities.add((value["dataset_checksum"], value["truth_artifact_sha256"]))
            generations.update(json.dumps(cell["generation"], sort_keys=True) for cell in cells)
            runs[control].append({"value": value, "evidence": cell_evidence})
            inputs.append({"control": control, "repetition": repetition, "path": str(path), "sha256": digest, "topology_identity_sha256": computed, "ready_sha256": ready, "runner_sha256": _sha256(runner_path), "client_attestation_sha256": attestation})
    _require(len(identities) == len(generations) == 1, "profile fixture, truth, or generation changed")
    rows: list[dict[str, Any]] = []
    tainted = False
    for concurrency in CONCURRENCIES:
        row: dict[str, Any] = {"probes": 2, "concurrency": concurrency, "controls": {}}
        for control in CONTROLS:
            cells = [next(cell for cell in run["value"]["cells"] if cell["concurrency"] == concurrency) for run in runs[control]]
            evidence = [run["evidence"][str(concurrency)] for run in runs[control]]
            qps = [cell["metrics"]["qps"] for cell in cells]
            p95 = [cell["metrics"]["p95_nanos"] for cell in cells]
            spread = max((max(qps) - min(qps)) / median(qps), (max(p95) - min(p95)) / median(p95))
            tainted |= spread > .10
            row["controls"][control] = {
                "recall_at_10_median": median(cell["metrics"]["recall_at_10"] for cell in cells),
                "qps_median": median(qps), "qps_min": min(qps), "qps_max": max(qps),
                "p95_nanos_median": median(p95), "p95_nanos_min": min(p95), "p95_nanos_max": max(p95),
                "max_relative_spread": spread,
                "counters_per_query_median": {key: median(cell["counters"][key] / 1000 for cell in cells) for key in sorted(COUNTERS)},
                "catalog_median": _median_tree([item["catalog"] for item in evidence]),
                "runtime_median": _median_tree([item["runtime"] for item in evidence]),
                "wall_median": _median_tree([item["wall"] for item in evidence]),
            }
        rows.append(row)
    identity = next(iter(identities))
    return {
        "schema_version": 1, "result_kind": "vector_partition_onramp_profile_v1", "status": "tainted" if tainted else "valid",
        "execution_identity": {"source_revision": source_revision, "vcs_modified": False, "executable_path": executable_path, "executable_sha256": executable_sha256, "container_image_sha256": container_image_sha256},
        "fixture_truth_identity": {"dataset_checksum": identity[0], "truth_artifact_sha256": identity[1]},
        "inputs": inputs, "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    for control in CONTROLS:
        parser.add_argument("--" + control, type=Path, action="append", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--executable-path", required=True)
    parser.add_argument("--executable-sha256", required=True)
    parser.add_argument("--container-image-sha256", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    paths = {control: getattr(args, control.replace("-", "_")) for control in CONTROLS}
    result = summarize(paths, args.source_revision, args.executable_path, args.executable_sha256, args.container_image_sha256)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
