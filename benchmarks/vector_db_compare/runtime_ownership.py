#!/usr/bin/env python3
"""Validate and summarize the bounded #4091 runtime-ownership evidence."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

from system_qualification import _is_sha256, _percentile, _require
from topology_tax import _git_sha, _load, _paths_overlap, _ready_identity, _sha256, _topology_identity, _uint


TOPOLOGIES = {
    "single": ("single_daemon_four_group", 1),
    "native": ("native_four_daemon_four_group", 4),
    "container": ("container_four_daemon_four_group", 4),
}
CONCURRENCIES = (1, 32)
CPU_SETS = ("0-2", "3-5", "6-8", "9-11")
MEMORY_BYTES = 6 << 30
SEMANTIC_COUNTERS = (
    "selected_partitions", "selected_groups", "requests", "rpcs", "retries", "redirects",
    "candidates", "edges", "query_bytes", "request_bytes", "candidate_bytes", "response_bytes",
)
PARITY_COUNTERS = SEMANTIC_COUNTERS
POSITIVE_COUNTERS = tuple(key for key in SEMANTIC_COUNTERS if key not in ("retries", "redirects"))
FRAME_COUNTERS = ("public_request_frame_bytes", "public_response_frame_bytes")
CUMULATIVE_RUNTIME = (
    "cpu_time_nanos", "run_queue_delay_nanos", "timeslices", "voluntary_context_switches",
    "nonvoluntary_context_switches", "total_alloc_bytes", "mallocs", "frees", "num_gc", "pause_total_nanos",
)
GAUGE_RUNTIME = ("rss_bytes", "peak_rss_bytes", "heap_alloc_bytes", "heap_objects", "goroutines")
PROOF_STAGES = ("total", "operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")
PROOF_COUNTS = ("reads", "successes", "failures", "verify_leader_calls", "log_barriers", "no_log_proofs")
SNAPSHOT_SINGLE_QPS = {1: 522.9998, 32: 2915.9708}
FROZEN_CONTAINER_QPS = {1: 40.1, 32: 126.4}


def _runtime_projection(cell: dict[str, Any], nodes: list[dict[str, Any]]) -> dict[str, int]:
    ownership = {node["node_config_sha256"]: node.get("runtime_ownership") for node in nodes}
    runtime = cell.get("runtime")
    _require(isinstance(runtime, list) and len(runtime) == len(nodes), "runtime evidence does not cover every node")
    _require({entry.get("node_config_sha256") for entry in runtime if isinstance(entry, dict)} == set(ownership), "runtime evidence node identity changed")
    out = {key: 0 for key in CUMULATIVE_RUNTIME}
    out.update({f"{key}_after": 0 for key in GAUGE_RUNTIME})
    for entry in runtime:
        before, after = entry.get("before"), entry.get("after")
        _require(isinstance(before, dict) and isinstance(after, dict), "runtime snapshots are invalid")
        _require(_uint(before.get("sample_unix_nano"), positive=True) and after.get("sample_unix_nano", 0) > before["sample_unix_nano"], "runtime sample time is invalid")
        declared = ownership[entry["node_config_sha256"]]
        for sample in (before, after):
            _require(_uint(sample.get("logical_cpus"), positive=True) and _uint(sample.get("gomaxprocs"), positive=True), "runtime CPU evidence is invalid")
            _require(isinstance(sample.get("go_memory_limit_bytes"), int) and sample["go_memory_limit_bytes"] > 0 and isinstance(sample.get("effective_cpu_set"), str) and sample["effective_cpu_set"], "runtime resource evidence is invalid")
            if declared is not None:
                _require(
                    sample["effective_cpu_set"] == declared["cpu_set"] and sample["gomaxprocs"] == declared["gomaxprocs"] and
                    sample["go_memory_limit_bytes"] == declared["go_memory_limit_bytes"],
                    "runtime evidence differs from declared ownership",
                )
        for key in CUMULATIVE_RUNTIME:
            _require(_uint(before.get(key)) and _uint(after.get(key)) and after[key] >= before[key], "runtime cumulative counter is invalid")
            out[key] += after[key] - before[key]
        for key in GAUGE_RUNTIME:
            _require(_uint(after.get(key), positive=key == "goroutines"), "runtime gauge is invalid")
            out[f"{key}_after"] += after[key]
    return out


def _proof_projection(cell: dict[str, Any]) -> dict[str, int]:
    total = cell.get("catalog_reads", {}).get("total")
    _require(isinstance(total, dict), "catalog proof evidence is missing")
    out: dict[str, int] = {}
    for stage in PROOF_STAGES:
        value = total.get(stage)
        _require(isinstance(value, dict), "catalog proof stage is missing")
        for key in PROOF_COUNTS:
            _require(_uint(value.get(key)), "catalog proof count is invalid")
            out[f"{stage}.{key}"] = value[key]
        _require(value["reads"] == value["successes"] == value["verify_leader_calls"] == value["no_log_proofs"], "catalog proof counts disagree")
        _require(value["failures"] == value["log_barriers"] == 0, "catalog proof used a failed or log-appending path")
    _require(out["total.reads"] == 3834 and out["operations_health.reads"] == 1000 and out["coordinator_lifecycle.reads"] == 1000 and out["shard_lifecycle.reads"] == 1834 and out["unknown.reads"] == 0, "catalog proof work changed")
    return out


def _proof_parity(cell: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    return tuple(cell["_proof_projection"].items())


def _validate_result(value: dict[str, Any], topology: str, nodes: list[dict[str, Any]], dataset_checksum: str, truth_sha256: str) -> dict[int, dict[str, Any]]:
    required = {"schema_version", "result_kind", "endpoint", "topology", "topology_identity_sha256", "dataset_checksum", "truth_artifact_sha256", "top_k", "ef_search", "warmup_queries", "started_at", "completed_at", "cells"}
    _require(set(value) == required and value.get("schema_version") == 1 and value.get("result_kind") == "vector_partition_system_bench_v1" and value.get("topology") == topology, "runtime-ownership result identity changed")
    _require(value.get("top_k") == 10 and value.get("ef_search") == 128 and value.get("warmup_queries") == 1000 and value.get("dataset_checksum") == dataset_checksum and value.get("truth_artifact_sha256") == truth_sha256, "runtime-ownership search contract changed")
    cells = value.get("cells")
    _require(isinstance(cells, list) and len(cells) == 2, "runtime-ownership result must contain c1 and c32")
    by_concurrency: dict[int, dict[str, Any]] = {}
    for cell in cells:
        _require(isinstance(cell, dict) and cell.get("status") == "valid" and not cell.get("error") and cell.get("budget") == {"probes": 2} and cell.get("concurrency") in CONCURRENCIES, "runtime-ownership cell is invalid")
        concurrency = cell["concurrency"]
        _require(concurrency not in by_concurrency, "runtime-ownership cell is duplicated")
        metrics = cell.get("metrics")
        _require(isinstance(metrics, dict) and metrics.get("queries") == metrics.get("completed_queries") == 1000 and metrics.get("result_count") == 10000 and metrics.get("errors") == metrics.get("timeouts") == 0, "runtime-ownership cell is incomplete")
        _require(isinstance(metrics.get("recall_at_10"), (int, float)) and not isinstance(metrics["recall_at_10"], bool) and math.isfinite(metrics["recall_at_10"]) and .90 <= metrics["recall_at_10"] <= 1, "runtime-ownership recall is below the floor")
        samples, elapsed = cell.get("total_nanos"), cell.get("elapsed_nanos")
        _require(isinstance(samples, list) and len(samples) == 1000 and all(_uint(sample, positive=True) for sample in samples), "runtime-ownership raw timings are invalid")
        _require(_uint(elapsed, positive=True) and elapsed >= max(sum(samples[worker::concurrency]) for worker in range(concurrency)), "runtime-ownership elapsed time is invalid")
        _require((metrics.get("p50_nanos"), metrics.get("p95_nanos"), metrics.get("p99_nanos")) == tuple(_percentile(samples, percentile) for percentile in (50, 95, 99)), "runtime-ownership percentiles changed")
        _require(math.isclose(metrics.get("qps", 0), 1_000_000_000_000 / elapsed, rel_tol=1e-12), "runtime-ownership QPS changed")
        counters = cell.get("counters")
        _require(isinstance(counters, dict) and set(counters) == set(SEMANTIC_COUNTERS) | set(FRAME_COUNTERS) and all(_uint(counters[key]) for key in counters), "runtime-ownership counters are invalid")
        _require(counters["retries"] == counters["redirects"] == 0 and all(counters[key] > 0 for key in POSITIVE_COUNTERS), "runtime-ownership counters are incomplete")
        generation = cell.get("generation")
        _require(generation == {"Index": "embedding_graph", "Generation": 1}, "runtime-ownership generation changed")
        cell["_runtime_projection"] = _runtime_projection(cell, nodes)
        cell["_proof_projection"] = _proof_projection(cell)
        by_concurrency[concurrency] = cell
    _require(set(by_concurrency) == set(CONCURRENCIES), "runtime-ownership concurrency matrix changed")
    return by_concurrency


def _validate_client(run: Path, topology: str, repetition: int, value: dict[str, Any], provenance: dict[str, Any]) -> dict[str, str]:
    command_path, time_path, rc_path = (run / name for name in ("bench.command.json", "bench.time", "bench.rc"))
    command = json.loads(command_path.read_text(encoding="utf-8"))
    _require(isinstance(command, list) and command[:4] == ["/usr/bin/time", "-v", "-o", str(time_path)] and command.count("system-bench") == 1, "runtime-ownership client command is invalid")
    index = command.index("system-bench")
    prefix = command[4:index]
    binary = str((run.parents[2] / "bin/treedb_vector_partition_bench").resolve())
    if topology == "single":
        _require(prefix == [binary], "single client process ownership changed")
    elif topology == "native":
        _require(prefix == ["taskset", "--cpu-list", CPU_SETS[0], "env", "GOMAXPROCS=3", binary], "native client process ownership changed")
    else:
        _require(prefix == ["docker", "exec", f"gomap4091-r{repetition}-group-a", "/treedb_vector_partition_bench"], "container client process ownership changed")
    args = command[index + 1:]
    _require(len(args) % 2 == 0, "runtime-ownership client arguments are invalid")
    flags = dict(zip(args[::2], args[1::2], strict=True))
    _require(len(flags) * 2 == len(args) and flags == {
        "-endpoint": value["endpoint"], "-topology": str(run / "topology.json"), "-dataset": provenance["dataset_directory"],
        "-truth-cache": provenance["truth_directory"], "-truth-cache-sha256": provenance["truth_sha256"], "-probes": "2",
        "-concurrency": "1,32", "-top-k": "10", "-ef-search": "128", "-warmup": "1000", "-out": str(run / "search.json"),
    }, "runtime-ownership client arguments changed")
    expected = ('\tCommand being timed: "' + " ".join(command[4:]) + '"\n').encode()
    _require(time_path.read_bytes().startswith(expected) and rc_path.read_bytes() == b"0\n", "runtime-ownership client process did not complete cleanly")
    return {name: _sha256(path) for name, path in (("command", command_path), ("time", time_path), ("rc", rc_path))}


def _distribution(cells: list[dict[str, Any]]) -> dict[str, Any]:
    qps = [cell["metrics"]["qps"] for cell in cells]
    p95 = [cell["metrics"]["p95_nanos"] for cell in cells]
    recalls = [cell["metrics"]["recall_at_10"] for cell in cells]
    runtime_keys = cells[0]["_runtime_projection"]
    return {
        "recall_at_10_min": min(recalls), "recall_at_10_median": median(recalls), "recall_at_10_max": max(recalls),
        "qps_min": min(qps), "qps_median": median(qps), "qps_max": max(qps),
        "p95_nanos_min": min(p95), "p95_nanos_median": median(p95), "p95_nanos_max": max(p95),
        "runtime_median": {key: median(cell["_runtime_projection"][key] for cell in cells) for key in runtime_keys},
        "public_frame_bytes_median": {key: median(cell["counters"][key] for cell in cells) for key in FRAME_COUNTERS},
    }


def summarize(root: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    provenance = _load(root / "provenance.json")
    hashes = ("binary_sha256", "rebinder_sha256", "fixture_manifest_sha256", "truth_sha256", "m3_artifact_sha256", "m3_descriptor_sha256")
    _require(
        isinstance(provenance, dict) and all(_is_sha256(provenance.get(key)) for key in hashes) and
        _git_sha(provenance.get("base_head")) and _git_sha(provenance.get("source_head")) and
        _is_sha256(str(provenance.get("container_image_id", "")).removeprefix("sha256:")),
        "runtime-ownership provenance is invalid",
    )
    binary = root / "bin/treedb_vector_partition_bench"
    rebinder = root / "bin/rebind_snapshot"
    fixture = Path(provenance["dataset_directory"]) / "fixture_manifest.json"
    truth_files = list(Path(provenance["truth_directory"]).glob("*.json"))
    descriptor = Path(provenance["m3_database_directory"]) / "vector_partition_variant_v1.json"
    _require(_sha256(binary) == provenance["binary_sha256"] and _sha256(rebinder) == provenance["rebinder_sha256"] and _sha256(fixture) == provenance["fixture_manifest_sha256"] and len(truth_files) == 1 and _sha256(truth_files[0]) == provenance["truth_sha256"] and _sha256(descriptor) == provenance["m3_descriptor_sha256"], "runtime-ownership retained input changed")
    fixture_value = _load(fixture)
    descriptor_value = _load(descriptor)
    artifacts = list((root / "250k/graph-overlap-020-out").glob(f"vector_partition_{provenance['m3_artifact_sha256'][:12]}_*.json"))
    _require(
        len(artifacts) == 1 and _sha256(artifacts[0]) == provenance["m3_artifact_sha256"] and
        descriptor_value.get("base_sha") == provenance["base_head"] and descriptor_value.get("head_sha") == provenance["source_head"] and
        descriptor_value.get("executable_sha256") == provenance["binary_sha256"] and descriptor_value.get("artifact_sha256") == provenance["m3_artifact_sha256"] and
        descriptor_value.get("fixture_checksum") == fixture_value.get("checksum"),
        "runtime-ownership M3 identity changed",
    )
    runs: dict[str, list[dict[int, dict[str, Any]]]] = {topology: [] for topology in TOPOLOGIES}
    inputs: list[dict[str, Any]] = []
    roots: list[Path] = []
    identities: set[str] = set()
    for topology, (topology_name, node_count) in TOPOLOGIES.items():
        for repetition in (1, 2, 3):
            run = root / "runs" / topology / f"repeat-{repetition}"
            search_path, topology_path = run / "search.json", run / "topology.json"
            value, topology_value = _load(search_path), _load(topology_path)
            computed, database_roots = _topology_identity(topology_value, topology_name, node_count)
            _require(value.get("topology_identity_sha256") == topology_value.get("topology_identity_sha256") == computed and computed not in identities, "runtime-ownership topology identity is invalid or reused")
            identities.add(computed)
            canonical_roots = [Path(path) for path in database_roots]
            _require(all(not _paths_overlap(path, other) for path in canonical_roots for other in roots), "runtime-ownership database roots overlap across runs")
            roots.extend(canonical_roots)
            nodes = topology_value["nodes"]
            if topology == "single":
                _require(all("runtime_ownership" not in node for node in nodes), "single control unexpectedly declares runtime ownership")
            else:
                _require([node.get("runtime_ownership") for node in nodes] == [{"cpu_set": cpu, "gomaxprocs": 3, "go_memory_limit_bytes": MEMORY_BYTES} for cpu in CPU_SETS], "four-daemon ownership changed")
            ready = {node["node_id"]: _ready_identity(topology_path, topology_value, node, provenance["source_head"], provenance["binary_sha256"]) for node in nodes}
            cells = _validate_result(value, topology_name, nodes, fixture_value["checksum"], provenance["truth_sha256"])
            runs[topology].append(cells)
            inputs.append({
                "topology": topology, "repetition": repetition, "search_path": str(search_path.relative_to(root)),
                "search_sha256": _sha256(search_path), "topology_sha256": _sha256(topology_path),
                "topology_identity_sha256": computed, "ready_sha256": ready,
                "client_attestation_sha256": _validate_client(run, topology, repetition, value, provenance),
            })
    all_cells = [cell for values in runs.values() for run in values for cell in run.values()]
    _require({cell["metrics"]["recall_at_10"] for cell in all_cells} == {.9246999999999927}, "runtime-ownership recall changed across topologies")
    _require({json.dumps(cell["generation"], sort_keys=True) for cell in all_cells} == {'{"Generation": 1, "Index": "embedding_graph"}'}, "runtime-ownership generation changed across topologies")
    rows = []
    for concurrency in CONCURRENCIES:
        cells = {topology: [run[concurrency] for run in runs[topology]] for topology in TOPOLOGIES}
        _require(len({tuple(cell["counters"][key] for key in PARITY_COUNTERS) for values in cells.values() for cell in values}) == 1, "runtime-ownership logical work or service payload bytes changed")
        _require(len({_proof_parity(cell) for values in cells.values() for cell in values}) == 1, "runtime-ownership proof counts changed")
        distributions = {topology: _distribution(values) for topology, values in cells.items()}
        native, container = distributions["native"], distributions["container"]
        qps_ratio = native["qps_median"] / container["qps_median"]
        p95_ratio = native["p95_nanos_median"] / container["p95_nanos_median"]
        _require(.9 <= qps_ratio <= 1.1 and .9 <= p95_ratio <= 1.1, "native and container runtime ownership differ by more than 10 percent")
        _require(distributions["single"]["qps_median"] >= .97 * SNAPSHOT_SINGLE_QPS[concurrency] and container["qps_median"] >= .97 * FROZEN_CONTAINER_QPS[concurrency], "single or container control regressed")
        rows.append({
            "concurrency": concurrency, "topologies": distributions,
            "native_over_container_qps": qps_ratio, "native_over_container_p95": p95_ratio,
            "native_over_single_qps": native["qps_median"] / distributions["single"]["qps_median"],
            "native_over_single_p95": native["p95_nanos_median"] / distributions["single"]["p95_nanos_median"],
        })
    return {
        "schema_version": 1, "result_kind": "vector_partition_runtime_ownership_comparison_v1", "status": "qualified",
        "execution_identity": {key: provenance[key] for key in ("base_head", "source_head", "binary_sha256", "container_image", "container_image_id", "rebinder_sha256")},
        "fixture_truth_m3_identity": {
            "dataset_checksum": fixture_value["checksum"], "fixture_manifest_sha256": provenance["fixture_manifest_sha256"],
            "truth_artifact_sha256": provenance["truth_sha256"], "m3_artifact_sha256": provenance["m3_artifact_sha256"],
            "m3_descriptor_sha256": provenance["m3_descriptor_sha256"],
        },
        "inputs": inputs, "rows": rows,
        "invariants": {
            "repetitions_per_topology": 3, "queries_per_cell": 1000, "probes": 2, "ef_search": 128,
            "native_container_median_parity_limit": .10, "recall_floor": .90,
            "semantic_work_and_service_payload_bytes_identical": True, "proof_counts_identical": True,
            "public_frame_bytes_expected_to_include_topology_specific_metadata": True,
        },
        "claim_boundary": "#4091 isolates explicit CPU and Go-runtime ownership. It does not change topology, routing, index/search, wire, or Raft proof semantics.",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.root)
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
