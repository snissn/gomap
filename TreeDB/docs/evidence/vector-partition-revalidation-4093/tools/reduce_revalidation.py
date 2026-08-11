#!/usr/bin/env python3
"""Fail-closed reducer for the focused #4093 TreeDB revalidation."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from statistics import median
import sys
from typing import Any


SOURCE_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(SOURCE_ROOT / "benchmarks/vector_db_compare"))
from system_qualification import ContractError, _is_sha256, _load, _percentile, _require  # noqa: E402
from topology_tax import COUNTERS as LOGICAL_COUNTERS, _paths_overlap, _ready_identity, _sha256, _topology_identity  # noqa: E402


MODES = ("strict", "fast", "pinned")
TOPOLOGIES = ("single", "native", "container")
PUBLIC_ROUTE = "treedb.nativewire.vector_search_v1"
TOPOLOGY_NAMES = {
    "single": "single_daemon_four_group",
    "native": "native_four_daemon_four_group",
    "container": "container_four_daemon_four_group",
}
SEQUENCE = (
    ("single", 1), ("native", 1), ("container", 1),
    ("native", 2), ("container", 2), ("single", 2),
    ("container", 3), ("single", 3), ("native", 3),
)
TIMINGS = (
    "admission", "operations_health", "service_adapter", "public_adapter",
    "router_open", "router_search", "placement", "coordinator_lifecycle",
    "dispatch", "queue", "rpc", "network", "read_index_apply",
    "generation_open", "shard_search", "response", "dedupe", "merge",
    "coordinator_total", "total", "client_encode", "client_write",
    "client_response_read", "client_decode", "client_total",
)
COUNTERS = LOGICAL_COUNTERS + (
    "snapshot_pins", "session_pins", "read_proofs", "generation_pins",
    "partition_opens", "public_request_frame_bytes", "public_response_frame_bytes",
)
SEMANTIC_COUNTERS = tuple(name for name in LOGICAL_COUNTERS if name != "request_bytes")
RUNTIME_DELTAS = (
    "cpu_time_nanos", "run_queue_delay_nanos", "timeslices",
    "voluntary_context_switches", "nonvoluntary_context_switches",
    "total_alloc_bytes", "mallocs", "frees", "num_gc", "pause_total_nanos",
)
CATALOG_STAGES = ("total", "strict_search", "serving_refresh", "operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")
CATALOG_FIELDS = (
    "reads", "successes", "failures", "verify_leader_calls", "log_barriers", "no_log_proofs",
    "total_nanos", "admission_nanos", "verify_leader_nanos", "barrier_nanos",
    "current_term_nanos", "raft_apply_nanos", "applied_read_nanos",
)
CATALOG_IDENTITY_FIELDS = (
    "last_term", "last_catalog_applied_index", "last_raft_applied_index", "last_raft_log_index",
)
BASELINE = {
    ("single", 1): (668.6856, 1_843_000),
    ("single", 32): (3043.812, 16_301_000),
    ("container", 1): (553.900, 2_551_000),
    ("container", 32): (1615.483, 30_206_000),
}
PROFILE_NAMES = ("allocs_baseline.pprof", "cpu.pprof", "trace.out", "heap.pprof", "allocs.pprof", "block.pprof", "mutex.pprof")


def _read(path: Path, limit: int = 16 << 20) -> bytes:
    _require(path.is_file(), f"missing retained artifact {path}")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    _require(len(raw) <= limit, f"{path} exceeds {limit} bytes")
    return raw


def _time(value: Any) -> datetime:
    _require(isinstance(value, str) and value.endswith("Z"), "retained timestamp is invalid")
    try:
        body = value[:-1]
        if "." in body:
            whole, fraction = body.split(".", 1)
            body = f"{whole}.{(fraction + '000000')[:6]}"
        return datetime.fromisoformat(body + "+00:00")
    except ValueError as exc:
        raise ContractError("retained timestamp is invalid") from exc


def _uint(value: Any, *, positive: bool = False) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= (1 if positive else 0)


def _catalog_identity(value: dict[str, Any]) -> dict[str, int]:
    _require(set(value) == set(CATALOG_STAGES) | set(CATALOG_IDENTITY_FIELDS), "catalog proof identity structure is invalid")
    identity = {key: value[key] for key in CATALOG_IDENTITY_FIELDS}
    _require(all(_uint(item, positive=True) for item in identity.values()) and
             identity["last_raft_applied_index"] >= identity["last_catalog_applied_index"] and
             identity["last_raft_log_index"] >= identity["last_raft_applied_index"],
             "catalog proof identity is invalid")
    return identity


def _validate_public_endpoint(topology: dict[str, Any], result: dict[str, Any]) -> None:
    public = [node for node in topology["nodes"] if "public_listen" in node]
    _require(len(public) == 1, "retained topology public node is invalid")

    def split(value: str) -> tuple[str, str]:
        parts = value[1:].rsplit("]:", 1) if value.startswith("[") and "]:" in value else value.rsplit(":", 1)
        _require(len(parts) == 2 and all(parts), "benchmark endpoint is invalid")
        return parts[0], parts[1]

    configured_host, configured_port = split(public[0]["public_listen"])
    actual_host, actual_port = split(result.get("endpoint", ""))
    if configured_host in ("0.0.0.0", "::"):
        owned_hosts = {split(group["listen"])[0] for group in public[0]["local_groups"]}
        _require(actual_port == configured_port and actual_host in owned_hosts, "benchmark endpoint is not owned by the retained public node")
    else:
        _require(result.get("endpoint") == public[0]["public_listen"], "benchmark endpoint is not owned by the retained public node")


def _validate_capability_key(provenance: dict[str, Any]) -> None:
    path = Path(str(provenance.get("capability_key_path", "")))
    _require(path.is_file() and _is_sha256(provenance.get("capability_key_sha256")) and
             _sha256(path) == provenance["capability_key_sha256"], "capability key changed")


def _validate_m3(root: Path, provenance: dict[str, Any]) -> None:
    descriptor_path = Path(provenance["m3_database_directory"]) / "vector_partition_variant_v1.json"
    fixture_path = Path(provenance["dataset_directory"]) / "fixture_manifest.json"
    descriptor = _load(descriptor_path, 1 << 20)
    fixture = _load(fixture_path, 1 << 20)
    artifacts = list((root / "250k/graph-overlap-020-out").glob(
        f"vector_partition_{provenance['m3_artifact_sha256'][:12]}_*.json"))
    _require(
        _sha256(descriptor_path) == provenance["m3_descriptor_sha256"] and
        len(artifacts) == 1 and _sha256(artifacts[0]) == provenance["m3_artifact_sha256"] and
        descriptor.get("base_sha") == provenance["base_sha"] and
        descriptor.get("head_sha") == provenance["source_head"] and
        descriptor.get("executable_sha256") == provenance["binary_sha256"] and
        descriptor.get("artifact_sha256") == provenance["m3_artifact_sha256"] and
        descriptor.get("fixture_checksum") == fixture.get("checksum") == provenance["fixture_checksum"],
        "M3 execution provenance changed",
    )


def _runtime(cell: dict[str, Any], expected_nodes: dict[str, dict[str, Any]]) -> dict[str, float | int]:
    nodes = cell.get("runtime")
    _require(isinstance(nodes, list) and nodes, "runtime observations are missing")
    result: dict[str, float | int] = {key: 0 for key in RUNTIME_DELTAS}
    result.update({"peak_rss_bytes": 0, "heap_alloc_bytes_after": 0, "heap_objects_after": 0, "goroutines_after": 0})
    identities: set[str] = set()
    for node in nodes:
        _require(isinstance(node, dict) and _is_sha256(node.get("node_config_sha256")), "runtime node identity is invalid")
        _require(node["node_config_sha256"] not in identities, "runtime node identity is duplicated")
        identities.add(node["node_config_sha256"])
        before, after = node.get("before"), node.get("after")
        _require(isinstance(before, dict) and isinstance(after, dict), "runtime observation is invalid")
        owner = expected_nodes.get(node["node_config_sha256"])
        _require(isinstance(owner, dict) and all(
            before.get(observed) == after.get(observed) == owner.get(configured)
            for observed, configured in (("effective_cpu_set", "cpu_set"), ("gomaxprocs", "gomaxprocs"), ("go_memory_limit_bytes", "go_memory_limit_bytes"))
        ), "runtime ownership changed during measurement")
        for key in RUNTIME_DELTAS:
            _require(_uint(before.get(key)) and _uint(after.get(key)) and after[key] >= before[key], f"runtime {key} regressed")
            result[key] += after[key] - before[key]
        _require(_uint(after.get("peak_rss_bytes"), positive=True), "runtime peak RSS is invalid")
        result["peak_rss_bytes"] = max(result["peak_rss_bytes"], after["peak_rss_bytes"])
        for source, target in (("heap_alloc_bytes", "heap_alloc_bytes_after"), ("heap_objects", "heap_objects_after"), ("goroutines", "goroutines_after")):
            _require(_uint(after.get(source), positive=True), f"runtime {source} is invalid")
            result[target] += after[source]
    return result


def _validate_catalog(cell: dict[str, Any], mode: str, concurrency: int, expected_ids: set[str]) -> int:
    catalog = cell.get("catalog_reads")
    _require(isinstance(catalog, dict) and isinstance(catalog.get("nodes"), list) and isinstance(catalog.get("total"), dict), "catalog proof evidence is invalid")
    _require({node.get("node_config_sha256") for node in catalog["nodes"] if isinstance(node, dict)} == expected_ids and len(catalog["nodes"]) == len(expected_ids), "catalog proof nodes changed")
    total = catalog["total"]
    total_identity = _catalog_identity(total)
    summed = {name: {key: 0 for key in CATALOG_FIELDS} for name in CATALOG_STAGES}
    summed_identity = {key: 0 for key in CATALOG_IDENTITY_FIELDS}
    for node in catalog["nodes"]:
        before, after, delta = node.get("before"), node.get("after"), node.get("delta")
        _require(all(isinstance(value, dict) for value in (before, after, delta)), "catalog node evidence is invalid")
        prior_identity, current_identity, retained_identity = (_catalog_identity(value) for value in (before, after, delta))
        _require(retained_identity == current_identity and all(current_identity[key] >= prior_identity[key] for key in CATALOG_IDENTITY_FIELDS), "catalog proof identity changed")
        for key in CATALOG_IDENTITY_FIELDS:
            summed_identity[key] = max(summed_identity[key], retained_identity[key])
        for name in CATALOG_STAGES:
            prior, current, retained = before.get(name), after.get(name), delta.get(name)
            _require(all(isinstance(value, dict) and set(value) == set(CATALOG_FIELDS) for value in (prior, current, retained)) and
                     all(_uint(prior[key]) and _uint(current[key]) and current[key] >= prior[key] for key in CATALOG_FIELDS),
                     "catalog node observation is invalid")
            expected = {key: current[key] - prior[key] for key in CATALOG_FIELDS}
            _require(retained == expected, "catalog node delta changed")
            for key in CATALOG_FIELDS:
                summed[name][key] += retained[key]
    _require(all(total.get(name) == summed[name] for name in CATALOG_STAGES), "catalog node totals disagree with the aggregate")
    _require(total_identity == summed_identity, "catalog proof identity totals disagree with the aggregate")
    strict, refresh, aggregate = total.get("strict_search"), total.get("serving_refresh"), total.get("total")
    _require(all(isinstance(value, dict) for value in (strict, refresh, aggregate)), "catalog proof attribution is invalid")
    for name in CATALOG_STAGES:
        value = total.get(name)
        _require(isinstance(value, dict) and
                 value.get("reads") == value.get("successes") == value.get("verify_leader_calls") == value.get("no_log_proofs") and
                 value.get("failures") == value.get("log_barriers") == 0,
                 "catalog proof was unsuccessful or appended a log barrier")
    want = 1000 if mode == "strict" else 0
    _require(strict.get("reads") == want and aggregate.get("reads") == want + refresh.get("reads"), "catalog proof count changed")
    for name in ("operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown"):
        _require(total.get(name, {}).get("reads") == 0, f"unexpected {name} catalog reads")
    _require(aggregate.get("log_barriers") == 0 and aggregate.get("failures") == 0, "search performed a log barrier or failed proof")
    counters = cell["counters"]
    _require(all(counters[key] == 0 for key in ("read_proofs", "generation_pins", "partition_opens")), "request path reopened data or consensus proof")
    _require(counters["snapshot_pins"] == (0 if mode == "pinned" else 1000), "snapshot pin count changed")
    _require(counters["session_pins"] == (concurrency if mode == "pinned" else 0), "session pin count changed")
    return strict["reads"]


def _validate_fast(cell: dict[str, Any], result: dict[str, Any], mode: str) -> None:
    evidence = cell.get("fast_evidence")
    if mode == "strict":
        _require(evidence is None and "min_index_age_nanos" not in cell and "max_index_age_nanos" not in cell, "strict row retained fast evidence")
        return
    _require(isinstance(evidence, dict), "fast row lacks snapshot evidence")
    _require(evidence.get("Generation") == cell["generation"] and _uint(evidence.get("IndexedThrough")), "fast snapshot generation or watermark is invalid")
    _require(isinstance(evidence.get("PublishedAt"), str) and evidence["PublishedAt"] and isinstance(evidence.get("TopologyDigest"), str) and evidence["TopologyDigest"] and isinstance(evidence.get("AuthorizationOverlayDigest"), str) and evidence["AuthorizationOverlayDigest"], "fast snapshot identity is invalid")
    minimum, maximum = cell.get("min_index_age_nanos"), cell.get("max_index_age_nanos")
    _require(_uint(minimum) and _uint(maximum) and minimum <= maximum <= result["max_index_age_nanos"], "fast snapshot age exceeded its bound")
    age = evidence.get("IndexAge")
    _require(_uint(age) and age <= minimum, "fast snapshot evidence age postdates its observed range")


def _validate_cell(cell: dict[str, Any], result: dict[str, Any], mode: str, concurrency: int, expected_nodes: dict[str, dict[str, Any]]) -> dict[str, Any]:
    _require(isinstance(cell, dict) and cell.get("status") == "valid" and not cell.get("error"), "benchmark cell failed")
    _require(cell.get("budget") == {"probes": 2} and cell.get("concurrency") == concurrency and cell.get("search_mode") == mode, "benchmark cell identity changed")
    metrics, counters, timings = cell.get("metrics"), cell.get("counters"), cell.get("timings")
    _require(isinstance(metrics, dict) and metrics.get("queries") == 1000 and metrics.get("completed_queries") == 1000 and metrics.get("result_count") == 10000 and metrics.get("errors") == 0 and metrics.get("timeouts") == 0, "benchmark cell is incomplete")
    recall = metrics.get("recall_at_10")
    _require(isinstance(recall, (int, float)) and not isinstance(recall, bool) and math.isfinite(recall) and .90 <= recall <= 1, "recall is below the frozen floor")
    _require(isinstance(counters, dict) and set(counters) == set(COUNTERS) and all(_uint(counters[key]) for key in COUNTERS), "benchmark counters are invalid")
    _require(counters["retries"] == 0 and counters["redirects"] == 0, "benchmark cell retried or followed a redirect")
    _require(isinstance(timings, dict) and set(timings) == set(TIMINGS) and all(_uint(timings[key]) for key in TIMINGS), "benchmark timings are invalid")
    samples, elapsed = cell.get("total_nanos"), cell.get("elapsed_nanos")
    _require(isinstance(samples, list) and len(samples) == 1000 and all(_uint(sample, positive=True) for sample in samples), "raw query timings are invalid")
    _require(timings["client_total"] == sum(samples), "client timing total changed")
    _require(_uint(elapsed, positive=True) and elapsed >= max(sum(samples[lane::concurrency]) for lane in range(concurrency)), "cell elapsed time is too small")
    _require((metrics.get("p50_nanos"), metrics.get("p95_nanos"), metrics.get("p99_nanos")) == tuple(_percentile(samples, value) for value in (50, 95, 99)), "cell percentiles changed")
    _require(math.isclose(metrics.get("qps", 0), 1_000_000_000_000 / elapsed, rel_tol=1e-12), "cell QPS changed")
    generation = cell.get("generation")
    _require(isinstance(generation, dict) and isinstance(generation.get("Index"), str) and generation["Index"] and _uint(generation.get("Generation"), positive=True), "cell generation is invalid")
    expected_ids = set(expected_nodes)
    catalog_proof_reads = _validate_catalog(cell, mode, concurrency, expected_ids)
    _validate_fast(cell, result, mode)
    runtime = _runtime(cell, expected_nodes)
    _require({node.get("node_config_sha256") for node in cell["runtime"] if isinstance(node, dict)} == expected_ids and len(cell["runtime"]) == len(expected_ids), "runtime observation nodes changed")
    return {
        "recall": recall, "qps": metrics["qps"], "p95_nanos": metrics["p95_nanos"],
        "generation": generation, "counters": counters, "timings": timings, "runtime": runtime,
        "catalog_proof_reads": catalog_proof_reads,
    }


def _validate_command(run_dir: Path, result: dict[str, Any], mode: str, topology: str,
                      provenance: dict[str, Any], concurrency_order: str) -> dict[str, str]:
    paths = {name: run_dir / f"bench-{mode}.{name}" for name in ("command.json", "time", "rc")}
    command = json.loads(_read(paths["command.json"], 1 << 20))
    _require(isinstance(command, list) and all(isinstance(part, str) and part for part in command), "benchmark command is invalid")
    _require(command[:4] == ["/usr/bin/time", "-v", "-o", str(paths["time"])], "benchmark time wrapper changed")
    try:
        marker = command.index("system-bench")
    except ValueError as exc:
        raise ContractError("benchmark command lacks system-bench") from exc
    args = command[marker + 1:]
    _require(len(args) % 2 == 0, "benchmark flags are malformed")
    flags = dict(zip(args[::2], args[1::2], strict=True))
    _require(len(flags) * 2 == len(args), "benchmark flags are duplicated")
    _require(flags == {
        "-endpoint": result["endpoint"], "-topology": str(run_dir / "topology.json"),
        "-dataset": provenance["dataset_directory"], "-truth-cache": provenance["truth_directory"],
        "-truth-cache-sha256": provenance["truth_sha256"], "-probes": "2",
        "-concurrency": concurrency_order, "-top-k": "10", "-ef-search": "128",
        "-warmup": "1000", "-out": str(run_dir / f"search-{mode}.json"),
        "-search-mode": mode, "-max-index-age": "1h", "-max-session-age": "2m",
    }, "benchmark command changed")
    timing = _read(paths["time"], 1 << 20).decode("utf-8")
    _require(_read(paths["rc"], 32) == b"0\n" and timing, "benchmark process did not complete")
    _require(timing.splitlines()[0] == f'\tCommand being timed: "{" ".join(command[4:])}"', "benchmark process command attestation changed")
    if topology == "single":
        _require(command[4] == provenance["binary_path"], "single benchmark client changed")
    elif topology == "native":
        _require(command[4:9] == ["taskset", "--cpu-list", "0-2", "env", "GOMAXPROCS=3"] and command[9] == provenance["binary_path"], "native benchmark ownership changed")
    else:
        _require(command[4:10] == ["docker", "exec", "-e", "GOMAXPROCS=3", f"gomap4093-r{run_dir.name.removeprefix('repeat-')}-group-a", "/treedb_vector_partition_bench"], "container benchmark client changed")
    return {key: _sha256(path) for key, path in paths.items()}


def _validate_result(path: Path, topology: str, mode: str, provenance: dict[str, Any], expected_nodes: dict[str, dict[str, Any]]) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    result = _load(path, 32 << 20)
    _require(result.get("schema_version") == 1 and result.get("result_kind") == "vector_partition_system_bench_v1", "benchmark result identity changed")
    _require(result.get("topology") == TOPOLOGY_NAMES[topology] and _is_sha256(result.get("topology_identity_sha256")), "benchmark topology identity is invalid")
    _require(result.get("dataset_checksum") == provenance["fixture_checksum"] and result.get("truth_artifact_sha256") == provenance["truth_sha256"], "benchmark fixture or truth changed")
    _require(result.get("top_k") == 10 and result.get("ef_search") == 128 and result.get("warmup_queries") == 1000 and result.get("search_mode") == mode, "benchmark configuration changed")
    expected_ages = {
        "strict": (None, None), "fast": (3_600_000_000_000, None),
        "pinned": (3_600_000_000_000, 120_000_000_000),
    }
    _require((result.get("max_index_age_nanos"), result.get("max_session_age_nanos")) == expected_ages[mode], "fast-read age bounds changed")
    _require(_time(result.get("started_at")) < _time(result.get("completed_at")), "benchmark interval is invalid")
    cells = result.get("cells")
    _require(isinstance(cells, list) and len(cells) == 2, "benchmark must retain c1 and c32")
    by_concurrency = {cell.get("concurrency"): cell for cell in cells if isinstance(cell, dict)}
    _require(set(by_concurrency) == {1, 32}, "benchmark c1/c32 set changed")
    return result, {concurrency: _validate_cell(by_concurrency[concurrency], result, mode, concurrency, expected_nodes) for concurrency in (1, 32)}


def _mode_order(topology: str, repetition: int) -> tuple[str, ...]:
    shift = (TOPOLOGIES.index(topology) + repetition - 1) % len(MODES)
    return MODES[shift:] + MODES[:shift]


def _concurrency_order(topology: str, repetition: int, mode: str) -> str:
    return "1,32" if (TOPOLOGIES.index(topology) + MODES.index(mode) + repetition) % 2 else "32,1"


def _median_row(cells: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "recall_min": min(cell["recall"] for cell in cells),
        "recall_median": median(cell["recall"] for cell in cells),
        "qps_min": min(cell["qps"] for cell in cells),
        "qps_median": median(cell["qps"] for cell in cells),
        "qps_max": max(cell["qps"] for cell in cells),
        "p95_nanos_min": min(cell["p95_nanos"] for cell in cells),
        "p95_nanos_median": median(cell["p95_nanos"] for cell in cells),
        "p95_nanos_max": max(cell["p95_nanos"] for cell in cells),
        "catalog_proof_reads": cells[0]["catalog_proof_reads"],
        "counter_median": {key: median(cell["counters"][key] for cell in cells) for key in COUNTERS},
        "timing_nanos_per_query_median": {key: median(cell["timings"][key] / 1000 for cell in cells) for key in TIMINGS},
        "runtime_per_query_median": {key: median(cell["runtime"][key] / 1000 for cell in cells) for key in RUNTIME_DELTAS},
        "peak_rss_bytes_median": median(cell["runtime"]["peak_rss_bytes"] for cell in cells),
    }


def _tail_explained(native: dict[str, Any], container: dict[str, Any], p95_ratio: float,
                    mean_ratio: float) -> bool:
    return (not .90 <= p95_ratio <= 1.10 and .90 <= mean_ratio <= 1.10 and
            native["p95_nanos_min"] <= container["p95_nanos_max"] and
            container["p95_nanos_min"] <= native["p95_nanos_max"])


def summarize(root: Path) -> dict[str, Any]:
    provenance = _load(root / "provenance.json", 1 << 20)
    _require(provenance.get("source_head") and provenance.get("vcs_modified") is False and _is_sha256(provenance.get("binary_sha256")), "execution provenance is invalid")
    _validate_capability_key(provenance)
    _require(_sha256(Path(provenance["binary_path"])) == provenance["binary_sha256"], "benchmark binary changed")
    _require(_sha256(Path(provenance["dataset_directory"]) / "fixture_manifest.json") == provenance["fixture_manifest_sha256"], "fixture manifest changed")
    truth_files = list(Path(provenance["truth_directory"]).glob("*.json"))
    _require(len(truth_files) == 1 and _sha256(truth_files[0]) == provenance["truth_sha256"], "truth artifact changed")
    _validate_m3(root, provenance)
    cells: dict[tuple[str, int, str, int], dict[str, Any]] = {}
    inputs: list[dict[str, Any]] = []
    topology_ids: set[str] = set()
    database_roots: list[Path] = []
    previous_completed: datetime | None = None
    logical_reference: dict[int, dict[str, int]] = {}
    generations: set[str] = set()
    for sequence, (topology, repetition) in enumerate(SEQUENCE, 1):
        run_dir = root / "runs" / topology / f"repeat-{repetition}"
        runner = _load(run_dir / "runner.json", 1 << 20)
        _require(runner.get("schema_version") == 1 and runner.get("result_kind") == "vector_partition_revalidation_run_v1" and runner.get("sequence") == sequence and runner.get("topology") == topology and runner.get("repetition") == repetition, "runner schedule changed")
        expected_modes = _mode_order(topology, repetition)
        _require(tuple(runner.get("mode_order", ())) == expected_modes and runner.get("concurrency_order") == {mode: _concurrency_order(topology, repetition, mode) for mode in MODES}, "runner order changed")
        started, completed = _time(runner.get("started_at")), _time(runner.get("completed_at"))
        _require(started < completed and (previous_completed is None or previous_completed < started), "runner intervals overlap")
        previous_completed = completed
        topology_path = run_dir / "topology.json"
        topology_value = _load(topology_path, 1 << 20)
        topology_name = TOPOLOGY_NAMES[topology]
        computed, roots = _topology_identity(topology_value, topology_name, 1 if topology == "single" else 4, PUBLIC_ROUTE)
        _require(topology_value.get("topology_identity_sha256") == computed and computed not in topology_ids, "topology identity changed or repeated")
        topology_ids.add(computed)
        canonical_roots = [Path(value) for value in roots]
        _require(all(not _paths_overlap(left, right) for index, left in enumerate(canonical_roots) for right in canonical_roots[index + 1:]), "topology database roots overlap")
        _require(all(not _paths_overlap(root_path, prior) for root_path in canonical_roots for prior in database_roots), "repetition database roots overlap")
        database_roots.extend(canonical_roots)
        ready = {node["node_id"]: _ready_identity(topology_path, topology_value, node, provenance["source_head"], provenance["binary_sha256"], PUBLIC_ROUTE) for node in topology_value["nodes"]}
        expected_nodes = {node["node_config_sha256"]: node["runtime_ownership"] for node in topology_value["nodes"]}
        mode_inputs: dict[str, Any] = {}
        prior_search_completed: datetime | None = None
        for mode in expected_modes:
            search_path = run_dir / f"search-{mode}.json"
            result, result_cells = _validate_result(search_path, topology, mode, provenance, expected_nodes)
            _validate_public_endpoint(topology_value, result)
            _require(result["topology_identity_sha256"] == computed and started <= _time(result["started_at"]) < _time(result["completed_at"]) <= completed, "benchmark escaped its runner interval")
            search_started, search_completed = _time(result["started_at"]), _time(result["completed_at"])
            _require(prior_search_completed is None or prior_search_completed < search_started, "benchmark mode intervals overlap")
            prior_search_completed = search_completed
            command = _validate_command(
                run_dir, result, mode, topology, provenance,
                _concurrency_order(topology, repetition, mode),
            )
            for concurrency, cell in result_cells.items():
                key = (topology, repetition, mode, concurrency)
                cells[key] = cell
                generations.add(json.dumps(cell["generation"], sort_keys=True))
                # request_bytes includes topology-specific wire identity and is itself topology tax.
                logical = {name: cell["counters"][name] for name in SEMANTIC_COUNTERS}
                if concurrency not in logical_reference:
                    logical_reference[concurrency] = logical
                _require(logical == logical_reference[concurrency], "logical search work changed across topology or mode")
            mode_inputs[mode] = {"path": str(search_path), "sha256": _sha256(search_path), "process": command}
        container_resources_sha256: str | None = None
        if topology == "container":
            resources_path = run_dir / "container-resources.json"
            resources = _load(resources_path, 1 << 20)
            hosts = resources.get("nodes")
            _require(resources.get("image_id") == provenance["container_image_id"] and isinstance(hosts, list) and len(hosts) == 4, "container image or node count changed")
            _require(all(host.get("CpusetCpus") == f"{index * 3}-{index * 3 + 2}" and host.get("Memory") == 6 << 30 and host.get("MemorySwap") == 6 << 30 and host.get("PidsLimit") == 768 for index, host in enumerate(hosts)), "container runtime ownership changed")
            container_resources_sha256 = _sha256(resources_path)
        profiles: dict[str, dict[str, str]] = {}
        if repetition == 1:
            for node in topology_value["nodes"]:
                directory = Path(node["profile_directory"])
                _require(directory.parent == Path(node["state_directory"]) and directory.is_dir(), "profile directory is invalid")
                artifacts = {path.name: _sha256(path) for path in directory.iterdir() if path.is_file()}
                _require(set(artifacts) == set(PROFILE_NAMES) and all((directory / name).stat().st_size > 0 for name in PROFILE_NAMES), "profile artifact set is incomplete")
                profiles[node["node_id"]] = artifacts
        retained_input = {
            "sequence": sequence, "topology": topology, "repetition": repetition,
            "runner_sha256": _sha256(run_dir / "runner.json"), "topology_sha256": _sha256(topology_path),
            "topology_identity_sha256": computed, "ready_sha256": ready, "search": mode_inputs,
            "profiles_sha256": profiles,
        }
        if container_resources_sha256 is not None:
            retained_input["container_resources_sha256"] = container_resources_sha256
        inputs.append(retained_input)
    _require(len(generations) == 1, "generation changed across the focused matrix")
    rows: list[dict[str, Any]] = []
    for mode in MODES:
        for concurrency in (1, 32):
            row: dict[str, Any] = {"search_mode": mode, "concurrency": concurrency, "topologies": {}}
            for topology in TOPOLOGIES:
                summary = _median_row([cells[(topology, repetition, mode, concurrency)] for repetition in (1, 2, 3)])
                _require(summary["qps_median"] >= (404.4 if concurrency == 1 else 1595.3), f"{topology}/{mode}/c{concurrency} misses competitive QPS")
                _require(summary["p95_nanos_median"] <= (5_000_000 if concurrency == 1 else 50_000_000), f"{topology}/{mode}/c{concurrency} misses competitive p95")
                if mode == "strict" and topology in ("single", "container"):
                    old_qps, old_p95 = BASELINE[(topology, concurrency)]
                    _require(summary["qps_median"] >= old_qps * .97 and summary["p95_nanos_median"] <= old_p95 * 1.03, f"{topology}/strict/c{concurrency} regressed its control")
                if concurrency == 1:
                    timing = summary["timing_nanos_per_query_median"]
                    removable = timing["client_encode"] + timing["client_write"] + timing["client_decode"] + timing["public_adapter"] + timing["service_adapter"]
                    summary["removable_onramp_ratio"] = removable / timing["client_total"]
                    _require(summary["removable_onramp_ratio"] <= .10, f"{topology}/{mode}/c1 removable onramp exceeds 10%")
                    _require(all(timing[key] / timing["client_total"] < .05 for key in ("client_encode", "client_write", "client_decode", "public_adapter", "service_adapter")), f"{topology}/{mode}/c1 has a removable 5% stage")
                row["topologies"][topology] = summary
            native, container = row["topologies"]["native"], row["topologies"]["container"]
            row["native_over_container_qps"] = native["qps_median"] / container["qps_median"]
            row["native_over_container_p95"] = native["p95_nanos_median"] / container["p95_nanos_median"]
            row["native_over_container_client_total"] = (
                native["timing_nanos_per_query_median"]["client_total"] /
                container["timing_nanos_per_query_median"]["client_total"]
            )
            tail_explained = _tail_explained(
                native, container, row["native_over_container_p95"],
                row["native_over_container_client_total"],
            )
            row["native_container_tail_explanation"] = (
                "overlapping_repetition_spread_with_mean_within_10_percent" if tail_explained else None
            )
            _require(.90 <= row["native_over_container_qps"] <= 1.10 and
                     (.90 <= row["native_over_container_p95"] <= 1.10 or tail_explained),
                     f"{mode}/c{concurrency} native and container differ without retained attribution")
            rows.append(row)
    return {
        "schema_version": 1, "result_kind": "vector_partition_revalidation_4093_v1", "status": "qualified",
        "execution": {"source_head": provenance["source_head"], "vcs_modified": False, "binary_sha256": provenance["binary_sha256"], "container_image_id": provenance["container_image_id"]},
        "fixture": {"fixture_checksum": provenance["fixture_checksum"], "fixture_manifest_sha256": provenance["fixture_manifest_sha256"], "truth_sha256": provenance["truth_sha256"], "m3_descriptor_sha256": provenance["m3_descriptor_sha256"]},
        "inputs": inputs, "generation": json.loads(next(iter(generations))), "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    result = summarize(args.root.resolve())
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
