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
        _require(_uint(elapsed, positive=True) and elapsed >= max(samples) and elapsed >= (sum(samples) + workers - 1) // workers, "system-bench elapsed time is invalid")
        _require((metrics.get("p50_nanos"), metrics.get("p95_nanos"), metrics.get("p99_nanos")) == (_percentile(samples, 50), _percentile(samples, 95), _percentile(samples, 99)), "system-bench percentiles changed")
        _require(math.isclose(metrics.get("qps", 0), 1_000_000_000_000 / elapsed, rel_tol=1e-12), "system-bench QPS changed")


def summarize(single_paths: list[Path], native_paths: list[Path]) -> dict[str, Any]:
    _require(len(single_paths) == len(native_paths) == 3, "topology-tax baseline requires three repetitions per topology")
    paths = dict(zip(TOPOLOGIES, (single_paths, native_paths)))
    runs: dict[str, list[dict[str, Any]]] = {}
    inputs: list[dict[str, Any]] = []
    for topology, topology_paths in paths.items():
        runs[topology] = []
        for repetition, path in enumerate(topology_paths, 1):
            value = _load(path)
            validate_run(value, topology)
            runs[topology].append(value)
            inputs.append({"topology": topology, "repetition": repetition, "path": str(path), "sha256": _sha256(path), "topology_identity_sha256": value["topology_identity_sha256"]})
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
                row["topologies"][topology] = {
                    "recall_at_10_median": median(cell["metrics"]["recall_at_10"] for cell in cells),
                    "qps_median": median(cell["metrics"]["qps"] for cell in cells),
                    "p95_nanos_median": median(cell["metrics"]["p95_nanos"] for cell in cells),
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
