#!/usr/bin/env python3
"""Validate and summarize the bounded #4096 strict-proof evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import stat
from statistics import median
from typing import Any

import runtime_ownership as base
from system_qualification import _is_sha256, _require


STRICT_COUNTERS = ("snapshot_pins", "read_proofs", "generation_pins", "partition_opens")
PROOF_STAGES = ("total", "operations_health", "strict_search", "serving_refresh", "coordinator_lifecycle", "shard_lifecycle", "unknown")
PROFILE_NAMES = {"allocs_baseline.pprof", "cpu.pprof", "trace.out", "heap.pprof", "allocs.pprof", "block.pprof", "mutex.pprof"}
ORIGINAL_DISTRIBUTION = base._distribution
REQUEST_BYTES_DELTA_LIMIT = .01


def _proof_projection(cell: dict[str, Any]) -> dict[str, int]:
    total = cell.get("catalog_reads", {}).get("total")
    _require(isinstance(total, dict), "strict catalog proof evidence is missing")
    out: dict[str, int] = {}
    for stage in PROOF_STAGES:
        value = total.get(stage)
        _require(isinstance(value, dict), "strict catalog proof stage is missing")
        for key in base.PROOF_COUNTS:
            _require(base._uint(value.get(key)), "strict catalog proof count is invalid")
            out[f"{stage}.{key}"] = value[key]
        _require(base._uint(value.get("total_nanos")), "strict catalog proof timing is invalid")
        out[f"{stage}.total_nanos"] = value["total_nanos"]
        _require(value["reads"] == value["successes"] == value["verify_leader_calls"] == value["no_log_proofs"], "strict catalog proof counts disagree")
        _require(value["failures"] == value["log_barriers"] == 0, "strict catalog proof failed or appended a log entry")
    for key in (*base.PROOF_COUNTS, "total_nanos"):
        _require(out[f"total.{key}"] == sum(out[f"{stage}.{key}"] for stage in PROOF_STAGES[1:]), "strict catalog proof totals do not match attributed stages")
    _require(all(out[f"{stage}.reads"] == 0 for stage in ("operations_health", "coordinator_lifecycle", "shard_lifecycle", "unknown")), "strict search retained duplicate catalog proofs")
    _require(out["strict_search.reads"] == 1000 and out["total.reads"] == 1000 + out["serving_refresh.reads"], "strict search must retain one ingress proof per query and account for background refresh")
    return out


def _proof_parity(cell: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    return tuple((key, value) for key, value in cell["_proof_projection"].items() if not key.startswith(("total.", "serving_refresh.")) and not key.endswith(".total_nanos"))


def _distribution(cells: list[dict[str, Any]]) -> dict[str, Any]:
    result = ORIGINAL_DISTRIBUTION(cells)
    result["catalog_work_nanos_per_query_median"] = median(cell["_proof_projection"]["strict_search.total_nanos"] / 1000 for cell in cells)
    result["serving_refresh_reads_median"] = median(cell["_proof_projection"]["serving_refresh.reads"] for cell in cells)
    result["serving_refresh_nanos_median"] = median(cell["_proof_projection"]["serving_refresh.total_nanos"] for cell in cells)
    result["request_work_median"] = {key: median(cell["counters"][key] for cell in cells) for key in STRICT_COUNTERS}
    result["request_bytes_median"] = median(cell["counters"]["request_bytes"] for cell in cells)
    return result


def _validate_request_bytes(distributions: dict[str, dict[str, Any]]) -> dict[str, int | float]:
    values = {topology: value["request_bytes_median"] for topology, value in distributions.items()}
    _require(values["native"] == values["container"] and max(values.values()) <= min(values.values()) * (1 + REQUEST_BYTES_DELTA_LIMIT), "strict capability request bytes changed beyond the topology-bound allowance")
    return values


def _profiles(root: Path, input_row: dict[str, Any], node_count: int) -> dict[str, str]:
    run = root / Path(input_row["search_path"]).parent
    paths = sorted(run.glob("state-*/profiles/*"))
    _require(len(paths) == node_count * len(PROFILE_NAMES), "strict profile set is incomplete")
    for directory in {path.parent for path in paths}:
        _require({path.name for path in directory.iterdir()} == PROFILE_NAMES, "strict profile directory changed")
    _require(all(path.is_file() and path.stat().st_size > 0 for path in paths), "strict profile artifact is empty")
    return {str(path.relative_to(root)): base._sha256(path) for path in paths}


def summarize(root: Path) -> dict[str, Any]:
    base.SEMANTIC_COUNTERS = tuple(dict.fromkeys(base.SEMANTIC_COUNTERS + STRICT_COUNTERS))
    base.PARITY_COUNTERS = tuple(key for key in base.SEMANTIC_COUNTERS if key != "request_bytes")
    base.PROOF_STAGES = PROOF_STAGES
    base._proof_projection = _proof_projection
    base._proof_parity = _proof_parity
    base._distribution = _distribution
    result = base.summarize(root)

    provenance = base._load(root / "provenance.json")
    key = Path(str(provenance.get("capability_key_path", ""))).resolve(strict=True)
    mode = key.stat()
    _require(_is_sha256(provenance.get("capability_key_sha256")) and key == (root / "capability.key").resolve() and stat.S_ISREG(mode.st_mode) and stat.S_IMODE(mode.st_mode) == 0o600 and mode.st_size == 32 and base._sha256(key) == provenance["capability_key_sha256"], "strict capability key identity changed")

    repo = Path(__file__).resolve().parents[2]
    onramp_path = repo / "TreeDB/docs/evidence/vector-partition-onramp-profile-4090/6c83bd1e/vector-onramp-profile.json"
    ownership_path = repo / "TreeDB/docs/evidence/vector-partition-runtime-ownership-4091/678d54b3/comparison.json"
    onramp, ownership = base._load(onramp_path), base._load(ownership_path)
    onramp_rows = {row["concurrency"]: row for row in onramp["rows"]}
    ownership_rows = {row["concurrency"]: row for row in ownership["rows"]}
    onramp_names = {"single": "single", "native": "native-budgeted", "container": "container"}
    comparisons = []
    for row in result["rows"]:
        concurrency = row["concurrency"]
        values: dict[str, Any] = {}
        for topology, current in row["topologies"].items():
            old_profile = onramp_rows[concurrency]["controls"][onramp_names[topology]]
            old_runtime = ownership_rows[concurrency]["topologies"][topology]
            lifecycle_reduction = 1 - current["catalog_work_nanos_per_query_median"] / old_profile["catalog_median"]["work_nanos_per_query"]["total"]
            qps_ratio = current["qps_median"] / old_runtime["qps_median"]
            p95_ratio = current["p95_nanos_median"] / old_runtime["p95_nanos_median"]
            _require(lifecycle_reduction >= .90, "strict catalog lifecycle work did not fall by at least 90 percent")
            _require(qps_ratio >= 1.05 and p95_ratio <= 1.05, "strict search did not materially improve QPS without p95 regression")
            _require(current["request_work_median"] == {"snapshot_pins": 1000, "read_proofs": 0, "generation_pins": 0, "partition_opens": 0}, "strict request work changed")
            values[topology] = {"catalog_lifecycle_work_reduction": lifecycle_reduction, "qps_over_4091": qps_ratio, "p95_over_4091": p95_ratio}
        comparisons.append({"concurrency": concurrency, "topologies": values, "request_bytes": _validate_request_bytes(row["topologies"])})

    for input_row in result["inputs"]:
        input_row["profile_sha256"] = _profiles(root, input_row, base.TOPOLOGIES[input_row["topology"]][1])
    result["result_kind"] = "vector_partition_strict_proof_comparison_v1"
    result["execution_identity"]["capability_key_sha256"] = provenance["capability_key_sha256"]
    result["baseline_identity"] = {"onramp_4090_sha256": base._sha256(onramp_path), "runtime_ownership_4091_sha256": base._sha256(ownership_path)}
    result["comparisons"] = comparisons
    result["invariants"].pop("semantic_work_and_service_payload_bytes_identical")
    result["invariants"].update({"semantic_work_identical": True, "strict_capability_request_bytes_topology_delta_limit": REQUEST_BYTES_DELTA_LIMIT, "snapshot_pins_per_cell": 1000, "strict_proofs_per_cell": 1000, "background_serving_refresh_separately_attributed": True, "data_group_proofs_per_cell": 0, "request_generation_pins_per_cell": 0, "request_partition_opens_per_cell": 0, "minimum_qps_improvement_over_4091": .05, "maximum_p95_ratio_over_4091": 1.05, "minimum_catalog_lifecycle_work_reduction_over_4090": .90})
    result["claim_boundary"] = "#4096 keeps ordinary Search strict while propagating one server-authenticated ingress proof over one immutable serving snapshot. It does not add relaxed/pinned APIs, change routing/index/search/topology, or replace the existing bounded framed JSON wire codec."
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(summarize(args.root.resolve(strict=True)), stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
