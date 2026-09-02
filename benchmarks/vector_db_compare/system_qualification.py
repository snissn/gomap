#!/usr/bin/env python3
"""Fail-closed contract checks for the #4019 local-system comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from typing import Any


FROZEN_PLAN_SEMANTIC_SHA256 = "67a2cc62199b77ec0d0a64dfb7bef85b3f1ebbb11d102d7c19ea645e694ab132"


class ContractError(ValueError):
    pass


def _require(ok: bool, message: str) -> None:
    if not ok:
        raise ContractError(message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _reject_json_constant(value: str) -> None:
    raise ContractError(f"non-finite JSON number {value}")


def _load(path: Path, limit: int = 16 << 20) -> dict[str, Any]:
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    _require(len(raw) <= limit, f"{path} exceeds {limit} bytes")
    value = json.loads(raw, parse_constant=_reject_json_constant)
    _require(isinstance(value, dict), f"{path} must contain an object")
    return value


def _ids(values: list[dict[str, Any]], name: str) -> list[str]:
    _require(all(isinstance(value, dict) for value in values), f"{name} entries must be objects")
    ids = [value.get("id") for value in values]
    _require(all(isinstance(value, str) and value for value in ids), f"{name} ids are required")
    _require(len(ids) == len(set(ids)), f"{name} ids must be unique")
    return ids


def _number(value: Any) -> bool:
    return (isinstance(value, int) and not isinstance(value, bool)) or (isinstance(value, float) and math.isfinite(value))


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _budget_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _semantic_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _percentile(values: list[int], percentile: int) -> int:
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * percentile + 99) // 100 - 1)]


def validate_plan(plan: dict[str, Any]) -> None:
    _require(plan.get("schema_version") == 1, "plan schema_version must be 1")
    _require(plan.get("result_kind") == "vector_partition_local_system_qualification_plan_v1", "unexpected plan kind")
    _require(plan.get("status") == "planned_no_measurement", "plan must not claim measurements")
    _require(plan.get("issue") == 4019, "plan must bind issue 4019")

    inputs = plan.get("accepted_inputs")
    _require(isinstance(inputs, dict), "accepted_inputs are required")
    _require(inputs.get("qualification_head_sha") == "eed54bc0b9ec3b705e9170be26ab069bdc9b9771", "accepted qualification head changed")
    _require(inputs.get("campaign_index_sha256") == "c20f11bb38898fd0d5907330bec3df80db29df14e44962a8766502e757849aa2", "accepted campaign changed")
    _require(inputs.get("validator_stdout_sha256") == "b1c2a147fb74725565ef72a111f1bd72549637564956cdf7d69a60f0ef166410", "accepted validator output changed")
    corpora = inputs.get("corpora")
    _require(isinstance(corpora, list) and len(corpora) == 2, "exactly two accepted corpora are required")
    _require(_ids(corpora, "corpus") == ["embedding_mixture_100k", "embedding_mixture_250k"], "unexpected corpus order or identity")
    expected_shapes = [(100000, 1000, 128), (250000, 1000, 128)]
    for corpus, shape in zip(corpora, expected_shapes):
        for key in ("fixture_checksum", "manifest_sha256", "truth_identity", "truth_artifact_sha256", "truth_sha256"):
            _require(_is_sha256(corpus.get(key)), f"corpus {corpus['id']} lacks {key}")
        _require((corpus.get("vectors"), corpus.get("queries"), corpus.get("dimensions")) == shape, f"corpus {corpus['id']} changes shape")
        _require(corpus.get("metric") == "cosine" and corpus.get("top_k") == 10, f"corpus {corpus['id']} changes metric/top-k")

    workload = plan.get("workload")
    _require(isinstance(workload, dict), "workload is required")
    _require(workload.get("serial_system_rows") is True, "system rows must be serial")
    _require(workload.get("repetitions") == 3, "three repetitions are required")
    _require(workload.get("concurrency") == [1, 8, 32, 64], "concurrency contract changed")
    _require(workload.get("important_concurrency") == [1, 8, 32], "important concurrency contract changed")
    _require(workload.get("warmup_queries_per_cell") == 1000, "warmup contract changed")
    _require(workload.get("cache_state") == "fresh_persistent_state_per_repetition; reopen_or_reconnect_before_search; no_os_cache_drop; warm_each_cell_immediately_before_timing", "cache-state contract changed")
    _require(workload.get("durability_state") == "durable_build_then_checkpoint_or_flush_before_reopen_or_reconnect", "durability contract changed")
    _require(workload.get("budget_order_by_repetition") == ["forward", "reverse", "deterministic_rotation"], "budget order contract changed")
    _require(workload.get("deterministic_rotation_offset") == 1, "budget rotation contract changed")
    _require(workload.get("matched_recall_floor") == 0.9, "matched recall floor changed")
    for key in ("tree_db", "milvus", "pgvector"):
        budgets = workload.get(key, {}).get("search_budgets")
        _require(isinstance(budgets, list) and budgets, f"{key} search budgets are required")
        encoded = [_budget_key(value) for value in budgets]
        _require(len(encoded) == len(set(encoded)), f"{key} search budgets must be unique")
    tree = workload["tree_db"]
    _require((tree.get("variant"), tree.get("partitions"), tree.get("groups"), tree.get("router_candidates"), tree.get("ef_search")) == ("graph-overlap-020-v1", 16, 4, 256, 128), "TreeDB accepted configuration changed")
    _require(tree["search_budgets"] == [{"probes": value} for value in (1, 2, 4, 8, 16)] and tree.get("selected_budget") == {"probes": 2}, "TreeDB probe contract changed")
    milvus = workload["milvus"]
    _require((milvus.get("index"), milvus.get("metric"), milvus.get("M"), milvus.get("efConstruction")) == ("HNSW", "COSINE", 16, 128), "Milvus index contract changed")
    _require(milvus["search_budgets"] == [{"ef": value} for value in (16, 32, 64, 128, 256, 512)], "Milvus search budgets changed")
    pgvector = workload["pgvector"]
    _require((pgvector.get("index"), pgvector.get("metric"), pgvector.get("m"), pgvector.get("ef_construction")) == ("hnsw", "vector_cosine_ops", 16, 128), "pgvector index contract changed")
    _require(pgvector["search_budgets"] == [{"ef_search": value} for value in (16, 32, 64, 128, 256, 512)], "pgvector search budgets changed")

    rows = plan.get("rows")
    _require(isinstance(rows, list), "rows are required")
    expected = [
        "treedb_single_daemon",
        "treedb_native_multi_daemon",
        "treedb_container_multi_daemon",
        "milvus_standalone",
        "postgres_pgvector",
    ]
    _require(_ids(rows, "row") == expected, "required system rows changed")
    for row in rows:
        boundary = row.get("boundary")
        _require(isinstance(boundary, dict) and boundary.get("client") and boundary.get("service") and boundary.get("topology"), f"row {row['id']} lacks a client/service/topology boundary")
        _require(isinstance(boundary.get("processes"), int) and boundary["processes"] > 0, f"row {row['id']} lacks process count")
        _require(isinstance(row.get("pinned_identity"), dict) and row["pinned_identity"], f"row {row['id']} lacks pinned identity")
        required = row.get("required_identity_fields")
        _require(isinstance(required, list) and required and len(required) == len(set(required)), f"row {row['id']} identity fields are invalid")
        _require(row.get("search_budget") in ("tree_db", "milvus", "pgvector"), f"row {row['id']} lacks a search budget")

    envelope = plan.get("resource_envelope")
    _require(isinstance(envelope, dict), "resource envelope is required")
    for key in ("logical_cpus", "memory_limit_bytes", "pids_limit", "minimum_free_bytes"):
        _require(isinstance(envelope.get(key), int) and envelope[key] > 0, f"resource envelope lacks {key}")
    _require(envelope.get("swap_limit_bytes") == 0, "swap must be disabled for measured rows")
    artifact = plan.get("artifact_contract")
    _require(isinstance(artifact, dict) and artifact.get("schema_version") == 1 and artifact.get("result_kind") == "vector_partition_local_system_qualification_v1", "artifact contract is invalid")
    for key in ("allowed_status", "required_phases", "required_resources", "required_search_metrics", "required_host_fields", "required_provenance_fields", "required_noise_fields", "tree_db_counters", "tree_db_positive_counters", "tree_db_timings", "tree_db_positive_timings"):
        _require(isinstance(artifact.get(key), list) and artifact[key] and len(artifact[key]) == len(set(artifact[key])), f"artifact contract lacks {key}")
    _require(artifact["allowed_status"] == ["valid", "failed", "unsupported", "incomplete"] and artifact.get("complete_status") == "complete", "artifact status contract changed")
    _require(set(artifact["tree_db_positive_counters"]) < set(artifact["tree_db_counters"]), "positive TreeDB counters must be a proper subset")
    _require(set(artifact["tree_db_positive_timings"]) < set(artifact["tree_db_timings"]), "positive TreeDB timings must be a proper subset")
    _require(_semantic_sha256(plan) == FROZEN_PLAN_SEMANTIC_SHA256, "frozen plan content changed")


def _resolve(value: Any, source_head_sha: str) -> Any:
    if isinstance(value, str):
        return value.replace("<source_head_sha>", source_head_sha)
    if isinstance(value, dict):
        return {key: _resolve(item, source_head_sha) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve(item, source_head_sha) for item in value]
    return value


def _valid_metrics(metrics: Any, required: list[str], queries: int, top_k: int) -> bool:
    if not isinstance(metrics, dict) or any(key not in metrics for key in required):
        return False
    integer_fields = ("queries", "completed_queries", "result_count", "errors", "timeouts", "p50_nanos", "p95_nanos", "p99_nanos")
    if any(not isinstance(metrics[key], int) or isinstance(metrics[key], bool) for key in integer_fields):
        return False
    if not _number(metrics["recall_at_10"]) or not _number(metrics["qps"]):
        return False
    if metrics["queries"] != queries or metrics["completed_queries"] != queries or metrics["result_count"] != queries * top_k:
        return False
    if metrics["errors"] != 0 or metrics["timeouts"] != 0 or not 0 <= metrics["recall_at_10"] <= 1 or metrics["qps"] <= 0:
        return False
    return 0 < metrics["p50_nanos"] <= metrics["p95_nanos"] <= metrics["p99_nanos"]


def _ordered_budgets(plan: dict[str, Any], budget_name: str, repetition: int) -> list[dict[str, Any]]:
    budgets = plan["workload"][budget_name]["search_budgets"]
    if repetition == 1:
        return budgets
    if repetition == 2:
        return list(reversed(budgets))
    offset = plan["workload"]["deterministic_rotation_offset"]
    return budgets[offset:] + budgets[:offset]


def matched_recall_buckets(plan: dict[str, Any], result: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the first per-system budget whose three-run median reaches the floor."""
    buckets: list[dict[str, Any]] = []
    contracts = {row["id"]: row for row in plan["rows"]}
    floor = plan["workload"]["matched_recall_floor"]
    for row in result["rows"]:
        if row["status"] != "valid":
            continue
        contract = contracts[row["id"]]
        configured = plan["workload"][contract["search_budget"]]
        candidates = configured["search_budgets"]
        if "selected_budget" in configured:
            candidates = [configured["selected_budget"]]
        for corpus in row["corpora"]:
            for concurrency in plan["workload"]["important_concurrency"]:
                selected = None
                for budget in candidates:
                    key = _budget_key(budget)
                    metrics = []
                    for repetition in corpus["repetitions"]:
                        cell = next((value for value in repetition["searches"] if _budget_key(value.get("budget")) == key and value.get("concurrency") == concurrency), None)
                        _require(isinstance(cell, dict) and isinstance(cell.get("metrics"), dict), f"row {row['id']} corpus {corpus['id']} lacks metrics for budget {key} at concurrency {concurrency}")
                        metrics.append(cell["metrics"])
                    recall = median(value["recall_at_10"] for value in metrics)
                    if recall >= floor:
                        selected = {
                            "row": row["id"],
                            "corpus": corpus["id"],
                            "concurrency": concurrency,
                            "budget": budget,
                            "recall_at_10_median": recall,
                            "qps_median": median(value["qps"] for value in metrics),
                            "qps_min": min(value["qps"] for value in metrics),
                            "qps_max": max(value["qps"] for value in metrics),
                            "p95_nanos_median": median(value["p95_nanos"] for value in metrics),
                            "p95_nanos_min": min(value["p95_nanos"] for value in metrics),
                            "p95_nanos_max": max(value["p95_nanos"] for value in metrics),
                        }
                        break
                _require(selected is not None, f"row {row['id']} corpus {corpus['id']} has no matched-recall budget at concurrency {concurrency}")
                buckets.append(selected)
    return buckets


def validate_result(plan: dict[str, Any], plan_sha256: str, result: dict[str, Any], *, require_complete: bool = False) -> bool:
    validate_plan(plan)
    _require(result.get("schema_version") == plan["artifact_contract"]["schema_version"], "result schema_version mismatch")
    _require(result.get("result_kind") == plan["artifact_contract"]["result_kind"], "unexpected result kind")
    _require(result.get("plan_sha256") == plan_sha256, "result does not bind the exact plan")
    _require(result.get("resource_envelope") == plan["resource_envelope"], "result changes the resource envelope")
    source_head = result.get("source_head_sha")
    _require(isinstance(source_head, str) and len(source_head) == 40 and all(c in "0123456789abcdef" for c in source_head), "source_head_sha must be lowercase git SHA-1")

    host = result.get("host")
    _require(isinstance(host, dict), "host identity is required")
    numeric_host_fields = ("logical_cpus", "memory_bytes", "storage_free_bytes")
    _require(all(isinstance(host.get(key), str) and host[key] for key in plan["artifact_contract"]["required_host_fields"] if key not in numeric_host_fields), "host identity is incomplete")
    _require(all(isinstance(host.get(key), int) and not isinstance(host[key], bool) and host[key] > 0 for key in numeric_host_fields), "host numeric identity is invalid")
    envelope = plan["resource_envelope"]
    storage_root = Path(host["storage_root"])
    storage_parent = Path(envelope["storage_root"].removesuffix("/<campaign-root>"))
    _require(host["cpu_model"] == envelope["host_cpu_model"] and host["logical_cpus"] == envelope["logical_cpus"], "host CPU does not match the resource envelope")
    _require(host["memory_bytes"] >= envelope["memory_limit_bytes"] and host["storage_free_bytes"] >= envelope["minimum_free_bytes"], "host capacity is below the resource envelope")
    _require(host["storage_filesystem"] == envelope["storage_filesystem"] and storage_root.is_absolute() and storage_root.name != "<campaign-root>" and ".." not in storage_root.parts and storage_root.parent == storage_parent, "host storage does not match the resource envelope")
    provenance = result.get("provenance")
    _require(isinstance(provenance, dict) and all(isinstance(provenance.get(key), str) and provenance[key] for key in plan["artifact_contract"]["required_provenance_fields"]), "result provenance is incomplete")
    _require(_is_sha256(provenance["commands_sha256"]) and _is_sha256(provenance["environment_sha256"]), "result provenance digests are invalid")
    _require(provenance["artifact_root"] == host["storage_root"], "artifact root differs from the pinned storage root")

    expected_corpora = [{key: corpus[key] for key in ("id", "fixture_checksum", "manifest_sha256", "truth_identity", "truth_artifact_sha256", "truth_sha256")} for corpus in plan["accepted_inputs"]["corpora"]]
    corpus_contracts = {corpus["id"]: corpus for corpus in plan["accepted_inputs"]["corpora"]}
    accepted_corpus_identities = {corpus["id"]: corpus for corpus in expected_corpora}
    _require(result.get("corpora") == expected_corpora, "result changes accepted corpus/query/truth identity")

    plan_rows = {row["id"]: row for row in plan["rows"]}
    rows = result.get("rows")
    _require(isinstance(rows, list) and _ids(rows, "result row") == list(plan_rows), "result rows are missing, duplicated, or reordered")
    allowed = set(plan["artifact_contract"]["allowed_status"])
    all_valid = True
    for row in rows:
        contract = plan_rows[row["id"]]
        status = row.get("status")
        _require(status in allowed, f"row {row['id']} has invalid status")
        _require(row.get("boundary") == contract["boundary"], f"row {row['id']} changes client/service/topology boundary")
        identity = row.get("identity")
        _require(isinstance(identity, dict), f"row {row['id']} identity is required")
        for field in contract["required_identity_fields"]:
            _require(field in identity and identity[field] not in (None, ""), f"row {row['id']} lacks identity field {field}")
            if field.endswith("_sha256"):
                _require(_is_sha256(identity[field]), f"row {row['id']} has invalid identity digest {field}")
        for field, value in _resolve(contract["pinned_identity"], source_head).items():
            _require(identity.get(field) == value, f"row {row['id']} changes pinned identity {field}")
        if status != "valid":
            _require(isinstance(row.get("reason"), str) and row["reason"], f"row {row['id']} non-valid status needs a reason")
            all_valid = False
            continue

        corpus_runs = row.get("corpora")
        _require(isinstance(corpus_runs, list) and [value.get("id") for value in corpus_runs] == [value["id"] for value in expected_corpora], f"row {row['id']} lacks both corpora")
        budgets = plan["workload"][contract["search_budget"]]["search_budgets"]
        expected_cells = {(_budget_key(budget), concurrency) for budget in budgets for concurrency in plan["workload"]["concurrency"]}
        for corpus in corpus_runs:
            tree_generation = None
            repetitions = corpus.get("repetitions")
            _require(isinstance(repetitions, list) and [value.get("repetition") for value in repetitions] == [1, 2, 3], f"row {row['id']} corpus {corpus['id']} lacks three repetitions")
            for repetition in repetitions:
                noise = repetition.get("noise")
                _require(repetition.get("status") == "valid" and isinstance(noise, dict) and noise.get("valid") is True, f"row {row['id']} has invalid or tainted repetition")
                _require(repetition.get("input_identity") == accepted_corpus_identities[corpus["id"]], f"row {row['id']} repetition does not bind its accepted corpus")
                _require(all(_number(noise.get(key)) and noise[key] >= 0 for key in plan["artifact_contract"]["required_noise_fields"] if key != "valid"), f"row {row['id']} lacks host-load observations")
                _require(repetition.get("warmup_queries_per_cell") == plan["workload"]["warmup_queries_per_cell"], f"row {row['id']} changes warmup count")
                ordered_budgets = _ordered_budgets(plan, contract["search_budget"], repetition["repetition"])
                _require(repetition.get("budget_order") == ordered_budgets, f"row {row['id']} changes the search-budget order")
                _require(all(isinstance(repetition.get("phases", {}).get(key), int) and not isinstance(repetition["phases"][key], bool) and repetition["phases"][key] >= 0 for key in plan["artifact_contract"]["required_phases"]), f"row {row['id']} lacks phase timing")
                _require(all(_number(repetition.get("resources", {}).get(key)) and repetition["resources"][key] >= 0 for key in plan["artifact_contract"]["required_resources"]), f"row {row['id']} lacks total-system resources")
                _require(all(isinstance(repetition["resources"][key], int) and not isinstance(repetition["resources"][key], bool) for key in plan["artifact_contract"]["required_resources"] if key != "cpu_seconds"), f"row {row['id']} has malformed byte resources")
                _require(repetition["resources"]["swap_bytes"] <= envelope["swap_limit_bytes"], f"row {row['id']} exceeds the swap limit")
                _require(all(repetition["resources"][key] > 0 for key in ("cpu_seconds", "peak_rss_bytes", "persistent_bytes")), f"row {row['id']} has empty total-system resource observations")
                if row["id"] == "treedb_container_multi_daemon":
                    expected_allocations = [{"cpuset_cpus": cpus, "memory_bytes": 6 * 1024**3, "memory_swap_bytes": 6 * 1024**3, "pids_limit": 768} for cpus in ("0-2", "3-5", "6-8", "9-11")]
                    _require(repetition["resources"].get("container_allocations") == expected_allocations, "container row changes its per-daemon resource allocation")
                cells = repetition.get("searches")
                _require(isinstance(cells, list), f"row {row['id']} searches are required")
                actual_cells = {(_budget_key(value.get("budget")), value.get("concurrency")) for value in cells}
                _require(len(cells) == len(actual_cells) and actual_cells == expected_cells, f"row {row['id']} search matrix is incomplete")
                expected_order = [(_budget_key(budget), concurrency) for budget in ordered_budgets for concurrency in plan["workload"]["concurrency"]]
                _require([(_budget_key(value.get("budget")), value.get("concurrency")) for value in cells] == expected_order, f"row {row['id']} search matrix order changed")
                for cell in cells:
                    corpus_contract = corpus_contracts[corpus["id"]]
                    _require(cell.get("status") == "valid" and _valid_metrics(cell.get("metrics"), plan["artifact_contract"]["required_search_metrics"], corpus_contract["queries"], corpus_contract["top_k"]), f"row {row['id']} has invalid search metrics")
                    if contract["system"] == "treedb":
                        counters = cell.get("counters")
                        _require(isinstance(counters, dict) and all(isinstance(counters.get(key), int) and not isinstance(counters[key], bool) and counters[key] >= 0 for key in plan["artifact_contract"]["tree_db_counters"]), f"row {row['id']} lacks TreeDB path counters")
                        _require(all(counters[key] > 0 for key in plan["artifact_contract"]["tree_db_positive_counters"]), f"row {row['id']} lacks nonzero TreeDB path proof")
                        timings = cell.get("timings")
                        _require(isinstance(timings, dict) and all(isinstance(timings.get(key), int) and not isinstance(timings[key], bool) and timings[key] >= 0 for key in plan["artifact_contract"]["tree_db_timings"]), f"row {row['id']} lacks TreeDB timing attribution")
                        _require(all(timings[key] > 0 for key in plan["artifact_contract"]["tree_db_positive_timings"]), f"row {row['id']} lacks nonzero TreeDB timing attribution")
                        generation = cell.get("generation")
                        _require(isinstance(generation, dict) and isinstance(generation.get("Index"), str) and generation["Index"] and isinstance(generation.get("Generation"), int) and not isinstance(generation["Generation"], bool) and generation["Generation"] > 0, f"row {row['id']} lacks TreeDB generation identity")
                        if tree_generation is None:
                            tree_generation = generation
                        _require(generation == tree_generation, f"row {row['id']} changes TreeDB generation identity")
                        samples = cell.get("total_nanos")
                        elapsed = cell.get("elapsed_nanos")
                        metrics = cell["metrics"]
                        _require(isinstance(samples, list) and len(samples) == metrics["queries"] and all(isinstance(value, int) and not isinstance(value, bool) and value > 0 for value in samples), f"row {row['id']} lacks TreeDB raw timing samples")
                        _require(isinstance(elapsed, int) and not isinstance(elapsed, bool) and elapsed >= max(samples) and elapsed >= (sum(samples) + cell["concurrency"] - 1) // cell["concurrency"], f"row {row['id']} has invalid TreeDB wall elapsed")
                        _require((metrics["p50_nanos"], metrics["p95_nanos"], metrics["p99_nanos"]) == (_percentile(samples, 50), _percentile(samples, 95), _percentile(samples, 99)), f"row {row['id']} changes TreeDB latency percentiles")
                        _require(math.isclose(metrics["qps"], metrics["queries"] * 1_000_000_000 / elapsed, rel_tol=1e-12), f"row {row['id']} changes TreeDB QPS")

    tree_identities = [row["identity"] for row in rows if plan_rows[row["id"]]["system"] == "treedb"]
    _require(len({identity["binary_sha256"] for identity in tree_identities}) == 1, "TreeDB rows do not use one benchmark binary")
    _require(len({row["identity"]["topology_identity_sha256"] for row in rows}) == len(rows), "system topology identities are not distinct")

    expected_status = plan["artifact_contract"]["complete_status"] if all_valid else "incomplete"
    _require(result.get("status") == expected_status, "top-level status does not match row status")
    if all_valid:
        matched_recall_buckets(plan, result)
    if require_complete:
        _require(all_valid, "qualification requires every row to be valid")
    return all_valid


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--result", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    args = parser.parse_args()
    plan = _load(args.plan)
    validate_plan(plan)
    if args.result:
        validate_result(plan, _sha256(args.plan), _load(args.result), require_complete=args.require_complete)


if __name__ == "__main__":
    main()
