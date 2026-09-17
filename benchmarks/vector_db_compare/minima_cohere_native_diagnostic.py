#!/usr/bin/env python3
"""Nonqualifying Minima-style public/native lifecycle using existing Cohere FP32 exports.

Separate from the frozen 8D Minima/M5 schema. No download or protected holdout access.
Freeze this reviewed harness before collection; use --freeze, then --run with its SHA256.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import threading
import time

import numpy as np

import minima_treedb_runner as existing
import minima_cohere_v4_production_gate as v4_gate
from treedb_client import _dense_work as dense_contract

SCHEMA = "treedb_minima_cohere_native_diagnostic/v2"
NORMALIZED_CAMPAIGN_SCHEMA = "treedb_cohere_normalized_v4_campaign/v1"
RSS_ARTIFACT_SCHEMA = "treedb_cohere_768_rss_boundary/v3"
QUANTIZED_RSS_ARTIFACT_SCHEMA = "treedb_cohere_768_sq8_rss_boundary/v1"
PAIRED_QUERY_ARTIFACT_SCHEMA = "treedb_cohere_768_sq8_paired_query/v1"
QUANTIZED_PROFILE_NAME = "minima_sq8"
DEFAULT_EF_CONSTRUCTION = 32
RSS_RECALL_TARGET = .90
RSS_CONTROLS = [32, 64, 128, 256, 512, 1024, 2048]
RSS_CALIBRATION_QUERIES = list(range(100))
RSS_REVALIDATION_QUERIES = list(range(100, 200))
RSS_SELECTION_PROTOCOL = "lowest_control_passing_both_fixed_query_sets/v1"
PAIRED_TIMING_REPETITIONS = 5
PAIRED_TIMING_QUERY_COUNT = 20
CAMPAIGN_PROFILE_LEGACY = "legacy_v2_v3"
CAMPAIGN_PROFILE_NORMALIZED_V4 = "normalized_v4"
NORMALIZED_REPRESENTATION = "cosine_normalized_f32_v1"
NORMALIZED_EF_SEARCH = 64
NORMALIZED_RERANK_CANDIDATES = 64
NORMALIZED_FILTER_EXACT_ROWS = 4096
NORMALIZED_FILTER_COLD_EXACT_MAX_ROWS = 5000
GIB = 1 << 30
FROZEN_JSON_MAX_BYTES = 1 << 20
EVIDENCE_JSON_MAX_BYTES = 64 << 20
HARNESS_TREE_PATHS = ("benchmarks/vector_db_compare", "clients/python/treedb_client")
_SQ8_SCORE_PLANE_IDENTITY_FIELDS = {
    "reason", "quantized_index_name", "quantized_codec", "quantized_version", "quantized_config_hash",
}
_COLUMN_GRAPH_BUILD_FIELDS = {
    "total_nanos", "snapshot_nanos", "row_extraction_nanos", "adjacency_build_nanos",
    "locality_remap_nanos", "asset_preparation_nanos", "inv_norm_preparation_nanos",
    "adjacency_state_preparation_nanos", "row_ref_preparation_nanos",
    "document_id_preparation_nanos", "quantized_preparation_nanos",
    "search_pack_preparation_nanos", "manifest_finalization_nanos", "file_sync_nanos",
    "file_sync_count", "namespace_sync_nanos", "namespace_sync_count", "publication_nanos",
    "construction_decisions",
}
_COLUMN_GRAPH_BUILD_POSITIVE_NANOS = {
    "total_nanos", "snapshot_nanos", "row_extraction_nanos", "adjacency_build_nanos",
    "asset_preparation_nanos", "quantized_preparation_nanos", "search_pack_preparation_nanos",
    "manifest_finalization_nanos", "publication_nanos",
}


class QualityUnqualified(RuntimeError):
    """Structurally valid evidence that misses a frozen quality gate."""


def quantized_representation_arm():
    return {
        "schema": "treedb_cohere_quantized_representation/v1",
        "source_vectors": "fp32",
        "query_vectors": "fp32",
        "truth_scores": "canonical_fp32_cosine",
        "query_mode": "quantized_rerank",
        "quantized_index_name": QUANTIZED_PROFILE_NAME,
        "codec": "scalar_u8",
        "version": 1,
        "calibration": "legacy",
        "quantized_config_hash": 0,
        "vector_m": 16,
        "requested_rerank_policy": "R=E_at_each_predeclared_coordinate",
    }


def normalized_v4_representation_arm():
    return {
        "schema": "treedb_cohere_normalized_v4_representation/v1",
        "canonical_representation": NORMALIZED_REPRESENTATION,
        "source_vectors": "fp32_normalized_once_before_WAL",
        "query_vectors": "fp32_normalized_once_before_search",
        "truths": ["canonical_normalized_f32_dot", "original_fp32_cosine"],
        "query_mode": "quantized_rerank",
        "quantized_index_name": QUANTIZED_PROFILE_NAME,
        "codec": "scalar_u8", "version": 1, "calibration": "legacy",
        "quantized_config_hash": 0, "vector_m": 16,
        "fixed_coordinate": {"ef_search": NORMALIZED_EF_SEARCH,
                             "rerank_candidates": NORMALIZED_RERANK_CANDIDATES},
        "filtered_route_policy": {
            "scalar_exact_max_rows": NORMALIZED_FILTER_EXACT_ROWS,
            "cold_exact_max_rows": NORMALIZED_FILTER_COLD_EXACT_MAX_ROWS,
        },
        "native_command_version": 4, "diagnostics_in_timed_lanes": False,
        "return_embedding_in_timed_lanes": False,
    }


def paired_timing_plan(query_count):
    if type(query_count) is not int or query_count <= 0:
        raise ValueError("paired timing requires available queries")
    return {
        "schema": "treedb_cohere_paired_timing_plan/v1",
        "warmup_batches": 1,
        "measured_repetitions": PAIRED_TIMING_REPETITIONS,
        "queries": list(range(min(PAIRED_TIMING_QUERY_COUNT, query_count))),
        "arm_order": "alternate_first_complete_arm_batch_by_repetition",
        "comparison": "native_v2_fp32_vs_native_v3_scalar_u8_rerank_on_one_immutable_code_declared_graph",
    }


def validate_quantized_options(query_mode, index_name, rss_only, rows,
                               sq8_rss_artifact=None, expected_sq8_rss_sha256=None):
    if query_mode == "exact":
        if index_name is not None or sq8_rss_artifact is not None or expected_sq8_rss_sha256 is not None:
            raise ValueError("exact Cohere mode does not accept quantized inputs")
        return
    if query_mode != "quantized_rerank" or index_name != QUANTIZED_PROFILE_NAME:
        raise ValueError("quantized Cohere mode requires the minima_sq8 profile")
    if rss_only and rows != 500000:
        raise ValueError("quantized RSS requires the frozen 500000-row export")
    locked_full = rows == 500000 and not rss_only
    lock_inputs = (sq8_rss_artifact is not None, expected_sq8_rss_sha256 is not None)
    if locked_full != all(lock_inputs) or (not locked_full and any(lock_inputs)):
        raise ValueError("full 500000-row SQ8 diagnostic requires one pinned prior SQ8 RSS artifact")
    if expected_sq8_rss_sha256 is not None and (
            not isinstance(expected_sq8_rss_sha256, str) or len(expected_sq8_rss_sha256) != 64
            or any(character not in "0123456789abcdef" for character in expected_sq8_rss_sha256)):
        raise ValueError("prior SQ8 RSS artifact SHA-256 must be lowercase hexadecimal")


def _coordinate_rows(quality):
    for split in ("calibration", "revalidation"):
        for row in quality[split]["curve"]:
            row.update(ef_search=row["control"], rerank_candidates=row["control"])
    selected = quality["selected_control"]
    quality["selected_coordinate"] = None if selected is None else {
        "ef_search": selected, "rerank_candidates": selected,
    }
    return quality


def fixed_set_quality_valid(quality, controls=RSS_CONTROLS,
                            calibration_queries=RSS_CALIBRATION_QUERIES,
                            revalidation_queries=RSS_REVALIDATION_QUERIES,
                            target=RSS_RECALL_TARGET, *, quantized_coordinates=False):
    """Validate the shared lowest-coordinate/two-fixed-set quality contract."""
    try:
        calibration = quality["calibration"]
        revalidation = quality["revalidation"]
        first, second = calibration["curve"], revalidation["curve"]
        selected = quality["selected_control"]
        split_keys = ({"queries", "curve"}, {"queries", "curve", "passed"})
        row_keys = {"control", "mean_recall_at_10", "mean_ndcg_at_10",
                    "per_query", "per_query_ndcg_at_10"}
        if quantized_coordinates:
            row_keys |= {"ef_search", "rerank_candidates"}
        if (quality["selection_protocol"] != RSS_SELECTION_PROTOCOL
                or not same_json(quality["target_mean_recall_at_10"], target)
                or not isinstance(calibration, dict) or not isinstance(revalidation, dict)
                or set(calibration) != split_keys[0] or set(revalidation) != split_keys[1]
                or not same_json(calibration["queries"], list(calibration_queries))
                or not same_json(revalidation["queries"], list(revalidation_queries))
                or revalidation["passed"] is not True
                or not first or len(first) != len(second)
                or not same_json([row["control"] for row in first], controls[:len(first)])
                or not same_json([row["control"] for row in second], controls[:len(second)])
                or selected != first[-1]["control"]
                or type(selected) is not int
                or (quantized_coordinates and not same_json(quality["selected_coordinate"], {
                        "ef_search": selected, "rerank_candidates": selected,
                    }))):
            return False
        for left, right in zip(first, second):
            control = left["control"]
            if (type(control) is not int or type(right["control"]) is not int
                    or right["control"] != control or set(left) != row_keys or set(right) != row_keys):
                return False
            if quantized_coordinates and (
                    any(type(row[name]) is not int for row in (left, right)
                        for name in ("ef_search", "rerank_candidates"))
                    or left["ef_search"] != control or left["rerank_candidates"] != control
                    or right["ef_search"] != control or right["rerank_candidates"] != control):
                return False
            for row, queries in ((left, calibration_queries), (right, revalidation_queries)):
                recalls = row["per_query"]
                ndcgs = row["per_query_ndcg_at_10"]
                if (type(row["mean_recall_at_10"]) not in (int, float)
                        or type(row["mean_ndcg_at_10"]) not in (int, float)
                        or len(recalls) != len(queries) or len(ndcgs) != len(queries)
                        or not all(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1
                                   for value in recalls + ndcgs)
                        or not math.isclose(row["mean_recall_at_10"], sum(recalls) / len(recalls),
                                            rel_tol=0.0, abs_tol=1e-12)
                        or not math.isclose(row["mean_ndcg_at_10"], sum(ndcgs) / len(ndcgs),
                                            rel_tol=0.0, abs_tol=1e-12)):
                    return False
        if any(left["mean_recall_at_10"] >= target and right["mean_recall_at_10"] >= target
               for left, right in zip(first[:-1], second[:-1])):
            return False
        return first[-1]["mean_recall_at_10"] >= target and second[-1]["mean_recall_at_10"] >= target
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return False


def quantized_quality_valid(quality, controls=RSS_CONTROLS,
                            calibration_queries=RSS_CALIBRATION_QUERIES,
                            revalidation_queries=RSS_REVALIDATION_QUERIES,
                            target=RSS_RECALL_TARGET):
    return fixed_set_quality_valid(
        quality, controls, calibration_queries, revalidation_queries, target,
        quantized_coordinates=True,
    )


def linux_process_identity_valid(identity, expected_pid=None):
    try:
        identity_pid, start = identity.split(":")
        pid = int(identity_pid)
        return (
            identity_pid == str(pid) and pid > 0
            and start == str(int(start)) and int(start) > 0
            and (expected_pid is None or pid == expected_pid)
        )
    except (AttributeError, TypeError, ValueError):
        return False


def process_peak_rss_valid(rss):
    try:
        pid = rss["pid"]
        return (
            rss["availability"] == "measured"
            and type(rss["bytes"]) is int and rss["bytes"] > 0
            and type(pid) is int and pid > 0
            and linux_process_identity_valid(rss["process_identity"], pid)
            and rss["source"] == "/proc/<pid>/status:VmHWM"
            and rss["scope"] == "process_lifetime_through_sample"
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def boundary_storage_valid(storage):
    return (
        isinstance(storage, dict)
        and storage.get("availability") == "measured"
        and type(storage.get("owned_bytes_at_rss_boundary")) is int
        and storage["owned_bytes_at_rss_boundary"] > 0
        and storage.get("scope") == "backend_owned_directory_at_initial_ready_quality_boundary"
    )


def column_graph_build_valid(build, *, expect_quantized_assets):
    if not isinstance(build, dict) or set(build) != _COLUMN_GRAPH_BUILD_FIELDS:
        return False
    numeric = _COLUMN_GRAPH_BUILD_FIELDS - {"construction_decisions"}
    required = _COLUMN_GRAPH_BUILD_POSITIVE_NANOS - {"quantized_preparation_nanos"}
    return (
        all(type(build[field]) is int and build[field] >= 0 for field in numeric)
        and all(build[field] > 0 for field in required)
        and ((build["quantized_preparation_nanos"] > 0)
             if expect_quantized_assets else build["quantized_preparation_nanos"] == 0)
        and build["file_sync_count"] > 0 and build["namespace_sync_count"] > 0
        and (build["construction_decisions"] is None
             or isinstance(build["construction_decisions"], dict))
    )


def sq8_requests_share_initial_owner(requests):
    try:
        snapshots = [row["score_plane"]["snapshot"] for row in requests]
        owner = snapshots[0]
        return (
            all(same_json(snapshot, owner) for snapshot in snapshots)
            and same_json(owner["base_manifest"], owner["current_manifest"])
            and owner["base_coverage_lsn"] == owner["current_coverage_lsn"]
        )
    except (KeyError, TypeError, IndexError):
        return False


def sq8_request_valid(row):
    try:
        if not _SQ8_SCORE_PLANE_IDENTITY_FIELDS <= set(row["score_plane"]):
            return False
        work = dense_contract.DenseSearchWork.from_dict(row["dense_work"])
        proof = dense_contract.DenseScorePlaneProof.from_dict(row["score_plane"])
        results = row["results"]
        ids = [result["id"] for result in results]
        scores = [result["score"] for result in results]
        generation = row["expected_generation"]
        projections_match = True
        for result in results:
            identifier = result["id"]
            if not isinstance(identifier, str) or not identifier.startswith("row-"):
                projections_match = False
                break
            ordinal = int(identifier.removeprefix("row-"))
            projections_match = projections_match and (
                identifier == f"row-{ordinal:06d}" and 0 <= ordinal < 500000
                and result["content"] == f"minima-cohere:{ordinal}"
                and result["meta"] == {
                    "user_id": f"{(ordinal * 7919) % 500000:06d}",
                    "fpath": f"/cohere/{ordinal // 256:06d}.txt",
                }
            )
        return (
            type(row["request_sequence"]) is int and row["request_sequence"] > 0
            and row["outcome"] == "success" and not row.get("error")
            and row["phase"] == "rss_quality"
            and type(row["eligible"]) is int and row["eligible"] == 500000
            and type(row["query"]) is int and 0 <= row["query"] < 200
            and type(row["command_version"]) is int and row["command_version"] == 3
            and type(row["started_monotonic_ns"]) is int
            and type(row["ended_monotonic_ns"]) is int
            and row["ended_monotonic_ns"] > row["started_monotonic_ns"]
            and type(row["duration_ns"]) is int and row["duration_ns"] > 0
            and row["duration_ns"] == row["ended_monotonic_ns"] - row["started_monotonic_ns"]
            and type(generation) is int and generation > 0
            and type(row["requested_ef_search"]) is int
            and row["requested_ef_search"] in RSS_CONTROLS
            and row["requested_ef_search"] >= 10
            and type(row["requested_rerank_candidates"]) is int
            and row["requested_rerank_candidates"] == row["requested_ef_search"]
            and same_json(row.get("lifecycle_state"), {
                "owner_advance": 0, "folded": False, "shadow_allowance": 0,
                "live_base": 500000, "live_suffix": 0,
            })
            and proof.version == 1 and proof.available and proof.completed and not proof.reason
            and proof.requested_mode == "quantized_rerank"
            and proof.effective_mode == "quantized_rerank" and proof.route == "quantized_rerank"
            and proof.quantized_index_name == QUANTIZED_PROFILE_NAME
            and proof.quantized_codec == "scalar_u8" and proof.quantized_version == 1
            and proof.quantized_config_hash == 0 and proof.requested_top_k == 10
            and proof.requested_ef_search == row["requested_ef_search"]
            and proof.requested_rerank_candidates == row["requested_rerank_candidates"]
            and existing.quantized_snapshot_valid(proof.snapshot, generation)
            and work.graph.snapshot == proof.snapshot
            and work.graph.base_candidates == 0 and work.graph.delta_scored == 0
            and work.graph.base_shadowed == 0
            and proof.normalized_candidate_width == row["requested_ef_search"]
            and proof.raw_candidate_width == proof.normalized_candidate_width
            and proof.rerank_candidate_cap == row["requested_rerank_candidates"]
            and proof.raw_retained_candidates <= proof.raw_candidate_width
            and proof.raw_retained_candidates <= proof.quantized_score_calls
            and proof.exact_suffix_score_calls == 0
            and proof.live_shortlist_candidates == min(
                proof.normalized_candidate_width,
                proof.raw_retained_candidates - work.graph.base_shadowed)
            and proof.actual_rerank_candidates == min(
                proof.live_shortlist_candidates, proof.rerank_candidate_cap)
            and proof.actual_rerank_candidates == proof.exact_base_rerank_score_calls
            and work.output.output_bytes > 0
            and dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            and dense_contract.dense_quantized_response_work_matches(
                work, proof, 10, len(results), False)
            and len(results) == 10 and len(ids) == len(set(ids)) and projections_match
            and all(type(score) in (int, float) and math.isfinite(score)
                    and -1.000001 <= score <= 1.000001 for score in scores)
            and type(row["recall"]) in (int, float) and math.isfinite(row["recall"])
            and 0 <= row["recall"] <= 1
            and type(row["ndcg_at_10"]) in (int, float) and math.isfinite(row["ndcg_at_10"])
            and 0 <= row["ndcg_at_10"] <= 1
            and list(zip((-score for score in scores), ids))
            == sorted(zip((-score for score in scores), ids))
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def sq8_effective_index_valid(index, construction, requests):
    try:
        expected_keys = {
            "name", "dimension", "metric", "generation", "contract_version",
            "embedding_field", "vector_index_name", "vector_strategy", "vector_m",
            "vector_ef_construction", "vector_ef_search", "quantized_indexes",
            "scalar_fields", "text_field", "text_index_name", "document_type",
            "capabilities", "typed_input",
        }
        generations = {row["expected_generation"] for row in requests}
        return (
            isinstance(index, dict) and set(index) == expected_keys
            and index["name"] == "minima_cohere"
            and type(index["dimension"]) is int and index["dimension"] == 768
            and index["metric"] == "cosine"
            and index["contract_version"] == existing.SERVICE_CONTRACT
            and index["embedding_field"] == "embedding"
            and index["vector_index_name"] == "embedding"
            and index["vector_strategy"] == "column_graph"
            and type(index["vector_m"]) is int and index["vector_m"] == 16
            and type(index["vector_ef_construction"]) is int
            and index["vector_ef_construction"] == construction["ef_construction"]
            and type(index["vector_ef_search"]) is int and index["vector_ef_search"] == 64
            and index["text_field"] == "content" and index["text_index_name"] == "content"
            and index["document_type"] == "treedb_document_service_v1"
            and index["typed_input"] is True
            and same_json(index["capabilities"], existing.QUANTIZED_INDEX_CAPABILITIES)
            and type(index["generation"]) is int and generations == {index["generation"]}
            and same_json(index["quantized_indexes"], [{
                "name": QUANTIZED_PROFILE_NAME, "codec": "scalar_u8", "version": 1,
            }])
            and same_json(index["scalar_fields"], [
                {"field": "meta.fpath", "index_name": "meta_fpath", "value_type": "string"},
                {"field": "meta.user_id", "index_name": "meta_user_id", "value_type": "string"},
            ])
        )
    except (KeyError, TypeError, ValueError):
        return False


def sq8_results_match_dataset(sq8, vectors, queries, truth):
    """Recompute retained quality and canonical FP32 scores from frozen inputs."""
    try:
        observed = {}
        for record in sq8["observed_execution"]["requests"]:
            query = record["query"]
            expected_ids = truth[query]
            ids = [result["id"] for result in record["results"]]
            recall = len(set(ids) & set(expected_ids)) / len(expected_ids)
            ndcg = binary_ndcg(ids, expected_ids)
            if (not math.isclose(record["recall"], recall, rel_tol=0.0, abs_tol=1e-12)
                    or not math.isclose(record["ndcg_at_10"], ndcg,
                                        rel_tol=0.0, abs_tol=1e-12)):
                return False
            key = (record["requested_ef_search"], query)
            if key in observed:
                return False
            observed[key] = (recall, ndcg)
            query_vector = np.asarray(queries[query], dtype=np.float64)
            query_norm = np.linalg.norm(query_vector)
            if not math.isfinite(query_norm) or query_norm <= 0:
                return False
            scores = []
            for result in record["results"]:
                row = int(result["id"].removeprefix("row-"))
                vector = np.asarray(vectors[row], dtype=np.float64)
                vector_norm = np.linalg.norm(vector)
                if not math.isfinite(vector_norm) or vector_norm <= 0:
                    return False
                score = np.dot(vector, query_vector) / (vector_norm * query_norm)
                if not math.isfinite(score) or abs(score - result["score"]) > 2e-5:
                    return False
                scores.append(score)
            if list(zip((-score for score in scores), ids)) != sorted(
                    zip((-score for score in scores), ids)):
                return False
        for split in ("calibration", "revalidation"):
            quality_split = sq8["quality"][split]
            for coordinate in quality_split["curve"]:
                expected = [observed[(coordinate["control"], query)]
                            for query in quality_split["queries"]]
                if (not same_json(coordinate["per_query"], [row[0] for row in expected])
                        or not same_json(coordinate["per_query_ndcg_at_10"],
                                         [row[1] for row in expected])):
                    return False
        return True
    except (KeyError, TypeError, ValueError, IndexError, ZeroDivisionError):
        return False


def sq8_rss_artifact_reasons(artifact, plan):
    """Validate one intrinsic SQ8 RSS producer against a prospective consumer plan."""
    reasons = []
    quality = artifact.get("quality", {})
    if (artifact.get("schema") != QUANTIZED_RSS_ARTIFACT_SCHEMA
            or artifact.get("backend") != "treedb" or artifact.get("state") != "calibrated"
            or artifact.get("reasons") != []):
        reasons.append("TreeDB SQ8 artifact is not calibrated")
    if (not quantized_quality_valid(quality)
            or quality.get("control_name") != "ef_search" or quality.get("exact_mode") is not False):
        reasons.append("TreeDB SQ8 fixed-set quality missed the target")
    rss = artifact.get("rss", {})
    if not process_peak_rss_valid(rss):
        reasons.append("TreeDB SQ8 server VmHWM is unavailable")
    if not boundary_storage_valid(artifact.get("storage")):
        reasons.append("TreeDB SQ8 initial-ready storage boundary is unavailable")
    expected_contract = (plan.get("comparison_contract") if "comparison_contract" in plan
                         else rss_comparison_contract(plan))
    if (not rss_comparison_contract_valid(artifact.get("comparison_contract"))
            or not same_json(artifact.get("comparison_contract"), expected_contract)):
        reasons.append("TreeDB SQ8 workload/RSS contract differs from the consumer plan")
    if not same_json(artifact.get("representation_arm"), quantized_representation_arm()):
        reasons.append("TreeDB SQ8 representation declaration differs from minima_sq8")
    provenance = artifact.get("provenance", {})
    expected_provenance = plan.get("provenance", plan)
    provenance_keys = (
        "harness_commit", "harness_source_sha256", "harness_trees", "product_commit",
        "product_trees", "service_sha256", "dataset_manifest_sha256",
        "dataset_files_sha256", "serving_sha256",
    )
    if any(not provenance.get(key)
           or not same_json(provenance.get(key), expected_provenance.get(key))
           for key in provenance_keys):
        reasons.append("TreeDB SQ8 provenance differs from the consumer plan")
    construction = artifact.get("construction_calibration_contract", {})
    expected_construction = (plan.get("construction_calibration_contract")
                             if "construction_calibration_contract" in plan
                             else construction_calibration_contract(plan["ef_construction"]))
    if not same_json(construction, expected_construction):
        reasons.append("TreeDB SQ8 construction contract differs from the consumer plan")
    execution = artifact.get("observed_execution", {})
    requests = execution.get("requests", [])
    if (execution.get("schema") != "treedb_cohere_sq8_execution/v1"
            or type(execution.get("native_command_version")) is not int
            or execution.get("native_command_version") != 3
            or not requests or any(not sq8_request_valid(row) for row in requests)
            or not sq8_requests_share_initial_owner(requests)):
        reasons.append("TreeDB SQ8 artifact lacks complete per-call native-v3 R=E proof")
    else:
        try:
            expected = {}
            for split in ("calibration", "revalidation"):
                queries = quality[split]["queries"]
                for coordinate in quality[split]["curve"]:
                    for query, recall, ndcg in zip(
                            queries, coordinate["per_query"],
                            coordinate["per_query_ndcg_at_10"]):
                        expected[(coordinate["control"], query)] = (recall, ndcg)
            observed = {(row["requested_ef_search"], row["query"]):
                        (row["recall"], row["ndcg_at_10"]) for row in requests}
            expected_order = [
                (coordinate["control"], query)
                for coordinate in quality["calibration"]["curve"]
                for query in quality["calibration"]["queries"] + quality["revalidation"]["queries"]
            ]
            matches = (
                [row["request_sequence"] for row in requests]
                == list(range(1, len(requests) + 1))
                and [(row["requested_ef_search"], row["query"]) for row in requests]
                == expected_order
                and len(observed) == len(requests) and set(observed) == set(expected)
                and all(all(math.isclose(left, right, rel_tol=0.0, abs_tol=1e-12)
                            for left, right in zip(observed[key], value))
                        for key, value in expected.items())
            )
        except (KeyError, TypeError, ValueError):
            matches = False
        if not matches:
            reasons.append("TreeDB SQ8 per-call evidence differs from the fixed-grid quality curves")
    readiness = artifact.get("readiness", {})
    if (readiness.get("graph_action") != "build"
            or type(readiness.get("successful_ann_queries")) is not int
            or readiness.get("successful_ann_queries") != len(requests)
            or not column_graph_build_valid(
                readiness.get("column_graph_build"), expect_quantized_assets=True,
            )
            or (((readiness.get("column_graph_build") or {}).get("construction_decisions")
                 is not None) != bool(plan.get("construction_decisions")))
            or not sq8_effective_index_valid(
                readiness.get("effective_index", {}), construction, requests)):
        reasons.append("TreeDB SQ8 effective collection metadata does not bind the request owner")
    return reasons


def sq8_artifact_matches_plan_dataset(artifact, plan):
    dataset = Path(plan["dataset"])
    vectors = np.memmap(dataset / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
    queries = np.memmap(
        dataset / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768),
    )
    truth, _ = strict_json_object(dataset / "truth.json", "frozen truth", EVIDENCE_JSON_MAX_BYTES)
    truth = truth["500000"]
    return sq8_results_match_dataset(artifact, vectors, queries, truth)


def locked_sq8_rss_selection(path, expected_sha256, plan):
    """Validate and bind the prior resource arm that owns the all-row decision."""
    artifact, raw = strict_json_object(path, "prior TreeDB SQ8 RSS artifact", EVIDENCE_JSON_MAX_BYTES)
    if bytes_digest(raw) != expected_sha256:
        raise ValueError("prior TreeDB SQ8 RSS artifact differs from its external SHA-256 pin")
    reasons = sq8_rss_artifact_reasons(artifact, plan)
    if not sq8_artifact_matches_plan_dataset(artifact, plan):
        reasons.append("TreeDB SQ8 results differ from frozen IDs, quality, or canonical FP32 scores")
    if reasons:
        raise ValueError("prior TreeDB SQ8 RSS artifact is not a complete current all-row decision: "
                         + "; ".join(reasons))
    selected = artifact["quality"]["selected_coordinate"]
    return {
        "schema": "treedb_cohere_sq8_coordinate_lock/v1",
        "artifact_path": str(Path(path).resolve()), "artifact_sha256": expected_sha256,
        "artifact_schema": artifact["schema"],
        "source_process_identity": artifact["rss"]["process_identity"],
        "selected_coordinate": dict(selected),
    }


def evaluate_locked_quantized_control(search, truth, selection, calibration_queries,
                                       revalidation_queries, target):
    """Confirm one prior-selected coordinate on a fresh graph without retuning it."""
    control = selection["selected_coordinate"]["ef_search"]

    def metrics(queries):
        recalls, ndcgs = [], []
        for query in queries:
            expected = truth[query]
            actual = search(control, query)
            if len(actual) != len(expected) or len(set(actual)) != len(actual):
                raise RuntimeError("locked SQ8 result cardinality differs from exact truth")
            recalls.append(len(set(actual) & set(expected)) / len(expected))
            ndcgs.append(binary_ndcg(actual, expected))
        return {
            "queries": list(queries),
            "mean_recall_at_10": sum(recalls) / len(recalls),
            "mean_ndcg_at_10": sum(ndcgs) / len(ndcgs),
            "per_query_recall": recalls, "per_query_ndcg_at_10": ndcgs,
        }

    first, second = metrics(calibration_queries), metrics(revalidation_queries)
    passed = first["mean_recall_at_10"] >= target and second["mean_recall_at_10"] >= target
    return {
        "schema": "treedb_cohere_sq8_locked_all_rows_confirmation/v1",
        "selection_protocol": "prior_sq8_rss_coordinate_revalidated_on_fresh_graph/v1",
        "coordinate": {"ef_search": control, "rerank_candidates": control},
        "fixed_sets": {"queries_0_99": first, "queries_100_199": second},
        "target_mean_recall_at_10": target, "passed": passed,
    }


def coordinate_lock_consumption_event(coordinate_lock, fresh_identity):
    source_identity = coordinate_lock.get("source_process_identity")
    if (not linux_process_identity_valid(source_identity)
            or not linux_process_identity_valid(fresh_identity)
            or fresh_identity == source_identity):
        raise RuntimeError("full SQ8 diagnostic did not create a fresh graph owner")
    return {
        "source_artifact_sha256": coordinate_lock["artifact_sha256"],
        "source_process_identity": coordinate_lock["source_process_identity"],
        "fresh_process_identity": fresh_identity,
        "coordinate": coordinate_lock["selected_coordinate"],
    }


def select_quantized_cohorts(search, truth, eligible_counts, rows, controls,
                             calibration_queries, revalidation_queries, target):
    """Select each ANN cohort independently; small filtered cohorts stay exact."""
    quality = {}
    queries = list(calibration_queries) + list(revalidation_queries)
    if not queries or max(queries) >= len(next(iter(truth.values()))):
        raise ValueError("quantized Cohere fixed query sets exceed the frozen oracle")
    for eligible in sorted(eligible_counts, key=lambda value: (value != rows, value)):
        expected = truth[str(eligible)]
        if eligible <= 4096 and eligible != rows:
            control = controls[0]
            per_query = [search(eligible, control, query) == expected[query] for query in queries]
            quality[str(eligible)] = {
                "selection_protocol": "typed_exact_correctness_at_first_predeclared_coordinate/v1",
                "target_mean_recall_at_10": 1.0,
                "selected_control": control if all(per_query) else None,
                "selected_coordinate": ({"ef_search": control, "rerank_candidates": control}
                                        if all(per_query) else None),
                "calibration": {"queries": list(calibration_queries), "curve": []},
                "revalidation": {"queries": list(revalidation_queries), "curve": [],
                                 "passed": all(per_query)},
                "exact_query_matches": per_query,
            }
            continue
        selected = calibrate_ann_control(
            lambda control, query, eligible=eligible: search(eligible, control, query),
            expected, controls, calibration_queries, revalidation_queries, target,
        )
        quality[str(eligible)] = _coordinate_rows(selected)
    return quality


def validate_quantized_response(response, rows, eligible, ef, filter_requested, generation):
    work, proof = getattr(response, "dense_work", None), getattr(response, "score_plane", None)
    try:
        live_eligible = work.graph.filter.eligible_rows if filter_requested else rows
        expected_route = ("typed_empty" if filter_requested and live_eligible == 0 else
                          "typed_exact" if filter_requested
                          and (live_eligible <= 4096 or proof.normalized_candidate_width == 0) else
                          "quantized_rerank")
        exact_calls = (proof.exact_base_rerank_score_calls + proof.exact_small_filter_score_calls
                       + proof.exact_suffix_score_calls)
        route_work_valid = {
            "typed_empty": (
                proof.quantized_score_calls == 0 and exact_calls == 0
                and proof.raw_retained_candidates == 0 and proof.live_shortlist_candidates == 0
                and proof.actual_rerank_candidates == 0 and work.graph.base_candidates == 0
                and work.graph.base_shadowed == 0 and work.output.output_bytes == 0
            ),
            "typed_exact": (
                proof.quantized_score_calls == 0 and proof.exact_base_rerank_score_calls == 0
                and exact_calls == live_eligible and proof.raw_retained_candidates == 0
                and proof.live_shortlist_candidates == 0 and proof.actual_rerank_candidates == 0
                and work.graph.base_candidates == 0
            ),
            "quantized_rerank": (
                proof.quantized_score_calls > 0
                and proof.raw_retained_candidates <= proof.quantized_score_calls
                and work.graph.base_shadowed <= proof.raw_retained_candidates
                and proof.live_shortlist_candidates
                    == min(proof.normalized_candidate_width,
                           proof.raw_retained_candidates - work.graph.base_shadowed)
                and proof.actual_rerank_candidates
                    == min(proof.live_shortlist_candidates, proof.rerank_candidate_cap)
                and proof.actual_rerank_candidates == proof.exact_base_rerank_score_calls
            ),
        }[expected_route]
        valid = (
            type(rows) is int and rows > 0 and type(eligible) is int and 0 <= eligible <= rows
            and type(ef) is int and ef > 0 and type(filter_requested) is bool
            and type(generation) is int and generation > 0
            and response.native_command_version == 3
            and response.index.generation == generation
            and response.index.vector_strategy == "column_graph"
            and response.route == "ann"
            and response.exact_fallbacks == 0
            and response.full_document_scan_fallbacks == 0
            and work is not None and proof is not None
            and work.version == 1 and work.completed
            and work.graph.available and work.graph.completed
            and work.output.attempted and work.output.completed and work.output.missing == 0
            and work.output.requested == len(response.documents)
            and proof.version == 1 and proof.available and proof.completed
            and proof.requested_mode == "quantized_rerank"
            and proof.effective_mode == "quantized_rerank"
            and proof.route == expected_route and not proof.reason
            and proof.quantized_index_name == QUANTIZED_PROFILE_NAME
            and proof.quantized_codec == "scalar_u8" and proof.quantized_version == 1
            and proof.quantized_config_hash == 0
            and proof.requested_top_k == 10
            and proof.requested_ef_search == ef
            and proof.requested_rerank_candidates == ef
            and proof.normalized_candidate_width <= min(rows, max(10, ef))
            and proof.normalized_candidate_width <= proof.raw_candidate_width
            and proof.raw_candidate_width <= rows
            and proof.rerank_candidate_cap == min(proof.normalized_candidate_width, ef)
            and existing.quantized_snapshot_valid(proof.snapshot, generation)
            and work.graph.snapshot == proof.snapshot
            and route_work_valid
            and (not filter_requested or 0 <= live_eligible <= eligible)
            and ((filter_requested
                  and proof.raw_retained_candidates <= work.graph.base_candidates
                  and work.graph.base_candidates <= proof.quantized_score_calls)
                 or (not filter_requested and work.graph.base_candidates == 0))
            and ((len(response.documents) == 0 and work.output.output_bytes == 0)
                 or (len(response.documents) > 0 and work.output.output_bytes > 0))
            and dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            and dense_contract.dense_quantized_response_work_matches(
                work, proof, 10, len(response.documents), filter_requested,
            )
        )
    except (AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise RuntimeError("quantized proof does not bind the Cohere request and current producer owner")


def quantized_upsert_state(state, rows, updated):
    """Apply one authored upsert, advancing its owner only when content changes."""
    try:
        rows = frozenset(rows)
        if (not rows or any(type(row) is not int or row < 0 for row in rows)
                or type(updated) is not bool
                or type(state["owner_advance"]) is not int or state["owner_advance"] < 0
                or type(state["folded"]) is not bool):
            return None
        current_updated = frozenset(state["updated"])
        current_touched = frozenset(state["touched"])
        current_deleted = frozenset(state["deleted"])
        changed = frozenset(
            row for row in rows
            if row in current_deleted or ((row in current_updated) != updated)
        )
        published = bool(changed)
        return {
            "updated": ((current_updated | rows) if updated else (current_updated - rows)),
            "touched": current_touched | changed,
            "deleted": current_deleted - rows,
            "owner_advance": state["owner_advance"] + int(published),
            "folded": state["folded"],
        }
    except (KeyError, TypeError, ValueError):
        return None


def quantized_lifecycle_states(initial_updated, initial_touched, initial_deleted,
                               initial_advance, mutation_windows,
                               request_start_ns, request_end_ns, folded=False):
    """Enumerate whole-batch states that can linearize inside one query interval."""
    if (type(initial_advance) is not int or initial_advance < 0
            or type(request_start_ns) is not int or type(request_end_ns) is not int
            or request_start_ns > request_end_ns or type(folded) is not bool):
        return []
    states = [{
        "updated": frozenset(initial_updated), "touched": frozenset(initial_touched),
        "deleted": frozenset(initial_deleted), "owner_advance": initial_advance,
        "folded": folded,
    }]
    mandatory_prefix = 0
    try:
        ordered_windows = sorted(mutation_windows, key=lambda item: item["started_monotonic_ns"])
    except (KeyError, TypeError, ValueError):
        return []
    for window in ordered_windows:
        try:
            started, ended, success = (window["started_monotonic_ns"],
                                       window.get("ended_monotonic_ns"), window.get("success"))
            rows = frozenset(window["rows"])
            updated, published = window["updated"], window["published"]
            if (type(started) is not int or started < 0
                    or (ended is not None and (type(ended) is not int or ended < started))
                    or (success is not None and type(success) is not bool) or not rows
                    or any(type(row) is not int or row < 0 for row in rows)
                    or type(updated) is not bool or type(published) is not bool
                    or ((ended is None) != (success is None))):
                return []
            if started > request_end_ns:
                break
            if success is False:
                continue
            prior = states[-1]
            current = quantized_upsert_state(prior, rows, updated)
            if current is None or (current["owner_advance"] != prior["owner_advance"]) != published:
                return []
            if published:
                states.append(current)
            if (published and success is True and ended is not None
                    and ended <= request_start_ns):
                mandatory_prefix = len(states) - 1
        except (KeyError, TypeError, ValueError):
            return []
    return states[mandatory_prefix:]


def quantized_snapshot_identity(snapshot):
    try:
        manifest = lambda value: {
            "generation": value.generation, "format": value.format,
            "version": value.version, "checksum": value.checksum,
        }
        return {
            "schema_hash": snapshot.schema_hash,
            "schema_generation": snapshot.schema_generation,
            "base_manifest": manifest(snapshot.base_manifest),
            "current_manifest": manifest(snapshot.current_manifest),
            "base_coverage_lsn": snapshot.base_coverage_lsn,
            "current_coverage_lsn": snapshot.current_coverage_lsn,
        }
    except (AttributeError, TypeError):
        return None


def quantized_snapshot_matches_state(snapshot, initial, state):
    current = quantized_snapshot_identity(snapshot)
    if current is None or initial is None:
        return False
    try:
        generation = initial["current_manifest"]["generation"] + state["owner_advance"]
        coverage = initial["current_coverage_lsn"] + state["owner_advance"]
        valid = (
            generation < 1 << 64 and coverage < 1 << 64
            and current["schema_hash"] == initial["schema_hash"]
            and current["schema_generation"] == initial["schema_generation"]
            and current["current_manifest"]["generation"] == generation
            and current["current_coverage_lsn"] == coverage
        )
        if state["folded"]:
            return valid and current["base_manifest"] == current["current_manifest"] \
                and current["base_coverage_lsn"] == current["current_coverage_lsn"]
        return valid and current["base_manifest"] == initial["base_manifest"] \
            and current["base_coverage_lsn"] == initial["base_coverage_lsn"]
    except (KeyError, TypeError):
        return False


def quantized_state_work(state, rows, eligible, ef, filter_requested):
    """Derive live base/suffix work and every legal producer plan for one state."""
    try:
        if (type(rows) is not int or rows <= 0 or type(eligible) is not int
                or not 0 <= eligible <= rows or type(ef) is not int or ef <= 0
                or type(state["owner_advance"]) is not int or state["owner_advance"] < 0
                or type(state["folded"]) is not bool or type(filter_requested) is not bool):
            return None
        if any(type(ordinal) is not int or not 0 <= ordinal < rows
               for values in (state["updated"], state["touched"], state["deleted"])
               for ordinal in values):
            return None
        touched, deleted = frozenset(state["touched"]), frozenset(state["deleted"])
        if not deleted <= touched:
            return None
        matches = lambda row: not filter_requested or (row * 7919) % rows < eligible
        base_domain = rows if not filter_requested else eligible
        shadowed = sum(1 for row in touched if matches(row))
        live_suffix = sum(1 for row in touched - deleted if matches(row))
        if shadowed > base_domain:
            return None
        live_base = base_domain - shadowed
        live_eligible = live_base + live_suffix

        def plan(domain, shadow_allowance):
            effective = min(domain, max(10, ef))
            return (effective, min(domain, effective + shadow_allowance),
                    min(domain, effective, ef), shadow_allowance, domain)

        if state["folded"]:
            live_base, live_suffix = live_eligible, 0
            plans = (plan(live_eligible, 0),)
        elif filter_requested:
            # A request-local current filter contains B only; a cached immutable-
            # base filter contains B+S and binds S as its shadow allowance.
            plans = tuple(dict.fromkeys((plan(live_base, 0), plan(base_domain, shadowed))))
        else:
            # Unfiltered graph search always uses the immutable base domain.
            plans = (plan(base_domain, shadowed),)
        return live_base, live_suffix, plans
    except (KeyError, TypeError):
        return None


def quantized_state_widths(state, rows, eligible, ef):
    """Compatibility view of the immutable-base plan used by older fixtures."""
    work = quantized_state_work(state, rows, eligible, ef, eligible != rows)
    return None if work is None else work[2][-1][:4]


def quantized_response_state_plans(response, state, rows, eligible, ef, filter_requested):
    work = quantized_state_work(state, rows, eligible, ef, filter_requested)
    if work is None:
        return []
    live_base, live_suffix, plans = work
    live_eligible = live_base + live_suffix
    try:
        proof, graph = response.score_plane, response.dense_work.graph
        if (proof.exact_suffix_score_calls != live_suffix
                or (filter_requested and graph.filter.eligible_rows != live_eligible)):
            return []
        observed = (proof.normalized_candidate_width, proof.raw_candidate_width,
                    proof.rerank_candidate_cap)
        matched = []
        for plan in plans:
            route = ("typed_empty" if filter_requested and live_eligible == 0 else
                     "typed_exact" if filter_requested
                     and (live_eligible <= 4096 or plan[4] == 0) else
                     "quantized_rerank")
            if (proof.route != route or observed != plan[:3]
                    or (route == "typed_exact" and proof.exact_small_filter_score_calls != live_base)
                    or (route != "typed_exact" and proof.exact_small_filter_score_calls != 0)):
                continue
            if ((route == "typed_empty" and graph.base_shadowed == 0)
                    or (route == "typed_exact" and graph.base_shadowed == plan[3])
                    or (route == "quantized_rerank" and graph.base_shadowed <= plan[3])):
                matched.append(plan)
        return matched
    except (AttributeError, KeyError, TypeError, ValueError):
        return []


def quantized_response_matches_state(response, state, rows, eligible, ef, initial_snapshot,
                                     filter_requested=None):
    if filter_requested is None:
        filter_requested = eligible != rows
    plans = quantized_response_state_plans(
        response, state, rows, eligible, ef, filter_requested,
    )
    if not plans:
        return False
    try:
        return (
            quantized_snapshot_matches_state(response.score_plane.snapshot, initial_snapshot, state)
            and quantized_response_projection_matches_state(response.documents, state)
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def quantized_projection_matches_update_states(
        documents, initial_updated, mutation_windows, request_start_ns, request_end_ns):
    """Compatibility helper for focused atomic-projection tests."""
    return any(quantized_response_projection_matches_state(documents, state)
               for state in quantized_lifecycle_states(
                   initial_updated, (), (), 0, mutation_windows, request_start_ns, request_end_ns,
               ))


def quantized_response_projection_matches_state(documents, state):
    try:
        return all(
            document.content == f"minima-cohere:{int(document.id.removeprefix('row-'))}"
            + (":updated" if int(document.id.removeprefix("row-")) in state["updated"] else "")
            and document.id == f"row-{int(document.id.removeprefix('row-')):06d}"
            and int(document.id.removeprefix("row-")) not in state["deleted"]
            for document in documents
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def owned_db_file_inventory(root):
    """Inventory owned files without hashing multi-gigabyte campaign payloads."""
    root = Path(root).resolve()
    if not root.is_dir():
        raise RuntimeError("campaign database directory is unavailable")
    categories = {
        "column_assets": 0, "command_wal": 0, "value_log": 0,
        "leaf_log": 0, "other": 0,
    }
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        size = path.stat().st_size
        if any(marker in "/" + relative for marker in ("/column_assets/", "/column-assets/")):
            category = "column_assets"
        elif "/wal/" in "/" + relative:
            category = "command_wal"
        elif "/value_vlog/" in "/" + relative:
            category = "value_log"
        elif "/leaf_vlog/" in "/" + relative:
            category = "leaf_log"
        else:
            category = "other"
        categories[category] += size
        files.append({"path": relative, "bytes": size, "category": category})
    total = sum(row["bytes"] for row in files)
    if total != sum(categories.values()) or total <= 0:
        raise RuntimeError("campaign database file inventory is inconsistent")
    return {
        "schema": "treedb_owned_db_file_inventory/v1",
        "root": str(root), "total_bytes": total,
        "category_bytes": categories, "files": files,
    }


def linux_process_memory(pid):
    fields = {}
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            name, separator, value = line.partition(":")
            if separator and name in ("VmRSS", "VmHWM", "VmSize", "VmPeak"):
                number, unit = value.split()
                if unit != "kB":
                    raise RuntimeError(f"unexpected /proc memory unit {unit!r}")
                fields[name.lower() + "_bytes"] = int(number) * 1024
    except (FileNotFoundError, PermissionError, ValueError) as exc:
        raise RuntimeError(f"owned process memory is unavailable: {exc}") from exc
    if set(fields) != {"vmrss_bytes", "vmhwm_bytes", "vmsize_bytes", "vmpeak_bytes"}:
        raise RuntimeError("owned process memory endpoint is incomplete")
    return fields


def canonical(value):
    return (json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


def same_json(left, right):
    try:
        return canonical(left) == canonical(right)
    except (TypeError, ValueError):
        return False


def strict_json_object(path, label, maximum_bytes=FROZEN_JSON_MAX_BYTES):
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(maximum_bytes + 1)
        if len(raw) > maximum_bytes:
            raise ValueError(f"{label} exceeds {maximum_bytes} bytes")

        def reject_duplicates(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"{label} contains duplicate key {key!r}")
                result[key] = value
            return result

        def reject_constant(value):
            raise ValueError(f"{label} contains nonfinite {value}")

        def reject_nonfinite_float(value):
            number = float(value)
            if not math.isfinite(number):
                raise ValueError(f"{label} contains nonfinite {value}")
            return number

        value = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicates,
                           parse_constant=reject_constant, parse_float=reject_nonfinite_float)
        if not isinstance(value, dict) or not value:
            raise ValueError(f"{label} must be a nonempty JSON object")
        return value, raw
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid {label}: {exc}") from exc


def bytes_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_frozen_json(path, value, label):
    raw = canonical(value)
    if len(raw) > FROZEN_JSON_MAX_BYTES:
        raise ValueError(f"{label} exceeds {FROZEN_JSON_MAX_BYTES} bytes")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("xb") as stream:
        stream.write(raw)
    return bytes_digest(raw)


def counts(rows):
    if rows == 512:
        return [64, 65, 128, 256, 512]
    return sorted({min(rows, n) for n in (4096, 4097, 5000, 50000, rows)})


def predicate(rows, eligible):
    return None if eligible == rows else {"field": "meta.user_id", "operator": "<", "value": f"{eligible:06d}"}


def normalized_v4_expected_routes(eligible, filter_requested):
    if type(eligible) is not int or eligible < 0 or type(filter_requested) is not bool:
        raise ValueError("normalized-v4 route inputs are invalid")
    if not filter_requested:
        return ("typed_hnsw",)
    if eligible == 0:
        return ("typed_empty",)
    if eligible <= NORMALIZED_FILTER_EXACT_ROWS:
        return ("typed_exact",)
    if eligible <= NORMALIZED_FILTER_COLD_EXACT_MAX_ROWS:
        return ("typed_exact", "typed_hnsw")
    return ("typed_hnsw",)


def make_document(vectors, row, total, updated=False):
    return {"id": f"row-{row:06d}", "content": f"minima-cohere:{row}" + (":updated" if updated else ""),
            "embedding": vectors[row].tolist(),
            "meta": {"user_id": f"{(row * 7919) % total:06d}", "fpath": f"/cohere/{row // 256:06d}.txt"}}


def exact_truth(vectors, queries, eligible_counts):
    """Bounded smoke oracle only; the 500K run consumes the existing exhaustive oracle."""
    normalized = np.asarray(vectors, dtype=np.float64)
    normalized /= np.linalg.norm(normalized, axis=1)[:, None]
    normalized_queries = np.asarray(queries, dtype=np.float64)
    normalized_queries /= np.linalg.norm(normalized_queries, axis=1)[:, None]
    ranks = (np.arange(len(vectors)) * 7919) % len(vectors)
    scores = normalized_queries @ normalized.T
    result = {}
    for eligible in eligible_counts:
        rows = np.flatnonzero(ranks < eligible)
        result[str(eligible)] = [[f"row-{int(row):06d}" for row in rows[np.lexsort((rows, -values[rows]))[:10]]]
                                for values in scores]
    return result


def canonical_normalized_f32_rows(vectors, *, row_batch=8192):
    """Match Go admission bits; accumulate dimensions in order, vectorize rows."""
    source = np.asarray(vectors, dtype=np.float32)
    if source.ndim != 2 or source.shape[1] == 0 or row_batch <= 0:
        raise ValueError("canonical vectors require positive dimensions and batch size")
    normalized = np.empty(source.shape, dtype=np.float32)
    for start in range(0, len(source), row_batch):
        # Float64 intermediates are bounded to this chunk, not another corpus.
        block = np.array(source[start:start + row_batch], dtype=np.float64, order="F")
        if not np.isfinite(block).all():
            raise ValueError("canonical vectors contain non-finite components")
        squared_norm = np.zeros(len(block), dtype=np.float64)
        for component in block.T:
            squared_norm += component * component
        if np.any(squared_norm <= 0) or not np.isfinite(squared_norm).all():
            raise ValueError("canonical vectors contain zero or non-finite norms")
        block *= (1.0 / np.sqrt(squared_norm))[:, None]
        normalized[start:start + len(block)] = block
    return normalized


def canonical_normalized_f32_truth(vectors, queries, eligible_counts, *, query_batch=8):
    """Canonical-f32 dot truth, kept distinct from original-cosine truth."""
    return canonical_normalized_f32_topk(
        canonical_normalized_f32_rows(vectors), canonical_normalized_f32_rows(queries),
        eligible_counts, query_batch=query_batch,
    )


def canonical_normalized_f32_topk(normalized, normalized_queries, eligible_counts, *, query_batch=8):
    """Score already-canonical rows; never normalize stored canonical bytes again."""
    ranks = (np.arange(len(normalized), dtype=np.int64) * 7919) % len(normalized)
    eligible_rows = {
        eligible: np.flatnonzero(ranks < eligible) for eligible in eligible_counts
    }
    result = {str(eligible): [] for eligible in eligible_counts}
    score_result = {str(eligible): [] for eligible in eligible_counts}
    for start in range(0, len(normalized_queries), query_batch):
        scores = normalized_queries[start:start + query_batch] @ normalized.T
        np.clip(scores, -1, 1, out=scores)
        for values in scores:
            for eligible in eligible_counts:
                rows = eligible_rows[eligible]
                if len(rows) <= 10:
                    selected = rows[np.lexsort((rows, -values[rows]))]
                else:
                    eligible_scores = values[rows]
                    cutoff = np.partition(eligible_scores, len(rows) - 10)[len(rows) - 10]
                    better = rows[eligible_scores > cutoff]
                    # rows are ascending ordinals, hence ascending row-%06d IDs.
                    tied = rows[eligible_scores == cutoff][:10 - len(better)]
                    selected = np.concatenate((better, tied))
                    selected = selected[np.lexsort((selected, -values[selected]))]
                result[str(eligible)].append(
                    [f"row-{int(row):06d}" for row in selected]
                )
                score_result[str(eligible)].append(
                    [float(values[row]) for row in selected]
                )
    return result, score_result


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "total_ns": sum(values),
            **{name + "_ns": values[math.ceil(len(values) * fraction) - 1]
               for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def dataset_identity(dataset, rows, *, normalized_v4=False):
    dataset = Path(dataset).resolve()
    manifest, _ = strict_json_object(dataset / "manifest.json", "dataset manifest")
    if (manifest.get("dimensions"), manifest.get("top_k"), manifest.get("exact_train_query_overlap")) != (768, 10, 0):
        raise ValueError("expected the existing real 768D top-10 nonoverlapping diagnostic export")
    supported = (512, 5000, 500000) if normalized_v4 else (512, 500000)
    if rows not in supported or rows > manifest["rows"] or math.gcd(rows, 7919) != 1:
        raise ValueError("unsupported real-prefix Cohere campaign size")
    query_count = 4 if rows == 512 else 200
    if query_count > manifest["query_count"]:
        raise ValueError("not enough exported queries")
    if rows == 500000 and (manifest["rows"], manifest["query_count"]) != (500000, 200):
        raise ValueError("diagnostic oracle requires exactly 500000 exported rows and 200 queries")
    files = {}
    for name, expected_size in (("documents", manifest["rows"] * 768 * 4),
                                ("queries", manifest["query_count"] * 768 * 4), ("truth", None)):
        path = dataset / (name + (".json" if name == "truth" else ".f32"))
        if expected_size is not None and path.stat().st_size != expected_size:
            raise ValueError(f"{name} size does not match dataset manifest")
        files[name] = digest(path)
        if files[name] != manifest[name + "_sha256"]:
            raise ValueError(f"{name} hash does not match dataset manifest")
    return dataset, manifest, files, query_count


def calibrate_ann_control(search, truth, controls, calibration_queries, revalidation_queries, target):
    if (not controls or controls != sorted(set(controls)) or not calibration_queries or not revalidation_queries
            or set(calibration_queries) & set(revalidation_queries) or not 0 < target <= 1):
        raise ValueError("invalid ANN quality calibration contract")

    def metrics(control, queries):
        recalls, ndcgs = [], []
        for query in queries:
            expected = truth[query]
            actual = search(control, query)
            if len(actual) != len(expected) or len(set(actual)) != len(actual):
                raise RuntimeError("ANN result cardinality differs from exact truth")
            recalls.append(len(set(actual) & set(expected)) / len(expected))
            ndcgs.append(binary_ndcg(actual, expected))
        return recalls, ndcgs

    calibration, revalidation, selected = [], [], None
    for control in controls:
        first, first_ndcg = metrics(control, calibration_queries)
        second, second_ndcg = metrics(control, revalidation_queries)
        calibration.append({"control": control, "mean_recall_at_10": sum(first) / len(first),
                            "mean_ndcg_at_10": sum(first_ndcg) / len(first_ndcg),
                            "per_query": first, "per_query_ndcg_at_10": first_ndcg})
        revalidation.append({"control": control, "mean_recall_at_10": sum(second) / len(second),
                             "mean_ndcg_at_10": sum(second_ndcg) / len(second_ndcg),
                             "per_query": second, "per_query_ndcg_at_10": second_ndcg})
        if calibration[-1]["mean_recall_at_10"] >= target and revalidation[-1]["mean_recall_at_10"] >= target:
            selected = control
            break
    return {
        "selection_protocol": RSS_SELECTION_PROTOCOL,
        "target_mean_recall_at_10": target, "selected_control": selected,
        "calibration": {"queries": list(calibration_queries), "curve": calibration},
        "revalidation": {"queries": list(revalidation_queries), "curve": revalidation,
                         "passed": selected is not None},
    }


def binary_ndcg(actual, expected):
    relevant = set(expected)
    ideal = sum(1 / math.log2(rank + 2) for rank in range(len(expected)))
    return sum(1 / math.log2(rank + 2) for rank, item in enumerate(actual) if item in relevant) / ideal


def construction_calibration_contract(ef_construction):
    return {
        "schema": "treedb_column_graph_construction_calibration/v1",
        "ef_construction": ef_construction, "control_ef_construction": 128,
        "max_absolute_recall_loss": .002, "max_absolute_binary_ndcg_loss": .002,
        "max_selected_route_regression": {"qps": .05, "p95": .05, "p99": .05},
    }


def physical_memory_bytes(value):
    """Canonicalize the legacy host-memory probe at the typed contract boundary."""
    if type(value) is int:
        result = value
    elif (isinstance(value, str) and value.isascii() and value.isdecimal()
          and str(int(value)) == value):
        result = int(value)
    else:
        raise ValueError("host physical memory must be a canonical positive integer")
    if result <= 0:
        raise ValueError("host physical memory must be a canonical positive integer")
    return result


def rss_comparison_contract(plan):
    return {
        "schema": "cohere_500k_768d_matched_rss/v3", "rows": plan["rows"],
        "dimensions": plan["dimensions"], "metric": "cosine", "top_k": plan["top_k"],
        "dataset_manifest_sha256": plan["dataset_manifest_sha256"],
        "dataset_files_sha256": plan["dataset_files_sha256"],
        "logical_ids": "row-<six-digit-ordinal>",
        "document": "id, content, FP32 embedding, nested meta.user_id and meta.fpath",
        "scalar_indexes": ["meta.fpath:string", "meta.user_id:string"],
        "query_filter": "none (all 500000 rows eligible)",
        "batch_size": plan["batch_size"], "durability_visibility": "durable_and_visible_before_ack",
        "cpu_affinity": plan["cpu_affinity"],
        "host_memory_bytes": physical_memory_bytes(plan["host_memory_bytes"]),
        "treedb_go_runtime": plan["treedb_go_runtime"],
        "host_resource_identity": plan["host_resource_identity"],
        "platform": plan["platform"], "quality_metric": "mean_recall_at_10",
        "quality_target": plan["rss_recall_target"],
        "quality_selection_protocol": RSS_SELECTION_PROTOCOL,
        "ann_controls": {
            "treedb_ef_search": plan["rss_controls"],
            "qdrant_hnsw_ef": plan["rss_controls"],
        },
        "calibration_queries": plan["rss_calibration_queries"],
        "revalidation_queries": plan["rss_revalidation_queries"],
        "rss_boundary": "fresh_server_and_backend_through_initial_ann_ready_and_quality_gated_query",
        "rss_scope": "server_process_lifetime_VmHWM_including_resident_mappings",
    }


def rss_comparison_contract_valid(contract):
    """Validate the immutable matched-RSS contract without trusting a sibling arm."""
    try:
        sha256_valid = lambda value: (
            isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        )
        expected_keys = {
            "schema", "rows", "dimensions", "metric", "top_k",
            "dataset_manifest_sha256", "dataset_files_sha256", "logical_ids", "document",
            "scalar_indexes", "query_filter", "batch_size", "durability_visibility",
            "cpu_affinity", "host_memory_bytes", "treedb_go_runtime",
            "host_resource_identity", "platform", "quality_metric", "quality_target",
            "quality_selection_protocol", "ann_controls", "calibration_queries",
            "revalidation_queries", "rss_boundary", "rss_scope",
        }
        hashes = contract["dataset_files_sha256"]
        affinity = contract["cpu_affinity"]
        host = contract["host_resource_identity"]
        runtime = contract["treedb_go_runtime"]
        return (
            isinstance(contract, dict) and set(contract) == expected_keys
            and contract["schema"] == "cohere_500k_768d_matched_rss/v3"
            and all(type(contract[name]) is int for name in (
                "rows", "dimensions", "top_k", "batch_size", "host_memory_bytes",
            ))
            and (contract["rows"], contract["dimensions"], contract["top_k"],
                 contract["batch_size"]) == (500000, 768, 10, 256)
            and contract["host_memory_bytes"] > 0
            and contract["metric"] == "cosine"
            and sha256_valid(contract["dataset_manifest_sha256"])
            and isinstance(hashes, dict) and set(hashes) == {"documents", "queries", "truth"}
            and all(sha256_valid(value) for value in hashes.values())
            and same_json(contract["scalar_indexes"], ["meta.fpath:string", "meta.user_id:string"])
            and isinstance(affinity, list) and affinity
            and all(type(cpu) is int and cpu >= 0 for cpu in affinity)
            and affinity == sorted(set(affinity))
            and isinstance(runtime, dict) and set(runtime) == {"GOMAXPROCS", "GOGC", "GOMEMLIMIT"}
            and all(isinstance(value, str) for value in runtime.values())
            and isinstance(host, dict)
            and set(host) == {"machine_id", "boot_id", "page_size_bytes", "numa_mems",
                              "cgroup_membership", "cgroup_limits", "cpu_model",
                              "cpu_features"}
            and all(isinstance(host[name], str) and host[name]
                    for name in ("machine_id", "boot_id", "numa_mems", "cgroup_membership",
                                 "cpu_model"))
            and isinstance(host["cpu_features"], list) and host["cpu_features"]
            and host["cpu_features"] == sorted(set(host["cpu_features"]))
            and all(isinstance(feature, str) and feature for feature in host["cpu_features"])
            and type(host["page_size_bytes"]) is int and host["page_size_bytes"] > 0
            and isinstance(host["cgroup_limits"], dict)
            and all(isinstance(key, str) and isinstance(value, str)
                    for key, value in host["cgroup_limits"].items())
            and isinstance(contract["platform"], str) and contract["platform"]
            and same_json(contract["quality_target"], RSS_RECALL_TARGET)
            and contract["quality_selection_protocol"] == RSS_SELECTION_PROTOCOL
            and same_json(contract["ann_controls"], {
                "treedb_ef_search": RSS_CONTROLS, "qdrant_hnsw_ef": RSS_CONTROLS,
            })
            and same_json(contract["calibration_queries"], RSS_CALIBRATION_QUERIES)
            and same_json(contract["revalidation_queries"], RSS_REVALIDATION_QUERIES)
            and contract["logical_ids"] == "row-<six-digit-ordinal>"
            and contract["document"] == "id, content, FP32 embedding, nested meta.user_id and meta.fpath"
            and contract["query_filter"] == "none (all 500000 rows eligible)"
            and contract["durability_visibility"] == "durable_and_visible_before_ack"
            and contract["quality_metric"] == "mean_recall_at_10"
            and contract["rss_boundary"]
                == "fresh_server_and_backend_through_initial_ann_ready_and_quality_gated_query"
            and contract["rss_scope"]
                == "server_process_lifetime_VmHWM_including_resident_mappings"
        )
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False


def treedb_service_environment(plan):
    child = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
    child.update({key: value for key, value in plan["treedb_go_runtime"].items() if value})
    return child


def python_runtime_identity():
    # Preserve the virtualenv path: following its symlink loses the dependency
    # context that argv and sys.executable actually selected.
    executable = Path(os.sys.executable).absolute()
    return {
        "python": os.sys.version, "numpy": np.__version__, "platform": platform.platform(),
        "python_executable": str(executable), "python_executable_sha256": digest(executable),
    }


def host_resource_identity():
    cgroup = Path("/proc/self/cgroup").read_text().strip()
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
    cpu = {key.strip(): value.strip()
           for line in Path("/proc/cpuinfo").read_text().split("\n\n", 1)[0].splitlines()
           if ":" in line for key, value in [line.split(":", 1)]}
    cpu_model = (cpu.get("model name") or cpu.get("Processor") or "").strip()
    cpu_features = sorted(set((cpu.get("flags") or cpu.get("Features") or "").split()))
    unified = next((line.split("::", 1)[1] for line in cgroup.splitlines() if line.startswith("0::")), None)
    limits = {}
    if unified is not None:
        root, current = Path("/sys/fs/cgroup"), Path("/sys/fs/cgroup") / unified.lstrip("/")
        if not current.resolve().is_relative_to(root):
            raise RuntimeError("cgroup path escaped its mount")
        while True:
            for name in ("cpu.max", "cpuset.cpus.effective", "cpuset.mems.effective",
                         "memory.high", "memory.max", "memory.swap.max"):
                path = current / name
                if path.is_file():
                    limits[f"{current.relative_to(root)}/{name}"] = path.read_text().strip()
            if current == root:
                break
            current = current.parent
    identity = {
        "machine_id": Path("/etc/machine-id").read_text().strip(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "page_size_bytes": os.sysconf("SC_PAGE_SIZE"), "numa_mems": status.get("Mems_allowed_list", "").strip(),
        "cgroup_membership": cgroup, "cgroup_limits": limits,
        "cpu_model": cpu_model, "cpu_features": cpu_features,
    }
    if (not identity["machine_id"] or not identity["boot_id"] or not identity["numa_mems"]
            or not identity["cpu_model"] or not identity["cpu_features"]):
        raise RuntimeError("host resource identity is incomplete")
    return identity


def normalized_v4_effective_resources(plan):
    """Resolve the tightest recorded cgroup-v2 capacity at this process."""
    host_memory = physical_memory_bytes(plan["host_memory_bytes"])
    affinity = plan.get("cpu_affinity")
    limits = (plan.get("host_resource_identity") or {}).get("cgroup_limits")
    if (not isinstance(affinity, list) or not affinity
            or any(type(cpu) is not int or cpu < 0 for cpu in affinity)
            or len(affinity) != len(set(affinity)) or not isinstance(limits, dict)):
        raise ValueError("normalized-v4 effective resource inputs are invalid")
    memory_limits, cpu_quotas = [host_memory], [len(affinity) * 1000]
    saw_memory, saw_cpu = False, False
    for name, raw in limits.items():
        if not isinstance(name, str) or not isinstance(raw, str):
            raise ValueError("normalized-v4 cgroup limits are invalid")
        if name.endswith("memory.max") or name.endswith("memory.high"):
            saw_memory = True
            if raw != "max":
                if not raw.isascii() or not raw.isdecimal() or str(int(raw)) != raw:
                    raise ValueError("normalized-v4 cgroup memory limit is invalid")
                memory_limits.append(int(raw))
        elif name.endswith("cpu.max"):
            saw_cpu = True
            parts = raw.split()
            if len(parts) != 2 or not parts[1].isascii() or not parts[1].isdecimal() \
                    or int(parts[1]) <= 0:
                raise ValueError("normalized-v4 cgroup CPU quota is invalid")
            if parts[0] != "max":
                if not parts[0].isascii() or not parts[0].isdecimal() or int(parts[0]) <= 0:
                    raise ValueError("normalized-v4 cgroup CPU quota is invalid")
                cpu_quotas.append(int(parts[0]) * 1000 // int(parts[1]))
    if not saw_memory or not saw_cpu:
        raise ValueError("normalized-v4 cgroup-v2 capacity is unavailable")
    return {
        "effective_memory_bytes": min(memory_limits),
        "effective_cpu_quota_millis": min(cpu_quotas),
    }


def normalized_v4_infrastructure_receipt(plan):
    """Fail closed before a full qualification arm if its local runner is undersized."""
    full = plan["rows"] == 500000
    required_free = (plan["minimum_free_bytes"] + plan["maximum_output_bytes"]
                     if full else plan["minimum_free_bytes"] + GIB)
    required_memory = plan["maximum_combined_rss_bytes"] if full else GIB
    parent = Path(plan["run_dir"]).parent
    try:
        disk_headroom = parent.is_dir() and shutil.disk_usage(parent).free >= required_free
    except OSError:
        disk_headroom = False
    try:
        host_memory = physical_memory_bytes(plan["host_memory_bytes"]) >= required_memory
    except (TypeError, ValueError):
        host_memory = False
    try:
        effective = normalized_v4_effective_resources(plan)
    except (KeyError, TypeError, ValueError):
        effective = {"effective_memory_bytes": 0, "effective_cpu_quota_millis": 0}
    required_cpu_millis = 1000
    if full:
        gomaxprocs = plan.get("gomaxprocs")
        try:
            required_cpu_millis = (int(gomaxprocs) * 1000
                                   if isinstance(gomaxprocs, str) and gomaxprocs.isdigit()
                                   else None)
        except ValueError:
            required_cpu_millis = None
    checks = {
        "cpu_affinity": bool(plan["cpu_affinity"]),
        "cgroup_cpu_quota": (required_cpu_millis is not None and required_cpu_millis > 0
                             and effective["effective_cpu_quota_millis"] >= required_cpu_millis),
        "cgroup_memory": effective["effective_memory_bytes"] >= required_memory,
        "disk_headroom": disk_headroom,
        "host_memory": host_memory,
        "linux_procfs": all(Path(path).is_file() for path in (
            "/proc/self/status", "/proc/cpuinfo", "/proc/self/cgroup",
        )),
    }
    reasons = [name for name, passed in checks.items() if not passed]
    return {
        "schema": "treedb_normalized_v4_infrastructure/v1",
        "state": "available" if not reasons else "unavailable",
        "runner": "shared_workstation_serialized_quiet_window",
        "dataset_cache": "persistent_local",
        "artifact_storage": "local_owned_directory",
        "requirements": {
            "minimum_free_bytes": required_free,
            "minimum_host_memory_bytes": required_memory,
            "minimum_effective_memory_bytes": required_memory,
            "minimum_cpu_quota_millis": required_cpu_millis,
        },
        "effective_resources": effective,
        "checks": checks,
        "operator_contract": {
            "other_benchmark_load": "excluded",
            "run_policy": "one_arm_at_a_time",
        },
        "reasons": reasons,
    }


def process_peak_at_boundary(pid, expected_identity, expected_affinity):
    sample = existing.common.process_peak_rss(pid)
    identity = existing.common.linux_process_identity(pid)
    try:
        affinity = sorted(os.sched_getaffinity(pid)) if type(pid) is int and pid > 0 else []
    except (OSError, ProcessLookupError):
        affinity = []
    if (sample.get("availability") != "measured" or sample.get("bytes") is None
            or identity != expected_identity or sample.get("process_identity") != expected_identity
            or affinity != expected_affinity):
        return {**sample, "availability": "unavailable", "bytes": None,
                "reason": "server process identity, affinity, or VmHWM changed/unavailable at boundary"}
    return sample


def finalize_rss_artifact(artifact, failure):
    if failure:
        artifact["state"] = "uncalibrated"
        artifact["reasons"].append(f"terminal failure: {failure}")
    return artifact


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    query_mode = getattr(args, "query_mode", "exact")
    quantized_index_name = getattr(args, "quantized_index_name", None)
    rss_only = getattr(args, "rss_only", False)
    campaign_profile = getattr(args, "campaign_profile", CAMPAIGN_PROFILE_LEGACY)
    normalized_v4 = campaign_profile == CAMPAIGN_PROFILE_NORMALIZED_V4
    sq8_rss_artifact = getattr(args, "all_rows_sq8_rss_artifact", None)
    expected_sq8_rss_sha256 = getattr(args, "expected_all_rows_sq8_rss_artifact_sha256", None)
    if normalized_v4:
        if rss_only or sq8_rss_artifact is not None or expected_sq8_rss_sha256 is not None:
            raise ValueError("normalized-v4 campaign does not consume legacy RSS coordinate selection")
        if query_mode == "quantized_rerank" and quantized_index_name != QUANTIZED_PROFILE_NAME:
            raise ValueError("normalized-v4 SQ8 arm requires the minima_sq8 code plane")
        if query_mode == "exact" and quantized_index_name is not None:
            raise ValueError("normalized-v4 FP32-only arm does not declare an SQ8 code plane")
    else:
        validate_quantized_options(
            query_mode, quantized_index_name, rss_only, args.rows,
            sq8_rss_artifact, expected_sq8_rss_sha256,
        )
    harness = producer_harness_commit(source, args.product_commit)
    dataset, manifest, files, query_count = dataset_identity(
        args.dataset, args.rows, normalized_v4=normalized_v4,
    )
    if rss_only and args.rows != 500000:
        raise ValueError("matched RSS mode requires the frozen 500000-row export")
    existing.service_binary_build_provenance(args.service_bin, args.product_commit)
    go_helper = getattr(args, "go_helper", None)
    go_tool = getattr(args, "go", "go")
    if normalized_v4:
        if go_helper is None or not go_helper.resolve().is_file():
            raise ValueError("normalized-v4 campaign requires the pinned Go measurement helper")
        result = subprocess.run(
            [go_tool, "version", "-m", str(go_helper.resolve())],
            text=True, capture_output=True, timeout=30, check=False,
        )
        if result.returncode != 0 or f"vcs.revision={args.product_commit}" not in result.stdout \
                or "vcs.modified=false" not in result.stdout:
            raise ValueError("normalized-v4 Go helper provenance differs from the candidate commit")
    root_tree = lambda path: subprocess.check_output(
        ["git", "rev-parse", harness + ":" + path], cwd=source, text=True,
    ).strip()
    product_tree = lambda path: subprocess.check_output(["git", "rev-parse", args.product_commit + ":" + path], cwd=source, text=True).strip()
    serving, serving_raw = strict_json_object(args.serving, "column_graph serving limits")
    existing.validate_column_graph_serving(serving)
    plan = {"schema": NORMALIZED_CAMPAIGN_SCHEMA if normalized_v4 else SCHEMA,
            "qualification": "not_evaluated", "mode": "smoke" if args.rows < 500000 else "diagnostic",
            "campaign_profile": campaign_profile,
            "harness_commit": harness, "harness_source_sha256": digest(Path(__file__)),
            "harness_trees": {path: root_tree(path) for path in HARNESS_TREE_PATHS},
            "product_commit": args.product_commit, "service_bin": str(args.service_bin.resolve()),
            "product_trees": {path: product_tree(path) for path in ("TreeDB", "cmd/treedb-document-service", "internal", "go.mod", "go.sum")},
            "service_sha256": digest(args.service_bin), "dataset": str(dataset),
            "dataset_manifest_sha256": digest(dataset / "manifest.json"), "dataset_files_sha256": files,
            "serving": serving, "serving_sha256": bytes_digest(serving_raw),
            "serving_path": str(args.serving.resolve()),
            "run_dir": str(args.run_dir.resolve()), "rows": args.rows, "dimensions": 768, "queries": query_count,
            "top_k": 10, "batch_size": 256, "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "rss_only": rss_only, "rss_recall_target": RSS_RECALL_TARGET,
            "ef_construction": args.ef_construction,
            "construction_decisions": args.construction_decisions,
            "construction_calibration_contract": construction_calibration_contract(args.ef_construction),
            "rss_controls": RSS_CONTROLS,
            "rss_calibration_queries": RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": RSS_REVALIDATION_QUERIES,
            "overlap_eligible": counts(args.rows)[1],
            "eligible_counts": counts(args.rows), "reader_concurrency": 4, "writer_calls": 8,
            "scalar_shape": "dispersed unique rank=(row*7919)%rows; user_id range, fpath equality for lifecycle delete",
            "writer_shape": "eight 256-row same-ID replacements after full initial ingestion; fixed live row count",
            "url": args.url, "native_address": args.native_address, "diagnostics_url": args.diagnostics_url,
            "operation_timeout_s": 600, "wall_limit_s": 2700, "minimum_free_bytes": 10 * GIB,
            "maximum_output_bytes": 11 * GIB, "maximum_combined_rss_bytes": 24 * GIB,
            "gomaxprocs": os.environ.get("GOMAXPROCS", ""),
            "treedb_go_runtime": {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")},
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "host_memory_bytes": existing.common.memory_bytes(),
            "host_resource_identity": host_resource_identity(),
            "blas_threads": {key: os.environ.get(key, "") for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")},
            **python_runtime_identity(),
            "query_usage": ("observed calibration 0..99 and observed revalidation 100..199; "
                            "both fixed sets select the control; neither is a holdout" if rss_only
                            else "diagnostic queries, not final holdout"),
            "infrastructure": "INFRASTRUCTURE_UNAVAILABLE: runner: shared workstation, serialized quiet window; persistent cache and local artifact storage"}
    if normalized_v4:
        plan.update(
            representation=NORMALIZED_REPRESENTATION,
            fixed_coordinate={"ef_search": NORMALIZED_EF_SEARCH,
                              "rerank_candidates": NORMALIZED_RERANK_CANDIDATES,
                              "top_k": 10},
            production_timing={"schema": "treedb_cohere_v4_production_timing/v1",
                               "queries": list(range(query_count)),
                               "warmup_queries_per_arm": min(20, query_count),
                               "measured_repetitions": 6,
                               "arm_order": "alternate_first_complete_arm_batch_by_repetition",
                               "diagnostics": False, "return_embedding": False},
            go_helper=str(go_helper.resolve()), go_helper_sha256=digest(go_helper.resolve()),
            go_tool=str(Path(go_tool).resolve()) if Path(go_tool).is_file() else go_tool,
            query_usage=("fixed E=R=64 normalized-v4 production comparison; all queries are retained "
                         "diagnostic evidence, not an unseen holdout"),
        )
        plan["infrastructure"] = normalized_v4_infrastructure_receipt(plan)
        if plan["infrastructure"]["state"] != "available":
            raise RuntimeError(
                "INFRASTRUCTURE_UNAVAILABLE: "
                + ", ".join(plan["infrastructure"]["reasons"])
            )
    if query_mode == "quantized_rerank":
        plan.update(
            query_mode=query_mode,
            quantized_index_name=quantized_index_name,
            vector_m=16,
            representation_arm=(normalized_v4_representation_arm()
                                if normalized_v4 else quantized_representation_arm()),
            quantized_coordinate_policy="ordered_ef_grid_with_requested_rerank_candidates_equal_ef",
            paired_timing=(None if rss_only or normalized_v4 else paired_timing_plan(query_count)),
            query_usage=(("fixed E=R=64 normalized-v4 production comparison; all queries are retained "
                          "diagnostic evidence, not an unseen holdout") if normalized_v4 else
                        ("observed fixed sets 0..99 and 100..199 both select every >4096 cohort; "
                         "the prior RSS-selected all-row coordinate is revalidated once on the fresh graph; "
                         "filtered <=4096 is typed-exact correctness; "
                         "after selection, paired timing reuses frozen observed queries without retuning"
                         if args.rows == 500000 else
                         "four observed queries select the unfiltered 512-row cohort; smaller filtered cohorts are typed-exact; "
                         "paired smoke timing reuses the same four observed queries without retuning")),
        )
        if args.rows == 500000 and not rss_only and not normalized_v4:
            plan["all_rows_coordinate_lock"] = locked_sq8_rss_selection(
                sq8_rss_artifact, expected_sq8_rss_sha256, plan,
            )
    return plan


def validate_plan(plan, expected):
    if canonical(plan) != canonical(expected):
        raise ValueError("frozen plan differs from current source/binary/dataset/configuration/environment")


def validate_imports(source):
    for imported, directory in ((existing, "benchmarks/vector_db_compare"),
                                (existing.common, "benchmarks/vector_db_compare"),
                                (dense_contract, "clients/python/treedb_client/src"),
                                (existing.TreeDBClient, "clients/python/treedb_client/src")):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / directory):
            raise ValueError("imported runner/client is outside the frozen source tree")


def producer_harness_commit(source, candidate_commit):
    """Bind producer imports to one clean committed harness tree."""
    # Keep this check in the top-level producer: an imported runner cannot
    # independently attest that its own bytes match the recorded commit.
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all", "--",
             *HARNESS_TREE_PATHS],
            cwd=source, text=True, capture_output=True, timeout=30, check=False,
        )
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=source, text=True,
            capture_output=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("producer harness source identity is unavailable") from exc
    commit = revision.stdout.strip()
    if status.returncode or status.stdout:
        raise RuntimeError("producer requires clean committed harness source trees")
    if (revision.returncode or len(commit) != 40
            or any(character not in "0123456789abcdef" for character in commit)):
        raise RuntimeError("producer could not bind an exact harness commit")
    if commit != candidate_commit:
        raise RuntimeError("producer harness HEAD differs from the candidate commit")
    return commit


def validate_shutdowns(lifetimes, expected_count=None):
    if expected_count is not None and len(lifetimes) != expected_count:
        raise RuntimeError("owned service lifetime count mismatch")
    for lifetime in lifetimes:
        terminal, exited = lifetime.get("terminal_work", {}), lifetime.get("exit", {})
        if (terminal.get("cleanup_completed") is not True or terminal.get("shutdown_failures") != 0
                or terminal.get("contract_version") != existing.SERVICE_CONTRACT
                or terminal.get("work", {}).get("pid") != lifetime["pid"]
                or exited.get("pid") != lifetime["pid"] or exited.get("exit_code") != 0
                or exited.get("availability") != "measured"
                or exited.get("linux_process_identity") != lifetime["linux_process_identity"]):
            raise RuntimeError("owned service did not complete a clean verified shutdown")


def expected_shutdown_lifetimes(plan, failed, qualification):
    if failed is not None and qualification != "valid_unqualified":
        return None
    if plan["rss_only"] or qualification == "valid_unqualified":
        return 1
    if (plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
            and plan.get("query_mode") == "quantized_rerank" and plan["rows"] >= 5000):
        return 3
    return 2


def process_cpu_endpoint(pid, expected_identity):
    """Read one owned Linux process CPU total without rounding to whole seconds."""
    result = {
        "availability": "unavailable", "pid": pid, "linux_process_identity": expected_identity,
        "cpu_ns": None, "source": "/proc/<pid>/stat utime+stime and SC_CLK_TCK",
        "scope": "owned_process_lifetime_through_sample",
    }
    try:
        before = existing.common.linux_process_identity(pid)
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        close = raw.rfind(")")
        fields = raw[close + 2:].split()
        ticks, hz = int(fields[11]) + int(fields[12]), os.sysconf("SC_CLK_TCK")
        after = existing.common.linux_process_identity(pid)
        if (close <= 0 or before != expected_identity or after != expected_identity
                or type(hz) is not int or hz <= 0 or ticks < 0):
            raise RuntimeError("owned process identity or CPU clock changed")
        result.update(availability="measured", cpu_ns=ticks * 1_000_000_000 // hz)
    except (IndexError, OSError, RuntimeError, TypeError, ValueError) as exc:
        result["reason"] = f"{type(exc).__name__}: {exc}"
    return result


def paired_resource_delta(before, after, records):
    """Validate same-lifetime endpoints and expose raw and per-call phase deltas."""
    try:
        if not isinstance(records, list) or not records:
            raise ValueError("paired resource delta requires retained calls")
        calls = len(records)
        modes = {record["request_mode"] for record in records}
        work_records = [dense_contract.DenseSearchWork.from_dict(record["dense_work"])
                        for record in records]
        if len(modes) != 1:
            raise ValueError("paired resource delta spans more than one request arm")
        before_work, after_work = before["work"], after["work"]
        before_cpu, after_cpu = before["server_cpu"], after["server_cpu"]
        if (before["availability"] != "measured" or after["availability"] != "measured"
                or type(before["captured_monotonic_ns"]) is not int
                or type(after["captured_monotonic_ns"]) is not int
                or before["captured_monotonic_ns"] >= after["captured_monotonic_ns"]
                or type(before["pid"]) is not int or before["pid"] <= 0
                or before["pid"] != after["pid"]
                or before["pid"] != before_work["pid"] or after["pid"] != after_work["pid"]
                or not isinstance(before["linux_process_identity"], str)
                or not before["linux_process_identity"]
                or before["linux_process_identity"] != after["linux_process_identity"]
                or before_work["schema_version"] != "treedb-work-v1"
                or after_work["schema_version"] != "treedb-work-v1"
                or before_work["scope"] != "process" or after_work["scope"] != "process"
                or before_work["origin_kind"] != "go_package_init"
                or after_work["origin_kind"] != "go_package_init"
                or before_work["origin_unix_nano"] != after_work["origin_unix_nano"]
                or type(before_work["snapshot_unix_nano"]) is not int
                or type(after_work["snapshot_unix_nano"]) is not int
                or before_work["snapshot_unix_nano"] > after_work["snapshot_unix_nano"]
                or before_cpu["availability"] != "measured" or after_cpu["availability"] != "measured"
                or before_cpu["pid"] != after_cpu["pid"]
                or before_cpu["pid"] != before["pid"]
                or before_cpu["linux_process_identity"] != after_cpu["linux_process_identity"]
                or before_cpu["linux_process_identity"] != before["linux_process_identity"]
                or before_cpu["source"] != "/proc/<pid>/stat utime+stime and SC_CLK_TCK"
                or after_cpu["source"] != before_cpu["source"]
                or before_cpu["scope"] != "owned_process_lifetime_through_sample"
                or after_cpu["scope"] != before_cpu["scope"]
                or before_cpu["cpu_ns"] is None or after_cpu["cpu_ns"] is None):
            raise ValueError("paired endpoints do not share one measured process lifetime")
        if (type(before["generation"]) is not int or before["generation"] <= 0
                or before["generation"] != after["generation"]
                or not same_json(before["typed_graph"], after["typed_graph"])
                or type(before["total_db_bytes_including_wal"]) is not int
                or before["total_db_bytes_including_wal"] <= 0
                or before["total_db_bytes_including_wal"] != after["total_db_bytes_including_wal"]
                or any(type(value) is not int or value != 0
                       for endpoint in (before, after)
                       for value in endpoint["drained_pending"].values())
                or any(type(endpoint["typed_graph"]["pending"][name]) is not int
                       or endpoint["typed_graph"]["pending"][name] != 0
                       for endpoint in (before, after)
                       for name in ("rows", "tombstones", "value_slots", "bytes"))):
            raise ValueError("paired endpoint graph was not stable and drained")
        if any(type(value) is not int or value < 0 for endpoint in (before, after)
               for value in (
                   endpoint["work"]["memory"]["total_alloc"],
                   endpoint["work"]["memory"]["mallocs"],
                   endpoint["work"]["memory"]["heap_alloc"],
                   endpoint["client_cpu_ns"], endpoint["server_cpu"]["cpu_ns"],
               )):
            raise ValueError("paired resource endpoint counters are not nonnegative integers")
        total_alloc = after_work["memory"]["total_alloc"] - before_work["memory"]["total_alloc"]
        mallocs = after_work["memory"]["mallocs"] - before_work["memory"]["mallocs"]
        server_cpu = after_cpu["cpu_ns"] - before_cpu["cpu_ns"]
        client_cpu = after["client_cpu_ns"] - before["client_cpu_ns"]
        if min(total_alloc, mallocs, server_cpu, client_cpu) < 0:
            raise ValueError("paired cumulative resource counter moved backwards")
        before_graph, after_graph = before_work["graph"], after_work["graph"]
        before_output, after_output = before_work["output"]["search"], after_work["output"]["search"]
        if (after_graph["requests"]["attempts"] - before_graph["requests"]["attempts"] != calls
                or after_graph["requests"]["completed"]
                    - before_graph["requests"]["completed"] != calls
                or after_graph["requests"]["errors"] != before_graph["requests"]["errors"]
                or after_graph["hnsw"] - before_graph["hnsw"] != calls
                or after_graph["empty"] != before_graph["empty"]
                or after_graph["exact"] != before_graph["exact"]
                or not same_json(after_graph["filters"], before_graph["filters"])
                or after_output["attempts"] - before_output["attempts"] != calls
                or after_output["completed"] - before_output["completed"] != calls
                or after_output["errors"] != before_output["errors"]):
            raise ValueError("paired endpoints do not bind one completed HNSW call per record")
        graph_fields = {
            "base_ann_scored": "base_ann_scored", "base_candidates": "base_candidates",
            "base_edges": "base_edges", "delta_scored": "delta_scored",
            "exact_base_scored": "exact_base_scored", "base_shadowed": "base_shadowed",
            "base_result_ids": "base_result_ids",
        }
        for counter, field in graph_fields.items():
            expected = sum(getattr(work.graph, field) for work in work_records)
            if after_graph[counter] - before_graph[counter] != expected:
                raise ValueError(f"paired graph {counter} delta differs from retained call work")
        output_fields = {
            "requested": "requested", "fetched": "fetched", "missing": "missing",
            "output_bytes": "output_bytes",
            "retained_payload_fetches": "retained_payload_fetches",
            "json_reconstruction_rows": "json_reconstruction_rows",
            "typed_column_rows": "typed_column_rows",
        }
        for counter, field in output_fields.items():
            expected = sum(getattr(work.output, field) for work in work_records)
            if after_output[counter] - before_output[counter] != expected:
                raise ValueError(f"paired output {counter} delta differs from retained call work")
        return {
            "availability": "measured",
            "scope": ("one drained synchronous arm batch bracketed by process-wide Go runtime totals; "
                      "diagnostic endpoint handling is included in allocation endpoints"),
            "calls": calls,
            "request_mode": next(iter(modes)),
            "producer_counter_binding": "exact_sum_of_retained_dense_work",
            "server_cpu_ns": server_cpu,
            "server_cpu_ns_per_call": server_cpu / calls,
            "client_harness_cpu_ns": client_cpu,
            "client_harness_cpu_ns_per_call": client_cpu / calls,
            "total_alloc_bytes": total_alloc,
            "total_alloc_bytes_per_call": total_alloc / calls,
            "mallocs": mallocs,
            "mallocs_per_call": mallocs / calls,
            "heap_alloc_bytes_before": before_work["memory"]["heap_alloc"],
            "heap_alloc_bytes_after": after_work["memory"]["heap_alloc"],
        }
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"paired resource endpoints are invalid: {exc}") from exc


def paired_batch_arm_order(batch_kind, repetition):
    if batch_kind == "warmup" and repetition == -1:
        return ["exact", "quantized_rerank"]
    if batch_kind != "measured" or type(repetition) is not int or repetition < 0:
        return None
    return (["exact", "quantized_rerank"] if repetition % 2 == 0
            else ["quantized_rerank", "exact"])


def paired_phase(batch_kind, repetition, mode):
    order = paired_batch_arm_order(batch_kind, repetition)
    if order is None or mode not in order:
        return None
    batch = "warmup" if batch_kind == "warmup" else f"measured_r{repetition}"
    return f"paired_{batch}_{mode}"


def paired_snapshot_from_record(record, mode, expected_phase, expected_query, selected, rows):
    """Revalidate one retained public call instead of trusting runtime admission."""
    try:
        quantized = mode == "quantized_rerank"
        if (mode not in ("exact", "quantized_rerank")
                or record["outcome"] != "success" or record.get("error")
                or record["phase"] != expected_phase
                or record["request_mode"] != mode
                or record["query"] != expected_query
                or record["eligible"] != rows or record["filter"] is not None
                or record["requested_ef_search"] != selected
                or type(record["expected_generation"]) is not int
                or record["expected_generation"] <= 0
                or type(record["recall"]) not in (int, float)
                or type(record["ndcg_at_10"]) not in (int, float)
                or not math.isfinite(record["recall"])
                or not math.isfinite(record["ndcg_at_10"])
                or not 0 <= record["recall"] <= 1
                or not 0 <= record["ndcg_at_10"] <= 1):
            return None
        work = dense_contract.DenseSearchWork.from_dict(record["dense_work"])
        if (not work.completed or not work.graph.completed or not work.graph.snapshot.available
                or work.graph.route != "typed_hnsw" or not work.output.attempted
                or not work.output.completed
                or work.output.requested != 10 or len(record["results"]) != 10
                or work.output.fetched != len(record["results"]) or work.output.missing != 0
                or work.output.retained_payload_fetches != 10
                or work.output.json_reconstruction_rows != 10
                or work.output.typed_column_rows > 10 or work.output.output_bytes <= 0
                or any(vars(work.graph.filter).values())
                or work.graph.snapshot.base_manifest != work.graph.snapshot.current_manifest
                or work.graph.snapshot.base_coverage_lsn != work.graph.snapshot.current_coverage_lsn
                or not existing.quantized_snapshot_valid(
                    work.graph.snapshot, record["expected_generation"],
                )
                or type(record["started_monotonic_ns"]) is not int
                or type(record["ended_monotonic_ns"]) is not int
                or type(record["duration_ns"]) is not int or record["duration_ns"] <= 0
                or record["ended_monotonic_ns"] - record["started_monotonic_ns"] != record["duration_ns"]):
            return None
        if quantized:
            proof = dense_contract.DenseScorePlaneProof.from_dict(record["score_plane"])
            if (record["command_version"] != 3
                    or record["requested_rerank_candidates"] != selected
                    or not proof.completed or proof.requested_mode != "quantized_rerank"
                    or proof.effective_mode != "quantized_rerank"
                    or proof.route != "quantized_rerank" or proof.reason
                    or proof.quantized_index_name != QUANTIZED_PROFILE_NAME
                    or proof.quantized_codec != "scalar_u8" or proof.quantized_version != 1
                    or proof.quantized_config_hash != 0
                    or proof.requested_top_k != 10
                    or proof.requested_ef_search != selected
                    or proof.requested_rerank_candidates != selected
                    or proof.quantized_score_calls <= 0
                    or proof.quantized_code_bytes_read <= 0
                    or not 10 <= proof.actual_rerank_candidates <= selected
                    or proof.actual_rerank_candidates != proof.exact_base_rerank_score_calls
                    or proof.exact_suffix_score_calls != 0
                    or proof.exact_small_filter_score_calls != 0
                    or work.graph.delta_scored != 0 or work.graph.base_shadowed != 0
                    or work.graph.base_candidates != 0
                    or not dense_contract.dense_score_plane_byte_counters_match(proof, 768)
                    or not dense_contract.dense_quantized_response_work_matches(
                        work, proof, 10, 10, False,
                    )):
                return None
            snapshot = proof.snapshot
        else:
            if (record["command_version"] != 2
                    or record.get("score_plane") is not None
                    or record["requested_rerank_candidates"] is not None
                    or work.graph.base_ann_scored <= 0
                    or not 0 < work.graph.base_candidates <= work.graph.base_ann_scored
                    or work.graph.base_edges <= 0 or work.graph.delta_scored != 0
                    or work.graph.exact_base_scored != 0 or work.graph.base_shadowed != 0
                    or work.graph.base_result_ids != 10):
                return None
            snapshot = work.graph.snapshot
        return quantized_snapshot_identity(snapshot)
    except (KeyError, TypeError, ValueError):
        return None


def paired_results_valid(record, rows):
    try:
        ids, scores = [], []
        for result in record["results"]:
            if set(result) != {"id", "content", "meta", "score"}:
                return False
            identifier = result["id"]
            if not isinstance(identifier, str) or not identifier.startswith("row-"):
                return False
            row = int(identifier.removeprefix("row-"))
            score = result["score"]
            if (identifier != f"row-{row:06d}" or not 0 <= row < rows
                    or result["content"] != f"minima-cohere:{row}"
                    or result["meta"] != {
                        "user_id": f"{(row * 7919) % rows:06d}",
                        "fpath": f"/cohere/{row // 256:06d}.txt",
                    }
                    or type(score) not in (int, float) or not math.isfinite(score)
                    or not -1.000001 <= score <= 1.000001):
                return False
            ids.append(identifier)
            scores.append(score)
        return (len(ids) == 10 and len(set(ids)) == len(ids)
                and list(zip((-score for score in scores), ids))
                    == sorted(zip((-score for score in scores), ids)))
    except (KeyError, TypeError, ValueError):
        return False


def paired_typed_graph_matches_owner(graph, owner, rows):
    """Bind aggregate asset/storage gauges to the immutable per-call owner."""
    try:
        zero_debt = {"rows": 0, "tombstones": 0, "value_slots": 0, "bytes": 0}
        return (
            graph["index"] == "embedding"
            and graph["publication_present"] is True
            and graph["publication_unchanged"] is True
            and graph["serving_ready"] is True
            and graph["invalid"] is False and graph["reconciling"] is False
            and graph["base_present"] is True
            and graph["base_manifest"] == owner["base_manifest"]
            and graph["current_manifest"] == owner["current_manifest"]
            and graph["base_coverage_lsn"] == owner["base_coverage_lsn"]
            and graph["current_coverage_lsn"] == owner["current_coverage_lsn"]
            and graph["base_rows"] == rows
            and graph["suffix_rows"] == 0 and graph["suffix_tombstones"] == 0
            and graph["suffix_value_slots"] == 0 and graph["suffix_payload_bytes"] == 0
            and graph["debt"] == zero_debt and graph["pending"] == zero_debt
            and type(graph["installed_asset_bytes"]) is int
            and graph["installed_asset_bytes"] >= 0
            and type(graph["base_asset_bytes"]) is int and graph["base_asset_bytes"] > 0
            and type(graph["owner_asset_bytes"]) is int and graph["owner_asset_bytes"] >= 0
        )
    except (KeyError, TypeError):
        return False


def paired_query_artifact_valid(artifact, plan):
    """Fail-closed reusable validator for the same-owner repeated query packet."""
    try:
        timing = plan["paired_timing"]
        repetitions = artifact["repetitions"]
        queries = timing["queries"]
        selected = artifact["selected_coordinate"]
        if (timing["schema"] != "treedb_cohere_paired_timing_plan/v1"
                or timing["warmup_batches"] != 1
                or timing["measured_repetitions"] != PAIRED_TIMING_REPETITIONS
                or timing["arm_order"] != "alternate_first_complete_arm_batch_by_repetition"
                or not isinstance(queries, list) or not queries
                or queries != list(range(len(queries)))
                or len(queries) > PAIRED_TIMING_QUERY_COUNT
                or artifact["schema"] != PAIRED_QUERY_ARTIFACT_SCHEMA
                or artifact["status"] != "complete"
                or artifact["collection"] != "minima_cohere"
                or artifact["rows"] != plan["rows"] or artifact["dimensions"] != 768
                or artifact["top_k"] != 10 or artifact["eligible"] != plan["rows"]
                or not same_json(artifact["representation_arm"], plan["representation_arm"])
                or not same_json(artifact["timing_plan"], timing)
                or type(selected["ef_search"]) is not int
                or selected["ef_search"] not in plan["rss_controls"]
                or selected != {"ef_search": selected["ef_search"],
                                "rerank_candidates": selected["ef_search"]}
                or len(repetitions) != timing["measured_repetitions"]):
            return False
        expected_owner = None
        expected_sequence = 1
        endpoint_identity = None
        prior_endpoint = None
        prior_request_end = None
        for batch_index, batch in enumerate([artifact["warmup"], *repetitions]):
            kind = "warmup" if batch_index == 0 else "measured"
            repetition = batch_index - 1
            expected_order = paired_batch_arm_order(kind, repetition)
            if (batch["batch_kind"] != kind
                    or batch["repetition"] != batch_index - 1
                    or batch["queries"] != queries or batch["arm_order"] != expected_order
                    or set(batch["arms"]) != {"exact", "quantized_rerank"}):
                return False
            batch_owners = set()
            for mode in expected_order:
                arm = batch["arms"][mode]
                if (arm["mode"] != mode or arm["queries"] != queries
                        or len(arm["requests"]) != len(queries)):
                    return False
                if batch_index == 0:
                    if set(arm) != {"mode", "queries", "requests"}:
                        return False
                else:
                    if set(arm) != {"mode", "queries", "requests", "resources"}:
                        return False
                    resources = arm["resources"]
                    recomputed = paired_resource_delta(
                        resources["before"], resources["after"], arm["requests"],
                    )
                    if (resources["delta"]["availability"] != "measured"
                            or resources["delta"]["calls"] != len(queries)
                            or not same_json(resources["delta"], recomputed)):
                        return False
                    for endpoint in (resources["before"], resources["after"]):
                        current_identity = {
                            "pid": endpoint["pid"],
                            "linux_process_identity": endpoint["linux_process_identity"],
                            "generation": endpoint["generation"],
                            "origin_unix_nano": endpoint["work"]["origin_unix_nano"],
                            "typed_graph": endpoint["typed_graph"],
                            "total_db_bytes_including_wal": endpoint["total_db_bytes_including_wal"],
                        }
                        if endpoint_identity is None:
                            endpoint_identity = current_identity
                        elif not same_json(endpoint_identity, current_identity):
                            return False
                        if prior_endpoint is not None:
                            counters = (
                                endpoint["work"]["memory"]["total_alloc"],
                                endpoint["work"]["memory"]["mallocs"],
                                endpoint["server_cpu"]["cpu_ns"], endpoint["client_cpu_ns"],
                            )
                            prior_counters = (
                                prior_endpoint["work"]["memory"]["total_alloc"],
                                prior_endpoint["work"]["memory"]["mallocs"],
                                prior_endpoint["server_cpu"]["cpu_ns"],
                                prior_endpoint["client_cpu_ns"],
                            )
                            if (endpoint["captured_monotonic_ns"] <= prior_endpoint["captured_monotonic_ns"]
                                    or any(current < prior for current, prior
                                           in zip(counters, prior_counters))):
                                return False
                        prior_endpoint = endpoint
                phase = paired_phase(kind, repetition, mode)
                for position, record in enumerate(arm["requests"]):
                    owner = paired_snapshot_from_record(
                        record, mode, phase, queries[position], selected["ef_search"], plan["rows"],
                    )
                    if (record["request_sequence"] != expected_sequence
                            or not paired_results_valid(record, plan["rows"])
                            or owner is None
                            or (prior_request_end is not None
                                and record["started_monotonic_ns"] < prior_request_end)):
                        return False
                    prior_request_end = record["ended_monotonic_ns"]
                    expected_sequence += 1
                    batch_owners.add(canonical(owner))
                    if expected_owner is None:
                        expected_owner = owner
                    elif not same_json(expected_owner, owner):
                        return False
                if batch_index > 0:
                    resources = arm["resources"]
                    if (resources["before"]["captured_monotonic_ns"]
                            >= arm["requests"][0]["started_monotonic_ns"]
                            or resources["after"]["captured_monotonic_ns"]
                            <= arm["requests"][-1]["ended_monotonic_ns"]):
                        return False
            if len(batch_owners) != 1 or not same_json(batch["common_owner"], expected_owner):
                return False
        storage = artifact["storage_attribution"]
        final_mode = repetitions[-1]["arm_order"][-1]
        final_endpoint = repetitions[-1]["arms"][final_mode]["resources"]["after"]
        logical = plan["rows"] * 768
        return (
            paired_typed_graph_matches_owner(
                final_endpoint["typed_graph"], expected_owner, plan["rows"],
            )
            and storage["actual_quantized_tvis_bytes"] is None
            and storage["actual_quantized_tvis_bytes_availability"] == "producer_unavailable"
            and storage["logical_sq8_code_bytes_per_vector"] == 768
            and storage["logical_sq8_code_bytes"] == logical
            and type(storage["total_db_bytes_including_wal"]) is int
            and storage["total_db_bytes_including_wal"] > 0
            and storage["total_db_bytes_including_wal"]
                == final_endpoint["total_db_bytes_including_wal"]
            and storage["wal_boundary"] == "included_in_total_db_bytes; no subtraction"
            and all(type(storage["aggregate_typed_graph_asset_bytes"][name]) is int
                    and storage["aggregate_typed_graph_asset_bytes"][name]
                        >= (1 if name == "base_asset_bytes" else 0)
                    and storage["aggregate_typed_graph_asset_bytes"][name]
                        == final_endpoint["typed_graph"][name]
                    for name in ("installed_asset_bytes", "base_asset_bytes", "owner_asset_bytes"))
            and isinstance(storage["actual_quantized_tvis_bytes_producer"], str)
            and "VectorIndexSearchStats" in storage["actual_quantized_tvis_bytes_producer"]
            and "public" in storage["actual_quantized_tvis_bytes_producer"]
        )
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False


class Run:
    def __init__(self, plan):
        self.plan = plan
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)  # Never reuse or overwrite a prior run.
        self.events = (self.output / "events.jsonl").open("x", buffering=1)
        self.lock, self.overlap_lock, self.cancel = threading.Lock(), threading.Lock(), threading.Event()
        self.failure = None
        self.controller = existing.ServiceController(Path(plan["service_bin"]), plan["url"], self.output / "db",
            "command_wal_durable", 600, 120, diagnostics_url=plan["diagnostics_url"],
            block_profile_rate=0, mutex_profile_fraction=0, native_address=plan["native_address"], measured=True,
            environment=treedb_service_environment(plan),
            construction_decisions=plan["construction_decisions"])
        self.clients = existing.ThreadLocalClients(plan["url"], plan["operation_timeout_s"], self.controller)
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(plan["rows"], 768))
        self.queries = np.memmap(data / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768))
        normalized_v4 = plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
        if plan["mode"] == "diagnostic":
            source_truth, _ = strict_json_object(
                data / "truth.json", "frozen truth", EVIDENCE_JSON_MAX_BYTES,
            )
        else:
            source_truth = exact_truth(self.vectors, self.queries, plan["eligible_counts"])
        self.original_cosine_truth = source_truth
        self.normalized_truth_scores = None
        if normalized_v4:
            self.truth, self.normalized_truth_scores = canonical_normalized_f32_truth(
                self.vectors, self.queries, plan["eligible_counts"],
            )
        else:
            self.truth = source_truth
        for eligible in plan["eligible_counts"]:
            if len(self.truth[str(eligible)]) != plan["queries"]:
                raise ValueError("oracle query cardinality mismatch")
        self.updated = set()
        self.overlap_initial = frozenset()
        self.overlap_initial_touched = frozenset()
        self.overlap_initial_deleted = frozenset()
        self.overlap_initial_advance = 0
        self.overlap_batches = []
        self.quantized_touched = set()
        self.quantized_deleted = set()
        self.quantized_owner_advance = 0
        self.quantized_folded = False
        self.quantized_initial_snapshot = None
        self.quantized_owner_snapshots = {}
        self.normalized_v4_resource_inventories = []
        self.info = None
        self.initial_graph_build = None
        self.quantized_requests = []
        self.quantized_quality = {}
        self.selected_efs = {}
        self.paired_query_artifact = None

    def emit(self, event, **fields):
        with self.lock:
            self.events.write(canonical({"event": event, **fields}).decode())

    def timed(self, phase, call, *, timing_evidence=None, **fields):
        if self.failure:
            raise RuntimeError(self.failure)
        start = time.monotonic_ns()
        try:
            result = call()
        except BaseException as exc:
            self.emit("call", phase=phase, start_ns=start, duration_ns=time.monotonic_ns() - start,
                      outcome="error", error=f"{type(exc).__name__}: {exc}", **fields)
            raise
        end = time.monotonic_ns()
        if timing_evidence is not None:
            timing_evidence.update(started_monotonic_ns=start, ended_monotonic_ns=end,
                                   duration_ns=end - start)
        self.emit("call", phase=phase, start_ns=start, end_ns=end, duration_ns=end - start, outcome="completed", **fields)
        return result

    def check_resources(self):
        free = shutil.disk_usage(self.output).free
        size = existing.common.disk_bytes(self.output)
        process = self.controller.process
        rss = 0
        for pid in (os.getpid(), process.pid if process else None):
            if pid is not None:
                try:
                    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                        if line.startswith("VmRSS:"):
                            rss += int(line.split()[1]) * 1024
                except FileNotFoundError:
                    pass
        elapsed = time.monotonic() - self.started
        self.emit("resource", elapsed_s=elapsed, free_bytes=free, output_bytes=size, combined_rss_bytes=rss)
        if (free < self.plan["minimum_free_bytes"] or size > self.plan["maximum_output_bytes"]
                or rss > self.plan["maximum_combined_rss_bytes"] or elapsed > self.plan["wall_limit_s"]):
            raise RuntimeError("frozen disk/RAM/wall budget exceeded")

    def guard(self):
        while not self.cancel.wait(1):
            if not self.failure:
                try:
                    self.check_resources()
                except BaseException as exc:
                    self.failure = f"resource guard: {exc}"
                    self.emit("guard_failed", error=self.failure)
            if self.failure:
                process = self.controller.process
                if process and existing.common.linux_process_identity(process.pid) == self.controller._owned_identity:
                    os.kill(process.pid, signal.SIGTERM)

    def ensure(self):
        vector_index_options = {"strategy": "column_graph", "ef_construction": self.plan["ef_construction"]}
        normalized_v4 = self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
        if normalized_v4:
            vector_index_options["representation"] = NORMALIZED_REPRESENTATION
            vector_index_options["m"] = 16
            vector_index_options["ef_search"] = NORMALIZED_EF_SEARCH
        if self.plan.get("query_mode") == "quantized_rerank":
            vector_index_options["m"] = self.plan["vector_m"]
            vector_index_options["quantized_indexes"] = [{
                "name": self.plan["quantized_index_name"], "codec": "scalar_u8", "version": 1,
            }]
        info = self.clients.ensure_index("minima_cohere", 768, "cosine", typed_input=True,
            scalar_fields=[{"field": "meta.user_id", "value_type": "string"}, {"field": "meta.fpath", "value_type": "string"}],
            vector_index_options=vector_index_options)
        if (info.name != "minima_cohere" or info.dimension != 768 or info.metric != "cosine"
                or info.contract_version != existing.SERVICE_CONTRACT
                or info.embedding_field != "embedding" or info.vector_index_name != "embedding"
                or info.text_field != "content" or info.text_index_name != "content"
                or info.document_type != "treedb_document_service_v1"
                or info.vector_strategy != "column_graph"
                or info.vector_m != (self.plan["vector_m"] if self.plan.get("query_mode") == "quantized_rerank" else 16)
                or info.vector_ef_construction != self.plan["ef_construction"]
                or info.extra.get("typed_input") is not True
                or {(f.field, f.value_type) for f in info.scalar_fields} != {("meta.user_id", "string"), ("meta.fpath", "string")}):
            raise RuntimeError("public collection schema differs from frozen typed schema")
        if normalized_v4 and (
                info.vector_representation != NORMALIZED_REPRESENTATION
                or info.vector_ef_search != NORMALIZED_EF_SEARCH):
            raise RuntimeError("public collection schema differs from frozen normalized-v4 schema")
        if self.plan.get("query_mode") == "quantized_rerank":
            if normalized_v4:
                quantized = [item.to_dict() for item in info.quantized_indexes]
                if quantized != [{"name": QUANTIZED_PROFILE_NAME, "codec": "scalar_u8", "version": 1}]:
                    raise RuntimeError("normalized-v4 collection SQ8 declaration differs from frozen minima_sq8")
            elif not existing.quantized_effective_index_valid(
                    info, "minima_cohere", 768, "cosine", self.plan["ef_construction"]):
                raise RuntimeError("public collection quantized profile differs from frozen minima_sq8")
            self.emit("quantized_index", observed=info.to_dict())
        self.info = info
        return info

    def optimize(self, action):
        result = self.clients.optimize_index("minima_cohere", column_graph_action=action,
            column_graph_serving=self.plan["serving"] if action in ("build", "ensure") else None,
            expected_generation=self.info.generation)
        if action == "fold" and self.plan.get("query_mode") == "quantized_rerank":
            with self.overlap_lock:
                self.quantized_folded = True
        return result

    def record_quantized_delete(self, rows):
        if self.plan.get("query_mode") != "quantized_rerank":
            return
        with self.overlap_lock:
            self.quantized_owner_advance += 1
            self.quantized_touched.update(rows)
            self.quantized_deleted.update(rows)
            self.updated.difference_update(rows)

    def _quantized_upsert_state_locked(self, rows, updated):
        prior = {
            "updated": frozenset(self.updated),
            "touched": frozenset(self.quantized_touched),
            "deleted": frozenset(self.quantized_deleted),
            "owner_advance": self.quantized_owner_advance,
            "folded": self.quantized_folded,
        }
        current = quantized_upsert_state(prior, rows, updated)
        if current is None:
            raise RuntimeError("invalid authored quantized upsert transition")
        return prior, current

    def _apply_quantized_upsert_state_locked(self, state):
        self.updated = set(state["updated"])
        self.quantized_touched = set(state["touched"])
        self.quantized_deleted = set(state["deleted"])
        self.quantized_owner_advance = state["owner_advance"]

    def quantized_allowed_states(self, writer_active, request_start_ns, request_end_ns):
        with self.overlap_lock:
            if writer_active:
                return quantized_lifecycle_states(
                    self.overlap_initial, self.overlap_initial_touched,
                    self.overlap_initial_deleted, self.overlap_initial_advance,
                    [dict(window) for window in self.overlap_batches],
                    request_start_ns, request_end_ns,
                )
            return [{
                "updated": frozenset(self.updated),
                "touched": frozenset(self.quantized_touched),
                "deleted": frozenset(self.quantized_deleted),
                "owner_advance": self.quantized_owner_advance,
                "folded": self.quantized_folded,
            }]

    def validate_quantized_lifecycle(self, response, record, writer_active, eligible, ef,
                                     filter_requested=None):
        if filter_requested is None:
            filter_requested = eligible != self.plan["rows"]
        states = self.quantized_allowed_states(
            writer_active, record["started_monotonic_ns"], record["ended_monotonic_ns"],
        )
        snapshot = quantized_snapshot_identity(response.score_plane.snapshot)
        if snapshot is None:
            raise RuntimeError("quantized response omitted its lifecycle owner identity")
        with self.overlap_lock:
            initial = self.quantized_initial_snapshot
            if initial is None:
                if (len(states) != 1 or states[0]["owner_advance"] != 0 or states[0]["folded"]
                        or snapshot["base_manifest"] != snapshot["current_manifest"]
                        or snapshot["base_coverage_lsn"] != snapshot["current_coverage_lsn"]):
                    raise RuntimeError("quantized response did not establish one initial graph owner")
                initial = snapshot
            matched = []
            for state in states:
                key = (state["owner_advance"], state["folded"])
                prior = self.quantized_owner_snapshots.get(key)
                plans = quantized_response_state_plans(
                    response, state, self.plan["rows"], eligible, ef, filter_requested,
                )
                if (plans and (prior is None or same_json(prior, snapshot))
                        and quantized_snapshot_matches_state(
                            response.score_plane.snapshot, initial, state,
                        ) and quantized_response_projection_matches_state(response.documents, state)):
                    matched.append((state, plans))
            if len(matched) != 1:
                raise RuntimeError("quantized response does not identify one exact projection/width/owner lifecycle state")
            if self.quantized_initial_snapshot is None:
                self.quantized_initial_snapshot = initial
            state, plans = matched[0]
            self.quantized_owner_snapshots[(state["owner_advance"], state["folded"])] = snapshot
        work = quantized_state_work(state, self.plan["rows"], eligible, ef, filter_requested)
        live_base, live_suffix, _ = work
        record["lifecycle_state"] = {
            "owner_advance": state["owner_advance"], "folded": state["folded"],
            "shadow_allowance": max(plan[3] for plan in plans),
            "live_base": live_base, "live_suffix": live_suffix,
        }
        return state

    def upsert(self, rows, phase, updated=False, record_quantized=False, record_overlap=False):
        batch = [make_document(self.vectors, row, self.plan["rows"], updated) for row in rows]
        window, expected_state = None, None
        if record_quantized or record_overlap:
            with self.overlap_lock:
                prior, expected_state = self._quantized_upsert_state_locked(rows, updated)
                published = expected_state["owner_advance"] != prior["owner_advance"]
                if record_overlap:
                    window = {
                        "started_monotonic_ns": time.monotonic_ns(), "ended_monotonic_ns": None,
                        "rows": tuple(rows), "updated": updated,
                        "published": published, "success": None,
                    }
                    self.overlap_batches.append(window)
        try:
            response = self.timed(
                phase,
                lambda: self.clients.native.upsert_documents(
                    "minima_cohere", batch, index_info=self.info),
                first_row=rows[0], rows=len(rows),
            )
            if response.upserted != len(rows) or response.ids != [d["id"] for d in batch]:
                raise RuntimeError("public upsert completion mismatch")
        except BaseException:
            if window is not None:
                with self.overlap_lock:
                    window.update(ended_monotonic_ns=time.monotonic_ns(), success=False)
            raise
        if expected_state is not None:
            with self.overlap_lock:
                prior, current = self._quantized_upsert_state_locked(rows, updated)
                if current != expected_state:
                    raise RuntimeError("authored quantized upsert state changed during the single-writer call")
                self._apply_quantized_upsert_state_locked(current)
                if window is not None:
                    window.update(ended_monotonic_ns=time.monotonic_ns(), success=True)
        return response

    def search_normalized_v4(self, phase, eligible, query, writer_active=False,
                             query_filter=None, request_mode=None):
        configured_quantized = self.plan.get("query_mode") == "quantized_rerank"
        request_mode = ("quantized_rerank" if configured_quantized else "exact") \
            if request_mode is None else request_mode
        if request_mode not in ("exact", "quantized_rerank") \
                or (request_mode == "quantized_rerank" and not configured_quantized):
            raise ValueError("normalized-v4 request mode is outside the frozen collection arm")
        selected_filter = predicate(self.plan["rows"], eligible) if query_filter is None else query_filter
        record = {
            "request_sequence": len(self.quantized_requests) + 1,
            "phase": phase, "eligible": eligible, "query": query,
            "requested_ef_search": NORMALIZED_EF_SEARCH,
            "requested_rerank_candidates": (
                NORMALIZED_RERANK_CANDIDATES if request_mode == "quantized_rerank" else None
            ),
            "command_version": 4, "request_mode": request_mode,
            "diagnostics": True, "return_embedding": False,
            "expected_generation": self.info.generation,
            "filter": selected_filter, "started_monotonic_ns": time.monotonic_ns(),
            "outcome": "error", "results": [],
        }
        with self.lock:
            self.quantized_requests.append(record)
        response = None
        try:
            timing = {}
            response = self.timed(
                phase,
                lambda: v4_gate.python_call(
                    self.clients.native, self.info, self.queries[query].tolist(), request_mode,
                    diagnostics=True, index="minima_cohere", top_k=10,
                    ef_search=NORMALIZED_EF_SEARCH,
                    rerank_candidates=NORMALIZED_RERANK_CANDIDATES,
                    quantized_index=QUANTIZED_PROFILE_NAME,
                ) if selected_filter is None else self.clients.native.query_by_embedding(
                    "minima_cohere", self.queries[query].tolist(), 10, selected_filter,
                    route="ann", ef_search=NORMALIZED_EF_SEARCH,
                    query_mode=request_mode,
                    **({"quantized_index_name": QUANTIZED_PROFILE_NAME,
                        "quantized_rerank_candidates": NORMALIZED_RERANK_CANDIDATES}
                       if request_mode == "quantized_rerank" else {}),
                    return_embedding=False, diagnostics=True, index_info=self.info,
                ),
                timing_evidence=timing, eligible=eligible, ef=NORMALIZED_EF_SEARCH,
                query=query, writer_active=writer_active, request_mode=request_mode,
                diagnostics=True,
            )
            record.update(timing)
            # Retain the returned evidence even when a subsequent validation
            # rejects it. This remains an error receipt, never accepted evidence.
            record.update(
                route_identity=asdict(response.route_identity) if response.route_identity is not None else None,
                dense_work=asdict(response.dense_work) if response.dense_work is not None else None,
                score_plane=asdict(response.score_plane) if response.score_plane is not None else None,
                results=[{"id": document.id, "content": document.content,
                          "meta": document.meta, "score": float(document.score)}
                         for document in response.documents],
            )
            receipt = v4_gate.validate_response(
                response, request_mode, diagnostics=True, top_k=min(10, eligible),
                expected_route=normalized_v4_expected_routes(
                    eligible, selected_filter is not None,
                ),
            )
            identity, work, proof = response.route_identity, response.dense_work, response.score_plane
            if (identity is None or work is None or identity.filter != (selected_filter is not None)
                    or identity.result_count != len(response.documents)
                    or identity.representation != NORMALIZED_REPRESENTATION
                    or identity.embedding_vector_reads != 0
                    or identity.embedding_vector_bytes != 0
                    or identity.embedding_output_bytes != 0):
                raise RuntimeError("normalized-v4 diagnostic receipt is not request-complete")
            if selected_filter is not None and work.graph.filter.eligible_rows != eligible:
                raise RuntimeError("normalized-v4 producer filter membership differs from the frozen cohort")
            if request_mode == "exact" and writer_active:
                states = self.quantized_allowed_states(
                    True, timing["started_monotonic_ns"], timing["ended_monotonic_ns"],
                )
                if not any(quantized_response_projection_matches_state(
                        response.documents, state) for state in states):
                    raise RuntimeError("normalized-v4 exact response crossed owner lifecycle states")
            if request_mode == "quantized_rerank":
                if (proof is None or proof.forbidden_stable_score_calls != 0
                        or proof.packed_score_batch_calls != identity.packed_score_calls
                        or proof.packed_score_candidates != identity.packed_score_candidates
                        or proof.packed_vector_bytes_read != identity.packed_vector_bytes_read):
                    raise RuntimeError("normalized-v4 SQ8 proof is not packed and same-owner")
                self.validate_quantized_lifecycle(
                    response, record, writer_active, eligible, NORMALIZED_EF_SEARCH,
                    selected_filter is not None,
                )
            ids, scores, results = [], [], []
            query_vector = canonical_normalized_f32_rows(self.queries[query:query + 1])[0]
            for document in response.documents:
                row = int(document.id.removeprefix("row-"))
                if (document.id != f"row-{row:06d}" or not 0 <= row < self.plan["rows"]
                        or (row * 7919) % self.plan["rows"] >= eligible
                        or document.embedding is not None or document.score is None):
                    raise RuntimeError("normalized-v4 result shape or cohort membership is invalid")
                expected_meta = {"user_id": f"{(row * 7919) % self.plan['rows']:06d}",
                                 "fpath": f"/cohere/{row // 256:06d}.txt"}
                base_content = f"minima-cohere:{row}"
                expected_content = ({base_content, base_content + ":updated"}
                                    if writer_active else
                                    {base_content + (":updated" if row in self.updated else "")})
                if document.meta != expected_meta or document.content not in expected_content:
                    raise RuntimeError("normalized-v4 result projection differs from the reachable state")
                if not math.isfinite(document.score):
                    raise RuntimeError("normalized-v4 score differs from canonical float32 dot")
                ids.append(document.id)
                scores.append(float(document.score))
                results.append({"id": document.id, "content": document.content,
                                "meta": document.meta, "score": float(document.score)})
            if ids:
                ordinals = [int(identifier.removeprefix("row-")) for identifier in ids]
                vectors = canonical_normalized_f32_rows(self.vectors[ordinals])
                expected_scores = np.clip(vectors @ query_vector, -1, 1)
                if not np.allclose(scores, expected_scores, rtol=2e-6, atol=2e-6):
                    raise RuntimeError("normalized-v4 score differs from canonical float32 dot")
            if list(zip((-score for score in scores), ids)) != sorted(zip((-score for score in scores), ids)):
                raise RuntimeError("normalized-v4 results are not canonically ordered")
            canonical_truth = [] if eligible == 0 else self.truth[str(eligible)][query]
            original_truth = [] if eligible == 0 else self.original_cosine_truth[str(eligible)][query]
            recall = len(set(ids) & set(canonical_truth)) / len(canonical_truth) if canonical_truth else (1.0 if not ids else 0.0)
            original_recall = len(set(ids) & set(original_truth)) / len(original_truth) if original_truth else (1.0 if not ids else 0.0)
            record.update(
                outcome="success", results=results, recall=recall,
                original_cosine_recall=original_recall,
                dense_work=asdict(work), score_plane=asdict(proof) if proof is not None else None,
                route_identity=receipt["route"],
            )
            self.emit(
                "search_result", phase=phase, eligible=eligible,
                ef=NORMALIZED_EF_SEARCH, rerank_candidates=(
                    NORMALIZED_RERANK_CANDIDATES if request_mode == "quantized_rerank" else None
                ), query=query, ids=ids, recall=recall,
                scores=scores,
                original_cosine_recall=original_recall, writer_active=writer_active,
                request_mode=request_mode, command_version=4, diagnostics=True,
                route_identity=record["route_identity"], dense_work=record["dense_work"],
                score_plane=record["score_plane"],
            )
            return response
        except BaseException as exc:
            record.setdefault("ended_monotonic_ns", time.monotonic_ns())
            record["error"] = f"{type(exc).__name__}: {exc}"
            self.emit("search_request_failure", request=record)
            raise

    def search(self, phase, eligible, ef, query, writer_active=False, query_filter=None,
               request_mode=None, request_ledger=None):
        if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
            if request_ledger is not None:
                raise ValueError("normalized-v4 production timing uses the dedicated retained lane")
            return self.search_normalized_v4(
                phase, eligible, query, writer_active, query_filter, request_mode,
            )
        configured_quantized = self.plan.get("query_mode") == "quantized_rerank"
        request_mode = ("quantized_rerank" if configured_quantized else "exact") \
            if request_mode is None else request_mode
        if request_mode not in ("exact", "quantized_rerank") \
                or (request_mode == "quantized_rerank" and not configured_quantized):
            raise ValueError("request mode is outside the frozen collection arm")
        quantized = request_mode == "quantized_rerank"
        selected_filter = predicate(self.plan["rows"], eligible) if query_filter is None else query_filter
        record = None
        if quantized or request_ledger is not None:
            ledger = self.quantized_requests if request_ledger is None else request_ledger
            with self.lock:
                record = {
                    "request_sequence": len(ledger) + 1,
                    "phase": phase, "eligible": eligible, "query": query,
                    "requested_ef_search": ef,
                    "requested_rerank_candidates": ef if quantized else None,
                    "command_version": 3 if quantized else 2,
                    "expected_generation": self.info.generation,
                    "started_monotonic_ns": time.monotonic_ns(), "outcome": "error", "results": [],
                }
                if request_ledger is not None:
                    record.update(request_mode=request_mode, filter=selected_filter)
                ledger.append(record)
        response = None
        try:
            call_timing = {}
            response = self.timed(phase, lambda: self.clients.native.query_by_embedding(
                "minima_cohere", self.queries[query].tolist(), 10, selected_filter,
                route="ann", ef_search=ef, index_info=self.info,
                **({"query_mode": "quantized_rerank", "quantized_index_name": QUANTIZED_PROFILE_NAME,
                    "quantized_rerank_candidates": ef} if quantized else {})),
                timing_evidence=call_timing, eligible=eligible, ef=ef, query=query,
                writer_active=writer_active, request_mode=request_mode)
            if record is not None:
                if not call_timing:
                    ended = time.monotonic_ns()
                    call_timing.update(
                        started_monotonic_ns=record["started_monotonic_ns"],
                        ended_monotonic_ns=ended,
                        duration_ns=ended - record["started_monotonic_ns"],
                    )
                record.update(call_timing)
                proof = getattr(response, "dense_work", None)
                score_plane = getattr(response, "score_plane", None)
                if proof is not None:
                    record["dense_work"] = asdict(proof)
                if score_plane is not None:
                    record["score_plane"] = asdict(score_plane)
            if quantized:
                validate_quantized_response(response, self.plan["rows"], eligible, ef,
                                            selected_filter is not None, self.info.generation)
            if ((not quantized and response.native_command_version != 2)
                or (not quantized and getattr(response, "score_plane", None) is not None)
                or response.index.generation != self.info.generation
                or response.index.vector_strategy != "column_graph" or response.route != "ann"
                or response.exact_fallbacks or response.full_document_scan_fallbacks or response.dense_work is None):
                raise RuntimeError("search left the required producer-backed native typed route")
            if eligible != self.plan["rows"] and (not response.dense_work.graph.filter.completed
                                                   or response.dense_work.graph.filter.eligible_rows != eligible):
                raise RuntimeError("producer scalar-filter membership differs from declared population")
            ids = [doc.id for doc in response.documents]
            if len(ids) != min(10, eligible) or len(set(ids)) != len(ids):
                raise RuntimeError("unexpected search result cardinality")
            results = [] if record is not None else None
            scores = []
            for doc in response.documents:
                row = int(doc.id.removeprefix("row-"))
                if not 0 <= row < self.plan["rows"] or (row * 7919) % self.plan["rows"] >= eligible:
                    raise RuntimeError("cross-filter search result")
                if doc.meta.get("user_id") != f"{(row * 7919) % self.plan['rows']:06d}" or doc.score is None:
                    raise RuntimeError("missing scalar/score projection")
                expected_meta = {"user_id": f"{(row * 7919) % self.plan['rows']:06d}",
                                 "fpath": f"/cohere/{row // 256:06d}.txt"}
                base_content = f"minima-cohere:{row}"
                expected_content = ({base_content, base_content + ":updated"}
                                    if writer_active else
                                    {base_content + (":updated" if row in self.updated else "")})
                if doc.meta != expected_meta or doc.content not in expected_content:
                    raise RuntimeError("search result omitted or changed a full document projection")
                # Correctness-only scoring is outside the public API timer.
                vector = np.asarray(self.vectors[row], dtype=np.float64)
                query_vector = np.asarray(self.queries[query], dtype=np.float64)
                score = np.dot(vector, query_vector) / (np.linalg.norm(vector) * np.linalg.norm(query_vector))
                if not math.isfinite(doc.score) or abs(score - doc.score) > 2e-5:
                    raise RuntimeError("returned score differs from independent full-vector cosine")
                scores.append(float(doc.score))
                if record is not None:
                    results.append({"id": doc.id, "content": doc.content,
                                    "meta": doc.meta, "score": float(doc.score)})
            if not quantized and writer_active:
                states = self.quantized_allowed_states(
                    True, call_timing["started_monotonic_ns"],
                    call_timing["ended_monotonic_ns"],
                )
                if not any(quantized_response_projection_matches_state(
                        response.documents, state) for state in states):
                    raise RuntimeError(
                        "exact response projection does not match one whole reachable mutation state",
                    )
            if quantized:
                self.validate_quantized_lifecycle(
                    response, record, writer_active, eligible, ef,
                    selected_filter is not None,
                )
            if list(zip((-score for score in scores), ids)) != sorted(zip((-score for score in scores), ids)):
                raise RuntimeError("results are not canonically ordered by score then ID")
            truth = [] if eligible == 0 else self.truth[str(eligible)][query]
            if (quantized and response.score_plane.route in ("typed_exact", "typed_empty")
                    and ids != truth):
                raise RuntimeError("typed-exact quantized result rank differs from frozen eligible truth")
            recall = len(set(ids) & set(truth)) / len(truth) if truth else (1.0 if not ids else 0.0)
            ndcg = binary_ndcg(ids, truth) if truth else (1.0 if not ids else 0.0)
            event = {"phase": phase, "eligible": eligible, "ef": ef, "query": query, "ids": ids,
                     "recall": recall, "ndcg_at_10": ndcg, "writer_active": writer_active,
                     "dense_work": record["dense_work"] if quantized else asdict(response.dense_work)}
            if record is not None:
                record.update(outcome="success", results=results, recall=recall, ndcg_at_10=ndcg)
            if quantized:
                event["rerank_candidates"] = ef
                event["score_plane"] = record["score_plane"]
            self.emit("search_result", **event)
            return response
        except BaseException as exc:
            if record is not None:
                record.setdefault("ended_monotonic_ns", time.monotonic_ns())
                for name in ("dense_work", "score_plane"):
                    value = getattr(response, name, None) if response is not None else None
                    if value is None:
                        value = getattr(exc, name, None)
                    if value is not None and name not in record:
                        record[name] = asdict(value)
                record["error"] = f"{type(exc).__name__}: {exc}"
                self.emit("search_request_failure", request=record)
            raise

    def check_documents(self, docs, expected_ids):
        if [None if doc is None else doc.id for doc in docs] != expected_ids:
            raise RuntimeError("public materialization ID ordering/missing mismatch")
        canonical_vectors = {}
        if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
            present = [doc for doc in docs if doc is not None]
            if present:
                rows = [int(doc.id.removeprefix("row-")) for doc in present]
                canonical_vectors = dict(zip(
                    (doc.id for doc in present), canonical_normalized_f32_rows(self.vectors[rows]),
                ))
        for doc in docs:
            if doc is None:
                continue
            row = int(doc.id.removeprefix("row-"))
            expected = make_document(self.vectors, row, self.plan["rows"], row in self.updated)
            if doc.content != expected["content"] or doc.meta != expected["meta"]:
                raise RuntimeError("public materialization payload mismatch")
            actual = np.asarray(doc.embedding, dtype=np.float64)
            want = np.asarray(self.vectors[row], dtype=np.float64)
            if actual.shape != (768,) or not np.isfinite(actual).all():
                raise RuntimeError("public materialization vector shape mismatch")
            actual_norm, want_norm = np.linalg.norm(actual), np.linalg.norm(want)
            if not (math.isfinite(actual_norm) and actual_norm > 0 and math.isfinite(want_norm) and want_norm > 0):
                raise RuntimeError("public materialization zero/nonfinite vector norm")
            want /= want_norm
            if self.plan.get("campaign_profile") != CAMPAIGN_PROFILE_NORMALIZED_V4:
                actual /= actual_norm
            else:
                want = canonical_vectors[doc.id]
                if abs(actual_norm - 1.0) > 1e-5 or not np.array_equal(actual.astype(np.float32), want):
                    raise RuntimeError("normalized-v4 output is not the stored canonical vector")
            if np.max(np.abs(actual - want)) > 1e-6:
                raise RuntimeError("public materialization full-vector mismatch")
        return canonical_vectors

    def overlap(self):
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        with self.overlap_lock:
            self.overlap_initial = frozenset(self.updated)
            self.overlap_initial_touched = frozenset(self.quantized_touched)
            self.overlap_initial_deleted = frozenset(self.quantized_deleted)
            self.overlap_initial_advance = self.quantized_owner_advance
            self.overlap_batches.clear()
        barrier = threading.Barrier(5)
        active = threading.Event()
        active.set()
        def reader(worker):
            barrier.wait(timeout=30)
            for n in range(64):
                eligible = self.plan["overlap_eligible"]
                ef = self.selected_efs.get(str(eligible), self.plan["overlap_ef"])
                request_mode = None
                if (quantized and self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4):
                    request_mode = "exact" if (n + worker) % 2 == 0 else "quantized_rerank"
                self.search("overlap_search", eligible, ef,
                            (n * 4 + worker) % self.plan["queries"], active.is_set(),
                            request_mode=request_mode)
        def writer():
            barrier.wait(timeout=30)
            try:
                for n in range(8):
                    start = (self.plan["rows"] - 256 * (n + 1)) % self.plan["rows"]
                    rows = list(range(start, start + 256))
                    self.upsert(rows, "overlap_replace", updated=True,
                                record_quantized=quantized,
                                record_overlap=True)
            finally:
                active.clear()
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(reader, n) for n in range(4)] + [pool.submit(writer)]
            for future in futures:
                future.result()

    def lifecycle(self):
        rows = list(range(256))
        ids = [f"row-{row:06d}" for row in rows]
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        self.upsert(rows, "explicit_update", updated=True, record_quantized=quantized)
        if not quantized:
            self.updated.update(rows)
        docs = self.timed("native_get_many", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
            modes = ("exact", "quantized_rerank") if quantized else ("exact",)
            for mode in modes:
                self.search("post_update_visibility", self.plan["rows"], NORMALIZED_EF_SEARCH, 0,
                            request_mode=mode)
        deleted = self.timed("http_delete_file", lambda: self.clients.delete_by_filter("minima_cohere",
            {"field": "meta.fpath", "operator": "==", "value": "/cohere/000000.txt"}, expected_generation=self.info.generation))
        if deleted.deleted != len(rows):
            raise RuntimeError("file delete cardinality mismatch")
        self.record_quantized_delete(rows)
        docs = self.timed("native_deleted_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, [None] * len(ids))
        self.upsert(rows, "reindex_replacement", updated=True, record_quantized=quantized)
        docs = self.timed("native_reinsert_visibility", lambda: self.clients.native.get_many("minima_cohere", ids, index_info=self.info))
        self.check_documents(docs, ids)
        if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
            for mode in (("exact", "quantized_rerank") if quantized else ("exact",)):
                self.search("post_replacement_visibility", self.plan["rows"], NORMALIZED_EF_SEARCH, 1,
                            request_mode=mode)
        empty_filter = {"field": "meta.user_id", "operator": "==", "value": "missing-user"}
        if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
            modes = ("exact", "quantized_rerank") if quantized else ("exact",)
            responses = [self.search("empty_user", 0, NORMALIZED_EF_SEARCH, 0,
                                     query_filter=empty_filter, request_mode=mode)
                         for mode in modes]
            if any(response.documents for response in responses):
                raise RuntimeError("empty-user normalized-v4 query returned documents")
            return
        if self.plan.get("query_mode") == "quantized_rerank":
            response = self.search("empty_user", 0, self.selected_efs[str(self.plan["rows"])], 0,
                                   query_filter=empty_filter)
        else:
            response = self.timed("empty_user", lambda: self.clients.native.query_by_embedding(
                "minima_cohere", self.queries[0].tolist(), 10, empty_filter,
                route="ann", ef_search=512, index_info=self.info))
        if response.documents:
            raise RuntimeError("empty-user query returned documents")

    def scroll(self):
        after, seen = None, 0
        final_state = hashlib.sha256()
        maximum_vector_error = 0.0
        maximum_norm_error = 0.0
        while True:
            result = self.clients.filter_documents("minima_cohere", limit=256, return_embedding=True,
                after_id=after, cursor_page=True, expected_generation=self.info.generation)
            expected = [f"row-{row:06d}" for row in range(seen, min(seen + 256, self.plan["rows"]))]
            canonical_vectors = self.check_documents(result.documents, expected)
            if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
                for document in result.documents:
                    row = int(document.id.removeprefix("row-"))
                    payload = canonical({
                        "id": document.id, "content": document.content, "meta": document.meta,
                    })
                    final_state.update(len(payload).to_bytes(8, "big"))
                    final_state.update(payload)
                    actual = np.asarray(document.embedding, dtype=np.float64)
                    expected_vector = canonical_vectors[document.id]
                    maximum_vector_error = max(
                        maximum_vector_error, float(np.max(np.abs(actual - expected_vector))),
                    )
                    maximum_norm_error = max(
                        maximum_norm_error, abs(float(np.linalg.norm(actual)) - 1.0),
                    )
            seen += len(result.documents)
            if result.exhausted:
                break
            if not result.next_after_id or result.next_after_id == after or not result.documents:
                raise RuntimeError("public verification cursor did not advance")
            after = result.next_after_id
        if seen != self.plan["rows"]:
            raise RuntimeError("public full-state count mismatch")
        self.emit(
            "full_state_verified", rows=seen, vectors_checked=seen,
            normalized_full_vector_tolerance=1e-6,
            projection_sha256=(final_state.hexdigest()
                               if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
                               else None),
            maximum_vector_error=(maximum_vector_error
                                  if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
                                  else None),
            maximum_norm_error=(maximum_norm_error
                                if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4
                                else None),
        )

    def paired_resource_endpoint(self):
        """Capture one drained, same-owner endpoint around a paired query batch."""
        pid = self.controller.pid
        identity = self.controller._owned_identity
        before_identity = existing.common.linux_process_identity(pid)
        captured = self.controller.stats_snapshot()
        after_identity = existing.common.linux_process_identity(pid)
        try:
            snapshot = captured["snapshot"]
            work = snapshot["work"]
            typed_graph = snapshot["last_opened_index"]["typed_graph"]
            pending = {
                key: int(snapshot.get("collections", {}).get(key, "0"))
                for key in (
                    "treedb.collections.write_domain.pending_docs",
                    "treedb.collections.write_domain.pending_bytes",
                    "treedb.collections.write_domain.pending_root_runs",
                    "treedb.collections.write_domain.pending_indexed_flush_units",
                    "treedb.collections.write_domain.pending_indexed_publication_bytes",
                )
            }
            typed_pending = typed_graph["pending"]
            if (captured["status"] != "captured" or before_identity != identity
                    or after_identity != identity or work["pid"] != pid
                    or work["schema_version"] != "treedb-work-v1" or work["scope"] != "process"
                    or work["origin_kind"] != "go_package_init"
                    or snapshot["last_opened_index"]["name"] != "minima_cohere"
                    or snapshot["last_opened_index"]["generation"] != self.info.generation
                    or any(pending.values())
                    or any(typed_pending[name] != 0 for name in (
                        "rows", "tombstones", "value_slots", "bytes",
                    ))):
                raise RuntimeError("diagnostic endpoint is not one drained owned graph")
            cpu = process_cpu_endpoint(pid, identity)
            if cpu["availability"] != "measured":
                raise RuntimeError("owned server CPU endpoint is unavailable")
            return {
                "availability": "measured",
                "captured_monotonic_ns": captured["captured_monotonic_ns"],
                "pid": pid, "linux_process_identity": identity,
                "generation": self.info.generation,
                "work": work, "server_cpu": cpu,
                "client_cpu_ns": time.process_time_ns(),
                "drained_pending": pending,
                "typed_graph": typed_graph,
                "total_db_bytes_including_wal": existing.common.disk_bytes(self.output / "db"),
            }
        except (KeyError, TypeError, ValueError, RuntimeError) as exc:
            raise RuntimeError(f"paired resource endpoint unavailable: {exc}") from exc

    def normalized_v4_resource_inventory(self, phase):
        if self.plan.get("campaign_profile") != CAMPAIGN_PROFILE_NORMALIZED_V4:
            raise RuntimeError("normalized-v4 resource inventory requested by a legacy campaign")
        endpoint = self.paired_resource_endpoint()
        graph = endpoint["typed_graph"]
        assets = graph.get("vector_assets")
        typed_parts = graph.get("typed_column_parts")
        if (not isinstance(assets, list) or not assets or not isinstance(typed_parts, list)
                or not graph.get("publication_unchanged") or not graph.get("serving_ready")):
            raise RuntimeError("normalized-v4 serving inventory is unavailable")
        inventory = {
            "schema": "treedb_cohere_normalized_v4_resource_inventory/v1",
            "phase": phase, "captured_monotonic_ns": endpoint["captured_monotonic_ns"],
            "pid": endpoint["pid"],
            "linux_process_identity": endpoint["linux_process_identity"],
            "generation": endpoint["generation"],
            "process_memory": linux_process_memory(endpoint["pid"]),
            "go_memory": endpoint["work"].get("memory"),
            "typed_graph": graph,
            "owned_files": owned_db_file_inventory(self.output / "db"),
        }
        if inventory["owned_files"]["total_bytes"] != endpoint["total_db_bytes_including_wal"]:
            raise RuntimeError("normalized-v4 file inventory differs from the owned-directory total")
        self.normalized_v4_resource_inventories.append(inventory)
        self.emit("normalized_v4_resource_inventory", inventory=inventory)
        return inventory

    def write_normalized_v4_resource_inventories(self):
        artifact = {
            "schema": "treedb_cohere_normalized_v4_resource_inventories/v1",
            "rows": self.plan["rows"], "dimensions": 768,
            "query_mode": self.plan.get("query_mode", "exact"),
            "representation": NORMALIZED_REPRESENTATION,
            "inventories": self.normalized_v4_resource_inventories,
        }
        phases = [row["phase"] for row in self.normalized_v4_resource_inventories]
        required = ["initial_ready", "pre_fold", "post_fold", "post_reopen", "final_verified"]
        if phases != required:
            raise RuntimeError(f"normalized-v4 resource inventory phases={phases} want {required}")
        path = self.output / "normalized_v4_resource_inventories.json"
        raw = canonical(artifact)
        with path.open("xb") as output:
            output.write(raw)
        self.emit("normalized_v4_resource_inventories", path=str(path), sha256=bytes_digest(raw), bytes=len(raw))
        return artifact

    def paired_query_batch(self, batch_kind, repetition, ledger):
        timing = self.plan["paired_timing"]
        queries = timing["queries"]
        arm_order = paired_batch_arm_order(batch_kind, repetition)
        if arm_order is None:
            raise RuntimeError("paired query batch identity is invalid")
        arms, common_owner = {}, None
        for mode in arm_order:
            before = self.paired_resource_endpoint() if batch_kind == "measured" else None
            records = []
            phase = paired_phase(batch_kind, repetition, mode)
            for query in queries:
                prior = len(ledger)
                self.search(
                    phase, self.plan["rows"],
                    self.selected_efs[str(self.plan["rows"])], query,
                    request_mode=mode, request_ledger=ledger,
                )
                if len(ledger) != prior + 1:
                    raise RuntimeError("paired query did not produce exactly one call record")
                record = ledger[-1]
                owner = paired_snapshot_from_record(
                    record, mode, phase, query,
                    self.selected_efs[str(self.plan["rows"])], self.plan["rows"],
                )
                if owner is None or (common_owner is not None and not same_json(owner, common_owner)):
                    raise RuntimeError("paired exact/SQ8 calls did not bind one immutable graph owner")
                common_owner = owner
                records.append(record)
            after = self.paired_resource_endpoint() if batch_kind == "measured" else None
            arm = {"mode": mode, "queries": list(queries), "requests": records}
            if before is not None and after is not None:
                arm["resources"] = {
                    "before": before, "after": after,
                    "delta": paired_resource_delta(before, after, records),
                }
            arms[mode] = arm
        return {
            "batch_kind": batch_kind, "repetition": repetition,
            "queries": list(queries), "arm_order": arm_order,
            "common_owner": common_owner, "arms": arms,
        }

    def paired_query_timing(self):
        if self.plan.get("query_mode") != "quantized_rerank" or self.plan.get("rss_only"):
            raise RuntimeError("paired query timing requires the full SQ8 diagnostic")
        timing = self.plan.get("paired_timing")
        if not same_json(timing, paired_timing_plan(self.plan["queries"])):
            raise RuntimeError("paired timing plan is not the frozen contract")
        selected = self.selected_efs.get(str(self.plan["rows"]))
        if type(selected) is not int or selected not in self.plan["rss_controls"]:
            raise RuntimeError("paired timing requires a frozen all-rows coordinate")
        ledger = []
        warmup = self.paired_query_batch("warmup", -1, ledger)
        repetitions = [
            self.paired_query_batch("measured", repetition, ledger)
            for repetition in range(timing["measured_repetitions"])
        ]
        final_mode = repetitions[-1]["arm_order"][-1]
        endpoint = repetitions[-1]["arms"][final_mode]["resources"]["after"]
        typed_graph = endpoint["typed_graph"]
        artifact = {
            "schema": PAIRED_QUERY_ARTIFACT_SCHEMA, "status": "complete",
            "collection": "minima_cohere", "rows": self.plan["rows"],
            "dimensions": 768, "top_k": 10, "eligible": self.plan["rows"],
            "representation_arm": self.plan["representation_arm"],
            "timing_plan": timing,
            "selected_coordinate": {"ef_search": selected, "rerank_candidates": selected},
            "warmup": warmup, "repetitions": repetitions,
            "storage_attribution": {
                "total_db_bytes_including_wal": endpoint["total_db_bytes_including_wal"],
                "wal_boundary": "included_in_total_db_bytes; no subtraction",
                "aggregate_typed_graph_asset_bytes": {
                    name: typed_graph[name] for name in (
                        "installed_asset_bytes", "base_asset_bytes", "owner_asset_bytes",
                    )
                },
                "logical_sq8_code_bytes_per_vector": 768,
                "logical_sq8_code_bytes": self.plan["rows"] * 768,
                "actual_quantized_tvis_bytes": None,
                "actual_quantized_tvis_bytes_availability": "producer_unavailable",
                "actual_quantized_tvis_bytes_producer": (
                    "internal VectorIndexSearchStats exposes QuantizedAssetMappedBytes and "
                    "QuantizedAssetHeapCopyBytes, but the public DenseSearchWork/score_plane transport "
                    "does not carry them; diagnostics typed_graph exposes only aggregate graph assets"
                ),
            },
        }
        if not paired_query_artifact_valid(artifact, self.plan):
            raise RuntimeError("paired query artifact failed its independent contract validator")
        self.paired_query_artifact = artifact
        self.emit("paired_query_timing", artifact=artifact)
        return artifact

    def normalized_v4_production_matrix(self):
        if (self.plan.get("campaign_profile") != CAMPAIGN_PROFILE_NORMALIZED_V4
                or self.plan.get("query_mode") != "quantized_rerank"
                or self.plan["rows"] < 5000):
            raise RuntimeError("normalized-v4 matrix requires the admitted SQ8 campaign database")
        timing = self.plan["production_timing"]
        if not same_json(timing, {
                "schema": "treedb_cohere_v4_production_timing/v1",
                "queries": list(range(self.plan["queries"])),
                "warmup_queries_per_arm": min(20, self.plan["queries"]),
                "measured_repetitions": 6,
                "arm_order": "alternate_first_complete_arm_batch_by_repetition",
                "diagnostics": False, "return_embedding": False}):
            raise RuntimeError("normalized-v4 production timing differs from the frozen plan")
        helper = Path(self.plan["go_helper"])
        if digest(helper) != self.plan["go_helper_sha256"]:
            raise RuntimeError("normalized-v4 Go helper changed after plan freeze")
        queries = [row.tolist() for row in self.queries]
        live_before = self.paired_resource_endpoint()
        go_native = v4_gate.run_go_helper(helper, [
            "-lane=native", f"-address={self.plan['native_address']}",
            f"-dataset={self.plan['dataset']}", "-index=minima_cohere",
            f"-generation={self.info.generation}",
            f"-quantized-index={QUANTIZED_PROFILE_NAME}",
        ])
        python_native = v4_gate.measure_python(
            self.clients.native, self.info, queries, index="minima_cohere",
            top_k=10, ef_search=NORMALIZED_EF_SEARCH,
            rerank_candidates=NORMALIZED_RERANK_CANDIDATES,
            quantized_index=QUANTIZED_PROFILE_NAME, repetitions=6,
            warmup_queries=min(20, len(queries)),
        )
        profiles = v4_gate.capture_profile(
            self.controller, self.clients.native, self.info, queries,
            self.output / "profiles", index="minima_cohere", top_k=10,
            ef_search=NORMALIZED_EF_SEARCH,
            rerank_candidates=NORMALIZED_RERANK_CANDIDATES,
            quantized_index=QUANTIZED_PROFILE_NAME,
        )
        profile_analysis = v4_gate.pprof_top(
            self.plan["go_tool"], Path(self.plan["service_bin"]), profiles, self.output,
        )
        live_after = self.paired_resource_endpoint()
        self.emit("production_matrix_owner_transition", state="service_stop_for_exclusive_go_seams",
                  linux_process_identity=self.controller._owned_identity)
        self.clients.close()
        seams = v4_gate.run_go_helper(helper, [
            "-lane=seams", f"-dir={self.output / 'db'}",
            f"-dataset={self.plan['dataset']}", f"-serving={self.plan['serving_path']}",
            "-index=minima_cohere", f"-quantized-index={QUANTIZED_PROFILE_NAME}",
        ])
        seam_records = seams.get("sub_lanes")
        if not isinstance(seam_records, list) or len(seam_records) != 3 or any(
                not isinstance(lane, dict) for lane in seam_records):
            raise RuntimeError("normalized-v4 Go seam inventory is malformed")
        by_name = {lane.get("lane"): lane for lane in seam_records}
        required = {"collection_search", "collection_fetch", "service"}
        if set(by_name) != required:
            raise RuntimeError("normalized-v4 Go seam inventory is incomplete")
        engine = self.normalized_v4_engine_diagnostic()
        self.controller.start()
        self.ensure()
        self.optimize("ensure")
        self.emit("production_matrix_owner_transition", state="service_restarted_and_readmitted",
                  linux_process_identity=self.controller._owned_identity)
        lanes = {
            "go_native": go_native, "python_native": python_native,
            "collection_search": by_name["collection_search"],
            "collection_fetch": by_name["collection_fetch"],
            "service": by_name["service"],
        }
        owner_identity = v4_gate.immutable_owner_identity(lanes)
        evaluation, failures = v4_gate.evaluate({
            "go_native": go_native, "python_native": python_native,
            "collection": by_name["collection_fetch"], "service": by_name["service"],
        })
        artifact = {
            "schema": "treedb_cohere_normalized_v4_production_matrix/v1",
            "rows": self.plan["rows"], "dimensions": 768,
            "query_order": timing["queries"], "top_k": 10,
            "ef_search": NORMALIZED_EF_SEARCH,
            "rerank_candidates": NORMALIZED_RERANK_CANDIDATES,
            "representation": NORMALIZED_REPRESENTATION,
            "diagnostics": False, "return_embedding": False,
            "live_resource_before": live_before, "live_resource_after": live_after,
            "lanes": lanes, "immutable_owner_identity": owner_identity,
            "producer_evaluation": evaluation,
            "producer_failures": failures,
            "profile_capture": profiles, "profile_analysis": profile_analysis,
            "engine_diagnostic": engine,
        }
        path = self.output / "normalized_v4_production_matrix.json"
        raw = canonical(artifact)
        with path.open("xb") as output:
            output.write(raw)
        self.emit("normalized_v4_production_matrix", path=str(path), sha256=bytes_digest(raw),
                  bytes=len(raw), lane_names=sorted(lanes))
        return artifact

    def normalized_v4_engine_diagnostic(self):
        """Measure candidate and packed stages on the stopped campaign owner."""
        output = self.output / "normalized_v4_engine_diagnostic.json"
        stdout = self.output / "normalized_v4_engine_diagnostic.stdout"
        stderr = self.output / "normalized_v4_engine_diagnostic.stderr"
        command = [
            self.plan["go_tool"], "test",
            "-ldflags", (
                "-X=github.com/snissn/gomap/TreeDB/collections."
                "cosineNormalizedF32CampaignLinkedCommit=" + self.plan["product_commit"]
            ),
            "./TreeDB/collections",
            "-run", "^TestCosineNormalizedF32V1CampaignEngineDiagnostic$",
            "-count=1", "-timeout=30m",
        ]
        environment = os.environ.copy()
        environment.update({
            "TREEDB_NORMALIZED_V4_DIAGNOSTIC_DB": str(self.output / "db"),
            "TREEDB_NORMALIZED_V4_DIAGNOSTIC_DATA": self.plan["dataset"],
            "TREEDB_NORMALIZED_V4_DIAGNOSTIC_SERVING": self.plan["serving_path"],
            "TREEDB_NORMALIZED_V4_DIAGNOSTIC_OUTPUT": str(output),
            "TREEDB_NORMALIZED_V4_SOURCE_COMMIT": self.plan["product_commit"],
            "TREEDB_NORMALIZED_V4_INDEX": "minima_cohere",
            "TREEDB_NORMALIZED_V4_QUANTIZED_INDEX": QUANTIZED_PROFILE_NAME,
            "TREEDB_NORMALIZED_V4_ROWS": str(self.plan["rows"]),
        })
        completed = subprocess.run(
            command, cwd=Path(__file__).resolve().parents[2], env=environment,
            text=True, capture_output=True, timeout=1900, check=False,
        )
        stdout.write_text(completed.stdout, encoding="utf-8")
        stderr.write_text(completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            raise RuntimeError(
                "normalized-v4 campaign engine diagnostic failed: "
                + completed.stderr.strip()
            )
        artifact, raw = strict_json_object(
            output, "normalized-v4 engine diagnostic", EVIDENCE_JSON_MAX_BYTES,
        )
        expected = {
            "schema": "treedb_cosine_normalized_f32_campaign_engine/v1",
            "source_commit": self.plan["product_commit"],
            "rows": self.plan["rows"], "dimensions": 768,
            "query_count": self.plan["queries"], "top_k": 10,
            "ef_search": NORMALIZED_EF_SEARCH,
            "rerank_candidates": NORMALIZED_RERANK_CANDIDATES,
            "representation": NORMALIZED_REPRESENTATION,
            "index": "minima_cohere", "quantized_index": QUANTIZED_PROFILE_NAME,
            "collection_generation": self.info.generation,
        }
        if any(artifact.get(key) != value for key, value in expected.items()):
            raise RuntimeError("normalized-v4 engine diagnostic identity differs from the campaign")
        if (artifact.get("dataset_manifest_sha256") != self.plan["dataset_manifest_sha256"]
                or artifact.get("queries_sha256") != self.plan["dataset_files_sha256"]["queries"]
                or artifact.get("serving_sha256") != self.plan["serving_sha256"]
                or len(artifact.get("shortlists", [])) != self.plan["queries"]
                or len(artifact.get("candidate_only", [])) != 6
                or len(artifact.get("packed_same_shortlist", [])) != 6
                or artifact.get("stable_duplicate_score_calls") != 0):
            raise RuntimeError("normalized-v4 engine diagnostic is incomplete")
        result = {
            "path": str(output), "sha256": bytes_digest(raw), "bytes": len(raw),
            "command": command,
            "stdout": {"path": str(stdout), "sha256": digest(stdout)},
            "stderr": {"path": str(stderr), "sha256": digest(stderr)},
            "artifact": artifact,
        }
        self.emit("normalized_v4_engine_diagnostic", **result)
        return result

    def rss_boundary(self):
        truth = self.truth[str(self.plan["rows"])]
        fp32_requests = []

        def search(ef, query):
            response = self.search(
                "rss_quality", self.plan["rows"], ef, query,
                request_ledger=None if self.plan.get("query_mode") == "quantized_rerank"
                else fp32_requests,
            )
            return [document.id for document in response.documents]

        quality = calibrate_ann_control(
            search, truth, self.plan["rss_controls"], self.plan["rss_calibration_queries"],
            self.plan["rss_revalidation_queries"], self.plan["rss_recall_target"],
        )
        quantized = self.plan.get("query_mode") == "quantized_rerank"
        if quantized:
            quality = _coordinate_rows(quality)
        process = self.controller.process
        rss = process_peak_at_boundary(
            process.pid if process else None, self.controller._owned_identity, self.plan["cpu_affinity"],
        )
        storage = {
            "availability": "measured",
            "owned_bytes_at_rss_boundary": existing.common.disk_bytes(self.output / "db"),
            "scope": "backend_owned_directory_at_initial_ready_quality_boundary",
        }
        reasons = []
        if not quality["revalidation"]["passed"]:
            reasons.append("no TreeDB EF passed both fixed recall query sets")
        if rss.get("availability") != "measured":
            reasons.append("TreeDB server VmHWM unavailable or process drifted")
        if not boundary_storage_valid(storage):
            reasons.append("TreeDB initial-ready storage boundary unavailable")
        artifact = {
            "schema": QUANTIZED_RSS_ARTIFACT_SCHEMA if quantized else RSS_ARTIFACT_SCHEMA,
            "state": "calibrated" if not reasons else "uncalibrated",
            "backend": "treedb", "comparison_contract": rss_comparison_contract(self.plan),
            "construction_calibration_contract": self.plan["construction_calibration_contract"],
            "quality": {**quality, "control_name": "ef_search", "exact_mode": False},
            "rss": rss, "storage": storage, "reasons": reasons,
            "readiness": {"graph_action": "build", "successful_ann_queries": sum(
                len(row["per_query"]) for row in quality["calibration"]["curve"]
            ) + sum(len(row["per_query"]) for row in quality["revalidation"]["curve"]),
                "effective_index": {"m": self.info.vector_m, "ef_construction": self.info.vector_ef_construction},
                "column_graph_build": asdict(self.initial_graph_build.status.column_graph_build),
            },
            "provenance": {key: self.plan[key] for key in (
                "harness_commit", "harness_source_sha256", "harness_trees", "product_commit",
                "product_trees", "service_sha256", "dataset_manifest_sha256", "dataset_files_sha256",
                "serving_sha256",
            )},
        }
        if quantized:
            artifact["representation_arm"] = quantized_representation_arm()
            artifact["observed_execution"] = {
                "schema": "treedb_cohere_sq8_execution/v1",
                "native_command_version": 3,
                # Freeze this strict selection ledger before any later full-run
                # diagnostic call can append to a different evidence packet.
                "requests": json.loads(canonical(self.quantized_requests)),
            }
            artifact["readiness"]["effective_index"] = self.info.to_dict()
        else:
            artifact["observed_quality"] = {
                "schema": "cohere_500k_768d_quality_observations/v1",
                "control_name": "ef_search",
                "requests": [
                    {
                        "request_sequence": record["request_sequence"],
                        "control": record["requested_ef_search"],
                        "query": record["query"],
                        "ids": [result["id"] for result in record["results"]],
                    }
                    for record in fp32_requests
                ],
                "exact_reference": None,
            }
        return artifact

    def write_normalized_v4_truth(self):
        if self.plan.get("campaign_profile") != CAMPAIGN_PROFILE_NORMALIZED_V4:
            raise RuntimeError("normalized-v4 truth is outside the legacy campaign")
        artifact = {
            "schema": "treedb_cohere_normalized_v4_truth/v1",
            "rows": self.plan["rows"], "dimensions": 768,
            "query_order": list(range(self.plan["queries"])),
            "eligible_counts": self.plan["eligible_counts"],
            "canonical_normalized_f32_dot": {
                "ordered_ids": self.truth, "ordered_scores": self.normalized_truth_scores,
            },
            "original_fp32_cosine": {"ordered_ids": self.original_cosine_truth},
            "source_truth_sha256": self.plan["dataset_files_sha256"]["truth"],
        }
        path = self.output / "normalized_v4_truth.json"
        raw = canonical(artifact)
        with path.open("xb") as output:
            output.write(raw)
        self.emit("normalized_v4_truth", path=str(path), sha256=bytes_digest(raw), bytes=len(raw))
        return artifact

    def execute(self):
        self.started = time.monotonic()
        monitor = threading.Thread(target=self.guard, daemon=True)
        self.emit("plan", plan=self.plan)
        monitor.start()
        failed, rss_artifact, qualification = None, None, "not_evaluated"
        try:
            self.timed("service_start", self.controller.start)
            self.timed("schema_ensure", self.ensure)
            for start in range(0, self.plan["rows"], 256):
                self.upsert(list(range(start, min(start + 256, self.plan["rows"]))), "initial_durable_ingest")
            self.initial_graph_build = self.timed("initial_graph_build", lambda: self.optimize("build"))
            decisions = self.initial_graph_build.status.column_graph_build.construction_decisions
            if (decisions is not None) != self.plan["construction_decisions"]:
                raise RuntimeError("construction decision observer response differs from frozen mode")
            if self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
                self.write_normalized_v4_truth()
            if self.plan["rss_only"]:
                rss_artifact = self.rss_boundary()
                if rss_artifact["reasons"]:
                    raise RuntimeError("; ".join(rss_artifact["reasons"]))
            elif self.plan.get("campaign_profile") == CAMPAIGN_PROFILE_NORMALIZED_V4:
                quantized = self.plan.get("query_mode") == "quantized_rerank"
                self.selected_efs = {
                    str(eligible): NORMALIZED_EF_SEARCH
                    for eligible in self.plan["eligible_counts"]
                }
                self.normalized_v4_resource_inventory("initial_ready")
                if quantized and self.plan["rows"] >= 5000:
                    self.normalized_v4_production_matrix()
                modes = ("exact", "quantized_rerank") if quantized else ("exact",)
                for mode in modes:
                    for eligible in self.plan["eligible_counts"]:
                        for query in range(self.plan["queries"]):
                            self.search("fixed_coordinate_curve", eligible, NORMALIZED_EF_SEARCH,
                                        query, request_mode=mode)
                self.overlap()
                self.lifecycle()
                self.normalized_v4_resource_inventory("pre_fold")
                self.timed("pre_close_fold", lambda: self.optimize("fold"))
                self.normalized_v4_resource_inventory("post_fold")
                self.timed("close", self.clients.close)
                expected_before_reopen = 2 if quantized and self.plan["rows"] >= 5000 else 1
                validate_shutdowns(self.controller.lifetimes, expected_before_reopen)
                self.timed("reopen", self.controller.start)
                self.timed("idempotent_ensure", self.ensure)
                self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
                self.normalized_v4_resource_inventory("post_reopen")
                for mode in modes:
                    for eligible in self.plan["eligible_counts"]:
                        for query in range(self.plan["queries"]):
                            self.search("post_reopen_curve", eligible, NORMALIZED_EF_SEARCH,
                                        query, request_mode=mode)
                self.timed("verification_only_full_scroll", self.scroll)
                self.normalized_v4_resource_inventory("final_verified")
                self.write_normalized_v4_resource_inventories()
                qualification = "producer_gates_passed"
            elif self.plan.get("query_mode") == "quantized_rerank":
                cold_populations = set()

                def quality_search(eligible, ef, query):
                    phase = "predicate_first_quality_query" if eligible not in cold_populations else "quality_selection"
                    cold_populations.add(eligible)
                    return [doc.id for doc in self.search(phase, eligible, ef, query).documents]

                if self.plan["rows"] == 500000:
                    all_rows = str(self.plan["rows"])
                    coordinate_lock = self.plan["all_rows_coordinate_lock"]
                    self.emit("coordinate_lock_consumed", **coordinate_lock_consumption_event(
                        coordinate_lock, self.controller._owned_identity,
                    ))
                    self.quantized_quality[all_rows] = evaluate_locked_quantized_control(
                        lambda control, query: [doc.id for doc in self.search(
                            "locked_all_rows_revalidation", self.plan["rows"], control, query,
                        ).documents],
                        self.truth[all_rows], coordinate_lock,
                        self.plan["rss_calibration_queries"], self.plan["rss_revalidation_queries"],
                        self.plan["rss_recall_target"],
                    )
                    self.emit("locked_all_rows_quality", **self.quantized_quality[all_rows])
                    if not self.quantized_quality[all_rows]["passed"]:
                        self.emit("quantized_quality", cohorts=self.quantized_quality)
                        raise QualityUnqualified(
                            "quantized fixed-set quality failed for cohorts " + all_rows,
                        )
                    self.quantized_quality.update(select_quantized_cohorts(
                        quality_search, self.truth,
                        [eligible for eligible in self.plan["eligible_counts"] if eligible != self.plan["rows"]],
                        self.plan["rows"], self.plan["rss_controls"],
                        self.plan["rss_calibration_queries"], self.plan["rss_revalidation_queries"],
                        self.plan["rss_recall_target"],
                    ))
                else:
                    smoke_queries = list(range(self.plan["queries"]))
                    self.quantized_quality = select_quantized_cohorts(
                        quality_search,
                        self.truth, self.plan["eligible_counts"], self.plan["rows"], self.plan["rss_controls"],
                        smoke_queries[:2], smoke_queries[2:], 1.0,
                    )
                failed_cohorts = [
                    eligible for eligible, row in self.quantized_quality.items()
                    if ((row.get("coordinate") if "coordinate" in row else row.get("selected_coordinate")) is None
                        or (row.get("passed") if "passed" in row
                            else row.get("revalidation", {}).get("passed")) is not True)
                ]
                self.emit("quantized_quality", cohorts=self.quantized_quality)
                if failed_cohorts:
                    raise QualityUnqualified(
                        "quantized fixed-set quality failed for cohorts " + ",".join(failed_cohorts))
                self.selected_efs = {
                    eligible: (row.get("coordinate") or row["selected_coordinate"])["ef_search"]
                    for eligible, row in self.quantized_quality.items()
                }
                # Selection is now frozen. Later diagnostic/lifecycle calls only reuse these coordinates.
                self.paired_query_timing()
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("fixed_coordinate_curve", eligible, self.selected_efs[str(eligible)], query)
                self.overlap()
                self.lifecycle()
                self.timed("pre_close_fold", lambda: self.optimize("fold"))
                self.timed("close", self.clients.close)
                validate_shutdowns(self.controller.lifetimes, 1)
                self.timed("reopen", self.controller.start)
                self.timed("idempotent_ensure", self.ensure)
                self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("post_reopen_curve", eligible, self.selected_efs[str(eligible)], query)
                self.timed("verification_only_full_scroll", self.scroll)
                qualification = "producer_gates_passed"
            else:
                # Cache-cold means first request for this predicate, not OS/disk-cold.
                for eligible in self.plan["eligible_counts"]:
                    self.search("predicate_first_query", eligible, 512, 0)
                    for ef in self.plan["efs"]:
                        for query in range(self.plan["queries"]):
                            self.search("warm_curve", eligible, ef, query)
                self.overlap()
                self.lifecycle()
                self.timed("pre_close_fold", lambda: self.optimize("fold"))
                self.timed("close", self.clients.close)
                validate_shutdowns(self.controller.lifetimes, 1)
                self.timed("reopen", self.controller.start)
                self.timed("idempotent_ensure", self.ensure)
                self.timed("reopen_graph_ensure", lambda: self.optimize("ensure"))
                for eligible in self.plan["eligible_counts"]:
                    for query in range(self.plan["queries"]):
                        self.search("post_reopen_curve", eligible, 512, query)
                self.timed("verification_only_full_scroll", self.scroll)
            if self.failure:
                raise RuntimeError(self.failure)
        except BaseException as exc:
            failed = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, QualityUnqualified):
                qualification = "valid_unqualified"
            if (not isinstance(exc, QualityUnqualified)
                    and self.plan.get("query_mode") == "quantized_rerank"
                    and self.quantized_requests):
                last = self.quantized_requests[-1]
                if last.get("outcome") == "error" and "error" not in last:
                    last["error"] = failed
                self.emit("quantized_failure_evidence", requests=self.quantized_requests)
            self.emit("failure", error=failed)
        finally:
            finalization_failures = []
            try:
                self.clients.close()
                expected_lifetimes = expected_shutdown_lifetimes(
                    self.plan, failed, qualification,
                )
                validate_shutdowns(self.controller.lifetimes, expected_lifetimes)
            except BaseException as exc:
                finalization_failures.append(f"shutdown: {type(exc).__name__}: {exc}")
            self.cancel.set()
            monitor.join(timeout=5)
            if self.failure and self.failure not in (failed or ""):
                finalization_failures.append(self.failure)
            try:
                if monitor.is_alive():
                    raise RuntimeError("resource guard did not stop")
                self.check_resources()
            except BaseException as exc:
                finalization_failures.append(
                    f"final resource guard: {type(exc).__name__}: {exc}",
                )
            if finalization_failures:
                self.emit("finalization_failure", errors=finalization_failures)
                failed = "; ".join(([failed] if failed else []) + finalization_failures)
                qualification = "invalid"
            if rss_artifact is not None:
                rss_artifact = finalize_rss_artifact(rss_artifact, failed)
                self.emit("rss_boundary", artifact=rss_artifact)
            self.emit("terminal", lifecycle_complete=failed is None, qualification=qualification, error=failed,
                      process_lifetimes=self.controller.lifetimes, final_disk_bytes=existing.common.disk_bytes(self.output / "db"))
            self.events.close()
            if rss_artifact is not None:
                (self.output / "rss.json").write_bytes(canonical(rss_artifact))
        return int(failed is not None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--service-bin", required=True, type=Path)
    parser.add_argument("--product-commit", required=True)
    parser.add_argument("--serving", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--rows", type=int, choices=[512, 5000, 500000], default=500000)
    parser.add_argument("--campaign-profile", choices=(CAMPAIGN_PROFILE_LEGACY, CAMPAIGN_PROFILE_NORMALIZED_V4),
                        default=CAMPAIGN_PROFILE_LEGACY)
    parser.add_argument("--rss-only", action="store_true")
    parser.add_argument("--query-mode", choices=("exact", "quantized_rerank"), default="exact")
    parser.add_argument("--quantized-index-name")
    parser.add_argument("--all-rows-sq8-rss-artifact", type=Path,
                        help="prior calibrated SQ8 RSS artifact that owns the 500K all-row coordinate")
    parser.add_argument("--expected-all-rows-sq8-rss-artifact-sha256",
                        help="external SHA-256 pin for --all-rows-sq8-rss-artifact")
    parser.add_argument(
        "--ef-construction", type=int, choices=(32, 64, 96, 128),
        default=DEFAULT_EF_CONSTRUCTION,
    )
    parser.add_argument("--construction-decisions", action="store_true")
    parser.add_argument("--go-helper", type=Path)
    parser.add_argument("--go", default="go")
    parser.add_argument("--url", default="http://127.0.0.1:17420")
    parser.add_argument("--native-address", default="127.0.0.1:17422")
    parser.add_argument("--diagnostics-url", default="http://127.0.0.1:17421")
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        print(write_frozen_json(args.freeze, plan, "frozen Cohere run plan"), args.freeze)
        return 0
    frozen, frozen_raw = strict_json_object(args.run, "frozen Cohere run plan")
    if not args.expected_plan_sha256 or bytes_digest(frozen_raw) != args.expected_plan_sha256:
        raise ValueError("externally pinned plan hash required")
    validate_plan(frozen, plan)
    if shutil.disk_usage(args.run_dir.parent).free < plan["minimum_free_bytes"] + GIB:
        raise RuntimeError("insufficient disk headroom before starting service")
    return Run(frozen).execute()


if __name__ == "__main__":
    raise SystemExit(main())
