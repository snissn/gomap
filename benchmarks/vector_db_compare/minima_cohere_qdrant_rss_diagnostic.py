#!/usr/bin/env python3
"""Matched 500Kx768D Cohere query-ready RSS boundary for Qdrant and TreeDB."""
from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_qdrant_runner as existing

SCHEMA = native.RSS_ARTIFACT_SCHEMA
CONTROLS = native.RSS_CONTROLS

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


def qdrant_point(document):
    return {
        "logical_id": document["id"], "vector": document["embedding"],
        "payload": {"id": document["id"], "content": document["content"], "meta": document["meta"]},
    }


def ready_snapshot(snapshot, rows):
    schema = snapshot.get("payload_schema") or {}
    config = snapshot.get("config") or {}
    hnsw, optimizer, params = (config.get("hnsw_config") or {}, config.get("optimizer_config") or {},
                               config.get("params") or {})
    vector = params.get("vectors") or {}
    exact_int = lambda value, expected: type(value) is int and value == expected
    config_matches = not (any((schema.get(field) or {}).get("data_type") != "keyword"
            for field in ("meta.user_id", "meta.fpath"))
            or not exact_int(hnsw.get("m"), 16) or not exact_int(hnsw.get("ef_construct"), 100)
            or not exact_int(hnsw.get("full_scan_threshold"), 10000) or hnsw.get("on_disk") is not False
            or not exact_int(optimizer.get("indexing_threshold"), 10000)
            or not exact_int(optimizer.get("max_optimization_threads"), 1)
            or params.get("on_disk_payload") is not True or not exact_int(vector.get("size"), 768)
            or str(vector.get("distance", "")).lower() != "cosine" or vector.get("on_disk") is not False)
    return (config_matches and snapshot.get("status") == "green"
            and existing.optimizer_is_ok(snapshot.get("optimizer_status"))
            and exact_int(snapshot.get("points_count"), rows)
            and exact_int(snapshot.get("exact_points_count"), rows)
            and exact_int(snapshot.get("indexed_vectors_count"), rows)
            and all(exact_int((schema.get(field) or {}).get("points"), rows)
                    for field in ("meta.user_id", "meta.fpath")))


def validate_ready_snapshot(snapshot, rows):
    if not ready_snapshot(snapshot, rows):
        raise RuntimeError("Qdrant did not reach the matched query-ready boundary")


def _process_peak_rss_valid(rss):
    try:
        pid = rss["pid"]
        identity = rss["process_identity"]
        identity_pid, start = identity.split(":")
        return (
            rss["availability"] == "measured"
            and type(rss["bytes"]) is int and rss["bytes"] > 0
            and type(pid) is int and pid > 0
            and identity_pid == str(pid) and start == str(int(start)) and int(start) > 0
            and rss["source"] == "/proc/<pid>/status:VmHWM"
            and rss["scope"] == "process_lifetime_through_sample"
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _column_graph_build_valid(build):
    if not isinstance(build, dict) or set(build) != _COLUMN_GRAPH_BUILD_FIELDS:
        return False
    numeric_fields = _COLUMN_GRAPH_BUILD_FIELDS - {"construction_decisions"}
    if any(type(build[field]) is not int or build[field] < 0 for field in numeric_fields):
        return False
    return (
        all(build[field] > 0 for field in _COLUMN_GRAPH_BUILD_POSITIVE_NANOS)
        and build["file_sync_count"] > 0
        and build["namespace_sync_count"] > 0
        and (build["construction_decisions"] is None or isinstance(build["construction_decisions"], dict))
    )


def _sq8_requests_share_initial_owner(requests):
    try:
        snapshots = [row["score_plane"]["snapshot"] for row in requests]
        owner = snapshots[0]
        return (
            all(native.same_json(snapshot, owner) for snapshot in snapshots)
            and native.same_json(owner["base_manifest"], owner["current_manifest"])
            and owner["base_coverage_lsn"] == owner["current_coverage_lsn"]
        )
    except (KeyError, TypeError, IndexError):
        return False


def fp32_quality_valid(quality, control_name):
    common = {
        "selection_protocol", "target_mean_recall_at_10", "selected_control",
        "calibration", "revalidation", "control_name", "exact_mode",
    }
    expected = set(common)
    if control_name == "hnsw_ef":
        expected |= {"exact_correctness_reference_recall_at_10",
                     "exact_correctness_reference_timing"}
    try:
        return (
            control_name in ("ef_search", "hnsw_ef")
            and isinstance(quality, dict) and set(quality) == expected
            and quality["control_name"] == control_name and quality["exact_mode"] is False
            and native.fixed_set_quality_valid(quality)
            and (control_name != "hnsw_ef" or (
                native.same_json(quality["exact_correctness_reference_recall_at_10"], 1.0)
                and quality["exact_correctness_reference_timing"] == "after_rss_boundary"
            ))
        )
    except (KeyError, TypeError, ValueError):
        return False


def compare_artifacts(treedb, qdrant):
    reasons = []
    for artifact, backend in ((treedb, "treedb"), (qdrant, "qdrant")):
        if (artifact.get("schema") != SCHEMA or artifact.get("backend") != backend
                or artifact.get("state") != "calibrated" or artifact.get("reasons") != []):
            reasons.append(f"{backend} artifact is not calibrated")
        control_name = "ef_search" if backend == "treedb" else "hnsw_ef"
        if not fp32_quality_valid(artifact.get("quality"), control_name):
            reasons.append(f"{backend} fixed-set quality missed the target")
        if not native.rss_comparison_contract_valid(artifact.get("comparison_contract")):
            reasons.append(f"{backend} matched RSS contract is invalid")
        rss = artifact.get("rss", {})
        if not _process_peak_rss_valid(rss):
            reasons.append(f"{backend} server VmHWM is unavailable")
    if not native.same_json(treedb.get("comparison_contract"), qdrant.get("comparison_contract")):
        reasons.append("TreeDB and Qdrant comparison contracts differ")
    tree_provenance, qdrant_provenance = treedb.get("provenance", {}), qdrant.get("provenance", {})
    if (not tree_provenance.get("harness_commit")
            or tree_provenance.get("harness_commit") != qdrant_provenance.get("harness_commit")
            or not native.same_json(tree_provenance.get("harness_trees"),
                                    qdrant_provenance.get("harness_trees"))):
        reasons.append("TreeDB and Qdrant harness provenance differs")
    if treedb.get("rss", {}).get("process_identity") == qdrant.get("rss", {}).get("process_identity"):
        reasons.append("backend process identities are not distinct")
    if reasons:
        return {"state": "uncalibrated", "reasons": reasons}
    tree_bytes, qdrant_bytes = treedb["rss"]["bytes"], qdrant["rss"]["bytes"]
    accepted = tree_bytes <= qdrant_bytes
    return {
        "state": "accept" if accepted else "investigate", "treedb_rss_bytes": tree_bytes,
        "qdrant_rss_bytes": qdrant_bytes, "delta_bytes": tree_bytes - qdrant_bytes,
        "treedb_to_qdrant_ratio": tree_bytes / qdrant_bytes,
        "recommendation": ("stop prioritizing TreeDB RSS for this 500K x 768D initial-ready workload"
                           if accepted else "quantify the higher TreeDB owner before redesign"),
    }


def _sq8_request_valid(row):
    try:
        if not _SQ8_SCORE_PLANE_IDENTITY_FIELDS <= set(row["score_plane"]):
            return False
        work = native.dense_contract.DenseSearchWork.from_dict(row["dense_work"])
        proof = native.dense_contract.DenseScorePlaneProof.from_dict(row["score_plane"])
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
                identifier == f"row-{ordinal:06d}"
                and 0 <= ordinal < 500000
                and result["content"] == f"minima-cohere:{ordinal}"
                and result["meta"] == {
                    "user_id": f"{(ordinal * 7919) % 500000:06d}",
                    "fpath": f"/cohere/{ordinal // 256:06d}.txt",
                }
            )
        return (
            type(row["request_sequence"]) is int and row["request_sequence"] > 0
            and row["outcome"] == "success" and not row.get("error") and row["phase"] == "rss_quality"
            and type(row["eligible"]) is int and row["eligible"] == 500000
            and type(row["query"]) is int and 0 <= row["query"] < 200
            and type(row["command_version"]) is int and row["command_version"] == 3
            and type(row["started_monotonic_ns"]) is int
            and type(row["ended_monotonic_ns"]) is int
            and row["ended_monotonic_ns"] > row["started_monotonic_ns"]
            and type(generation) is int and generation > 0
            and type(row["requested_ef_search"]) is int
            and type(row["requested_rerank_candidates"]) is int
            and row["requested_ef_search"] in native.RSS_CONTROLS
            and row["requested_ef_search"] >= 10
            and row["requested_rerank_candidates"] == row["requested_ef_search"]
            and native.same_json(row.get("lifecycle_state"), {
                "owner_advance": 0, "folded": False, "shadow_allowance": 0,
                "live_base": 500000, "live_suffix": 0,
            })
            and proof.version == 1 and proof.available and proof.completed and not proof.reason
            and proof.requested_mode == "quantized_rerank"
            and proof.effective_mode == "quantized_rerank" and proof.route == "quantized_rerank"
            and proof.quantized_index_name == native.QUANTIZED_PROFILE_NAME
            and proof.quantized_codec == "scalar_u8" and proof.quantized_version == 1
            and proof.quantized_config_hash == 0 and proof.requested_top_k == 10
            and proof.requested_ef_search == row["requested_ef_search"]
            and proof.requested_rerank_candidates == row["requested_rerank_candidates"]
            and native.existing.quantized_snapshot_valid(proof.snapshot, generation)
            and work.graph.snapshot == proof.snapshot
            and work.graph.base_candidates == 0
            and work.graph.delta_scored == 0 and work.graph.base_shadowed == 0
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
            and native.dense_contract.dense_score_plane_byte_counters_match(proof, 768)
            and native.dense_contract.dense_quantized_response_work_matches(work, proof, 10, len(results), False)
            and len(results) == 10 and len(ids) == len(set(ids))
            and projections_match
            and all(type(score) in (int, float) and native.math.isfinite(score)
                    and -1.000001 <= score <= 1.000001 for score in scores)
            and type(row["recall"]) in (int, float) and native.math.isfinite(row["recall"])
            and 0 <= row["recall"] <= 1
            and type(row["ndcg_at_10"]) in (int, float) and native.math.isfinite(row["ndcg_at_10"])
            and 0 <= row["ndcg_at_10"] <= 1
            and list(zip((-score for score in scores), ids)) == sorted(zip((-score for score in scores), ids))
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _sq8_effective_index_valid(index, construction, requests):
    try:
        expected_keys = {
            "name", "dimension", "metric", "generation", "contract_version",
            "embedding_field", "vector_index_name", "vector_strategy", "vector_m",
            "vector_ef_construction", "vector_ef_search", "quantized_indexes",
            "scalar_fields", "text_field", "text_index_name", "document_type",
            "capabilities", "typed_input",
        }
        expected_capabilities = native.existing.QUANTIZED_INDEX_CAPABILITIES
        quantized = index["quantized_indexes"]
        scalar = index["scalar_fields"]
        generations = {row["expected_generation"] for row in requests}
        return (
            isinstance(index, dict) and set(index) == expected_keys
            and index["name"] == "minima_cohere" and type(index["dimension"]) is int
            and index["dimension"] == 768
            and index["metric"] == "cosine" and index["contract_version"] == native.existing.SERVICE_CONTRACT
            and index["embedding_field"] == "embedding" and index["vector_index_name"] == "embedding"
            and index["vector_strategy"] == "column_graph" and type(index["vector_m"]) is int
            and index["vector_m"] == 16 and type(index["vector_ef_construction"]) is int
            and index["vector_ef_construction"] == construction["ef_construction"]
            and type(index["vector_ef_search"]) is int and index["vector_ef_search"] == 64
            and index["text_field"] == "content" and index["text_index_name"] == "content"
            and index["document_type"] == "treedb_document_service_v1"
            and index["typed_input"] is True
            and native.same_json(index["capabilities"], expected_capabilities)
            and type(index["generation"]) is int and generations == {index["generation"]}
            and native.same_json(quantized, [{
                "name": native.QUANTIZED_PROFILE_NAME, "codec": "scalar_u8", "version": 1,
            }])
            and native.same_json(scalar, [
                {"field": "meta.fpath", "index_name": "meta_fpath", "value_type": "string"},
                {"field": "meta.user_id", "index_name": "meta_user_id", "value_type": "string"},
            ])
        )
    except (KeyError, TypeError, ValueError):
        return False


def sq8_results_match_dataset(sq8, vectors, queries, truth):
    """Recheck retained IDs, quality curves and FP32 scores without copying the corpus."""
    try:
        observed = {}
        for record in sq8["observed_execution"]["requests"]:
            query = record["query"]
            expected_ids = truth[query]
            ids = [result["id"] for result in record["results"]]
            recall = len(set(ids) & set(expected_ids)) / len(expected_ids)
            ndcg = native.binary_ndcg(ids, expected_ids)
            if (not native.math.isclose(record["recall"], recall, rel_tol=0.0, abs_tol=1e-12)
                    or not native.math.isclose(record["ndcg_at_10"], ndcg,
                                               rel_tol=0.0, abs_tol=1e-12)):
                return False
            observed[(record["requested_ef_search"], query)] = (recall, ndcg)
            query_vector = np.asarray(queries[query], dtype=np.float64)
            query_norm = np.linalg.norm(query_vector)
            if not native.math.isfinite(query_norm) or query_norm <= 0:
                return False
            for result in record["results"]:
                row = int(result["id"].removeprefix("row-"))
                vector = np.asarray(vectors[row], dtype=np.float64)
                vector_norm = np.linalg.norm(vector)
                if not native.math.isfinite(vector_norm) or vector_norm <= 0:
                    return False
                score = np.dot(vector, query_vector) / (vector_norm * query_norm)
                if not native.math.isfinite(score) or abs(score - result["score"]) > 2e-5:
                    return False
        for split in ("calibration", "revalidation"):
            quality_split = sq8["quality"][split]
            for coordinate in quality_split["curve"]:
                control = coordinate["control"]
                expected = [observed[(control, query)] for query in quality_split["queries"]]
                if (not native.same_json(coordinate["per_query"], [row[0] for row in expected])
                        or not native.same_json(coordinate["per_query_ndcg_at_10"],
                                                [row[1] for row in expected])):
                    return False
        return True
    except (KeyError, TypeError, ValueError, IndexError, ZeroDivisionError):
        return False


def _sq8_artifact_reasons(sq8, fp32_treedb, fp32_qdrant):
    reasons = []
    if (sq8.get("schema") != native.QUANTIZED_RSS_ARTIFACT_SCHEMA
            or sq8.get("backend") != "treedb" or sq8.get("state") != "calibrated"
            or sq8.get("reasons") != []):
        reasons.append("TreeDB SQ8 artifact is not calibrated")
    quality = sq8.get("quality", {})
    if (not native.quantized_quality_valid(quality)
            or quality.get("control_name") != "ef_search" or quality.get("exact_mode") is not False):
        reasons.append("TreeDB SQ8 fixed-set quality missed the target")
    rss = sq8.get("rss", {})
    if not _process_peak_rss_valid(rss):
        reasons.append("TreeDB SQ8 server VmHWM is unavailable")
    if (not native.rss_comparison_contract_valid(sq8.get("comparison_contract"))
            or not native.same_json(sq8.get("comparison_contract"), fp32_treedb.get("comparison_contract")) \
            or not native.same_json(sq8.get("comparison_contract"), fp32_qdrant.get("comparison_contract"))):
        reasons.append("TreeDB SQ8 workload/RSS contract differs from the FP32 arms")
    if not native.same_json(sq8.get("representation_arm"), native.quantized_representation_arm()):
        reasons.append("TreeDB SQ8 representation declaration differs from minima_sq8")
    provenance = sq8.get("provenance", {})
    fp32_provenance = fp32_treedb.get("provenance", {})
    common_provenance = (
        "harness_commit", "harness_source_sha256", "harness_trees", "product_commit", "product_trees",
        "service_sha256", "dataset_manifest_sha256", "dataset_files_sha256", "serving_sha256",
    )
    if any(not provenance.get(key)
           or not native.same_json(provenance.get(key), fp32_provenance.get(key))
           for key in common_provenance):
        reasons.append("TreeDB SQ8 provenance differs from the FP32 TreeDB arm")
    construction = sq8.get("construction_calibration_contract", {})
    construction_ef = construction.get("ef_construction")
    if (type(construction_ef) is not int or construction_ef not in (32, 64, 96, 128)
            or not native.same_json(
                construction, native.construction_calibration_contract(construction_ef))
            or not native.same_json(construction, fp32_treedb.get("construction_calibration_contract"))
            ):
        reasons.append("TreeDB SQ8 construction contract differs from the FP32 TreeDB arm")
    execution = sq8.get("observed_execution", {})
    requests = execution.get("requests", [])
    if (execution.get("schema") != "treedb_cohere_sq8_execution/v1"
            or type(execution.get("native_command_version")) is not int
            or execution.get("native_command_version") != 3
            or not requests or any(not _sq8_request_valid(row) for row in requests)
            or not _sq8_requests_share_initial_owner(requests)):
        reasons.append("TreeDB SQ8 artifact lacks complete per-call native-v3 R=E proof")
    else:
        try:
            quality = sq8["quality"]
            expected = {}
            for split in ("calibration", "revalidation"):
                queries = quality[split]["queries"]
                for coordinate in quality[split]["curve"]:
                    for query, recall, ndcg in zip(
                            queries, coordinate["per_query"], coordinate["per_query_ndcg_at_10"]):
                        expected[(coordinate["control"], query)] = (recall, ndcg)
            observed = {(row["requested_ef_search"], row["query"]):
                        (row["recall"], row["ndcg_at_10"]) for row in requests}
            expected_order = [
                (coordinate["control"], query)
                for coordinate in quality["calibration"]["curve"]
                for query in quality["calibration"]["queries"] + quality["revalidation"]["queries"]
            ]
            matches = ([row["request_sequence"] for row in requests] == list(range(1, len(requests) + 1))
                       and [(row["requested_ef_search"], row["query"]) for row in requests] == expected_order
                       and len(observed) == len(requests) and set(observed) == set(expected)
                       and all(all(native.math.isclose(left, right, rel_tol=0.0, abs_tol=1e-12)
                                   for left, right in zip(observed[key], value))
                               for key, value in expected.items()))
        except (KeyError, TypeError, ValueError):
            matches = False
        if not matches:
            reasons.append("TreeDB SQ8 per-call evidence differs from the fixed-grid quality curves")
    readiness = sq8.get("readiness", {})
    effective_index = readiness.get("effective_index", {})
    if (readiness.get("graph_action") != "build"
            or type(readiness.get("successful_ann_queries")) is not int
            or readiness.get("successful_ann_queries") != len(requests)
            or not _column_graph_build_valid(readiness.get("column_graph_build"))
            or not _sq8_effective_index_valid(effective_index, construction, requests)):
        reasons.append("TreeDB SQ8 effective collection metadata does not bind the request owner")
    if rss.get("process_identity") in {
        fp32_treedb.get("rss", {}).get("process_identity"), fp32_qdrant.get("rss", {}).get("process_identity"),
    }:
        reasons.append("TreeDB SQ8 process identity is not a fresh resource arm")
    return reasons


def compare_three_arms(fp32_treedb, fp32_qdrant, sq8_treedb):
    """Nest the frozen FP32 decision and add a non-promotional representation row."""
    fp32_decision = compare_artifacts(fp32_treedb, fp32_qdrant)
    reasons = _sq8_artifact_reasons(sq8_treedb, fp32_treedb, fp32_qdrant)
    if fp32_decision.get("state") == "uncalibrated":
        reasons.append("the nested FP32 comparison is uncalibrated")
    if reasons:
        sq8_row = {"state": "unavailable", "reasons": reasons}
    else:
        tree_bytes = sq8_treedb["rss"]["bytes"]
        qdrant_bytes = fp32_qdrant["rss"]["bytes"]
        sq8_row = {
            "state": "quality_matched_observation",
            "comparison_kind": "TreeDB_scalar_u8_rerank_vs_Qdrant_FP32",
            "claim_boundary": "different representations; does not revise the nested FP32 decision",
            "treedb_sq8_rss_bytes": tree_bytes,
            "qdrant_fp32_rss_bytes": qdrant_bytes,
            "delta_bytes": tree_bytes - qdrant_bytes,
            "treedb_sq8_to_qdrant_fp32_ratio": tree_bytes / qdrant_bytes,
        }
    return {
        "schema": "treedb_cohere_768_three_arm_comparison/v1",
        "state": "complete" if not reasons else "partial",
        "fp32_treedb_vs_fp32_qdrant": fp32_decision,
        "sq8_treedb_vs_fp32_qdrant": sq8_row,
    }


def comparison_contract(dataset_manifest_sha256, dataset_files_sha256, cpu_affinity):
    return native.rss_comparison_contract({
        "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "dataset_files_sha256": dataset_files_sha256, "cpu_affinity": cpu_affinity,
        "host_memory_bytes": existing.memory_bytes(), "platform": native.platform.platform(),
        "treedb_go_runtime": {key: os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")},
        "host_resource_identity": native.host_resource_identity(),
        "rss_recall_target": native.RSS_RECALL_TARGET,
        "rss_controls": native.RSS_CONTROLS,
        "rss_calibration_queries": native.RSS_CALIBRATION_QUERIES,
        "rss_revalidation_queries": native.RSS_REVALIDATION_QUERIES,
    })


def validate_imports(source):
    for imported in (native, existing):
        if not Path(inspect.getfile(imported)).resolve().is_relative_to(source / "benchmarks/vector_db_compare"):
            raise ValueError("imported RSS dependency is outside the frozen source tree")


def prepare(args):
    source = Path(__file__).resolve().parents[2]
    validate_imports(source)
    harness = native.existing.repository_commit()
    dataset, _, files, query_count = native.dataset_identity(args.dataset, 500000)
    tree, tree_raw = native.strict_json_object(
        args.treedb_artifact, "TreeDB RSS artifact", native.EVIDENCE_JSON_MAX_BYTES,
    )
    binary = args.qdrant_bin.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError("Qdrant binary must be an executable file")
    parsed = urllib.parse.urlparse(args.url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.port is None:
        raise ValueError("Qdrant URL must name an explicit 127.0.0.1 HTTP port")
    if args.storage_path.exists() or args.run_dir.exists():
        raise ValueError("Qdrant storage and run directories must not exist before the owned launch")
    if importlib.metadata.version("qdrant-client") != existing.CLIENT_VERSION:
        raise RuntimeError(f"qdrant-client must be exactly {existing.CLIENT_VERSION}")
    affinity = sorted(os.sched_getaffinity(0))
    root_tree = lambda path: subprocess.check_output(
        ["git", "rev-parse", "HEAD:" + path], cwd=source, text=True,
    ).strip()
    harness_trees = {path: root_tree(path) for path in (
        "benchmarks/vector_db_compare", "clients/python/treedb_client",
    )}
    contract = comparison_contract(native.digest(dataset / "manifest.json"), files, affinity)
    if (tree.get("schema") != SCHEMA or tree.get("backend") != "treedb" or tree.get("state") != "calibrated"
            or tree.get("reasons") != []
            or not fp32_quality_valid(tree.get("quality"), "ef_search")
            or not native.same_json(tree.get("comparison_contract"), contract)
            or not native.rss_comparison_contract_valid(tree.get("comparison_contract"))
            or not _process_peak_rss_valid(tree.get("rss", {}))
            or tree.get("provenance", {}).get("harness_commit") != harness
            or not native.same_json(tree.get("provenance", {}).get("harness_trees"), harness_trees)):
        raise RuntimeError("TreeDB RSS artifact quality, contract, or harness provenance is not current")
    plan = {
        "schema": "treedb_cohere_qdrant_rss_plan/v3", "harness_commit": harness,
        "harness_source_sha256": native.digest(Path(__file__)),
        "harness_trees": harness_trees,
        "dataset": str(dataset), "dataset_manifest_sha256": native.digest(dataset / "manifest.json"),
        "dataset_files_sha256": files, "treedb_artifact": str(args.treedb_artifact.resolve()),
        "treedb_artifact_sha256": native.bytes_digest(tree_raw),
        "comparison_contract": contract, "qdrant_bin": str(binary),
        "qdrant_bin_sha256": native.digest(binary), "qdrant_server_version": existing.SERVER_VERSION,
        "qdrant_client_version": existing.CLIENT_VERSION,
        "storage_path": str(args.storage_path.resolve()), "url": args.url, "collection": args.collection,
        "run_dir": str(args.run_dir.resolve()), "cpu_affinity": affinity,
        "batch_size": 256, "rows": 500000, "queries": query_count, "dimensions": 768, "top_k": 10,
        "controls": CONTROLS, "operation_timeout_s": args.operation_timeout,
        "startup_timeout_s": args.startup_timeout,
        "optimizer_timeout_s": args.optimizer_timeout, "poll_interval_s": args.poll_interval,
        "production_hnsw": existing.PRODUCTION_HNSW_CONFIG,
        "production_optimizers": existing.PRODUCTION_OPTIMIZERS_CONFIG,
        "initial_upload_hnsw": existing.INITIAL_UPLOAD_HNSW_CONFIG,
        "initial_upload_optimizers": existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
    }
    sq8_path = getattr(args, "treedb_sq8_artifact", None)
    if sq8_path is not None:
        sq8, sq8_raw = native.strict_json_object(
            sq8_path, "TreeDB SQ8 artifact", native.EVIDENCE_JSON_MAX_BYTES,
        )
        if _sq8_artifact_reasons(sq8, tree, {
            "comparison_contract": contract, "provenance": tree["provenance"],
            "rss": {"process_identity": "pending-owned-qdrant-process"},
        }):
            raise RuntimeError("TreeDB SQ8 artifact is not a current complete representation arm")
        vectors = np.memmap(dataset / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
        queries = np.memmap(dataset / "queries.f32", mode="r", dtype="<f4", shape=(query_count, 768))
        truth = json.loads((dataset / "truth.json").read_text())["500000"]
        if not sq8_results_match_dataset(sq8, vectors, queries, truth):
            raise RuntimeError("TreeDB SQ8 results differ from frozen IDs, recall, or canonical FP32 scores")
        plan.update(
            schema="treedb_cohere_qdrant_rss_plan/v4_three_arm",
            treedb_sq8_artifact=str(sq8_path.resolve()),
            treedb_sq8_artifact_sha256=native.bytes_digest(sq8_raw),
        )
    return plan


def validate_plan(plan, expected):
    if native.canonical(plan) != native.canonical(expected):
        raise ValueError("frozen Qdrant RSS plan differs from current source/runtime/dataset")


class Run:
    optimization_snapshot = existing.QdrantMinimaRunner.optimization_snapshot
    server_log_snapshot = existing.QdrantMinimaRunner.server_log_snapshot

    def __init__(self, plan, client_factory, models, api_key=""):
        self.plan, self.client_factory, self.models = plan, client_factory, models
        self.api_key = api_key
        self.client = self.process = self.process_identity = self.process_command_identity = None
        self.server_pid = None
        self.server_log = None
        self.output = Path(plan["run_dir"])
        self.output.mkdir(parents=True, exist_ok=False)
        self.collection = plan["collection"]
        self.operation_timeout = plan["operation_timeout_s"]
        self.optimizer_timeout = plan["optimizer_timeout_s"]
        self.poll_interval = plan["poll_interval_s"]
        self.storage_path = Path(plan["storage_path"])
        self.resource_server_name = "Qdrant"
        self.server_log_path = self.output / "qdrant.log"
        self.config = {"scalar_fields": ["meta.user_id", "meta.fpath"]}
        self.readiness_evidence = []
        data = Path(plan["dataset"])
        self.vectors = np.memmap(data / "documents.f32", mode="r", dtype="<f4", shape=(500000, 768))
        self.queries = np.memmap(
            data / "queries.f32", mode="r", dtype="<f4", shape=(plan["queries"], 768),
        )
        self.truth = json.loads((data / "truth.json").read_text())["500000"]

    def wait_ready(self, rows, phase, production=True):
        deadline = time.monotonic() + self.optimizer_timeout
        while True:
            saved_timeout = self.optimizer_timeout
            self.optimizer_timeout = max(0, deadline - time.monotonic())
            try:
                existing.QdrantMinimaRunner.wait_ready(self, expected_count=rows, phase=phase)
            finally:
                self.optimizer_timeout = saved_timeout
            snapshot = self.readiness_evidence[-1]["snapshots"][-1]
            if not production or ready_snapshot(snapshot, rows):
                return snapshot
            if time.monotonic() >= deadline:
                raise TimeoutError("Qdrant full query-ready boundary exceeded optimizer timeout")
            time.sleep(self.poll_interval)

    def start_server(self):
        if self.storage_path.exists():
            raise RuntimeError("owned Qdrant storage path already exists")
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.server_log = (self.output / "qdrant.log").open("xb")
        parsed = urllib.parse.urlparse(self.plan["url"])
        env = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "TZ") if key in os.environ}
        env.update(QDRANT__SERVICE__HOST="127.0.0.1", QDRANT__SERVICE__HTTP_PORT=str(parsed.port),
                   QDRANT__STORAGE__STORAGE_PATH=str(self.storage_path))
        if self.api_key:
            env["QDRANT__SERVICE__API_KEY"] = self.api_key
        self.process = subprocess.Popen([self.plan["qdrant_bin"]], stdin=subprocess.DEVNULL,
                                        stdout=self.server_log, stderr=subprocess.STDOUT,
                                        cwd=self.output, env=env, start_new_session=True)
        self.server_pid = self.process.pid
        deadline, last = time.monotonic() + self.plan["startup_timeout_s"], None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned Qdrant exited during startup with {self.process.returncode}")
            try:
                identity = existing.linux_process_identity(self.server_pid)
                info = existing.server_info(self.plan["url"], self.api_key)
                if (identity and info.get("version") == self.plan["qdrant_server_version"]
                        and Path(f"/proc/{self.server_pid}/exe").resolve() == Path(self.plan["qdrant_bin"])
                        and existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                    self.process_identity = identity
                    self.process_command_identity = existing.server_process_identity(self.server_pid)
                    self.client = self.client_factory()
                    return
            except Exception as exc:
                last = exc
            time.sleep(self.plan["poll_interval_s"])
        raise TimeoutError(f"owned Qdrant startup exceeded timeout: {last}")

    def stop_server(self):
        if self.process is None:
            return
        if self.process.poll() is not None:
            raise RuntimeError(f"owned Qdrant exited before shutdown with {self.process.returncode}")
        if (self.process_identity is not None
                and existing.linux_process_identity(self.server_pid) != self.process_identity):
            raise RuntimeError("owned Qdrant identity changed; refusing to signal")
        self.process.terminate()
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
            raise RuntimeError("owned Qdrant required forced shutdown")
        if self.server_log:
            self.server_log.close()
        if self.process.returncode != 0:
            raise RuntimeError(f"owned Qdrant shutdown exited with {self.process.returncode}")

    def validate_fresh(self):
        identity = existing.linux_process_identity(self.server_pid)
        collections = getattr(self.client.get_collections(), "collections", None)
        collection_dir = self.storage_path / "collections"
        if (identity != self.process_identity or sorted(os.sched_getaffinity(self.server_pid)) != self.plan["cpu_affinity"]
                or collections is None or collections or (collection_dir.exists() and any(collection_dir.iterdir()))):
            raise RuntimeError("Qdrant process/backend is not fresh or changed before load")
        peak = native.process_peak_at_boundary(self.server_pid, identity, self.plan["cpu_affinity"])
        if peak.get("availability") != "measured":
            raise RuntimeError("fresh Qdrant process VmHWM is unavailable")
        return peak

    def create(self):
        self.client.create_collection(
            collection_name=self.collection,
            vectors_config=self.models.VectorParams(size=768, distance=self.models.Distance.COSINE, on_disk=False),
            hnsw_config=self.models.HnswConfigDiff(**self.plan["initial_upload_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["initial_upload_optimizers"]),
            on_disk_payload=True,
            timeout=self.operation_timeout,
        )
        for field in self.config["scalar_fields"]:
            self.client.create_payload_index(collection_name=self.collection, field_name=field,
                field_schema=self.models.PayloadSchemaType.KEYWORD, wait=True, timeout=self.operation_timeout)
        self.wait_ready(0, "fresh_collection", production=False)

    def load(self):
        for start in range(0, self.plan["rows"], self.plan["batch_size"]):
            points = []
            for row in range(start, min(start + self.plan["batch_size"], self.plan["rows"])):
                document = native.make_document(self.vectors, row, self.plan["rows"])
                value = qdrant_point(document)
                points.append(self.models.PointStruct(
                    id=existing.point_id(value["logical_id"]), vector=value["vector"], payload=value["payload"],
                ))
            self.client.upsert(collection_name=self.collection, points=points, wait=True,
                               timeout=self.operation_timeout)
        self.client.update_collection(
            collection_name=self.collection,
            hnsw_config=self.models.HnswConfigDiff(**self.plan["production_hnsw"]),
            optimizers_config=self.models.OptimizersConfigDiff(**self.plan["production_optimizers"]),
            timeout=self.operation_timeout,
        )
        return self.wait_ready(self.plan["rows"], "initial_load_to_ann_ready")

    def search(self, control, query, exact=False):
        response = self.client.query_points(
            collection_name=self.collection, query=self.queries[query].tolist(), limit=10,
            with_payload=True, with_vectors=False,
            search_params=self.models.SearchParams(hnsw_ef=control, exact=exact),
            timeout=self.operation_timeout,
        )
        ids = []
        for point in getattr(response, "points", response):
            payload = getattr(point, "payload", None) or {}
            if (not isinstance(payload.get("id"), str) or not isinstance(payload.get("content"), str)
                    or set((payload.get("meta") or {})) != {"user_id", "fpath"}):
                raise RuntimeError("Qdrant query returned a mismatched logical document")
            try:
                row = int(payload["id"].removeprefix("row-"))
                expected = native.make_document(self.vectors, row, self.plan["rows"])
            except (IndexError, ValueError):
                raise RuntimeError("Qdrant query returned an invalid logical ID") from None
            if payload["id"] != expected["id"] or payload["content"] != expected["content"] or payload["meta"] != expected["meta"]:
                raise RuntimeError("Qdrant query returned mismatched logical payload values")
            ids.append(payload["id"])
        return ids

    def execute(self):
        artifact = None
        tree = None
        try:
            self.start_server()
            baseline = self.validate_fresh()
            tree, tree_raw = native.strict_json_object(
                self.plan["treedb_artifact"], "TreeDB RSS artifact", native.EVIDENCE_JSON_MAX_BYTES,
            )
            if native.bytes_digest(tree_raw) != self.plan["treedb_artifact_sha256"]:
                raise RuntimeError("TreeDB RSS artifact changed after freeze")
            if not native.same_json(tree.get("comparison_contract"), self.plan["comparison_contract"]):
                raise RuntimeError("TreeDB RSS artifact does not match the Qdrant plan")
            self.create()
            readiness = self.load()
            quality = native.calibrate_ann_control(
                lambda control, query: self.search(control, query), self.truth, self.plan["controls"],
                native.RSS_CALIBRATION_QUERIES, native.RSS_REVALIDATION_QUERIES, native.RSS_RECALL_TARGET,
            )
            rss = native.process_peak_at_boundary(
                self.server_pid, self.process_identity, self.plan["cpu_affinity"],
            )
            # Keep the exact correctness reference outside the sampled ANN RSS boundary.
            exact_ids = self.search(self.plan["controls"][-1], 0, exact=True)
            if set(exact_ids) != set(self.truth[0]):
                raise RuntimeError("Qdrant exact correctness reference differs from exhaustive truth")
            reasons = []
            if not quality["revalidation"]["passed"]:
                reasons.append("no Qdrant hnsw_ef passed both fixed recall query sets")
            if rss.get("availability") != "measured":
                reasons.append("Qdrant server VmHWM unavailable or process drifted")
            if (existing.server_process_identity(self.server_pid) != self.process_command_identity
                    or Path(f"/proc/{self.server_pid}/exe").resolve() != Path(self.plan["qdrant_bin"])
                    or not existing.server_process_owns_endpoint(self.server_pid, self.plan["url"])):
                reasons.append("Qdrant binary, command, or listener changed before result finalization")
            artifact = {
                "schema": SCHEMA, "state": "calibrated" if not reasons else "uncalibrated",
                "backend": "qdrant", "comparison_contract": self.plan["comparison_contract"],
                "quality": {**quality, "control_name": "hnsw_ef", "exact_mode": False,
                            "exact_correctness_reference_recall_at_10": 1.0,
                            "exact_correctness_reference_timing": "after_rss_boundary"},
                "rss": rss, "reasons": reasons, "readiness": readiness,
                "fresh_start_peak_rss": baseline,
                "durability_visibility": "Qdrant 1.19.0 upsert wait=true",
                "physical_id_mapping": "uuid5(logical row-*); payload id preserves the matched logical ID",
                "configuration": {key: self.plan[key] for key in (
                    "production_hnsw", "production_optimizers", "initial_upload_hnsw", "initial_upload_optimizers",
                )},
                "provenance": {key: self.plan[key] for key in (
                    "harness_commit", "harness_source_sha256", "harness_trees", "qdrant_bin_sha256",
                    "qdrant_server_version", "qdrant_client_version", "dataset_manifest_sha256",
                    "dataset_files_sha256", "treedb_artifact_sha256",
                )},
            }
            artifact["provenance"].update(process_identity=self.process_identity,
                                          process_command_identity=self.process_command_identity)
            comparison = compare_artifacts(tree, artifact)
        except BaseException as exc:
            reason = f"{type(exc).__name__}: {exc}"
            artifact = artifact or {"schema": SCHEMA, "backend": "qdrant",
                                    "comparison_contract": self.plan["comparison_contract"],
                                    "quality": {"revalidation": {"passed": False}},
                                    "rss": {"availability": "unavailable", "bytes": None,
                                            "process_identity": self.process_identity or ""},
                                    "readiness": self.readiness_evidence[-1] if self.readiness_evidence else {}}
            artifact["state"] = "uncalibrated"
            artifact.setdefault("reasons", []).append(reason)
            comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
        finally:
            try:
                if self.client:
                    self.client.close()
            except BaseException as exc:
                artifact["state"] = "uncalibrated"
                artifact.setdefault("reasons", []).append(f"client close: {type(exc).__name__}: {exc}")
                comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
            try:
                self.stop_server()
            except BaseException as exc:
                artifact["state"] = "uncalibrated"
                artifact.setdefault("reasons", []).append(f"server stop: {type(exc).__name__}: {exc}")
                comparison = {"state": "uncalibrated", "reasons": artifact["reasons"]}
        (self.output / "qdrant-rss.json").write_bytes(native.canonical(artifact))
        (self.output / "comparison.json").write_bytes(native.canonical(comparison))
        three_arm = None
        if "treedb_sq8_artifact" in self.plan:
            try:
                sq8, sq8_raw = native.strict_json_object(
                    self.plan["treedb_sq8_artifact"], "TreeDB SQ8 artifact",
                    native.EVIDENCE_JSON_MAX_BYTES,
                )
                if native.bytes_digest(sq8_raw) != self.plan["treedb_sq8_artifact_sha256"]:
                    raise RuntimeError("TreeDB SQ8 artifact changed after freeze")
                three_arm = compare_three_arms(tree, artifact, sq8)
                three_arm["input_artifact_sha256"] = {
                    "treedb_fp32": self.plan["treedb_artifact_sha256"],
                    "treedb_sq8": self.plan["treedb_sq8_artifact_sha256"],
                    "qdrant_fp32": native.digest(self.output / "qdrant-rss.json"),
                }
            except BaseException as exc:
                three_arm = {
                    "schema": "treedb_cohere_768_three_arm_comparison/v1", "state": "partial",
                    "fp32_treedb_vs_fp32_qdrant": comparison,
                    "sq8_treedb_vs_fp32_qdrant": {
                        "state": "unavailable", "reasons": [f"{type(exc).__name__}: {exc}"],
                    },
                }
            (self.output / "three-arm-comparison.json").write_bytes(native.canonical(three_arm))
        print(json.dumps(comparison, sort_keys=True, allow_nan=False))
        return int(comparison["state"] == "uncalibrated" or (three_arm is not None and three_arm["state"] != "complete"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--freeze", type=Path)
    mode.add_argument("--run", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--treedb-artifact", required=True, type=Path)
    parser.add_argument("--treedb-sq8-artifact", type=Path)
    parser.add_argument("--qdrant-bin", required=True, type=Path)
    parser.add_argument("--storage-path", required=True, type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--api-key", default=os.environ.get("QDRANT_API_KEY", ""))
    parser.add_argument("--collection", default="minima_cohere_rss")
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--operation-timeout", type=int, default=600)
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--optimizer-timeout", type=float, default=2700)
    parser.add_argument("--poll-interval", type=float, default=.25)
    args = parser.parse_args()
    plan = prepare(args)
    if args.freeze:
        print(native.write_frozen_json(args.freeze, plan, "frozen Qdrant RSS plan"), args.freeze)
        return 0
    frozen, frozen_raw = native.strict_json_object(args.run, "frozen Qdrant RSS plan")
    if not args.expected_plan_sha256 or native.bytes_digest(frozen_raw) != args.expected_plan_sha256:
        raise ValueError("externally pinned Qdrant RSS plan hash required")
    validate_plan(frozen, plan)
    from qdrant_client import QdrantClient, models
    factory = lambda: QdrantClient(url=args.url, api_key=args.api_key or None,
                                   timeout=args.operation_timeout, prefer_grpc=False)
    return Run(plan, factory, models, args.api_key).execute()


if __name__ == "__main__":
    raise SystemExit(main())
