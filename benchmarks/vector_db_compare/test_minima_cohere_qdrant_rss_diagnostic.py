"""Fail-closed checks for the matched Cohere 768D RSS boundary."""
import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_cohere_qdrant_rss_diagnostic as qdrant_rss
from test_minima_cohere_native_diagnostic import quantized_response


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: plain(item) for key, item in vars(value).items()}
    return value


def sq8_quality():
    truth = [[f"row-{rank:06d}" for rank in range(10)] for _ in range(200)]
    quality = native._coordinate_rows(native.calibrate_ann_control(
        lambda _control, query: truth[query], truth, [32],
        native.RSS_CALIBRATION_QUERIES, native.RSS_REVALIDATION_QUERIES, native.RSS_RECALL_TARGET,
    ))
    quality.update(control_name="ef_search", exact_mode=False)
    return quality


def fp32_quality(control_name):
    truth = [[f"row-{rank:06d}" for rank in range(10)] for _ in range(200)]
    quality = native.calibrate_ann_control(
        lambda _control, query: truth[query], truth, native.RSS_CONTROLS,
        native.RSS_CALIBRATION_QUERIES, native.RSS_REVALIDATION_QUERIES,
        native.RSS_RECALL_TARGET,
    )
    quality.update(control_name=control_name, exact_mode=False)
    if control_name == "hnsw_ef":
        quality.update(exact_correctness_reference_recall_at_10=1.0,
                       exact_correctness_reference_timing="after_rss_boundary")
    return quality


def tree_provenance():
    return {
        "harness_commit": "a" * 40, "harness_source_sha256": "b" * 64,
        "harness_trees": {"benchmarks": "c" * 40}, "product_commit": "d" * 40,
        "product_trees": {"TreeDB": "e" * 40}, "service_sha256": "f" * 64,
        "dataset_manifest_sha256": "1" * 64,
        "dataset_files_sha256": {"documents": "2" * 64}, "serving_sha256": "3" * 64,
    }


def rss_contract():
    return native.rss_comparison_contract({
        "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
        "dataset_manifest_sha256": "4" * 64,
        "dataset_files_sha256": {
            "documents": "5" * 64, "queries": "6" * 64, "truth": "7" * 64,
        },
        "cpu_affinity": [0, 1], "host_memory_bytes": 1 << 30,
        "treedb_go_runtime": {"GOMAXPROCS": "2", "GOGC": "100", "GOMEMLIMIT": ""},
        "host_resource_identity": {
            "machine_id": "machine", "boot_id": "boot", "page_size_bytes": 4096,
            "numa_mems": "0", "cgroup_membership": "0::/", "cgroup_limits": {},
            "cpu_model": "test CPU", "cpu_features": ["avx2", "sse2"],
        },
        "platform": "test-platform", "rss_recall_target": native.RSS_RECALL_TARGET,
        "rss_controls": native.RSS_CONTROLS,
        "rss_calibration_queries": native.RSS_CALIBRATION_QUERIES,
        "rss_revalidation_queries": native.RSS_REVALIDATION_QUERIES,
    })


def construction_contract():
    return native.construction_calibration_contract(128)


def rss_sample(pid, rss_bytes):
    return {
        "availability": "measured", "bytes": rss_bytes, "pid": pid,
        "process_identity": f"{pid}:{pid * 10}",
        "source": "/proc/<pid>/status:VmHWM", "scope": "process_lifetime_through_sample",
    }


def storage_sample(size=4096):
    return {
        "availability": "measured", "owned_bytes_at_rss_boundary": size,
        "scope": "backend_owned_directory_at_initial_ready_quality_boundary",
    }


def qdrant_guard():
    sample = {
        "elapsed_s": 1.0, "free_bytes": 20 << 30, "owned_bytes": 4096,
        "combined_current_rss_bytes": 8192,
        "harness": {"pid": 1, "process_identity": "1:10", "current_rss_bytes": 4096},
        "server": {"pid": 2, "process_identity": "2:20", "current_rss_bytes": 4096},
    }
    cleanup = copy.deepcopy(sample)
    cleanup.update(elapsed_s=2.0, combined_current_rss_bytes=4096, server=None)
    first = copy.deepcopy(cleanup)
    first["elapsed_s"] = 0.0
    return {
        "schema": "treedb_cohere_qdrant_resource_guard/v1", "status": "passed",
        "scope": "harness_start_through_owned_process_cleanup",
        "sample_count": 3, "max_combined_current_rss_bytes": 8192,
        "max_owned_bytes": 4096, "minimum_free_bytes_observed": 20 << 30,
        "first_sample": first, "last_sample": sample,
        "cleanup_sample": cleanup,
        "failure": None, "limits": copy.deepcopy(qdrant_rss.RESOURCE_GUARD),
    }


def qdrant_cleanup():
    return {
        "schema": "treedb_owned_qdrant_cleanup/v1", "status": "clean",
        "process_identity": "2:20", "harness_pgid": 10, "server_pgid": 10,
        "term_sent": True, "kill_sent": False, "exit_code": 0,
        "client_closed": True, "log_closed": True, "failure": None,
    }


def sq8_build_evidence():
    fields = native._COLUMN_GRAPH_BUILD_FIELDS - {"construction_decisions"}
    return {**{field: 1 for field in fields}, "construction_decisions": None}


def sq8_effective_index():
    return {
        "name": "minima_cohere", "dimension": 768, "metric": "cosine", "generation": 7,
        "contract_version": native.existing.SERVICE_CONTRACT,
        "embedding_field": "embedding", "vector_index_name": "embedding",
        "vector_strategy": "column_graph", "vector_m": 16, "vector_ef_construction": 128,
        "text_field": "content", "text_index_name": "content",
        "document_type": "treedb_document_service_v1", "typed_input": True,
        "vector_ef_search": 64,
        "capabilities": {
            "dense_vector_search": True, "exact_dense_scoring": False,
            "metadata_filters": True, "keyword_search": True, "hybrid_search": True,
            "keyword_metadata_filters": True, "hybrid_metadata_filters": True,
            "benchmark_lifecycle": True, "vector_index_maintenance": True,
            "no_document_vector_search": True, "column_graph_vector_search": True,
            "exact_column_graph_search": True, "quantized_vector_search": True,
            "quantized_rerank": True, "scalar_u8_quantized_rerank": True,
            "typed_dense_quantized_rerank": True, "rabitq_1bit_experimental": False,
        },
        "quantized_indexes": [{"name": "minima_sq8", "codec": "scalar_u8", "version": 1}],
        "scalar_fields": [
            {"field": "meta.fpath", "index_name": "meta_fpath", "value_type": "string"},
            {"field": "meta.user_id", "index_name": "meta_user_id", "value_type": "string"},
        ],
    }


def sq8_request(query=0):
    response = quantized_response(500000, 32)
    return {
        "request_sequence": query + 1, "phase": "rss_quality", "eligible": 500000, "query": query,
        "requested_ef_search": 32, "requested_rerank_candidates": 32,
        "command_version": 3, "expected_generation": 7,
        "started_monotonic_ns": query * 2 + 1, "ended_monotonic_ns": query * 2 + 2,
        "duration_ns": 1,
        "outcome": "success", "recall": 1.0, "ndcg_at_10": 1.0,
        "lifecycle_state": {"owner_advance": 0, "folded": False, "shadow_allowance": 0,
                            "live_base": 500000, "live_suffix": 0},
        "dense_work": plain(response.dense_work), "score_plane": plain(response.score_plane),
        "results": [{"id": f"row-{row:06d}", "content": f"minima-cohere:{row}",
                     "meta": {"user_id": f"{(row * 7919) % 500000:06d}",
                              "fpath": f"/cohere/{row // 256:06d}.txt"}, "score": 1.0}
                    for row in range(10)],
    }


class CohereQdrantRSSDiagnosticTests(unittest.TestCase):
    def test_qdrant_storage_is_strictly_owned_by_the_run_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary) / "run"
            self.assertEqual(
                qdrant_rss.validate_storage_containment(run, run / "storage"),
                (run.resolve(), (run / "storage").resolve()),
            )
            for storage in (run, Path(temporary) / "sibling"):
                with self.subTest(storage=storage), \
                        self.assertRaisesRegex(ValueError, "strict descendant"):
                    qdrant_rss.validate_storage_containment(run, storage)

    def test_frozen_plan_comparison_is_type_exact(self):
        qdrant_rss.validate_plan({"version": 1}, {"version": 1})
        with self.assertRaisesRegex(ValueError, "frozen Qdrant RSS plan differs"):
            qdrant_rss.validate_plan({"version": True}, {"version": 1})

    def test_comparison_contract_binds_treedb_go_runtime(self):
        with patch.dict(qdrant_rss.os.environ,
                        {"GOMAXPROCS": "6", "GOGC": "75", "GOMEMLIMIT": "12GiB",
                         "TREEDB_LEAF_PAGE_CACHE_ENTRIES": "262144"}, clear=True):
            contract = qdrant_rss.comparison_contract("a" * 64, {"documents": "b" * 64}, [0, 1])
            child = native.treedb_service_environment({"treedb_go_runtime": contract["treedb_go_runtime"]})
        self.assertEqual(contract["treedb_go_runtime"], {
            "GOMAXPROCS": "6", "GOGC": "75", "GOMEMLIMIT": "12GiB",
        })
        self.assertNotIn("TREEDB_LEAF_PAGE_CACHE_ENTRIES", child)
        self.assertTrue(contract["host_resource_identity"]["machine_id"])
        self.assertTrue(contract["host_resource_identity"]["boot_id"])
        self.assertGreater(contract["host_resource_identity"]["page_size_bytes"], 0)
        self.assertTrue(contract["host_resource_identity"]["cpu_model"])
        self.assertTrue(contract["host_resource_identity"]["cpu_features"])
        self.assertNotIn("relative_ndcg_max_absolute_loss", contract)
        self.assertNotIn("relative_ndcg_control_ef_construction", contract)

    def test_control_selection_requires_both_fixed_query_sets(self):
        truth = [[f"row-{query}-{rank}" for rank in range(10)] for query in range(6)]
        calls = []

        def search(control, query):
            calls.append((control, query))
            keep = 8 if control == 8 and query >= 2 else 9
            return truth[query][:keep] + [f"miss-{query}-{rank}" for rank in range(10 - keep)]

        quality = native.calibrate_ann_control(search, truth, [8, 16], [0, 1], [2, 3, 4, 5], .9)
        self.assertEqual(quality["selected_control"], 16)
        self.assertEqual(quality["selection_protocol"], native.RSS_SELECTION_PROTOCOL)
        self.assertEqual(quality["calibration"]["queries"], [0, 1])
        self.assertEqual(quality["revalidation"]["queries"], [2, 3, 4, 5])
        self.assertTrue(quality["revalidation"]["passed"])
        self.assertEqual([point["control"] for point in quality["revalidation"]["curve"]], [8, 16])
        self.assertEqual(calls, [(8, 0), (8, 1), (8, 2), (8, 3), (8, 4), (8, 5),
                                 (16, 0), (16, 1),
                                 (16, 2), (16, 3), (16, 4), (16, 5)])

    def test_qdrant_document_preserves_native_logical_shape(self):
        vector = np.asarray([[float(index) for index in range(768)]], dtype=np.float32)
        document = native.make_document(vector, 0, 500000)
        point = qdrant_rss.qdrant_point(document)
        self.assertEqual(point["logical_id"], document["id"])
        self.assertEqual(point["vector"], document["embedding"])
        self.assertEqual(point["payload"], {
            "id": document["id"], "content": document["content"], "meta": document["meta"],
        })

    def test_readiness_requires_exact_indexed_count_and_scalar_indexes(self):
        ready = {"status": "green", "optimizer_status": "ok", "points_count": 500000,
                 "exact_points_count": 500000, "indexed_vectors_count": 500000,
                 "payload_schema": {
                     "meta.user_id": {"data_type": "keyword", "points": 500000},
                     "meta.fpath": {"data_type": "keyword", "points": 500000},
                 },
                 "config": {
                     "hnsw_config": {"m": 16, "ef_construct": 100, "full_scan_threshold": 10000,
                                     "on_disk": False},
                     "optimizer_config": {"indexing_threshold": 10000, "max_optimization_threads": 1},
                     "params": {"on_disk_payload": True,
                                "vectors": {"size": 768, "distance": "Cosine", "on_disk": False}},
                 }}
        qdrant_rss.validate_ready_snapshot(ready, 500000)
        for field, value in (("status", "yellow"), ("optimizer_status", {"ok": False}),
                             ("exact_points_count", 499999), ("indexed_vectors_count", 499999)):
            with self.subTest(field=field):
                changed = copy.deepcopy(ready)
                changed[field] = value
                with self.assertRaisesRegex(RuntimeError, "query-ready"):
                    qdrant_rss.validate_ready_snapshot(changed, 500000)
        changed = copy.deepcopy(ready)
        del changed["payload_schema"]["meta.fpath"]
        self.assertFalse(qdrant_rss.ready_snapshot(changed, 500000))
        with self.assertRaisesRegex(RuntimeError, "query-ready"):
            qdrant_rss.validate_ready_snapshot(changed, 500000)
        for path in ("points", "m", "dimension"):
            with self.subTest(type_drift=path):
                changed = copy.deepcopy(ready)
                if path == "points":
                    changed["points_count"] = 500000.0
                elif path == "m":
                    changed["config"]["hnsw_config"]["m"] = 16.0
                else:
                    changed["config"]["params"]["vectors"]["size"] = 768.0
                self.assertFalse(qdrant_rss.ready_snapshot(changed, 500000))

    def test_comparison_accepts_only_matched_calibrated_artifacts(self):
        contract = rss_contract()
        self.assertTrue(native.rss_comparison_contract_valid(contract))
        for mutation in ("rows_type", "cpu_type", "page_type", "control_type"):
            with self.subTest(contract_type_drift=mutation):
                changed_contract = copy.deepcopy(contract)
                if mutation == "rows_type":
                    changed_contract["rows"] = 500000.0
                elif mutation == "cpu_type":
                    changed_contract["cpu_affinity"][0] = False
                elif mutation == "page_type":
                    changed_contract["host_resource_identity"]["page_size_bytes"] = 4096.0
                else:
                    changed_contract["ann_controls"]["treedb_ef_search"][0] = 32.0
                self.assertFalse(native.rss_comparison_contract_valid(changed_contract))
        tree = {"schema": native.RSS_ARTIFACT_SCHEMA, "state": "calibrated", "backend": "treedb",
                "comparison_contract": contract, "quality": fp32_quality("ef_search"),
                "rss": rss_sample(1, 100), "storage": storage_sample(), "reasons": [],
                "provenance": {"harness_commit": "a" * 40, "harness_trees": {"benchmarks": "b" * 40}}}
        qdrant = {**copy.deepcopy(tree), "backend": "qdrant",
                  "quality": fp32_quality("hnsw_ef"), "rss": rss_sample(2, 120),
                  "resource_guard": qdrant_guard(), "cleanup": qdrant_cleanup()}
        qdrant["provenance"]["process_identity"] = "2:20"
        decision = qdrant_rss.compare_artifacts(tree, qdrant)
        self.assertEqual(decision["state"], "accept")
        self.assertEqual(decision["delta_bytes"], -20)
        self.assertAlmostEqual(decision["treedb_to_qdrant_ratio"], 100 / 120)

        qdrant["rss"]["bytes"] = 80
        decision = qdrant_rss.compare_artifacts(tree, qdrant)
        self.assertEqual(decision["state"], "investigate")
        self.assertEqual(decision["delta_bytes"], 20)

        for mutation in (
            "contract", "rss", "rss_source", "rss_scope", "rss_pid", "quality",
            "quality_missing_curve", "quality_control_order", "quality_mean", "quality_ndcg_type",
            "provenance", "reasons", "storage", "guard", "guard_limit",
            "guard_identity", "cleanup", "cleanup_identity", "cleanup_identity_type",
        ):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(qdrant)
                if mutation == "contract":
                    changed["comparison_contract"]["rows"] += 1
                elif mutation == "rss":
                    changed["rss"]["availability"] = "unavailable"
                elif mutation == "rss_source":
                    changed["rss"]["source"] = "/proc/self/status:VmHWM"
                elif mutation == "rss_scope":
                    changed["rss"]["scope"] = "point_sample"
                elif mutation == "rss_pid":
                    changed["rss"]["pid"] = 3
                elif mutation == "quality":
                    changed["quality"]["revalidation"]["passed"] = False
                elif mutation == "quality_missing_curve":
                    changed["quality"]["revalidation"]["curve"] = []
                elif mutation == "quality_control_order":
                    changed["quality"]["calibration"]["curve"][0]["control"] = 64
                elif mutation == "quality_mean":
                    changed["quality"]["calibration"]["curve"][0]["mean_recall_at_10"] = 0.0
                elif mutation == "quality_ndcg_type":
                    changed["quality"]["revalidation"]["curve"][0]["per_query_ndcg_at_10"][0] = True
                elif mutation == "provenance":
                    changed["provenance"]["harness_commit"] = "c" * 40
                elif mutation == "reasons":
                    changed["reasons"] = ["retained failure"]
                elif mutation == "storage":
                    changed["storage"]["owned_bytes_at_rss_boundary"] = 0
                elif mutation == "guard":
                    changed["resource_guard"]["status"] = "failed"
                elif mutation == "guard_limit":
                    changed["resource_guard"]["limits"]["wall_limit_s"] += 1
                elif mutation == "guard_identity":
                    changed["resource_guard"]["last_sample"]["server"]["process_identity"] = "3:30"
                elif mutation == "cleanup":
                    changed["cleanup"]["kill_sent"] = True
                elif mutation == "cleanup_identity_type":
                    changed["cleanup"]["process_identity"] = None
                else:
                    changed["cleanup"]["process_identity"] = "3:30"
                self.assertEqual(qdrant_rss.compare_artifacts(tree, changed)["state"], "uncalibrated")

    def test_three_arm_envelope_nests_original_decision_and_never_promotes_sq8(self):
        contract = rss_contract()
        provenance = tree_provenance()
        tree = {"schema": native.RSS_ARTIFACT_SCHEMA, "state": "calibrated", "backend": "treedb",
                "comparison_contract": contract, "quality": fp32_quality("ef_search"),
                "rss": rss_sample(1, 100), "storage": storage_sample(), "reasons": [],
                "provenance": provenance, "construction_calibration_contract": construction_contract()}
        qdrant = {**copy.deepcopy(tree), "backend": "qdrant",
                  "quality": fp32_quality("hnsw_ef"), "rss": rss_sample(2, 120),
                  "resource_guard": qdrant_guard(), "cleanup": qdrant_cleanup()}
        qdrant["provenance"]["process_identity"] = "2:20"
        sq8 = {**copy.deepcopy(tree), "schema": native.QUANTIZED_RSS_ARTIFACT_SCHEMA,
               "quality": sq8_quality(),
               "rss": rss_sample(3, 80),
               "representation_arm": native.quantized_representation_arm(),
               "readiness": {"graph_action": "build", "successful_ann_queries": 200,
                             "column_graph_build": sq8_build_evidence(),
                             "effective_index": sq8_effective_index()},
               "observed_execution": {"schema": "treedb_cohere_sq8_execution/v1",
                                      "native_command_version": 3,
                                      "requests": [sq8_request(query) for query in range(200)]}}
        expected = qdrant_rss.compare_artifacts(tree, qdrant)
        envelope = qdrant_rss.compare_three_arms(tree, qdrant, sq8)
        self.assertEqual(envelope["fp32_treedb_vs_fp32_qdrant"], expected)
        self.assertEqual(envelope["sq8_treedb_vs_fp32_qdrant"]["state"], "quality_matched_observation")
        self.assertNotIn(envelope["sq8_treedb_vs_fp32_qdrant"]["state"], ("accept", "investigate"))
        self.assertNotIn("recommendation", envelope["sq8_treedb_vs_fp32_qdrant"])
        self.assertNotIn("m5", str(envelope).lower())
        vectors = np.zeros((10, 768), dtype=np.float32)
        vectors[:, 0] = 1
        queries = np.zeros((200, 768), dtype=np.float32)
        queries[:, 0] = 1
        truth = [[f"row-{row:06d}" for row in range(10)] for _ in range(200)]
        self.assertTrue(native.sq8_results_match_dataset(sq8, vectors, queries, truth))
        wrong_score = copy.deepcopy(sq8)
        wrong_score["observed_execution"]["requests"][0]["results"][0]["score"] = .5
        self.assertFalse(native.sq8_results_match_dataset(wrong_score, vectors, queries, truth))
        wrong_ndcg = copy.deepcopy(sq8)
        wrong_ndcg["quality"]["calibration"]["curve"][0]["per_query_ndcg_at_10"] = [0.0] * 100
        wrong_ndcg["quality"]["calibration"]["curve"][0]["mean_ndcg_at_10"] = 0.0
        wrong_ndcg["quality"]["revalidation"]["curve"][0]["per_query_ndcg_at_10"] = [0.0] * 100
        wrong_ndcg["quality"]["revalidation"]["curve"][0]["mean_ndcg_at_10"] = 0.0
        self.assertFalse(native.sq8_results_match_dataset(wrong_ndcg, vectors, queries, truth))
        self.assertEqual(
            qdrant_rss.compare_three_arms(tree, qdrant, wrong_ndcg)["sq8_treedb_vs_fp32_qdrant"]["state"],
            "unavailable",
        )

        for mutation in (
            "profile", "request_r", "missing_work", "stale_owner", "proof_unavailable",
            "success_error", "zero_duration", "duration_mismatch", "zero_output", "base_below_retained",
            "base_above_scores",
            "readiness_generation", "readiness_capability", "readiness_capability_type",
            "readiness_indexes", "readiness_typed_input", "readiness_vector_ef_type",
            "readiness_scalar_identity", "readiness_scalar_index_type", "readiness_index_name_type",
            "quality_control", "quality_exact", "construction", "construction_mode",
            "product_provenance",
            "fabricated_selection", "rss_source", "rss_scope", "rss_pid", "retained_reasons",
            "fake_build", "missing_reason", "missing_config_hash", "candidate_width", "raw_width",
            "rerank_cap", "suffix_work", "delta_work", "shadow_work", "snapshot_drift",
            "snapshot_not_initial", "zero_schema_hash", "representation_version_type",
            "request_command_type", "eligible_type", "readiness_dimension_type", "readiness_m_type",
            "quality_coordinate_type", "execution_version_type", "successful_count_type", "request_ndcg",
            "lifecycle_state",
        ):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(sq8)
                if mutation == "profile":
                    changed["representation_arm"]["requested_rerank_policy"] = "R=64"
                elif mutation == "request_r":
                    changed["observed_execution"]["requests"][0]["requested_rerank_candidates"] = 16
                elif mutation == "missing_work":
                    del changed["observed_execution"]["requests"][0]["score_plane"]
                elif mutation == "stale_owner":
                    changed["observed_execution"]["requests"][0]["score_plane"]["snapshot"]["schema_generation"] = 8
                elif mutation == "proof_unavailable":
                    changed["observed_execution"]["requests"][0]["score_plane"]["available"] = False
                elif mutation == "success_error":
                    changed["observed_execution"]["requests"][0]["error"] = "retained failure"
                elif mutation == "zero_duration":
                    row = changed["observed_execution"]["requests"][0]
                    row["ended_monotonic_ns"] = row["started_monotonic_ns"]
                elif mutation == "duration_mismatch":
                    changed["observed_execution"]["requests"][0]["duration_ns"] = 2
                elif mutation == "zero_output":
                    changed["observed_execution"]["requests"][0]["dense_work"]["output"]["output_bytes"] = 0
                elif mutation == "base_below_retained":
                    changed["observed_execution"]["requests"][0]["dense_work"]["graph"]["base_candidates"] = 1
                elif mutation == "base_above_scores":
                    changed["observed_execution"]["requests"][0]["dense_work"]["graph"]["base_candidates"] = 65
                elif mutation == "readiness_generation":
                    changed["readiness"]["effective_index"]["generation"] = 999
                elif mutation == "readiness_capability":
                    changed["readiness"]["effective_index"]["capabilities"]["typed_dense_quantized_rerank"] = False
                elif mutation == "readiness_capability_type":
                    changed["readiness"]["effective_index"]["capabilities"]["dense_vector_search"] = "true"
                elif mutation == "readiness_indexes":
                    changed["readiness"]["effective_index"]["quantized_indexes"] = []
                elif mutation == "readiness_typed_input":
                    changed["readiness"]["effective_index"]["typed_input"] = False
                elif mutation == "readiness_vector_ef_type":
                    changed["readiness"]["effective_index"]["vector_ef_search"] = "64"
                elif mutation == "readiness_scalar_identity":
                    changed["readiness"]["effective_index"]["scalar_fields"][1]["index_name"] = "meta_fpath"
                elif mutation == "readiness_scalar_index_type":
                    changed["readiness"]["effective_index"]["scalar_fields"][0]["index_name"] = 1
                elif mutation == "readiness_index_name_type":
                    changed["readiness"]["effective_index"]["name"] = 1
                elif mutation == "quality_control":
                    changed["quality"]["control_name"] = "candidate_cap"
                elif mutation == "quality_exact":
                    changed["quality"]["exact_mode"] = True
                elif mutation == "construction":
                    changed["construction_calibration_contract"]["ef_construction"] = 64
                elif mutation == "construction_mode":
                    changed["readiness"]["column_graph_build"]["construction_decisions"] = {}
                elif mutation == "product_provenance":
                    changed["provenance"]["product_commit"] = "0" * 40
                elif mutation == "fabricated_selection":
                    changed["quality"]["calibration"]["curve"].insert(
                        0, copy.deepcopy(changed["quality"]["calibration"]["curve"][0]),
                    )
                elif mutation == "rss_source":
                    changed["rss"]["source"] = "/proc/self/status:VmHWM"
                elif mutation == "rss_scope":
                    changed["rss"]["scope"] = "point_sample"
                elif mutation == "rss_pid":
                    changed["rss"]["pid"] = 4
                elif mutation == "retained_reasons":
                    changed["reasons"] = ["retained failure"]
                elif mutation == "fake_build":
                    changed["readiness"]["column_graph_build"] = {"completed": True}
                elif mutation == "missing_reason":
                    del changed["observed_execution"]["requests"][0]["score_plane"]["reason"]
                elif mutation == "missing_config_hash":
                    del changed["observed_execution"]["requests"][0]["score_plane"]["quantized_config_hash"]
                elif mutation == "candidate_width":
                    changed["observed_execution"]["requests"][0]["score_plane"]["normalized_candidate_width"] -= 1
                elif mutation == "raw_width":
                    changed["observed_execution"]["requests"][0]["score_plane"]["raw_candidate_width"] += 1
                elif mutation == "rerank_cap":
                    changed["observed_execution"]["requests"][0]["score_plane"]["rerank_candidate_cap"] -= 1
                elif mutation == "suffix_work":
                    changed["observed_execution"]["requests"][0]["score_plane"]["exact_suffix_score_calls"] = 1
                elif mutation == "delta_work":
                    changed["observed_execution"]["requests"][0]["dense_work"]["graph"]["delta_scored"] = 1
                elif mutation == "shadow_work":
                    changed["observed_execution"]["requests"][0]["dense_work"]["graph"]["base_shadowed"] = 1
                elif mutation == "snapshot_drift":
                    row = changed["observed_execution"]["requests"][-1]
                    row["score_plane"]["snapshot"]["base_coverage_lsn"] += 1
                    row["score_plane"]["snapshot"]["current_coverage_lsn"] += 1
                    row["dense_work"]["graph"]["snapshot"] = copy.deepcopy(row["score_plane"]["snapshot"])
                elif mutation == "snapshot_not_initial":
                    for snapshot_owner in ("score_plane", "dense_work"):
                        snapshot = (changed["observed_execution"]["requests"][0][snapshot_owner]["snapshot"]
                                    if snapshot_owner == "score_plane"
                                    else changed["observed_execution"]["requests"][0][snapshot_owner]["graph"]["snapshot"])
                        snapshot["current_coverage_lsn"] += 1
                elif mutation == "zero_schema_hash":
                    row = changed["observed_execution"]["requests"][0]
                    row["score_plane"]["snapshot"]["schema_hash"] = 0
                    row["dense_work"]["graph"]["snapshot"]["schema_hash"] = 0
                elif mutation == "representation_version_type":
                    changed["representation_arm"]["version"] = 1.0
                elif mutation == "request_command_type":
                    changed["observed_execution"]["requests"][0]["command_version"] = 3.0
                elif mutation == "eligible_type":
                    changed["observed_execution"]["requests"][0]["eligible"] = 500000.0
                elif mutation == "readiness_dimension_type":
                    changed["readiness"]["effective_index"]["dimension"] = 768.0
                elif mutation == "readiness_m_type":
                    changed["readiness"]["effective_index"]["vector_m"] = True
                elif mutation == "quality_coordinate_type":
                    changed["quality"]["selected_coordinate"]["ef_search"] = 32.0
                elif mutation == "execution_version_type":
                    changed["observed_execution"]["native_command_version"] = 3.0
                elif mutation == "successful_count_type":
                    changed["readiness"]["successful_ann_queries"] = 200.0
                elif mutation == "request_ndcg":
                    changed["observed_execution"]["requests"][0]["ndcg_at_10"] = 0.0
                else:
                    changed["observed_execution"]["requests"][0]["lifecycle_state"]["owner_advance"] = 1
                rejected = qdrant_rss.compare_three_arms(tree, qdrant, changed)
                self.assertEqual(rejected["fp32_treedb_vs_fp32_qdrant"], expected)
                self.assertEqual(rejected["sq8_treedb_vs_fp32_qdrant"]["state"], "unavailable")

        coordinated_tree, coordinated_qdrant, coordinated_sq8 = (
            copy.deepcopy(tree), copy.deepcopy(qdrant), copy.deepcopy(sq8),
        )
        for artifact in (coordinated_tree, coordinated_qdrant, coordinated_sq8):
            artifact["comparison_contract"]["rows"] = 500000.0
        coordinated = qdrant_rss.compare_three_arms(
            coordinated_tree, coordinated_qdrant, coordinated_sq8,
        )
        self.assertEqual(coordinated["fp32_treedb_vs_fp32_qdrant"]["state"], "uncalibrated")
        self.assertEqual(coordinated["sq8_treedb_vs_fp32_qdrant"]["state"], "unavailable")

    def test_peak_rss_rejects_process_drift(self):
        measured = {"availability": "measured", "bytes": 4096, "process_identity": "7:11"}
        with patch.object(native.existing.common, "process_peak_rss", return_value=measured), \
                patch.object(native.existing.common, "linux_process_identity", return_value="7:12"), \
                patch.object(native.os, "sched_getaffinity", return_value={0, 1}):
            sample = native.process_peak_at_boundary(7, "7:11", [0, 1])
        self.assertEqual(sample["availability"], "unavailable")
        self.assertIsNone(sample["bytes"])

    def test_terminal_failure_invalidates_sampled_treedb_rss(self):
        artifact = {"state": "calibrated", "reasons": []}
        self.assertEqual(native.finalize_rss_artifact(artifact, "shutdown failed"), {
            "state": "uncalibrated", "reasons": ["terminal failure: shutdown failed"],
        })

    def test_qdrant_readiness_keeps_polling_for_full_index_counts(self):
        run = qdrant_rss.Run.__new__(qdrant_rss.Run)
        run.optimizer_timeout, run.poll_interval, run.readiness_evidence = 1, 0, []

        def weak_ready(*_args, **_kwargs):
            run.readiness_evidence.append({"snapshots": [{"poll": len(run.readiness_evidence)}]})

        with patch.object(qdrant_rss.existing.QdrantMinimaRunner, "wait_ready", side_effect=weak_ready) as wait, \
                patch.object(qdrant_rss, "ready_snapshot", side_effect=[False, True]), \
                patch.object(qdrant_rss.time, "sleep"):
            self.assertEqual(run.wait_ready(500000, "initial"), {"poll": 1})
        self.assertEqual(wait.call_count, 2)

    def test_owned_qdrant_startup_is_atomic_and_unauthenticated(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = qdrant_rss.Run.__new__(qdrant_rss.Run)
            run.output = Path(temporary) / "run"
            run.output.mkdir()
            run.storage_path = Path(temporary) / "storage"
            run.client_factory = object
            run.harness_pgid = 44
            run.resource_lock = threading.Lock()
            run.plan = {
                "url": "http://127.0.0.1:6333", "qdrant_bin": str(Path(sys.executable).resolve()),
                "qdrant_server_version": "1.19.0", "startup_timeout_s": 1, "poll_interval_s": 0,
            }
            process = MagicMock(pid=os.getpid())
            process.poll.return_value = None
            process.wait.return_value = 0

            def launch_process(*_args, **_kwargs):
                self.assertTrue(run.resource_lock.locked())
                return process

            with patch.dict(qdrant_rss.os.environ, {
                        "QDRANT_API_KEY": "ambient-client-secret",
                        "QDRANT__SERVICE__API_KEY": "ambient-server-secret",
                        "QDRANT__CLUSTER__ENABLED": "true",
                    }), \
                    patch.object(qdrant_rss.subprocess, "Popen",
                                 side_effect=launch_process) as launch, \
                    patch.object(qdrant_rss.existing, "linux_process_identity", return_value="1:1"), \
                    patch.object(qdrant_rss.existing, "server_info", return_value={"version": "1.19.0"}) as info, \
                    patch.object(qdrant_rss.existing, "server_process_owns_endpoint", return_value=True), \
                    patch.object(qdrant_rss.existing, "server_process_identity", return_value="qdrant"), \
                    patch.object(qdrant_rss.os, "getpgid", return_value=44):
                run.start_server()
            info.assert_called_once_with(run.plan["url"], "")
            self.assertIs(run.process, process)
            self.assertEqual(run.process_identity, "1:1")
            self.assertEqual(run.server_pgid, 44)
            self.assertNotIn("QDRANT__CLUSTER__ENABLED", launch.call_args.kwargs["env"])
            self.assertNotIn("QDRANT_API_KEY", launch.call_args.kwargs["env"])
            self.assertNotIn("QDRANT__SERVICE__API_KEY", launch.call_args.kwargs["env"])
            self.assertNotIn("start_new_session", launch.call_args.kwargs)
            run.server_log.close()

    def test_main_rejects_ambient_qdrant_credentials_before_argument_processing(self):
        for variable in qdrant_rss.QDRANT_CREDENTIAL_ENV:
            with self.subTest(variable=variable), \
                    patch.dict(qdrant_rss.os.environ, {variable: "secret"}, clear=True), \
                    self.assertRaisesRegex(ValueError, variable):
                qdrant_rss.main()

    def test_owned_qdrant_reaps_child_when_identity_capture_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = qdrant_rss.Run.__new__(qdrant_rss.Run)
            run.output = Path(temporary) / "run"
            run.output.mkdir()
            run.storage_path = Path(temporary) / "storage"
            run.harness_pgid = 44
            run.resource_lock = threading.Lock()
            run.process = run.process_identity = run.process_command_identity = None
            run.server_pid = run.server_pgid = None
            run.plan = {
                "url": "http://127.0.0.1:6333",
                "qdrant_bin": str(Path(sys.executable).resolve()),
            }
            process = MagicMock(pid=os.getpid())
            process.poll.return_value = None
            process.wait.return_value = 0
            with patch.object(qdrant_rss.subprocess, "Popen", return_value=process), \
                    patch.object(qdrant_rss.existing, "linux_process_identity", return_value=None), \
                    self.assertRaisesRegex(RuntimeError, "identity is unavailable"):
                run.start_server()
            process.terminate.assert_called_once_with()
            process.wait.assert_called_once_with(timeout=30)
            self.assertIsNone(run.process)
            run.server_log.close()

    def test_owned_qdrant_rejects_nonzero_shutdown(self):
        run = qdrant_rss.Run.__new__(qdrant_rss.Run)
        run.process = MagicMock(returncode=1)
        run.process.poll.return_value = None
        run.process_identity, run.server_log = "7:11", None
        run.server_pid, run.server_pgid, run.harness_pgid = 7, 10, 10
        run.client = None
        with self.assertRaisesRegex(RuntimeError, "shutdown exited with 1"):
            with patch.object(qdrant_rss.existing, "linux_process_identity", return_value="7:11"), \
                    patch.object(qdrant_rss.os, "getpgid", return_value=10):
                run.stop_server()

    def test_qdrant_resource_guard_is_continuous_bounded_and_identity_owned(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = qdrant_rss.Run.__new__(qdrant_rss.Run)
            run.output = root / "run"
            run.output.mkdir()
            run.storage_path = run.output / "storage"
            run.storage_path.mkdir()
            run.started = qdrant_rss.time.monotonic()
            run.plan = {"resource_guard": {
                "poll_interval_s": 1, "minimum_free_bytes": 10,
                "maximum_owned_bytes": 100, "maximum_combined_rss_bytes": 500,
                "wall_limit_s": 60,
            }}
            run.harness_identity = "6:10"
            run.resource_lock = qdrant_rss.threading.Lock()
            run.cleaning_up = False
            run.guard_summary = {"sample_count": 0, "max_combined_current_rss_bytes": 0,
                                 "max_owned_bytes": 0, "minimum_free_bytes_observed": None,
                                 "first_sample": None, "last_sample": None,
                                 "cleanup_sample": None}
            run.process = MagicMock(pid=7)
            run.process.poll.return_value = None
            run.process_identity = "7:11"
            samples = [
                {"pid": 6, "process_identity": "6:10", "current_rss_bytes": 100},
                {"pid": 7, "process_identity": "7:11", "current_rss_bytes": 200},
            ]
            with patch.object(qdrant_rss.Run, "current_rss", side_effect=samples), \
                    patch.object(qdrant_rss.existing, "disk_bytes", return_value=50), \
                    patch.object(qdrant_rss.shutil, "disk_usage",
                                 return_value=MagicMock(free=1000)):
                run.check_resources()
            sample = run.guard_summary["last_sample"]
            self.assertEqual(sample["combined_current_rss_bytes"], 300)
            self.assertEqual(sample["owned_bytes"], 50)
            self.assertEqual(sample["server"]["process_identity"], "7:11")
            self.assertEqual(run.guard_summary["sample_count"], 1)
            self.assertEqual(run.guard_summary["minimum_free_bytes_observed"], 1000)

            run.cleaning_up = True
            run.process.poll.return_value = 0
            with patch.object(qdrant_rss.Run, "current_rss", return_value=samples[0]), \
                    patch.object(qdrant_rss.existing, "disk_bytes", return_value=50), \
                    patch.object(qdrant_rss.shutil, "disk_usage",
                                 return_value=MagicMock(free=1000)):
                run.check_resources()
            self.assertIsNone(run.guard_summary["cleanup_sample"]["server"])
            self.assertEqual(run.guard_summary["cleanup_sample"]["combined_current_rss_bytes"], 100)

            run.process.poll.side_effect = [None, 0]
            def exiting_current_rss(_pid, identity):
                if identity == "6:10":
                    return samples[0]
                raise RuntimeError("exited during read")

            with patch.object(qdrant_rss.Run, "current_rss",
                              side_effect=exiting_current_rss), \
                    patch.object(qdrant_rss.existing, "disk_bytes", return_value=50), \
                    patch.object(qdrant_rss.shutil, "disk_usage",
                                 return_value=MagicMock(free=1000)):
                run.check_resources()
            self.assertIsNone(run.guard_summary["cleanup_sample"]["server"])

            run.cleaning_up = False
            run.process.poll.side_effect = None
            run.process.poll.return_value = None
            run.plan["resource_guard"]["maximum_combined_rss_bytes"] = 1
            with patch.object(qdrant_rss.Run, "current_rss", side_effect=samples), \
                    patch.object(qdrant_rss.existing, "disk_bytes", return_value=50), \
                    patch.object(qdrant_rss.shutil, "disk_usage",
                                 return_value=MagicMock(free=1000)), \
                    self.assertRaisesRegex(RuntimeError, "disk/RAM/wall budget"):
                run.check_resources()

    def test_qdrant_resource_guard_signals_only_the_exact_owned_lifetime(self):
        for observed, expected_calls in (("7:11", 1), ("7:12", 0)):
            with self.subTest(observed=observed):
                run = qdrant_rss.Run.__new__(qdrant_rss.Run)
                run.cancel = MagicMock()
                run.cancel.wait.side_effect = [False, True]
                run.resource_failure = None
                run.plan = {"resource_guard": {"poll_interval_s": 1}}
                run.guard_summary = {"failure": None}
                run.guard_termination_sent = False
                run.harness_pgid = 10
                run.process_identity = "7:11"
                run.process = MagicMock(pid=7)
                run.process.poll.return_value = None
                with patch.object(run, "check_resources",
                                  side_effect=RuntimeError("budget")), \
                        patch.object(qdrant_rss.existing, "linux_process_identity",
                                     return_value=observed), \
                        patch.object(qdrant_rss.os, "getpgid", return_value=10):
                    run.guard_resources()
                self.assertIn("resource guard", run.resource_failure)
                self.assertEqual(run.process.terminate.call_count, expected_calls)
                self.assertEqual(run.guard_termination_sent, expected_calls == 1)

    def test_qdrant_current_rss_requires_stable_owned_identity(self):
        status = "Name:\tqdrant\nVmRSS:\t123 kB\n"
        with patch.object(qdrant_rss.Path, "read_text", return_value=status), \
                patch.object(qdrant_rss.existing, "linux_process_identity",
                             side_effect=["7:11", "7:11"]):
            self.assertEqual(qdrant_rss.Run.current_rss(7, "7:11"), {
                "pid": 7, "process_identity": "7:11", "current_rss_bytes": 123 * 1024,
            })
        with patch.object(qdrant_rss.Path, "read_text", return_value=status), \
                patch.object(qdrant_rss.existing, "linux_process_identity",
                             side_effect=["7:11", "7:12"]), \
                self.assertRaisesRegex(RuntimeError, "changed across"):
            qdrant_rss.Run.current_rss(7, "7:11")

    def test_qdrant_cleanup_rechecks_identity_before_forced_shutdown(self):
        run = qdrant_rss.Run.__new__(qdrant_rss.Run)
        run.client, run.server_log = MagicMock(), MagicMock()
        run.process = MagicMock(pid=7, returncode=None)
        run.process.poll.return_value = None
        run.process.wait.side_effect = subprocess.TimeoutExpired("qdrant", 30)
        run.server_pid, run.process_identity = 7, "7:11"
        run.harness_pgid = run.server_pgid = 10
        with patch.object(qdrant_rss.existing, "linux_process_identity",
                         side_effect=["7:11", "7:12"]), \
                patch.object(qdrant_rss.os, "getpgid", return_value=10):
            cleanup = run.cleanup_owned()
        self.assertEqual(cleanup["status"], "failed")
        self.assertTrue(cleanup["term_sent"])
        self.assertFalse(cleanup["kill_sent"])
        self.assertIn("refusing forced shutdown", cleanup["failure"])
        run.process.kill.assert_not_called()
        run.server_log.close.assert_called_once()

    def test_qdrant_cleanup_records_clean_term_and_rejects_forced_kill(self):
        for forced in (False, True):
            with self.subTest(forced=forced):
                run = qdrant_rss.Run.__new__(qdrant_rss.Run)
                run.client, run.server_log = MagicMock(), MagicMock()
                run.process = MagicMock(pid=7, returncode=(-9 if forced else 0))
                run.process.poll.return_value = None
                if forced:
                    run.process.wait.side_effect = [subprocess.TimeoutExpired("qdrant", 30), None]
                run.server_pid, run.process_identity = 7, "7:11"
                run.harness_pgid = run.server_pgid = 10
                with patch.object(qdrant_rss.existing, "linux_process_identity",
                                  return_value="7:11"), \
                        patch.object(qdrant_rss.os, "getpgid", return_value=10):
                    cleanup = run.cleanup_owned()
                self.assertEqual(cleanup["status"], "failed" if forced else "clean")
                self.assertTrue(cleanup["term_sent"])
                self.assertEqual(cleanup["kill_sent"], forced)
                self.assertEqual(run.process.kill.call_count, int(forced))
                self.assertTrue(cleanup["client_closed"])
                self.assertTrue(cleanup["log_closed"])

    def test_qdrant_resource_envelope_is_frozen(self):
        self.assertEqual(qdrant_rss.RESOURCE_GUARD, {
            "poll_interval_s": 1, "wall_limit_s": 2700,
            "minimum_free_bytes": 10 << 30, "maximum_owned_bytes": 11 << 30,
            "maximum_combined_rss_bytes": 24 << 30,
        })

    def test_qdrant_guard_rejects_identity_and_time_drift(self):
        guard = qdrant_guard()
        self.assertTrue(qdrant_rss._qdrant_resource_guard_valid(guard))
        for mutate in (
                lambda value: value["last_sample"]["harness"].update(process_identity="1:not-a-start"),
                lambda value: value["last_sample"].update(elapsed_s=-0.5),
                lambda value: value["last_sample"].update(combined_current_rss_bytes=1),
                lambda value: value["cleanup_sample"].update(elapsed_s=2701),
                lambda value: value["cleanup_sample"].update(server=value["last_sample"]["server"]),
                lambda value: value.update(minimum_free_bytes_observed=1),
                lambda value: value.update(sample_count=1)):
            changed = copy.deepcopy(guard)
            mutate(changed)
            self.assertFalse(qdrant_rss._qdrant_resource_guard_valid(changed))


if __name__ == "__main__":
    unittest.main()
