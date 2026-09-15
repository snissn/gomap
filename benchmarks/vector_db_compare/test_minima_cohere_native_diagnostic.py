"""Small fail-closed checks; no service, protected holdout or corpus collection."""
import copy
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import numpy as np
from treedb_client import IndexInfo

import minima_cohere_native_diagnostic as diagnostic
import minima_qdrant_runner as frozen


def quantized_response(eligible=500000, ef=128, generation=7, *, filter_requested=None):
    filtered = eligible != 500000 if filter_requested is None else filter_requested
    route = ("typed_empty" if eligible == 0 else "typed_exact"
             if filtered and eligible <= 4096 else "quantized_rerank")
    graph_route = route if route != "quantized_rerank" else "typed_hnsw"
    exact = eligible if route == "typed_exact" else ef if route == "quantized_rerank" else 0
    quantized = ef * 2 if route == "quantized_rerank" else 0
    width = min(eligible, max(10, ef))
    manifest = SimpleNamespace(generation=1, format="tcs1", version=1, checksum=1)
    snapshot = SimpleNamespace(available=True, schema_hash=1, schema_generation=generation,
                               base_manifest=manifest, current_manifest=manifest,
                               base_coverage_lsn=1, current_coverage_lsn=1)
    score_plane = SimpleNamespace(
        version=1, available=True, completed=True,
        requested_mode="quantized_rerank", effective_mode="quantized_rerank", route=route, reason="",
        quantized_index_name="minima_sq8", quantized_codec="scalar_u8", quantized_version=1,
        quantized_config_hash=0, requested_top_k=10, requested_ef_search=ef,
        requested_rerank_candidates=ef, normalized_candidate_width=width,
        raw_candidate_width=width, rerank_candidate_cap=min(width, ef),
        raw_retained_candidates=ef if route == "quantized_rerank" else 0,
        live_shortlist_candidates=ef if route == "quantized_rerank" else 0,
        actual_rerank_candidates=ef if route == "quantized_rerank" else 0,
        quantized_score_calls=quantized, quantized_code_bytes_read=quantized * 768,
        exact_base_rerank_score_calls=ef if route == "quantized_rerank" else 0,
        exact_suffix_score_calls=0, exact_small_filter_score_calls=exact if route == "typed_exact" else 0,
        exact_base_vector_bytes_read=exact * 768 * 4,
        exact_suffix_vector_bytes_read=0, snapshot=snapshot,
    )
    graph = SimpleNamespace(
        available=True, completed=True, route=graph_route, base_ann_scored=quantized,
        base_candidates=ef if route == "quantized_rerank" and filtered else 0,
        base_edges=0, delta_scored=0,
        exact_base_scored=exact, base_shadowed=0, base_result_ids=exact,
        filter=SimpleNamespace(attempted=filtered, completed=filtered, eligible_rows=eligible if filtered else 0,
                               source_ids=eligible if filtered else 0, source_bytes=0, inspected_entries=0,
                               mapping_work_charged=0, retained_bytes=0, scratch_id_bytes=0,
                               scratch_rows=0, ordinal_growth_peak_bytes=0),
        snapshot=snapshot,
    )
    result_count = min(10, eligible)
    dense = SimpleNamespace(
        version=1, completed=True, graph=graph,
        output=SimpleNamespace(attempted=True, completed=True, requested=result_count, fetched=result_count,
                               missing=0, output_bytes=1 if result_count else 0,
                               retained_payload_fetches=result_count,
                               json_reconstruction_rows=result_count, typed_column_rows=result_count),
    )
    return SimpleNamespace(
        native_command_version=3, index=SimpleNamespace(generation=generation, vector_strategy="column_graph"),
        route="ann", exact_fallbacks=0, full_document_scan_fallbacks=0,
        dense_work=dense, score_plane=score_plane, documents=[SimpleNamespace(id=f"row-{row:06d}")
                                                              for row in range(result_count)],
    )


def quantized_index_info():
    return IndexInfo.from_dict({
        "typed_input": True,
        "name": "minima_cohere",
        "dimension": 768,
        "metric": "cosine",
        "generation": 7,
        "contract_version": diagnostic.existing.SERVICE_CONTRACT,
        "embedding_field": "embedding",
        "vector_index_name": "embedding",
        "vector_strategy": "column_graph",
        "vector_m": 16,
        "vector_ef_construction": 32,
        "vector_ef_search": 64,
        "quantized_indexes": [{"name": "minima_sq8", "codec": "scalar_u8", "version": 1}],
        "scalar_fields": [
            {"field": "meta.fpath", "index_name": "meta_fpath", "value_type": "string"},
            {"field": "meta.user_id", "index_name": "meta_user_id", "value_type": "string"},
        ],
        "text_field": "content",
        "text_index_name": "content",
        "document_type": "treedb_document_service_v1",
        "capabilities": copy.deepcopy(diagnostic.existing.QUANTIZED_INDEX_CAPABILITIES),
    })


def plain(value):
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: plain(item) for key, item in vars(value).items()}
    return value


def paired_record(mode, sequence, query=0, ef=32, phase=None, started_ns=None):
    response = quantized_response(500000, ef)
    dense = plain(response.dense_work)
    if mode == "exact":
        dense["graph"].update(
            base_ann_scored=ef * 2, base_candidates=ef, base_edges=ef,
            delta_scored=0, exact_base_scored=0, base_shadowed=0,
            base_result_ids=10,
        )
    results = [
        {"id": f"row-{row:06d}", "content": f"minima-cohere:{row}",
         "meta": {"user_id": f"{(row * 7919) % 500000:06d}",
                  "fpath": f"/cohere/{row // 256:06d}.txt"}, "score": 1.0}
        for row in range(10)
    ]
    record = {
        "request_sequence": sequence, "phase": phase or f"paired_{mode}", "eligible": 500000,
        "query": query, "request_mode": mode, "requested_ef_search": ef,
        "requested_rerank_candidates": ef if mode == "quantized_rerank" else None,
        "command_version": 3 if mode == "quantized_rerank" else 2,
        "expected_generation": 7, "filter": None,
        "started_monotonic_ns": started_ns if started_ns is not None else sequence * 10,
        "ended_monotonic_ns": (started_ns if started_ns is not None else sequence * 10) + 1,
        "duration_ns": 1, "outcome": "success", "results": results,
        "recall": 1.0, "ndcg_at_10": 1.0, "dense_work": dense,
    }
    if mode == "quantized_rerank":
        record["score_plane"] = plain(response.score_plane)
    return record


def paired_endpoint(counter, records):
    works = [diagnostic.dense_contract.DenseSearchWork.from_dict(record["dense_work"])
             for record in records]
    graph_total = lambda field: sum(getattr(work.graph, field) for work in works)
    output_total = lambda field: sum(getattr(work.output, field) for work in works)
    calls = len(works)
    return {
        "availability": "measured", "captured_monotonic_ns": counter * 1000,
        "pid": 11, "linux_process_identity": "11:owner",
        "generation": 7,
        "work": {"pid": 11, "schema_version": "treedb-work-v1", "scope": "process",
                 "origin_kind": "go_package_init", "origin_unix_nano": 1,
                 "snapshot_unix_nano": counter,
                 "memory": {"total_alloc": counter * 1000, "mallocs": counter * 100,
                            "heap_alloc": 100, "heap_sys": 200, "sys": 300, "num_gc": 1},
                 "graph": {
                     "requests": {"attempts": calls, "completed": calls, "errors": 0},
                     "filters": {"attempts": 0, "completed": 0, "errors": 0},
                     "empty": 0, "exact": 0, "hnsw": calls,
                     **{field: graph_total(field) for field in (
                         "base_ann_scored", "base_candidates", "base_edges", "delta_scored",
                         "exact_base_scored", "base_shadowed", "base_result_ids",
                     )},
                 },
                 "output": {"search": {
                     "attempts": calls, "completed": calls, "errors": 0,
                     **{field: output_total(field) for field in (
                         "requested", "fetched", "missing", "output_bytes",
                         "retained_payload_fetches", "json_reconstruction_rows",
                         "typed_column_rows",
                     )},
                 }}},
        "server_cpu": {"availability": "measured", "pid": 11,
                       "linux_process_identity": "11:owner", "cpu_ns": counter * 10000,
                       "source": "/proc/<pid>/stat utime+stime and SC_CLK_TCK",
                       "scope": "owned_process_lifetime_through_sample"},
        "client_cpu_ns": counter * 100000,
        "drained_pending": {"pending_docs": 0, "pending_bytes": 0},
        "typed_graph": {
            "index": "embedding", "publication_present": True,
            "publication_unchanged": True, "serving_ready": True,
            "invalid": False, "reconciling": False, "base_present": True,
            "base_manifest": {"generation": 1, "format": "tcs1", "version": 1,
                              "checksum": 1},
            "current_manifest": {"generation": 1, "format": "tcs1", "version": 1,
                                 "checksum": 1},
            "base_coverage_lsn": 1, "current_coverage_lsn": 1,
            "base_rows": 500000, "suffix_rows": 0, "suffix_tombstones": 0,
            "suffix_value_slots": 0, "suffix_payload_bytes": 0,
            "installed_asset_bytes": 300, "base_asset_bytes": 200,
            "owner_asset_bytes": 0,
            "debt": {"rows": 0, "tombstones": 0, "value_slots": 0, "bytes": 0},
            "pending": {"rows": 0, "tombstones": 0,
                        "value_slots": 0, "bytes": 0},
        },
        "total_db_bytes_including_wal": 1000,
    }


class NativeCohereDiagnosticTests(unittest.TestCase):
    def test_full_sq8_requires_one_pinned_prior_rss_decision(self):
        sha = "a" * 64
        diagnostic.validate_quantized_options("quantized_rerank", "minima_sq8", False, 500000,
                                              Path("rss.json"), sha)
        for path, digest in ((None, None), (Path("rss.json"), None), (None, sha)):
            with self.subTest(path=path, digest=digest), \
                    self.assertRaisesRegex(ValueError, "requires one pinned"):
                diagnostic.validate_quantized_options(
                    "quantized_rerank", "minima_sq8", False, 500000, path, digest,
                )
        for rows, rss_only in ((512, False), (500000, True)):
            for path, digest in ((Path("rss.json"), sha), (Path("rss.json"), None), (None, sha)):
                with self.subTest(rows=rows, rss_only=rss_only, path=path, digest=digest), \
                        self.assertRaisesRegex(ValueError, "requires one pinned"):
                    diagnostic.validate_quantized_options(
                        "quantized_rerank", "minima_sq8", rss_only, rows, path, digest,
                    )
        with self.assertRaisesRegex(ValueError, "lowercase hexadecimal"):
            diagnostic.validate_quantized_options(
                "quantized_rerank", "minima_sq8", False, 500000, Path("rss.json"), "A" * 64,
            )

    def test_locked_sq8_rss_artifact_binds_provenance_requests_and_coordinate(self):
        quality_truth = [[f"row-{rank:06d}" for rank in range(10)] for _ in range(200)]
        quality = diagnostic._coordinate_rows(diagnostic.calibrate_ann_control(
            lambda _control, query: quality_truth[query], quality_truth, [32],
            diagnostic.RSS_CALIBRATION_QUERIES, diagnostic.RSS_REVALIDATION_QUERIES,
            diagnostic.RSS_RECALL_TARGET,
        ))
        quality.update(control_name="ef_search", exact_mode=False)
        provenance = {
            "harness_commit": "a" * 40, "harness_source_sha256": "b" * 64,
            "harness_trees": {"benchmarks": "c" * 40}, "product_commit": "d" * 40,
            "product_trees": {"TreeDB": "e" * 40}, "service_sha256": "f" * 64,
            "dataset_manifest_sha256": "1" * 64,
            "dataset_files_sha256": {
                "documents": "2" * 64, "queries": "3" * 64, "truth": "4" * 64,
            },
            "serving_sha256": "5" * 64,
        }
        plan = {
            **provenance, "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
            "cpu_affinity": [0, 1], "host_memory_bytes": 1 << 30,
            "treedb_go_runtime": {"GOMAXPROCS": "2", "GOGC": "", "GOMEMLIMIT": ""},
            "host_resource_identity": {
                "machine_id": "machine", "boot_id": "boot", "page_size_bytes": 4096,
                "numa_mems": "0", "cgroup_membership": "0::/", "cgroup_limits": {},
                "cpu_model": "test CPU", "cpu_features": ["avx2", "sse2"],
            },
            "platform": "test", "rss_recall_target": diagnostic.RSS_RECALL_TARGET,
            "rss_controls": diagnostic.RSS_CONTROLS,
            "rss_calibration_queries": diagnostic.RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": diagnostic.RSS_REVALIDATION_QUERIES,
            "ef_construction": 32,
        }
        requests = [{
            "request_sequence": sequence, "phase": "rss_quality", "eligible": 500000,
            "query": query, "requested_ef_search": 32, "requested_rerank_candidates": 32,
            "command_version": 3, "outcome": "success",
        } for sequence, query in enumerate(range(200), 1)]
        artifact = {
            "schema": diagnostic.QUANTIZED_RSS_ARTIFACT_SCHEMA, "state": "calibrated",
            "backend": "treedb", "reasons": [], "quality": quality,
            "comparison_contract": diagnostic.rss_comparison_contract(plan),
            "representation_arm": diagnostic.quantized_representation_arm(),
            "construction_calibration_contract": diagnostic.construction_calibration_contract(32),
            "provenance": provenance,
            "rss": {"process_identity": "41:99"},
            "readiness": {"graph_action": "build", "successful_ann_queries": len(requests),
                          "effective_index": quantized_index_info().to_dict()},
            "observed_execution": {"schema": "treedb_cohere_sq8_execution/v1",
                                   "native_command_version": 3, "requests": requests},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rss.json"
            path.write_bytes(diagnostic.canonical(artifact))
            sha = diagnostic.digest(path)
            validator = lambda value, _plan: ([] if diagnostic.same_json(value, artifact)
                                               else ["mutated intrinsic evidence"])
            with patch.object(diagnostic, "sq8_rss_artifact_reasons", side_effect=validator), \
                    patch.object(diagnostic, "sq8_artifact_matches_plan_dataset", return_value=True):
                selection = diagnostic.locked_sq8_rss_selection(path, sha, plan)
            self.assertEqual(selection["selected_coordinate"], {
                "ef_search": 32, "rerank_candidates": 32,
            })
            self.assertEqual(selection["source_process_identity"], "41:99")
            self.assertNotIn("quality_sha256", selection)
            for mutation in ("hash", "provenance", "request", "coordinate"):
                with self.subTest(mutation=mutation):
                    changed = copy.deepcopy(artifact)
                    expected = sha
                    if mutation == "hash":
                        expected = "0" * 64
                    elif mutation == "provenance":
                        changed["provenance"]["service_sha256"] = "0" * 64
                    elif mutation == "request":
                        changed["observed_execution"]["requests"][0]["requested_rerank_candidates"] = 16
                    else:
                        changed["quality"]["selected_coordinate"]["ef_search"] = 64
                    path = Path(directory) / f"{mutation}.json"
                    path.write_bytes(diagnostic.canonical(changed))
                    if mutation != "hash":
                        expected = diagnostic.digest(path)
                    with patch.object(diagnostic, "sq8_rss_artifact_reasons", side_effect=validator), \
                            patch.object(diagnostic, "sq8_artifact_matches_plan_dataset", return_value=True), \
                            self.assertRaisesRegex(ValueError, "prior TreeDB SQ8 RSS"):
                        diagnostic.locked_sq8_rss_selection(path, expected, plan)

    def test_locked_control_confirms_both_sets_once_without_retuning(self):
        truth = [[f"row-{query}-{rank}" for rank in range(10)] for query in range(4)]
        calls = []

        def search(control, query):
            calls.append((control, query))
            return truth[query]

        result = diagnostic.evaluate_locked_quantized_control(
            search, truth,
            {"schema": "treedb_cohere_sq8_coordinate_lock/v1",
             "selected_coordinate": {"ef_search": 64, "rerank_candidates": 64}},
            [0, 1], [2, 3], .9,
        )
        self.assertEqual(calls, [(64, 0), (64, 1), (64, 2), (64, 3)])
        self.assertTrue(result["passed"])
        self.assertEqual(result["selection_protocol"],
                         "prior_sq8_rss_coordinate_revalidated_on_fresh_graph/v1")
        self.assertEqual(result["fixed_sets"]["queries_0_99"]["per_query_recall"], [1.0, 1.0])
        failed = diagnostic.evaluate_locked_quantized_control(
            lambda _control, query: truth[query][:-2] + [f"miss-{query}-0", f"miss-{query}-1"],
            truth,
            {"selected_coordinate": {"ef_search": 64, "rerank_candidates": 64}},
            [0, 1], [2, 3], .9,
        )
        self.assertFalse(failed["passed"])

    def test_coordinate_lock_consumption_requires_a_distinct_fresh_owner(self):
        lock = {
            "artifact_sha256": "a" * 64, "source_process_identity": "7:11",
            "selected_coordinate": {"ef_search": 64, "rerank_candidates": 64},
        }
        event = diagnostic.coordinate_lock_consumption_event(lock, "8:12")
        self.assertEqual(event, {
            "source_artifact_sha256": "a" * 64,
            "source_process_identity": "7:11", "fresh_process_identity": "8:12",
            "coordinate": {"ef_search": 64, "rerank_candidates": 64},
        })
        for identity in (None, "", "7:11"):
            with self.subTest(identity=identity), self.assertRaisesRegex(RuntimeError, "fresh graph"):
                diagnostic.coordinate_lock_consumption_event(lock, identity)

    def test_diagnostic_rejects_superset_exports_before_reading_payloads(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = Path(directory)
            args = SimpleNamespace(dataset=dataset, rows=500000, product_commit="a" * 40)
            for rows, queries in ((500001, 200), (500000, 201)):
                with self.subTest(rows=rows, queries=queries):
                    manifest = {"dimensions": 768, "top_k": 10, "exact_train_query_overlap": 0,
                                "rows": rows, "query_count": queries}
                    (dataset / "manifest.json").write_bytes(diagnostic.canonical(manifest))
                    with patch.object(diagnostic, "producer_harness_commit", return_value="a" * 40):
                        with self.assertRaisesRegex(ValueError, "exactly 500000 exported rows and 200 queries"):
                            diagnostic.prepare(args)

    def test_producer_source_identity_does_not_trust_imported_checker(self):
        dirty = Mock(
            returncode=0,
            stdout=" M clients/python/treedb_client/src/treedb_client/client.py\n",
        )
        revision = Mock(returncode=0, stdout="a" * 40 + "\n")
        with patch.object(diagnostic.existing, "repository_commit",
                          return_value="a" * 40) as imported_checker, \
                patch.object(diagnostic.subprocess, "run",
                             side_effect=[dirty, revision]):
            with self.assertRaisesRegex(RuntimeError, "clean committed harness"):
                diagnostic.producer_harness_commit(Path("/candidate"), "a" * 40)
        imported_checker.assert_not_called()

        clean = Mock(returncode=0, stdout="")
        revision = Mock(returncode=0, stdout="b" * 40 + "\n")
        with patch.object(diagnostic.subprocess, "run", side_effect=[clean, revision]):
            with self.assertRaisesRegex(RuntimeError, "differs from the candidate"):
                diagnostic.producer_harness_commit(Path("/candidate"), "a" * 40)

        with patch.object(diagnostic.subprocess, "run",
                          side_effect=subprocess.TimeoutExpired("git", 30)):
            with self.assertRaisesRegex(RuntimeError, "source identity is unavailable"):
                diagnostic.producer_harness_commit(Path("/candidate"), "a" * 40)

    def test_real_dimension_independent_oracle_and_scalar_membership(self):
        vectors = np.zeros((16, 768), dtype=np.float32)
        vectors[:, 0] = 1
        vectors[:, 767] = np.arange(16)
        query = np.zeros((1, 768), dtype=np.float32)
        query[0, 767] = 1
        truth = diagnostic.exact_truth(vectors, query, [4, 16])
        self.assertEqual(truth["16"][0][0], "row-000015")  # Not ordinal/first-coordinate oracle.
        self.assertTrue(all((int(row[4:]) * 7919) % 16 < 4 for row in truth["4"][0]))
        document = diagnostic.make_document(vectors, 3, 16, True)
        self.assertEqual(len(document["embedding"]), 768)
        self.assertEqual(document["embedding"][-1], 3)
        self.assertEqual(document["meta"]["user_id"], f"{(3 * 7919) % 16:06d}")
        self.assertTrue(document["content"].endswith(":updated"))

    def test_no_clobber_and_provenance_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(FileExistsError):
                diagnostic.Run({"run_dir": directory})
        plan = {"schema": diagnostic.SCHEMA, "product_commit": "a" * 40,
                "dataset_files_sha256": {"documents": "b" * 64}}
        diagnostic.validate_plan(plan, copy.deepcopy(plan))
        changed = copy.deepcopy(plan)
        changed["dataset_files_sha256"]["documents"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "frozen plan differs"):
            diagnostic.validate_plan(plan, changed)
        with self.assertRaisesRegex(ValueError, "frozen plan differs"):
            diagnostic.validate_plan({"version": True}, {"version": 1})

    def test_reviewed_json_inputs_are_bounded_duplicate_safe_and_finite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviewed.json"
            path.write_text('{"outer":{"value":1,"value":2}}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate key"):
                diagnostic.strict_json_object(path, "reviewed fixture")
            path.write_text('{"value":NaN}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "nonfinite"):
                diagnostic.strict_json_object(path, "reviewed fixture")
            path.write_text('{"value":1e999}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "nonfinite"):
                diagnostic.strict_json_object(path, "reviewed fixture")
            path.write_bytes(b"x" * 9)
            with self.assertRaisesRegex(ValueError, "exceeds 8 bytes"):
                diagnostic.strict_json_object(path, "reviewed fixture", 8)
            path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "nonempty JSON object"):
                diagnostic.strict_json_object(path, "reviewed fixture")
            frozen = Path(directory) / "frozen.json"
            digest = diagnostic.write_frozen_json(frozen, {"version": 1}, "reviewed fixture")
            self.assertEqual(digest, diagnostic.digest(frozen))
            with self.assertRaises(FileExistsError):
                diagnostic.write_frozen_json(frozen, {"version": 1}, "reviewed fixture")
            with self.assertRaisesRegex(ValueError, "exceeds"):
                diagnostic.write_frozen_json(
                    Path(directory) / "oversized.json",
                    {"value": "x" * diagnostic.FROZEN_JSON_MAX_BYTES}, "reviewed fixture",
                )

    def test_hashing_quantiles_and_frozen_minima_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture"
            path.write_bytes(b"abc")
            self.assertEqual(diagnostic.digest(path), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        self.assertEqual(diagnostic.quantiles(list(range(1, 101)))["p95_ns"], 95)
        self.assertEqual(diagnostic.counts(500000), [4096, 4097, 5000, 50000, 500000])
        self.assertIsNone(diagnostic.predicate(500000, 500000))
        self.assertEqual(diagnostic.predicate(500000, 4097)["value"], "004097")
        self.assertIn("0x6", frozen.GENERATOR)
        self.assertNotEqual(diagnostic.SCHEMA, frozen.MEASURED_SCHEMA)
        self.assertEqual(diagnostic.RSS_CALIBRATION_QUERIES, list(range(100)))
        self.assertEqual(diagnostic.RSS_REVALIDATION_QUERIES, list(range(100, 200)))
        self.assertEqual(diagnostic.binary_ndcg(["a", "x"], ["a", "b"]), 1 / (1 + 1 / np.log2(3)))
        self.assertEqual(diagnostic.construction_calibration_contract(64), {
            "schema": "treedb_column_graph_construction_calibration/v1",
            "ef_construction": 64, "control_ef_construction": 128,
            "max_absolute_recall_loss": .002, "max_absolute_binary_ndcg_loss": .002,
            "max_selected_route_regression": {"qps": .05, "p95": .05, "p99": .05},
        })
        self.assertEqual(diagnostic.quantized_representation_arm()["requested_rerank_policy"],
                         "R=E_at_each_predeclared_coordinate")

    def test_sq8_quality_order_uses_scores_recomputed_from_frozen_vectors(self):
        vectors = np.zeros((2, 768), dtype=np.float32)
        queries = np.zeros((1, 768), dtype=np.float32)
        vectors[0, 0] = queries[0, 0] = 1
        vectors[1, :2] = (1, .004)
        request = {
            "query": 0, "requested_ef_search": 32,
            "results": [
                {"id": "row-000001", "score": 1.0},
                {"id": "row-000000", "score": .999999},
            ],
            "recall": 1.0, "ndcg_at_10": 1.0,
        }
        curve = [{
            "control": 32, "per_query": [1.0],
            "per_query_ndcg_at_10": [1.0],
        }]
        artifact = {
            "observed_execution": {"requests": [request]},
            "quality": {
                "calibration": {"queries": [0], "curve": copy.deepcopy(curve)},
                "revalidation": {"queries": [0], "curve": copy.deepcopy(curve)},
            },
        }
        correct = copy.deepcopy(artifact)
        correct["observed_execution"]["requests"][0]["results"] = [
            {"id": "row-000000", "score": 1.0},
            {"id": "row-000001", "score": float(1 / np.sqrt(1 + .004 ** 2))},
        ]
        self.assertTrue(diagnostic.sq8_results_match_dataset(
            correct, vectors, queries, [["row-000000", "row-000001"]],
        ))
        self.assertFalse(diagnostic.sq8_results_match_dataset(
            artifact, vectors, queries, [["row-000000", "row-000001"]],
        ))

    def test_quantized_cli_contract_is_opt_in_and_has_no_fixed_r(self):
        self.assertEqual(diagnostic.DEFAULT_EF_CONSTRUCTION, 32)
        diagnostic.validate_quantized_options("exact", None, False, 500000)
        diagnostic.validate_quantized_options("quantized_rerank", "minima_sq8", False, 512)
        diagnostic.validate_quantized_options("quantized_rerank", "minima_sq8", True, 500000)
        for values in (("exact", "minima_sq8", False, 500000),
                       ("quantized_rerank", "wrong", False, 500000),
                       ("quantized_rerank", "minima_sq8", True, 512)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                diagnostic.validate_quantized_options(*values)
        source = Path(diagnostic.__file__).read_text()
        self.assertNotIn('parser.add_argument("--quantized-rerank-candidates"', source)

    def test_shutdown_history_import_origin_and_zero_vectors_fail_closed(self):
        lifetime = {"pid": 123, "linux_process_identity": "123:456",
                    "exit": {"pid": 123, "linux_process_identity": "123:456", "exit_code": 0, "availability": "measured"},
                    "terminal_work": {"cleanup_completed": True, "shutdown_failures": 0,
                                      "contract_version": diagnostic.existing.SERVICE_CONTRACT, "work": {"pid": 123}}}
        diagnostic.validate_shutdowns([lifetime], 1)
        lifetime["terminal_work"]["shutdown_failures"] = 1
        with self.assertRaisesRegex(RuntimeError, "clean verified shutdown"):
            diagnostic.validate_shutdowns([lifetime], 1)
        diagnostic.validate_imports(Path(diagnostic.__file__).resolve().parents[2])
        with self.assertRaisesRegex(ValueError, "outside the frozen source"):
            diagnostic.validate_imports(Path("/nonexistent-frozen-tree"))
        run = object.__new__(diagnostic.Run)
        run.plan, run.updated, run.vectors = {"rows": 16}, set(), np.ones((16, 768), dtype=np.float32)
        document = diagnostic.make_document(run.vectors, 0, 16)
        document["embedding"] = [0.0] * 768
        with self.assertRaisesRegex(RuntimeError, "zero/nonfinite vector norm"):
            run.check_documents([SimpleNamespace(**document)], ["row-000000"])

    def test_fold_uses_previously_admitted_limits(self):
        run = object.__new__(diagnostic.Run)
        run.plan, run.info, run.clients = {"serving": {"limit": 1}}, SimpleNamespace(generation=7), Mock()
        run.optimize("build")
        self.assertEqual(run.clients.optimize_index.call_args.kwargs["column_graph_serving"], {"limit": 1})
        run.optimize("fold")
        self.assertIsNone(run.clients.optimize_index.call_args.kwargs["column_graph_serving"])

    def test_ensure_binds_effective_construction_width(self):
        run = object.__new__(diagnostic.Run)
        run.plan = {"ef_construction": 64}
        run.clients = Mock()
        observed = SimpleNamespace(
            name="minima_cohere", dimension=768, metric="cosine",
            contract_version=diagnostic.existing.SERVICE_CONTRACT,
            embedding_field="embedding", vector_index_name="embedding",
            text_field="content", text_index_name="content", document_type="treedb_document_service_v1",
            vector_strategy="column_graph", vector_m=16,
            vector_ef_construction=64, extra={"typed_input": True},
            scalar_fields=[SimpleNamespace(field="meta.user_id", value_type="string"),
                           SimpleNamespace(field="meta.fpath", value_type="string")],
        )
        run.clients.ensure_index.return_value = observed
        run.ensure()
        self.assertEqual(run.clients.ensure_index.call_args.kwargs["vector_index_options"],
                         {"strategy": "column_graph", "ef_construction": 64})

    def test_quantized_ensure_declares_and_observes_one_legacy_sq8_profile(self):
        run = object.__new__(diagnostic.Run)
        run.plan = {"ef_construction": 32, "query_mode": "quantized_rerank",
                    "quantized_index_name": "minima_sq8", "vector_m": 16}
        run.clients, run.emit = Mock(), Mock()
        observed = quantized_index_info()
        run.clients.ensure_index.return_value = observed
        run.ensure()
        self.assertEqual(run.clients.ensure_index.call_args.kwargs["vector_index_options"], {
            "strategy": "column_graph", "ef_construction": 32, "m": 16,
            "quantized_indexes": [{"name": "minima_sq8", "codec": "scalar_u8", "version": 1}],
        })
        mutations = {
            "required_capability": lambda value: value["capabilities"].update(
                typed_dense_quantized_rerank=False),
            "forbidden_capability": lambda value: value["capabilities"].update(
                exact_dense_scoring=True),
            "profile_name": lambda value: value["quantized_indexes"][0].update(name="copied_fp32"),
            "calibration": lambda value: value["quantized_indexes"][0].update(
                scalar_u8_calibration={"mode": "unexpected"}),
            "scalar_order": lambda value: value["scalar_fields"].reverse(),
            "scalar_physical_name": lambda value: value["scalar_fields"][0].update(
                index_name="meta_user_id"),
            "vector_ef_search": lambda value: value.update(vector_ef_search=65),
            "vector_identity": lambda value: value.update(vector_index_name="other"),
            "text_identity": lambda value: value.update(text_index_name="other"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(observed.to_dict())
                mutate(changed)
                run.clients.ensure_index.return_value = IndexInfo.from_dict(changed)
                with self.assertRaisesRegex(RuntimeError, "public collection"):
                    run.ensure()

    def test_quantized_proof_binds_request_owner_route_and_byte_counts(self):
        response = quantized_response()
        diagnostic.validate_quantized_response(response, 500000, 500000, 128, False, 7)
        smoke = quantized_response(512, filter_requested=False)
        diagnostic.validate_quantized_response(smoke, 512, 512, 128, False, 7)
        self.assertEqual(smoke.score_plane.route, "quantized_rerank")
        for path, value in (
            (("native_command_version",), 2),
            (("score_plane", "quantized_index_name"), "wrong"),
            (("score_plane", "requested_rerank_candidates"), 64),
            (("score_plane", "requested_ef_search"), 64),
            (("score_plane", "snapshot", "schema_generation"), 8),
            (("dense_work", "graph", "snapshot"), SimpleNamespace(available=True, schema_generation=8)),
            (("score_plane", "quantized_code_bytes_read"), 0),
            (("score_plane", "raw_retained_candidates"), 64),
            (("score_plane", "available"), False),
            (("score_plane", "completed"), False),
            (("score_plane", "reason"), "unavailable"),
            (("dense_work", "graph", "base_candidates"), 127),
            (("dense_work", "graph", "base_candidates"), 257),
            (("dense_work", "output", "output_bytes"), 0),
        ):
            with self.subTest(path=path):
                changed = copy.deepcopy(response)
                owner = changed
                for name in path[:-1]:
                    owner = getattr(owner, name)
                setattr(owner, path[-1], value)
                with self.assertRaisesRegex(RuntimeError, "quantized proof"):
                    diagnostic.validate_quantized_response(changed, 500000, 500000, 128, False, 7)

        for eligible, route in ((512, "typed_exact"), (513, "typed_exact"),
                                (1000, "typed_exact"), (4096, "typed_exact"),
                                (4097, "quantized_rerank")):
            with self.subTest(eligible=eligible):
                boundary = quantized_response(eligible)
                diagnostic.validate_quantized_response(boundary, 500000, eligible, 128, True, 7)
                self.assertEqual(boundary.score_plane.route, route)
        small = quantized_response(4096)
        small.score_plane.route = "quantized_rerank"
        with self.assertRaisesRegex(RuntimeError, "quantized proof"):
            diagnostic.validate_quantized_response(small, 500000, 4096, 128, True, 7)
        zero_hash = copy.deepcopy(response)
        zero_hash.score_plane.snapshot.schema_hash = 0
        zero_hash.dense_work.graph.snapshot.schema_hash = 0
        with self.assertRaisesRegex(RuntimeError, "quantized proof"):
            diagnostic.validate_quantized_response(zero_hash, 500000, 500000, 128, False, 7)

    def test_quantized_cohort_selection_is_independent_two_set_and_r_equals_e(self):
        truth = {str(eligible): [[f"{eligible}-{query}-{rank}" for rank in range(10)]
                                 for query in range(6)] for eligible in (4096, 4097, 5000, 6000)}
        calls = []

        def search(eligible, control, query):
            calls.append((eligible, control, control, query))
            expected = truth[str(eligible)][query]
            if eligible in (4097, 5000) and control == 8 and (eligible != 5000 or query >= 2):
                return expected[:8] + [f"miss-{eligible}-{query}-{rank}" for rank in range(2)]
            return expected

        quality = diagnostic.select_quantized_cohorts(
            search, truth, [4096, 4097, 5000, 6000], 6000, [8, 16], [0, 1], [2, 3, 4, 5], .9,
        )
        self.assertEqual(quality["4096"]["selected_coordinate"], {"ef_search": 8, "rerank_candidates": 8})
        self.assertEqual(quality["4097"]["selected_coordinate"], {"ef_search": 16, "rerank_candidates": 16})
        self.assertEqual(quality["5000"]["selected_coordinate"], {"ef_search": 16, "rerank_candidates": 16})
        self.assertEqual(quality["6000"]["selected_coordinate"], {"ef_search": 8, "rerank_candidates": 8})
        self.assertTrue(all(ef == rerank for _, ef, rerank, _ in calls))
        self.assertEqual(sum(eligible == 6000 for eligible, *_ in calls), 6)  # One unfiltered selection.
        self.assertEqual(sum(eligible == 4096 for eligible, *_ in calls), 6)  # Exact correctness, no sweep.
        self.assertEqual([row["ef_search"] for row in quality["4097"]["calibration"]["curve"]], [8, 16])
        self.assertEqual([row["rerank_candidates"] for row in quality["4097"]["revalidation"]["curve"]], [8, 16])
        self.assertTrue(diagnostic.quantized_quality_valid(
            quality["4097"], [8, 16], [0, 1], [2, 3, 4, 5], .9,
        ))
        fabricated = copy.deepcopy(quality["4097"])
        fabricated["selected_control"] = 8
        self.assertFalse(diagnostic.quantized_quality_valid(
            fabricated, [8, 16], [0, 1], [2, 3, 4, 5], .9,
        ))
        for path in ("selected_coordinate", "curve_coordinate", "query", "target"):
            with self.subTest(type_drift=path):
                drifted = copy.deepcopy(quality["4097"])
                if path == "selected_coordinate":
                    drifted["selected_coordinate"]["ef_search"] = 16.0
                elif path == "curve_coordinate":
                    drifted["calibration"]["curve"][0]["ef_search"] = 8.0
                elif path == "query":
                    drifted["calibration"]["queries"][0] = 0.0
                else:
                    drifted["target_mean_recall_at_10"] = 1
                self.assertFalse(diagnostic.quantized_quality_valid(
                    drifted, [8, 16], [0, 1], [2, 3, 4, 5], .9,
                ))

        smoke_truth = {str(eligible): [[f"{eligible}-{query}-{rank}" for rank in range(10)]
                                       for query in range(4)]
                       for eligible in (64, 65, 128, 256, 512)}
        smoke_calls = []

        def smoke_search(eligible, control, query):
            smoke_calls.append((eligible, control, query))
            return smoke_truth[str(eligible)][query]

        smoke_quality = diagnostic.select_quantized_cohorts(
            smoke_search, smoke_truth, [64, 65, 128, 256, 512], 512,
            [8, 16], [0, 1], [2, 3], 1.0,
        )
        self.assertEqual(smoke_quality["512"]["selected_coordinate"],
                         {"ef_search": 8, "rerank_candidates": 8})
        self.assertTrue(smoke_quality["512"]["calibration"]["curve"])
        self.assertEqual(sum(eligible == 512 for eligible, *_ in smoke_calls), 4)
        self.assertEqual(sum(eligible == 256 for eligible, *_ in smoke_calls), 4)
        self.assertFalse(smoke_quality["256"]["calibration"]["curve"])

    def test_search_preserves_exact_request_and_quantized_full_result_contract(self):
        vectors = np.zeros((16, 768), dtype=np.float32)
        vectors[:, 0] = 1
        queries = np.zeros((1, 768), dtype=np.float32)
        queries[:, 0] = 1

        def runner(mode):
            run = object.__new__(diagnostic.Run)
            run.plan = {"rows": 16, **({"query_mode": "quantized_rerank"} if mode == "quantized" else {})}
            run.info = SimpleNamespace(generation=7)
            run.vectors, run.queries, run.updated = vectors, queries, set()
            run.overlap_lock = diagnostic.threading.Lock()
            run.overlap_initial = frozenset()
            run.overlap_initial_touched = frozenset()
            run.overlap_initial_deleted = frozenset()
            run.overlap_initial_advance = 0
            run.overlap_batches = []
            run.quantized_touched, run.quantized_deleted = set(), set()
            run.quantized_owner_advance = 0
            run.quantized_folded = False
            run.quantized_initial_snapshot = None
            run.quantized_owner_snapshots = {}
            run.quantized_requests, run.truth = [], {"16": [[f"row-{row:06d}" for row in range(10)]]}
            run.lock = diagnostic.threading.Lock()
            run.emit = Mock()
            response = quantized_response(16, 32, filter_requested=False)
            response.native_command_version = 3 if mode == "quantized" else 2
            if mode == "exact":
                response.score_plane = None
            response.documents = []
            for row in range(10):
                document = diagnostic.make_document(vectors, row, 16)
                response.documents.append(SimpleNamespace(**document, score=1.0))
            native = Mock()
            native.query_by_embedding.return_value = response
            run.clients = SimpleNamespace(native=native)
            run.timed = lambda _phase, call, **_fields: call()
            return run, response, native

        exact, _, exact_native = runner("exact")
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)):
            exact.search("exact", 16, 32, 0)
        self.assertNotIn("query_mode", exact_native.query_by_embedding.call_args.kwargs)
        self.assertNotIn("quantized_rerank_candidates", exact_native.query_by_embedding.call_args.kwargs)

        fp32, _, fp32_native = runner("exact")
        fp32_ledger = []
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)):
            fp32.search("rss_quality", 16, 32, 0, request_ledger=fp32_ledger)
        self.assertEqual(fp32_ledger[0]["request_mode"], "exact")
        self.assertEqual(fp32_ledger[0]["command_version"], 2)
        self.assertEqual([row["id"] for row in fp32_ledger[0]["results"]],
                         [f"row-{row:06d}" for row in range(10)])
        self.assertNotIn("query_mode", fp32_native.query_by_embedding.call_args.kwargs)

        # The SQ8-declared collection can issue a native-v2 FP32 control through
        # the same public method without creating a second graph or profile.
        paired, paired_response, paired_native = runner("quantized")
        paired_response.native_command_version = 2
        paired_response.score_plane = None
        paired_ledger = []
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)):
            paired.search("paired_exact", 16, 32, 0,
                          request_mode="exact", request_ledger=paired_ledger)
        self.assertEqual(paired_ledger[0]["request_mode"], "exact")
        self.assertEqual(paired_ledger[0]["command_version"], 2)
        self.assertIsNone(paired_ledger[0]["requested_rerank_candidates"])
        self.assertNotIn("query_mode", paired_native.query_by_embedding.call_args.kwargs)

        quantized, response, native = runner("quantized")
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)), \
                patch.object(diagnostic, "validate_quantized_response") as validate:
            quantized.search("sq8", 16, 32, 0)
        self.assertEqual(native.query_by_embedding.call_args.kwargs["quantized_rerank_candidates"], 32)
        self.assertEqual(native.query_by_embedding.call_args.kwargs["quantized_index_name"], "minima_sq8")
        validate.assert_called_once_with(response, 16, 16, 32, False, 7)
        self.assertEqual(quantized.quantized_requests[0]["outcome"], "success")
        self.assertEqual(len(quantized.quantized_requests[0]["results"]), 10)

        # Concurrent full projections may linearize before or after the one
        # authored replacement batch; lifecycle validation still enforces that
        # every returned row belongs to one whole reachable state.
        concurrent, concurrent_response, _ = runner("quantized")
        concurrent_response.documents[0].content += ":updated"
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)), \
                patch.object(diagnostic, "validate_quantized_response"), \
                patch.object(concurrent, "validate_quantized_lifecycle"):
            concurrent.search("overlap", 16, 32, 0, writer_active=True)

        for mutation in ("duplicate", "nonfinite", "projection"):
            with self.subTest(mutation=mutation):
                changed, changed_response, _ = runner("quantized")
                if mutation == "duplicate":
                    changed_response.documents[1].id = changed_response.documents[0].id
                elif mutation == "nonfinite":
                    changed_response.documents[0].score = float("nan")
                else:
                    changed_response.documents[0].content = "missing"
                with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)), \
                        patch.object(diagnostic, "validate_quantized_response"):
                    with self.assertRaises(RuntimeError):
                        changed.search("sq8", 16, 32, 0)
                self.assertEqual(changed.quantized_requests[0]["outcome"], "error")
                self.assertTrue(changed.quantized_requests[0]["error"])
                self.assertIn("dense_work", changed.quantized_requests[0])
                self.assertIn("score_plane", changed.quantized_requests[0])

        # Later fixed-coordinate and reopen calls share the same typed-exact
        # correctness gate as selection; a valid proof/projection is not enough.
        exact_rank, _, exact_rank_native = runner("quantized")
        exact_rank.plan["rows"] = 32
        exact_rank.vectors = np.zeros((32, 768), dtype=np.float32)
        exact_rank.vectors[:, 0] = 1
        exact_rank.queries = np.zeros((1, 768), dtype=np.float32)
        exact_rank.queries[:, 0] = 1
        exact_rank.truth = diagnostic.exact_truth(exact_rank.vectors, exact_rank.queries, [16])
        eligible_rows = [row for row in range(32) if (row * 7919) % 32 < 16]

        def ranked_response(ids):
            value = quantized_response(16, 32, filter_requested=True)
            value.documents = [SimpleNamespace(
                **diagnostic.make_document(exact_rank.vectors, int(identifier[4:]), 32), score=1.0,
            ) for identifier in ids]
            return value

        expected_ids = exact_rank.truth["16"][0]
        exact_rank_native.query_by_embedding.return_value = ranked_response(expected_ids)
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)):
            exact_rank.search("fixed_coordinate_curve", 16, 32, 0)

        wrong_ids = sorted(expected_ids[:9] + [f"row-{eligible_rows[10]:06d}"])
        exact_rank_native.query_by_embedding.return_value = ranked_response(wrong_ids)
        with patch.object(diagnostic, "asdict", side_effect=lambda value: vars(value)), \
                self.assertRaisesRegex(RuntimeError, "typed-exact.*frozen eligible truth"):
            exact_rank.search("post_reopen_curve", 16, 32, 0)
        self.assertEqual(exact_rank.quantized_requests[-1]["outcome"], "error")

    def test_paired_query_packet_binds_owner_order_raw_resources_and_unavailable_tvis(self):
        timing = {**diagnostic.paired_timing_plan(1), "queries": [0]}
        plan = {
            "rows": 500000, "paired_timing": timing,
            "representation_arm": diagnostic.quantized_representation_arm(),
            "rss_controls": [32],
        }
        sequence = 1
        clock = 1
        completed_records = []

        def batch(kind, repetition):
            nonlocal sequence, clock
            order = diagnostic.paired_batch_arm_order(kind, repetition)
            arms, common_owner = {}, None
            for mode in order:
                phase = diagnostic.paired_phase(kind, repetition, mode)
                before = None
                if kind == "measured":
                    before = paired_endpoint(clock, completed_records)
                    clock += 1
                records = [paired_record(mode, sequence, phase=phase,
                                         started_ns=clock * 1000)]
                sequence += 1
                clock += 1
                completed_records.extend(records)
                owner = diagnostic.paired_snapshot_from_record(
                    records[0], mode, phase, 0, 32, 500000,
                )
                self.assertIsNotNone(owner)
                common_owner = common_owner or owner
                arm = {"mode": mode, "queries": [0], "requests": records}
                if kind == "measured":
                    after = paired_endpoint(clock, completed_records)
                    clock += 1
                    arm["resources"] = {
                    "before": before, "after": after,
                    "delta": diagnostic.paired_resource_delta(before, after, records),
                    }
                arms[mode] = arm
            return {"batch_kind": kind, "repetition": repetition, "queries": [0],
                    "arm_order": order, "common_owner": common_owner, "arms": arms}

        warmup = batch("warmup", -1)
        repetitions = [batch("measured", repetition) for repetition in range(5)]
        artifact = {
            "schema": diagnostic.PAIRED_QUERY_ARTIFACT_SCHEMA, "status": "complete",
            "collection": "minima_cohere", "rows": 500000, "dimensions": 768,
            "top_k": 10, "eligible": 500000,
            "representation_arm": diagnostic.quantized_representation_arm(),
            "timing_plan": timing, "selected_coordinate": {"ef_search": 32, "rerank_candidates": 32},
            "warmup": warmup, "repetitions": repetitions,
            "storage_attribution": {
                "total_db_bytes_including_wal": 1000,
                "wal_boundary": "included_in_total_db_bytes; no subtraction",
                "aggregate_typed_graph_asset_bytes": {
                    "installed_asset_bytes": 300, "base_asset_bytes": 200,
                    "owner_asset_bytes": 0,
                },
                "logical_sq8_code_bytes_per_vector": 768,
                "logical_sq8_code_bytes": 500000 * 768,
                "actual_quantized_tvis_bytes": None,
                "actual_quantized_tvis_bytes_availability": "producer_unavailable",
                "actual_quantized_tvis_bytes_producer": (
                    "internal VectorIndexSearchStats has physical counters but the public "
                    "DenseSearchWork and score_plane transport omit them"
                ),
            },
        }
        self.assertTrue(diagnostic.paired_query_artifact_valid(artifact, plan))
        for name, mutate in {
            "owner": lambda value: value["repetitions"][0]["common_owner"].update(
                schema_hash=2),
            "order": lambda value: value["repetitions"][0]["arm_order"].reverse(),
            "sequence": lambda value: value["repetitions"][0]["arms"]["exact"]["requests"][0].update(
                request_sequence=99),
            "exact_mode": lambda value: value["repetitions"][0]["arms"]["exact"]["requests"][0].update(
                command_version=3),
            "exact_work": lambda value: value["repetitions"][0]["arms"]["exact"]["requests"][0][
                "dense_work"]["graph"].update(base_ann_scored=0),
            "sq8_mode": lambda value: value["repetitions"][0]["arms"]["quantized_rerank"]["requests"][0].update(
                requested_rerank_candidates=16),
            "sq8_work": lambda value: value["repetitions"][0]["arms"]["quantized_rerank"]["requests"][0][
                "score_plane"].update(exact_suffix_score_calls=1,
                                      exact_suffix_vector_bytes_read=768 * 4),
            "timing": lambda value: value["repetitions"][0]["arms"]["exact"]["requests"][0].update(
                duration_ns=2),
            "allocation": lambda value: value["repetitions"][0]["arms"]["exact"]["resources"]["delta"].update(
                total_alloc_bytes=0),
            "graph_counter": lambda value: value["repetitions"][0]["arms"]["exact"]["resources"][
                "after"]["work"]["graph"].update(base_ann_scored=999),
            "pending": lambda value: value["repetitions"][0]["arms"]["exact"]["resources"][
                "after"]["typed_graph"]["pending"].update(rows=1),
            "physical_inference": lambda value: value["storage_attribution"].update(
                actual_quantized_tvis_bytes=384000000),
            "total_disk": lambda value: value["storage_attribution"].update(
                total_db_bytes_including_wal=999),
        }.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(artifact)
                mutate(changed)
                self.assertFalse(diagnostic.paired_query_artifact_valid(changed, plan))

    def test_quantized_overlap_projection_requires_one_reachable_atomic_batch_state(self):
        def document(row, updated):
            return SimpleNamespace(id=f"row-{row:06d}",
                                   content=f"minima-cohere:{row}" + (":updated" if updated else ""))

        batch = frozenset(range(2049, 2305))
        overlapping = [{"started_monotonic_ns": 5, "ended_monotonic_ns": 15,
                        "rows": batch, "updated": True, "published": True,
                        "success": True}]
        self.assertFalse(diagnostic.quantized_projection_matches_update_states(
            [document(0, True)], frozenset(), overlapping, 10, 20,
        ))
        self.assertFalse(diagnostic.quantized_projection_matches_update_states(
            [document(2049, True), document(2050, False)], frozenset(), overlapping, 10, 20,
        ))
        self.assertTrue(diagnostic.quantized_projection_matches_update_states(
            [document(2049, True), document(2050, True)], frozenset(), overlapping, 10, 20,
        ))
        self.assertTrue(diagnostic.quantized_projection_matches_update_states(
            [document(2049, False)], frozenset(), overlapping, 10, 20,
        ))
        completed_before = [{"started_monotonic_ns": 1, "ended_monotonic_ns": 4,
                             "rows": batch, "updated": True, "published": True,
                             "success": True}]
        self.assertFalse(diagnostic.quantized_projection_matches_update_states(
            [document(2049, False)], frozenset(), completed_before, 10, 20,
        ))
        self.assertTrue(diagnostic.quantized_projection_matches_update_states(
            [document(2049, True)], frozenset(), completed_before, 10, 20,
        ))
        started_after = [{"started_monotonic_ns": 21, "ended_monotonic_ns": 22,
                          "rows": batch, "updated": True, "published": True,
                          "success": True}]
        self.assertFalse(diagnostic.quantized_projection_matches_update_states(
            [document(2049, True)], frozenset(), started_after, 10, 20,
        ))

    def test_quantized_lifecycle_joins_projection_width_owner_and_fold_state(self):
        def document(row, updated=False):
            return SimpleNamespace(id=f"row-{row:06d}",
                                   content=f"minima-cohere:{row}" + (":updated" if updated else ""))

        def owner(response, advance, folded=False, checksum_drift=0):
            initial = baseline.score_plane.snapshot
            current = SimpleNamespace(
                generation=initial.current_manifest.generation + advance,
                format="tcs1", version=1,
                checksum=initial.current_manifest.checksum + advance + checksum_drift,
            )
            snapshot = SimpleNamespace(
                available=True, schema_hash=initial.schema_hash,
                schema_generation=initial.schema_generation,
                base_manifest=current if folded else initial.base_manifest,
                current_manifest=current,
                base_coverage_lsn=(initial.current_coverage_lsn + advance
                                   if folded else initial.base_coverage_lsn),
                current_coverage_lsn=initial.current_coverage_lsn + advance,
            )
            response.score_plane.snapshot = snapshot
            response.dense_work.graph.snapshot = snapshot

        def run_at(rows=500000):
            run = object.__new__(diagnostic.Run)
            run.plan = {"rows": rows, "query_mode": "quantized_rerank"}
            run.overlap_lock = diagnostic.threading.Lock()
            run.updated, run.quantized_touched, run.quantized_deleted = set(), set(), set()
            run.quantized_owner_advance, run.quantized_folded = 0, False
            run.overlap_initial = frozenset()
            run.overlap_initial_touched = frozenset()
            run.overlap_initial_deleted = frozenset()
            run.overlap_initial_advance = 0
            run.overlap_batches = []
            run.quantized_initial_snapshot = None
            run.quantized_owner_snapshots = {}
            return run

        baseline = quantized_response()
        baseline.documents = [document(row) for row in range(10)]
        run = run_at()
        initial_record = {"started_monotonic_ns": 1, "ended_monotonic_ns": 2}
        state = run.validate_quantized_lifecycle(baseline, initial_record, False, 500000, 128)
        self.assertEqual((state["owner_advance"], state["folded"]), (0, False))

        # One two-row batch can linearize during the query. Its projection, global
        # shadow allowance, raw width, and acquired owner must all select state 1.
        run.overlap_batches = [{"started_monotonic_ns": 5, "ended_monotonic_ns": 15,
                                "rows": (0, 1), "updated": True, "published": True,
                                "success": True}]
        changed = quantized_response()
        changed.documents = [document(row, row in (0, 1)) for row in range(10)]
        changed.score_plane.raw_candidate_width = 130
        changed.score_plane.exact_suffix_score_calls = 2
        changed.score_plane.exact_suffix_vector_bytes_read = 2 * 768 * 4
        changed.dense_work.graph.delta_scored = 2
        owner(changed, 1)
        overlap_record = {"started_monotonic_ns": 10, "ended_monotonic_ns": 20}
        state = run.validate_quantized_lifecycle(changed, overlap_record, True, 500000, 128)
        self.assertEqual(state["owner_advance"], 1)
        self.assertEqual(overlap_record["lifecycle_state"]["shadow_allowance"], 2)

        zero_observed = copy.deepcopy(changed)
        self.assertTrue(diagnostic.quantized_response_matches_state(
            zero_observed, state, 500000, 500000, 128, run.quantized_initial_snapshot,
        ))
        nonzero_observed = copy.deepcopy(changed)
        nonzero_observed.dense_work.graph.base_shadowed = 1
        self.assertTrue(diagnostic.quantized_response_matches_state(
            nonzero_observed, state, 500000, 500000, 128, run.quantized_initial_snapshot,
        ))

        filtered = diagnostic.quantized_state_widths(state, 500000, 4097, 128)
        self.assertEqual(filtered, (128, 129, 128, 1))
        self.assertEqual(diagnostic.quantized_state_widths(state, 500000, 500000, 128),
                         (128, 130, 128, 2))

        # Filtered requests may legally prepare against the current postings or
        # bind the cached immutable-base filter. Both must carry the same B/D
        # split, while widths and shadow accounting stay paired to one plan.
        filtered_current = quantized_response(4097)
        filtered_current.documents = [document(row, row in (0, 1)) for row in range(10)]
        filtered_current.score_plane.exact_suffix_score_calls = 1
        filtered_current.score_plane.exact_suffix_vector_bytes_read = 768 * 4
        filtered_current.dense_work.graph.delta_scored = 1
        filtered_current.score_plane.raw_candidate_width = 128
        owner(filtered_current, 1)
        diagnostic.validate_quantized_response(filtered_current, 500000, 4097, 128, True, 7)
        state = run.validate_quantized_lifecycle(
            filtered_current, dict(overlap_record), True, 4097, 128, True,
        )
        self.assertEqual(diagnostic.quantized_state_work(state, 500000, 4097, 128, True), (
            4096, 1, ((128, 128, 128, 0, 4096), (128, 129, 128, 1, 4097)),
        ))

        filtered_cached = copy.deepcopy(filtered_current)
        filtered_cached.score_plane.raw_candidate_width = 129
        filtered_cached.dense_work.graph.base_shadowed = 1
        run.validate_quantized_lifecycle(
            filtered_cached, dict(overlap_record), True, 4097, 128, True,
        )
        wrong_pair = copy.deepcopy(filtered_current)
        wrong_pair.dense_work.graph.base_shadowed = 1
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(
                wrong_pair, dict(overlap_record), True, 4097, 128, True,
            )
        wrong_suffix = copy.deepcopy(filtered_cached)
        wrong_suffix.score_plane.exact_suffix_score_calls = 0
        wrong_suffix.score_plane.exact_suffix_vector_bytes_read = 0
        wrong_suffix.dense_work.graph.delta_scored = 0
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(
                wrong_suffix, dict(overlap_record), True, 4097, 128, True,
            )

        # A current filter can become suffix-only even above the normal typed
        # exact threshold. The producer then labels the zero-base plan exact;
        # the cached immutable-base alternative remains an ANN plan.
        matching_rows = frozenset(
            row for row in range(500000) if (row * 7919) % 500000 < 5000
        )
        suffix_only_state = {
            "updated": matching_rows, "touched": matching_rows,
            "deleted": frozenset(), "owner_advance": 1, "folded": False,
        }
        suffix_only = quantized_response(5000)
        suffix_only.documents = [document(row, row in matching_rows) for row in range(10)]
        suffix_only.score_plane.route = "typed_exact"
        suffix_only.score_plane.normalized_candidate_width = 0
        suffix_only.score_plane.raw_candidate_width = 0
        suffix_only.score_plane.rerank_candidate_cap = 0
        suffix_only.score_plane.raw_retained_candidates = 0
        suffix_only.score_plane.live_shortlist_candidates = 0
        suffix_only.score_plane.actual_rerank_candidates = 0
        suffix_only.score_plane.quantized_score_calls = 0
        suffix_only.score_plane.quantized_code_bytes_read = 0
        suffix_only.score_plane.exact_base_rerank_score_calls = 0
        suffix_only.score_plane.exact_small_filter_score_calls = 0
        suffix_only.score_plane.exact_suffix_score_calls = 5000
        suffix_only.score_plane.exact_base_vector_bytes_read = 0
        suffix_only.score_plane.exact_suffix_vector_bytes_read = 5000 * 768 * 4
        suffix_only.dense_work.graph.route = "typed_exact"
        suffix_only.dense_work.graph.base_ann_scored = 0
        suffix_only.dense_work.graph.base_candidates = 0
        suffix_only.dense_work.graph.exact_base_scored = 0
        suffix_only.dense_work.graph.base_result_ids = 0
        suffix_only.dense_work.graph.delta_scored = 5000
        owner(suffix_only, 1)
        diagnostic.validate_quantized_response(suffix_only, 500000, 5000, 128, True, 7)
        self.assertTrue(diagnostic.quantized_response_matches_state(
            suffix_only, suffix_only_state, 500000, 5000, 128,
            run.quantized_initial_snapshot, True,
        ))

        mixed = copy.deepcopy(changed)
        mixed.documents[1].content = "minima-cohere:1"
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(mixed, overlap_record, True, 500000, 128)
        stale = copy.deepcopy(changed)
        owner(stale, 0)
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(stale, overlap_record, True, 500000, 128)

        # After the writer joins, the same state is deterministic. Fold/reopen must
        # move the base identity to that exact owner without inventing an advance.
        run.updated, run.quantized_touched = {0, 1}, {0, 1}
        run.quantized_owner_advance = 1
        run.quantized_folded = True
        folded = copy.deepcopy(changed)
        folded.score_plane.raw_candidate_width = 128
        folded.score_plane.exact_suffix_score_calls = 0
        folded.score_plane.exact_suffix_vector_bytes_read = 0
        folded.dense_work.graph.delta_scored = 0
        owner(folded, 1, folded=True)
        folded_record = {"started_monotonic_ns": 30, "ended_monotonic_ns": 31}
        state = run.validate_quantized_lifecycle(folded, folded_record, False, 500000, 128)
        self.assertTrue(state["folded"])
        unfolded_owner = copy.deepcopy(folded)
        owner(unfolded_owner, 1, folded=False)
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(unfolded_owner, folded_record, False, 500000, 128)
        drifted = copy.deepcopy(folded)
        owner(drifted, 1, folded=True, checksum_drift=1)
        with self.assertRaisesRegex(RuntimeError, "one exact projection/width/owner"):
            run.validate_quantized_lifecycle(drifted, folded_record, False, 500000, 128)

    def test_overlap_upsert_registers_the_whole_batch_before_the_native_call(self):
        run = object.__new__(diagnostic.Run)
        run.plan = {"rows": 4, "query_mode": "quantized_rerank"}
        run.vectors = np.ones((4, 768), dtype=np.float32)
        run.info = SimpleNamespace(generation=7)
        run.overlap_lock = diagnostic.threading.Lock()
        run.overlap_batches, run.updated = [], set()
        run.quantized_touched, run.quantized_deleted = set(), set()
        run.quantized_owner_advance, run.quantized_folded = 0, False
        native = Mock()
        native.upsert_documents.return_value = SimpleNamespace(
            upserted=2, ids=["row-000000", "row-000001"],
        )
        run.clients = SimpleNamespace(native=native)

        def timed(_phase, call, **_fields):
            if _phase.startswith("overlap"):
                self.assertGreaterEqual(len(run.overlap_batches), 1)
                self.assertEqual(run.overlap_batches[-1]["rows"], (0, 1))
                self.assertIsNone(run.overlap_batches[-1]["success"])
            return call()

        run.timed = timed
        run.upsert([0, 1], "overlap_replace", updated=True, record_overlap=True)
        self.assertTrue(run.overlap_batches[0]["success"])
        self.assertTrue(run.overlap_batches[0]["published"])
        self.assertTrue(run.overlap_batches[0]["updated"])
        self.assertGreaterEqual(run.overlap_batches[0]["ended_monotonic_ns"],
                                run.overlap_batches[0]["started_monotonic_ns"])
        self.assertEqual(run.updated, {0, 1})
        self.assertEqual(run.quantized_touched, {0, 1})
        self.assertEqual(run.quantized_owner_advance, 1)

        run.upsert([0, 1], "overlap_noop", updated=True, record_overlap=True)
        self.assertTrue(run.overlap_batches[1]["success"])
        self.assertFalse(run.overlap_batches[1]["published"])
        self.assertEqual(run.quantized_owner_advance, 1)
        states = diagnostic.quantized_lifecycle_states(
            (), (), (), 0, run.overlap_batches, 0,
            run.overlap_batches[-1]["ended_monotonic_ns"],
        )
        self.assertEqual(len(states), 2)
        self.assertEqual(states[-1]["owner_advance"], 1)

        # The explicit lifecycle update after the repeated overlap batch is the
        # same authored document and therefore cannot invent another owner.
        run.upsert([0, 1], "explicit_noop", updated=True, record_quantized=True)
        self.assertEqual(run.quantized_owner_advance, 1)

        failed = object.__new__(diagnostic.Run)
        failed.plan, failed.vectors, failed.info = run.plan, run.vectors, run.info
        failed.overlap_lock = diagnostic.threading.Lock()
        failed.overlap_batches, failed.updated = [], set()
        failed.quantized_touched, failed.quantized_deleted = set(), set()
        failed.quantized_owner_advance, failed.quantized_folded = 0, False
        failed.clients = SimpleNamespace(native=Mock())
        failed.clients.native.upsert_documents.side_effect = RuntimeError("write failed")
        failed.timed = lambda _phase, call, **_fields: call()
        with self.assertRaisesRegex(RuntimeError, "write failed"):
            failed.upsert([0, 1], "overlap_replace", updated=True, record_overlap=True)
        self.assertFalse(failed.overlap_batches[0]["success"])
        self.assertEqual(failed.updated, set())

    def test_quantized_lifecycle_rejects_fabricated_upsert_publication(self):
        initial = {"updated": frozenset({0, 1}), "touched": frozenset({0, 1}),
                   "deleted": frozenset(), "owner_advance": 1, "folded": False}
        no_op = diagnostic.quantized_upsert_state(initial, [0, 1], True)
        self.assertEqual(no_op, initial)
        replacement = diagnostic.quantized_upsert_state(initial, [0, 1], False)
        self.assertEqual(replacement["owner_advance"], 2)
        self.assertEqual(replacement["updated"], frozenset())
        mixed = diagnostic.quantized_upsert_state({**initial, "touched": frozenset()}, [0, 2], True)
        self.assertEqual(mixed["touched"], frozenset({2}))

        window = {"started_monotonic_ns": 2, "ended_monotonic_ns": 3,
                  "rows": (0, 1), "updated": True, "published": True, "success": True}
        self.assertEqual(diagnostic.quantized_lifecycle_states(
            initial["updated"], initial["touched"], initial["deleted"], 1,
            [window], 0, 4,
        ), [])
        for missing in ("updated", "published"):
            with self.subTest(missing=missing):
                malformed = dict(window)
                malformed.pop(missing)
                self.assertEqual(diagnostic.quantized_lifecycle_states(
                    (), (), (), 0, [malformed], 0, 4,
                ), [])
        for success, ended in ((None, 3), (True, None), (False, None)):
            with self.subTest(success=success, ended=ended):
                malformed = {**window, "success": success, "ended_monotonic_ns": ended}
                self.assertEqual(diagnostic.quantized_lifecycle_states(
                    (), (), (), 0, [malformed], 0, 4,
                ), [])

    def test_quantized_overlap_owner_counts_follow_authored_state_at_both_scales(self):
        for total, expected_advance in ((512, 2), (500000, 8)):
            with self.subTest(total=total):
                state = {"updated": frozenset(), "touched": frozenset(),
                         "deleted": frozenset(), "owner_advance": 0, "folded": False}
                publications = []
                for iteration in range(8):
                    start = (total - 256 * (iteration + 1)) % total
                    current = diagnostic.quantized_upsert_state(
                        state, range(start, start + 256), True,
                    )
                    publications.append(current["owner_advance"] > state["owner_advance"])
                    state = current
                self.assertEqual(state["owner_advance"], expected_advance)
                self.assertEqual(sum(publications), expected_advance)

                # The 512-row explicit update is a no-op after overlap; at 500K
                # it is a new replacement. Deletion and reinsert each publish.
                current = diagnostic.quantized_upsert_state(state, range(256), True)
                self.assertEqual(
                    current["owner_advance"], expected_advance + (total != 512),
                )
                state = {**current,
                         "updated": current["updated"] - frozenset(range(256)),
                         "touched": current["touched"] | frozenset(range(256)),
                         "deleted": current["deleted"] | frozenset(range(256)),
                         "owner_advance": current["owner_advance"] + 1}
                reinserted = diagnostic.quantized_upsert_state(state, range(256), True)
                self.assertEqual(reinserted["owner_advance"],
                                 expected_advance + (total != 512) + 2)
                self.assertFalse(reinserted["deleted"])


if __name__ == "__main__":
    unittest.main()
