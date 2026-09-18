import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import minima_cohere_q5_analyze as analyzer


COMMIT = "a" * 40
FROZEN_LEGACY_FAILURES = [
    "initial exact oracle mismatch for broad_10pct",
    "RuntimeError: timed query 3 does not match its frozen oracle",
]
FROZEN_LEGACY_IDS = [f"minima/broad_10pct/{ordinal:06d}" for ordinal in range(1000, 1005)]
FROZEN_LEGACY_ROUTE_DEFINITIONS = {
    "small": ("complete_exact", "bounded_complete_set", 16, 16, 16, 16, 16, 5, 16),
    "all_match": ("vector_aligned_ann", "vector_aligned_scalar",
                  4096, 32, 32, None, None, 5, 32),
    "over_limit_4097": ("vector_aligned_ann", "vector_aligned_scalar",
                        4096, 32, 32, None, None, 5, 32),
    "broad_10pct": ("complete_finite_ann", "bounded_complete_set",
                    1000, 1000, 1000, None, None, 0, 1000),
    "sparse_over_limit": ("vector_aligned_ann", "vector_aligned_scalar",
                          4096, 32, 32, None, None, 5, 32),
    "mixed_broad_narrow": ("mixed_refined", "bounded_candidate_refinement",
                           4101, 5, 5, 5, 5, 5, 5),
    "empty_user": ("complete_exact", "bounded_complete_set", 0, 0, 0, 0, 0, 0, 0),
    "empty_file": ("complete_exact", "bounded_complete_set", 16, 0, 0, 0, 0, 0, 0),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def frozen_legacy_routes():
    routes = {}
    for name, (plan, membership, probe, candidates, retained, visited, scored,
               admitted, allowed) in FROZEN_LEGACY_ROUTE_DEFINITIONS.items():
        routes[name] = ({
            "identity": "native_base_plus_live_delta", "declared_scalar_filtering": True,
            "native_base_plus_live_delta": True, "full_document_scan_fallbacks": 0,
            "scalar_filter_unbounded": 0, "probe_ids": probe, "candidate_ids": candidates,
            "retained_candidate_ids": retained, "refined_candidate_ids": retained,
            "membership_source": membership, "plan": plan,
            "allowed_id_materialization_rows": allowed, "primary_document_scans": 0,
            "visited_candidates": visited, "scored_candidates": scored,
            "admitted_candidates": admitted,
        }, {
            "membership_source": membership, "plan": plan, "probe_ids": probe,
            "candidates": scored, "candidate_ids": candidates, "retained": retained,
            "refined": retained, "visited": visited, "scored": scored,
            "admitted": admitted, "visibility_mismatches": 0, "visibility_retries": 0,
        })
    return routes


def bounded_manifest():
    populations = {
        "small": (128, 16), "all_match": (7616, 7616),
        "over_limit_4097": (10000, 4097), "broad_10pct": (10000, 1000),
        "sparse_over_limit": (12000, 4097), "mixed_broad_narrow": (10000, 5),
        "empty_user": (128, 0), "empty_file": (128, 0),
    }
    queries = []
    for name, (_, eligible) in populations.items():
        ids = (FROZEN_LEGACY_IDS if name == "broad_10pct"
               else ([] if eligible == 0
                     else [f"minima/{name}/{ordinal:06d}" for ordinal in range(5)]))
        scores = [0.9] * len(ids)
        queries.append({
            "scenario": name,
            "initial_oracle_ids": list(ids), "initial_oracle_scores": list(scores),
            "final_oracle_ids": list(ids), "final_oracle_scores": list(scores),
        })
    return {
        "schema": "treedb_rag_minima_manifest/v2", "fixture": "bounded-50k",
        "config": {
            "dimension": 8, "top_k": 5, "batch_size": 256, "lookup_limit": 4096,
            "warmup_queries": 32, "timed_queries": 1024,
            "reader_concurrency": 4, "writer_concurrency": 1,
            "order_tolerance": 0, "score_tolerance": 0.000001,
        },
        "corpora": [
            {"name": name, "corpus_rows": rows, "eligible_rows": eligible,
             "selectivity": eligible / rows}
            for name, (rows, eligible) in populations.items()
        ],
        "queries": queries,
        "operations": [],
        "corpus_sha256": "1" * 64, "query_sha256": "2" * 64,
        "operation_sha256": "3" * 64, "expected_state_sha256": "4" * 64,
    }


def bounded_plan(manifest, serving):
    return {
        "schema": analyzer.BOUNDED_SQ8_PLAN_SCHEMA,
        "manifest": {
            "schema": manifest["schema"], "fixture": manifest["fixture"],
            "config": manifest["config"], "corpus_sha256": manifest["corpus_sha256"],
            "query_sha256": manifest["query_sha256"],
            "operation_sha256": manifest["operation_sha256"],
            "expected_state_sha256": manifest["expected_state_sha256"],
        },
        "quantized_profile": {
            "schema": "treedb_minima_quantized_profile/v1", "name": "minima_sq8",
            "query_mode": "quantized_rerank", "index_name": "minima_sq8",
            "codec": "scalar_u8", "version": 1, "calibration": "legacy",
            "quantized_config_hash": 0, "requested_ef_search": 64,
            "requested_rerank_candidates": 64, "native_command_version": 3,
        },
        "vector_strategy": "column_graph", "transport": "native",
        "durability_profile": "command_wal_durable", "vector_m": 16,
        "ef_construction": 32, "serving": serving,
    }


def bounded_backend(role, manifest, service, service_sha):
    exact = role == "exact"
    config = {
        "harness_commit": COMMIT, "product_commit": COMMIT,
        "runner_sha256": analyzer.sha256_file(
            Path(analyzer.__file__).with_name("minima_treedb_runner.py")),
        "service_binary": str(service), "service_binary_sha256": service_sha,
        "service_binary_vcs_revision": COMMIT, "service_binary_vcs_modified": "false",
        "vector_strategy": "native_runtime" if exact else "column_graph",
        "transport": "http" if exact else "native", "profile": "command_wal_durable",
        "dimension": "8", "ef_search": "128" if exact else "64",
        "url": "http://127.0.0.1:18040" if exact else "http://127.0.0.1:18050",
        "collection": f"minima_q5_bounded_{'exact' if exact else 'sq8'}_{COMMIT[:12]}",
    }
    if not exact:
        config.update(ef_construction_requested="32", query_mode="quantized_rerank",
                      quantized_index_name="minima_sq8")
    return {
        "name": "treedb", "configuration": config,
        "manifest": {key: manifest[key] for key in (
            "corpus_sha256", "operation_sha256", "query_sha256",
        )},
        "operations": {"manifest_ordered": True},
    }


def clean_bounded_artifact(role, manifest, service, service_sha, plan_sha=None):
    backend = bounded_backend(role, manifest, service, service_sha)
    artifact = {
        "schema": (analyzer.BOUNDED_ARTIFACT_SCHEMA if role == "exact"
                   else analyzer.BOUNDED_SQ8_ARTIFACT_SCHEMA),
        "state": "partial", "passing": False, "readiness_recommendation": "not_evaluated",
        "manifest": manifest, "backends": [backend], "scenarios": [], "failures": [],
        "backend_raw_evidence": {"treedb": {"final_scroll_state": {"match": True}}},
    }
    if role == "exact":
        artifact["native_path_proof"] = {"strategy": "native_runtime"}
    else:
        plan = bounded_plan(manifest, {})
        artifact.update(quantized_plan_sha256=plan_sha,
                        quantized_profile=plan["quantized_profile"])
    return artifact


def known_legacy_failure_artifact(manifest, service, service_sha):
    artifact = clean_bounded_artifact("exact", manifest, service, service_sha)
    artifact["schema"] = "treedb_rag_application/minima_diagnostic_v1"
    artifact["failures"] = list(FROZEN_LEGACY_FAILURES)
    artifact["native_path_proof"] = {
        "schema": "treedb_minima_native_path_proof/v1", "strategy": "native_runtime",
        "availability": "unavailable", "counters": None,
        "reason": ("native baseline diagnostic; typed column_graph lifecycle counters require "
                   "M1-M4; bounded sparse scenario does not preserve full <1% selectivity"),
    }
    backend = artifact["backends"][0]
    backend["operations"] = {
        "batch_insert_during_search": False, "empty_cases_checked": False,
        "explicit_delete_visible": False, "explicit_update_visible": False,
        "manifest_ordered": False, "reindex_delete_replace": False,
        "reindex_execution_sha256": "", "reindex_execution_trace": {"operations": []},
        "reindex_operations_executed": 0, "timed_execution_sha256": "",
        "timed_execution_trace": {"queries": [], "rounds": []},
        "timed_queries_executed": 0, "timed_rounds_completed": 0,
    }
    backend["reopen"] = {
        "attempted": False, "committed_parity": False, "result_manifest_hash": "",
    }
    scenarios, events, routes = [], [], {}
    population = {row["name"]: row for row in manifest["corpora"]}
    route_contracts = frozen_legacy_routes()
    for query in manifest["queries"]:
        name = query["scenario"]
        expected, expected_scores = query["initial_oracle_ids"], query["initial_oracle_scores"]
        mismatch = name == "broad_10pct"
        actual = [] if mismatch else list(expected)
        actual_scores = [] if mismatch else list(expected_scores)
        route, routes[name] = copy.deepcopy(route_contracts[name])
        if name in analyzer.LEGACY_BASELINE_ANN_ROUTES:
            scored = 2064
            visited = scored if name == "broad_10pct" else scored + 16
            route.update(visited_candidates=visited, scored_candidates=scored)
            routes[name].update(candidates=scored, visited=visited, scored=scored)
        scenarios.append({
            "scenario": name, "backend": "treedb",
            "corpus_rows": population[name]["corpus_rows"],
            "expected_matches": population[name]["eligible_rows"],
            "selectivity": population[name]["selectivity"],
            "initial_oracle_ids": expected, "initial_actual_ids": actual,
            "initial_oracle_scores": expected_scores,
            "initial_actual_scores": actual_scores,
            "final_oracle_ids": query["final_oracle_ids"],
            "final_oracle_scores": query["final_oracle_scores"],
            "order_tolerance": manifest["config"]["order_tolerance"],
            "score_tolerance": manifest["config"]["score_tolerance"],
            "actual_ids": [], "actual_scores": [], "reopen_ids": [], "reopen_parity": True,
            "errors": 0, "timeouts": 0,
            "recall": 1.0 if query["final_oracle_ids"] == [] else 0.0,
            "overlap": 1.0 if query["final_oracle_ids"] == [] else 0.0,
            "route": route,
            "correctness": {
                "cross_user_results": 0, "stale_delete_ids": 0,
                "stale_insert_ids": 0, "stale_update_ids": 0,
            },
            "visibility": {
                "generation_consistent": True, "visibility_mismatch_count": 0,
                "visibility_retry_count": 0,
            },
        })
        events.append({
            "kind": "oracle_comparison", "operation": "initial_oracle_comparison",
            "scenario": name, "expected_ids": expected, "actual_ids": actual,
            "match": not mismatch, "maximum_score_delta": 0.0,
        })
    artifact["scenarios"] = scenarios
    phases = []
    for index, (name, classification) in enumerate(analyzer.LEGACY_BASELINE_PHASES):
        start = 100 + index * 20
        phases.append({
            "name": name, "classification": classification,
            "start_nanos": start, "end_nanos": start + 10, "duration_nanos": 10,
            "resource_segments": [{"start": {}, "end": {}}],
            "sample_count": 1, "sample_duration_nanos": 1,
        })
    artifact["backend_raw_evidence"] = {"treedb": {
        "diagnostic_resume": None,
        "diagnostics": copy.deepcopy(analyzer.LEGACY_BASELINE_DIAGNOSTICS),
        "events": events, "final_scroll_state": {}, "native_route_responses": routes,
        "phase_attribution": {
            "clock": "time.monotonic_ns", "total_start_nanos": 100,
            "total_end_nanos": 160, "total_duration_nanos": 60,
            "unattributed_nanos": 30,
            "unattributed_rule": (
                "total_duration_nanos = sum(phase.duration_nanos) + unattributed_nanos; "
                "unattributed_nanos <= max(60000000000, total_duration_nanos / 100); "
                "unattributed covers only runner bookkeeping between declared boundaries"
            ),
            "phases": phases,
        },
        "phase_latency_distributions": {}, "resource_availability": {},
        "resource_measurement": {}, "restart_boundary": {}, "service_log": {},
        "timed_overlap": {},
        "upsert_batch_correlation_contract": copy.deepcopy(
            analyzer.LEGACY_BASELINE_CORRELATION_CONTRACT,
        ),
        "upsert_batch_correlations": [],
    }}
    return artifact


def set_legacy_ann_work(artifact, name, scored, visited):
    scenario = next(row for row in artifact["scenarios"] if row["scenario"] == name)
    raw = artifact["backend_raw_evidence"]["treedb"]["native_route_responses"][name]
    scenario["route"].update(
        scored_candidates=scored,
        visited_candidates=visited,
    )
    raw.update(candidates=scored, scored=scored, visited=visited)


class Q5AnalyzeTest(unittest.TestCase):
    def test_fp32_readiness_requires_nonquantized_build_and_exact_count_type(self):
        build = {
            **{field: 1 for field in analyzer.native._COLUMN_GRAPH_BUILD_FIELDS
               if field != "construction_decisions"},
            "construction_decisions": None,
            "quantized_preparation_nanos": 0,
        }
        tree = {
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
            "readiness": {
                "graph_action": "build", "successful_ann_queries": 2,
                "column_graph_build": build,
                "effective_index": {"m": 16, "ef_construction": 32},
            },
            "quality": {
                "calibration": {"curve": [{"per_query": [1.0]}]},
                "revalidation": {"curve": [{"per_query": [1.0]}]},
            },
        }
        self.assertTrue(analyzer._fp32_rss_readiness_valid(tree))
        for label, mutate in {
            "quantized_stage": lambda row: row["readiness"]["column_graph_build"].update(
                quantized_preparation_nanos=1,
            ),
            "float_count": lambda row: row["readiness"].update(
                successful_ann_queries=2.0,
            ),
            "bool_count": lambda row: row["readiness"].update(
                successful_ann_queries=True,
            ),
        }.items():
            changed = copy.deepcopy(tree)
            mutate(changed)
            with self.subTest(label=label):
                self.assertFalse(analyzer._fp32_rss_readiness_valid(changed))

    def test_packet_serving_configuration_requires_current_physical_limits(self):
        serving = {
            "Publication": {key: 1 for key in (
                "Rows", "Tombstones", "ValueSlots", "OwnedBytes", "EncodedOutputBytes",
            )},
            "Owners": {
                **{key: 1 for key in ("Owners", "States", "StateBytes", "AssetBytes")},
                "Cold": {key: 1 for key in (
                    "ManifestRecords", "ManifestBytes", "AssetBytes", "DecodedTermBytes",
                )},
                "Physical": {key: 1 for key in (
                    "segments", "descriptors", "mapped_bytes", "fallback_bytes", "inventory_bytes",
                )},
            },
            "CandidateOutput": {"Bytes": 1, "AppenderAttempts": 1},
            "Maintenance": {key: 1 for key in (
                "NativeEntries", "ColumnSegments", "ManifestRecords", "LifecycleEntries",
                "NativeBytes", "ColumnBytes", "ManifestBytes", "RetainedBytes", "PagerPages",
            )},
            "Filter": {key: 1 for key in (
                "SourceIDs", "SourceBytes", "RetainedBytes", "MappingWork", "InspectedEntries",
            )},
            "FoldRows": 1,
            "SearchCandidates": 1,
        }
        analyzer.validate_serving_configuration(serving)
        del serving["Owners"]["Physical"]
        with self.assertRaisesRegex(analyzer.EvidenceError, "invalid frozen TreeDB serving"):
            analyzer.validate_serving_configuration(serving)

    @staticmethod
    def _append_construction_calls(events, rows):
        def append(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        append("service_start")
        append("schema_ensure")
        for start in range(0, rows, 256):
            append("initial_durable_ingest", first_row=start, rows=min(256, rows - start))
        append("initial_graph_build")

    def test_strict_json_rejects_duplicates_and_nonfinite(self):
        with self.assertRaisesRegex(analyzer.EvidenceError, "duplicate"):
            analyzer.decode_json(b'{"a":1,"a":2}')
        with self.assertRaisesRegex(analyzer.EvidenceError, "non-finite"):
            analyzer.decode_json(b'{"a":NaN}')
        with self.assertRaisesRegex(analyzer.EvidenceError, "non-finite"):
            analyzer.decode_json(b'{"a":1e999}')

    def test_execution_shapes_are_fixed_independently_of_frozen_plan_values(self):
        native_plan = {
            "eligible_counts": analyzer.native.counts(512),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "overlap_eligible": 65, "reader_concurrency": 4, "writer_calls": 8,
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": analyzer.native.RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": analyzer.native.RSS_REVALIDATION_QUERIES,
            "rss_recall_target": analyzer.native.RSS_RECALL_TARGET,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
        }
        self.assertTrue(analyzer._native_diagnostic_shape_valid(native_plan, 512))
        for field, value in (("efs", [128]), ("reader_concurrency", 1),
                             ("writer_calls", 1), ("overlap_eligible", 64)):
            changed = copy.deepcopy(native_plan)
            changed[field] = value
            with self.subTest(native=field):
                self.assertFalse(analyzer._native_diagnostic_shape_valid(changed, 512))

        qdrant_plan = {
            "rows": 500000, "queries": 200, "dimensions": 768,
            "top_k": 10, "batch_size": 256, "controls": analyzer.native.RSS_CONTROLS,
            "dataset_manifest_sha256": "a" * 64,
            "dataset_files_sha256": {
                "documents": "b" * 64, "queries": "c" * 64, "truth": "d" * 64,
            },
            "cpu_affinity": list(range(6)), "platform": "test-linux",
            "comparison_contract": {
                "rows": 500000, "dimensions": 768, "top_k": 10, "batch_size": 256,
                "dataset_manifest_sha256": "a" * 64,
                "dataset_files_sha256": {
                    "documents": "b" * 64, "queries": "c" * 64, "truth": "d" * 64,
                },
                "cpu_affinity": list(range(6)), "platform": "test-linux",
                "ann_controls": {
                    "treedb_ef_search": analyzer.native.RSS_CONTROLS,
                    "qdrant_hnsw_ef": analyzer.native.RSS_CONTROLS,
                },
            },
            "production_hnsw": analyzer.qdrant.existing.PRODUCTION_HNSW_CONFIG,
            "production_optimizers": analyzer.qdrant.existing.PRODUCTION_OPTIMIZERS_CONFIG,
            "initial_upload_hnsw": analyzer.qdrant.existing.INITIAL_UPLOAD_HNSW_CONFIG,
            "initial_upload_optimizers": analyzer.qdrant.existing.INITIAL_UPLOAD_OPTIMIZERS_CONFIG,
        }
        self.assertTrue(analyzer._qdrant_diagnostic_shape_valid(qdrant_plan))
        for field, value in (("batch_size", 1000), ("cpu_affinity", list(range(1, 6))),
                             ("initial_upload_hnsw", {"m": 16})):
            changed = copy.deepcopy(qdrant_plan)
            changed[field] = value
            with self.subTest(qdrant=field):
                self.assertFalse(analyzer._qdrant_diagnostic_shape_valid(changed))

    def test_jsonl_is_streamed_with_line_and_total_caps(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_bytes(b'{"event":"one"}\n{"event":"two"}\n')
            self.assertEqual([row["event"] for row in analyzer.read_jsonl(path)], ["one", "two"])
            with mock.patch.object(analyzer, "MAX_JSONL_LINE_BYTES", 8):
                with self.assertRaisesRegex(analyzer.EvidenceError, "line 1"):
                    list(analyzer.read_jsonl(path))
            with mock.patch.object(analyzer, "MAX_JSONL_BYTES", 20):
                with self.assertRaisesRegex(analyzer.EvidenceError, "total byte"):
                    list(analyzer.read_jsonl(path))
            path.write_bytes(b'{"payload":"' + b"x" * 100 + b'"}')
            with mock.patch.object(analyzer, "MAX_JSONL_LINE_BYTES", 16):
                with self.assertRaisesRegex(analyzer.EvidenceError, "line 1"):
                    list(analyzer.read_jsonl(path))

    def test_json_and_packet_reads_are_bounded_before_decode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oversized.json"
            path.write_bytes(b"{" + b" " * 100 + b"}")
            with self.assertRaisesRegex(analyzer.EvidenceError, "exceeds 8 bytes"):
                analyzer.read_json(path, "oversized", 8)
            with mock.patch.object(analyzer, "MAX_PACKET_BYTES", 8), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "exceeds 1 MiB"):
                analyzer.load_packet(path, "0" * 64)

    def test_go_inputs_require_reproducible_candidate_build_metadata(self):
        output = "\n".join((
            "/packet/service: go1.26.0",
            "\tpath\tgithub.com/snissn/gomap/cmd/treedb-document-service",
            "\tmod\tgithub.com/snissn/gomap\t(devel)",
            "\tbuild\t-trimpath=true",
            "\tbuild\tvcs=git",
            f"\tbuild\tvcs.revision={COMMIT}",
            "\tbuild\tvcs.modified=false",
        ))
        completed = mock.Mock(returncode=0, stdout=output)
        with mock.patch.object(analyzer.subprocess, "run", return_value=completed):
            build = analyzer._go_binary_build(
                Path("/packet/service"), COMMIT,
                "github.com/snissn/gomap/cmd/treedb-document-service",
            )
        self.assertEqual(build["go_version"], "go1.26.0")
        self.assertEqual(build["build_settings"]["vcs.revision"], COMMIT)
        changed = output.replace("vcs.modified=false", "vcs.modified=true")
        with mock.patch.object(
                analyzer.subprocess, "run", return_value=mock.Mock(returncode=0, stdout=changed)), \
                self.assertRaisesRegex(analyzer.EvidenceError, "reproducible candidate"):
            analyzer._go_binary_build(
                Path("/packet/service"), COMMIT,
                "github.com/snissn/gomap/cmd/treedb-document-service",
            )

    def test_consumer_source_requires_clean_candidate_import_blobs(self):
        analyzer_raw = Path(analyzer.__file__).read_bytes()

        def clean_run(argv, **_kwargs):
            if argv[1] == "show":
                return mock.Mock(returncode=0, stdout=analyzer_raw)
            if argv[1] == "rev-parse":
                return mock.Mock(returncode=0, stdout="b" * 40 + "\n")
            return mock.Mock(returncode=0, stdout="")

        with mock.patch.object(analyzer.sys, "modules", {}), \
                mock.patch.object(analyzer.subprocess, "run", side_effect=clean_run):
            identity = analyzer.validate_consumer_source(COMMIT)
        self.assertEqual(identity["candidate_commit"], COMMIT)
        self.assertEqual(identity["analyzer_sha256"], hashlib.sha256(analyzer_raw).hexdigest())

        def dirty_run(argv, **_kwargs):
            if argv[1] == "status":
                return mock.Mock(returncode=0, stdout=" M analyzer.py\n")
            return mock.Mock(returncode=0, stdout="")

        with mock.patch.object(analyzer.subprocess, "run", side_effect=dirty_run), \
                self.assertRaisesRegex(analyzer.EvidenceError, "dirty"):
            analyzer.validate_consumer_source(COMMIT)

    def test_normalized_build_admission_preserves_identity_without_requiring_trimpath(self):
        package = "github.com/snissn/gomap/cmd/treedb-document-service"
        base = "\n".join((
            "/packet/service: go1.26.0", f"\tpath\t{package}",
            "\tmod\tgithub.com/snissn/gomap\t(devel)", "\tbuild\tvcs=git",
            f"\tbuild\tvcs.revision={COMMIT}", "\tbuild\tvcs.modified=false",
        ))
        for extra in ("", "\n\tbuild\t-trimpath=true"):
            with self.subTest(trimpath=bool(extra)), mock.patch.object(
                    analyzer.subprocess, "run", return_value=mock.Mock(returncode=0, stdout=base + extra)):
                build = analyzer._go_binary_build(
                    Path("/packet/service"), COMMIT, package, require_trimpath=False,
                )
                self.assertEqual(build["build_settings"].get("-trimpath"), "true" if extra else None)
                self.assertEqual(build["build_settings"]["vcs.revision"], COMMIT)
        for changed in (
            base, base.replace("vcs.modified=false", "vcs.modified=true"),
            base.replace(COMMIT, "b" * 40), base.replace(package, package + "-wrong"),
            base.replace("vcs=git", "vcs=other"), base.replace("go1.26.0", "go1.25.0"),
            base.replace("\tmod\tgithub.com/snissn/gomap", "\tmod\twrong/module"),
        ):
            with self.subTest(metadata=changed), mock.patch.object(
                    analyzer.subprocess, "run", return_value=mock.Mock(returncode=0, stdout=changed)):
                with self.assertRaises(analyzer.EvidenceError):
                    analyzer._go_binary_build(Path("/packet/service"), COMMIT, package)
                if changed != base:
                    with self.assertRaises(analyzer.EvidenceError):
                        analyzer._go_binary_build(
                            Path("/packet/service"), COMMIT, package, require_trimpath=False,
                        )

    def test_normalized_consumer_reanalysis_is_bound_to_only_reviewed_offline_changes(self):
        repair = "b" * 40
        relative = "benchmarks/vector_db_compare/minima_cohere_q5_analyze.py"
        allowed = [relative, "benchmarks/vector_db_compare/test_minima_cohere_q5_analyze.py",
                   "benchmarks/vector_db_compare/cohere_scale_harness.md"]
        raw = Path(analyzer.__file__).read_bytes()

        def run(argv, **_kwargs):
            if argv[1] == "show":
                return mock.Mock(returncode=0, stdout=raw)
            if argv[1] == "rev-parse":
                return mock.Mock(returncode=0, stdout="c" * 40 + "\n")
            if argv[1] == "diff" and "--name-only" in argv:
                self.assertIn(COMMIT, argv)
                self.assertIn(repair, argv)
                self.assertIn("TreeDB", argv)
                self.assertIn("clients/python/treedb_client", argv)
                return mock.Mock(returncode=0, stdout="\0".join(allowed) + "\0")
            if argv[1] == "merge-base":
                return mock.Mock(returncode=0, stdout="")
            if argv[1] == "diff":
                self.assertIn(repair, argv)
            return mock.Mock(returncode=0, stdout="")

        with mock.patch.object(analyzer.sys, "modules", {}), \
                mock.patch.object(analyzer.subprocess, "run", side_effect=run):
            identity = analyzer.validate_consumer_source(COMMIT, analyzer_commit=repair)
        self.assertEqual(identity["candidate_commit"], COMMIT)
        self.assertEqual(identity["analyzer_commit"], repair)
        self.assertEqual(identity["consumer_only_changed_paths"], sorted(allowed))
        self.assertEqual(identity["analyzer_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertIn("producer_harness_trees", identity)

        for changed in (
            "TreeDB/collections/vector_index_rebuild.go", "go.mod", "internal/foo.go",
            "cmd/treedb-document-service/main.go",
            "benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py",
            "clients/python/treedb_client/src/treedb_client/native.py",
        ):
            def drift(argv, **kwargs):
                if argv[1] == "diff" and "--name-only" in argv:
                    return mock.Mock(returncode=0, stdout=changed + "\0")
                return run(argv, **kwargs)
            with self.subTest(changed=changed), mock.patch.object(analyzer.sys, "modules", {}), \
                    mock.patch.object(analyzer.subprocess, "run", side_effect=drift), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "consumer-only"):
                analyzer.validate_consumer_source(COMMIT, analyzer_commit=repair)
        for operation in ("diff", "status", "show", "merge-base"):
            def invalid(argv, **kwargs):
                if argv[1] == operation:
                    return mock.Mock(returncode=1, stdout=b"wrong blob" if operation == "show"
                                     else "dirty or wrong revision")
                return run(argv, **kwargs)
            with self.subTest(operation=operation), mock.patch.object(analyzer.sys, "modules", {}), \
                    mock.patch.object(analyzer.subprocess, "run", side_effect=invalid), \
                    self.assertRaises(analyzer.EvidenceError):
                analyzer.validate_consumer_source(COMMIT, analyzer_commit=repair)
        with self.assertRaises(analyzer.EvidenceError):
            analyzer.validate_consumer_source(COMMIT, analyzer_commit="not-a-commit")

        def changed_import(argv, **kwargs):
            if argv[1] == "show" and argv[2].endswith("minima_cohere_native_diagnostic.py"):
                value = (b"changed producer import" if argv[2].startswith(COMMIT)
                         else Path(analyzer.native.__file__).read_bytes())
                return mock.Mock(returncode=0, stdout=value)
            return run(argv, **kwargs)
        with mock.patch.object(analyzer.sys, "modules", {"native": analyzer.native}), \
                mock.patch.object(analyzer.subprocess, "run", side_effect=changed_import), \
                self.assertRaisesRegex(analyzer.EvidenceError, "producer import"):
            analyzer.validate_consumer_source(COMMIT, analyzer_commit=repair)

    def test_analyzer_commit_override_is_normalized_only(self):
        with mock.patch.object(analyzer, "load_packet", return_value=(Path("packet"), {
                "schema": analyzer.PACKET_SCHEMA, "candidate_commit": COMMIT})), \
                self.assertRaisesRegex(analyzer.EvidenceError, "normalized-v4"):
            analyzer._analyze("packet", "a" * 64, lambda _: None, analyzer_commit="b" * 40)

    def test_inventory_has_one_path_and_hash_per_semantic_role(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arms = {role: "arm_" + role for role in analyzer.ARM_KINDS}
            plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
            dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
            inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
            logical = [*arms.values(), *plans.values(), *dataset.values(), *inputs.values()]
            files = {}
            for number, name in enumerate(logical):
                raw = f"{number}:{name}".encode()
                (root / name).write_bytes(raw)
                files[name] = {"path": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            packet = {"files": files, "arms": arms, "plans": plans,
                      "dataset": dataset, "inputs": inputs}
            resolved = analyzer.resolve_inventory(root / "packet.json", packet)
            self.assertEqual(set(resolved), set(logical))

            changed = copy.deepcopy(packet)
            second = logical[1]
            changed["files"][second]["path"] = changed["files"][logical[0]]["path"]
            with self.assertRaisesRegex(analyzer.EvidenceError, "same path"):
                analyzer.resolve_inventory(root / "packet.json", changed)
            changed = copy.deepcopy(packet)
            changed["files"][second]["sha256"] = changed["files"][logical[0]]["sha256"]
            with self.assertRaisesRegex(analyzer.EvidenceError, "unique"):
                analyzer.resolve_inventory(root / "packet.json", changed)
            fifo = root / logical[0]
            fifo.unlink()
            os.mkfifo(fifo)
            with self.assertRaisesRegex(analyzer.EvidenceError, "regular file"):
                analyzer.resolve_inventory(root / "packet.json", packet)

    def test_go_validator_keeps_exact_native_and_sq8_plan_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exact = root / "exact.json"
            sq8 = root / "sq8.json"
            plan = root / "plan.json"
            manifest_path = root / "manifest.json"
            serving_path = root / "serving.json"
            binary = root / "validator"
            service = root / "service"
            serving = {}
            manifest = bounded_manifest()
            service.write_bytes(b"service")
            service_sha = hashlib.sha256(b"service").hexdigest()
            plan_json = bounded_plan(manifest, serving)
            plan.write_bytes(canonical(plan_json))
            plan_sha = hashlib.sha256(plan.read_bytes()).hexdigest()
            manifest_path.write_bytes(canonical(manifest))
            serving_path.write_bytes(canonical(serving))
            exact.write_bytes(canonical(clean_bounded_artifact(
                "exact", manifest, service, service_sha,
            )))
            sq8.write_bytes(canonical(clean_bounded_artifact(
                "sq8", manifest, service, service_sha, plan_sha,
            )))
            binary.write_bytes(b"binary")
            packet = {
                "candidate_commit": COMMIT,
                "inputs": {
                    "bounded_validator_binary": "validator",
                    "bounded_manifest": "manifest",
                    "bounded_sq8_plan": "plan",
                    "treedb_service_binary": "service",
                    "serving": "serving",
                },
                "arms": {"bounded_exact": "exact", "bounded_sq8": "sq8"},
                "files": {
                    "plan": {"sha256": plan_sha},
                    "service": {"sha256": service_sha},
                },
            }
            paths = {
                "validator": binary, "manifest": manifest_path, "plan": plan,
                "service": service, "serving": serving_path, "exact": exact, "sq8": sq8,
            }
            calls = []
            baseline = analyzer.validate_bounded_artifacts(packet, paths, calls.append)
            self.assertEqual(baseline["classification"], "completed_clean")
            self.assertEqual(len(calls), 2)
            self.assertNotIn("-minima-quantized-plan", calls[0])
            self.assertIn("-minima-quantized-plan", calls[1])
            self.assertEqual(calls[0][-1], COMMIT)
            self.assertEqual(calls[1][-1], COMMIT)

            sq8_clean = json.loads(sq8.read_text())
            changed = copy.deepcopy(sq8_clean)
            changed["raw_evidence"] = changed.pop("backend_raw_evidence")
            sq8.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "completed clean"):
                analyzer.validate_bounded_artifacts(packet, paths, calls.append)
            sq8.write_bytes(canonical(sq8_clean))

            changed_plan = copy.deepcopy(plan_json)
            changed_plan["ef_construction"] = 64
            plan.write_bytes(canonical(changed_plan))
            with self.assertRaisesRegex(analyzer.EvidenceError, "frozen manifest and profile"):
                analyzer.validate_bounded_artifacts(packet, paths, calls.append)
            plan.write_bytes(canonical(plan_json))

            changed = json.loads(exact.read_text())
            changed["manifest"]["fixture"] = "bounded-250k"
            exact.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "inventoried manifest"):
                analyzer.validate_bounded_artifacts(packet, paths, calls.append)

            exact.write_bytes(canonical(known_legacy_failure_artifact(
                manifest, service, service_sha,
            )))
            baseline = analyzer.validate_bounded_artifacts(packet, paths, calls.append)
            self.assertEqual(
                baseline["classification"], "known_legacy_complete_finite_ann_failure",
            )
            self.assertFalse(baseline["lifecycle_claim_available"])
            self.assertFalse(baseline["latency_claim_available"])

            changed = json.loads(exact.read_text())
            changed["native_path_proof"]["strategy"] = "column_graph"
            exact.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "native_runtime"):
                analyzer.validate_bounded_artifacts(packet, paths, calls.append)

    def test_frozen_legacy_baseline_failure_is_narrow_and_uses_real_raw_key(self):
        self.assertEqual(analyzer.BOUNDED_MANIFEST_SCHEMA, "treedb_rag_minima_manifest/v2")
        self.assertEqual(
            analyzer.BOUNDED_ARTIFACT_SCHEMA,
            "treedb_rag_application/minima_diagnostic_v1",
        )
        self.assertEqual(
            analyzer.LEGACY_BASELINE_CLASSIFICATION,
            "known_legacy_complete_finite_ann_failure",
        )
        self.assertEqual(analyzer.LEGACY_BASELINE_FAILURES, FROZEN_LEGACY_FAILURES)
        self.assertEqual(analyzer.LEGACY_BASELINE_IDS, FROZEN_LEGACY_IDS)
        self.assertEqual(analyzer._legacy_baseline_routes(), frozen_legacy_routes())
        manifest = bounded_manifest()
        service = Path("/packet/service")
        artifact = known_legacy_failure_artifact(manifest, service, "a" * 64)
        backend = artifact["backends"][0]
        self.assertTrue(analyzer._known_legacy_baseline_failure(artifact, backend))

        def drift_tolerance(row, field, value):
            row["manifest"]["config"][field] = value
            for scenario in row["scenarios"]:
                scenario[field] = value

        mutations = {
            "extra failure": lambda row: row["failures"].append("timeout"),
            "wrong actual IDs": lambda row: row["scenarios"][3].update(
                initial_actual_ids=["other"]),
            "wrong declared initial IDs": lambda row: row["scenarios"][0].update(
                initial_oracle_ids=["other"]),
            "wrong declared initial scores": lambda row: row["scenarios"][0].update(
                initial_oracle_scores=[0.8]),
            "wrong declared final IDs": lambda row: row["scenarios"][0].update(
                final_oracle_ids=["other"]),
            "wrong declared final scores": lambda row: row["scenarios"][0].update(
                final_oracle_scores=[0.8]),
            "wrong order tolerance": lambda row: row["scenarios"][0].update(
                order_tolerance=1),
            "boolean order tolerance": lambda row: row["scenarios"][0].update(
                order_tolerance=False),
            "wrong score tolerance": lambda row: row["scenarios"][0].update(
                score_tolerance=0.1),
            "coordinated order tolerance drift": lambda row: drift_tolerance(
                row, "order_tolerance", 1),
            "coordinated score tolerance drift": lambda row: drift_tolerance(
                row, "score_tolerance", 0.1),
            "wrong recall summary": lambda row: row["scenarios"][0].update(recall=1.0),
            "wrong overlap summary": lambda row: row["scenarios"][6].update(overlap=0.0),
            "wrong plan": lambda row: row["scenarios"][3]["route"].update(plan="complete_exact"),
            "cleanup claim": lambda row: row["backend_raw_evidence"]["treedb"].update(
                final_scroll_state={"match": True}),
            "other mismatch": lambda row: row["backend_raw_evidence"]["treedb"]["events"][0].update(
                match=False),
            "bad score": lambda row: row["scenarios"][0].update(initial_actual_scores=[None]),
            "bad score delta": lambda row: row["backend_raw_evidence"]["treedb"]["events"][0].update(
                maximum_score_delta=None),
            "wrong raw key": lambda row: row.update(
                raw_evidence=row.pop("backend_raw_evidence")),
            "boolean zero route field": lambda row: row["scenarios"][1]["route"].update(
                primary_document_scans=False),
            "float deterministic work": lambda row: (
                row["scenarios"][0]["route"].update(scored_candidates=16.0),
                row["backend_raw_evidence"]["treedb"]["native_route_responses"][
                    "small"
                ].update(candidates=16.0, scored=16.0),
            ),
            "numeric false passing": lambda row: row.update(passing=0),
        }
        for label, mutate in mutations.items():
            changed = copy.deepcopy(artifact)
            mutate(changed)
            with self.subTest(label=label):
                self.assertFalse(analyzer._known_legacy_baseline_failure(
                    changed, changed["backends"][0],
                ))

    def test_legacy_ann_work_accepts_source_bounds_and_reconciles_every_copy(self):
        manifest = bounded_manifest()
        artifact = known_legacy_failure_artifact(manifest, Path("/packet/service"), "a" * 64)

        accepted = {
            "all_match": (32, 32),
            "over_limit_4097": (4096, 8192),
            "broad_10pct": (2048, 2048),
            "sparse_over_limit": (4096, 4096),
        }
        for name, (scored, visited) in accepted.items():
            changed = copy.deepcopy(artifact)
            set_legacy_ann_work(changed, name, scored, visited)
            with self.subTest(accepted=name):
                self.assertTrue(analyzer._known_legacy_baseline_failure(
                    changed, changed["backends"][0],
                ))

        rejected = {
            "zero scored": ("all_match", 0, 0),
            "negative scored": ("all_match", -1, -1),
            "scored below admitted": ("all_match", 4, 4),
            "scored below vector seed workload": ("all_match", 31, 31),
            "oversized scored": ("all_match", 4097, 4097),
            "finite seed work": ("broad_10pct", 2048, 2049),
            "visited below scored": ("all_match", 2048, 2047),
            "excessive seed work": ("all_match", 2048, 6145),
        }
        for label, (name, scored, visited) in rejected.items():
            changed = copy.deepcopy(artifact)
            set_legacy_ann_work(changed, name, scored, visited)
            with self.subTest(rejected=label):
                self.assertFalse(analyzer._known_legacy_baseline_failure(
                    changed, changed["backends"][0],
                ))

        for value in (False, 2064.0, "2064", None):
            for owner, field in (
                ("scenario", "scored_candidates"),
                ("scenario", "visited_candidates"),
                ("raw", "candidates"), ("raw", "scored"), ("raw", "visited"),
            ):
                changed = copy.deepcopy(artifact)
                scenario = next(
                    row for row in changed["scenarios"] if row["scenario"] == "all_match"
                )
                raw = changed["backend_raw_evidence"]["treedb"][
                    "native_route_responses"
                ]["all_match"]
                (scenario["route"] if owner == "scenario" else raw)[field] = value
                with self.subTest(type=type(value).__name__, owner=owner, field=field):
                    self.assertFalse(analyzer._known_legacy_baseline_failure(
                        changed, changed["backends"][0],
                    ))

        for label, mutate in {
            "raw scored mismatch": lambda row: row["backend_raw_evidence"]["treedb"][
                "native_route_responses"
            ]["all_match"].update(scored=2063),
            "raw candidates mismatch": lambda row: row["backend_raw_evidence"]["treedb"][
                "native_route_responses"
            ]["all_match"].update(candidates=2063),
            "raw visited mismatch": lambda row: row["backend_raw_evidence"]["treedb"][
                "native_route_responses"
            ]["all_match"].update(visited=2079),
            "missing counter": lambda row: row["scenarios"][1]["route"].pop(
                "scored_candidates"
            ),
            "extra route field": lambda row: row["scenarios"][1]["route"].update(
                completed=True
            ),
            "coordinated admission drift": lambda row: (
                row["scenarios"][1]["route"].update(admitted_candidates=4),
                row["backend_raw_evidence"]["treedb"]["native_route_responses"][
                    "all_match"
                ].update(admitted=4),
            ),
        }.items():
            changed = copy.deepcopy(artifact)
            mutate(changed)
            with self.subTest(reconciliation=label):
                self.assertFalse(analyzer._known_legacy_baseline_failure(
                    changed, changed["backends"][0],
                ))

    def test_legacy_failure_rejects_completion_claims_and_malformed_shapes(self):
        artifact = known_legacy_failure_artifact(
            bounded_manifest(), Path("/packet/service"), "a" * 64,
        )
        raw = lambda row: row["backend_raw_evidence"]["treedb"]
        mutations = {
            "timed overlap": lambda row: raw(row).update(timed_overlap={"completed": True}),
            "diagnostic resume": lambda row: raw(row).update(
                diagnostic_resume={"resumed": True}),
            "correlation evidence": lambda row: raw(row)["upsert_batch_correlations"].append(
                {"outcome": "completed"}),
            "correlation claim": lambda row: raw(row)[
                "upsert_batch_correlation_contract"
            ].update(record_count=1),
            "enabled diagnostics": lambda row: raw(row)["diagnostics"].update(enabled=True),
            "later lifecycle phase": lambda row: raw(row)["phase_attribution"]["phases"][
                -1
            ].update(name="lifecycle_mutations"),
            "phase completion field": lambda row: raw(row)["phase_attribution"]["phases"][
                -1
            ].update(completed=True),
            "zero duration phase hidden as bookkeeping": lambda row: (
                raw(row)["phase_attribution"]["phases"][0].update(
                    end_nanos=100, duration_nanos=0,
                ),
                raw(row)["phase_attribution"].update(unattributed_nanos=40),
            ),
            "extra raw claim": lambda row: raw(row).update(lifecycle_complete=True),
            "malformed manifest": lambda row: row.update(manifest=[]),
            "malformed query": lambda row: row["manifest"]["queries"].__setitem__(0, None),
            "malformed scenario": lambda row: row["scenarios"].__setitem__(0, None),
            "malformed raw route": lambda row: raw(row)["native_route_responses"].update(
                all_match=None),
            "malformed phase": lambda row: raw(row)["phase_attribution"]["phases"].__setitem__(
                0, None),
            "malformed phase resource": lambda row: raw(row)["phase_attribution"]["phases"][
                0
            ].update(resource_segments=[None]),
        }
        for label, mutate in mutations.items():
            changed = copy.deepcopy(artifact)
            mutate(changed)
            with self.subTest(label=label):
                self.assertFalse(analyzer._known_legacy_baseline_failure(
                    changed, changed["backends"][0],
                ))

    def test_receipts_bind_dataset_outputs_and_typed_unqualified_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "receipts.json"
            arms = {role: "arm_" + role for role in analyzer.ARM_KINDS}
            plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
            dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
            inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
            logical = [*arms.values(), *plans.values(), *dataset.values(), *inputs.values()]
            files = {name: {"sha256": f"{number + 10:064x}"}
                     for number, name in enumerate(logical)}
            dataset_dir = root / "dataset"
            paths = {
                dataset["manifest"]: dataset_dir / "manifest.json",
                dataset["documents"]: dataset_dir / "documents.f32",
                dataset["queries"]: dataset_dir / "queries.f32",
                dataset["truth"]: dataset_dir / "truth.json",
                inputs["bounded_validator_binary"]: root / "bin" / "validator",
                inputs["bounded_manifest"]: root / "bounded" / "manifest.json",
                inputs["bounded_sq8_plan"]: root / "inputs" / "bounded-plan.json",
                inputs["treedb_service_binary"]: root / "bin" / "service",
                inputs["serving"]: root / "inputs" / "serving.json",
                inputs["qdrant_binary"]: root / "bin" / "qdrant",
            }
            native_roles = analyzer.PLAN_KINDS - {"qdrant_fp32_rss"}
            for role in native_roles:
                filename = ("rss.json" if role in {"treedb_fp32_rss", "treedb_sq8_rss"}
                            else "events.jsonl")
                paths[arms[role]] = root / "runs" / role / filename
            qdrant_run = root / "runs" / "qdrant_fp32_rss"
            paths.update({
                arms["qdrant_fp32_rss"]: qdrant_run / "qdrant-rss.json",
                arms["fp32_comparison"]: qdrant_run / "comparison.json",
                arms["three_arm_comparison"]: qdrant_run / "three-arm-comparison.json",
                arms["bounded_exact"]: root / "runs" / "bounded-exact.json",
                arms["bounded_sq8"]: root / "runs" / "bounded-sq8.json",
                arms["command_receipts"]: path,
            })
            environment = {name: "" for name in analyzer.REQUIRED_RECEIPT_ENV}
            environment.update(
                HOME="/home/test", PATH="/usr/bin", TMPDIR="/tmp/q5", TZ="UTC",
                LANG="C.UTF-8", LC_ALL="C.UTF-8", GOWORK="off", GOMAXPROCS="6",
                PYTHONHASHSEED="0", PYTHONDONTWRITEBYTECODE="1",
                OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1",
            )
            plan_paths = {role: root / "plans" / (plans[role] + ".json")
                          for role in analyzer.PLAN_KINDS}
            for role, plan_path in plan_paths.items():
                plan_path.parent.mkdir(parents=True, exist_ok=True)
                paths[plans[role]] = plan_path
            dataset_hashes = {
                role: files[dataset[role]]["sha256"]
                for role in ("documents", "queries", "truth")
            }
            runtime = {key: environment[key]
                       for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")}
            for role, plan_path in plan_paths.items():
                if role == "qdrant_fp32_rss":
                    plan = {
                        "dataset": str(dataset_dir),
                        "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                        "dataset_files_sha256": dataset_hashes,
                        "treedb_artifact": str(paths[arms["treedb_fp32_rss"]]),
                        "treedb_artifact_sha256": files[arms["treedb_fp32_rss"]]["sha256"],
                        "treedb_sq8_artifact": str(paths[arms["treedb_sq8_rss"]]),
                        "treedb_sq8_artifact_sha256": files[arms["treedb_sq8_rss"]]["sha256"],
                        "qdrant_bin": str(paths[inputs["qdrant_binary"]]),
                        "qdrant_bin_sha256": files[inputs["qdrant_binary"]]["sha256"],
                        "storage_path": str(qdrant_run / "storage"),
                        "url": "http://127.0.0.1:18140", "collection": "minima_q5",
                        "run_dir": str(qdrant_run), "operation_timeout_s": 600,
                        "startup_timeout_s": 120.0, "optimizer_timeout_s": 2700.0,
                        "poll_interval_s": .25,
                        "rows": 500000, "dimensions": 768, "top_k": 10,
                        "batch_size": 256, "controls": analyzer.native.RSS_CONTROLS,
                        "cpu_affinity": list(range(6)), "platform": "test-linux",
                        "comparison_contract": {
                            "treedb_go_runtime": runtime,
                            "rows": 500000, "dimensions": 768, "top_k": 10,
                            "batch_size": 256,
                            "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                            "dataset_files_sha256": dataset_hashes,
                            "cpu_affinity": list(range(6)), "platform": "test-linux",
                            "ann_controls": {
                                "treedb_ef_search": analyzer.native.RSS_CONTROLS,
                                "qdrant_hnsw_ef": analyzer.native.RSS_CONTROLS,
                            },
                        },
                    }
                else:
                    quantized = role in {
                        "smoke_sq8", "treedb_sq8_rss", "full_sq8_events",
                    }
                    rss_only = role in {"treedb_fp32_rss", "treedb_sq8_rss"}
                    plan = {
                        "dataset": str(dataset_dir),
                        "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                        "dataset_files_sha256": dataset_hashes,
                        "service_bin": str(paths[inputs["treedb_service_binary"]]),
                        "service_sha256": files[inputs["treedb_service_binary"]]["sha256"],
                        "product_commit": COMMIT,
                        "serving_path": str(paths[inputs["serving"]]),
                        "serving_sha256": files[inputs["serving"]]["sha256"],
                        "run_dir": str(paths[arms[role]].parent),
                        "rows": 512 if role.startswith("smoke_") else 500000,
                        "rss_only": rss_only,
                        "ef_construction": analyzer.native.DEFAULT_EF_CONSTRUCTION,
                        "construction_decisions": False,
                        "url": f"http://127.0.0.1:{18020 + len(role)}",
                        "native_address": f"127.0.0.1:{18120 + len(role)}",
                        "diagnostics_url": f"http://127.0.0.1:{18220 + len(role)}",
                        "treedb_go_runtime": runtime,
                        "cpu_affinity": list(range(6)),
                    }
                    if quantized:
                        plan.update(
                            query_mode="quantized_rerank",
                            quantized_index_name=analyzer.native.QUANTIZED_PROFILE_NAME,
                        )
                    if role == "full_sq8_events":
                        plan["all_rows_coordinate_lock"] = {
                            "artifact_path": str(paths[arms["treedb_sq8_rss"]]),
                            "artifact_sha256": files[arms["treedb_sq8_rss"]]["sha256"],
                        }
                plan["blas_threads"] = {
                    key: environment[key]
                    for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS")
                }
                plan_path.write_bytes(canonical(plan))

            def bounded_command(role):
                argv = [str(paths[inputs["bounded_validator_binary"]]), "-workload=minima",
                        "-validate-minima-artifact", str(paths[arms[role]])]
                if role == "bounded_sq8":
                    argv.extend([
                        "-minima-quantized-plan", str(paths[inputs["bounded_sq8_plan"]]),
                        "-minima-expected-quantized-plan-sha256",
                        files[inputs["bounded_sq8_plan"]]["sha256"],
                    ])
                return [*argv, "-minima-expected-commit", COMMIT]

            def plan_command(role, mode):
                plan = json.loads(plan_paths[role].read_text())
                script = analyzer.qdrant.__file__ if role == "qdrant_fp32_rss" \
                    else analyzer.native.__file__
                argv = [
                    "/usr/bin/taskset", "-c", "0-5",
                    str(Path(analyzer.sys.executable).resolve()), str(Path(script).resolve()),
                ]
                if role == "qdrant_fp32_rss":
                    options = (
                        ("--dataset", plan["dataset"]),
                        ("--treedb-artifact", plan["treedb_artifact"]),
                        ("--treedb-sq8-artifact", plan["treedb_sq8_artifact"]),
                        ("--qdrant-bin", plan["qdrant_bin"]),
                        ("--storage-path", plan["storage_path"]), ("--url", plan["url"]),
                        ("--collection", plan["collection"]), ("--run-dir", plan["run_dir"]),
                        ("--operation-timeout", plan["operation_timeout_s"]),
                        ("--startup-timeout", plan["startup_timeout_s"]),
                        ("--optimizer-timeout", plan["optimizer_timeout_s"]),
                        ("--poll-interval", plan["poll_interval_s"]),
                    )
                else:
                    options = [
                        ("--dataset", plan["dataset"]), ("--service-bin", plan["service_bin"]),
                        ("--product-commit", COMMIT), ("--serving", plan["serving_path"]),
                        ("--run-dir", plan["run_dir"]), ("--rows", plan["rows"]),
                        ("--query-mode", plan.get("query_mode", "exact")),
                        ("--ef-construction", plan["ef_construction"]),
                        ("--url", plan["url"]), ("--native-address", plan["native_address"]),
                        ("--diagnostics-url", plan["diagnostics_url"]),
                    ]
                    if plan["rss_only"]:
                        argv.append("--rss-only")
                    if plan.get("query_mode") == "quantized_rerank":
                        options.append(("--quantized-index-name", plan["quantized_index_name"]))
                    if role == "full_sq8_events":
                        lock = plan["all_rows_coordinate_lock"]
                        options.extend([
                            ("--all-rows-sq8-rss-artifact", lock["artifact_path"]),
                            ("--expected-all-rows-sq8-rss-artifact-sha256",
                             lock["artifact_sha256"]),
                        ])
                for option, value in options:
                    argv.extend([option, str(value)])
                argv.extend([f"--{mode}", str(plan_paths[role])])
                if mode == "run":
                    argv.extend([
                        "--expected-plan-sha256", files[plans[role]]["sha256"],
                    ])
                return argv

            runs = {
                role: {"freeze_argv": (plan_command(role, "freeze")
                                        if role in analyzer.PLAN_KINDS else None),
                       "freeze_exit_code": 0 if role in analyzer.PLAN_KINDS else None,
                       "run_argv": (plan_command(role, "run")
                                    if role in analyzer.PLAN_KINDS else bounded_command(role)),
                       "cwd": str(root), "environment": environment, "umask": "0022",
                       "started_utc": "2026-09-15T00:00:00Z",
                       "ended_utc": "2026-09-15T00:01:00Z",
                       "exit_code": 1 if role == "full_sq8_events" else 0,
                       "plan_sha256": (files[plans[role]]["sha256"]
                                       if role in analyzer.PLAN_KINDS else None),
                       "output_sha256": files[arms[role]]["sha256"]}
                for role in analyzer.RUN_ARM_KINDS
            }
            runs["bounded_exact"].update(
                started_utc="2026-09-15T00:01:00Z", ended_utc="2026-09-15T00:02:00Z",
            )
            runs["bounded_sq8"].update(
                started_utc="2026-09-15T00:04:00Z", ended_utc="2026-09-15T00:05:00Z",
            )
            runner_prefix = [
                "/usr/bin/taskset", "-c", "0-5", str(Path(analyzer.sys.executable).resolve()),
                str(Path(analyzer.__file__).with_name("minima_treedb_runner.py").resolve()),
            ]
            bounded_manifest_path = paths[inputs["bounded_manifest"]]
            exact_commands = [
                {
                    "argv": [
                        str(paths[inputs["bounded_validator_binary"]]), "-workload=minima",
                        "-dump-minima-manifest", str(bounded_manifest_path),
                        "-minima-bounded-total-rows", "50000",
                    ],
                    "started_utc": "2026-09-15T00:00:00Z",
                    "ended_utc": "2026-09-15T00:00:01Z", "exit_code": 0,
                    "output_sha256": files[inputs["bounded_manifest"]]["sha256"],
                },
                {
                    "argv": [
                        *runner_prefix, "--manifest", str(bounded_manifest_path),
                        "--output", str(paths[arms["bounded_exact"]]),
                        "--service-bin", str(paths[inputs["treedb_service_binary"]]),
                        "--url", "http://127.0.0.1:18040",
                        "--data-dir", str(bounded_manifest_path.parent / "exact-db"),
                        "--collection", f"minima_q5_bounded_exact_{COMMIT[:12]}",
                        "--profile", "command_wal_durable", "--strategy", "native_runtime",
                        "--operation-timeout", "120", "--startup-timeout", "120",
                        "--ef-search", "128",
                    ],
                    "started_utc": "2026-09-15T00:00:01Z",
                    "ended_utc": "2026-09-15T00:01:00Z", "exit_code": 0,
                    "output_sha256": files[arms["bounded_exact"]]["sha256"],
                },
            ]
            sq8_common = [
                *runner_prefix, "--manifest", str(bounded_manifest_path),
                "--strategy", "column_graph", "--transport", "native",
                "--column-graph-serving", str(paths[inputs["serving"]]),
                "--profile", "command_wal_durable", "--ef-construction", "32",
                "--query-mode", "quantized_rerank", "--quantized-index-name", "minima_sq8",
                "--ef-search", "64", "--quantized-rerank-candidates", "64",
            ]
            sq8_commands = [
                {
                    "argv": [*sq8_common, "--write-quantized-plan",
                             str(paths[inputs["bounded_sq8_plan"]])],
                    "started_utc": "2026-09-15T00:02:00Z",
                    "ended_utc": "2026-09-15T00:02:01Z", "exit_code": 0,
                    "output_sha256": files[inputs["bounded_sq8_plan"]]["sha256"],
                },
                {
                    "argv": [
                        *sq8_common, "--quantized-plan",
                        str(paths[inputs["bounded_sq8_plan"]]),
                        "--expected-quantized-plan-sha256",
                        files[inputs["bounded_sq8_plan"]]["sha256"],
                        "--output", str(paths[arms["bounded_sq8"]]),
                        "--service-bin", str(paths[inputs["treedb_service_binary"]]),
                        "--url", "http://127.0.0.1:18050",
                        "--native-address", "127.0.0.1:18052",
                        "--data-dir", str(bounded_manifest_path.parent / "sq8-db"),
                        "--collection", f"minima_q5_bounded_sq8_{COMMIT[:12]}",
                        "--operation-timeout", "120", "--startup-timeout", "120",
                    ],
                    "started_utc": "2026-09-15T00:02:01Z",
                    "ended_utc": "2026-09-15T00:04:00Z", "exit_code": 0,
                    "output_sha256": files[arms["bounded_sq8"]]["sha256"],
                },
            ]
            preparations = {
                role: {
                    "cwd": str(root), "environment": copy.deepcopy(environment),
                    "umask": "0022", "commands": commands,
                }
                for role, commands in (("bounded_exact", exact_commands),
                                       ("bounded_sq8", sq8_commands))
            }
            receipts = {
                "schema": analyzer.RECEIPT_SCHEMA, "candidate_commit": COMMIT,
                "dataset_manifest_sha256": files[dataset["manifest"]]["sha256"],
                "dataset_files_sha256": dataset_hashes,
                "preparations": preparations, "runs": runs,
            }
            path.write_bytes(canonical(receipts))
            packet = {"candidate_commit": COMMIT, "declared_quality_outcome": "miss",
                      "arms": arms, "plans": plans, "dataset": dataset,
                      "inputs": inputs, "files": files}
            analyzer.validate_receipts(
                packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                {"classification": "completed_clean"},
            )
            for mutation in (
                    "exit", "environment", "environment_extra", "environment_value",
                    "environment_path", "plan", "argv", "matching_extra",
                    "missing_input", "duplicate", "affinity", "qdrant_api_key",
                    "producer_exit", "producer_argv", "producer_environment",
                    "producer_order", "validator_order", "missing_preparation"):
                changed = copy.deepcopy(receipts)
                if mutation == "exit":
                    changed["runs"]["full_sq8_events"]["exit_code"] = 0
                elif mutation == "environment":
                    del changed["runs"]["smoke_exact"]["environment"]["TMPDIR"]
                elif mutation == "environment_extra":
                    changed["runs"]["smoke_exact"]["environment"]["UNCONTROLLED"] = "1"
                elif mutation == "environment_value":
                    changed["runs"]["smoke_exact"]["environment"]["PYTHONHASHSEED"] = "random"
                elif mutation == "environment_path":
                    changed["runs"]["smoke_exact"]["environment"]["PATH"] = "bin:/usr/bin"
                elif mutation == "argv":
                    changed["runs"]["bounded_exact"]["run_argv"].append("--unexpected")
                elif mutation == "matching_extra":
                    changed["runs"]["smoke_exact"]["freeze_argv"].extend([
                        "--unexpected", "same",
                    ])
                    changed["runs"]["smoke_exact"]["run_argv"].extend([
                        "--unexpected", "same",
                    ])
                elif mutation == "missing_input":
                    for command in ("freeze_argv", "run_argv"):
                        argv = changed["runs"]["smoke_exact"][command]
                        index = argv.index("--dataset")
                        del argv[index:index + 2]
                elif mutation == "duplicate":
                    changed["runs"]["smoke_exact"]["freeze_argv"].extend([
                        "--rows", "512",
                    ])
                    changed["runs"]["smoke_exact"]["run_argv"].extend([
                        "--rows", "512",
                    ])
                elif mutation == "affinity":
                    changed["runs"]["smoke_exact"]["freeze_argv"][2] = "1-5"
                    changed["runs"]["smoke_exact"]["run_argv"][2] = "1-5"
                elif mutation == "qdrant_api_key":
                    for command in ("freeze_argv", "run_argv"):
                        changed["runs"]["qdrant_fp32_rss"][command].extend([
                            "--api-key", "secret",
                        ])
                elif mutation == "producer_exit":
                    changed["preparations"]["bounded_exact"]["commands"][1]["exit_code"] = 1
                elif mutation == "producer_argv":
                    changed["preparations"]["bounded_sq8"]["commands"][1]["argv"].append(
                        "--unexpected",
                    )
                elif mutation == "producer_environment":
                    changed["preparations"]["bounded_exact"]["environment"]["UNCONTROLLED"] = "1"
                elif mutation == "producer_order":
                    changed["preparations"]["bounded_sq8"]["commands"][0][
                        "started_utc"] = "2026-09-15T00:00:30Z"
                elif mutation == "validator_order":
                    changed["runs"]["bounded_sq8"]["started_utc"] = "2026-09-15T00:03:00Z"
                elif mutation == "missing_preparation":
                    del changed["preparations"]["bounded_sq8"]
                else:
                    changed["runs"]["smoke_exact"]["plan_sha256"] = "f" * 64
                path.write_bytes(canonical(changed))
                with self.subTest(mutation=mutation), self.assertRaises(analyzer.EvidenceError):
                    analyzer.validate_receipts(
                        packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                        {"classification": "completed_clean"},
                    )

            known_failure_receipt = copy.deepcopy(receipts)
            known_failure_receipt["preparations"]["bounded_exact"]["commands"][1][
                "exit_code"] = 1
            path.write_bytes(canonical(known_failure_receipt))
            analyzer.validate_receipts(
                packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                {"classification": analyzer.LEGACY_BASELINE_CLASSIFICATION},
            )

            qdrant_plan = json.loads(plan_paths["qdrant_fp32_rss"].read_text())
            changed_plan = copy.deepcopy(qdrant_plan)
            changed_plan["cpu_affinity"] = list(range(1, 6))
            plan_paths["qdrant_fp32_rss"].write_bytes(canonical(changed_plan))
            changed = copy.deepcopy(receipts)
            for command in ("freeze_argv", "run_argv"):
                changed["runs"]["qdrant_fp32_rss"][command][2] = "1-5"
            path.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "comparison"):
                analyzer.validate_receipts(
                    packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                    {"classification": "completed_clean"},
                )
            plan_paths["qdrant_fp32_rss"].write_bytes(canonical(qdrant_plan))

            outside = root / "outside" / "service-copy"
            outside.parent.mkdir()
            paths[inputs["treedb_service_binary"]].parent.mkdir(parents=True, exist_ok=True)
            paths[inputs["treedb_service_binary"]].write_bytes(b"same binary")
            outside.write_bytes(b"same binary")
            smoke_plan = json.loads(plan_paths["smoke_exact"].read_text())
            smoke_plan["service_bin"] = str(outside)
            plan_paths["smoke_exact"].write_bytes(canonical(smoke_plan))
            changed = copy.deepcopy(receipts)
            for command in ("freeze_argv", "run_argv"):
                argv = changed["runs"]["smoke_exact"][command]
                argv[argv.index("--service-bin") + 1] = str(outside)
            path.write_bytes(canonical(changed))
            with self.assertRaisesRegex(analyzer.EvidenceError, "packet inventory"):
                analyzer.validate_receipts(
                    packet, paths, receipts["dataset_manifest_sha256"], dataset_hashes,
                    {"classification": "completed_clean"},
                )

    def _full_events(self, include_retune=False, quality_pass=True):
        truth = {"500000": [
            [f"row-{query * 10 + rank:06d}" for rank in range(10)]
            for query in range(200)
        ]}
        plan = {
            "schema": analyzer.native.SCHEMA, "qualification": "not_evaluated",
            "mode": "diagnostic", "rows": 500000, "dimensions": 768, "queries": 200,
            "top_k": 10, "batch_size": 256, "query_mode": "quantized_rerank",
            "rss_only": False, "product_commit": COMMIT,
            "harness_commit": COMMIT, "dataset_manifest_sha256": "0" * 64,
            "dataset_files_sha256": {
                "documents": "1" * 64, "queries": "2" * 64, "truth": "3" * 64,
            },
            "eligible_counts": analyzer.FULL_ELIGIBLE_COUNTS,
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": list(range(100)),
            "rss_revalidation_queries": list(range(100, 200)),
            "rss_recall_target": .9,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
            "quantized_index_name": analyzer.native.QUANTIZED_PROFILE_NAME,
            "quantized_coordinate_policy": "ordered_ef_grid_with_requested_rerank_candidates_equal_ef",
            "representation_arm": analyzer.native.quantized_representation_arm(),
            "paired_timing": analyzer.native.paired_timing_plan(200),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "overlap_eligible": 4097, "reader_concurrency": 4, "writer_calls": 8,
            "wall_limit_s": 2700, "minimum_free_bytes": 10 * analyzer.native.GIB,
            "maximum_output_bytes": 11 * analyzer.native.GIB,
            "maximum_combined_rss_bytes": 24 * analyzer.native.GIB,
        }
        coordinate = {"ef_search": 32, "rerank_candidates": 32}
        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        plan["all_rows_coordinate_lock"] = {
            "schema": "treedb_cohere_sq8_coordinate_lock/v1",
            "artifact_path": "/frozen/sq8.json", "artifact_sha256": "4" * 64,
            "artifact_schema": analyzer.native.QUANTIZED_RSS_ARTIFACT_SCHEMA,
            "source_process_identity": "10:20", "selected_coordinate": coordinate,
        }
        events = [{"event": "plan", "plan": plan}, {
            "event": "resource", "elapsed_s": 1.0,
            "free_bytes": 20 * analyzer.native.GIB, "output_bytes": 1,
            "combined_rss_bytes": 1,
        }]
        self._append_construction_calls(events, 500000)
        events.append({
            "event": "coordinate_lock_consumed", "source_artifact_sha256": "4" * 64,
            "source_process_identity": "10:20", "fresh_process_identity": "11:21",
            "coordinate": coordinate,
        })
        def append_search(event):
            start = len(events) * 10 + 1
            events.extend([{
                "event": "call", "phase": event["phase"], "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                "eligible": event["eligible"], "ef": event["ef"],
                "query": event["query"], "writer_active": False,
                "request_mode": "quantized_rerank",
            }, event])
        for query in range(200):
            ids = (truth["500000"][query] if quality_pass else
                   [f"row-{400000 + rank:06d}" for rank in range(10)])
            append_search({
                "event": "search_result", "phase": "locked_all_rows_revalidation",
                "eligible": 500000, "ef": 32, "rerank_candidates": 32, "query": query,
                "ids": ids, "recall": 1.0 if quality_pass else 0.0,
                "ndcg_at_10": 1.0 if quality_pass else 0.0,
                "score_plane": {"snapshot": copy.deepcopy(owner)},
            })
        if include_retune:
            events.append({
                "event": "search_result", "phase": "quality_selection", "eligible": 500000,
                "ef": 64, "rerank_candidates": 64, "query": 0,
                "ids": truth["500000"][0], "recall": 1.0, "ndcg_at_10": 1.0,
            })
        metric = 1.0 if quality_pass else 0.0
        fixed = lambda queries: {
            "queries": queries, "mean_recall_at_10": metric, "mean_ndcg_at_10": metric,
            "per_query_recall": [metric] * 100, "per_query_ndcg_at_10": [metric] * 100,
        }
        quality = {
            "schema": "treedb_cohere_sq8_locked_all_rows_confirmation/v1",
            "selection_protocol": "prior_sq8_rss_coordinate_revalidated_on_fresh_graph/v1",
            "coordinate": coordinate,
            "fixed_sets": {"queries_0_99": fixed(list(range(100))),
                           "queries_100_199": fixed(list(range(100, 200)))},
            "target_mean_recall_at_10": .9, "passed": quality_pass,
        }
        events.extend([
            {"event": "locked_all_rows_quality", **quality},
            {"event": "quantized_quality", "cohorts": {"500000": quality}},
        ])
        if quality_pass:
            events.extend([
                {"event": "paired_query_timing",
                 "artifact": {"selected_coordinate": copy.deepcopy(coordinate),
                              "warmup": {"common_owner": copy.deepcopy(owner)}}},
                {"event": "full_state_verified", "rows": 500000, "vectors_checked": 500000,
                 "normalized_full_vector_tolerance": 1e-6},
                {"event": "terminal", "lifecycle_complete": True,
                 "qualification": "producer_gates_passed", "error": None,
                 "process_lifetimes": [{"linux_process_identity": "11:21"}],
                 "final_disk_bytes": 1},
            ])
        else:
            error = "QualityUnqualified: quantized fixed-set quality failed for cohorts 500000"
            events.extend([
                {"event": "failure", "error": error},
                {"event": "terminal", "lifecycle_complete": False,
                 "qualification": "valid_unqualified", "error": error,
                 "process_lifetimes": [{"linux_process_identity": "11:21"}],
                 "final_disk_bytes": 1},
            ])
        return events, truth

    def _smoke_sq8_events(self):
        truth = {}
        for eligible in analyzer.native.counts(512):
            ids = []
            for ordinal in range(512):
                if (ordinal * 7919) % 512 < eligible:
                    ids.append(f"row-{ordinal:06d}")
                if len(ids) == 10:
                    break
            truth[str(eligible)] = [list(ids) for _ in range(4)]
        plan = {
            "schema": analyzer.native.SCHEMA, "qualification": "not_evaluated",
            "mode": "smoke", "rows": 512, "dimensions": 768, "queries": 4,
            "top_k": 10, "batch_size": 256, "query_mode": "quantized_rerank",
            "rss_only": False, "product_commit": COMMIT, "harness_commit": COMMIT,
            "dataset_manifest_sha256": "0" * 64,
            "dataset_files_sha256": {
                "documents": "1" * 64, "queries": "2" * 64, "truth": "3" * 64,
            },
            "eligible_counts": analyzer.native.counts(512),
            "rss_controls": analyzer.native.RSS_CONTROLS,
            "rss_calibration_queries": analyzer.native.RSS_CALIBRATION_QUERIES,
            "rss_revalidation_queries": analyzer.native.RSS_REVALIDATION_QUERIES,
            "rss_recall_target": analyzer.native.RSS_RECALL_TARGET,
            "ef_construction": 32, "construction_decisions": False,
            "construction_calibration_contract":
                analyzer.native.construction_calibration_contract(32),
            "paired_timing": analyzer.native.paired_timing_plan(4),
            "efs": [128, 256, 512, 1024, 2048], "overlap_ef": 512,
            "reader_concurrency": 4, "writer_calls": 8,
            "overlap_eligible": 65, "wall_limit_s": 2700,
            "minimum_free_bytes": 10 * analyzer.native.GIB,
            "maximum_output_bytes": 11 * analyzer.native.GIB,
            "maximum_combined_rss_bytes": 24 * analyzer.native.GIB,
        }
        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        events = [{"event": "plan", "plan": plan}, {
            "event": "resource", "elapsed_s": 1.0,
            "free_bytes": 20 * analyzer.native.GIB, "output_bytes": 1,
            "combined_rss_bytes": 1,
        }]
        self._append_construction_calls(events, 512)

        def append_search(phase, eligible, query, *, mode="quantized_rerank",
                          writer_active=False, ids=None):
            ids = truth[str(eligible)][query] if ids is None and eligible else (ids or [])
            start = len(events) * 10 + 1
            events.extend([{
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                "eligible": eligible, "ef": 32, "query": query,
                "writer_active": writer_active, "request_mode": mode,
            }, {
                "event": "search_result", "phase": phase, "eligible": eligible,
                "ef": 32, "query": query, "ids": ids, "recall": 1.0,
                "ndcg_at_10": 1.0, "writer_active": writer_active,
                "dense_work": {},
                **({"rerank_candidates": 32,
                    "score_plane": {"snapshot": copy.deepcopy(owner)}}
                   if mode == "quantized_rerank" else {}),
            }])

        def append_call(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        for eligible in [512, 64, 65, 128, 256]:
            for query in range(4):
                append_search(
                    "predicate_first_quality_query" if query == 0 else "quality_selection",
                    eligible, query,
                )
        cohorts = analyzer.native.select_quantized_cohorts(
            lambda eligible, _control, query: truth[str(eligible)][query],
            truth, plan["eligible_counts"], 512, analyzer.native.RSS_CONTROLS,
            [0, 1], [2, 3], 1.0,
        )
        events.append({"event": "quantized_quality", "cohorts": cohorts})

        batches = []
        sequence = 1
        for kind, repetition in [("warmup", -1), *[("measured", value) for value in range(5)]]:
            order = analyzer.native.paired_batch_arm_order(kind, repetition)
            arms = {}
            for mode in order:
                phase = analyzer.native.paired_phase(kind, repetition, mode)
                records = []
                for query in range(4):
                    ids = truth["512"][query]
                    append_search(phase, 512, query, mode=mode, ids=ids)
                    record = {
                        "request_sequence": sequence, "phase": phase,
                        "eligible": 512, "query": query, "requested_ef_search": 32,
                        "results": [{"id": identifier} for identifier in ids],
                        "recall": 1.0, "ndcg_at_10": 1.0, "dense_work": {},
                    }
                    if mode == "quantized_rerank":
                        record.update(
                            requested_rerank_candidates=32,
                            score_plane={"snapshot": copy.deepcopy(owner)},
                        )
                    records.append(record)
                    sequence += 1
                arms[mode] = {"requests": records}
            batches.append({
                "batch_kind": kind, "repetition": repetition,
                "arm_order": order, "common_owner": copy.deepcopy(owner), "arms": arms,
            })
        paired = {"warmup": batches[0], "repetitions": batches[1:]}
        events.append({"event": "paired_query_timing", "artifact": paired})
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("fixed_coordinate_curve", eligible, query)
        for worker in range(4):
            for number in range(64):
                append_search("overlap_search", 65, (number * 4 + worker) % 4)
        for number in range(8):
            append_call("overlap_replace", first_row=(512 - 256 * (number + 1)) % 512,
                        rows=256)
        append_call("explicit_update", first_row=0, rows=256)
        append_call("native_get_many")
        append_call("http_delete_file")
        append_call("native_deleted_visibility")
        append_call("reindex_replacement", first_row=0, rows=256)
        append_call("native_reinsert_visibility")
        append_search("empty_user", 0, 0, ids=[])
        for phase in ("pre_close_fold", "close", "reopen", "idempotent_ensure",
                      "reopen_graph_ensure"):
            append_call(phase)
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("post_reopen_curve", eligible, query)
        events.append({
            "event": "full_state_verified", "rows": 512, "vectors_checked": 512,
            "normalized_full_vector_tolerance": 1e-6,
        })
        append_call("verification_only_full_scroll")
        events.append({
            "event": "terminal", "lifecycle_complete": True, "error": None,
            "process_lifetimes": [{}, {}], "final_disk_bytes": 1,
        })
        return events, truth

    def test_smoke_sq8_consumes_one_complete_producer_phase_ledger(self):
        packet = {
            "candidate_commit": COMMIT,
            "files": {
                "manifest": {"sha256": "0" * 64},
                "documents": {"sha256": "1" * 64},
                "queries": {"sha256": "2" * 64},
                "truth": {"sha256": "3" * 64},
            },
            "dataset": {"manifest": "manifest", "documents": "documents",
                        "queries": "queries", "truth": "truth"},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "smoke.jsonl"
            events, truth = self._smoke_sq8_events()
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                for label, predicate in (
                        ("quality", lambda event: event.get("event") == "quantized_quality"),
                        ("empty", lambda event: event.get("phase") == "empty_user"
                         and event.get("event") == "search_result"),
                        ("mutation", lambda event: event.get("phase") == "overlap_replace"
                         and event.get("event") == "call"),
                        ("fixed", lambda event: event.get("phase") == "fixed_coordinate_curve"
                         and event.get("event") == "search_result")):
                    changed = copy.deepcopy(events)
                    del changed[next(index for index, event in enumerate(changed) if predicate(event))]
                    path.write_bytes(b"".join(canonical(event) for event in changed))
                    with self.subTest(label=label), self.assertRaises(analyzer.EvidenceError):
                        analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                changed = copy.deepcopy(events)
                fixed = next(event for event in changed
                             if event.get("event") == "search_result"
                             and event.get("phase") == "fixed_coordinate_curve")
                fixed["score_plane"]["snapshot"]["schema_hash"] = 9
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with self.assertRaisesRegex(analyzer.EvidenceError, "pre-mutation graph owner"):
                    analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                changed = copy.deepcopy(events)
                search_index = next(index for index, event in enumerate(changed)
                                    if event.get("event") == "search_result"
                                    and event.get("phase") == "fixed_coordinate_curve")
                changed[search_index + 1:search_index + 1] = copy.deepcopy(
                    changed[search_index - 1:search_index + 1]
                )
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with self.assertRaises(analyzer.EvidenceError):
                    analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)
                for label, call in (
                        ("unknown", {
                            "event": "call", "phase": "unplanned_retry", "start_ns": 1,
                            "end_ns": 2, "duration_ns": 1, "outcome": "completed",
                        }),
                        ("duplicate-build", copy.deepcopy(next(
                            event for event in events
                            if event.get("phase") == "initial_graph_build"
                        )))):
                    changed = copy.deepcopy(events)
                    changed.insert(-1, call)
                    path.write_bytes(b"".join(canonical(event) for event in changed))
                    with self.subTest(label=label), self.assertRaises(analyzer.EvidenceError):
                        analyzer.validate_smoke_log(path, "quantized_rerank", packet, truth)

    def test_exact_smoke_requires_the_complete_search_and_lifecycle_ledger(self):
        _, truth = self._smoke_sq8_events()
        plan = {
            "rows": 512, "queries": 4, "eligible_counts": analyzer.native.counts(512),
            "efs": [128, 256, 512, 1024, 2048], "overlap_eligible": 65,
            "overlap_ef": 512, "reader_concurrency": 4, "writer_calls": 8,
        }
        events = []
        self._append_construction_calls(events, 512)

        def append_call(phase, **fields):
            start = len(events) * 10 + 1
            events.append({
                "event": "call", "phase": phase, "start_ns": start,
                "end_ns": start + 5, "duration_ns": 5, "outcome": "completed",
                **fields,
            })

        def append_search(phase, eligible, ef, query, writer_active=False):
            append_call(
                phase, eligible=eligible, ef=ef, query=query,
                writer_active=writer_active, request_mode="exact",
            )
            events.append({
                "event": "search_result", "phase": phase, "eligible": eligible,
                "ef": ef, "query": query, "ids": truth[str(eligible)][query],
                "recall": 1.0, "ndcg_at_10": 1.0,
                "writer_active": writer_active, "dense_work": {},
            })

        for eligible in plan["eligible_counts"]:
            append_search("predicate_first_query", eligible, 512, 0)
            for ef in plan["efs"]:
                for query in range(4):
                    append_search("warm_curve", eligible, ef, query)
        for worker in range(4):
            for number in range(64):
                append_search("overlap_search", 65, 512, (number * 4 + worker) % 4)
        for number in range(8):
            append_call("overlap_replace", first_row=(512 - 256 * (number + 1)) % 512,
                        rows=256)
        append_call("explicit_update", first_row=0, rows=256)
        for phase in ("native_get_many", "http_delete_file", "native_deleted_visibility"):
            append_call(phase)
        append_call("reindex_replacement", first_row=0, rows=256)
        append_call("native_reinsert_visibility")
        append_call("empty_user")
        for phase in ("pre_close_fold", "close", "reopen", "idempotent_ensure",
                      "reopen_graph_ensure"):
            append_call(phase)
        for eligible in plan["eligible_counts"]:
            for query in range(4):
                append_search("post_reopen_curve", eligible, 512, query)
        full_state_index = len(events)
        events.append({"event": "full_state_verified"})
        append_call("verification_only_full_scroll")
        events.append({"event": "terminal"})
        analyzer._validate_exact_smoke_searches(events, plan, truth, full_state_index)
        changed = copy.deepcopy(events)
        del changed[next(index for index, event in enumerate(changed)
                         if event.get("phase") == "empty_user")]
        with self.assertRaisesRegex(analyzer.EvidenceError, "empty_user"):
            analyzer._validate_exact_smoke_searches(changed, plan, truth, full_state_index - 1)

    def test_full_log_consumes_lock_once_without_all_row_retune(self):
        packet = {
            "candidate_commit": COMMIT,
            "arms": {"treedb_sq8_rss": "rss"},
            "files": {
                "dataset_manifest": {"sha256": "0" * 64},
                "dataset_documents": {"sha256": "1" * 64},
                "dataset_queries": {"sha256": "2" * 64},
                "dataset_truth": {"sha256": "3" * 64},
                "rss": {"sha256": "4" * 64},
            },
            "dataset": {"manifest": "dataset_manifest", "documents": "dataset_documents", "queries": "dataset_queries",
                        "truth": "dataset_truth"},
        }
        rss = {"quality": {"selected_coordinate": {"ef_search": 32, "rerank_candidates": 32}},
               "rss": {"process_identity": "10:20"}}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            events, truth = self._full_events()
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    mock.patch.object(analyzer, "paired_results_match_dataset", return_value=True), \
                    mock.patch.object(analyzer, "_paired_owner_matches_locked_calls", return_value=True), \
                    mock.patch.object(analyzer, "_validate_post_selection_searches", return_value=set()), \
                    mock.patch.object(analyzer, "paired_statistics", return_value={"ok": True}):
                passed, stats, state = analyzer.validate_full_sq8_log(
                    path, packet, truth, rss, None, None,
                )
            self.assertTrue(passed)
            self.assertEqual(stats, {"ok": True})
            self.assertEqual(state["rows"], 500000)

            with mock.patch.object(
                    analyzer.native, "sq8_rss_artifact_reasons",
                    return_value=["construction contract differs"]), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "intrinsically valid"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    mock.patch.object(analyzer, "_paired_owner_matches_locked_calls", return_value=False), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "locked all-row graph owner"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            changed = copy.deepcopy(events)
            next(event for event in changed
                 if event.get("event") == "paired_query_timing")["artifact"][
                     "selected_coordinate"] = {"ef_search": 64, "rerank_candidates": 64}
            path.write_bytes(b"".join(canonical(event) for event in changed))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "paired_query_artifact_valid", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "locked all-row coordinate"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(quality_pass=False)
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)):
                passed, stats, state = analyzer.validate_full_sq8_log(
                    path, packet, truth, rss, None, None,
                )
            self.assertFalse(passed)
            self.assertIsNone(stats)
            self.assertIsNone(state)
            for cohorts in ("", "4097", "500000,4097"):
                changed = copy.deepcopy(events)
                error = "QualityUnqualified: quantized fixed-set quality failed for cohorts " + cohorts
                next(event for event in changed if event.get("event") == "failure")["error"] = error
                changed[-1]["error"] = error
                path.write_bytes(b"".join(canonical(event) for event in changed))
                with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                        mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                        mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                        mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                        mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                        self.subTest(cohorts=cohorts), \
                        self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            changed = copy.deepcopy(events)
            changed.insert(-2, {
                "event": "call", "phase": "post_miss_retry", "start_ns": 1,
                "end_ns": 2, "duration_ns": 1, "outcome": "completed",
            })
            path.write_bytes(b"".join(canonical(event) for event in changed))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)), \
                    self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(include_retune=True)
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                with self.assertRaisesRegex(analyzer.EvidenceError, "retuned"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events()
            next(event for event in events
                 if event.get("event") == "coordinate_lock_consumed")["fresh_process_identity"] = "12:22"
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "validate_shutdowns"), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True):
                with self.assertRaisesRegex(analyzer.EvidenceError, "fresh owner"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

            events, truth = self._full_events(quality_pass=False)
            events.insert(-1, {"event": "finalization_failure", "errors": ["disk cap"]})
            path.write_bytes(b"".join(canonical(event) for event in events))
            with mock.patch.object(analyzer.native, "sq8_requests_share_initial_owner", return_value=True), \
                    mock.patch.object(analyzer.native, "sq8_rss_artifact_reasons", return_value=[]), \
                    mock.patch.object(analyzer, "_validate_quantized_search_path", return_value=True), \
                    mock.patch.object(analyzer, "_recompute_filtered_quality", return_value=({}, True)):
                with self.assertRaisesRegex(analyzer.EvidenceError, "terminal disposition"):
                    analyzer.validate_full_sq8_log(path, packet, truth, rss, None, None)

    def test_filtered_quality_is_rebuilt_in_exact_producer_order(self):
        truth, rows = {}, []
        for eligible in analyzer.FULL_ELIGIBLE_COUNTS[:-1]:
            expected = [[f"row-{query:06d}"] for query in range(200)]
            truth[str(eligible)] = expected
            for query in range(200):
                event = {
                    "eligible": eligible, "ef": 32, "rerank_candidates": 32,
                    "query": query, "ids": expected[query],
                    "phase": "predicate_first_quality_query" if query == 0 else "quality_selection",
                }
                rows.append((len(rows), event, 1.0, 1.0))
        cohorts, passed = analyzer._recompute_filtered_quality(rows, truth, [32, 64])
        self.assertTrue(passed)
        self.assertEqual(set(cohorts), {"4096", "4097", "5000", "50000"})
        self.assertEqual(cohorts["4096"]["exact_query_matches"], [True] * 200)
        self.assertEqual(cohorts["4097"]["selected_coordinate"], {
            "ef_search": 32, "rerank_candidates": 32,
        })
        failed_rows = copy.deepcopy(rows)
        for index in range(200, 300):
            position, event, _, ndcg = failed_rows[index]
            failed_rows[index] = (position, event, 0.0, ndcg)
        failed_cohorts, passed = analyzer._recompute_filtered_quality(
            failed_rows, truth, [32],
        )
        self.assertFalse(passed)
        self.assertIsNone(failed_cohorts["4097"]["selected_coordinate"])
        self.assertEqual(failed_cohorts["50000"]["selected_coordinate"], {
            "ef_search": 32, "rerank_candidates": 32,
        })
        rows[201], rows[202] = rows[202], rows[201]
        with self.assertRaisesRegex(analyzer.EvidenceError, "ordered frozen grid"):
            analyzer._recompute_filtered_quality(rows, truth, [32, 64])

    def test_paired_statistics_do_not_pool_calls_across_repetitions(self):
        repetitions = []
        for repetition, exact in enumerate(([1, 100], [2, 200], [3, 300], [4, 400], [5, 500])):
            def arm(values):
                requests, clock = [], 10
                for value in values:
                    requests.append({"duration_ns": value, "started_monotonic_ns": clock,
                                     "ended_monotonic_ns": clock + value})
                    clock += value
                return {"requests": requests, "resources": {"delta": {
                    "server_cpu_ns_per_call": 1, "client_harness_cpu_ns_per_call": 2,
                    "total_alloc_bytes_per_call": 3, "mallocs_per_call": 4,
                }}}
            repetitions.append({
                "repetition": repetition,
                "arms": {
                    "exact": arm(exact),
                    "quantized_rerank": arm([value * 2 for value in exact]),
                },
            })
        result = analyzer.paired_statistics({"repetitions": repetitions})
        exact = result["arms"]["exact"]
        self.assertEqual(exact["per_repetition"][0]["p50_ns"], 1)
        self.assertEqual(exact["per_repetition"][0]["p95_ns"], 100)
        self.assertEqual(exact["across_repetitions"]["p95_ns"], {"median": 300, "mad": 100})
        self.assertEqual(exact["raw_repetitions_ns"][0], [1, 100])

    def test_paired_scores_are_recomputed_from_frozen_vectors(self):
        vectors = analyzer.np.zeros((2, 768), dtype=analyzer.np.float32)
        queries = analyzer.np.zeros((1, 768), dtype=analyzer.np.float32)
        vectors[:, 0], queries[:, 0] = 1, 1
        artifact = {"warmup": {"arms": {"exact": {"requests": [{
            "query": 0, "results": [{"id": "row-000001", "score": 1.0}],
            "recall": 1.0, "ndcg_at_10": 1.0,
        }]}}}, "repetitions": []}
        truth = [["row-000001"]]
        self.assertTrue(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["score"] = .5
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["score"] = 1.0
        artifact["warmup"]["arms"]["exact"]["requests"][0]["recall"] = 0.0
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        artifact["warmup"]["arms"]["exact"]["requests"][0]["recall"] = 1.0
        artifact["warmup"]["arms"]["exact"]["requests"][0]["results"][0]["id"] = "row-000000"
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth,
        ))
        exact = artifact["warmup"]["arms"]["exact"]["requests"][0]
        exact["results"][0]["id"] = "row-000001"
        artifact["warmup"]["arms"]["quantized_rerank"] = {
            "requests": [copy.deepcopy(exact)],
        }
        self.assertTrue(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth, {0: ["row-000001"]},
        ))
        artifact["warmup"]["arms"]["quantized_rerank"]["requests"][0]["results"][0]["id"] = "row-000000"
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, truth, {0: ["row-000001"]},
        ))

    def test_paired_order_uses_recomputed_scores_and_locked_graph_owner(self):
        vectors = analyzer.np.zeros((2, 768), dtype=analyzer.np.float32)
        queries = analyzer.np.zeros((1, 768), dtype=analyzer.np.float32)
        vectors[0, 0] = queries[0, 0] = 1
        vectors[1, :2] = (1, .004)
        record = {
            "query": 0,
            "results": [
                {"id": "row-000001", "score": 1.0},
                {"id": "row-000000", "score": .999999},
            ],
            "recall": 1.0, "ndcg_at_10": 1.0,
        }
        artifact = {"warmup": {"arms": {"exact": {"requests": [record]}}},
                    "repetitions": []}
        self.assertFalse(analyzer.paired_results_match_dataset(
            artifact, vectors, queries, [["row-000000", "row-000001"]],
        ))

        owner = {
            "schema_hash": 1, "schema_generation": 2,
            "base_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                              "checksum": 4},
            "current_manifest": {"generation": 3, "format": "tcs1", "version": 1,
                                 "checksum": 4},
            "base_coverage_lsn": 5, "current_coverage_lsn": 5,
        }
        locked = [(0, {"score_plane": {"snapshot": copy.deepcopy(owner)}})]
        paired = {"warmup": {"common_owner": copy.deepcopy(owner)}}
        self.assertTrue(analyzer._paired_owner_matches_locked_calls(locked, paired))
        paired["warmup"]["common_owner"]["schema_hash"] = 9
        self.assertFalse(analyzer._paired_owner_matches_locked_calls(locked, paired))

    def test_fp32_rss_quality_is_recomputed_from_ordered_id_ledger(self):
        truth = [["row-000000", "row-000001"], ["row-000001", "row-000000"]]
        quality = {
            "calibration": {"queries": [0], "curve": [{
                "control": 32, "mean_recall_at_10": 1.0, "mean_ndcg_at_10": 1.0,
                "per_query": [1.0], "per_query_ndcg_at_10": [1.0],
            }]},
            "revalidation": {"queries": [1], "curve": [{
                "control": 32, "mean_recall_at_10": 1.0, "mean_ndcg_at_10": 1.0,
                "per_query": [1.0], "per_query_ndcg_at_10": [1.0],
            }]},
        }
        observations = {
            "schema": "cohere_500k_768d_quality_observations/v1",
            "control_name": "hnsw_ef",
            "requests": [
                {"request_sequence": 1, "control": 32, "query": 0, "ids": truth[0]},
                {"request_sequence": 2, "control": 32, "query": 1, "ids": truth[1]},
            ],
            "exact_reference": {"query": 0, "ids": truth[0]},
        }
        artifact = {"quality": quality, "observed_quality": observations}
        analyzer._validate_fp32_quality_observations(artifact, truth, "hnsw_ef", 2)
        changed = copy.deepcopy(artifact)
        changed["observed_quality"]["exact_reference"]["ids"].reverse()
        with self.assertRaisesRegex(analyzer.EvidenceError, "truth order"):
            analyzer._validate_fp32_quality_observations(changed, truth, "hnsw_ef", 2)
        changed = copy.deepcopy(artifact)
        changed["quality"]["revalidation"]["curve"][0]["per_query"][0] = 0.5
        with self.assertRaisesRegex(analyzer.EvidenceError, "aggregate"):
            analyzer._validate_fp32_quality_observations(changed, truth, "hnsw_ef", 2)

    def test_frozen_truth_requires_exact_cohorts_ids_and_membership(self):
        truth = {}
        for eligible in analyzer.FULL_ELIGIBLE_COUNTS:
            selected = []
            ordinal = 0
            while len(selected) < 10:
                if (ordinal * 7919) % 500000 < eligible:
                    selected.append(f"row-{ordinal:06d}")
                ordinal += 1
            truth[str(eligible)] = [list(selected) for _ in range(200)]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "truth.json"
            path.write_bytes(canonical(truth))
            self.assertEqual(analyzer._full_truth(path), truth)
            truth["4096"][0][0] = "row-499999"
            path.write_bytes(canonical(truth))
            with self.assertRaisesRegex(analyzer.EvidenceError, "outside"):
                analyzer._full_truth(path)

    def _minimal_packet(self, declared):
        arms = {role: role for role in analyzer.ARM_KINDS}
        plans = {role: "plan_" + role for role in analyzer.PLAN_KINDS}
        dataset = {role: "dataset_" + role for role in analyzer.DATASET_KINDS}
        inputs = {role: "input_" + role for role in analyzer.INPUT_KINDS}
        files = {name: {"sha256": f"{number + 1:064x}"}
                 for number, name in enumerate(dataset.values())}
        return {
            "schema": analyzer.PACKET_SCHEMA, "candidate_commit": COMMIT,
            "files": files, "arms": arms, "plans": plans,
            "dataset": dataset, "inputs": inputs,
            "declared_quality_outcome": declared,
            "tradeoff_review": {
                "schema": analyzer.TRADEOFF_REVIEW_SCHEMA,
                "disposition": ("not_reached_after_quality_miss" if declared == "miss"
                                else "no_material_regression"),
                "rationale": "reviewed test disposition", "authority": None,
            },
        }

    def test_material_regression_requires_explicit_owner_acceptance(self):
        base = {"schema": analyzer.TRADEOFF_REVIEW_SCHEMA,
                "rationale": "reviewed tradeoff", "authority": None}
        self.assertTrue(analyzer.validate_tradeoff_review(
            {**base, "disposition": "no_material_regression"}, True,
        ))
        self.assertFalse(analyzer.validate_tradeoff_review(
            {**base, "disposition": "unaccepted_material_regression"}, True,
        ))
        with self.assertRaisesRegex(analyzer.EvidenceError, "owner acceptance"):
            analyzer.validate_tradeoff_review(
                {**base, "disposition": "owner_accepted_material_regression"}, True,
            )
        self.assertTrue(analyzer.validate_tradeoff_review({
            **base, "disposition": "owner_accepted_material_regression",
            "authority": "https://github.com/snissn/gomap/issues/4688#issuecomment-1",
        }, True))

    def test_states_and_final_state_wording_are_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.json"
            packet = self._minimal_packet("miss")
            raw = canonical(packet)
            path.write_bytes(raw)
            pin = hashlib.sha256(raw).hexdigest()
            paths = {name: Path(directory) / name for name in set(packet["arms"].values())}
            paths.update({name: Path(directory) / name for name in packet["dataset"].values()})
            paths.update({name: Path(directory) / name for name in packet["inputs"].values()})
            dataset_paths = {kind: paths[name] for kind, name in packet["dataset"].items()}
            with mock.patch.object(analyzer, "resolve_inventory", return_value=paths), \
                    mock.patch.object(analyzer, "validate_consumer_source", return_value={}), \
                    mock.patch.object(analyzer, "validate_dataset", return_value=(dataset_paths, {})), \
                    mock.patch.object(analyzer, "validate_go_inputs", return_value={}), \
                    mock.patch.object(analyzer, "validate_receipts"), \
                    mock.patch.object(analyzer, "validate_plans"), \
                    mock.patch.object(analyzer, "validate_bounded_artifacts", return_value={
                        "classification": analyzer.LEGACY_BASELINE_CLASSIFICATION,
                        "completed_clean": False, "lifecycle_claim_available": False,
                        "latency_claim_available": False,
                        "representation_matched_comparison": False,
                        "known_limitation": "test limitation",
                    }), \
                    mock.patch.object(analyzer, "validate_rss_and_comparisons",
                                      return_value=({}, {}, {}, {"state": "accept"}, {"state": "complete"})), \
                    mock.patch.object(analyzer, "_smoke_truth", return_value={}), \
                    mock.patch.object(analyzer, "validate_smoke_log"), \
                    mock.patch.object(analyzer, "_full_truth", return_value={}), \
                    mock.patch.object(analyzer.np, "memmap", return_value=object()), \
                    mock.patch.object(analyzer, "validate_full_sq8_log",
                                      return_value=(False, None, None)):
                result = analyzer.analyze(path, pin, lambda _: None)
                self.assertEqual(result["state"], "valid_unqualified", result)
                self.assertEqual(
                    result["bounded_legacy_control"]["classification"],
                    analyzer.LEGACY_BASELINE_CLASSIFICATION,
                )
                self.assertIn("#4617", result["limitations"][-1])
                assurance = result["final_state_assurance"]
                self.assertFalse(assurance["producer_attested"])
                self.assertFalse(assurance["independently_recomputed"])
                self.assertFalse(assurance["digest_claim"])
                self.assertEqual(
                    assurance["availability"], "not_reached_after_frozen_quality_miss",
                )

                packet["declared_quality_outcome"] = "pass"
                raw = canonical(packet)
                path.write_bytes(raw)
                result = analyzer.analyze(
                    path, hashlib.sha256(raw).hexdigest(), lambda _: None,
                )
                self.assertEqual(result["state"], "invalid")

    def test_normalized_truth_recomputation_uses_go_checked_shared_oracle(self):
        vectors = analyzer.np.tile(analyzer.np.asarray([1, 0], dtype=analyzer.np.float32), (128, 1))
        vectors[::2] = [0, 1]
        queries = analyzer.np.asarray([[1, 0]], dtype=analyzer.np.float32)
        ids, scores = analyzer.native.canonical_normalized_f32_truth(vectors, queries, [128])
        def recompute(want_ids, want_scores):
            # Small source arrays test the math, not final packet admission.
            with mock.patch.object(analyzer.np, "memmap", side_effect=[vectors, queries]), \
                    mock.patch.object(analyzer, "FULL_ELIGIBLE_COUNTS", [128]):
                return analyzer._normalized_recompute_canonical_truth(
                    {"documents": "documents.f32", "queries": "queries.f32"}, want_ids, want_scores,
                )
        normalized, normalized_queries = recompute(ids, scores)
        analyzer.np.testing.assert_array_equal(normalized, vectors)
        analyzer.np.testing.assert_array_equal(normalized_queries, queries)
        bad_ids = copy.deepcopy(ids)
        bad_ids["128"][0][-1] = "row-000025"
        with self.assertRaisesRegex(analyzer.EvidenceError, "source recomputation"):
            recompute(bad_ids, scores)
        bad_scores = copy.deepcopy(scores)
        bad_scores["128"][0][0] = 0.5
        with self.assertRaisesRegex(analyzer.EvidenceError, "source recomputation"):
            recompute(ids, bad_scores)

    def test_normalized_result_scores_apply_clamp_and_contract_tolerance(self):
        vectors = analyzer.np.asarray([[1.0000001, 0], [0.5, 0]], dtype=analyzer.np.float32)
        queries = analyzer.np.asarray([[1, 0]], dtype=analyzer.np.float32)
        self.assertTrue(analyzer._normalized_result_scores_match(
            ["row-000000", "row-000001"], [1, .5000025], 0, vectors, queries,
        ))
        self.assertFalse(analyzer._normalized_result_scores_match(
            ["row-000001"], [.50001], 0, vectors, queries,
        ))
        for invalid in (1.0000001, float("inf"), float("nan")):
            self.assertFalse(analyzer._normalized_result_scores_match(
                ["row-000000"], [invalid], 0, vectors, queries,
            ))
        ids, scores = analyzer.native.canonical_normalized_f32_topk(vectors, queries, [2])
        self.assertEqual(scores["2"][0], [1.0, 0.5])

    def test_normalized_engine_recomputes_same_shortlist_raw_repetitions(self):
        shortlists = [{
            "query": query, "ordinals": list(range(64)),
            "candidate_work": {
                "quantized_score_calls": 128,
                "quantized_code_bytes_read": 128 * 768,
                "prepared_graph_search_views": 1,
            },
            "packed_score_calls": 1, "packed_score_candidates": 64,
            "packed_vector_bytes_read": 64 * 768 * 4,
        } for query in range(200)]
        benchmark = lambda nanos, arm: [{
            "ordinal": ordinal,
            "arm_order": ordinal % 2 if arm == "candidate_only" else 1 - ordinal % 2,
            "iterations": 10, "elapsed_nanos": nanos * 2000,
            "nanos_per_query": nanos, "qps": 1e9 / nanos,
            "bytes_per_query": 0, "allocs_per_query": 0,
        } for ordinal in range(6)]
        artifact = {
            "schema": "treedb_cosine_normalized_f32_campaign_engine/v1",
            "rows": 500000, "dimensions": 768, "query_count": 200, "top_k": 10,
            "ef_search": 64, "rerank_candidates": 64,
            "representation": analyzer.native.NORMALIZED_REPRESENTATION,
            "index": "minima_cohere", "quantized_index": analyzer.native.QUANTIZED_PROFILE_NAME,
            "collection_generation": 12,
            "stable_duplicate_score_calls": 0, "shortlists": shortlists,
            "candidate_only": benchmark(80_000, "candidate_only"),
            "packed_same_shortlist": benchmark(6_900, "packed_same_shortlist"),
        }
        summary, failures = analyzer._normalized_validate_engine(artifact)
        self.assertFalse(failures)
        self.assertEqual(summary["summaries"]["packed_same_shortlist"]["median_ns"], 6_900)
        owner = {
            "schema_hash": 11, "schema_generation": 12,
            "base_manifest_generation": 13, "base_manifest_checksum": 14,
            "current_manifest_generation": 13, "current_manifest_checksum": 14,
            "current_coverage_lsn": 15,
        }
        artifact["serving_owner"] = {
            "index": "embedding", "publication_present": True,
            "publication_unchanged": True, "serving_ready": True,
            "invalid": False, "reconciling": False,
            "base_manifest": {"generation": 13, "checksum": 14},
            "current_manifest": {"generation": 13, "checksum": 14},
            "current_coverage_lsn": 15,
        }
        analyzer._normalized_validate_engine(artifact, owner)
        changed = copy.deepcopy(artifact)
        changed["collection_generation"] = 13
        with self.assertRaisesRegex(analyzer.EvidenceError, "timed serving owner"):
            analyzer._normalized_validate_engine(changed, owner)
        changed = copy.deepcopy(artifact)
        changed["packed_same_shortlist"][5]["ordinal"] = 4
        with self.assertRaisesRegex(analyzer.EvidenceError, "raw benchmark"):
            analyzer._normalized_validate_engine(changed)
        changed = copy.deepcopy(artifact)
        changed["shortlists"][0]["packed_vector_bytes_read"] -= 4
        with self.assertRaisesRegex(analyzer.EvidenceError, "shortlist work"):
            analyzer._normalized_validate_engine(changed)
        changed = copy.deepcopy(artifact)
        changed["collection_generation"] = 0
        with self.assertRaisesRegex(analyzer.EvidenceError, "engine diagnostic identity"):
            analyzer._normalized_validate_engine(changed)

    def test_normalized_producer_coordinates_bind_runtime_host_and_interpreter(self):
        runtime = analyzer.native.python_runtime_identity()
        executable = str(Path(analyzer.sys.executable).absolute())
        self.assertEqual(runtime["python_executable"], executable)
        self.assertEqual(runtime["python_executable_sha256"], analyzer.sha256_file(Path(executable)))
        self.assertEqual(analyzer._consumer_runtime_identity(), runtime)
        coordinate = {
            "python": "producer python", "numpy": "producer numpy", "platform": "producer host",
            "python_executable": executable, "python_executable_sha256": "a" * 64,
            "cpu_affinity": [0, 1], "gomaxprocs": "2",
            "treedb_go_runtime": {"GOMAXPROCS": "2", "GOGC": "", "GOMEMLIMIT": ""},
            "blas_threads": {"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1"},
            "host_memory_bytes": str(32 * analyzer.native.GIB),
            "host_resource_identity": {"machine_id": "host", "boot_id": "boot"},
            "infrastructure": {"effective_resources": {
                "effective_memory_bytes": 32 * analyzer.native.GIB,
                "effective_cpu_quota_millis": 2000,
            }},
        }
        plans = {"exact": copy.deepcopy(coordinate), "sq8": copy.deepcopy(coordinate)}
        self.assertEqual(
            analyzer._normalized_producer_coordinates(plans)["python"], "producer python",
        )
        for field in ("python", "python_executable_sha256", "host_resource_identity"):
            changed = copy.deepcopy(plans)
            changed["sq8"][field] = "different" if field != "host_resource_identity" else {
                "machine_id": "other", "boot_id": "boot",
            }
            with self.subTest(field=field), self.assertRaisesRegex(
                    analyzer.EvidenceError, "different producer coordinates"):
                analyzer._normalized_producer_coordinates(changed)

        script = "/source/minima_cohere_native_diagnostic.py"
        self.assertEqual(
            analyzer._normalized_command_flags(
                [executable, script, "--rows", "500000"], coordinate,
            ),
            {"--rows": "500000"},
        )
        with self.assertRaisesRegex(analyzer.EvidenceError, "reviewed producer"):
            analyzer._normalized_command_flags(
                ["/different/python", script, "--rows", "500000"], coordinate,
            )

    def test_normalized_analyzer_recomputes_cgroup_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            plan = {
                "rows": 500000, "run_dir": str(Path(directory) / "run"),
                "minimum_free_bytes": 10 * analyzer.native.GIB,
                "maximum_output_bytes": 11 * analyzer.native.GIB,
                "maximum_combined_rss_bytes": 24 * analyzer.native.GIB,
                "host_memory_bytes": str(32 * analyzer.native.GIB),
                "cpu_affinity": [0, 1], "gomaxprocs": "2",
                "host_resource_identity": {"cgroup_limits": {
                    "scope/cpu.max": "max 100000", "scope/memory.high": "max",
                    "scope/memory.max": "max",
                }},
            }
            with mock.patch.object(
                    analyzer.native.shutil, "disk_usage",
                    return_value=SimpleNamespace(free=22 * analyzer.native.GIB)):
                plan["infrastructure"] = analyzer.native.normalized_v4_infrastructure_receipt(plan)
            analyzer._normalized_validate_infrastructure(plan, "test")

            changed = copy.deepcopy(plan)
            changed["infrastructure"]["effective_resources"]["effective_memory_bytes"] -= 1
            with self.assertRaisesRegex(analyzer.EvidenceError, "preflight is unavailable"):
                analyzer._normalized_validate_infrastructure(changed, "test")
            changed = copy.deepcopy(plan)
            changed["host_resource_identity"]["cgroup_limits"]["scope/memory.max"] = str(
                8 * analyzer.native.GIB
            )
            with self.assertRaisesRegex(analyzer.EvidenceError, "preflight is unavailable"):
                analyzer._normalized_validate_infrastructure(changed, "test")

    def test_normalized_production_identity_accepts_actual_packed_wire_shapes(self):
        owner = {
            "schema_hash": 11, "schema_generation": 12,
            "base_manifest_generation": 13, "base_manifest_checksum": 14,
            "current_manifest_generation": 13, "current_manifest_checksum": 14,
            "current_coverage_lsn": 15,
        }
        direct = {
            "version": 1, "representation": analyzer.native.NORMALIZED_REPRESENTATION,
            "query_mode": "exact", "execution_route": "typed_hnsw",
            "return_embedding": False, "diagnostics": False, "filter": False,
            "top_k": 10, "ef_search": 64, "rerank_candidates": 0,
            "result_count": 10, **owner,
            "fp32_score_calls": 1150, "fp32_vector_bytes_read": 1150 * 768 * 4,
            "quantized_index_name": "", "quantized_codec": "", "quantized_version": 0,
            "embedding_vector_reads": 0, "embedding_vector_bytes": 0,
            "embedding_output_bytes": 0, "packed_score_calls": 6,
            "packed_score_candidates": 81, "packed_vector_bytes_read": 81 * 768 * 4,
        }
        analyzer._normalized_production_identity(direct, "exact", "go_native")
        sq8 = {
            **direct, "query_mode": "quantized_rerank", "rerank_candidates": 64,
            "quantized_index_name": analyzer.native.QUANTIZED_PROFILE_NAME,
            "quantized_codec": "scalar_u8", "quantized_version": 1,
            "quantized_score_calls": 1154, "quantized_code_bytes_read": 1154 * 768,
            "fp32_score_calls": 64, "fp32_vector_bytes_read": 64 * 768 * 4,
            "packed_score_calls": 1, "packed_score_candidates": 64,
            "packed_vector_bytes_read": 64 * 768 * 4,
        }
        analyzer._normalized_production_identity(
            sq8, "quantized_rerank", "python_native",
        )

        receipt = {
            "Available": True, "Representation": analyzer.native.NORMALIZED_REPRESENTATION,
            "QueryMode": "exact", "Route": "typed_hnsw", "ResultCount": 10,
            "SchemaHash": 11, "SchemaGeneration": 12,
            "BaseManifestGeneration": 13, "BaseManifestChecksum": 14,
            "CurrentManifestGeneration": 13, "CurrentManifestChecksum": 14,
            "CurrentCoverageLSN": 15, "QuantizedIndexName": "",
            "QuantizedCodec": "", "QuantizedVersion": 0,
        }
        collection = {
            "receipt": receipt, "quantized_score_calls": 0, "fp32_score_calls": 0,
            "packed_score_calls": 6, "packed_score_candidates": 81,
            "packed_vector_bytes_read": 81 * 768 * 4,
            "embedding_vector_reads": 0, "embedding_vector_bytes": 0,
            "embedding_output_bytes": 0,
        }
        analyzer._normalized_production_identity(
            collection, "exact", "collection_search",
        )
        collection_sq8 = {
            **collection,
            "receipt": {
                **receipt, "QueryMode": "quantized_rerank",
                "QuantizedIndexName": analyzer.native.QUANTIZED_PROFILE_NAME,
                "QuantizedCodec": "scalar_u8", "QuantizedVersion": 1,
            },
            "quantized_score_calls": 1154, "fp32_score_calls": 64,
            "packed_score_calls": 1, "packed_score_candidates": 64,
            "packed_vector_bytes_read": 64 * 768 * 4,
        }
        analyzer._normalized_production_identity(
            collection_sq8, "quantized_rerank", "collection_fetch",
        )

        for field in ("quantized_index_name", "quantized_codec", "quantized_version"):
            changed = copy.deepcopy(direct)
            changed.pop(field)
            with self.subTest(shape="client", field=field):
                analyzer._normalized_production_identity(changed, "exact", "service")
            changed = copy.deepcopy(collection)
            changed["receipt"].pop({
                "quantized_index_name": "QuantizedIndexName",
                "quantized_codec": "QuantizedCodec",
                "quantized_version": "QuantizedVersion",
            }[field])
            with self.subTest(shape="collection", field=field), self.assertRaisesRegex(
                    analyzer.EvidenceError, "crossed score planes"):
                analyzer._normalized_production_identity(
                    changed, "exact", "collection_search",
                )

        # Go omitempty drops all optional zero values; Python includes defaults.
        omitted = json.loads(json.dumps({
            key: value for key, value in direct.items()
            if key not in ("quantized_index_name", "quantized_codec", "quantized_version")
        }))
        analyzer._normalized_production_identity(omitted, "exact", "go_native")
        for field, invalid in (
            ("quantized_index_name", "minima_sq8"), ("quantized_codec", "scalar_u8"),
            ("quantized_version", 1), ("quantized_version", None),
            ("quantized_version", False), ("quantized_version", 0.0),
            ("quantized_score_calls", 1), ("quantized_score_calls", False),
            ("quantized_code_bytes_read", None), ("rerank_candidates", "0"),
            ("quantized_index_name", None), ("quantized_codec", None),
        ):
            with self.subTest(field=field, invalid=invalid), self.assertRaisesRegex(
                    analyzer.EvidenceError, "crossed the SQ8 plane"):
                analyzer._normalized_production_identity(
                    {**omitted, field: invalid}, "exact", "go_native",
                )

        changed = {**direct, "packed_vector_bytes_read": 1}
        with self.assertRaisesRegex(analyzer.EvidenceError, "packed FP32"):
            analyzer._normalized_production_identity(changed, "exact", "service")
        changed = copy.deepcopy(collection_sq8)
        changed["receipt"]["QueryMode"] = "exact"
        with self.assertRaisesRegex(analyzer.EvidenceError, "production route"):
            analyzer._normalized_production_identity(
                changed, "quantized_rerank", "collection_fetch",
            )

    def test_normalized_matrix_recomputes_immutable_owner_identity(self):
        identity = {
            "schema_hash": 11, "schema_generation": 12,
            "base_manifest_generation": 13, "base_manifest_checksum": 14,
            "current_manifest_generation": 13, "current_manifest_checksum": 14,
            "current_coverage_lsn": 15,
        }

        def lane(*, receipt=False):
            route = identity
            if receipt:
                route = {"receipt": {
                    receipt_name: identity[canonical]
                    for canonical, receipt_name
                    in analyzer.native.v4_gate._OWNER_IDENTITY_FIELDS
                }}
            arm = {"repetitions": [{"observations": [{"route": copy.deepcopy(route)}]}]}
            return {"exact": copy.deepcopy(arm), "sq8": copy.deepcopy(arm)}

        lanes = {
            "go_native": lane(), "python_native": lane(),
            "collection_search": lane(receipt=True),
            "collection_fetch": lane(receipt=True), "service": lane(),
        }
        proof = analyzer.native.v4_gate.immutable_owner_identity(lanes)
        self.assertEqual(
            analyzer._normalized_matrix_owner_identity(
                {"immutable_owner_identity": proof}, lanes,
            ),
            proof,
        )
        with self.assertRaisesRegex(analyzer.EvidenceError, "missing or inconsistent"):
            analyzer._normalized_matrix_owner_identity(
                {"immutable_owner_identity": {**proof, "current_coverage_lsn": 16}}, lanes,
            )
        lanes["service"]["sq8"]["repetitions"][0]["observations"][0]["route"][
            "current_manifest_checksum"
        ] += 1
        with self.assertRaisesRegex(analyzer.EvidenceError, "cross graph publications"):
            analyzer._normalized_matrix_owner_identity(
                {"immutable_owner_identity": proof}, lanes,
            )

    def test_normalized_serving_owner_and_retained_scores_are_independently_bound(self):
        owner = {
            "schema_hash": 11, "schema_generation": 12,
            "base_manifest_generation": 13, "base_manifest_checksum": 14,
            "current_manifest_generation": 13, "current_manifest_checksum": 14,
            "current_coverage_lsn": 15,
        }
        graph = {
            "index": "embedding",
            "publication_present": True, "publication_unchanged": True,
            "serving_ready": True, "invalid": False, "reconciling": False,
            "base_manifest": {"generation": 13, "checksum": 14},
            "current_manifest": {"generation": 13, "checksum": 14},
            "current_coverage_lsn": 15,
        }
        analyzer._normalized_ready_graph_owner(graph, owner, 12, "test")
        for field, value in (
                ("publication_present", False), ("invalid", True),
                ("current_coverage_lsn", 16), ("index", "other")):
            changed = copy.deepcopy(graph)
            changed[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(
                    analyzer.EvidenceError, "timed serving owner"):
                analyzer._normalized_ready_graph_owner(changed, owner, 12, "test")
        with self.assertRaisesRegex(analyzer.EvidenceError, "timed serving owner"):
            analyzer._normalized_ready_graph_owner(graph, owner, 13, "test")

        vectors = analyzer.np.asarray([[1, 0], [0, 1], [1, 1]], dtype=analyzer.np.float32)
        vectors /= analyzer.np.linalg.norm(vectors, axis=1)[:, None]
        queries = analyzer.np.asarray([[1, 0]], dtype=analyzer.np.float32)
        self.assertTrue(analyzer._normalized_result_scores_match(
            ["row-000000", "row-000002"], [1.0, 2 ** -.5], 0, vectors, queries,
        ))
        self.assertFalse(analyzer._normalized_result_scores_match(
            ["row-000000", "row-000002"], [1.0, .5], 0, vectors, queries,
        ))
        self.assertTrue(analyzer._normalized_result_scores_match(
            [], [], 0, vectors, queries,
        ))

    def test_normalized_sq8_exact_route_requires_packed_fp32_work(self):
        identity = {
            "version": 1,
            "representation": analyzer.native.NORMALIZED_REPRESENTATION,
            "query_mode": "quantized_rerank",
            "execution_route": "typed_exact",
            "return_embedding": False,
            "diagnostics": True,
            "filter": True,
            "top_k": 10,
            "ef_search": 64,
            "rerank_candidates": 64,
            "result_count": 10,
            "schema_hash": 1,
            "schema_generation": 1,
            "base_manifest_generation": 1,
            "base_manifest_checksum": 1,
            "current_manifest_generation": 1,
            "current_manifest_checksum": 1,
            "current_coverage_lsn": 1,
            "fp32_score_calls": 4096,
            "fp32_vector_bytes_read": 4096 * 768 * 4,
            "embedding_vector_reads": 0,
            "embedding_vector_bytes": 0,
            "embedding_output_bytes": 0,
            "quantized_index_name": analyzer.native.QUANTIZED_PROFILE_NAME,
            "quantized_codec": "scalar_u8",
            "quantized_version": 1,
            "quantized_score_calls": 0,
            "quantized_code_bytes_read": 0,
            "packed_score_calls": 1,
            "packed_score_candidates": 4096,
            "packed_vector_bytes_read": 4096 * 768 * 4,
        }
        analyzer._normalized_route_identity(
            identity, "quantized_rerank", eligible=4096, filtered=True, result_count=10,
        )
        suffix = {
            **identity, "current_manifest_generation": 2, "current_manifest_checksum": 2,
            "packed_score_calls": 0, "packed_score_candidates": 0, "packed_vector_bytes_read": 0,
        }
        analyzer._normalized_route_identity(
            {**suffix, "packed_score_calls": 1, "packed_score_candidates": 4000,
             "packed_vector_bytes_read": 4000 * 768 * 4},
            "quantized_rerank", eligible=4096, filtered=True, result_count=10,
        )
        for route in ("typed_exact", "typed_hnsw"):
            filtered = route == "typed_exact"
            eligible = 4096 if filtered else 500000
            changed = {**suffix, "execution_route": route, "filter": filtered,
                       "quantized_score_calls": int(route == "typed_hnsw"),
                       "quantized_code_bytes_read": 768 if route == "typed_hnsw" else 0}
            with self.subTest(suffix_route=route):
                analyzer._normalized_route_identity(
                    changed, "quantized_rerank", eligible=eligible,
                    filtered=filtered, result_count=10,
                )
                with self.assertRaisesRegex(analyzer.EvidenceError, "base/suffix"):
                    analyzer._normalized_route_identity(
                        {**changed, "current_manifest_generation": 1, "current_manifest_checksum": 1},
                        "quantized_rerank", eligible=eligible, filtered=filtered, result_count=10,
                    )
        exact = {
            **identity, "query_mode": "exact", "rerank_candidates": 0,
            "quantized_index_name": "", "quantized_codec": "",
            "quantized_version": 0,
            "fp32_score_calls": 4097, "fp32_vector_bytes_read": 4097 * 768 * 4,
            "packed_score_candidates": 4097,
            "packed_vector_bytes_read": 4097 * 768 * 4,
        }
        analyzer._normalized_route_identity(
            exact, "exact", eligible=4097, filtered=True, result_count=10,
        )
        for field, value in (
            ("quantized_index_name", "forged"),
            ("quantized_codec", "scalar_u8"),
            ("quantized_version", 1),
        ):
            with self.subTest(mode="exact", field=field), self.assertRaisesRegex(
                    analyzer.EvidenceError, "crossed the SQ8 score plane"):
                analyzer._normalized_route_identity(
                    {**exact, field: value}, "exact",
                    eligible=4097, filtered=True, result_count=10,
                )
        for field, value in (
            ("quantized_score_calls", 1),
            ("packed_score_calls", 0),
            ("packed_score_candidates", 0),
            ("packed_vector_bytes_read", 1),
        ):
            with self.subTest(field=field):
                changed = {**identity, field: value}
                with self.assertRaises(analyzer.EvidenceError):
                    analyzer._normalized_route_identity(
                        changed, "quantized_rerank", eligible=4096,
                        filtered=True, result_count=10,
                    )
        empty = {
            **identity, "execution_route": "typed_empty", "result_count": 0,
            "fp32_score_calls": 0, "fp32_vector_bytes_read": 0,
            "packed_score_calls": 0, "packed_score_candidates": 0,
            "packed_vector_bytes_read": 0,
        }
        analyzer._normalized_route_identity(
            empty, "quantized_rerank", eligible=0, filtered=True, result_count=0,
        )
        with self.assertRaisesRegex(analyzer.EvidenceError, "route identity"):
            analyzer._normalized_route_identity(
                identity, "quantized_rerank", eligible=5001,
                filtered=True, result_count=10,
            )

    def _normalized_lifecycle_result(self, eligible=500000, after_score=1.0):
        # Exercise the real curve/score/quality checks with small source arrays;
        # unrelated shutdown/route/projection checks have their own regressions.
        ordinals = []
        for row in range(500000):
            if (row * 7919) % 500000 < eligible:
                ordinals.append(row)
                if len(ordinals) == 11:
                    break
        ids = [f"row-{row:06d}" for row in ordinals]
        vectors = analyzer.np.tile([1.0, 0.0], (ordinals[-1] + 1, 1)).astype("float32")
        queries = analyzer.np.tile([1.0, 0.0], (200, 1)).astype("float32")
        truth = {str(eligible): [ids[:10] for _ in range(200)]}
        plan = {"campaign_profile": analyzer.native.CAMPAIGN_PROFILE_NORMALIZED_V4,
                "rss_only": False, "rows": 500000, "query_mode": "exact"}
        calls = [{"event": "call", "phase": "initial_durable_ingest",
                  "first_row": start, "rows": min(256, 500000 - start), "outcome": "completed"}
                 for start in range(0, 500000, 256)]
        phases = ("service_start", "schema_ensure", "initial_graph_build", "explicit_update",
                  "native_get_many", "http_delete_file", "native_deleted_visibility",
                  "reindex_replacement", "native_reinsert_visibility", "pre_close_fold", "close",
                  "reopen", "idempotent_ensure", "reopen_graph_ensure",
                  "verification_only_full_scroll", *("overlap_replace",) * 8)
        calls.extend({"event": "call", "phase": phase, "outcome": "completed"} for phase in phases)

        def search(phase, query, result_ids, scores, count=eligible):
            return {"event": "search_result", "phase": phase, "request_mode": "exact",
                    "eligible": count, "query": query, "ids": result_ids, "scores": scores,
                    "recall": len(set(result_ids) & set(ids[:10])) / 10,
                    "original_cosine_recall": len(set(result_ids) & set(ids[:10])) / 10,
                    "command_version": 4, "diagnostics": True, "writer_active": False,
                    "route_identity": {"execution_route": "typed_exact" if count <= 4096
                                       else "typed_hnsw"}}

        curves = [search(phase, query, result_ids, [score] * 10)
                  for phase, result_ids, score in (
                      ("fixed_coordinate_curve", ids[:10], 1.0),
                      ("post_reopen_curve", ids[:9] + ids[10:], after_score))
                  for query in range(200)]
        mutable = [search("overlap_search", 0, [], [], 4097) for _ in range(256)]
        mutable.extend(search(phase, 0, [], [], 0) for phase in (
            "post_update_visibility", "post_replacement_visibility", "empty_user"))
        full = {"event": "full_state_verified", "rows": 500000, "vectors_checked": 500000,
                "projection_sha256": "expected", "maximum_vector_error": 0, "maximum_norm_error": 0}
        terminal = {"event": "terminal", "qualification": "producer_gates_passed",
                    "lifecycle_complete": True, "error": None, "final_disk_bytes": 100}
        events = [{"event": "plan", "plan": plan}, *calls, *curves, *mutable, full, terminal]
        with mock.patch.object(analyzer, "read_native_events", return_value=events), \
                mock.patch.object(analyzer, "FULL_ELIGIBLE_COUNTS", [eligible]), \
                mock.patch.object(analyzer.native, "validate_shutdowns"), \
                mock.patch.object(analyzer, "_normalized_route_identity"), \
                mock.patch.object(analyzer, "_normalized_dense_event"), \
                mock.patch.object(analyzer, "_normalized_expected_projection_digest", return_value="expected"):
            return analyzer._normalized_validate_events(
                Path("unused"), plan, "exact", truth, truth, vectors, queries,
            )

    def test_normalized_lifecycle_validates_approximate_results_per_phase(self):
        result = self._normalized_lifecycle_result()
        self.assertEqual(result["quality"], {"exact": 1.0})
        self.assertEqual(result["post_reopen_quality"], {"exact": .9})

    def test_normalized_lifecycle_still_rejects_wrong_scores_and_exhaustive_changes(self):
        with self.assertRaisesRegex(analyzer.EvidenceError, "result ordering"):
            self._normalized_lifecycle_result(after_score=.5)
        with self.assertRaisesRegex(analyzer.EvidenceError, "post-reopen search decisions"):
            self._normalized_lifecycle_result(eligible=4096)

    def test_normalized_terminal_requires_stopped_totals_and_all_clean_lifetimes(self):
        for mode, count in (("exact", 2), ("sq8", 3)):
            plan = {"campaign_profile": analyzer.native.CAMPAIGN_PROFILE_NORMALIZED_V4,
                    "rss_only": False, "rows": 500000,
                    "query_mode": "exact" if mode == "exact" else "quantized_rerank"}
            lifetimes = [{
                "pid": pid, "linux_process_identity": f"{pid}:123",
                "exit": {"pid": pid, "linux_process_identity": f"{pid}:123",
                         "exit_code": 0, "availability": "measured"},
                "terminal_work": {"cleanup_completed": True, "shutdown_failures": 0,
                                  "contract_version": analyzer.native.existing.SERVICE_CONTRACT,
                                  "work": {"pid": pid}},
            } for pid in range(10, 10 + count)]
            terminal = {"event": "terminal", "qualification": "producer_gates_passed",
                        "lifecycle_complete": True, "error": None,
                        "final_disk_bytes": 100, "process_lifetimes": lifetimes}
            events = [{"event": "plan", "plan": plan}, terminal]
            # A valid terminal proceeds to the separate construction-ledger gate.
            with mock.patch.object(analyzer, "read_native_events", return_value=events):
                with self.assertRaisesRegex(analyzer.EvidenceError, "construction call ledger"):
                    analyzer._normalized_validate_events(Path("unused"), plan, mode, {}, {}, None, None)
            changes = [
                ("missing_disk", lambda row: row.pop("final_disk_bytes")),
                *[(f"disk_{value!r}", lambda row, value=value: row.update(final_disk_bytes=value))
                  for value in (0, -1, True, "100", None)],
                ("missing_lifetimes", lambda row: row.pop("process_lifetimes")),
                ("incomplete_lifetimes", lambda row: row["process_lifetimes"].pop()),
                ("bad_exit", lambda row: row["process_lifetimes"][-1]["exit"].update(exit_code=1)),
                ("wrong_pid", lambda row: row["process_lifetimes"][-1]["exit"].update(pid=999)),
                ("cleanup_incomplete", lambda row: row["process_lifetimes"][-1]["terminal_work"].update(cleanup_completed=False)),
                ("shutdown_failure", lambda row: row["process_lifetimes"][-1]["terminal_work"].update(shutdown_failures=1)),
            ]
            for name, mutate in changes:
                changed = copy.deepcopy(events)
                mutate(changed[-1])
                with self.subTest(mode=mode, name=name), \
                        mock.patch.object(analyzer, "read_native_events", return_value=changed):
                    with self.assertRaisesRegex(analyzer.EvidenceError, "complete cleanly|shut down cleanly"):
                        analyzer._normalized_validate_events(Path("unused"), plan, mode, {}, {}, None, None)

    def test_normalized_disk_comparison_uses_shutdown_totals_not_live_categories(self):
        evidence = {"exact": {"terminal": {"final_disk_bytes": 1000}},
                    "sq8": {"terminal": {"final_disk_bytes": 1100}}}
        resources = {"exact": {"final": {"total_owned_bytes": 100000}},
                     "sq8": {"final": {"total_owned_bytes": 1,
                                       "quantized_assets": [{"bytes": 100}]}}}
        summary = analyzer._normalized_disk_comparison(evidence, resources)
        self.assertEqual(summary["disk_delta_bytes"], 100)
        self.assertEqual(summary["final_disk_bytes"], {"exact": 1000, "sq8": 1100})
        self.assertTrue(all(summary["disk_checks"].values()))
        evidence["sq8"]["terminal"]["final_disk_bytes"] = 999
        self.assertFalse(analyzer._normalized_disk_comparison(evidence, resources)["disk_checks"]["sq8_delta_nonnegative"])
        evidence["sq8"]["terminal"]["final_disk_bytes"] = 1000 + (64 << 20) + 126
        self.assertFalse(analyzer._normalized_disk_comparison(evidence, resources)["disk_checks"]["sq8_delta_explained_by_derived_plane"])

    def test_normalized_resource_inventory_proves_one_fp32_authority(self):
        ref = {"Kind": "typed_column_part", "Namespace": "docs/column-assets",
               "Generation": 1, "PartID": 1, "FileID": 1, "Offset": 0,
               "Length": 500000 * 768 * 4, "Checksum": 1}
        normalized = {
            "role": "normalized_vectors", "asset_id": analyzer.native.NORMALIZED_REPRESENTATION,
            "logical_type": "float32_vector", "physical_encoding": "raw_float32_vector",
            "rows": 500000, "bytes": 500000 * 768 * 4,
            "logical_payload_bytes": 500000 * 768 * 4, "ref": ref,
        }
        topology = {
            "role": "hnsw_search_pack", "asset_id": "hnsw_topology_pack_v2",
            "logical_type": "hnsw_search_pack", "physical_encoding": "hnsw_topology_pack_v2",
            "rows": 500000, "bytes": 1234,
            "ref": {**ref, "PartID": 2, "FileID": 2, "Length": 1234},
        }
        codes = {
            "role": "quantized_codes", "asset_id": "quantized/minima_sq8/codes",
            "logical_type": "byte_vector", "physical_encoding": "raw_fixed_bytes",
            "rows": 500000, "bytes": 500000 * 768,
            "logical_payload_bytes": 500000 * 768,
            "ref": {**ref, "PartID": 3, "FileID": 3, "Length": 500000 * 768},
        }
        files = [
            {"path": "wal/000001", "bytes": 100, "category": "command_wal"},
            {"path": "db/column_assets/docs/column-assets/assets/segments/segment-000001.tca",
             "bytes": normalized["bytes"], "category": "column_assets"},
            {"path": "db/column_assets/docs/column-assets/assets/segments/segment-000002.tca",
             "bytes": topology["bytes"], "category": "column_assets"},
            {"path": "db/column_assets/docs/column-assets/assets/segments/segment-000003.tca",
             "bytes": codes["bytes"], "category": "column_assets"},
        ]
        column_bytes = sum(row["bytes"] for row in files if row["category"] == "column_assets")
        inventory = {
            "schema": "treedb_cohere_normalized_v4_resource_inventory/v1",
            "typed_graph": {
                "publication_present": True, "serving_ready": True,
                "publication_unchanged": True, "invalid": False, "reconciling": False,
                "base_rows": 500000, "suffix_rows": 0,
                "vector_assets": [normalized, topology, codes],
                "typed_column_parts": [{"rows": 500000, "role": "float32_vectors", "ref": ref}],
                "logical_resources": {
                    "active_heap_copy_bytes": 0, "active_mapped_bytes": 1,
                    "fallback_reads": 0,
                },
                "physical": {
                    "closed_db": False, "mapped_bytes": 1,
                    "descriptors_in_flight": 0, "fallback_backings": 0,
                    "fallback_bytes": 0, "fallback_bytes_in_flight": 0,
                    "fallback_segments": 0, "mapped_bytes_in_flight": 0,
                },
            },
            "owned_files": {
                "schema": "treedb_owned_db_file_inventory/v1",
                "total_bytes": column_bytes + 100,
                "category_bytes": {"column_assets": column_bytes, "command_wal": 100,
                                   "value_log": 0, "leaf_log": 0, "other": 0},
                "files": files,
            },
            "process_memory": {"vmrss_bytes": 1, "vmhwm_bytes": 2,
                               "vmsize_bytes": 3, "vmpeak_bytes": 4},
            "go_memory": {"heap_alloc_bytes": 1},
        }
        summary = analyzer._normalized_asset_inventory(inventory, "sq8")
        self.assertEqual(summary["normalized_asset"]["ref"], ref)
        for name, mutate in {
            "duplicate_fp32": lambda value: value["typed_graph"]["vector_assets"].append(
                {**normalized, "role": "other_fp32"}),
            "unshared_authority": lambda value: value["typed_graph"].update(
                typed_column_parts=[]),
            "unexpected_alpha": lambda value: value["typed_graph"]["vector_assets"].append({
                "role": "quantized_alpha", "asset_id": "unexpected-alpha",
                "logical_type": "scalar_u8_alpha", "physical_encoding": "raw_float32_uint32",
                "rows": 1, "bytes": 8, "ref": {**ref, "PartID": 4, "Length": 8},
            }),
            "forged_file_total": lambda value: value["owned_files"]["files"][0].update(bytes=99),
            "missing_asset": lambda value: value["owned_files"]["files"].pop(),
            "invalid_extent": lambda value: value["typed_graph"]["vector_assets"][0]["ref"].update(Offset=1),
        }.items():
            with self.subTest(name=name):
                changed = copy.deepcopy(inventory)
                mutate(changed)
                with self.assertRaises(analyzer.EvidenceError):
                    analyzer._normalized_asset_inventory(changed, "sq8")

    def test_invalid_normalized_packet_keeps_normalized_analysis_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.json"
            raw = canonical({"schema": analyzer.NORMALIZED_PACKET_SCHEMA})
            path.write_bytes(raw)
            result = analyzer.analyze(path, hashlib.sha256(raw).hexdigest())
            self.assertEqual(result["schema"], analyzer.NORMALIZED_ANALYSIS_SCHEMA)
            self.assertEqual(result["state"], "invalid")


if __name__ == "__main__":
    unittest.main()
