#!/usr/bin/env python3
"""Unit tests for treedb_vectordbbench_artifact.py helpers."""

from __future__ import annotations

import contextlib
import io
import inspect
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import treedb_vectordbbench_artifact as harness


def lifecycle_fixture(root: Path) -> tuple[dict, list[dict]]:
    profile = root / "profiles" / "build.cpu.pprof"
    profile.parent.mkdir(parents=True)
    profile.write_bytes(b"profile")
    stages = [
        "startup", "reset", "load_start", "load_end", "drain_checkpoint",
        "optimize_start", "optimize_end", "cache_prime", "cache_warm",
        "graceful_close", "cold_open_ready", "exact_verify", "route_verify", "teardown",
    ]
    events = []
    for sequence, stage in enumerate(stages):
        loaded = sequence >= 3
        durable = sequence >= 4
        reopened = sequence >= 11
        state = {
            "rows": {
                "client_sent": 50_000 if loaded else 0,
                "server_accepted": 50_000 if loaded else 0,
                "server_durable": 50_000 if durable else 0,
                "reopened": 50_000 if reopened else 0,
            },
            "wal": {"frontier": sequence, "bytes_written_total": sequence * 100},
            "counters": {
                "writes": 50_000 if loaded else 0,
                "checkpoints": 1 if durable else 0,
                "builds": 1 if sequence >= 6 else 0,
            },
        }
        if sequence >= 6:
            state["index"] = {"identity": "index-a", "asset_generation": 7, "status": "ready"}
        if sequence >= 9:
            state["database"] = {"identity": "database-a", "commit_seq": 50_000}
        if stage == "route_verify":
            state["route"] = {
                "name": "exact_hnsw_search_pack_v1",
                "fallback_reason": "none",
                "optimized": True,
                "index_identity": "index-a",
                "index_asset_generation": 7,
            }
        events.append({
            "schema_version": harness.LIFECYCLE_EVENT_SCHEMA,
            "sequence": sequence,
            "stage": stage,
            "timestamp": f"2026-08-27T00:00:{sequence:02d}Z",
            "state": state,
        })
    lifecycle_path = root / "lifecycle.jsonl"
    lifecycle_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    manifest = {
        "schema_version": harness.ARTIFACT_SCHEMA,
        "context": {
            "gomap": {"commit": "1" * 40, "dirty": False},
            "vectordbbench": {"commit": "2" * 40, "dirty": False},
            "host": {
                "logical_cpu_count": 16,
                "physical_cpu_count": 8,
                "memory_bytes": 64 * 1024**3,
                "storage": {"kind": "local-nvme", "filesystem": "xfs"},
            },
        },
        "service": {
            "profile": "command_wal_durable",
            "binary": {"sha256": "3" * 64},
        },
        "harness": {
            "case_type": "Performance768D50K",
            "num_per_batch": 500,
            "num_concurrency": "32",
            "m": 16,
            "ef_construction": 128,
        },
    }
    manifest["lifecycle"] = {
        "schema_version": harness.LIFECYCLE_SCHEMA,
        "result_status": "completed",
        "file": "lifecycle.jsonl",
        "sha256": harness.sha256_file(lifecycle_path),
        "expected_rows": 50_000,
        "dataset": {"name": "cohere-50k", "sha256": "4" * 64, "dimensions": 768, "vectors": 50_000},
        "identity": {
            "gomap_commit": "1" * 40,
            "vectordbbench_commit": "2" * 40,
            "service_binary_sha256": "3" * 64,
            "config_sha256": harness.lifecycle_config_sha256(manifest),
        },
        "raw_artifacts": [{"path": "profiles/build.cpu.pprof", "sha256": harness.sha256_file(profile)}],
        "profiles": [{
            "path": "profiles/build.cpu.pprof",
            "sha256": harness.sha256_file(profile),
            "kind": "cpu",
            "before_sequence": 5,
            "after_sequence": 6,
        }],
    }
    harness.write_json(root / "manifest.json", manifest)
    return manifest, events


def rewrite_lifecycle_fixture(root: Path, manifest: dict, events: list[dict]) -> None:
    lifecycle_path = root / "lifecycle.jsonl"
    lifecycle_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in events), encoding="utf-8")
    manifest["lifecycle"]["sha256"] = harness.sha256_file(lifecycle_path)
    harness.write_json(root / "manifest.json", manifest)


class RouteProofSummaryTest(unittest.TestCase):
    def test_iso_now_is_utc(self) -> None:
        self.assertTrue(harness.iso_now().endswith("Z"))

    def test_cpu_brand_is_recorded(self) -> None:
        self.assertTrue(harness.cpu_brand())

    def test_scalar_summary_extracts_required_counters(self) -> None:
        response = {
            "metric": "cosine",
            "query_mode": "quantized_rerank",
            "quantized_index_name": "embedding.scalar_u8.fast",
            "quantized_rerank_candidates": 32,
            "no_documents": True,
            "results": [{"id": "101", "ordinal": 0, "score": 1.0}],
            "stats": {
                "documents_fetched": 0,
                "document_bytes": 0,
                "quantized_scorer_active": 1,
                "quantized_score_calls": 7,
                "quantized_rerank_candidates": 4,
                "quantized_rerank_exact_score_calls": 4,
                "search_route_quantized_rerank": 1,
                "score_batch_calls": 1,
                "score_batch_optimized": 1,
            },
            "diagnostics": {"route": "quantized_rerank", "fallback_reason": "none"},
        }

        got = harness.proof_summary(
            "scalar",
            "idx",
            response,
            {"top_k": 2, "query_mode": "quantized_rerank", "quantized_rerank_candidates": 32},
        )

        self.assertEqual(got["route"], "quantized_rerank")
        self.assertEqual(got["fallback_reason"], "none")
        self.assertEqual(got["documents_fetched"], 0)
        self.assertEqual(got["quantized_scorer_active"], 1)
        self.assertEqual(got["quantized_rerank_exact_score_calls"], 4)
        self.assertEqual(got["score_batch_optimized"], 1)
        self.assertEqual(got["score_batch_fallback"], 0)
        self.assertEqual(got["response"]["quantized_rerank_candidates"], 32)

    def test_missing_fallback_reason_normalizes_to_none(self) -> None:
        self.assertEqual(harness.fallback_reason({"diagnostics": {}}), "none")


class ArtifactRootTest(unittest.TestCase):
    def test_prepare_artifact_root_rejects_non_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "old-manifest.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "must be new or empty"):
                harness.prepare_artifact_root(root)


class SmokeShapeTest(unittest.TestCase):
    def test_campaign_shape_is_valid_and_deterministic(self) -> None:
        harness.validate_smoke_shape(768, 256, 100, 192, 150)
        documents = harness.smoke_documents(256, 768)

        self.assertEqual(len(documents), 256)
        self.assertEqual(len(documents[0]["embedding"]), 768)
        self.assertEqual(documents[0], harness.smoke_documents(1, 768)[0])

    def test_rejects_rerank_shorter_than_top_k(self) -> None:
        with self.assertRaisesRegex(ValueError, "rerank candidates"):
            harness.validate_smoke_shape(768, 256, 100, 192, 32)

    def test_parse_args_rejects_invalid_shape_before_service_start(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            harness.parse_args(["--smoke-documents", "256", "--smoke-top-k", "100", "--rerank-candidates", "32"])


class VDBBenchBatchTest(unittest.TestCase):
    def test_vdbbench_rows_receive_default_and_override_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            with mock.patch.dict("os.environ", {}, clear=True):
                args = harness.parse_args([])
            default = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state)
            cli_override_args = harness.parse_args(["--num-per-batch", "500"])
            with mock.patch.dict("os.environ", {"TREEDB_VDBBENCH_NUM_PER_BATCH": "250"}, clear=True):
                override_args = harness.parse_args([])
            override = harness.vdbbench_row_env(override_args, Path("/vdbbench"), Path("/gomap"), state)

        self.assertEqual(default["NUM_PER_BATCH"], "1000")
        self.assertEqual(cli_override_args.num_per_batch, 500)
        self.assertEqual(override["NUM_PER_BATCH"], "250")

    def test_vdbbench_rows_use_separate_result_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = harness.HarnessState(root=Path(tmp))
            args = harness.parse_args([])
            exact = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state, "exact")
            scalar = harness.vdbbench_row_env(args, Path("/vdbbench"), Path("/gomap"), state, "scalar")

        self.assertEqual(exact["RESULTS_LOCAL_DIR"], str(Path(tmp) / "vdbbench-results" / "exact"))
        self.assertEqual(scalar["RESULTS_LOCAL_DIR"], str(Path(tmp) / "vdbbench-results" / "scalar"))

    def test_parse_args_rejects_nonpositive_batch_before_service_start(self) -> None:
        for value in ("0", "-1"):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args(["--num-per-batch", value])

    def test_parse_args_rejects_nonpositive_batch_environment(self) -> None:
        for value in ("0", "-1"):
            with mock.patch.dict("os.environ", {"TREEDB_VDBBENCH_NUM_PER_BATCH": value}, clear=True):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    harness.parse_args([])


class VDBBenchLoadMetricsTest(unittest.TestCase):
    def test_canonical_result_records_separated_durations_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "vdbbench-results" / "TreeDB" / "result_test.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"run_id": "run-1", "results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 5.0, "inserted_count": 0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            got = harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

        self.assertEqual(got["vector_count"], 50_000)
        self.assertEqual(got["insert_vectors_per_second"], 25_000)
        self.assertEqual(got["throughput_vector_count"], 50_000)
        self.assertIn("sentinel inserted_count=0", got["throughput_vector_count_source"])
        self.assertEqual(got["offline_optimize_duration_seconds"], 3.0)
        self.assertEqual(len(got["result_sha256"]), 64)
        self.assertEqual(got["task_config_sha256"], "04fa505555bec57a85ed8491a57562ac60212fd9ea34871a1f9893622e68d394")

    def test_canonical_result_fails_closed_when_duration_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 0.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "load_duration"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_fails_closed_when_total_is_inconsistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 6.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "load_duration !="):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_rejects_unsuccessful_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":(",
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 5.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "did not report success"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_canonical_result_rejects_partial_insert_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / "result_test.json"
            path.write_text(json.dumps({"results": [{
                "label": ":)",
                "metrics": {
                    "insert_duration": 2.0,
                    "optimize_duration": 3.0,
                    "load_duration": 5.0,
                    "inserted_count": 49_999,
                },
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "inserted_count 49999 != expected"):
                harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

    def test_partial_multirow_run_preserves_completed_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            args = harness.parse_args(["--run-vdbbench"])
            record = mock.Mock(command_string="vdbbench exact", exit_code=0)
            with mock.patch.object(harness, "run_command", side_effect=[record, RuntimeError("second row failed")]), \
                    mock.patch.object(harness, "capture_vdbbench_load_metrics", return_value={"insert_duration_seconds": 1.0}):
                with self.assertRaisesRegex(RuntimeError, "second row failed"):
                    harness.run_vdbbench_rows(
                        state,
                        args=args,
                        gomap_root=root,
                        vectordbbench_dir=root,
                        base_url="http://127.0.0.1:1",
                        index_prefix="test",
                    )

            sidecar = json.loads((root / "vdbbench_load_metrics.json").read_text(encoding="utf-8"))
            self.assertEqual([row["row"] for row in sidecar["rows"]], ["exact"])

    def test_dry_run_does_not_write_load_metrics_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root)
            args = harness.parse_args(["--run-vdbbench", "--vdbbench-dry-run", "--rows", "exact"])
            record = mock.Mock(command_string="vdbbench exact --dry-run", exit_code=0)
            with mock.patch.object(harness, "run_command", return_value=record):
                harness.run_vdbbench_rows(
                    state,
                    args=args,
                    gomap_root=root,
                    vectordbbench_dir=root,
                    base_url="http://127.0.0.1:1",
                    index_prefix="test",
                )

            self.assertFalse((root / "vdbbench_load_metrics.json").exists())

    def test_capture_fails_closed_on_ambiguous_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            results = root / "results"
            results.mkdir()
            (results / "result_one.json").write_text("{}", encoding="utf-8")
            (results / "result_two.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "expected exactly one"):
                harness.capture_vdbbench_load_metrics(results, set(), "idx", "Performance1536D50K", root)

    def test_standard_case_rejects_zero_vector_count(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive vector count"):
            harness.case_vector_count("Performance1536D0K")

    def test_custom_dataset_uses_selected_result_size(self) -> None:
        task_config = {"case_config": {"custom_case": {"dataset_config": {"size": "50000"}}}}

        count, source = harness.result_vector_count(task_config, "PerformanceCustomDataset")

        self.assertEqual(count, 50_000)
        self.assertEqual(source, "task_config.case_config.custom_case.dataset_config.size")

    def test_custom_dataset_rejects_invalid_result_size(self) -> None:
        with self.assertRaisesRegex(ValueError, "PerformanceCustomDataset"):
            harness.result_vector_count({"case_config": {"custom_case": {"dataset_config": {"size": "0"}}}}, "PerformanceCustomDataset")


class HarnessOrderTest(unittest.TestCase):
    def test_vdbbench_rows_run_before_route_proof_smoke(self) -> None:
        source = inspect.getsource(harness.main)

        self.assertLess(source.index("run_vdbbench_rows("), source.index("run_route_proof_smoke("))

    def test_route_proof_can_be_skipped_for_measurement_only_runs(self) -> None:
        args = harness.parse_args(["--run-vdbbench", "--skip-route-proof"])

        self.assertTrue(args.skip_route_proof)

    def test_route_proof_skip_requires_real_load_evidence(self) -> None:
        invalid = [
            ["--skip-route-proof"],
            ["--run-vdbbench", "--skip-route-proof", "--vdbbench-dry-run"],
            ["--run-vdbbench", "--skip-route-proof", "--skip-load"],
            ["--run-vdbbench", "--skip-route-proof", "--rows", ""],
        ]
        for argv in invalid:
            with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                harness.parse_args(argv)


class LifecycleValidatorTest(unittest.TestCase):
    def test_complete_fixture_passes_and_reconstructs_t_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertTrue(got["complete"])
        self.assertEqual(got["last_stage"], "teardown")
        self.assertEqual(got["counts"], {
            "client_sent": 50_000,
            "server_accepted": 50_000,
            "server_durable": 50_000,
            "reopened": 50_000,
        })
        self.assertEqual(got["t_ready_seconds"], 8.0)

    def test_interrupted_fixture_is_analyzable_but_never_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "interrupted"
            manifest["lifecycle"]["profiles"] = []
            rewrite_lifecycle_fixture(root, manifest, events[:4])

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertEqual(got["last_stage"], "load_end")
        self.assertTrue(any("result_status" in item for item in got["completion_errors"]))

    def test_cli_fails_closed_unless_analyzable_partial_is_explicitly_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            manifest["lifecycle"]["result_status"] = "partial"
            manifest["lifecycle"]["profiles"] = []
            rewrite_lifecycle_fixture(root, manifest, events[:4])
            with contextlib.redirect_stdout(io.StringIO()):
                strict = harness.main(["--validate-lifecycle", str(root)])
                analyzable = harness.main(["--validate-lifecycle", str(root), "--allow-partial"])

        self.assertEqual(strict, 1)
        self.assertEqual(analyzable, 0)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events.pop(5)
            rewrite_lifecycle_fixture(root, manifest, events)
            with contextlib.redirect_stdout(io.StringIO()):
                malformed_complete = harness.main([
                    "--validate-lifecycle", str(root), "--allow-partial",
                ])

        self.assertEqual(malformed_complete, 1)

        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            missing_manifest = harness.main([
                "--validate-lifecycle", tmp, "--allow-partial",
            ])

        self.assertEqual(missing_manifest, 1)

    def test_complete_gate_rejects_missing_or_out_of_order_stage(self) -> None:
        for mutation, expected in (
            (lambda rows: rows.pop(5), "missing required stage optimize_start"),
            (lambda rows: rows.__setitem__(slice(5, 7), [rows[6], rows[5]]), "sequence must increase"),
        ):
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"] + got["completion_errors"]), got)

    def test_identity_and_config_must_match_manifest(self) -> None:
        for key, value, expected in (
            ("gomap_commit", "f" * 40, "gomap_commit does not match"),
            ("config_sha256", "f" * 64, "config_sha256 does not match"),
        ):
            with self.subTest(key=key):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["identity"][key] = value
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_effective_service_and_harness_config_must_be_present(self) -> None:
        mutations = (
            (lambda row: row["service"].__setitem__("profile", ""), "service.profile"),
            (lambda row: row["harness"].__setitem__("case_type", ""), "harness.case_type"),
            (lambda row: row["harness"].__setitem__("num_concurrency", ""), "harness.num_concurrency"),
            (lambda row: row["harness"].__setitem__("num_concurrency", "1,nope"), "harness.num_concurrency"),
            (lambda row: row["harness"].__setitem__("num_per_batch", 0), "harness.num_per_batch"),
            (lambda row: row["harness"].__setitem__("m", None), "harness.m"),
            (lambda row: row["harness"].__setitem__("ef_construction", -1), "harness.ef_construction"),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    mutation(manifest)
                    manifest["lifecycle"]["identity"]["config_sha256"] = harness.lifecycle_config_sha256(manifest)
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["analyzable"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)

    def test_teardown_counts_must_match_expected_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            events[-1]["state"]["rows"]["client_sent"] += 1
            events[-1]["state"]["rows"]["server_accepted"] += 1
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["complete"])
        self.assertTrue(any("stage teardown rows.server_accepted" in item for item in got["completion_errors"]), got)

    def test_reset_and_load_start_must_prove_an_empty_boundary(self) -> None:
        for sequence, stage in ((1, "reset"), (2, "load_start")):
            with self.subTest(stage=stage):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    for key in ("client_sent", "server_accepted", "server_durable", "reopened"):
                        events[sequence]["state"]["rows"][key] = 1
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(
                    f"stage {stage} rows.client_sent must be zero" in item
                    for item in got["completion_errors"]
                ), got)

    def test_teardown_must_be_the_terminal_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, events = lifecycle_fixture(root)
            late = json.loads(json.dumps(events[-1]))
            late["sequence"] += 1
            late["stage"] = "post_teardown"
            late["timestamp"] = "2026-08-27T00:00:14Z"
            late["state"]["counters"]["writes"] += 1
            events.append(late)
            rewrite_lifecycle_fixture(root, manifest, events)

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["complete"])
        self.assertEqual(got["last_stage"], "post_teardown")
        self.assertTrue(any("teardown must be the final" in item for item in got["completion_errors"]), got)

    def test_stale_index_generation_or_fallback_route_fails_closed(self) -> None:
        def stale_identity(rows: list[dict]) -> None:
            rows[10]["state"]["index"]["identity"] = "stale-index"

        def stale_generation(rows: list[dict]) -> None:
            rows[11]["state"]["index"]["asset_generation"] = 8

        def fallback_route(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["fallback_reason"] = "exact_scan"

        def stale_route_identity(rows: list[dict]) -> None:
            rows[12]["state"]["route"]["index_identity"] = "stale-index"

        for mutation, expected in (
            (stale_identity, "index identity changed"),
            (stale_generation, "index asset generation changed"),
            (stale_route_identity, "optimized route proof failed"),
            (fallback_route, "optimized route proof failed"),
        ):
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["completion_errors"]), got)

    def test_raw_checksum_and_profile_association_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lifecycle_fixture(root)
            (root / "profiles" / "build.cpu.pprof").write_bytes(b"corrupt")

            got = harness.validate_lifecycle_artifact(root)

        self.assertFalse(got["analyzable"])
        self.assertTrue(any("checksum mismatch" in item for item in got["errors"]), got)

        for malformed in (999, []):
            with self.subTest(after_sequence=malformed):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, _ = lifecycle_fixture(root)
                    manifest["lifecycle"]["profiles"][0]["after_sequence"] = malformed
                    harness.write_json(root / "manifest.json", manifest)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any("profile state association" in item for item in got["errors"]), got)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest, _ = lifecycle_fixture(root)
            manifest["lifecycle"]["profiles"] = []
            harness.write_json(root / "manifest.json", manifest)

            got = harness.validate_lifecycle_artifact(root)

        self.assertTrue(got["analyzable"])
        self.assertFalse(got["complete"])
        self.assertTrue(any("at least one profile" in item for item in got["completion_errors"]), got)

    def test_rows_wal_and_counters_must_be_monotonic(self) -> None:
        mutations = (
            (lambda rows: rows[5]["state"]["rows"].__setitem__("server_durable", 0), "rows.server_durable decreased"),
            (lambda rows: rows[5]["state"]["wal"].__setitem__("frontier", 0), "wal.frontier decreased"),
            (lambda rows: rows[7]["state"]["counters"].__setitem__("builds", 0), "counters.builds decreased"),
        )
        for mutation, expected in mutations:
            with self.subTest(expected=expected):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    manifest, events = lifecycle_fixture(root)
                    mutation(events)
                    rewrite_lifecycle_fixture(root, manifest, events)

                    got = harness.validate_lifecycle_artifact(root)

                self.assertFalse(got["complete"])
                self.assertTrue(any(expected in item for item in got["errors"]), got)


class ManifestFileListTest(unittest.TestCase):
    def test_file_identity_records_size_and_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "service"
            path.write_bytes(b"treedb")

            got = harness.file_identity(path)

        self.assertEqual(got["bytes"], 6)
        self.assertEqual(len(got["sha256"]), 64)

    def test_artifact_file_list_skips_treedb_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "manifest.json").write_text("{}", encoding="utf-8")
            (root / "treedb-data" / "maindb").mkdir(parents=True)
            (root / "treedb-data" / "maindb" / "segment.log").write_text("data", encoding="utf-8")

            files, truncated = harness.artifact_file_list(root)

        self.assertEqual(files, ["manifest.json"])
        self.assertFalse(truncated)

    def test_manifest_does_not_reference_missing_metrics_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = harness.HarnessState(root=root, vdbbench=[{"row": "exact"}])

            harness.write_manifest(state, args=harness.parse_args([]), context={}, service_command=None)

            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertIsNone(manifest["vdbbench_load_metrics"])

    def test_manifest_uses_service_identity_captured_before_launch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = root / "service"
            service.write_bytes(b"original")
            identity = harness.file_identity(service)
            state = harness.HarnessState(root=root, service_binary=identity)
            service.write_bytes(b"replaced")

            harness.write_manifest(
                state,
                args=harness.parse_args([]),
                context={},
                service_command=[str(service)],
            )

            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["service"]["binary"], identity)


if __name__ == "__main__":
    unittest.main()
