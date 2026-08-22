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
                "metrics": {"insert_duration": 2.0, "optimize_duration": 3.0, "load_duration": 5.0},
                "task_config": {"db_config": {"index_name": "idx"}},
            }]}), encoding="utf-8")

            got = harness.load_metrics_from_result(path, "idx", "Performance1536D50K", root)

        self.assertEqual(got["vector_count"], 50_000)
        self.assertEqual(got["insert_vectors_per_second"], 25_000)
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
