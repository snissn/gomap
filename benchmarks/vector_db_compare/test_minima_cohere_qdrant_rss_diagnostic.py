"""Fail-closed checks for the matched Cohere 768D RSS boundary."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_cohere_qdrant_rss_diagnostic as qdrant_rss


class CohereQdrantRSSDiagnosticTests(unittest.TestCase):
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

    def test_comparison_accepts_only_matched_calibrated_artifacts(self):
        contract = {"schema": "cohere-rss/v1", "rows": 500000, "dimensions": 768,
                    "dataset_files_sha256": {"documents": "a" * 64}, "cpu_affinity": [0, 1]}
        tree = {"schema": native.RSS_ARTIFACT_SCHEMA, "state": "calibrated", "backend": "treedb",
                "comparison_contract": contract, "quality": {"revalidation": {"passed": True}},
                "rss": {"availability": "measured", "bytes": 100, "process_identity": "1:1"},
                "provenance": {"harness_commit": "a" * 40, "harness_trees": {"benchmarks": "b" * 40}}}
        qdrant = {**copy.deepcopy(tree), "backend": "qdrant",
                  "rss": {"availability": "measured", "bytes": 120, "process_identity": "2:2"}}
        decision = qdrant_rss.compare_artifacts(tree, qdrant)
        self.assertEqual(decision["state"], "accept")
        self.assertEqual(decision["delta_bytes"], -20)
        self.assertAlmostEqual(decision["treedb_to_qdrant_ratio"], 100 / 120)

        qdrant["rss"]["bytes"] = 80
        decision = qdrant_rss.compare_artifacts(tree, qdrant)
        self.assertEqual(decision["state"], "investigate")
        self.assertEqual(decision["delta_bytes"], 20)

        for mutation in ("contract", "rss", "quality", "provenance"):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(qdrant)
                if mutation == "contract":
                    changed["comparison_contract"]["rows"] += 1
                elif mutation == "rss":
                    changed["rss"]["availability"] = "unavailable"
                elif mutation == "quality":
                    changed["quality"]["revalidation"]["passed"] = False
                else:
                    changed["provenance"]["harness_commit"] = "c" * 40
                self.assertEqual(qdrant_rss.compare_artifacts(tree, changed)["state"], "uncalibrated")

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

    def test_owned_qdrant_readiness_uses_configured_api_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            run = qdrant_rss.Run.__new__(qdrant_rss.Run)
            run.output = Path(temporary) / "run"
            run.output.mkdir()
            run.storage_path = Path(temporary) / "storage"
            run.api_key = "secret"
            run.client_factory = object
            run.plan = {
                "url": "http://127.0.0.1:6333", "qdrant_bin": str(Path(sys.executable).resolve()),
                "qdrant_server_version": "1.19.0", "startup_timeout_s": 1, "poll_interval_s": 0,
            }
            process = MagicMock(pid=os.getpid())
            process.poll.return_value = None
            with patch.dict(qdrant_rss.os.environ, {"QDRANT__CLUSTER__ENABLED": "true"}), \
                    patch.object(qdrant_rss.subprocess, "Popen", return_value=process) as launch, \
                    patch.object(qdrant_rss.existing, "linux_process_identity", return_value="1:1"), \
                    patch.object(qdrant_rss.existing, "server_info", return_value={"version": "1.19.0"}) as info, \
                    patch.object(qdrant_rss.existing, "server_process_owns_endpoint", return_value=True), \
                    patch.object(qdrant_rss.existing, "server_process_identity", return_value="qdrant"):
                run.start_server()
            info.assert_called_once_with(run.plan["url"], "secret")
            self.assertNotIn("QDRANT__CLUSTER__ENABLED", launch.call_args.kwargs["env"])
            self.assertEqual(launch.call_args.kwargs["env"]["QDRANT__SERVICE__API_KEY"], "secret")
            run.server_log.close()

    def test_owned_qdrant_rejects_nonzero_shutdown(self):
        run = qdrant_rss.Run.__new__(qdrant_rss.Run)
        run.process = MagicMock(returncode=1)
        run.process.poll.return_value = None
        run.process_identity, run.server_log = None, None
        with self.assertRaisesRegex(RuntimeError, "shutdown exited with 1"):
            run.stop_server()


if __name__ == "__main__":
    unittest.main()
