"""Fail-closed checks for the matched Cohere 768D RSS boundary."""
import copy
import unittest
from unittest.mock import patch

import numpy as np

import minima_cohere_native_diagnostic as native
import minima_cohere_qdrant_rss_diagnostic as qdrant_rss


class CohereQdrantRSSDiagnosticTests(unittest.TestCase):
    def test_control_selection_uses_disjoint_calibration_and_evaluation(self):
        truth = [[f"row-{query}-{rank}" for rank in range(10)] for query in range(6)]
        calls = []

        def search(control, query):
            calls.append((control, query))
            keep = {8: 8, 16: 9}[control]
            return truth[query][:keep] + [f"miss-{query}-{rank}" for rank in range(10 - keep)]

        quality = native.calibrate_ann_control(search, truth, [8, 16], [0, 1], [2, 3, 4, 5], .9)
        self.assertEqual(quality["selected_control"], 16)
        self.assertEqual(quality["calibration"]["queries"], [0, 1])
        self.assertEqual(quality["evaluation"]["queries"], [2, 3, 4, 5])
        self.assertTrue(quality["evaluation"]["passed"])
        self.assertEqual(calls, [(8, 0), (8, 1), (16, 0), (16, 1),
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
        with self.assertRaisesRegex(RuntimeError, "query-ready"):
            qdrant_rss.validate_ready_snapshot(changed, 500000)

    def test_comparison_accepts_only_matched_calibrated_artifacts(self):
        contract = {"schema": "cohere-rss/v1", "rows": 500000, "dimensions": 768,
                    "dataset_files_sha256": {"documents": "a" * 64}, "cpu_affinity": [0, 1]}
        tree = {"schema": native.RSS_ARTIFACT_SCHEMA, "state": "calibrated", "backend": "treedb",
                "comparison_contract": contract, "quality": {"evaluation": {"passed": True}},
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
                    changed["quality"]["evaluation"]["passed"] = False
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


if __name__ == "__main__":
    unittest.main()
