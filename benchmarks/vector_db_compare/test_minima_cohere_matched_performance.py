import json
from pathlib import Path
import tempfile
import threading
import unittest

import minima_cohere_matched_performance as subject


class MatchedPerformanceTest(unittest.TestCase):
    def test_distribution_uses_nearest_rank_percentiles(self):
        self.assertEqual(subject.distribution([4, 1, 3, 2]), {
            "count": 4, "mean_ns": 2.5, "p50_ns": 2, "p95_ns": 4, "p99_ns": 4, "max_ns": 4,
        })

    def test_mean_recall(self):
        self.assertEqual(subject.mean_recall([["a", "b"], ["x", "z"]], [["a", "c"], ["x", "y"]]), {
            "mean_recall_at_10": .5, "per_query": [.5, .5],
        })

    def test_timed_window_runs_each_worker(self):
        result, samples = subject.timed_window(lambda query: query, [0, 1], 2, .01)
        self.assertGreater(result["qps"], 0)
        self.assertEqual(result["latency"]["count"], len(samples))

    def test_adapter_plan_and_mixed_overlap(self):
        plan = {"queries": 200, "qdrant_configuration": {
            "initial_upload_hnsw": {}, "initial_upload_optimizers": {},
            "production_hnsw": {}, "production_optimizers": {},
        }}
        self.assertEqual(subject.qdrant_plan(plan, Path("/tmp/run"))["queries"], 200)

        writer_started, reader_seen = threading.Event(), threading.Event()
        def read(query):
            self.assertTrue(writer_started.wait(1))
            reader_seen.set()
            return query
        def write():
            writer_started.set()
            self.assertTrue(reader_seen.wait(1))
            return "complete"
        result, reads, writes = subject.mixed_window(read, [write], [0, 1], 2)
        self.assertGreater(result["overlapping_reads"]["latency"]["count"], 0)
        self.assertEqual((len(reads), writes), (128, ["complete"]))

    def test_lock_order_and_result_provenance_fail_closed(self):
        plan = {"pair_order": [["treedb", "qdrant"]], "campaign_commit": "c",
                "harness_sha256": "h", "dataset_manifest_sha256": "m",
                "dataset_files_sha256": {"documents": "d"}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock = subject.acquire_campaign_lock(root)
            try:
                with self.assertRaisesRegex(RuntimeError, "another campaign"):
                    subject.acquire_campaign_lock(root)
            finally:
                lock.close()
            self.assertEqual(subject.require_predecessors(plan, root, "p", 1, "treedb"), 1)
            with self.assertRaisesRegex(RuntimeError, "predecessor"):
                subject.require_predecessors(plan, root, "p", 1, "qdrant")
            result = {"state": "complete", "backend": "treedb", "repetition": 1, "order": 1,
                      "plan_sha256": "p", "campaign_commit": "c", "harness_sha256": "h",
                      "dataset_manifest_sha256": "m", "dataset_files_sha256": {"documents": "d"}}
            path = root / "repeat-1-treedb" / "result.json"
            path.parent.mkdir()
            path.write_text(json.dumps(result))
            self.assertEqual(subject.require_predecessors(plan, root, "p", 1, "qdrant"), 2)
            result["campaign_commit"] = "wrong"
            path.write_text(json.dumps(result))
            with self.assertRaisesRegex(RuntimeError, "identity mismatch"):
                subject.require_predecessors(plan, root, "p", 1, "qdrant")


if __name__ == "__main__":
    unittest.main()
