import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

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

    def test_reviewed_control_requires_every_lower_candidate_to_fail(self):
        artifact = {"quality": {"selected_control": 64, "target_mean_recall_at_10": .9,
            "calibration": {"curve": [
                {"control": 32, "mean_recall_at_10": .89},
                {"control": 64, "mean_recall_at_10": .91},
            ]}, "evaluation": {"passed": True, "mean_recall_at_10": .9}}}
        self.assertEqual(subject.reviewed_selected_control(artifact, "test", [32, 64, 128]), 64)
        artifact["quality"]["calibration"]["curve"][0]["mean_recall_at_10"] = .9
        with self.assertRaisesRegex(RuntimeError, "lowest passing"):
            subject.reviewed_selected_control(artifact, "test", [32, 64, 128])

    def test_control_and_host_provenance_fail_closed(self):
        trees = {path: path + "-tree" for path in subject.SOURCE_PATHS}
        hashes = {"service": "service", "serving": "serving", "qdrant": "qdrant"}
        tree = {"provenance": {"harness_commit": "head", "product_commit": "head",
            "harness_trees": {path: trees[path] for path in subject.HARNESS_PATHS},
            "product_trees": {path: trees[path] for path in subject.PRODUCT_PATHS},
            "service_sha256": "service", "serving_sha256": "serving"}}
        qdrant = {"provenance": {"harness_commit": "head",
            "harness_trees": {path: trees[path] for path in subject.HARNESS_PATHS},
            "qdrant_bin_sha256": "qdrant", "qdrant_client_version": "1.19.0"}}
        subject.validate_control_provenance(tree, qdrant, "head", trees, hashes)
        tree["provenance"]["product_commit"] = "old"
        with self.assertRaisesRegex(RuntimeError, "campaign checkout"):
            subject.validate_control_provenance(tree, qdrant, "head", trees, hashes)

        runtime = {key: subject.os.environ.get(key, "") for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")}
        plan = {"platform": "platform", "host_resource_identity": {"boot_id": "boot"},
                "host_memory_bytes": "memory", "treedb_go_runtime": runtime}
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "boot"}):
            with mock.patch.object(subject.native.existing.common, "memory_bytes", return_value="memory"):
                subject.validate_host_identity(plan)
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "new"}):
            with self.assertRaisesRegex(RuntimeError, "host resource identity"):
                subject.validate_host_identity(plan)

        contract = {**plan, "cpu_affinity": [0, 1]}
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "boot"}), \
                mock.patch.object(subject.native.existing.common, "memory_bytes", return_value="memory"), \
                mock.patch.object(subject.os, "sched_getaffinity", return_value={0, 1}):
            self.assertEqual(subject.validate_control_environment(contract)["cpu_affinity"], [0, 1])
            contract["cpu_affinity"] = [0]
            with self.assertRaisesRegex(RuntimeError, "different environment"):
                subject.validate_control_environment(contract)
        environment = subject.python_environment()
        self.assertEqual(json.loads(json.dumps(environment)), environment)

        with mock.patch.dict(subject.os.environ, subject.THREAD_ENVIRONMENT, clear=False):
            subject.validate_thread_environment()
            subject.os.environ["GOMAXPROCS"] = "1"
            with self.assertRaisesRegex(RuntimeError, "GOMAXPROCS=6"):
                subject.validate_thread_environment()

        run = mock.Mock(failure="resource guard: over budget")
        with self.assertRaisesRegex(RuntimeError, "over budget"):
            subject.check_final_tree_resources(run, mock.Mock(is_alive=lambda: False))
        run.check_resources.assert_called_once()

    def test_timed_window_runs_each_worker(self):
        result, samples = subject.timed_window(lambda query: query,
            lambda value: self.assertIn(value, (0, 1)), [0, 1], 2, .01)
        self.assertGreater(result["qps"], 0)
        self.assertEqual(result["latency"]["count"], len(samples))
        def reject(_):
            raise RuntimeError("invalid response")
        with self.assertRaisesRegex(RuntimeError, "invalid response"):
            subject.timed_window(lambda query: query, reject, [0], 1, .001)

    def test_tree_query_validation_checks_logical_documents(self):
        documents = [SimpleNamespace(
            id=f"row-{row:06d}", content=f"minima-cohere:{row}", embedding=None, score=.5,
            meta={"user_id": f"{(row * 7919) % subject.ROWS:06d}",
                  "fpath": f"/cohere/{row // 256:06d}.txt"},
        ) for row in range(subject.TOP_K)]
        response = SimpleNamespace(
            native_command_version=2, documents=documents, route="ann", exact_fallbacks=0,
            full_document_scan_fallbacks=0,
            index=SimpleNamespace(generation=7, vector_strategy="column_graph", extra={"typed_input": True}),
            dense_work=SimpleNamespace(completed=True,
                                       output=SimpleNamespace(fetched=subject.TOP_K)),
        )
        subject.validate_tree_queries([response], 7)
        documents[-1].content += ":updated"
        with self.assertRaisesRegex(RuntimeError, "invalid logical document"):
            subject.validate_tree_queries([response], 7)
        subject.validate_tree_queries([response], 7, [subject.TOP_K - 1])
        documents[-1].content = f"minima-cohere:{subject.TOP_K - 1}"
        with self.assertRaisesRegex(RuntimeError, "invalid logical document"):
            subject.validate_tree_queries([response], 7, [subject.TOP_K - 1])
        subject.validate_tree_queries([response], 7, transitioning_rows=[subject.TOP_K - 1])
        documents[-1] = documents[0]
        with self.assertRaisesRegex(RuntimeError, "duplicate logical IDs"):
            subject.validate_tree_queries([response], 7)

    def test_qdrant_query_validation_checks_logical_documents(self):
        points = []
        for row in range(subject.TOP_K):
            logical_id = f"row-{row:06d}"
            points.append(SimpleNamespace(
                id=subject.qdrant_rss.existing.point_id(logical_id), score=.5, vector=None,
                payload={"id": logical_id, "content": f"minima-cohere:{row}", "meta": {
                    "user_id": f"{(row * 7919) % subject.ROWS:06d}",
                    "fpath": f"/cohere/{row // 256:06d}.txt",
                }},
            ))
        response = SimpleNamespace(points=points)
        subject.validate_qdrant_queries([response])
        points[-1].payload["content"] += ":updated"
        with self.assertRaisesRegex(RuntimeError, "invalid logical document"):
            subject.validate_qdrant_queries([response])
        subject.validate_qdrant_queries([response], [subject.TOP_K - 1])
        points[-1].payload["content"] = f"minima-cohere:{subject.TOP_K - 1}"
        with self.assertRaisesRegex(RuntimeError, "invalid logical document"):
            subject.validate_qdrant_queries([response], [subject.TOP_K - 1])
        subject.validate_qdrant_queries([response], transitioning_rows=[subject.TOP_K - 1])

    def test_adapter_plan_and_mixed_overlap(self):
        runtime = {"GOMAXPROCS": "6", "GOGC": "80", "GOMEMLIMIT": "20GiB"}
        plan = {"queries": 200, "treedb_go_runtime": runtime, "qdrant_configuration": {
            "initial_upload_hnsw": {}, "initial_upload_optimizers": {},
            "production_hnsw": {}, "production_optimizers": {},
        }}
        self.assertEqual(subject.qdrant_plan(plan, Path("/tmp/run"))["queries"], 200)
        self.assertEqual(subject.tree_plan(plan, Path("/tmp/run"))["treedb_go_runtime"], runtime)

        with tempfile.TemporaryDirectory() as directory:
            limits = {"minimum_free_bytes": 0, "maximum_output_bytes": 1 << 40,
                      "maximum_combined_rss_bytes": 1 << 40, "wall_limit_s": 60}
            run = SimpleNamespace(output=Path(directory), process=None, plan=limits)
            subject.check_qdrant_resources(run, subject.time.monotonic())
            limits["wall_limit_s"] = -1
            with self.assertRaisesRegex(RuntimeError, "Qdrant disk/RAM/wall budget"):
                subject.check_qdrant_resources(run, subject.time.monotonic())

        class CancelAfterThreeChecks:
            calls = 0
            def wait(self, _):
                self.calls += 1
                return self.calls == 3
        process = mock.Mock(pid=17)
        process.poll.side_effect = [None, 0]
        guarded = SimpleNamespace(process=process, process_identity="owned")
        failures = []
        with mock.patch.object(subject, "check_qdrant_resources", side_effect=RuntimeError("over")), \
                mock.patch.object(subject.qdrant_rss.existing, "linux_process_identity", return_value="owned"):
            cancel = CancelAfterThreeChecks()
            subject.guard_qdrant_resources(guarded, 0, cancel, failures)
        self.assertEqual((cancel.calls, failures), (3, ["resource guard: over"]))
        process.terminate.assert_called_once()

        writer_started, reader_seen = threading.Event(), threading.Event()
        def read(query):
            self.assertTrue(writer_started.wait(1))
            reader_seen.set()
            return query
        def write():
            writer_started.set()
            self.assertTrue(reader_seen.wait(1))
            return "complete"
        result, reads, writes = subject.mixed_window(read, [([7], write)], [0, 1], 2, 64, 256)
        self.assertGreater(result["overlapping_reads"]["latency"]["count"], 0)
        self.assertEqual((len(reads), [value for _, _, value, _ in writes]), (128, ["complete"]))
        self.assertEqual(subject.mixed_read_state(25, 35, [
            (10, 20, None, [1]), (30, 40, None, [2]), (50, 60, None, [3]),
        ]), ({1}, {2}))

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
