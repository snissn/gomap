import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import minima_cohere_matched_performance as subject


class MatchedPerformanceTest(unittest.TestCase):
    def test_runtime_serving_preflight_binds_file_to_frozen_plan(self):
        def populated(shape):
            return {key: 1 if nested is None else populated(nested)
                    for key, nested in shape.items()}

        serving = populated(subject.native.existing.COLUMN_GRAPH_SERVING_SHAPE)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "serving.json"
            path.write_text(json.dumps(serving))
            plan = {
                "schema": subject.PLAN_SCHEMA, "pair_order": subject.PAIR_ORDER,
                "harness_sha256": "hash", "campaign_commit": "head", "source_trees": {},
                "qdrant_client_version": "1.19.0", "python_environment": {},
                "cpu_affinity": list(range(6)), "service_bin": str(root / "service"),
                "service_sha256": "hash", "qdrant_bin": str(root / "qdrant"),
                "qdrant_sha256": "hash", "serving_path": str(path),
                "serving_sha256": "hash", "serving": serving,
                "control_evidence": str(root / "control.json"),
                "control_evidence_sha256": "hash", "dataset": str(root / "dataset"),
                "dataset_manifest_sha256": "hash",
                "dataset_files_sha256": {name: "hash" for name in ("documents", "queries", "truth")},
            }
            patches = (
                mock.patch.object(subject, "digest", return_value="hash"),
                mock.patch.object(subject, "repository_commit", return_value="head"),
                mock.patch.object(subject, "repository_trees", return_value={}),
                mock.patch.object(subject, "python_environment", return_value={}),
                mock.patch.object(subject, "validate_host_identity"),
                mock.patch.object(subject, "validate_thread_environment"),
                mock.patch.object(subject, "go_binary_commit", return_value="head"),
                mock.patch.object(subject.native, "validate_imports"),
                mock.patch.object(subject.qdrant_rss, "validate_imports"),
                mock.patch.object(subject.importlib.metadata, "version", return_value="1.19.0"),
                mock.patch.object(subject.os, "sched_getaffinity", return_value=set(range(6))),
            )
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], \
                    patches[6], patches[7], patches[8], patches[9], patches[10]:
                subject.validate_runtime(plan, "hash", root / "plan.json")
                plan["serving"] = {**serving, "SearchCandidates": 2}
                with self.assertRaisesRegex(RuntimeError, "serving file differs"):
                    subject.validate_runtime(plan, "hash", root / "plan.json")
                plan["serving"] = {"SearchCandidates": 1}
                with self.assertRaises(ValueError):
                    subject.validate_runtime(plan, "hash", root / "plan.json")

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
            "selection_protocol": subject.native.RSS_SELECTION_PROTOCOL,
            "calibration": {"queries": list(range(100)), "curve": [
                {"control": 32, "mean_recall_at_10": .9, "per_query": [.9] * 100},
                {"control": 64, "mean_recall_at_10": .91, "per_query": [.91] * 100},
            ]}, "revalidation": {"queries": subject.MEASUREMENT_QUERIES, "passed": True, "curve": [
                {"control": 32, "mean_recall_at_10": .89, "per_query": [.89] * 100},
                {"control": 64, "mean_recall_at_10": .9, "per_query": [.9] * 100},
            ]}}}
        self.assertEqual(subject.reviewed_selected_control(artifact, "test", [32, 64, 128]), 64)
        artifact["quality"]["revalidation"]["curve"][0]["mean_recall_at_10"] = .9
        artifact["quality"]["revalidation"]["curve"][0]["per_query"] = [.9] * 100
        with self.assertRaisesRegex(RuntimeError, "lowest passing control"):
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
                "host_memory_bytes": 32 << 30, "treedb_go_runtime": runtime}
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "boot"}):
            with mock.patch.object(subject.native.existing.common, "memory_bytes",
                                   return_value=str(32 << 30)):
                subject.validate_host_identity(plan)
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "new"}):
            with self.assertRaisesRegex(RuntimeError, "host resource identity"):
                subject.validate_host_identity(plan)

        contract = {**plan, "cpu_affinity": [0, 1]}
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value={"boot_id": "boot"}), \
                mock.patch.object(subject.native.existing.common, "memory_bytes",
                                  return_value=str(32 << 30)), \
                mock.patch.object(subject.os, "sched_getaffinity", return_value={0, 1}):
            control_environment = subject.validate_control_environment(contract)
            self.assertEqual(control_environment["cpu_affinity"], [0, 1])
            self.assertEqual(control_environment["host_memory_bytes"], 32 << 30)
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

    def test_typed_memory_contract_round_trips_through_matched_performance(self):
        runtime = {
            key: subject.os.environ.get(key, "")
            for key in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT")
        }
        host = {
            "machine_id": "machine", "boot_id": "boot", "page_size_bytes": 4096,
            "numa_mems": "0", "cgroup_membership": "0::/", "cgroup_limits": {},
            "cpu_model": "test CPU", "cpu_features": ["avx2"],
        }
        files = {"documents": "b" * 64, "queries": "c" * 64, "truth": "d" * 64}
        with mock.patch.object(subject.platform, "platform", return_value="platform"), \
                mock.patch.object(subject.native, "host_resource_identity", return_value=host), \
                mock.patch.object(subject.qdrant_rss.existing, "memory_bytes",
                                  return_value=str(32 << 30)), \
                mock.patch.object(subject.os, "sched_getaffinity", return_value={0, 1}):
            contract = subject.qdrant_rss.comparison_contract("a" * 64, files, [0, 1])
            self.assertEqual(contract["host_memory_bytes"], 32 << 30)
            self.assertTrue(subject.native.rss_comparison_contract_valid(contract))
            environment = subject.validate_control_environment(contract)
            plan = {
                "platform": contract["platform"],
                "host_resource_identity": contract["host_resource_identity"],
                "host_memory_bytes": environment["host_memory_bytes"],
                "treedb_go_runtime": runtime,
            }
            subject.validate_host_identity(plan)

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
        qdrant_plan = subject.qdrant_plan(plan, Path("/tmp/run"))
        self.assertEqual(qdrant_plan["queries"], 200)
        self.assertEqual(qdrant_plan["resource_guard"], {
            "poll_interval_s": 1, "wall_limit_s": 3600,
            "minimum_free_bytes": 10 << 30, "maximum_owned_bytes": 12 << 30,
            "maximum_combined_rss_bytes": 26 << 30,
        })
        self.assertEqual(subject.tree_plan(plan, Path("/tmp/run"))["treedb_go_runtime"], runtime)

        with tempfile.TemporaryDirectory() as directory:
            limits = {"minimum_free_bytes": 0, "maximum_owned_bytes": 1 << 40,
                      "maximum_combined_rss_bytes": 1 << 40, "wall_limit_s": 60}
            run = SimpleNamespace(
                output=Path(directory), process=None, server_pid=None, cleaning_up=False,
                harness_identity="harness", plan={"resource_guard": limits},
                resource_lock=threading.Lock(), combined_peak_rss_bytes=0,
            )
            memory = {"VmRSS": 100, "VmHWM": 200}
            with mock.patch.object(subject, "qdrant_process_memory", return_value=memory):
                subject.check_qdrant_resources(run, subject.time.monotonic())
            self.assertGreater(run.combined_peak_rss_bytes, 0)
            limits["maximum_combined_rss_bytes"] = 1
            with mock.patch.object(subject, "qdrant_process_memory", return_value=memory), \
                    self.assertRaisesRegex(RuntimeError, "Qdrant disk/RAM/wall budget"):
                subject.check_qdrant_resources(run, subject.time.monotonic())
            limits["maximum_combined_rss_bytes"] = 1 << 40
            limits["wall_limit_s"] = -1
            with mock.patch.object(subject, "qdrant_process_memory", return_value=memory), \
                    self.assertRaisesRegex(RuntimeError, "Qdrant disk/RAM/wall budget"):
                subject.check_qdrant_resources(run, subject.time.monotonic())

        class CancelAfterThreeChecks:
            calls = 0
            def wait(self, _):
                self.calls += 1
                return self.calls == 3
        process = mock.Mock(pid=17)
        process.poll.return_value = None
        guarded = SimpleNamespace(
            process=process, server_pid=17, process_identity="owned",
            server_pgid=19, harness_pgid=19, cleaning_up=False,
            resource_lock=threading.Lock(), record_resource_failure=mock.Mock(),
        )
        failures = []
        with mock.patch.object(subject, "check_qdrant_resources", side_effect=RuntimeError("over")), \
                mock.patch.object(subject.qdrant_rss.existing, "linux_process_identity", return_value="owned"), \
                mock.patch.object(subject.os, "getpgid", return_value=19):
            cancel = CancelAfterThreeChecks()
            subject.guard_qdrant_resources(guarded, 0, cancel, failures)
        self.assertEqual((cancel.calls, failures), (1, ["resource guard: over"]))
        guarded.record_resource_failure.assert_called_once_with("resource guard: over")
        process.terminate.assert_called_once()

        tree_process = mock.Mock(pid=18)
        tree_guarded = SimpleNamespace(
            cancel=CancelAfterThreeChecks(), failure=None,
            check_resources=mock.Mock(side_effect=RuntimeError("over")), emit=mock.Mock(),
            controller=SimpleNamespace(process=tree_process, _owned_identity="owned"),
        )
        with mock.patch.object(subject.native.existing.common, "linux_process_identity",
                               side_effect=["owned", None]), \
                mock.patch.object(subject.native.os, "kill") as kill:
            subject.native.Run.guard(tree_guarded)
        self.assertEqual(tree_guarded.failure, "resource guard: over")
        self.assertEqual(tree_guarded.cancel.calls, 3)
        kill.assert_called_once_with(18, subject.native.signal.SIGTERM)

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

    def test_qdrant_resource_monitor_rejects_process_and_identity_drift(self):
        process = mock.Mock(pid=17, returncode=3)
        run = SimpleNamespace(
            process=process, server_pid=17, process_identity="17:11",
            server_pgid=19, harness_pgid=19, harness_identity="6:10",
            cleaning_up=False, resource_lock=threading.Lock(),
            combined_peak_rss_bytes=0, output=Path("/tmp"),
            plan={"resource_guard": {"minimum_free_bytes": 0, "maximum_owned_bytes": 1 << 40,
                  "maximum_combined_rss_bytes": 1 << 40, "wall_limit_s": 60}},
        )
        process.poll.return_value = 3
        with mock.patch.object(subject, "qdrant_process_memory", return_value={"VmRSS": 1, "VmHWM": 1}), \
                self.assertRaisesRegex(RuntimeError, "exited during resource monitoring"):
            subject.check_qdrant_resources(run, subject.time.monotonic())
        process.poll.return_value = None
        with mock.patch.object(subject.os, "getpgid", return_value=20), \
                mock.patch.object(subject, "qdrant_process_memory", return_value={"VmRSS": 1, "VmHWM": 1}), \
                self.assertRaisesRegex(RuntimeError, "identity or process group changed"):
            subject.check_qdrant_resources(run, subject.time.monotonic())

        status = "VmRSS:\t10 kB\nVmHWM:\t20 kB\n"
        with mock.patch.object(subject.Path, "read_text", return_value=status), \
                mock.patch.object(subject.qdrant_rss.existing, "linux_process_identity",
                                  side_effect=["17:11", "17:12"]), \
                self.assertRaisesRegex(RuntimeError, "changed across"):
            subject.qdrant_process_memory(17, "17:11")

    def test_qdrant_auxiliary_clients_all_close_after_one_failure(self):
        clients = [mock.Mock(), mock.Mock(), mock.Mock()]
        clients[1].close.side_effect = RuntimeError("close failed")
        owned = list(clients)
        with self.assertRaisesRegex(RuntimeError, "close failed"):
            subject.close_qdrant_clients(owned)
        self.assertEqual(owned, [])
        for client in clients:
            client.close.assert_called_once_with()

    def test_qdrant_failure_still_runs_owned_cleanup(self):
        fake_run = SimpleNamespace(
            resource_lock=threading.Lock(), combined_peak_rss_bytes=0, cleaning_up=False,
            start_server=mock.Mock(side_effect=RuntimeError("launch failed")),
            cleanup_owned=mock.Mock(return_value={"status": "clean", "failure": None}),
        )
        monitor = mock.Mock()
        monitor.is_alive.return_value = False
        qdrant_module = SimpleNamespace(QdrantClient=object, models=object)
        plan = {"qdrant_configuration": {
            "initial_upload_hnsw": {}, "initial_upload_optimizers": {},
            "production_hnsw": {}, "production_optimizers": {},
        }}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.dict(subject.sys.modules, {"qdrant_client": qdrant_module}), \
                mock.patch.object(subject.qdrant_rss, "Run", return_value=fake_run) as construct, \
                mock.patch.object(subject.threading, "Thread", return_value=monitor), \
                mock.patch.object(subject, "check_qdrant_resources") as resources:
            run_dir = Path(directory) / "run"
            result = subject.run_qdrant(plan, run_dir)
        self.assertEqual(result["state"], "failed")
        self.assertIn("launch failed", result["failure"])
        self.assertIn("resource_guard", construct.call_args.args[0])
        fake_run.cleanup_owned.assert_called_once_with()
        self.assertEqual(resources.call_count, 2)

    def test_qdrant_constructor_failure_retains_a_result_directory(self):
        qdrant_module = SimpleNamespace(QdrantClient=object, models=object)
        plan = {"qdrant_configuration": {
            "initial_upload_hnsw": {}, "initial_upload_optimizers": {},
            "production_hnsw": {}, "production_optimizers": {},
        }}
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.dict(subject.sys.modules, {"qdrant_client": qdrant_module}), \
                mock.patch.object(subject.qdrant_rss, "Run", side_effect=RuntimeError("identity unavailable")):
            run_dir = Path(directory) / "run"
            result = subject.run_qdrant(plan, run_dir)
            self.assertTrue(run_dir.is_dir())
        self.assertEqual(result["state"], "failed")
        self.assertIn("identity unavailable", result["failure"])

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
