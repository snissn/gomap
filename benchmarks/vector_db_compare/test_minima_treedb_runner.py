from __future__ import annotations

import json
import io
import os
import socket
import sys
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parents[2] / "clients/python/treedb_client/src"))
import minima_qdrant_runner as common
import minima_treedb_runner as runner


class FakeClient:
    def __init__(self, response: object, *, present_ids: set[str] | None = None, count: int = 0,
                 upsert_error: BaseException | None = None) -> None:
        self.response = response
        self.call: tuple[object, ...] | None = None
        self.present_ids = set(present_ids or ())
        self.count = count
        self.upsert_error = upsert_error

    def query_by_embedding(self, *args: object, **kwargs: object) -> object:
        self.call = (*args, kwargs)
        return self.response

    def upsert_documents(self, _index: str, documents: list[dict[str, object]],
                         **_kwargs: object) -> object:
        if self.upsert_error is not None:
            raise self.upsert_error
        ids = [str(row["id"]) for row in documents]
        self.count += sum(identifier not in self.present_ids for identifier in ids)
        self.present_ids.update(ids)
        return SimpleNamespace(upserted=len(documents), ids=ids)

    def filter_documents(self, _index: str, filter: dict[str, object], **_kwargs: object) -> object:
        identifier = str(filter["value"])
        documents = [SimpleNamespace(id=identifier)] if identifier in self.present_ids else []
        return SimpleNamespace(matched_count=len(documents), documents=documents)

    def delete_by_filter(self, *_args: object, **_kwargs: object) -> object:
        self.count = 0
        self.present_ids.clear()
        return SimpleNamespace(deleted=1)

    def count_documents(self, *_args: object, **_kwargs: object) -> object:
        return SimpleNamespace(count=self.count)


def write_health_service(binary: Path) -> None:
    binary.write_text(f"""#!{sys.executable}
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-addr", required=True)
parser.add_argument("-dir", required=True)
parser.add_argument("-profile")
args = parser.parse_args()
data_dir = Path(args.dir)
data_dir.mkdir(parents=True, exist_ok=True)
(data_dir / "pid").write_text(str(os.getpid()), encoding="utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        contract = "{runner.SERVICE_CONTRACT}" if (data_dir / "compatible").exists() else "wrong"
        body = json.dumps({{"ok": True, "contract_version": contract}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

class Server(HTTPServer):
    allow_reuse_address = True

host, port = args.addr.rsplit(":", 1)
Server((host, int(port)), Handler).serve_forever()
""", encoding="utf-8")
    binary.chmod(0o755)




class MinimaTreeDBRunnerTest(unittest.TestCase):
    def workload(self, response: object, *, diagnostics_dir: Path | None = None,
                 client: FakeClient | None = None) -> runner.TreeDBMinimaRunner:
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.client = client or FakeClient(response)
        workload.collection = "owned"
        workload.config = {"top_k": 5, "batch_size": 3}
        workload.ef_search = 64
        workload.specs = {"mixed": {"name": "mixed", "filter": "user_id+fpath", "user_id": "u", "fpath": "/a"}}
        workload.queries = {"mixed": {"vector": [1.0, 0.0]}}
        workload.evidence = common.Evidence({"corpora": [{"name": "mixed"}]})
        workload.route_evidence = {}
        workload.diagnostics_dir = diagnostics_dir
        workload.diagnostic_slow_seconds = 30
        workload.diagnostic_profile_seconds = 1
        workload.diagnostic_capture_timeout = 1
        workload.batch_correlations = []
        workload.diagnostic_resume = None
        workload._diagnostic_lock = runner.threading.Lock()
        workload._expected_rows = 0
        workload._expected_insert_batches = {}
        workload.controller = SimpleNamespace(
            stats_snapshot=lambda: {"status": "disabled"},
            capture_profiles=lambda *_args, **_kwargs: {"status": "captured"},
        )
        return workload

    def response(self, **changes: object) -> object:
        values = {
            "route": "ann", "native_base_plus_live_delta": True, "exact_fallbacks": 0,
            "full_document_scan_fallbacks": 0, "documents": [SimpleNamespace(
                id="d", content="c", score=1.0, meta={"user_id": "u", "fpath": "/a"})],
        }
        values.update(changes)
        return SimpleNamespace(**values)

    def test_repository_commit_requires_clean_staged_unstaged_and_untracked_state(self) -> None:
        for porcelain in ("M  staged.py\n", " M unstaged.py\n", "?? untracked.py\n"):
            with self.subTest(porcelain=porcelain), \
                 mock.patch.object(runner.subprocess, "run", return_value=SimpleNamespace(stdout=porcelain)) as run:
                with self.assertRaisesRegex(RuntimeError, "clean source checkout"):
                    runner.repository_commit()
                run.assert_called_once_with(
                    ["git", "status", "--porcelain", "--untracked-files=all"],
                    cwd=Path(runner.__file__).resolve().parents[2],
                    check=True, capture_output=True, text=True,
                )

    def test_repository_commit_binds_head_after_clean_status(self) -> None:
        commit = "a" * 40
        with mock.patch.object(
            runner.subprocess, "run",
            side_effect=[SimpleNamespace(stdout=""), SimpleNamespace(stdout=commit + "\n")],
        ) as run:
            self.assertEqual(runner.repository_commit(), commit)
        self.assertEqual(run.call_count, 2)

    def test_initial_load_phase_excludes_setup_and_aligns_resource_boundary(self) -> None:
        baseline = {"captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "disk_bytes": 100}
        end = {"captured": True, "rss_bytes": 20, "cpu_seconds": 2.0, "disk_bytes": 200}
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload._phase_total_start = None
        workload._phase_start = None
        workload._phase_name = None
        workload._phase_resource_start = None
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload.controller = SimpleNamespace(pid=123)
        workload.storage_path = Path("/data")
        workload.process_identity = lambda pid: f"process-{pid}"
        workload.resource_server_name = "TreeDB"
        events: list[str] = []
        ticks = iter((500, 700, 710))
        resources = iter((baseline, end))

        def monotonic_ns() -> int:
            events.append("wall")
            return next(ticks)

        def resource_usage(*_args: object) -> dict[str, object]:
            events.append("resource")
            return next(resources)

        with mock.patch.object(runner.time, "monotonic_ns", side_effect=monotonic_ns), \
             mock.patch.object(common, "server_resource_usage", side_effect=resource_usage):
            workload.begin_phase_attribution()
            workload.phase_transition("warmup_search")

        initial = workload._phase_boundaries[0]
        self.assertEqual(workload._phase_total_start, 500)
        self.assertEqual(initial["start_nanos"], workload._phase_total_start)
        self.assertEqual(initial["start_nanos"] - 100, 400)
        self.assertEqual(initial["duration_nanos"], 200)
        self.assertEqual(initial["resource_segments"][0]["start"]["pid"], 123)
        self.assertEqual(initial["resource_segments"][0]["end"]["pid"], 123)

    def test_restart_phase_splits_old_and_new_process_resources(self) -> None:
        old_start = {
            "captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "disk_bytes": 100,
            "pid": 100, "process_identity": "old-process",
        }
        old_end = {
            "captured": True, "rss_bytes": 12, "cpu_seconds": 2.0, "disk_bytes": 120,
            "pid": 100, "process_identity": "old-process",
        }
        new_end = {"captured": True, "rss_bytes": 20, "cpu_seconds": 3.0, "disk_bytes": 140}
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload._phase_total_start = 100
        workload._phase_start = 100
        workload._phase_name = "restart_open_readiness"
        workload._phase_resource_start = old_start
        workload._phase_restart_old_end = old_end
        workload._phase_boundaries = []
        workload.controller = SimpleNamespace(pid=101)
        workload.storage_path = Path("/data")
        workload.resource_server_name = "TreeDB"
        workload.process_identity = lambda pid: "new-process" if pid == 101 else "old-process"
        with mock.patch.object(runner.time, "monotonic_ns", side_effect=(200, 210)), \
             mock.patch.object(common, "server_resource_usage", return_value=new_end):
            workload.phase_transition("post_reopen")
        segments = workload._phase_boundaries[0]["resource_segments"]
        self.assertEqual([segment["start"]["pid"] for segment in segments], [100, 101])
        self.assertEqual([segment["end"]["pid"] for segment in segments], [100, 101])
        self.assertEqual(segments[1]["start"]["cpu_seconds"], 0.0)
        self.assertEqual(segments[1]["start"]["rss_bytes"], 0)
        self.assertEqual(segments[1]["start"]["disk_bytes"], old_end["disk_bytes"])
        self.assertEqual(segments[1]["end"]["cpu_seconds"], 3.0)

    def test_restart_controller_uses_shutdown_endpoint_for_phase_and_aggregate(self) -> None:
        baseline = {"captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "disk_bytes": 100}
        shutdown_end = {"captured": True, "rss_bytes": 15, "cpu_seconds": 3.0, "disk_bytes": 140}

        class Controller:
            pid = 100
            last_shutdown_resource_end = shutdown_end

            def start(self) -> None:
                self.pid = 101

        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.controller = Controller()
        workload.restart_origin = (100, "old-process")
        workload._controller_restart_origin = (100, "old-process")
        workload.restart_origin_resource_end = None
        workload.resource_baseline = baseline
        workload.completed_resource_segments = []
        workload._phase_restart_old_end = None

        self.assertEqual(workload.restart_controller(), 101)
        self.assertEqual(workload._phase_restart_old_end["pid"], 100)
        self.assertEqual(workload._phase_restart_old_end["process_identity"], "old-process")
        self.assertIs(workload.restart_origin_resource_end, shutdown_end)
        segment = workload.completed_resource_segments[0]
        self.assertEqual(segment["cpu_seconds"], 2.0)
        self.assertEqual(segment["disk_bytes"], 40)

    def test_restart_aggregate_uses_process_lifetime_baseline(self) -> None:
        old_end = {"captured": True, "rss_bytes": 15, "cpu_seconds": 3.0, "disk_bytes": 140}
        new_end = {"captured": True, "rss_bytes": 20, "cpu_seconds": 1.5, "disk_bytes": 160}
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.restart_origin_resource_end = old_end
        workload.resource_baseline = None

        def base_restart() -> None:
            workload.resource_baseline = new_end

        with mock.patch.object(common.QdrantMinimaRunner, "restart_backend", side_effect=base_restart):
            workload.restart_backend()
        self.assertEqual(workload.resource_baseline["rss_bytes"], 0)
        self.assertEqual(workload.resource_baseline["cpu_seconds"], 0.0)
        self.assertEqual(workload.resource_baseline["disk_bytes"], old_end["disk_bytes"])


    def resume_workload(self, directory: Path, *, present_ids: set[str], count: int) -> runner.TreeDBMinimaRunner:
        workload = self.workload(
            self.response(), diagnostics_dir=directory,
            client=FakeClient(self.response(), present_ids=present_ids, count=count),
        )
        workload.config = {"top_k": 5, "batch_size": 256}
        workload.specs = {
            "resume": {
                "name": "resume", "corpus_rows": 1024, "filter": "user_id",
                "eligible_start": 0, "eligible_rows": 0, "user_id": "target", "fpath": "",
            },
        }
        workload.manifest = {
            "operations": [{
                "name": "initial_batch_insert",
                "insert_ranges": [{"scenario": "resume", "start": 0, "rows": 1024}],
            }],
        }
        workload._expected_insert_batches = {("initial_batch_insert", "resume", 512): 768}
        workload.storage_path = directory
        workload.resource_server_name = "TreeDB"
        workload.controller.pid = 123
        workload.connect = lambda: None
        workload.ensure_compatible = lambda: None
        workload._initial_prefix_identity = lambda *_args: {
            "algorithm": "test", "expected_rows": 512, "actual_rows": 512,
            "expected_digest": "expected", "actual_digest": "expected", "match": True,
        }
        return workload

    def test_qdrant_lifecycle_hooks_are_isolated_from_treedb(self) -> None:
        workload = self.workload(self.response())
        workload.restore_production_configuration = mock.Mock(
            side_effect=AssertionError("Qdrant production transition invoked for TreeDB"),
        )
        workload.initial_load_to_query_boundary()
        workload.restore_production_configuration.assert_not_called()
        workload.wait_ready(expected_count=0, phase="initial_load_to_query")

    def test_search_uses_public_filtered_ann_and_captures_actual_interval(self) -> None:
        workload = self.workload(self.response())
        interval: dict[str, int] = {}
        ids, scores = workload.search("timed", "mixed", interval)
        self.assertEqual((ids, scores), (["d"], [1.0]))
        self.assertLess(interval["started_monotonic_ns"], interval["ended_monotonic_ns"])
        call = workload.client.call
        assert call is not None
        self.assertEqual(call[:3], ("owned", [1.0, 0.0], 5))
        self.assertEqual(call[3], {"operator": "AND", "conditions": [
            {"field": "meta.user_id", "operator": "==", "value": "u"},
            {"field": "meta.fpath", "operator": "==", "value": "/a"},
        ]})
        self.assertEqual(call[4], {"route": "ann", "ef_search": 64})
        self.assertIs(workload.route_evidence["mixed"], workload.client.response)

    def test_search_rejects_hidden_exact_fallback(self) -> None:
        workload = self.workload(self.response(exact_fallbacks=1))
        with self.assertRaisesRegex(RuntimeError, "left required native route"):
            workload.search("final", "mixed")
        self.assertNotIn("mixed", workload.route_evidence)

    def test_document_mapping_keeps_minima_fields_in_public_meta(self) -> None:
        document = {"id": "d", "content": "c", "vector": [1.0, 0.0], "user_id": "u", "fpath": "/a"}
        self.assertEqual(runner.service_document(document), {
            "id": "d", "content": "c", "embedding": [1.0, 0.0],
            "meta": {"user_id": "u", "fpath": "/a"},
        })

    def test_mutation_overrides_forward_writer_start_callbacks(self) -> None:
        workload = self.workload(self.response())
        workload.controller.stats_snapshot = mock.Mock(side_effect=AssertionError("default upsert requested diagnostics"))
        starts: list[str] = []
        document = {"id": "d", "content": "c", "vector": [1.0, 0.0], "user_id": "u", "fpath": "/a"}
        workload.upsert(
            "replacement_insert", "mixed", [document],
            on_writer_start=lambda: starts.append("upsert"),
        )
        workload.delete_filter(
            {
                "name": "delete_by_user_id_and_fpath", "target": "mixed",
                "filter": {"user_id": "u", "fpath": "/a"},
            },
            on_writer_start=lambda: starts.append("delete"),
        )
        self.assertEqual(starts, ["upsert", "delete"])
        self.assertEqual(workload.batch_correlations, [])
        workload.controller.stats_snapshot.assert_not_called()
    def test_diagnostic_upsert_correlates_batch_and_snapshots(self) -> None:
        workload = self.workload(self.response(), diagnostics_dir=Path("diagnostics"))
        snapshots = iter(({"status": "captured", "snapshot": {"stage": "before"}},
                          {"status": "captured", "snapshot": {"stage": "after"}}))
        workload.controller.stats_snapshot = lambda: next(snapshots)
        workload._expected_insert_batches[("initial_batch_insert", "mixed", 3)] = 6
        documents = [
            {"id": f"minima/mixed/{ordinal:06d}", "content": "c", "vector": [1.0, 0.0],
             "user_id": "u", "fpath": "/a"}
            for ordinal in range(3, 6)
        ]
        workload.upsert("initial_batch_insert", "mixed", documents)
        correlation = workload.batch_correlations[0]
        self.assertEqual(
            (correlation["operation"], correlation["scenario"], correlation["batch_ordinal"],
             correlation["batch_start"], correlation["rows"], correlation["accumulated_expected_rows"]),
            ("initial_batch_insert", "mixed", 1, 3, 3, 6),
        )
        self.assertEqual(correlation["before_stats"]["snapshot"]["stage"], "before")
        self.assertEqual(correlation["after_stats"]["snapshot"]["stage"], "after")
        self.assertEqual(correlation["outcome"], "completed")
        self.assertEqual(correlation["profile_capture"], {"status": "not_triggered"})
    def test_completed_upsert_stops_slow_watcher_before_after_stats(self) -> None:
        workload = self.workload(self.response(), diagnostics_dir=Path("diagnostics"))
        workload.diagnostic_slow_seconds = 0.01
        workload._expected_insert_batches[("initial_batch_insert", "mixed", 0)] = 1
        snapshots = 0

        def stats_snapshot() -> dict[str, object]:
            nonlocal snapshots
            snapshots += 1
            if snapshots == 2:
                runner.time.sleep(0.03)
            return {"status": "captured"}

        workload.controller.stats_snapshot = stats_snapshot
        workload.controller.capture_profiles = mock.Mock(return_value={"status": "captured"})
        document = {"id": "minima/mixed/000000", "content": "c", "vector": [1.0, 0.0],
                    "user_id": "u", "fpath": "/a"}
        with mock.patch.object(runner.time, "monotonic_ns", side_effect=range(100, 300, 10)):
            workload.upsert("initial_batch_insert", "mixed", [document])
        workload.controller.capture_profiles.assert_not_called()
        self.assertEqual(workload.batch_correlations[0]["profile_capture"], {"status": "not_triggered"})

    def test_diagnostic_batch_watchers_keep_iteration_local_capture_state(self) -> None:
        workload = self.workload(self.response(), diagnostics_dir=Path("diagnostics"))
        workload._expected_insert_batches = {
            ("initial_batch_insert", "mixed", 0): 3,
            ("initial_batch_insert", "mixed", 3): 6,
        }
        documents = [
            {"id": f"minima/mixed/{ordinal:06d}", "content": "c", "vector": [1.0, 0.0],
             "user_id": "u", "fpath": "/a"}
            for ordinal in range(6)
        ]
        deferred: list[object] = []

        class DeferredThread:
            def __init__(self, *, target: object, **_kwargs: object) -> None:
                self.target = target
                deferred.append(self)

            def start(self) -> None:
                pass

            def join(self, _timeout: float) -> None:
                pass

            def is_alive(self) -> bool:
                return False

        with mock.patch.object(runner.threading, "Thread", DeferredThread):
            workload.upsert("initial_batch_insert", "mixed", documents)
        first_slow_capture = deferred[0].target.__kwdefaults__["capture_batch"]
        first_slow_capture("late")
        self.assertEqual(workload.batch_correlations[0]["capture_reason"], "late")
        self.assertEqual(workload.batch_correlations[1]["profile_capture"], {"status": "not_triggered"})

    def test_unmapped_diagnostic_upsert_uses_public_counts(self) -> None:
        client = FakeClient(self.response(), count=0)
        workload = self.workload(self.response(), diagnostics_dir=Path("diagnostics"), client=client)
        document = {"id": "new", "content": "c", "vector": [1.0, 0.0], "user_id": "u", "fpath": "/a"}
        workload.upsert("explicit_update", "mixed", [document])
        correlation = workload.batch_correlations[0]
        self.assertEqual(correlation["accumulated_rows_source"], "public_count")
        self.assertEqual(correlation["before_public_count"], {"status": "captured", "rows": 0})
        self.assertEqual(correlation["after_public_count"], {"status": "captured", "rows": 1})
        self.assertEqual(correlation["accumulated_expected_rows"], 1)


    def test_failed_upsert_keeps_timeout_correlation_and_capture_failure_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = FakeClient(self.response(), upsert_error=TimeoutError("request timed out"))
            workload = self.workload(self.response(), diagnostics_dir=Path(directory), client=client)
            workload.diagnostic_slow_seconds = 60
            workload.controller.stats_snapshot = mock.Mock(side_effect=[
                {"status": "captured", "snapshot": {"phase": "before"}},
                {"status": "failed", "error": "stats unavailable"},
            ])
            workload.controller.capture_profiles = mock.Mock(return_value={
                "captures": {"cpu": {"status": "failed", "error": "unavailable"}},
            })
            document = {"id": "minima/mixed/000000", "content": "c", "vector": [1.0, 0.0],
                        "user_id": "u", "fpath": "/a"}
            with self.assertRaises(TimeoutError):
                workload.upsert("initial_batch_insert", "mixed", [document])
            correlation = workload.batch_correlations[0]
            self.assertEqual(correlation["outcome"], "timeout")
            self.assertIn("TimeoutError", correlation["error"])
            self.assertEqual(correlation["after_stats"]["status"], "failed")
            self.assertEqual(correlation["capture_reason"], "timeout")
            self.assertEqual(correlation["profile_capture"]["captures"]["cpu"]["status"], "failed")

    def test_capture_setup_failure_does_not_replace_upsert_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = FakeClient(self.response(), upsert_error=TimeoutError("request timed out"))
            workload = self.workload(self.response(), diagnostics_dir=Path(directory), client=client)
            workload.diagnostic_slow_seconds = 60
            workload.controller.capture_profiles = mock.Mock(side_effect=RuntimeError("cannot start capture worker"))
            document = {"id": "minima/mixed/000000", "content": "c", "vector": [1.0, 0.0],
                        "user_id": "u", "fpath": "/a"}
            with self.assertRaisesRegex(TimeoutError, "request timed out"):
                workload.upsert("initial_batch_insert", "mixed", [document])
            correlation = workload.batch_correlations[0]
            self.assertEqual(correlation["outcome"], "timeout")
            self.assertEqual(correlation["capture_reason"], "timeout")
            self.assertEqual(correlation["profile_capture"]["status"], "failed")
            self.assertIn("cannot start capture worker", correlation["profile_capture"]["error"])

    def test_diagnostic_reads_and_profile_captures_are_bounded_and_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = runner.ServiceController(
                Path("/bin/false"), "http://127.0.0.1:1", Path(directory) / "data", "test", 1, 1,
                diagnostics_url="http://127.0.0.1:2",
            )
            with mock.patch.object(runner.urllib.request, "urlopen", return_value=io.BytesIO(b"12345")):
                with self.assertRaisesRegex(RuntimeError, "exceeded 4 bytes"):
                    controller._read_bounded("http://127.0.0.1:2/debug", 1, 4)

            def capture_response(url: str, _timeout: float, _maximum: int) -> bytes:
                if "/mutex" in url:
                    raise OSError("mutex unavailable")
                return b"profile"

            with mock.patch.object(controller, "_read_bounded", side_effect=capture_response):
                evidence = controller.capture_profiles(Path(directory) / "captures", profile_seconds=1, capture_timeout=2)
            self.assertEqual(set(evidence["captures"]), set(runner.DIAGNOSTIC_PROFILE_ENDPOINTS))
            self.assertEqual(evidence["captures"]["mutex"]["status"], "failed")
            self.assertIn("mutex unavailable", evidence["captures"]["mutex"]["error"])
            self.assertEqual(evidence["captures"]["cpu"]["status"], "captured")
            self.assertEqual(evidence["manifest"]["status"], "captured")

    def test_exact_resume_accepts_only_first_wholly_missing_batch(self) -> None:
        selected_ids = {f"minima/resume/{ordinal:06d}" for ordinal in range(512, 768)}
        cases = [
            ("all_present", selected_ids, 768, "all-present", "rejected_all_present"),
            ("mixed", {next(iter(selected_ids))}, 513, "mixed", "rejected_mixed"),
            ("ambiguous_count", set(), 511, "visible rows=511", "rejected_ambiguous_count"),
        ]
        with mock.patch.object(common, "server_resource_usage", return_value={}):
            for name, present, count, message, state in cases:
                with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                    workload = self.resume_workload(Path(directory), present_ids=present, count=count)
                    with self.assertRaisesRegex(RuntimeError, message):
                        workload.run_diagnostic_resume("resume", 512)
                    self.assertEqual(workload.diagnostic_resume["state"], state)
                    self.assertEqual(workload.batch_correlations, [])

            with tempfile.TemporaryDirectory() as directory:
                workload = self.resume_workload(Path(directory), present_ids=set(), count=512)
                workload.run_diagnostic_resume("resume", 512)
                self.assertEqual(workload.diagnostic_resume["state"], "completed")
                self.assertTrue(workload.diagnostic_resume["nonqualifying"])
                self.assertEqual(workload.diagnostic_resume["present_ids_after"], 256)
                self.assertEqual(workload.diagnostic_resume["visible_rows_after"], 768)
                self.assertEqual(workload.batch_correlations[0]["accumulated_expected_rows"], 768)

    def test_exact_resume_rejects_matching_count_with_prefix_digest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(common, "server_resource_usage", return_value={}):
            workload = self.resume_workload(Path(directory), present_ids=set(), count=512)
            workload._initial_prefix_identity = lambda *_args: {
                "algorithm": "test", "expected_rows": 512, "actual_rows": 512,
                "expected_digest": "expected", "actual_digest": "unrelated-replacement", "match": False,
            }
            with self.assertRaisesRegex(RuntimeError, "does not match the exact initial prefix"):
                workload.run_diagnostic_resume("resume", 512)
            self.assertEqual(workload.diagnostic_resume["state"], "rejected_prefix_mismatch")
            self.assertFalse(workload.diagnostic_resume["prefix_identity"]["match"])
            self.assertEqual(workload.batch_correlations, [])

    def test_exact_resume_unexpected_exception_sets_failed_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(common, "server_resource_usage", return_value={}):
            workload = self.resume_workload(Path(directory), present_ids=set(), count=512)

            def fail_connect() -> None:
                raise OSError("unexpected connection failure")

            workload.connect = fail_connect
            with self.assertRaisesRegex(OSError, "unexpected connection failure"):
                workload.run_diagnostic_resume("resume", 512)
            self.assertEqual(workload.diagnostic_resume["state"], "failed")
            self.assertEqual(workload.diagnostic_resume["failure_phase"], "preflight")
            self.assertIn("OSError", workload.diagnostic_resume["error"])


    def test_diagnostic_controller_argv_and_stats_readiness_are_opt_in(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            controller = runner.ServiceController(
                Path("/service"), "http://127.0.0.1:17120", root / "data", "command_wal_durable", 1, 1,
                diagnostics_url="http://127.0.0.1:17121", block_profile_rate=7,
                mutex_profile_fraction=11,
            )
            process = mock.MagicMock(pid=42)
            process.poll.return_value = None
            health_client = mock.MagicMock()
            health_client.health.return_value = {"ok": True, "contract_version": runner.SERVICE_CONTRACT}
            with mock.patch.object(runner.subprocess, "Popen", return_value=process) as popen, \
                 mock.patch.object(runner, "TreeDBClient", return_value=health_client), \
                 mock.patch.object(controller, "_read_json", return_value={
                     "contract_version": runner.SERVICE_CONTRACT,
                 }) as read_stats:
                controller.start()
            argv = popen.call_args.args[0]
            self.assertEqual(argv[-6:], [
                "-pprof", "127.0.0.1:17121", "-block-profile-rate", "7",
                "-mutex-profile-fraction", "11",
            ])
            read_stats.assert_called_once_with(runner.DIAGNOSTICS_STATS_PATH)
            controller.process = None
            assert controller.log_file is not None
            self.assertEqual(
                controller._listen_address("http://[::1]:17121", "diagnostics"),
                "[::1]:17121",
            )
            controller.log_file.close()
            controller.log_file = None

            default = runner.ServiceController(
                Path("/service"), "http://127.0.0.1:17120", root / "default", "command_wal_durable", 1, 1,
            )
            process = mock.MagicMock(pid=43)
            process.poll.return_value = None
            with mock.patch.object(runner.subprocess, "Popen", return_value=process) as popen, \
                 mock.patch.object(runner, "TreeDBClient", return_value=health_client):
                default.start()
            self.assertNotIn("-pprof", popen.call_args.args[0])
            default.process = None
            assert default.log_file is not None
            default.log_file.close()
            default.log_file = None

    def test_shutdown_timeout_is_separate_and_retains_last_live_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            controller = runner.ServiceController(
                Path("/service"), "http://127.0.0.1:17120", data_dir,
                "command_wal_durable", 3600, 120,
            )
            process = mock.MagicMock(pid=55)
            process.poll.return_value = None
            process.wait.side_effect = [
                runner.subprocess.TimeoutExpired("service", 0.05),
                0,
            ]
            controller.process = process

            def usage(rss: int, cpu: float) -> dict[str, object]:
                return {
                    "captured": True,
                    "rss_bytes": rss,
                    "cpu_seconds": cpu,
                    "availability": {
                        "rss_bytes": "test", "cpu_seconds": "test",
                        "bytes_per_op": "unavailable", "allocs_per_op": "unavailable",
                        "measurement_error": "",
                    },
                }

            with mock.patch.object(
                common, "server_process_resource_usage",
                side_effect=[usage(100, 1.0), usage(110, 2.0), usage(120, 3.0)],
            ), mock.patch.object(common, "disk_bytes", return_value=140):
                controller.stop()

        self.assertEqual(controller.startup_timeout, 3600)
        self.assertEqual(controller.shutdown_timeout, 120)
        self.assertEqual(controller.last_shutdown_resource_end["rss_bytes"], 120)
        self.assertEqual(controller.last_shutdown_resource_end["cpu_seconds"], 3.0)
        self.assertEqual(controller.last_shutdown_resource_end["disk_bytes"], 140)
        process.terminate.assert_called_once_with()
        process.kill.assert_not_called()
        self.assertTrue(all(call.kwargs["timeout"] <= 0.05 for call in process.wait.call_args_list))

    def test_default_cli_preserves_frozen_timeout_and_disables_diagnostics(self) -> None:
        argv = [
            "minima_treedb_runner.py", "--manifest", "manifest.json", "--output", "output.json",
            "--service-bin", "service", "--data-dir", "data", "--collection", "owned",
        ]
        with mock.patch.object(sys, "argv", argv):
            args = runner.parse_args()
        self.assertEqual(args.operation_timeout, 120)
        self.assertEqual(args.startup_timeout, 120)
        self.assertIsNone(args.diagnostics_dir)
        self.assertIsNone(args.diagnostic_resume_scenario)
        self.assertIsNone(args.diagnostic_resume_start)

    def test_script_guards_empty_diagnostic_array_for_bash_nounset(self) -> None:
        script = (Path(__file__).parents[2] / "scripts/bench_minima_qualification.sh").read_text(encoding="utf-8")
        guarded = '${treedb_diagnostic_args[@]+"${treedb_diagnostic_args[@]}"}'
        self.assertEqual(script.count(guarded), 2)
        self.assertNotIn('"${treedb_diagnostic_args[@]}" ||', script)

    def test_service_log_evidence_is_bounded_and_keeps_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = runner.ServiceController(
                Path("/bin/false"), "http://127.0.0.1:1", Path(directory) / "data", "test", 1, 1,
            )
            controller.log_path.write_bytes(b"x" * (runner.SERVICE_LOG_TAIL_BYTES + 10) + b"root cause\n")
            evidence = controller.log_evidence()
            self.assertEqual(evidence["path"], str(controller.log_path))
            self.assertLessEqual(len(evidence["tail"].encode()), runner.SERVICE_LOG_TAIL_BYTES)
            self.assertTrue(evidence["tail"].endswith("root cause\n"))

    def test_start_timeout_cleans_live_child_and_can_retry_same_port(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "never-ready-service"
            write_health_service(binary)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            data_dir = root / "data"
            controller = runner.ServiceController(
                binary, f"http://127.0.0.1:{port}", data_dir, "test", 2, 1,
            )
            original_stop = controller.stop

            def noisy_stop() -> None:
                original_stop()
                raise RuntimeError("cleanup noise")

            with mock.patch.object(controller, "stop", side_effect=noisy_stop):
                with self.assertRaisesRegex(TimeoutError, "readiness exceeded"):
                    controller.start()
            child_pid = int((data_dir / "pid").read_text(encoding="utf-8"))
            self.assertIsNone(controller.process)
            self.assertIsNone(controller.log_file)
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)

            (data_dir / "compatible").touch()
            try:
                controller.start()
                self.assertIsNotNone(controller.pid)
                self.assertIsNotNone(controller.log_file)
            finally:
                controller.stop()
            self.assertIsNone(controller.process)
            self.assertIsNone(controller.log_file)

    def test_main_unwritable_output_cleans_child_and_preserves_write_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "service"
            write_health_service(binary)
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            data_dir = root / "data"
            data_dir.mkdir()
            (data_dir / "compatible").touch()
            output = root / "output-directory"
            output.mkdir()
            args = SimpleNamespace(
                manifest=root / "manifest.json", output=output, service_bin=binary,
                url=f"http://127.0.0.1:{port}", data_dir=data_dir, profile="test",
                startup_timeout=2, collection="owned", operation_timeout=1, ef_search=1, small=False,
                diagnostics_dir=None, diagnostics_url="http://127.0.0.1:17121",
                diagnostic_slow_seconds=30, diagnostic_profile_seconds=5,
                diagnostic_capture_timeout=10, diagnostic_resume_scenario=None,
                diagnostic_resume_start=None,
            )
            controllers: list[runner.ServiceController] = []

            class FakeRunner:
                def __init__(self, _manifest: object, *, controller: runner.ServiceController, **_kwargs: object) -> None:
                    self.controller = controller
                    self.evidence = SimpleNamespace(failures=[])
                    controller.start()
                    controllers.append(controller)

                def run(self) -> None:
                    pass

                def artifact(self) -> dict[str, object]:
                    return {}

                def close(self) -> None:
                    raise RuntimeError("cleanup noise")

            with mock.patch.object(runner, "parse_args", return_value=args), \
                 mock.patch.object(common, "load_manifest", return_value={}), \
                 mock.patch.object(runner, "TreeDBMinimaRunner", FakeRunner):
                with self.assertRaises(IsADirectoryError):
                    runner.main()
            controller = controllers[0]
            self.assertEqual(controller.startup_timeout, 2)
            self.assertEqual(controller.shutdown_timeout, 1)
            child_pid = int((data_dir / "pid").read_text(encoding="utf-8"))
            self.assertIsNone(controller.process)
            self.assertIsNone(controller.log_file)
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
            try:
                controller.start()
                self.assertIsNotNone(controller.pid)
            finally:
                controller.stop()

    def test_resource_provenance_names_treedb_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(
                common.subprocess, "run", return_value=SimpleNamespace(stdout="2048 00:02.5")
            ):
                resource = common.server_resource_usage(321, Path(directory), "TreeDB")
        self.assertEqual(resource["availability"]["rss_bytes"], "TreeDB server PID 321")
        self.assertEqual(resource["availability"]["cpu_seconds"], "TreeDB server PID 321")

    def test_artifact_uses_shared_segment_delta_resource_semantics(self) -> None:
        baseline = {
            "captured": True, "rss_bytes": 100, "cpu_seconds": 1.0, "disk_bytes": 1000,
            "availability": {"rss_bytes": "test", "cpu_seconds": "test", "disk_bytes": "test"},
        }
        end = {
            "captured": True, "rss_bytes": 125, "cpu_seconds": 2.5, "disk_bytes": 1100,
            "availability": {"rss_bytes": "test", "cpu_seconds": "test", "disk_bytes": "test"},
        }
        segment = common.resource_delta(baseline, end)
        resource = {
            "captured": True, "rss_bytes": 25, "cpu_seconds": 1.5, "disk_bytes": 100,
            "semantics": common.RESOURCE_SEMANTICS, "segments": [segment],
            "baseline": baseline, "end": end,
        }
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.resource_evidence = lambda: resource
        workload.controller = SimpleNamespace(
            profile="test", binary=Path("/bin/false"), log_path=Path("/tmp/service.log"),
            diagnostics_url=None, block_profile_rate=1, mutex_profile_fraction=1,
            startup_timeout=3600, shutdown_timeout=120, pid=123,
            log_evidence=lambda: {"path": "/tmp/service.log", "tail": "test", "max_tail_bytes": 64 << 10},
        )
        workload.source_commit = "a" * 40
        workload.runner_sha256 = "b" * 64
        workload.service_binary_sha256 = "c" * 64
        workload.operation_timeout_seconds = 120
        workload._phase_total_start = workload._phase_start = runner.time.monotonic_ns()
        workload._phase_name = "initial_durable_load"
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload._phase_resource_start = baseline
        workload._phase_restart_old_end = None
        workload.storage_path = Path("/tmp/data")
        workload.resource_server_name = "TreeDB"
        workload.process_identity = lambda pid: f"process-{pid}"
        workload.evidence = SimpleNamespace(samples=[])
        workload.url = "http://127.0.0.1:1"
        workload.collection = "owned"
        workload.config = {"dimension": 8, "metric": "cosine"}
        workload.ef_search = 64
        workload.effective_collection = {
            "dimension": 8,
            "metric": "cosine",
            "scalar_fields": [
                {"field": "meta.user_id", "value_type": "string"},
                {"field": "meta.fpath", "value_type": "string"},
            ],
            "vector_strategy": "native_runtime",
        }
        workload.batch_correlations = []
        workload.diagnostic_resume = None
        workload.diagnostics_dir = None
        workload.diagnostic_slow_seconds = 30
        workload.diagnostic_profile_seconds = 5
        workload.diagnostic_capture_timeout = 10
        workload.route_evidence = {"small": SimpleNamespace(
            native_base_plus_live_delta=True,
            full_document_scan_fallbacks=0,
            scalar_filter_unbounded=0,
            scalar_filter_probe_ids=0,
            scalar_filter_candidates=41,
            scalar_filter_candidate_ids=5,
            scalar_filter_retained_candidate_ids=5,
            scalar_filter_refined_candidate_ids=5,
            scalar_filter_membership_source="finite_scalar",
            scalar_filter_plan="complete_finite_ann",
            allowed_id_materialization_rows=0,
            primary_document_scans=0,
            scalar_filter_visited=41,
            scalar_filter_scored=41,
            scalar_filter_admitted=5,
            visibility_mismatch_count=0,
            visibility_retry_count=0,
        )}
        base_artifact = {
            "backends": [{"configuration": {"initial_upload_hnsw": "qdrant-only"}}],
            "scenarios": [{"scenario": "small"}],
            "backend_raw_evidence": {"qdrant": {
                "collection_configuration_transition": {"attempted": False},
                "readiness": {"sessions": []},
            }},
        }
        with mock.patch.object(common.QdrantMinimaRunner, "artifact", return_value=base_artifact), \
             mock.patch.object(common, "server_resource_usage", return_value=baseline):
            artifact = workload.artifact()
        configuration = artifact["backends"][0]["configuration"]
        self.assertEqual(configuration["scalar_fields"], "meta.user_id,meta.fpath")
        self.assertEqual(json.loads(configuration["effective_collection"]), workload.effective_collection)
        raw = artifact["backend_raw_evidence"]["treedb"]
        self.assertEqual(raw["resource_measurement"], resource)
        self.assertEqual(raw["resource_availability"]["measurement"], common.RESOURCE_SEMANTICS)
        self.assertNotIn("initial_upload_hnsw", configuration)
        self.assertNotIn("collection_configuration_transition", raw)
        self.assertNotIn("readiness", raw)
        self.assertEqual(configuration["operation_timeout_seconds"], "120")
        self.assertEqual(configuration["startup_reopen_timeout_seconds"], "3600")
        self.assertEqual(configuration["shutdown_timeout_seconds"], "120")
        self.assertEqual(configuration["product_commit"], "a" * 40)
        self.assertEqual(raw["phase_attribution"]["phases"][0]["classification"], "production_path")
        self.assertEqual(artifact["scenarios"][0]["route"]["candidate_ids"], 5)
        self.assertEqual(artifact["scenarios"][0]["route"]["visited_candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidate_ids"], 5)


if __name__ == "__main__":
    unittest.main()
