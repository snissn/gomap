from __future__ import annotations

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
    def __init__(self, response: object) -> None:
        self.response = response
        self.call: tuple[object, ...] | None = None

    def query_by_embedding(self, *args: object, **kwargs: object) -> object:
        self.call = (*args, kwargs)
        return self.response


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
    def workload(self, response: object) -> runner.TreeDBMinimaRunner:
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.client = FakeClient(response)
        workload.collection = "owned"
        workload.config = {"top_k": 5}
        workload.ef_search = 64
        workload.specs = {"mixed": {"name": "mixed", "filter": "user_id+fpath", "user_id": "u", "fpath": "/a"}}
        workload.queries = {"mixed": {"vector": [1.0, 0.0]}}
        workload.evidence = common.Evidence({"corpora": [{"name": "mixed"}]})
        workload.route_evidence = {}
        return workload

    def response(self, **changes: object) -> object:
        values = {
            "route": "ann", "native_base_plus_live_delta": True, "exact_fallbacks": 0,
            "full_document_scan_fallbacks": 0, "documents": [SimpleNamespace(
                id="d", content="c", score=1.0, meta={"user_id": "u", "fpath": "/a"})],
        }
        values.update(changes)
        return SimpleNamespace(**values)

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


    def test_service_log_evidence_is_bounded_and_keeps_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = runner.ServiceController(
                Path("/bin/false"), "http://127.0.0.1:1", Path(directory) / "data", "test", 1,
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
                binary, f"http://127.0.0.1:{port}", data_dir, "test", 2,
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
            log_evidence=lambda: {"path": "/tmp/service.log", "tail": "test", "max_tail_bytes": 64 << 10},
        )
        workload.url = "http://127.0.0.1:1"
        workload.collection = "owned"
        workload.config = {"dimension": 8, "metric": "cosine"}
        workload.ef_search = 64
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
        base_artifact = {"backends": [{}], "scenarios": [{"scenario": "small"}], "backend_raw_evidence": {"qdrant": {}}}
        with mock.patch.object(common.QdrantMinimaRunner, "artifact", return_value=base_artifact):
            artifact = workload.artifact()
        raw = artifact["backend_raw_evidence"]["treedb"]
        self.assertEqual(raw["resource_measurement"], resource)
        self.assertEqual(raw["resource_availability"]["measurement"], common.RESOURCE_SEMANTICS)
        self.assertEqual(artifact["scenarios"][0]["route"]["candidate_ids"], 5)
        self.assertEqual(artifact["scenarios"][0]["route"]["visited_candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidate_ids"], 5)


if __name__ == "__main__":
    unittest.main()
