from __future__ import annotations

import json
import copy
from dataclasses import asdict, replace
import io
import os
import socket
import subprocess
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
import minima_treedb_probe as probe
import minima_treedb_probe_analyze as probe_analyze
from treedb_client import DenseSearchWork, TreeDBProtocolError, TreeDBServiceError
from treedb_client._native import _dense_work


def public_dense_work() -> DenseSearchWork:
    golden = Path(__file__).parents[2] / "TreeDB/nativewire/testdata/dense_work_v1.hex"
    return _dense_work(bytes.fromhex(golden.read_text()))


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
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("-addr", required=True)
parser.add_argument("-dir", required=True)
parser.add_argument("-profile")
parser.add_argument("-pprof")
parser.add_argument("-block-profile-rate")
parser.add_argument("-mutex-profile-fraction")
args = parser.parse_args()
data_dir = Path(args.dir)
data_dir.mkdir(parents=True, exist_ok=True)
(data_dir / "pid").write_text(str(os.getpid()), encoding="utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        contract = "{runner.SERVICE_CONTRACT}" if (data_dir / "compatible").exists() else "wrong"
        body = json.dumps({{"ok": True, "contract_version": contract, "work": work}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

class Server(HTTPServer):
    allow_reuse_address = True

work = {{"pid": os.getpid(), "schema_version": "treedb-work-v1", "scope": "process",
        "origin_kind": "go_package_init", "origin_unix_nano": 7, "fixture_cleanup": False}}
def stop(*_args):
    # The controller must retain work emitted after the last live stats read.
    work["fixture_cleanup"] = True
    print(json.dumps({{"event": "treedb_document_service_terminal_work", "version": 1,
        "contract_version": "{runner.SERVICE_CONTRACT}", "cleanup_completed": True,
        "shutdown_failures": 0, "work": work}}), flush=True)
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
if args.pprof:
    host, port = args.pprof.rsplit(":", 1)
    diagnostics = Server((host, int(port)), Handler)
    threading.Thread(target=diagnostics.serve_forever, daemon=True).start()
host, port = args.addr.rsplit(":", 1)
Server((host, int(port)), Handler).serve_forever()
""", encoding="utf-8")
    binary.chmod(0o755)




class ProbeContractTest(unittest.TestCase):
    def test_batch_stats_io_failure_preserves_cleanup_and_failure_result(self):
        from contextlib import ExitStack
        for operation in ('close', 'hash'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                root = Path(tmp)
                args = SimpleNamespace(binary=root / 'binary', binding=root / 'binding.json',
                    manifest=root / 'manifest.json', serving=root / 'serving.json',
                    output=root / 'run', build_profile=False)
                args.binding.write_text(json.dumps({'manifest_sha256': 'hash', 'serving': str(args.serving),
                    'serving_sha256': 'hash', 'build_profile': False, 'source_commit': 'commit',
                    'binary': str(args.binary), 'binary_sha256': 'hash'}))
                args.serving.write_text('{}')
                batch_file = mock.Mock()
                if operation == 'close':
                    batch_file.close.side_effect = OSError('injected close failure')
                original_open = Path.open
                def open_file(path, *positional, **keywords):
                    return batch_file if path.name == 'batch-stats.jsonl' else original_open(path, *positional, **keywords)
                def digest(path):
                    if Path(path).name == 'batch-stats.jsonl' and operation == 'hash':
                        raise OSError('injected hash failure')
                    return 'hash'
                controller = mock.Mock(pid=42, lifetimes=[{'exit_code': 0}])
                fake_runner = mock.Mock(evidence=SimpleNamespace(requests=[], samples=[]))
                fake_runner.create_owned_collection.side_effect = RuntimeError('injected setup failure')
                for patch in (
                    mock.patch.object(probe.argparse.ArgumentParser, 'parse_args', return_value=args),
                    mock.patch.object(Path, 'open', open_file),
                    mock.patch.object(probe.tr.common, 'load_manifest', return_value={}),
                    mock.patch.object(probe, 'initial_batches', return_value=[('test', i * 256, 256) for i in range(64)]),
                    mock.patch.object(probe.tr, 'file_sha256', side_effect=digest),
                    mock.patch.object(probe.tr, 'repository_commit', return_value='commit'),
                    mock.patch.object(probe.tr, 'service_binary_build_provenance'),
                    mock.patch.object(probe.subprocess, 'check_output', return_value=b''),
                    mock.patch.object(probe.socket, 'socket'),
                    mock.patch.object(probe.os, 'sched_getaffinity', return_value=set(range(6)), create=True),
                    mock.patch.object(probe, 'python_inputs', return_value={}),
                    mock.patch.object(probe.tr.common, 'linux_process_identity', return_value={}),
                    mock.patch.object(probe.tr, 'ServiceController', return_value=controller),
                    mock.patch.object(probe.tr, 'TreeDBMinimaRunner', return_value=fake_runner),
                    mock.patch.object(probe.signal, 'signal'),
                    mock.patch.object(probe.traceback, 'print_exc'),
                ):
                    stack.enter_context(patch)
                self.assertTrue(probe.main())
                fake_runner.close.assert_called_once_with()
                controller.stop.assert_called_once_with()
                result = json.loads((args.output / 'result.json').read_text())
                self.assertIn(f'batch stats finalization: OSError: injected {operation} failure', result['failures'])
                self.assertEqual(result['lifetimes'], controller.lifetimes)
                self.assertEqual(result['requests'], [])
                self.assertNotIn('batch_stats_sha256', result)

    def test_probe_and_analyzer_reject_optimized_python(self):
        for module in (probe, probe_analyze):
            for flags, setting in [(['-O'], ''), ([], '1'), ([], '2')]:
                with self.subTest(module=module.__name__, flags=flags, setting=setting):
                    completed = subprocess.run([sys.executable, *flags, module.__file__, '--help'],
                        env={**os.environ, 'PYTHONOPTIMIZE': setting,
                             'PYTHONPATH': os.pathsep.join(sys.path)}, capture_output=True, text=True)
                    self.assertNotEqual(completed.returncode, 0)
                    self.assertIn('require Python without -O or PYTHONOPTIMIZE', completed.stderr)

    def test_affinity_and_imported_client_identity(self):
        probe.validate_affinity(list(range(8, 14)), list(range(8, 14)))
        for client, server in [([0] * 6, [0] * 6), (list(range(5)), list(range(5))),
                               (list(range(6)), list(range(8, 14)))]:
            with self.assertRaises(AssertionError):
                probe.validate_affinity(client, server)
        client = sys.modules['treedb_client']
        with mock.patch.object(client, '__file__', '/installed/treedb_client/__init__.py'):
            with self.assertRaises(AssertionError):
                probe.python_inputs()

    def test_population_and_partial_batches(self):
        for size in (250_000, 500_000, 1_000_000):
            manifest = {'fixture': f'bounded-{size // 1000}k', 'corpora': [{'corpus_rows': size}],
                        'config': {'batch_size': 256}, 'operations': [{'name': 'initial_batch_insert',
                        'insert_ranges': [{'scenario': 'first', 'start': 0, 'rows': 128},
                                          {'scenario': 'rest', 'start': 128, 'rows': size - 1456}]}]}
            batches = probe.initial_batches(manifest)
            self.assertEqual(batches[:2], [('first', 0, 128), ('rest', 128, 256)])
            self.assertEqual(sum(x[2] for x in batches), size - 1328)
            self.assertEqual(batches[-1][1] + batches[-1][2], size - 1328)
            for key, value in [('fixture', 'full'), ('corpora', [{'corpus_rows': size + 1}]),
                               ('config', {'batch_size': 512}), ('operations', [{'name': 'initial_batch_insert', 'insert_ranges': []}])]:
                with self.subTest(size=size, key=key), self.assertRaises(AssertionError):
                    probe.initial_batches({**manifest, key: value})

    def test_build_requires_complete_profile_identity_and_work(self):
        def snapshot(renews, allocated):
            return {'work': {'pid': 42, 'origin_unix_nano': 7, 'indexed_json': {'rows': 0},
                    'runtime': {'queries_completed': 0}, 'memory': {'total_alloc': allocated, 'mallocs': allocated},
                    'fold': {'publications': 0, 'candidate_bytes_charged': 0, 'appender_attempts_charged': 0,
                             **{key: {'attempts': n, 'completed': n, 'errors': 0}
                                for key, n in [('build', 0), ('public', 0), ('renew', renews)]}}}}
        capture = {'requests': [{'operation_name': 'column_graph_initial_build', 'outcome': 'success',
                   'started_monotonic_ns': 3, 'ended_monotonic_ns': 4}], 'profile_seconds': 60,
                   'cpu': {'started_monotonic_ns': 1, 'ended_monotonic_ns': 5},
                   'active_profile_probe': {'status': 500, 'body': 'profiling already in use', 'observed_monotonic_ns': 2},
                   'before_work': snapshot(0, 10), 'after_work': snapshot(1, 20), 'after_profile_work': snapshot(1, 25),
                   'public_count': 248672, 'response': {'status': {'strategy': 'column_graph',
                   'column_graph_build': {'total_nanos': 10}}, 'timing': {'total_nanos': 11}},
                   'allocation_delta': {'total_alloc': 10, 'mallocs': 10}}
        probe.validate_build_capture(capture, 42, 7, 248672)
        cases = [(('cpu', 'ended_monotonic_ns'), 4), (('active_profile_probe', 'status'), 200),
                 (('active_profile_probe', 'body'), 'not active'), (('profile_seconds',), 8),
                 (('after_work', 'work', 'pid'), 43), (('after_work', 'work', 'origin_unix_nano'), 8),
                 (('after_work', 'work', 'indexed_json', 'rows'), 1),
                 (('after_work', 'work', 'fold', 'renew', 'errors'), 1),
                 (('public_count',), 248671), (('allocation_delta', 'total_alloc'), 11),
                 (('response', 'status', 'column_graph_build', 'total_nanos'), 0)]
        for path, value in cases:
            bad = copy.deepcopy(capture)
            target = bad
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(AssertionError):
                probe.validate_build_capture(bad, 42, 7, 248672)



    def test_analyzer_rejects_recorded_input_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            packet = Path(directory)
            binary, serving, manifest = (packet / name for name in ('service', 'serving.json', 'manifest.json'))
            for path in (binary, serving, manifest, packet / 'launch.py', packet / 'run.sh'):
                path.write_text(path.name)
            binding = {'source_commit': 'reviewed', 'binary': str(binary), 'serving': str(serving),
                       'binary_sha256': probe_analyze.h(binary), 'serving_sha256': probe_analyze.h(serving),
                       'manifest_sha256': probe_analyze.h(manifest), 'build_profile': False}
            binding_path = packet / 'source-binding.json'
            binding_path.write_text(json.dumps(binding))
            probe_path, analyzer_path = Path(probe.__file__).resolve(), Path(probe_analyze.__file__).resolve()
            inputs = [binary, serving, manifest, binding_path, probe_path, analyzer_path, packet / 'launch.py', packet / 'run.sh']
            inputs += list((probe_path.parents[2] / 'clients/python/treedb_client/src/treedb_client').rglob('*.py'))
            loaded = probe.python_inputs()
            authorization = {'authorized': True, 'sha256': {**loaded, **{str(path): probe_analyze.h(path) for path in inputs}}}
            authorization_path = packet / 'launch-authorization.json'
            authorization_path.write_text(json.dumps(authorization))
            result = {key: binding[key] for key in ('source_commit', 'binary_sha256', 'serving_sha256', 'manifest_sha256', 'build_profile')}
            result.update(binding_sha256=probe_analyze.h(binding_path), script_sha256=probe_analyze.h(probe_path),
                          python_inputs_sha256=loaded, python_executable=str(Path(sys.executable).resolve()),
                          affinity=list(range(8,14)), server_affinity=list(range(8,14)))
            probe_analyze.validate_run_inputs(result, packet, manifest)
            for key in binding:
                if key not in result:
                    continue
                bad = {**result, key: True if key == 'build_profile' else 'different-run'}
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    probe_analyze.validate_run_inputs(bad, packet, manifest)
            authorization['authorized'] = False
            authorization_path.write_text(json.dumps(authorization))
            with self.assertRaises(AssertionError):
                probe_analyze.validate_run_inputs(result, packet, manifest)
            authorization['authorized'] = True
            for path in (str(manifest), str(Path(runner.__file__).resolve()), str(Path(sys.executable).resolve())):
                bad = copy.deepcopy(authorization)
                del bad['sha256'][path]
                authorization_path.write_text(json.dumps(bad))
                with self.subTest(missing=path), self.assertRaises(KeyError):
                    probe_analyze.validate_run_inputs(result, packet, manifest)
            authorization_path.write_text(json.dumps(authorization))
            bad = copy.deepcopy(result)
            bad['python_inputs_sha256'][str(Path(runner.__file__).resolve())] = 'substituted'
            with self.assertRaises(AssertionError):
                probe_analyze.validate_run_inputs(bad, packet, manifest)


class MinimaTreeDBRunnerTest(unittest.TestCase):
    def workload(self, response: object, *, diagnostics_dir: Path | None = None,
                 client: FakeClient | None = None) -> runner.TreeDBMinimaRunner:
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.client = client or FakeClient(response)
        workload.strategy, workload.transport = "native_runtime", "http"
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
        workload._batch_correlation_max_records = 10_000
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

    def test_column_graph_requires_explicit_limits_before_loading_or_launching(self) -> None:
        with mock.patch.object(runner, "parse_args", return_value=SimpleNamespace(strategy="column_graph", column_graph_serving=None)), \
             mock.patch.object(common, "load_manifest") as load, \
             mock.patch.object(runner, "ServiceController") as controller:
            with self.assertRaisesRegex(SystemExit, "column_graph requires --column-graph-serving"):
                runner.main()
        load.assert_not_called()
        controller.assert_not_called()

    def test_repository_commit_binds_head_after_clean_status(self) -> None:
        commit = "a" * 40
        with mock.patch.object(
            runner.subprocess, "run",
            side_effect=[SimpleNamespace(stdout=""), SimpleNamespace(stdout=commit + "\n")],
        ) as run:
            self.assertEqual(runner.repository_commit(), commit)
        self.assertEqual(run.call_count, 2)

    def test_service_binary_build_provenance_accepts_clean_matching_binary(self) -> None:
        commit = "a" * 40
        output = (
            "service: go1.25.5\n"
            "\tpath\tgithub.com/snissn/gomap/cmd/treedb-document-service\n"
            f"\tbuild\tvcs.revision={commit}\n"
            "\tbuild\tvcs.modified=false\n"
        )
        with mock.patch.object(
            runner.subprocess, "run",
            return_value=SimpleNamespace(returncode=0, stdout=output, stderr=""),
        ) as run:
            self.assertEqual(
                runner.service_binary_build_provenance(Path("service"), commit),
                (commit, "false"),
            )
        run.assert_called_once_with(
            ["go", "version", "-m", "service"],
            check=False, capture_output=True, text=True,
        )

    def test_service_binary_build_provenance_rejects_stale_dirty_and_missing_metadata(self) -> None:
        commit = "a" * 40
        cases = (
            ("stale revision", "b" * 40, "false", "does not match"),
            ("dirty build", commit, "true", "vcs.modified=false"),
            ("missing revision", None, "false", "missing an exact vcs.revision"),
            ("missing modified status", commit, None, "vcs.modified=false"),
        )
        for name, revision, modified, error in cases:
            settings = []
            if revision is not None:
                settings.append(f"\tbuild\tvcs.revision={revision}")
            if modified is not None:
                settings.append(f"\tbuild\tvcs.modified={modified}")
            output = "service: go1.25.5\n" + "\n".join(settings) + "\n"
            with self.subTest(name=name), mock.patch.object(
                runner.subprocess, "run",
                return_value=SimpleNamespace(returncode=0, stdout=output, stderr=""),
            ):
                with self.assertRaisesRegex(RuntimeError, error):
                    runner.service_binary_build_provenance(Path("service"), commit)

    def test_phase_boundaries_exclude_slow_disk_sampling_and_align_cpu_with_wall(self) -> None:
        process_samples = iter((
            {"captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "availability": {}},
            {"captured": True, "rss_bytes": 20, "cpu_seconds": 2.0, "availability": {}},
            {"captured": True, "rss_bytes": 30, "cpu_seconds": 3.0, "availability": {}},
        ))
        disk_samples = iter((100, 200))
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload._phase_total_start = None
        workload._phase_start = None
        workload._phase_name = None
        workload._phase_resource_start = None
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload.controller = SimpleNamespace(pid=123)
        workload._batch_prepared = True
        workload.storage_path = Path(".")
        workload.process_identity = lambda pid: f"process-{pid}"
        workload.resource_server_name = "TreeDB"
        events: list[str] = []
        ticks = iter((500, 700, 710))

        def monotonic_ns() -> int:
            events.append("wall")
            return next(ticks)

        def process_usage(*_args: object) -> dict[str, object]:
            events.append("process")
            return next(process_samples)

        def disk_usage(*_args: object) -> int:
            events.append("disk")
            return next(disk_samples)

        with mock.patch.object(runner.time, "monotonic_ns", side_effect=monotonic_ns), \
             mock.patch.object(common, "server_process_resource_usage", side_effect=process_usage), \
             mock.patch.object(common, "disk_bytes", side_effect=disk_usage):
            workload.begin_phase_attribution()
            workload.phase_transition("warmup_search")

        initial = workload._phase_boundaries[0]
        self.assertEqual(events, [
            "disk", "process", "wall",
            "wall", "process", "disk", "process", "wall",
        ])
        self.assertEqual(workload._phase_total_start, 500)
        self.assertEqual(initial["start_nanos"], workload._phase_total_start)
        self.assertEqual(initial["start_nanos"] - 100, 400)
        self.assertEqual(initial["duration_nanos"], 200)
        self.assertEqual(initial["resource_segments"][0]["start"]["cpu_seconds"], 1.0)
        self.assertEqual(initial["resource_segments"][0]["end"]["cpu_seconds"], 2.0)
        self.assertEqual(workload._phase_resource_start["cpu_seconds"], 3.0)
        self.assertEqual(workload._phase_resource_start["disk_bytes"], 200)

    def test_restart_phase_splits_old_and_new_process_resources(self) -> None:
        old_start = {
            "captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "disk_bytes": 100,
            "pid": 100, "process_identity": "old-process",
        }
        old_end = {
            "captured": True, "rss_bytes": 12, "cpu_seconds": 2.0, "disk_bytes": 120,
            "pid": 100, "process_identity": "old-process",
        }
        new_end = {
            "captured": True, "rss_bytes": 20, "cpu_seconds": 3.0,
            "availability": {},
        }
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
             mock.patch.object(common, "server_process_resource_usage", return_value=new_end), \
             mock.patch.object(common, "disk_bytes", return_value=140):
            workload.phase_transition("post_reopen")
        segments = workload._phase_boundaries[0]["resource_segments"]
        self.assertEqual([segment["start"]["pid"] for segment in segments], [100, 101])
        self.assertEqual([segment["end"]["pid"] for segment in segments], [100, 101])
        self.assertEqual(segments[1]["start"]["cpu_seconds"], 0.0)
        self.assertEqual(segments[1]["start"]["rss_bytes"], 0)
        self.assertEqual(segments[1]["start"]["disk_bytes"], old_end["disk_bytes"])
        self.assertEqual(segments[1]["end"]["cpu_seconds"], 3.0)

    def test_restart_controller_uses_shutdown_endpoint_for_phase_and_aggregate(self) -> None:
        baseline = {"captured": True, "rss_bytes": 10, "cpu_seconds": 1.0, "disk_bytes": 100,
                    "pid": 100, "linux_process_identity": "100:10"}
        shutdown_end = {"captured": True, "rss_bytes": 15, "cpu_seconds": 3.0, "disk_bytes": 140,
                        "pid": 100, "linux_process_identity": "100:10"}

        class Controller:
            pid = 100
            last_shutdown_resource_end = shutdown_end

            def start(self) -> None:
                self.pid = 101

        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.controller = Controller()
        workload.restart_origin = (100, "old-process")
        workload.restart_origin_linux_identity = "100:10"
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
        # Match the constructor's unprepared state: diagnostic resume does not
        # enter normal phase attribution before selecting its frozen batch.
        workload._batch_prepared = False
        workload._batch_correlation_expected_identities = set()
        workload._batch_correlation_max_records = 0
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
    def test_diagnostic_upsert_compacts_completed_batch_stats(self) -> None:
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
        self.assertNotIn("before_stats", correlation)
        self.assertNotIn("after_stats", correlation)
        self.assertEqual(correlation["stats_retention"], "compact_completed")
        self.assertEqual(correlation["outcome"], "completed")
        self.assertEqual(correlation["profile_capture"], {"status": "not_triggered"})

    def test_high_cardinality_completed_correlations_are_width_independent(self) -> None:
        wide_stats = {
            "status": "captured",
            "snapshot": {f"metric_{index}": index for index in range(2_000)},
        }
        correlations = []
        for sequence in range(10_000):
            correlation = {
                "sequence": sequence,
                "operation": "initial_batch_insert",
                "scenario": "large",
                "batch_ordinal": sequence,
                "batch_start": sequence * 256,
                "rows": 256,
                "accumulated_expected_rows_before": sequence * 256,
                "accumulated_expected_rows": (sequence + 1) * 256,
                "accumulated_rows_source": "frozen_manifest",
                "before_stats": wide_stats,
                "after_stats": wide_stats,
                "outcome": "completed",
                "profile_capture": {"status": "not_triggered"},
                "started_monotonic_ns": sequence * 10,
                "ended_monotonic_ns": sequence * 10 + 5,
                "duration_nanos": 5,
            }
            runner.TreeDBMinimaRunner._compact_completed_batch_correlation(correlation)
            correlations.append(correlation)
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.batch_correlations = correlations
        workload._batch_correlation_max_records = 10_000
        contract = workload._batch_correlation_contract()
        artifact = {"backend_raw_evidence": {"treedb": {
            "upsert_batch_correlations": correlations,
            "upsert_batch_correlation_contract": contract,
        }}}
        encoded = json.dumps(artifact, separators=(",", ":")).encode()
        self.assertTrue(all(
            "before_stats" not in correlation and "after_stats" not in correlation
            for correlation in correlations
        ))
        self.assertEqual(contract["record_count"], 10_000)
        self.assertEqual(contract["compact_completed_records"], 10_000)
        self.assertEqual(contract["full_diagnostic_records"], 0)
        self.assertLessEqual(
            max(len(json.dumps(correlation, separators=(",", ":")).encode()) for correlation in correlations),
            runner.COMPACT_BATCH_CORRELATION_MAX_BYTES,
        )
        self.assertLess(
            len(encoded),
            runner.COMPACT_BATCH_CORRELATION_MAX_BYTES * len(correlations),
        )


    def test_completed_diagnostics_require_manifest_batch_cardinality(self) -> None:
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.batch_correlations = []
        workload._batch_correlation_max_records = 10
        workload.operations = {"manifest_ordered": True}
        workload.diagnostics_dir = Path("diagnostics")
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            workload._batch_correlation_contract()

        workload.operations["manifest_ordered"] = False
        self.assertEqual(workload._batch_correlation_contract()["record_count"], 0)
        workload.operations["manifest_ordered"] = True
        workload.diagnostics_dir = None
        self.assertEqual(workload._batch_correlation_contract()["record_count"], 0)
        captured = {
            "outcome": "completed",
            "capture_reason": "slow",
            "before_stats": {"wide": True},
            "after_stats": {"wide": True},
            "profile_capture": {"status": "not_triggered"},
        }
        with self.assertRaisesRegex(RuntimeError, "cannot be compacted"):
            runner.TreeDBMinimaRunner._compact_completed_batch_correlation(captured)
        self.assertIn("before_stats", captured)
        near_limit = {
            "sequence": 0,
            "operation": "initial_batch_insert",
            "scenario": "large",
            "batch_start": 0,
            "rows": 1,
            "outcome": "completed",
            "profile_capture": {"status": "not_triggered"},
            "before_public_count": {"status": "failed", "error": "x" * 1617},
        }
        minified = json.dumps(near_limit, separators=(",", ":"), allow_nan=False)
        self.assertLessEqual(len(minified.encode()), runner.COMPACT_BATCH_CORRELATION_MAX_BYTES)
        with self.assertRaisesRegex(RuntimeError, "exceeds"):
            runner.TreeDBMinimaRunner._compact_completed_batch_correlation(near_limit)
        duplicate = {
            "sequence": 0,
            "operation": "initial_batch_insert",
            "scenario": "large",
            "batch_start": 0,
            "rows": 1,
            "outcome": "completed",
            "stats_retention": "compact_completed",
            "profile_capture": {"status": "not_triggered"},
        }
        workload.batch_correlations = [dict(duplicate) for _ in range(10)]
        workload._batch_correlation_expected_identities = {
            ("initial_batch_insert", "large", index, 1) for index in range(10)
        }
        workload.operations["manifest_ordered"] = True
        workload.diagnostics_dir = Path("diagnostics")
        with self.assertRaisesRegex(RuntimeError, "identity/cardinality"):
            workload._batch_correlation_contract()
        workload.batch_correlations = [
            {
                **duplicate,
                "sequence": index,
                "batch_start": index,
            }
            for index in range(10)
        ]
        workload.batch_correlations[0].update({
            "outcome": "failed",
            "capture_reason": "slow",
            "stats_retention": "full_diagnostic",
            "before_stats": {"status": "captured"},
            "after_stats": {"status": "captured"},
            "profile_capture": {"status": "captured"},
        })
        with self.assertRaisesRegex(RuntimeError, "failed outcome"):
            workload._batch_correlation_contract()
        invalid_diagnostic = {
            "sequence": 0,
            "operation": "initial_batch_insert",
            "scenario": "large",
            "batch_start": 0,
            "rows": 1,
            "outcome": "completed",
            "capture_reason": "slow",
            "stats_retention": "full_diagnostic",
            "before_stats": None,
            "after_stats": None,
            "profile_capture": {},
        }
        workload.operations = {}
        workload.batch_correlations = [invalid_diagnostic]
        with self.assertRaisesRegex(RuntimeError, "stats retention"):
            workload._batch_correlation_contract()
        invalid_diagnostic["before_stats"] = {"status": "captured"}
        invalid_diagnostic["after_stats"] = {"status": "captured"}
        with self.assertRaisesRegex(RuntimeError, "stats retention"):
            workload._batch_correlation_contract()
        invalid_diagnostic["profile_capture"] = {"status": "captured"}
        invalid_diagnostic["capture_reason"] = "failed"
        invalid_diagnostic["outcome"] = "timeout"
        with self.assertRaisesRegex(RuntimeError, "outcome and capture reason"):
            workload._batch_correlation_contract()

    def test_slow_completed_upsert_retains_full_stats_and_profile_manifest(self) -> None:
        workload = self.workload(self.response(), diagnostics_dir=Path("diagnostics"))
        workload.diagnostic_slow_seconds = 0.001
        workload._expected_insert_batches[("initial_batch_insert", "mixed", 0)] = 1
        workload.controller.stats_snapshot = mock.Mock(side_effect=[
            {"status": "captured", "snapshot": {"stage": "before"}},
            {"status": "captured", "snapshot": {"stage": "after"}},
        ])
        workload.controller.capture_profiles = mock.Mock(return_value={
            "status": "captured", "manifest": {"status": "captured", "path": "profiles.json"},
        })
        upsert_documents = workload.client.upsert_documents

        def slow_upsert(*args: object, **kwargs: object) -> object:
            runner.time.sleep(0.02)
            return upsert_documents(*args, **kwargs)

        workload.client.upsert_documents = slow_upsert
        document = {"id": "minima/mixed/000000", "content": "c", "vector": [1.0, 0.0],
                    "user_id": "u", "fpath": "/a"}
        workload.upsert("initial_batch_insert", "mixed", [document])
        correlation = workload.batch_correlations[0]
        self.assertEqual(correlation["stats_retention"], "full_diagnostic")
        self.assertEqual(correlation["before_stats"]["snapshot"]["stage"], "before")
        self.assertEqual(correlation["after_stats"]["snapshot"]["stage"], "after")
        self.assertEqual(correlation["capture_reason"], "slow")
        self.assertEqual(correlation["profile_capture"]["manifest"]["path"], "profiles.json")

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
            self.assertEqual(correlation["stats_retention"], "full_diagnostic")
            self.assertEqual(correlation["before_stats"]["snapshot"]["phase"], "before")
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
                self.assertTrue(workload._batch_prepared)
                expected_batches = {
                    ("initial_batch_insert", "resume", start): start + 256
                    for start in range(0, 1024, 256)
                }
                self.assertEqual(workload._expected_insert_batches, expected_batches)
                workload._prepare_batch_correlations()
                self.assertEqual(workload._expected_insert_batches, expected_batches)
                self.assertEqual(workload._batch_correlation_max_records, 4)

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

    def test_shutdown_retains_last_live_resources_when_terminal_sample_regresses(self) -> None:
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
                side_effect=[usage(100, 1.0), usage(120, 3.0), usage(0, 0.0)],
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

    def test_shutdown_retains_peak_independently_of_final_cpu_and_rss(self) -> None:
        unavailable = {"availability": "unavailable", "bytes": None, "pid": 55,
                       "process_identity": "", "source": "/proc/<pid>/status:VmHWM",
                       "scope": "process_lifetime_through_sample"}

        def measured(value: int, identity: str = "55:123", pid: int = 55) -> dict[str, object]:
            return {**unavailable, "availability": "measured", "bytes": value,
                    "process_identity": identity, "pid": pid}

        cases = (
            ("terminal unavailable", [measured(100), measured(300), unavailable], 300),
            ("lower later peak", [measured(300), measured(200), unavailable], 300),
            ("different lifetime", [measured(100), measured(900, "55:999"), unavailable], 100),
            ("different pid", [measured(100), measured(900, "56:123", 56), unavailable], 100),
            ("never available", [unavailable, unavailable, unavailable], None),
            ("first available during shutdown", [unavailable, measured(300), unavailable], 300),
        )
        for name, peaks, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                controller = runner.ServiceController(
                    Path("/service"), "http://127.0.0.1:17120", Path(directory),
                    "command_wal_durable", 1, 1,
                )
                process = mock.MagicMock(pid=55)
                process.poll.return_value = None
                process.wait.side_effect = [runner.subprocess.TimeoutExpired("service", 0.05), 0]
                controller.process = process
                samples = [{"captured": True, "rss_bytes": 100 + i, "cpu_seconds": float(i + 1),
                            "availability": {}, "peak_rss": peak} for i, peak in enumerate(peaks)]
                with mock.patch.object(common, "server_process_resource_usage", side_effect=samples), \
                     mock.patch.object(common, "disk_bytes", return_value=140):
                    controller.stop()
                endpoint = controller.last_shutdown_resource_end
                self.assertEqual(endpoint["rss_bytes"], 102)
                self.assertEqual(endpoint["cpu_seconds"], 3.0)
                self.assertEqual(endpoint["peak_rss"]["bytes"], expected)
                self.assertEqual(endpoint["peak_rss"]["availability"], "unavailable" if expected is None else "measured")
                if expected is not None:
                    self.assertEqual(endpoint["peak_rss"]["process_identity"], "55:123")
                process.kill.assert_not_called()

    def test_shutdown_retains_positive_rss_when_terminal_cpu_is_equal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            controller = runner.ServiceController(
                Path("/service"), "http://127.0.0.1:17120", data_dir,
                "command_wal_durable", 3600, 120,
            )
            process = mock.MagicMock(pid=55)
            process.poll.return_value = None
            process.wait.return_value = 0
            controller.process = process
            valid = {
                "captured": True,
                "rss_bytes": 120,
                "cpu_seconds": 3.0,
                "availability": {
                    "rss_bytes": "test", "cpu_seconds": "test",
                    "bytes_per_op": "unavailable", "allocs_per_op": "unavailable",
                    "measurement_error": "",
                },
            }
            zero_rss = {**valid, "rss_bytes": 0}
            with mock.patch.object(
                common, "server_process_resource_usage",
                side_effect=[valid, zero_rss],
            ), mock.patch.object(common, "disk_bytes", return_value=140):
                controller.stop()

        self.assertEqual(controller.last_shutdown_resource_end["rss_bytes"], 120)
        self.assertEqual(controller.last_shutdown_resource_end["cpu_seconds"], 3.0)

    def test_shutdown_deadline_kills_reaps_cleans_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            controller = runner.ServiceController(
                Path("/service"), "http://127.0.0.1:17120", data_dir,
                "command_wal_durable", 3600, 120,
            )
            process = mock.MagicMock(pid=56)
            process.poll.return_value = None
            process.wait.return_value = 0
            log_file = mock.MagicMock()
            controller.process = process
            controller.log_file = log_file
            usage = {
                "captured": True,
                "rss_bytes": 120,
                "cpu_seconds": 3.0,
                "availability": {
                    "rss_bytes": "test", "cpu_seconds": "test",
                    "bytes_per_op": "unavailable", "allocs_per_op": "unavailable",
                    "measurement_error": "",
                },
            }
            continuation = mock.Mock()
            with mock.patch.object(
                common, "server_process_resource_usage", return_value=usage,
            ), mock.patch.object(common, "disk_bytes", return_value=150), \
                 mock.patch.object(runner.time, "monotonic", side_effect=[0, 121]):
                with self.assertRaisesRegex(TimeoutError, "graceful shutdown exceeded 120s"):
                    controller.stop()
                    continuation()

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=5)
        continuation.assert_not_called()
        self.assertEqual(controller.last_shutdown_resource_end["disk_bytes"], 150)
        self.assertIsNone(controller.process)
        self.assertIsNone(controller.log_file)
        log_file.close.assert_called_once_with()

    def test_main_shutdown_timeout_writes_nonqualifying_partial_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "partial.json"
            shutdown_end = {
                "captured": True, "rss_bytes": 120, "cpu_seconds": 3.0, "disk_bytes": 150,
                "availability": {
                    "rss_bytes": "test", "cpu_seconds": "test", "disk_bytes": "test",
                },
            }
            workload = object.__new__(runner.TreeDBMinimaRunner)
            workload.controller = SimpleNamespace(
                pid=None, last_shutdown_resource_end=shutdown_end,
            )
            workload._phase_total_start = 100
            workload._phase_start = 200
            workload._phase_name = "restart_open_readiness"
            workload._phase_resource_start = {
                **shutdown_end, "pid": 100, "process_identity": "old-process",
            }
            workload._phase_boundaries = []
            workload._phase_attribution = None
            workload._phase_restart_old_end = None
            workload._controller_restart_origin = (100, "old-process")
            workload.evidence = SimpleNamespace(failures=[], samples=[])

            def fail_run() -> None:
                raise TimeoutError("TreeDB graceful shutdown exceeded 120s")

            workload.run = fail_run
            workload.close = lambda: None
            workload.artifact = lambda: {
                "state": "partial",
                "passing": False,
                "failures": list(workload.evidence.failures),
                "phase_attribution": workload._finish_phase_attribution(),
            }
            args = SimpleNamespace(strategy="native_runtime",
                manifest=root / "manifest.json", output=output, service_bin=root / "service",
                url="http://127.0.0.1:17120", data_dir=root / "data",
                profile="command_wal_durable", startup_timeout=3600,
                operation_timeout=120, collection="owned", ef_search=128, small=False,
                diagnostics_dir=None, diagnostics_url="http://127.0.0.1:17121",
                diagnostic_slow_seconds=30, diagnostic_profile_seconds=5,
                diagnostic_capture_timeout=10, diagnostic_resume_scenario=None,
                diagnostic_resume_start=None,
            )
            with mock.patch.object(runner, "parse_args", return_value=args), \
                 mock.patch.object(common, "load_manifest", return_value={}), \
                 mock.patch.object(runner, "TreeDBMinimaRunner", return_value=workload), \
                 mock.patch.object(runner.time, "monotonic_ns", side_effect=[300, 310]):
                self.assertEqual(runner.main(), 1)

            artifact = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(
            artifact["failures"],
            ["TimeoutError: TreeDB graceful shutdown exceeded 120s"],
        )
        phase = artifact["phase_attribution"]["phases"][-1]
        self.assertFalse(phase["resource_evidence_complete"])
        self.assertEqual(phase["incomplete_reason"], "graceful_shutdown_failed_before_reopen")
        self.assertEqual(len(phase["resource_segments"]), 1)
        self.assertEqual(phase["resource_segments"][0]["end"]["pid"], 100)

    def test_replacement_startup_timeout_preserves_genuine_old_endpoint(self) -> None:
        old_start = {
            "captured": True, "rss_bytes": 100, "cpu_seconds": 2.0, "disk_bytes": 140,
            "availability": {}, "pid": 100, "process_identity": "old-process",
        }
        old_end = {
            "captured": True, "rss_bytes": 120, "cpu_seconds": 3.0, "disk_bytes": 150,
            "availability": {}, "pid": 100, "process_identity": "old-process",
        }
        failed_replacement_end = {
            "captured": True, "rss_bytes": 80, "cpu_seconds": 0.5, "disk_bytes": 160,
            "availability": {}, "pid": 101, "process_identity": "new-process",
        }
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.controller = SimpleNamespace(
            pid=None, last_shutdown_resource_end=failed_replacement_end,
        )
        workload._phase_total_start = 100
        workload._phase_start = 200
        workload._phase_name = "restart_open_readiness"
        workload._phase_resource_start = old_start
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload._phase_restart_old_end = old_end
        workload._controller_restart_origin = (100, "old-process")
        workload.evidence = SimpleNamespace(samples=[])

        with mock.patch.object(runner.time, "monotonic_ns", side_effect=[300, 310]):
            phase = workload._finish_phase_attribution()["phases"][-1]

        self.assertFalse(phase["resource_evidence_complete"])
        self.assertEqual(
            phase["incomplete_reason"], "replacement_service_unavailable_after_shutdown",
        )
        self.assertEqual(phase["resource_segments"], [{"start": old_start, "end": old_end}])
        self.assertEqual(workload._phase_restart_old_end, old_end)
        self.assertNotEqual(phase["resource_segments"][0]["end"], failed_replacement_end)

    def test_restart_verification_failure_splits_reused_pid_identity(self) -> None:
        old_start = {
            "captured": True, "rss_bytes": 100, "cpu_seconds": 2.0, "disk_bytes": 140,
            "availability": {}, "pid": 100, "process_identity": "old-process",
        }
        old_end = {
            "captured": True, "rss_bytes": 120, "cpu_seconds": 3.0, "disk_bytes": 150,
            "availability": {}, "pid": 100, "process_identity": "old-process",
        }
        new_process = {
            "captured": True, "rss_bytes": 80, "cpu_seconds": 0.5,
            "availability": {}, "pid": 100, "process_identity": "new-process",
        }
        workload = object.__new__(runner.TreeDBMinimaRunner)
        workload.controller = SimpleNamespace(pid=100, last_shutdown_resource_end=old_end)
        workload._phase_total_start = 100
        workload._phase_start = 200
        workload._phase_name = "restart_open_readiness"
        workload._phase_resource_start = old_start
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload._phase_restart_old_end = old_end
        workload._controller_restart_origin = (100, "old-process")
        workload.evidence = SimpleNamespace(samples=[])

        with mock.patch.object(workload, "_phase_process_snapshot", return_value=new_process), \
             mock.patch.object(workload, "_phase_disk_snapshot", return_value={
                 "captured": True, "disk_bytes": 160, "availability": {},
             }), mock.patch.object(runner.time, "monotonic_ns", side_effect=[300, 310]):
            phase = workload._finish_phase_attribution()["phases"][-1]

        self.assertFalse(phase["resource_evidence_complete"])
        self.assertEqual(phase["incomplete_reason"], "restart_verification_failed_after_reopen")
        self.assertEqual(len(phase["resource_segments"]), 2)
        self.assertEqual(phase["resource_segments"][0], {"start": old_start, "end": old_end})
        self.assertEqual(phase["resource_segments"][1]["start"], {
            "captured": True, "rss_bytes": 0, "cpu_seconds": 0.0, "disk_bytes": 150,
            "availability": {}, "pid": 100, "process_identity": "new-process",
        })
        self.assertEqual(phase["resource_segments"][1]["end"]["process_identity"], "new-process")

    def test_main_unexpected_service_exit_writes_nonqualifying_partial_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "partial.json"
            phase_start = {
                "captured": True, "rss_bytes": 100, "cpu_seconds": 2.0, "disk_bytes": 140,
                "availability": {}, "pid": 100, "process_identity": "old-process",
            }
            workload = object.__new__(runner.TreeDBMinimaRunner)
            workload.controller = SimpleNamespace(pid=None, last_shutdown_resource_end=None)
            workload._phase_total_start = 100
            workload._phase_start = 200
            workload._phase_name = "timed_search_write_overlap"
            workload._phase_resource_start = phase_start
            workload._phase_boundaries = []
            workload._phase_attribution = None
            workload._phase_restart_old_end = None
            workload._controller_restart_origin = None
            workload.evidence = SimpleNamespace(failures=[], samples=[])

            def fail_run() -> None:
                raise RuntimeError("TreeDB service exited unexpectedly")

            workload.run = fail_run
            workload.close = lambda: None
            workload.artifact = lambda: {
                "state": "partial",
                "passing": False,
                "failures": list(workload.evidence.failures),
                "phase_attribution": workload._finish_phase_attribution(),
            }
            args = SimpleNamespace(strategy="native_runtime",
                manifest=root / "manifest.json", output=output, service_bin=root / "service",
                url="http://127.0.0.1:17120", data_dir=root / "data",
                profile="command_wal_durable", startup_timeout=3600,
                operation_timeout=120, collection="owned", ef_search=128, small=False,
                diagnostics_dir=None, diagnostics_url="http://127.0.0.1:17121",
                diagnostic_slow_seconds=30, diagnostic_profile_seconds=5,
                diagnostic_capture_timeout=10, diagnostic_resume_scenario=None,
                diagnostic_resume_start=None,
            )
            with mock.patch.object(runner, "parse_args", return_value=args), \
                 mock.patch.object(common, "load_manifest", return_value={}), \
                 mock.patch.object(runner, "TreeDBMinimaRunner", return_value=workload), \
                 mock.patch.object(runner.time, "monotonic_ns", side_effect=[300, 310]):
                self.assertEqual(runner.main(), 1)
            artifact = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(
            artifact["failures"],
            ["RuntimeError: TreeDB service exited unexpectedly"],
        )
        phase = artifact["phase_attribution"]["phases"][-1]
        self.assertFalse(phase["resource_evidence_complete"])
        self.assertEqual(phase["incomplete_reason"], "service_unavailable_before_phase_endpoint")
        self.assertEqual(phase["resource_segments"][0]["start"], phase_start)
        self.assertEqual(phase["resource_segments"][0]["end"], phase_start)

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
        self.assertEqual(script.count(guarded), 3)
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
            args = SimpleNamespace(strategy="native_runtime",
                manifest=root / "manifest.json", output=output, service_bin=binary,
                url=f"http://127.0.0.1:{port}", data_dir=data_dir, profile="test",
                startup_timeout=2, collection="owned", operation_timeout=1, ef_search=1, small=False,
                diagnostics_dir=None, diagnostics_url="http://127.0.0.1:17121",
                diagnostic_slow_seconds=30, diagnostic_profile_seconds=5,
                diagnostic_capture_timeout=10, diagnostic_resume_scenario=None,
                diagnostic_resume_start=None,
            )
            controllers: list[runner.ServiceController] = []
            retained_request = {"request_sequence": 1, "outcome": "error", "error": "earlier query failed",
                                "dense_work": asdict(public_dense_work())}

            class FakeRunner:
                artifact_calls = 0
                def __init__(self, _manifest: object, *, controller: runner.ServiceController, **_kwargs: object) -> None:
                    self.controller = controller
                    self.evidence = SimpleNamespace(failures=["earlier query failed"], requests=[retained_request])
                    self.setup_interval = {"started_monotonic_ns": 1, "ended_monotonic_ns": 2}
                    self._phase_boundaries = [{"name": "warmup_search", "start_nanos": 3, "end_nanos": 4}]
                    controller.start()
                    controllers.append(controller)

                def run(self) -> None:
                    pass

                def artifact(self) -> dict[str, object]:
                    FakeRunner.artifact_calls += 1
                    if self.controller.pid is None:
                        raise AssertionError("artifact was not constructed at the live endpoint")
                    raise ValueError("artifact construction failed")

                def close(self) -> None:
                    raise RuntimeError("cleanup noise")

            with mock.patch.object(runner, "parse_args", return_value=args), \
                 mock.patch.object(common, "load_manifest", return_value={}), \
                 mock.patch.object(runner, "TreeDBMinimaRunner", FakeRunner), \
                 mock.patch.object(Path, "write_text", autospec=True, side_effect=Path.write_text) as write:
                with self.assertRaises(IsADirectoryError):
                    runner.main()
            write.assert_called_once()
            failed = json.loads(write.call_args.args[1])
            self.assertEqual(failed["failures"], ["earlier query failed",
                "artifact construction failed: ValueError: artifact construction failed",
                "cleanup failed: RuntimeError: cleanup noise"])
            raw = failed["backend_raw_evidence"]["treedb"]
            self.assertEqual(raw["request_evidence"], [retained_request])
            self.assertEqual(raw["setup_interval"], {"started_monotonic_ns": 1, "ended_monotonic_ns": 2})
            self.assertEqual(raw["phase_attribution"]["phases"][0]["name"], "warmup_search")
            self.assertNotIn("total_end_nanos", raw["phase_attribution"])
            self.assertEqual(FakeRunner.artifact_calls, 1)
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
            ), mock.patch.object(common, "linux_process_identity", return_value="321:10"):
                resource = common.server_resource_usage(321, Path(directory), "TreeDB")
        self.assertEqual(resource["availability"]["rss_bytes"], "TreeDB server PID 321")
        self.assertEqual(resource["availability"]["cpu_seconds"], "TreeDB server PID 321")

    def test_artifact_uses_shared_segment_delta_resource_semantics(self) -> None:
        baseline = {
            "pid": 321, "linux_process_identity": "321:10",
            "captured": True, "rss_bytes": 100, "cpu_seconds": 1.0, "disk_bytes": 1000,
            "availability": {"rss_bytes": "test", "cpu_seconds": "test", "disk_bytes": "test"},
        }
        end = {
            "pid": 321, "linux_process_identity": "321:10",
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
        workload.strategy, workload.transport = "native_runtime", "http"
        workload.column_graph_serving = None
        workload.runner_sha256 = "b" * 64
        workload.service_binary_sha256 = "c" * 64
        workload.service_binary_vcs_revision = "a" * 40
        workload.service_binary_vcs_modified = "false"
        workload.operation_timeout_seconds = 120
        workload._phase_total_start = workload._phase_start = runner.time.monotonic_ns()
        workload._phase_name = "initial_durable_load"
        workload._phase_boundaries = []
        workload._phase_attribution = None
        workload._phase_resource_start = baseline
        workload._phase_restart_old_end = None
        storage = tempfile.TemporaryDirectory()
        self.addCleanup(storage.cleanup)
        workload.storage_path = Path(storage.name)
        workload.resource_server_name = "TreeDB"
        workload.process_identity = lambda pid: f"process-{pid}"
        workload.evidence = SimpleNamespace(samples=[])
        workload.url = "http://127.0.0.1:1"
        workload.collection = "owned"
        workload.config = {"dimension": 8, "metric": "cosine"}
        workload.manifest = {"schema": common.MANIFEST_SCHEMA}
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
        workload._batch_correlation_max_records = 10
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
                "resource_measurement": resource,
                "collection_configuration_transition": {"attempted": False},
                "readiness": {"sessions": []},
            }},
        }
        with mock.patch.object(common.QdrantMinimaRunner, "artifact", return_value=base_artifact), \
             mock.patch.object(common, "server_process_resource_usage", return_value=baseline), \
             mock.patch.object(common, "disk_bytes", return_value=1100) as disk_bytes:
            artifact = workload.artifact()
        disk_bytes.assert_not_called()
        configuration = artifact["backends"][0]["configuration"]
        self.assertEqual(configuration["scalar_fields"], "meta.user_id,meta.fpath")
        self.assertEqual(json.loads(configuration["effective_collection"]), workload.effective_collection)
        raw = artifact["backend_raw_evidence"]["treedb"]
        self.assertEqual(raw["resource_measurement"], resource)
        self.assertEqual(raw["resource_availability"]["measurement"], common.RESOURCE_SEMANTICS)
        self.assertEqual(raw["upsert_batch_correlations"], [])
        self.assertEqual(raw["upsert_batch_correlation_contract"], {
            "schema": runner.BATCH_CORRELATION_SCHEMA,
            "record_count": 0,
            "maximum_record_count": 10,
            "compact_completed_records": 0,
            "full_diagnostic_records": 0,
            "compact_record_max_bytes": runner.COMPACT_BATCH_CORRELATION_MAX_BYTES,
            "full_stats_retention": ["failed", "timeout", "slow", "profile_captured"],
        })
        self.assertNotIn("initial_upload_hnsw", configuration)
        self.assertNotIn("collection_configuration_transition", raw)
        self.assertNotIn("readiness", raw)
        self.assertEqual(configuration["operation_timeout_seconds"], "120")
        self.assertEqual(configuration["startup_reopen_timeout_seconds"], "3600")
        self.assertEqual(configuration["shutdown_timeout_seconds"], "120")
        self.assertEqual(configuration["product_commit"], "a" * 40)
        self.assertEqual(configuration["service_binary_vcs_revision"], "a" * 40)
        self.assertEqual(configuration["service_binary_vcs_modified"], "false")
        self.assertEqual(raw["phase_attribution"]["phases"][0]["classification"], "production_path")
        self.assertEqual(artifact["scenarios"][0]["route"]["candidate_ids"], 5)
        self.assertEqual(artifact["scenarios"][0]["route"]["visited_candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidates"], 41)
        self.assertEqual(raw["native_route_responses"]["small"]["candidate_ids"], 5)


class MinimaTypedRunnerTest(unittest.TestCase):
    workload = MinimaTreeDBRunnerTest.workload
    response = MinimaTreeDBRunnerTest.response

    def test_native_and_control_clients_share_shutdown_ownership(self) -> None:
        controller = SimpleNamespace(native_address="127.0.0.1:17122", stop=mock.Mock())
        control, native = mock.Mock(), mock.Mock()
        with mock.patch.object(runner, "TreeDBClient", side_effect=[control, native]) as factory:
            clients = runner.ThreadLocalClients("http://127.0.0.1:17120", 3, controller)
            self.assertIs(clients.current(), control)
            self.assertIs(clients.native, native)
            self.assertIs(clients.native, native)
            self.assertEqual(factory.call_count, 2)
            self.assertEqual(factory.call_args.kwargs["native_address"], controller.native_address)
            clients.close()
        control.close.assert_called_once()
        native.close.assert_called_once()
        controller.stop.assert_called_once()

    def test_reopen_ensures_existing_graph_before_queries(self) -> None:
        workload = self.workload(self.response())
        workload.strategy = "column_graph"
        workload._graph_built = True
        workload.column_graph_serving = {"SearchCandidates": 4096}
        workload.controller.start = mock.Mock()
        workload.clients = mock.Mock()
        workload.ensure_compatible = mock.Mock()
        workload.connect()
        workload.ensure_compatible.assert_called_once()
        workload.clients.optimize_index.assert_called_once_with(
            "owned", column_graph_action="ensure", column_graph_serving=workload.column_graph_serving)
    def test_typed_runner_uses_native_batches_and_snapshot_fetch(self) -> None:
        info = SimpleNamespace(name="owned", generation=7, dimension=2, metric="cosine",
                               vector_strategy="column_graph", extra={"typed_input": True},
                               scalar_fields=[SimpleNamespace(field=f"meta.{name}", value_type="string")
                                              for name in ("user_id", "fpath")],
                               to_dict=lambda: {"typed_input": True, "generation": 7})
        response = self.response(native_base_plus_live_delta=False, native_command_version=2, index=info)
        workload = self.workload(response)
        workload.strategy, workload.transport = "column_graph", "native"
        workload.config.update(dimension=2, metric="cosine")
        workload.column_graph_serving = {"SearchCandidates": 4096}
        workload._graph_built = False
        workload.index_info = None
        control, native = mock.Mock(), mock.Mock()
        workload.client = control
        workload.clients = SimpleNamespace(native=native)
        control.ensure_index.return_value = info
        native.query_by_embedding.return_value = response
        native.upsert_documents.return_value = SimpleNamespace(upserted=1, ids=["d"])
        native.get_many.return_value = [response.documents[0], None, response.documents[0]]
        workload.create_owned_collection()
        self.assertTrue(control.ensure_index.call_args.kwargs["typed_input"])
        self.assertEqual(control.ensure_index.call_args.kwargs["vector_index_options"], {"strategy": "column_graph"})
        document = {"id": "d", "content": "c", "vector": [1., 0.], "user_id": "u", "fpath": "/a"}
        workload.upsert("insert", "mixed", [document])
        self.assertIs(native.upsert_documents.call_args.kwargs["index_info"], info)
        control.upsert_documents.assert_not_called()
        self.assertIs(workload.initial_load_to_query_boundary(), control.optimize_index.return_value)
        self.assertTrue(workload._graph_built)
        self.assertEqual(control.optimize_index.call_args.kwargs["column_graph_action"], "build")
        self.assertEqual(workload.search("query", "mixed"), (["d"], [1.]))
        self.assertIs(native.query_by_embedding.call_args.kwargs["index_info"], info)
        control.query_by_embedding.assert_not_called()
        fetched = workload.retrieve("retrieve", "mixed", ["d", "missing", "d"])
        self.assertEqual([row.payload["id"] for row in fetched], ["d", "d"])
        native.get_many.assert_called_once_with("owned", ["d", "missing", "d"], index_info=info)
        control.filter_documents.assert_not_called()
        self.assertEqual(sum(s["category"] == "fetch" for s in workload.evidence.samples), 1)
        native.query_by_embedding.return_value = self.response(index=info, native_command_version=1)
        with self.assertRaisesRegex(RuntimeError, "left required native route"):
            workload.search("wrong_dispatch", "mixed")


class MinimaMeasuredRunnerTest(unittest.TestCase):
    workload = MinimaTreeDBRunnerTest.workload
    response = MinimaTreeDBRunnerTest.response

    def test_measured_shutdown_live_sample_preserves_disk_availability(self) -> None:
        for initial_exists, final_exists in ((True, True), (False, True), (True, False)):
            with self.subTest(initial_exists=initial_exists, final_exists=final_exists), \
                 tempfile.TemporaryDirectory() as directory:
                data_dir = Path(directory)
                controller = runner.ServiceController(Path("service"), "http://127.0.0.1:1",
                                                      data_dir, "test", 1, 1, measured=True)
                process = SimpleNamespace(pid=4321, returncode=None)
                controller.process = process
                controller._owned_identity = "4321:77"
                controller.lifetimes = [{"exit": {"availability": "measured", "exit_code": 0}}]
                reaps = iter((None, None, 0, 0))
                def reap():
                    result = next(reaps)
                    process.returncode = result
                    return result
                samples = [{"captured": True, "linux_process_identity": "4321:77",
                            "cpu_seconds": cpu, "rss_bytes": 100 + cpu,
                            "availability": {"cpu_seconds": source, "rss_bytes": source}}
                           for cpu, source in ((1, "first live sample"), (2, "later live sample"))]
                with mock.patch.object(controller, "_reap_owned", side_effect=reap), \
                     mock.patch.object(controller, "work_snapshot"), \
                     mock.patch.object(controller, "_terminal_work"), \
                     mock.patch.object(common, "server_process_resource_usage", side_effect=samples), \
                     mock.patch.object(common, "linux_process_identity", return_value="4321:77"), \
                     mock.patch.object(common, "disk_bytes", side_effect=(10, 20)), \
                     mock.patch.object(Path, "exists", side_effect=(initial_exists, final_exists)), \
                     mock.patch.object(runner.os, "kill") as kill, \
                     mock.patch.object(runner.time, "sleep"):
                    controller._stop_measured()
                kill.assert_called_once_with(4321, runner.signal.SIGTERM)
                self.assertIsNone(controller.process)
                endpoint = controller.last_shutdown_resource_end
                self.assertEqual(endpoint["captured"], initial_exists and final_exists)
                self.assertEqual(endpoint["cpu_seconds"], 2)
                self.assertEqual(endpoint["disk_bytes"], 20)
                self.assertEqual(endpoint["availability"], {
                    "cpu_seconds": "later live sample", "rss_bytes": "later live sample",
                    "disk_bytes": str(data_dir),
                })

    def test_measured_treedb_restart_uses_inherited_owner_checks_without_docker(self) -> None:
        workload = self.workload(self.response())
        old_end = {"captured": True, "rss_bytes": 15, "cpu_seconds": 3.0, "disk_bytes": 140}
        first_work = {"work": {"pid": 101, "origin_unix_nano": 8}}
        workload.controller = SimpleNamespace(pid=None, last_shutdown_resource_end=old_end,
            lifetimes=[{"ordinal": 0, "linux_process_identity": "100:7", "last_live_work": {"work": {"pid": 100}}}])
        def start():
            workload.controller.pid = 101
            workload.controller.lifetimes.append({"ordinal": 1, "first_work": first_work})
        workload.controller.start = mock.Mock(side_effect=start)
        workload.measured, workload.deployment = True, "owned_process"
        workload.resource_server_name, workload.server_pid = "TreeDB", 100
        workload.url, workload.server_listener_port = "http://127.0.0.1:17120", None
        workload.storage_path = Path("/data")
        workload.restart_server, workload.restart_identity = workload.restart_controller, "owned controller"
        workload.restart_origin = workload._controller_restart_origin = (100, "old")
        workload.restart_origin_linux_identity = "100:7"
        workload.resource_baseline = old_end
        workload.completed_resource_segments = []
        workload.lifetime_ordinal = 0
        workload.process_running = mock.Mock(return_value=False)
        workload.process_owns_endpoint = mock.Mock(return_value=True)
        workload.process_identity = mock.Mock(return_value="new")
        workload.measurement_configuration = {"cpu_affinity": "0"}
        workload._verify_measured_server_affinity = mock.Mock()
        workload._verify_measured_docker_runtime = mock.Mock(side_effect=AssertionError("TreeDB entered Docker verification"))
        with mock.patch.object(common, "linux_process_identity", return_value="101:8"), \
             mock.patch.object(common, "server_resource_usage", return_value=dict(old_end)):
            workload.restart_backend()
        workload.controller.start.assert_called_once_with()
        workload.process_running.assert_called_once_with(100)
        workload.process_owns_endpoint.assert_called_once_with(101, workload.url, None)
        workload._verify_measured_server_affinity.assert_called_once_with("0")
        workload._verify_measured_docker_runtime.assert_not_called()
        self.assertEqual(workload.server_pid, 101)
        self.assertEqual(workload.lifetime_ordinal, 1)
        self.assertIs(workload.controller.lifetimes[1]["first_work"], first_work)
        self.assertEqual(workload.resource_baseline["cpu_seconds"], 0.0)
        self.assertEqual(workload.resource_baseline["disk_bytes"], old_end["disk_bytes"])

    def test_outer_query_timer_ends_before_owned_proof_normalization(self) -> None:
        interval = {}
        proof = public_dense_work()
        def normalize(value):
            self.assertIs(value, proof)
            self.assertEqual(interval["ended_monotonic_ns"], 40)
            return asdict(value)
        response = self.response(dense_work=proof)
        workload = self.workload(response)
        workload.measured = True
        workload.index_info = SimpleNamespace(generation=7)
        workload.evidence.request_context = lambda: {
            "phase": "warmup_search", "lifetime_ordinal": 0, "transport": "http"}
        with mock.patch.object(runner.time, "monotonic_ns", side_effect=[10, 20, 30, 40, 50, 60]), \
             mock.patch.object(runner, "asdict", side_effect=normalize):
            self.assertEqual(workload.search("warmup_search", "mixed", interval), (["d"], [1.]))
        record = workload.evidence.requests[0]
        self.assertEqual(interval, {"started_monotonic_ns": 10, "ended_monotonic_ns": 40,
                                    "request_sequence": 1})
        self.assertEqual(record["dense_work"], asdict(proof))
        self.assertEqual(record["result_count"], 1)
        self.assertEqual(workload.evidence.samples[0]["duration_nanos"], 10)
        self.assertEqual(record["ended_monotonic_ns"] - record["started_monotonic_ns"], 30)

    def test_service_and_post_decode_failures_keep_owned_proof(self) -> None:
        for failure_kind in ("service", "client_decode", "runner_decode"):
            with self.subTest(failure_kind=failure_kind):
                proof = public_dense_work()
                if failure_kind == "service":
                    proof = replace(proof, completed=False,
                                    graph=replace(proof.graph, completed=False, base_result_ids=256))
                proof = DenseSearchWork.from_dict(asdict(proof))
                response = self.response(dense_work=proof)
                workload = self.workload(response)
                workload.measured = True
                workload.index_info = SimpleNamespace(generation=7)
                workload.evidence.request_context = lambda: {
                    "phase": "timed_search_write_overlap", "lifetime_ordinal": 0, "transport": "http"}
                if failure_kind == "runner_decode":
                    response.documents[0].score = None
                    error_type = RuntimeError
                else:
                    failure = (TreeDBServiceError("internal", "cancelled after work", dense_work=proof)
                               if failure_kind == "service" else
                               TreeDBProtocolError("invalid document", dense_work=proof))
                    workload.client.query_by_embedding = mock.Mock(side_effect=failure)
                    error_type = type(failure)
                with self.assertRaises(error_type):
                    workload.search("timed_search_with_batch_insert", "mixed", {})
                record = workload.evidence.requests[0]
                self.assertEqual(record["dense_work"], asdict(proof))
                self.assertEqual(record["outcome"], "error")
                self.assertTrue(record["error"])
                self.assertGreaterEqual(record["ended_monotonic_ns"], record["started_monotonic_ns"])

    def test_legacy_control_preserves_client_decode_without_normalizing_proof(self) -> None:
        workload = self.workload(self.response(dense_work=public_dense_work()))
        workload.measured = False
        with mock.patch.object(runner, "asdict",
                               side_effect=AssertionError("added capture in legacy control")) as normalize:
            self.assertEqual(workload.search("warmup_search", "mixed", {}), (["d"], [1.]))
            normalize.assert_not_called()
        self.assertEqual(workload.evidence.requests, [])
        self.assertEqual(len(workload.evidence.samples), 2)

    def test_measured_wait4_is_exclusive_and_retains_shutdown_peak(self) -> None:
        controller = runner.ServiceController(Path("service"), "http://127.0.0.1:1", Path("data"),
                                              "test", 1, 1, measured=True)
        process = SimpleNamespace(pid=4321, returncode=None,
                                  poll=mock.Mock(side_effect=AssertionError("poll reaped child")),
                                  wait=mock.Mock(side_effect=AssertionError("wait reaped child")))
        controller.process = process
        controller._owned_identity = "4321:77"
        controller.lifetimes = [{"exit": {"availability": "unavailable"}}]
        with mock.patch.object(runner.os, "wait4", side_effect=[
                (0, 0, None), (4321, 0, SimpleNamespace(ru_maxrss=32768))]) as wait4:
            self.assertEqual(controller.pid, 4321)
            self.assertIsNone(controller.pid)
            self.assertEqual(controller._reap_owned(), 0)
        self.assertEqual(wait4.call_count, 2)
        self.assertEqual(process.returncode, 0)
        exit_record = controller.lifetimes[0]["exit"]
        self.assertEqual(exit_record["peak_rss_bytes"], 32768 * 1024)
        self.assertEqual(exit_record["scope"], "owned_process_start_through_exit")
        self.assertEqual(exit_record["linux_process_identity"], "4321:77")
        for failure in (ChildProcessError("already reaped"), (99, 0, SimpleNamespace(ru_maxrss=1)),
                        (4321, 0, SimpleNamespace(ru_maxrss=None))):
            process.returncode = None
            controller.lifetimes[0]["exit"] = {"availability": "unavailable"}
            with mock.patch.object(runner.os, "wait4", side_effect=failure if isinstance(failure, Exception) else None,
                                   return_value=failure):
                self.assertEqual(controller._reap_owned(), 255)
            self.assertEqual(controller.lifetimes[0]["exit"]["availability"], "unavailable")
            self.assertNotIn("peak_rss_bytes", controller.lifetimes[0]["exit"])

    @unittest.skipUnless(sys.platform == "linux" and hasattr(os, "wait4"), "owned wait4 is Linux-only")
    def test_measured_real_child_cleanup_startup_and_measurement_failures(self) -> None:
        for failure in (None, "startup", "measurement"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root/"service"
                write_health_service(binary)
                ports = []
                for _ in range(2):
                    with socket.socket() as listener:
                        listener.bind(("127.0.0.1", 0))
                        ports.append(listener.getsockname()[1])
                data = root/"data"
                data.mkdir()
                if failure != "startup":
                    (data/"compatible").touch()
                controller = runner.ServiceController(binary, f"http://127.0.0.1:{ports[0]}", data,
                    "test", 0.3 if failure == "startup" else 2, 2,
                    diagnostics_url=f"http://127.0.0.1:{ports[1]}", measured=True)
                try:
                    if failure == "startup":
                        with self.assertRaises(TimeoutError):
                            controller.start()
                    else:
                        controller.start()
                        child = controller.process
                        self.assertEqual(controller.lifetimes[0]["first_work"]["availability"], "measured")
                        with mock.patch.object(child, "poll", side_effect=AssertionError("Popen poll reaps")), \
                             mock.patch.object(child, "wait", side_effect=AssertionError("Popen wait reaps")):
                            if failure == "measurement":
                                with mock.patch.object(common, "server_process_resource_usage", side_effect=RuntimeError("resource failed")):
                                    with self.assertRaisesRegex(RuntimeError, "resource failed"):
                                        controller.stop()
                            else:
                                controller.stop()
                    lifetime = controller.lifetimes[0]
                    self.assertIsNone(controller.process)
                    self.assertEqual(lifetime["exit"]["availability"], "measured")
                    self.assertEqual(lifetime["exit"]["exit_code"], 0)
                    self.assertGreater(lifetime["exit"]["peak_rss_bytes"], 0)
                    self.assertTrue(lifetime["terminal_work"]["work"]["fixture_cleanup"])
                    self.assertEqual(lifetime["terminal_work"]["work"]["origin_unix_nano"], 7)
                    self.assertFalse(common.server_process_running(lifetime["pid"]))
                    if failure != "startup":
                        self.assertFalse(lifetime["last_live_work"]["work"]["fixture_cleanup"])
                finally:
                    controller.stop()

    def test_terminal_collection_uses_only_owned_region_and_rejects_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            controller = runner.ServiceController(Path("service"), "http://127.0.0.1:1",
                                                  Path(directory)/"data", "test", 1, 1, measured=True)
            row = {"event": "treedb_document_service_terminal_work", "version": 1,
                   "contract_version": runner.SERVICE_CONTRACT, "cleanup_completed": True,
                   "shutdown_failures": 0, "work": {"pid": 123, "origin_unix_nano": 7}}
            line = ("service: " + json.dumps(row) + "\n").encode()
            controller.log_path.write_bytes(line)
            controller._log_region_start = len(line)
            controller.lifetimes = [{}]
            with controller.log_path.open("ab") as stream:
                stream.write(b"ordinary log\n" + line)
            controller._terminal_work()
            self.assertEqual(controller.lifetimes[0]["terminal_work"], row)
            with controller.log_path.open("ab") as stream:
                stream.write(line)
            controller.lifetimes = [{}]
            controller._terminal_work()
            self.assertIn("got 2", controller.lifetimes[0]["terminal_error"])
            self.assertEqual(controller.lifetimes[0]["terminal_work"], row)
            # A second record stops parsing immediately, retaining only the first.
            with controller.log_path.open("ab") as stream:
                stream.write(b"treedb_document_service_terminal_work {invalid JSON\n")
            controller.lifetimes = [{}]
            controller._terminal_work()
            self.assertIn("got 2", controller.lifetimes[0]["terminal_error"])
            self.assertEqual(controller.lifetimes[0]["terminal_work"], row)
            for prefix in (b"", line):
                with self.subTest(prefix=bool(prefix)):
                    controller.log_path.write_bytes(prefix + b"x" * 1025 + b"\n" + line)
                    controller._log_region_start = 0
                    controller.lifetimes = [{}]
                    with mock.patch.object(runner, "DIAGNOSTIC_STATS_BYTES", 1024):
                        controller._terminal_work()
                    self.assertIn("exceeds bound", controller.lifetimes[0]["terminal_error"])
                    self.assertEqual(controller.lifetimes[0].get("terminal_work"), row if prefix else None)

    def test_effective_control_flag_disables_added_listener_and_capture(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            serving = root/"serving.json"
            serving.write_text('{"SearchCandidates":4096}')
            argv = ["runner", "--manifest", str(root/"manifest"), "--output", str(root/"out"),
                    "--service-bin", str(root/"service"), "--data-dir", str(root/"data"),
                    "--collection", "owned", "--strategy", "column_graph", "--transport", "http",
                    "--column-graph-serving", str(serving), "--measured", "--legacy-diagnostic-control",
                    "--freeze", str(root/"freeze"), "--expected-freeze-sha256", "f"*64,
                    "--comparator-bin", str(root/"comparator")]
            fake = mock.Mock()
            fake.artifact.return_value = {"failures": []}
            with mock.patch.object(sys, "argv", argv), \
                 mock.patch.object(common, "load_manifest", return_value={}), \
                 mock.patch.object(common, "load_measured_freeze", return_value={"reviewed_product_commit":"a"*40}), \
                 mock.patch.object(common, "measured_source_configuration", return_value={}), \
                 mock.patch.object(runner, "ServiceController") as controller, \
                 mock.patch.object(runner, "TreeDBMinimaRunner", return_value=fake):
                self.assertEqual(runner.main(), 0)
            self.assertIsNone(controller.call_args.kwargs["diagnostics_url"])
            self.assertFalse(controller.call_args.kwargs["measured"])
            self.assertEqual(controller.call_args.kwargs["block_profile_rate"], 0)
            self.assertEqual(controller.call_args.kwargs["mutex_profile_fraction"], 0)
            self.assertFalse(fake.measured)
            self.assertIsNone(fake.evidence.request_context)
            controller.return_value.stop.assert_called_once()
            self.assertIn("measurement_control", json.loads((root/"out").read_text()))


if __name__ == "__main__":
    unittest.main()
