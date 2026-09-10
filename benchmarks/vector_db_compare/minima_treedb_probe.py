"""One bounded native initial ingest; existing service/client, optional Build attribution.

CPU profiles are fixed 8-second observations containing 32 writes and remaining
idle/background activity. Exact write intervals and allocation endpoints are
separate. Allocation profiles are sampled and may lag GC; no forced GC.
"""
import argparse
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import sys
import socket
import subprocess
import time
import traceback
from urllib.error import HTTPError

import minima_treedb_runner as tr

if sys.flags.optimize:
    raise RuntimeError('Minima diagnostics require Python without -O or PYTHONOPTIMIZE')

def python_inputs():
    """Bind concrete imported files and reject an installed client substitution."""
    source = Path(__file__).resolve().parents[2]
    expected = {'minima_treedb_runner': source / 'benchmarks/vector_db_compare/minima_treedb_runner.py',
                'minima_qdrant_runner': source / 'benchmarks/vector_db_compare/minima_qdrant_runner.py'}
    client = source / 'clients/python/treedb_client/src/treedb_client'
    paths = {Path(sys.executable).resolve()}
    for name, module in list(sys.modules.items()):
        filename = getattr(module, '__file__', None)
        if not filename:
            continue
        path = Path(filename).resolve()
        if name in expected:
            assert path == expected[name], (name, path)
        if name == 'treedb_client' or name.startswith('treedb_client.'):
            assert path.is_relative_to(client), (name, path)
        if path.is_file():
            paths.add(path)
    return {str(path): tr.file_sha256(path) for path in sorted(paths)}


def validate_affinity(client, server):
    assert client == server and len(client) == len(set(client)) == 6
    assert all(type(cpu) is int and cpu >= 0 for cpu in client)


def validate_batch_stats(observed, pid, name, generation, rows):
    assert observed['status'] == 'captured', observed
    snapshot = observed['snapshot']
    assert snapshot['work']['pid'] == pid
    assert not any(snapshot['work']['indexed_json'].values())
    assert not any(snapshot['work']['runtime'].values())
    active = snapshot['last_opened_index']
    assert active['name'] == name and active['generation'] == generation
    insert = active['insert']
    assert insert['Documents'] == rows
    assert insert['ColumnPublishRows'] >= rows
    assert insert['ColumnPublishCommit'] > 0 and insert['ColumnPublishManifestBytes'] > 0
    dependency = insert['ColumnPublishFinalizeCandidateDependencyBytes']
    owned = insert['ColumnPublishFinalizeCandidateOwnedBytes']
    pending = insert['ColumnPublishFinalizeAdmissionPendingBytes']
    commits = insert['ColumnPublishFinalizeAdmissionPendingCommits']
    hard = insert['ColumnPublishFinalizeHardAdmissionCount']
    assert all(type(x) is int for x in [dependency, owned, pending, commits, hard])
    assert 0 < dependency <= owned <= pending and 0 < commits
    assert hard in (0, 1) and bool(hard) == (pending > 256 << 20 or commits > 65536)

def initial_batches(manifest):
    sizes = {'bounded-250k': 250_000, 'bounded-500k': 500_000, 'bounded-1000k': 1_000_000}
    assert manifest['fixture'] in sizes, 'unsupported diagnostic fixture'
    assert sum(x['corpus_rows'] for x in manifest['corpora']) == sizes[manifest['fixture']]
    assert manifest['config']['batch_size'] == 256
    initial = next(x for x in manifest['operations'] if x['name'] == 'initial_batch_insert')
    batches = [(x['scenario'], start, min(256, x['start'] + x['rows'] - start))
               for x in initial['insert_ranges']
               for start in range(x['start'], x['start'] + x['rows'], 256)]
    assert sum(x[2] for x in batches) == sizes[manifest['fixture']] - 1328
    return batches


def validate_build_capture(capture, pid, origin, rows):
    request, = capture['requests']
    assert request['operation_name'] == 'column_graph_initial_build' and request['outcome'] == 'success'
    assert capture['profile_seconds'] == 60
    assert capture['cpu']['started_monotonic_ns'] < capture['active_profile_probe']['observed_monotonic_ns'] < request['started_monotonic_ns'] < request['ended_monotonic_ns'] < capture['cpu']['ended_monotonic_ns']
    assert capture['active_profile_probe']['status'] == 500
    assert 'profiling already in use' in capture['active_profile_probe']['body']
    for name in ('before_work', 'after_work', 'after_profile_work'):
        work = capture[name]['work']
        assert work['pid'] == pid and work['origin_unix_nano'] == origin
        assert not any(work['indexed_json'].values()) and not any(work['runtime'].values())
        for operation in ('build', 'public'):
            assert work['fold'][operation] == {'attempts': 0, 'completed': 0, 'errors': 0}
        renews = 0 if name == 'before_work' else 1
        assert work['fold']['renew'] == {'attempts': renews, 'completed': renews, 'errors': 0}
        assert all(work['fold'][key] == 0 for key in ('publications', 'candidate_bytes_charged', 'appender_attempts_charged'))
    assert capture['public_count'] == rows and capture['response']['status']['strategy'] == 'column_graph'
    stages = capture['response']['status']['column_graph_build']
    assert stages['total_nanos'] > 0 and all(type(v) is int and v >= 0 for v in stages.values())
    assert capture['response']['timing']['total_nanos'] >= stages['total_nanos']
    before, after = (capture[key]['work']['memory'] for key in ('before_work', 'after_work'))
    assert capture['allocation_delta'] == {key: after[key] - before[key] for key in ('total_alloc', 'mallocs')}
    assert all(value >= 0 for value in capture['allocation_delta'].values())


if __name__ == '__main__' and sys.argv[1:] == ['--self-check']:
    import copy
    good = {'status': 'captured', 'snapshot': {'work': {'pid': 42, 'indexed_json': {}, 'runtime': {}},
        'last_opened_index': {'name': 'test', 'generation': 7, 'insert': {'Documents': 256,
            'ColumnPublishRows': 256, 'ColumnPublishCommit': 1, 'ColumnPublishManifestBytes': 1,
            'ColumnPublishFinalizeCandidateDependencyBytes': 10, 'ColumnPublishFinalizeCandidateOwnedBytes': 15, 'ColumnPublishFinalizeAdmissionPendingBytes': 20,
            'ColumnPublishFinalizeAdmissionPendingCommits': 2, 'ColumnPublishFinalizeHardAdmissionCount': 0}}}}
    validate_batch_stats(good, 42, 'test', 7, 256)
    for pending, commits, hard in [(256 << 20, 65536, 0), ((256 << 20) + 1, 1, 1), (1, 65537, 1)]:
        boundary = copy.deepcopy(good)
        values = boundary['snapshot']['last_opened_index']['insert']
        values.update(ColumnPublishFinalizeCandidateDependencyBytes=1, ColumnPublishFinalizeCandidateOwnedBytes=1,
            ColumnPublishFinalizeAdmissionPendingBytes=pending,
            ColumnPublishFinalizeAdmissionPendingCommits=commits,
            ColumnPublishFinalizeHardAdmissionCount=hard)
        validate_batch_stats(boundary, 42, 'test', 7, 256)
        values['ColumnPublishFinalizeHardAdmissionCount'] = 1 - hard
        try: validate_batch_stats(boundary, 42, 'test', 7, 256)
        except AssertionError: pass
        else: raise AssertionError('accepted mismatched boundary receipt')
    for path, value in [(('status',), 'failed'), (('snapshot','work','pid'),43),
        (('snapshot','last_opened_index','generation'),8),
        (('snapshot','last_opened_index','insert','Documents'),255),
        (('snapshot','last_opened_index','insert','ColumnPublishCommit'),0),
        (('snapshot','last_opened_index','insert','ColumnPublishFinalizeCandidateDependencyBytes'),0),
        (('snapshot','last_opened_index','insert','ColumnPublishFinalizeAdmissionPendingBytes'),9),
        (('snapshot','last_opened_index','insert','ColumnPublishFinalizeHardAdmissionCount'),1),
        (('snapshot','last_opened_index','insert','ColumnPublishFinalizeCandidateOwnedBytes'),9),
        (('snapshot','last_opened_index','insert','ColumnPublishFinalizeCandidateOwnedBytes'),21)]:
        bad = copy.deepcopy(good)
        target = bad
        for key in path[:-1]: target = target[key]
        target[path[-1]] = value
        try: validate_batch_stats(bad, 42, 'test', 7, 256)
        except AssertionError: pass
        else: raise AssertionError(f'accepted invalid stats: {path}')
    print('PASS: current batch stats identity, population and publication checks; 13 negative controls and 3 valid threshold controls')
    raise SystemExit(0)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--serving', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--build-profile', action='store_true', help='append one fixed 60-second Build CPU observation')
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False)
    data_dir = args.output / 'db'
    manifest = tr.common.load_manifest(args.manifest)
    batches = initial_batches(manifest)
    binding = json.loads(args.binding.read_text())
    assert tr.file_sha256(args.manifest) == binding['manifest_sha256']
    assert str(args.serving.resolve()) == binding['serving']
    assert tr.file_sha256(args.serving) == binding['serving_sha256']
    assert args.build_profile is binding['build_profile']
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=Path(tr.__file__).resolve().parents[2])
    assert tr.repository_commit() == binding['source_commit']
    assert str(args.binary.resolve()) == binding['binary']
    assert tr.file_sha256(args.binary) == binding['binary_sha256']
    tr.service_binary_build_provenance(args.binary, tr.repository_commit())
    for port in (17220, 17221, 17222):
        with socket.socket() as check:
            check.bind(('127.0.0.1', port))

    full = [i for i, (_, _, rows) in enumerate(batches) if rows == 256]
    windows = {'early': full[:32], 'late': full[-32:]}
    assert all(indices == list(range(indices[0], indices[0] + 32)) for indices in windows.values())
    assert windows['early'][-1] < windows['late'][0]
    result = {'kind': 'bounded_native_initial_ingest_batch_phase_attribution', 'qualifying': False,
              'source_commit': tr.repository_commit(), 'binary_sha256': tr.file_sha256(args.binary),
              'script_sha256': tr.file_sha256(Path(__file__)),
              'manifest_sha256': tr.file_sha256(args.manifest),
              'serving_sha256': tr.file_sha256(args.serving),
              'affinity': sorted(os.sched_getaffinity(0)), 'batch_size': 256,
              'python_inputs_sha256': python_inputs(), 'python_executable': str(Path(sys.executable).resolve()),
              'profile_seconds': 8, 'build_profile': args.build_profile, 'profile_rates': {'block': 0, 'mutex': 0},
              'expected_batches': len(batches), 'expected_rows': sum(x[2] for x in batches),
              'window_batch_indices_zero_based': windows, 'windows': [], 'failures': [],
              'completed_batches': 0, 'acknowledged_rows': 0, 'batch_stats_records': 0,
              'batch_stats_path': str(args.output / 'batch-stats.jsonl'),
              'binding_sha256': tr.file_sha256(args.binding),
              'notes': ['CPU profiles include idle remainder and profiler/observer work; not throughput denominators',
                        'Allocation endpoints are process TotalAlloc/Mallocs and include concurrent runtime work',
                        'Allocation profiles are sampled and GC-lagged; no forced GC/checkpoint/Build/search',
                        'Existing stats are read after every acknowledged batch, outside request timer; observer and background effects prevent throughput acceptance',
                        'Nested timing fields and background durations are not additive foreground phases']}
    controller = runner = None
    batch_stats_file = (args.output / 'batch-stats.jsonl').open('x')

    def save():
        (args.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')

    def work():
        observed = controller.work_snapshot()
        assert observed['availability'] == 'measured', observed
        w = observed['work']
        assert w['pid'] == controller.pid
        assert not any(w['indexed_json'].values()) and not any(w['runtime'].values()), w
        return observed

    def profile(path, endpoint, timeout=20):
        entry = {'path': str(path), 'endpoint': endpoint, 'started_monotonic_ns': time.monotonic_ns()}
        body = controller._read_bounded(controller.diagnostics_url + endpoint, timeout, 64 << 20)
        path.write_bytes(body)
        entry.update(ended_monotonic_ns=time.monotonic_ns(), bytes=len(body), sha256=tr.file_sha256(path))
        return entry

    def write_batch(index):
        scenario, start, rows = batches[index]
        documents = [tr.common.generated_document(runner.specs[scenario], ordinal)
                     for ordinal in range(start, start + rows)]
        runner.upsert('initial_batch_insert', scenario, documents, wait_ready=False)
        result['completed_batches'] += 1
        result['acknowledged_rows'] += rows
        observer_started = time.monotonic_ns()
        observed = controller.stats_snapshot()
        observer_ended = time.monotonic_ns()
        validate_batch_stats(observed, controller.pid, runner.collection, runner.index_info.generation, rows)
        record = {'index': index, 'scenario': scenario, 'rows': rows,
                  'preexisting_rows': result['acknowledged_rows'] - rows,
                  'request': runner.evidence.requests[-1],
                  'observer_started_monotonic_ns': observer_started,
                  'observer_ended_monotonic_ns': observer_ended, 'stats': observed}
        batch_stats_file.write(json.dumps(record, separators=(',', ':')) + '\n')
        result['batch_stats_records'] += 1

    def capture_window(name, indices):
        window = {'name': name, 'batch_indices_zero_based': indices,
                  'preexisting_rows': result['acknowledged_rows'], 'rows': 32 * 256}
        result['windows'].append(window)
        window['alloc_before'] = profile(args.output / (name + '-alloc-before.pprof'), '/debug/pprof/allocs')
        with ThreadPoolExecutor(max_workers=1) as pool:
            cpu = pool.submit(profile, args.output / (name + '-cpu.pprof'), '/debug/pprof/profile?seconds=8')
            time.sleep(.2)
            try:
                controller._read_bounded(controller.diagnostics_url + '/debug/pprof/profile?seconds=1', 3, 4096)
            except HTTPError as exc:
                body = exc.read(4096).decode('utf-8', errors='replace')
                window['active_profile_probe'] = {'status': exc.code, 'body': body,
                    'observed_monotonic_ns': time.monotonic_ns()}
                assert exc.code == 500 and 'profiling already in use' in body, window
            else:
                raise RuntimeError('CPU profiler was not active before the write window')
            window['before_work'] = work()
            sequence = len(runner.evidence.requests)
            window['write_started_monotonic_ns'] = time.monotonic_ns()
            for index in indices:
                assert not cpu.done(), 'write window exceeded fixed CPU profile'
                write_batch(index)
            window['write_ended_monotonic_ns'] = time.monotonic_ns()
            assert not cpu.done(), 'last write exceeded fixed CPU profile'
            window['after_work'] = work()
            window['requests'] = runner.evidence.requests[sequence:]
            assert len(window['requests']) == 32
            assert all(r['outcome'] == 'success' and r['requested_count'] == r['result_count'] == 256
                       and r['transport'] == 'native' for r in window['requests'])
            window['cpu'] = cpu.result()
        window['alloc_after'] = profile(args.output / (name + '-alloc-after.pprof'), '/debug/pprof/allocs')
        window['after_profile_work'] = work()
        before, after = (window[key]['work']['memory'] for key in ('before_work', 'after_work'))
        window['allocation_delta'] = {key: after[key] - before[key] for key in ('total_alloc', 'mallocs')}
        assert all(value >= 0 for value in window['allocation_delta'].values())
        print(json.dumps({'window': name, 'preexisting_rows': window['preexisting_rows'],
              'write_seconds': (window['write_ended_monotonic_ns'] - window['write_started_monotonic_ns']) / 1e9,
              'allocation_delta': window['allocation_delta']}), flush=True)
        save()

    def capture_build():
        capture = {'profile_seconds': 60, 'notes': [
            'CPU contains Build plus idle remainder; exact request duration is separate.',
            'Stage times are nested; Build response also includes serving admission and cache priming.',
            'Allocation profiles are sampled and GC-lagged; no forced GC.']}
        result['build'] = capture
        capture['alloc_before'] = profile(args.output / 'build-alloc-before.pprof', '/debug/pprof/allocs')
        with ThreadPoolExecutor(max_workers=1) as pool:
            cpu = pool.submit(profile, args.output / 'build-cpu.pprof', '/debug/pprof/profile?seconds=60', 70)
            time.sleep(.2)
            try:
                controller._read_bounded(controller.diagnostics_url + '/debug/pprof/profile?seconds=1', 3, 4096)
            except HTTPError as exc:
                text = exc.read(4096).decode('utf-8', errors='replace')
                capture['active_profile_probe'] = {'status': exc.code, 'body': text,
                    'observed_monotonic_ns': time.monotonic_ns()}
                assert exc.code == 500 and 'profiling already in use' in text
            else:
                raise RuntimeError('CPU profiler was not active before Build')
            capture['before_work'] = work()
            sequence = len(runner.evidence.requests)
            assert not cpu.done(), 'Build started after CPU profile ended'
            response = runner.initial_load_to_query_boundary()
            assert not cpu.done(), 'Build exceeded fixed CPU profile'
            capture['after_work'] = work()
            capture['requests'] = runner.evidence.requests[sequence:]
            request, = capture['requests']
            assert request['operation_name'] == 'column_graph_initial_build' and request['outcome'] == 'success'
            capture['response'] = asdict(response)
            assert response.status.strategy == 'column_graph'
            assert response.status.column_graph_build.total_nanos > 0
            capture['cpu'] = cpu.result()
        capture['alloc_after'] = profile(args.output / 'build-alloc-after.pprof', '/debug/pprof/allocs')
        capture['after_profile_work'] = work()
        before, after = (capture[key]['work']['memory'] for key in ('before_work', 'after_work'))
        capture['allocation_delta'] = {key: after[key] - before[key] for key in ('total_alloc', 'mallocs')}
        assert all(value >= 0 for value in capture['allocation_delta'].values())
        capture['public_count'] = runner.client.count_documents(runner.collection).count
        assert capture['public_count'] == result['expected_rows'] and runner._graph_built
        validate_build_capture(capture, controller.pid, result['initial_work']['work']['origin_unix_nano'], result['expected_rows'])
        print(json.dumps({'build_nanos': request['ended_monotonic_ns'] - request['started_monotonic_ns'],
                          'stages': capture['response']['status']['column_graph_build']}), flush=True)


    def interrupted(signum, frame):
        raise RuntimeError(f'diagnostic interrupted by signal {signum}')

    signal.signal(signal.SIGTERM, interrupted)
    try:
        controller = tr.ServiceController(args.binary, 'http://127.0.0.1:17220', data_dir,
            'command_wal_durable', 120, 120, diagnostics_url='http://127.0.0.1:17221',
            block_profile_rate=0, mutex_profile_fraction=0, native_address='127.0.0.1:17222', measured=True)
        controller.log_path = args.output / 'service.log'
        runner = tr.TreeDBMinimaRunner(manifest, controller=controller,
            collection='minima_harness_cli_treedb', operation_timeout=120, ef_search=2048,
            strategy='column_graph', transport='native', column_graph_serving=json.loads(args.serving.read_text()))
        result['pid'] = controller.pid
        result['linux_process_identity'] = tr.common.linux_process_identity(controller.pid)
        result['server_affinity'] = sorted(os.sched_getaffinity(controller.pid))
        validate_affinity(result['affinity'], result['server_affinity'])
        runner.connect()
        runner.measured = True
        runner.evidence.request_context = runner._request_context
        runner.create_owned_collection()
        runner._phase_name = 'initial_durable_load'
        result['initial_work'] = work()
        result['initial_stats'] = controller.stats_snapshot()
        assert result['initial_stats']['status'] == 'captured'
        result['effective_collection'] = runner.effective_collection
        result['ingest_started_monotonic_ns'] = time.monotonic_ns()
        window_starts = {indices[0]: (name, indices) for name, indices in windows.items()}
        index = 0
        while index < len(batches):
            if index in window_starts:
                name, indices = window_starts[index]
                capture_window(name, indices)
                index += len(indices)
            else:
                write_batch(index)
                index += 1
            if index % 100 == 0:
                print(json.dumps({'completed_batches': index, 'rows': result['acknowledged_rows']}), flush=True)
        result['ingest_ended_monotonic_ns'] = time.monotonic_ns()
        result['final_work'] = work()
        result['public_count'] = runner.client.count_documents(runner.collection).count
        assert result['public_count'] == result['expected_rows'] == result['acknowledged_rows']
        assert result['completed_batches'] == result['batch_stats_records'] == result['expected_batches']
        assert not runner._graph_built
        assert not any(r['operation'] == 'search' or 'build' in r['operation_name'] for r in runner.evidence.requests)
        if args.build_profile:
            capture_build()
        result['completed'] = True
    except BaseException as exc:
        result['failures'].append(f'{type(exc).__name__}: {exc}')
        traceback.print_exc()
    finally:
        batch_stats_file.close()
        result['batch_stats_sha256'] = tr.file_sha256(args.output / 'batch-stats.jsonl')
        if runner is not None:
            result['requests'] = runner.evidence.requests
            result['samples'] = runner.evidence.samples
        for cleanup in ((runner.close,) if runner is not None else ()) + ((controller.stop,) if controller else ()):
            try:
                cleanup()
            except BaseException as exc:
                result['failures'].append(f'cleanup: {type(exc).__name__}: {exc}')
        if controller:
            result['lifetimes'] = controller.lifetimes
        try:
            loaded = python_inputs()
            for path, digest in result['python_inputs_sha256'].items():
                assert tr.file_sha256(Path(path)) == digest, path
            result['python_inputs_sha256'].update(loaded)
        except BaseException as exc:
            result['failures'].append(f'Python provenance: {type(exc).__name__}: {exc}')
        save()
    return bool(result['failures'])


if __name__ == '__main__':
    raise SystemExit(main())
