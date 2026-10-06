#!/usr/bin/env python3
"""Fail closed on standalone lifecycle provenance, raw runs and denominators."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

COMPONENTS = {'index', 'persistent_vlog', 'persistent_leaf_log', 'typed_assets', 'redo_wal', 'other'}
OPERATIONS = {'ordinary_get', 'prepared_get', 'indexed_update', 'typed_replace', 'delete', 'typed_insert', 'typed_upsert', 'post_upsert_get'}
METRICS = {'ns/op', 'B/op', 'allocs/op', 'calls/op', 'loop-ns/call', 'mixed-calls/s',
           'mixed-p95-ns/call', 'mixed-p99-ns/call', 'loop-B/call', 'loop-allocs/call',
           'sampled-heap-high-B', 'process-retained-heap-B'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def hexadecimal(value, length=64):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value) is not None


def manifest_hash(manifest):
    digest = hashlib.sha256()
    for path, value in sorted(manifest.items()):
        digest.update(path.encode() + b'\0' + value.encode() + b'\0')
    return digest.hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f'duplicate JSON key: {key}')
        result[key] = value
    return result


def decode(text):
    return json.loads(text, object_pairs_hook=unique_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f'nonfinite JSON {value}')))


def source_valid(source):
    require(hexadecimal(source['commit'], 40) and type(source['clean']) is bool, 'malformed source identity')
    for key, width, binding in (('runtime_blobs', 40, 'runtime_sha256'),
                                ('runtime_files', 64, 'runtime_working_sha256'),
                                ('harness_files', 64, 'harness_sha256')):
        files = source[key]
        require(type(files) is dict and bool(files), f'missing {key}')
        for path, identity in files.items():
            require(isinstance(path, str) and not Path(path).is_absolute() and '..' not in Path(path).parts
                    and hexadecimal(identity, width), f'malformed {key} path/blob')
        require(hexadecimal(source[binding]) and manifest_hash(files) == source[binding], f'{binding} manifest mismatch')
    require(source['runtime_files'].keys() == source['runtime_blobs'].keys(), 'runtime inventories differ')
    tests = source['compiled_test_files']
    require(isinstance(tests, list) and tests == sorted(set(tests)) and bool(tests), 'malformed compiled test inventory')
    require(all(path.startswith('TreeDB/collections/') and path.endswith('_test.go') for path in tests), 'wrong test package')
    require(set(tests) == {path for path in source['harness_files'] if path.endswith('_test.go')}, 'unbound compiled tests')
    require('TreeDB/collections/r1_lifecycle_5060_bench_test.go' in tests, 'missing benchmark source')
    require({'scripts/r1_lifecycle_capture.sh', 'scripts/r1_lifecycle_capture.py',
             'scripts/r1_lifecycle_validate.py', 'scripts/r1_lifecycle_validate_test.py',
             'scripts/r1_collection_source.py'} <= source['harness_files'].keys(), 'missing capture/helper source')


def result_valid(result, config):
    require(result['schema'] == 'gomap-r1-lifecycle-result-v1', 'wrong result schema')
    require(integer(result['pid'], 1) and integer(result['gomaxprocs'], 1), 'missing actual process/concurrency identity')
    epochs = result['epochs']
    require(integer(epochs, 1) and epochs in (1, config['epochs']), 'unexpected calibration epochs')
    require(result['documents'] == config['documents'] and result['calls_per_epoch'] == config['calls_per_epoch'], 'fixture dimensions differ')
    require(hexadecimal(result['fixture_sha256']), 'malformed fixture hash')
    total = epochs * config['calls_per_epoch']
    require(integer(result['total_calls'], 1) and result['total_calls'] == total, 'wrong logical call denominator')
    for key in ('call_ns', 'loop_bytes', 'loop_allocs', 'p95_ns', 'p99_ns', 'sampled_heap_high_bytes', 'process_retained_heap_bytes'):
        require(integer(result[key], 1), f'missing or invalid {key}')
    require(result['p95_ns'] <= result['p99_ns'] <= result['call_ns'], 'invalid latency ordering')
    require(set(result['operations']) == OPERATIONS and all(integer(value, 1) and value == total // 8
            for value in result['operations'].values()), 'wrong actual operation mix/denominators')
    phases = ['ingest']
    for epoch in range(epochs):
        phases += [f'churn-{epoch}', f'checkpoint-{epoch}', f'maintenance-{epoch}']
        if epoch == 0:
            phases.append('after_view_release')
    phases.append('reopen')
    require([row['phase'] for row in result['census']] == phases, 'missing/duplicate/out-of-order storage census')
    for row in result['census']:
        for key in ('logical_bytes', 'regular_files'):
            require(set(row[key]) == COMPONENTS and all(integer(n) for n in row[key].values()), f'invalid {key} components')
        for key in ('heap_alloc', 'heap_inuse', 'heap_objects', 'num_gc'):
            require(integer(row[key]), f'invalid sampled {key}')
    require(len(result['maintenance']) == epochs, 'missing maintenance metrics')
    for epoch, row in enumerate(result['maintenance']):
        require(row['epoch'] == epoch and integer(row['checkpoint_ns'], 1) and integer(row['maintenance_ns'], 1), 'invalid maintenance timer')
        require(type(row['overlay']) is dict and bool(row['overlay']), 'missing actual overlay result')
        for key in ('typed_deleted_segments', 'typed_retained_bytes', 'typed_rewrite_debt_bytes',
                    'vlog_deleted_segments', 'vlog_pending_segments', 'vlog_retained_bytes'):
            require(integer(row[key]), f'invalid maintenance {key}')


def raw_results(text, config, pid):
    require('\nPASS\n' in '\n' + text and 'FAIL' not in text and 'panic:' not in text, 'failed or incomplete raw benchmark')
    results, metrics = [], []
    for line in text.splitlines():
        if 'R1_LIFECYCLE_RESULT ' in line:
            result = decode(line.split('R1_LIFECYCLE_RESULT ', 1)[1])
            result_valid(result, config)
            require(result['pid'] == pid, 'raw output/process PID mismatch')
            results.append(result)
        match = re.match(r'^BenchmarkR1Lifecycle5060(?:-\d+)?\s+(\d+)\s+(.+)$', line)
        if match:
            tokens = match.group(2).split()
            require(len(tokens) % 2 == 0, 'malformed benchmark metrics')
            values = {}
            for value, unit in zip(tokens[::2], tokens[1::2]):
                require(unit not in values, 'duplicate benchmark metric')
                number = float(value)
                require(math.isfinite(number) and number >= 0, 'nonfinite/negative benchmark metric')
                values[unit] = number
            require(set(values) == METRICS, 'missing or unexpected benchmark metrics')
            require(int(match.group(1)) == config['epochs'], 'benchmark epoch denominator differs')
            metrics.append(values)
    expected_epochs = [1] if config['epochs'] == 1 else [1, config['epochs']]
    require([result['epochs'] for result in results] == expected_epochs, 'missing/duplicate calibration or final result')
    require(len(metrics) == 1, 'missing/duplicate final benchmark metrics')
    final, values = results[-1], metrics[0]
    exact = {'calls/op': final['calls_per_epoch'], 'loop-ns/call': final['call_ns'] / final['total_calls'],
             'mixed-calls/s': final['total_calls'] * 1e9 / final['call_ns'],
             'mixed-p95-ns/call': final['p95_ns'], 'mixed-p99-ns/call': final['p99_ns'],
             'loop-B/call': final['loop_bytes'] / final['total_calls'],
             'loop-allocs/call': final['loop_allocs'] / final['total_calls'],
             'sampled-heap-high-B': final['sampled_heap_high_bytes'],
             'process-retained-heap-B': final['process_retained_heap_bytes']}
    require(all(math.isclose(values[key], n, rel_tol=1e-5, abs_tol=0.51) for key, n in exact.items()), 'Go metrics disagree with actual counts/results')
    require(values['ns/op'] > 0 and values['B/op'] > 0 and values['allocs/op'] > 0, 'invalid epoch metrics')
    return {'result': final, 'go_metrics': values, 'calibration_epochs': expected_epochs[:-1]}


def validate(path, expected_runtime=None, expected_harness=None, expected_commit=None):
    path = Path(path)
    packet = decode(path.read_text())
    require(packet['schema'] == 'gomap-r1-lifecycle-packet-v1', 'wrong packet schema')
    config = packet['config']
    require(config['qualification'] in ('rehearsal', 'retained'), 'wrong qualification')
    require(integer(config['repetitions'], 1) and config['repetitions'] <= 100, 'invalid repetitions')
    require(integer(config['epochs'], 1) and config['epochs'] <= 1000, 'invalid epochs')
    for key, multiple in (('documents', 32), ('calls_per_epoch', 8)):
        require(integer(config[key], multiple) and config[key] % multiple == 0 and config[key] <= 1 << 20, 'invalid fixture dimensions')
    before = packet['source_before']
    source_valid(before)
    source_valid(packet['source_after'])
    require(before == packet['source_after'], 'source changed during capture')
    for key, expected in (('runtime_sha256', expected_runtime), ('harness_sha256', expected_harness), ('commit', expected_commit)):
        require(expected is None or before[key] == expected, f'frozen {key} mismatch')
    if config['qualification'] == 'retained':
        require(before['clean'] and config['repetitions'] >= 5 and config['epochs'] == 5
                and config['documents'] == 4096 and config['calls_per_epoch'] == 1024, 'weakened retained fixture/source gate')
        require(hexadecimal(config['landed_tooling_commit'], 40) and isinstance(config['review_url'], str)
                and config['review_url'].startswith('https://github.com/'), 'missing review/landing declaration')
    toolchain = packet['toolchain']
    require(toolchain['go'].startswith('go version go1.') and bool(toolchain['cc']) and bool(toolchain['uname']['system'])
            and bool(toolchain['binary_buildinfo']) and bool(toolchain['filesystem']), 'missing actual build/host identity')
    require(toolchain['go_env']['GOWORK'] == 'off' and toolchain['go_env']['GOTOOLCHAIN'] == 'local', 'unfrozen toolchain/workspace')
    require(hashlib.sha256((path.parent / 'collections.test').read_bytes()).hexdigest() == toolchain['binary_sha256'], 'binary hash mismatch')
    require(hashlib.sha256((path.parent / 'build.log').read_bytes()).hexdigest() == packet['build_log_sha256'], 'build log hash mismatch')
    invocation = packet['invocation']
    require(invocation[1:] == ['-test.run=^$', '-test.bench=^BenchmarkR1Lifecycle5060$',
            f"-test.benchtime={config['epochs']}x", '-test.count=1', '-test.benchmem', '-test.v'], 'mislabeled benchmark command')
    require(len(packet['runs']) == config['repetitions'], 'missing fresh process runs')
    require(len({record['pid'] for record in packet['runs']}) == config['repetitions']
            and all(integer(record['pid'], 1) for record in packet['runs']), 'missing/duplicate fresh process IDs')
    results = []
    for repetition, record in enumerate(packet['runs'], 1):
        require(record['repetition'] == repetition and record['log'] == f'run-{repetition:03d}.log', 'duplicate/wrong raw run')
        require(type(record['exit_code']) is int and record['exit_code'] == 0, 'failed process')
        for event in (record['before'], record['after']):
            require(isinstance(event['utc'], str) and bool(event['utc']) and integer(event['monotonic_ns'], 1)
                    and integer(event['cpu_count'], 1) and len(event['loadavg']) == 3
                    and all(type(n) in (int, float) and math.isfinite(n) and n >= 0 for n in event['loadavg']), 'missing actual load/environment observation')
        require(record['after']['monotonic_ns'] > record['before']['monotonic_ns'], 'invalid process interval')
        raw = (path.parent / record['log']).read_bytes()
        require(hashlib.sha256(raw).hexdigest() == record['log_sha256'], 'raw log hash mismatch')
        results.append(raw_results(raw.decode(), config, record['pid']))
    require(len({row['result']['fixture_sha256'] for row in results}) == 1, 'fixture drift between fresh processes')
    return results


def summarize(packet, results):
    text = ['# Standalone R1 lifecycle diagnostic', '',
            f"Qualification: **{packet['config']['qualification']}**. No SQLite or cross-fixture speed comparison.",
            f"Source `{packet['source_before']['commit']}`; runtime `{packet['source_before']['runtime_sha256']}`; harness `{packet['source_before']['harness_sha256']}`.",
            f"{len(results)} fresh processes; {packet['config']['epochs']} final epochs/process; {packet['config']['documents']} live rows; {packet['config']['calls_per_epoch']} calls/epoch.", '',
            '| Final metric | Median | Minimum | Maximum | max/min |', '| --- | ---: | ---: | ---: | ---: |']
    for key in sorted(METRICS):
        values = [row['go_metrics'][key] for row in results]
        text.append(f'| {key} | {statistics.median(values):.6g} | {min(values):.6g} | {max(values):.6g} | {max(values)/min(values) if min(values) else 0:.4g} |')
    text += ['', 'Logical storage medians across final process results:', '',
             '| Phase | index | vlog | leaf log | typed assets | redo WAL | other | all bytes | regular files | growth from ingest |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    components = ['index', 'persistent_vlog', 'persistent_leaf_log', 'typed_assets', 'redo_wal', 'other']
    for index, phase in enumerate(results[0]['result']['census']):
        rows = [row['result']['census'][index] for row in results]
        cells = [statistics.median(row['logical_bytes'][key] for row in rows) for key in components]
        totals = [sum(row['logical_bytes'].values()) for row in rows]
        files = [sum(row['regular_files'].values()) for row in rows]
        growth = [sum(row['result']['census'][index]['logical_bytes'].values()) -
                  sum(row['result']['census'][0]['logical_bytes'].values()) for row in results]
        cells += [statistics.median(totals), statistics.median(files), statistics.median(growth)]
        text.append('| ' + phase['phase'] + ' | ' + ' | '.join(f'{n:.6g}' for n in cells) + ' |')
    text += ['', 'Maintenance medians (zero deleted segments remain zero):', '',
             '| Epoch | checkpoint ns | maintenance ns | typed deleted | typed retained bytes | typed rewrite debt | vlog deleted | vlog pending | vlog retained bytes |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for epoch in range(packet['config']['epochs']):
        keys = ['checkpoint_ns', 'maintenance_ns', 'typed_deleted_segments', 'typed_retained_bytes',
                'typed_rewrite_debt_bytes', 'vlog_deleted_segments', 'vlog_pending_segments', 'vlog_retained_bytes']
        cells = [statistics.median(row['result']['maintenance'][epoch][key] for row in results) for key in keys]
        text.append('| ' + str(epoch) + ' | ' + ' | '.join(f'{n:.6g}' for n in cells) + ' |')
    text += ['', 'Go calibration results remain in raw logs and are excluded from this table.',
             'Call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics also include ID preparation and latency/count bookkeeping. Maintenance and phase oracles are excluded.',
             'Heap high is sampled at epoch boundaries; retained heap after GC includes live oracle maps and latency samples. RSS and unsampled peak are unavailable.',
             'Component census uses logical file lengths, including redo WAL separately; no-op maintenance is recorded without a reclamation claim.',
             'Raw logs and packet retain per-epoch checkpoint/maintenance/debt/component observations and actual host load. Spread is descriptive; this diagnostic supplies no automatic performance acceptance threshold.', '']
    return '\n'.join(text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet')
    parser.add_argument('--expected-runtime')
    parser.add_argument('--expected-harness')
    parser.add_argument('--expected-commit')
    args = parser.parse_args()
    rows = validate(args.packet, args.expected_runtime, args.expected_harness, args.expected_commit)
    print(summarize(decode(Path(args.packet).read_text()), rows))
