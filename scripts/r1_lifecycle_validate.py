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
SCOPE = {'backend': 'direct', 'profile': 'command_wal_durable', 'cached_wrapper': False,
         'command_wal': True, 'disable_background_prune': True}
SCHEDULE = 'direct-backend-fold-rewrite-vacuum-v1'


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


def working_set(config):
    ids = sorted({f"row-{pair * 37 % config['documents']:03d}" for pair in range(config['calls_per_epoch'] // 2)})
    return {'policy': 'repeat-stride-37-each-epoch', 'stride': 37,
            'nominal_limit': min(config['documents'], config['calls_per_epoch'] // 2),
            'distinct_ids_per_epoch': len(ids),
            'expected_distinct_ids_total': len(ids),
            'expected_cross_epoch_revisited_ids': len(ids) * (config['epochs'] - 1),
            'expected_revisited_distinct_ids': len(ids) if config['epochs'] > 1 else 0,
            'ids_sha256': hashlib.sha256(('\n'.join(ids) + '\n').encode()).hexdigest()}


def counters_valid(values, required, label):
    require(type(values) is dict and set(required) <= values.keys(), f'missing {label} attribution')
    require(all(integer(value) for value in values.values()), f'malformed {label} counters')


def typed_gc_valid(stats):
    require(stats['DryRun'] is False, 'typed GC reported dry run as completed work')
    counters_valid({key: value for key, value in stats.items() if key not in ('DryRun', 'Plan')},
                   ('SegmentsEligible', 'SegmentsDeleted', 'SegmentsRetained', 'BytesEligible', 'BytesDeleted', 'BytesRetained'), 'typed GC')
    require(stats['SegmentsDeleted'] <= stats['SegmentsEligible'] and stats['BytesDeleted'] <= stats['BytesEligible'], 'typed deletion exceeds eligibility')
    reachability_valid(stats['Plan'])


def reachability_valid(plan):
    require(plan['Complete'] is True and type(plan['ProtectOnly']) is bool, 'incomplete typed reachability plan')
    require(plan['Entries'] is None and plan['SegmentEntries'] is None, 'unbounded per-reference/segment packet detail')
    require(integer(plan['RewriteDebtBytes']), 'missing rewrite debt')
    counters_valid(plan['Sources'], ('ManifestRoots', 'ManifestRecords', 'ActiveManifestRefs', 'RecoveryManifestRefs',
                    'CandidateRefs', 'PendingRefs', 'PreparedRefs', 'PreparedQueryRefs', 'QuarantineRefs',
                    'QuarantineSegmentRecords', 'PinnedRefs', 'MappedResourcePins', 'ActiveManifestBytes',
                    'RecoveryManifestBytes', 'CandidateBytes', 'PendingBytes', 'PreparedBytes', 'PreparedQueryBytes',
                    'QuarantineBytes', 'QuarantineSegmentBytes', 'PinnedBytes', 'MappedResourcePinBytes'), 'typed reachability sources')
    counters_valid(plan['Refs'], ('Total', 'Protected', 'Reclaimable', 'Uncertain', 'BytesTotal', 'BytesProtected', 'BytesReclaimable', 'BytesUncertain'), 'typed refs')
    counters_valid(plan['Segments'], ('Total', 'Protected', 'Reclaimable', 'Mixed', 'Unknown', 'Missing',
                    'OutOfBoundsRefs', 'QuarantineSegments', 'QuarantineSegmentMismatches', 'BytesTotal',
                    'BytesProtected', 'BytesReclaimable', 'BytesWholeReclaimable', 'BytesUnknown', 'BytesQuarantined'), 'typed segments')
    counters_valid(plan['MappedResources'], ('ActiveHandles', 'ActiveMappedBytes', 'ActiveHeapCopyBytes',
                    'ActiveDerivedMetadataBytes', 'PinnedRefs', 'PinnedBytes', 'UnconvertiblePins', 'DeniedResources', 'FallbackReads'), 'mapped resources')


def vlog_gc_valid(stats):
    classes = ('Total', 'Referenced', 'Active', 'Protected', 'ProtectedInUse', 'ProtectedRetained',
               'ProtectedOverlap', 'ProtectedOther', 'Eligible', 'Deleted', 'Pending')
    required = [prefix + suffix for prefix in ('Segments', 'Bytes') for suffix in classes]
    counters_valid(stats, required, 'value-log GC')
    require(stats['SegmentsDeleted'] <= stats['SegmentsEligible'] and stats['BytesDeleted'] <= stats['BytesEligible'], 'value-log deletion exceeds eligibility')


def rewrite_valid(stats, dry_run):
    require(stats['DryRun'] is dry_run, 'mislabeled rewrite probe/work')
    reachability_valid(stats['Plan'])
    require(stats['SupersededRefs'] is None and stats['RemappedRefs'] is None, 'unbounded rewrite ref detail')
    counters_valid({key: value for key, value in stats.items() if key not in ('DryRun', 'Plan', 'SupersededRefs', 'RemappedRefs')},
                   ('SegmentsEligible', 'SegmentsRewritten', 'RefsEligible', 'RefsRemapped', 'BytesCopied', 'BytesReclaimable', 'BytesRetained'), 'rewrite')
    require(stats['SegmentsRewritten'] <= stats['SegmentsEligible'] and stats['RefsRemapped'] <= stats['RefsEligible'], 'rewrite work exceeds eligibility')
    if dry_run:
        require(stats['SegmentsRewritten'] == stats['RefsRemapped'] == 0, 'dry-run counted as remapped work')


def reclaim_valid(row):
    for key in ('plan_ns', 'gc_ns'):
        require(integer(row[key], 1), f'missing reclaim {key}')
    for key in ('probe_ns', 'rewrite_ns', 'checkpoint_ns', 'candidate_refs'):
        require(integer(row[key]), f'invalid reclaim {key}')
    typed_gc_valid(row['plan_gc'])
    typed_gc_valid(row['typed_gc'])
    debt = row['plan_gc']['Plan']['RewriteDebtBytes']
    if debt == 0:
        require(row['decision'] == 'no_debt' and row['probe'] is None and row['rewrite'] is None
                and row['probe_ns'] == row['rewrite_ns'] == row['checkpoint_ns'] == 0, 'rewrite without selected debt')
    else:
        require(row['probe_ns'] > 0, 'missing debt eligibility probe')
        rewrite_valid(row['probe'], True)
        eligible = row['probe']['SegmentsEligible'] > 0 and row['probe']['RefsEligible'] > 0
        if eligible:
            require(row['decision'] == 'eligible' and row['rewrite_ns'] > 0 and row['checkpoint_ns'] > 0, 'eligible rewrite missing work/checkpoint')
            rewrite_valid(row['rewrite'], False)
        else:
            require(row['decision'] == 'protected_or_ineligible' and row['rewrite'] is None
                    and row['rewrite_ns'] == row['checkpoint_ns'] == 0, 'protected rewrite counted as work')


def maintenance_valid(row, documents):
    required_timers = ('flush_ns', 'checkpoint_ns', 'before_fold_gc_ns', 'fold_ns',
                       'fold_checkpoint_ns', 'overlay_ns', 'overlay_checkpoint_ns', 'vlog_gc_ns', 'vacuum_ns')
    require(all(integer(row[key], 1) for key in required_timers), 'missing actual maintenance API timer')
    typed_gc_valid(row['before_fold_gc'])
    fold = row['fold']
    stats = fold['stats']
    require(type(stats['Compacted']) is bool and stats['PublishedRefs'] is None
            and stats['SupersededRefs'] is None, 'missing logical fold or unbounded refs')
    counters_valid({key: value for key, value in stats.items() if key not in ('Compacted', 'PublishedRefs', 'SupersededRefs')},
                   ('PreviousGeneration', 'NewGeneration', 'ManifestRecordsBefore', 'ManifestRecordsAfter',
                    'MutationPartsBefore', 'MutationPartsAfter', 'RowsScanned', 'DeletedRows', 'RowsCompacted', 'PhysicalBytesRead', 'AssetsPublished'), 'logical fold')
    require(integer(fold['published_refs']) and integer(fold['superseded_refs']), 'missing folded ref counts')
    # The selected paired mutation workload creates history every epoch.
    require(stats['Compacted'] and stats['RowsCompacted'] == documents and stats['MutationPartsBefore'] > 0
            and stats['MutationPartsAfter'] == 0 and stats['NewGeneration'] > stats['PreviousGeneration']
            and fold['published_refs'] > 0 and fold['superseded_refs'] > 0, 'logical history not folded for complete live population')
    require(type(row['overlay']) is dict and bool(row['overlay']), 'missing actual overlay result')
    reclaim_valid(row['reclaim'])
    require(row['reclaim']['plan_gc']['Plan']['Sources']['ActiveManifestRefs'] < row['before_fold_gc']['Plan']['Sources']['ActiveManifestRefs'], 'fold did not reset active manifest lineage')
    vlog_gc_valid(row['vlog_gc'])
    vacuum = row['vacuum']
    require(vacuum['WorkCompleted'] is True and vacuum['Canceled'] is False and bool(vacuum['Phase']), 'vacuum did not complete')
    counters_valid({key: value for key, value in vacuum.items() if type(value) is not str and type(value) is not bool},
                   ('AttemptID', 'TotalDuration', 'RecoverableRoots', 'ReplacementPagerPages'), 'vacuum')
    elapsed = sum(row[key] for key in required_timers)
    elapsed += sum(row['reclaim'][key] for key in ('plan_ns', 'probe_ns', 'rewrite_ns', 'checkpoint_ns', 'gc_ns'))
    require(integer(row['maintenance_ns'], 1) and row['maintenance_ns'] == elapsed, 'maintenance API timer sum mismatch')


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
    require(result['schema'] == 'gomap-r1-lifecycle-result-v2', 'wrong result schema')
    require(integer(result['pid'], 1) and integer(result['gomaxprocs'], 1), 'missing actual process/concurrency identity')
    require(result['schedule'] == SCHEDULE and result['backend_profile'] == SCOPE['profile'], 'mislabeled execution scope/schedule')
    require(result['gomaxprocs'] == int(config['runtime_environment']['GOMAXPROCS']), 'actual process concurrency differs')
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
    selected = working_set(config)
    distinct = selected['distinct_ids_per_epoch']
    require(integer(result['distinct_ids'], 1) and result['distinct_ids'] == distinct
            and integer(result['cross_epoch_revisited_ids']) and result['cross_epoch_revisited_ids'] == distinct * (epochs - 1)
            and integer(result['revisited_distinct_ids']) and result['revisited_distinct_ids'] == (distinct if epochs > 1 else 0), 'result does not repeat bounded working set')
    require(len(result['id_coverage']) == epochs, 'missing actual epoch ID coverage')
    for epoch, coverage in enumerate(result['id_coverage']):
        require(all(integer(coverage[key]) for key in ('epoch', 'distinct_ids', 'new_ids', 'revisited_ids', 'cumulative_distinct_ids')), 'malformed actual ID coverage')
        require(coverage == {'epoch': epoch, 'distinct_ids': distinct, 'new_ids': distinct if epoch == 0 else 0,
                'revisited_ids': 0 if epoch == 0 else distinct, 'cumulative_distinct_ids': distinct,
                'ids_sha256': selected['ids_sha256']}, 'epoch expands or changes bounded working set')
    phases = ['ingest']
    for epoch in range(epochs):
        phases += [f'churn-{epoch}', f'checkpoint-{epoch}', f'folded-{epoch}', f'before_vacuum-{epoch}', f'maintenance-{epoch}']
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
        require(integer(row['epoch']) and row['epoch'] == epoch, 'wrong maintenance epoch')
        maintenance_valid(row, config['documents'])
    reclaim_valid(result['after_view_release_gc'])


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
    # Go prints epoch ns/op rounded; the enclosing epoch timer cannot be shorter
    # than the sum of its measured call intervals. Allow half a printed ns/op.
    require((values['ns/op'] + 0.51) * config['epochs'] >= final['call_ns'], 'epoch timer shorter than actual call timers')
    return {'result': final, 'go_metrics': values, 'calibration_epochs': expected_epochs[:-1]}


def validate(path, expected_runtime=None, expected_harness=None, expected_commit=None):
    path = Path(path)
    packet = decode(path.read_text())
    require(packet['schema'] == 'gomap-r1-lifecycle-packet-v2', 'wrong packet schema')
    config = packet['config']
    require(config['qualification'] in ('rehearsal', 'retained'), 'wrong qualification')
    require(integer(config['repetitions'], 1) and config['repetitions'] <= 100, 'invalid repetitions')
    require(integer(config['epochs'], 1) and config['epochs'] <= 1000, 'invalid epochs')
    for key, multiple in (('documents', 32), ('calls_per_epoch', 8)):
        require(integer(config[key], multiple) and config[key] % multiple == 0 and config[key] <= 1 << 20, 'invalid fixture dimensions')
    require(config['working_set'] == working_set(config), 'mislabeled deterministic working set config')
    require(config['execution_scope'] == SCOPE and config['schedule'] == SCHEDULE, 'mislabeled direct-backend schedule')
    require(config['runtime_environment'] == {'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'GOFLAGS': ''}, 'unfrozen runtime environment')
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
    require(toolchain['go'] == 'go version go1.26.4 linux/amd64' and bool(toolchain['cc']) and bool(toolchain['uname']['system'])
            and bool(toolchain['binary_buildinfo']) and bool(toolchain['filesystem']), 'missing actual build/host identity')
    require(toolchain['go_env']['GOWORK'] == 'off' and toolchain['go_env']['GOTOOLCHAIN'] == 'local', 'unfrozen toolchain/workspace')
    require(toolchain['go_env']['GOVERSION'] == 'go1.26.4' and 'go1.26.4' in toolchain['binary_buildinfo'], 'build toolchain differs from frozen version')
    require(all(toolchain['environment'][key] == value for key, value in config['runtime_environment'].items()), 'caller environment mismatch')
    capture_directory = Path(toolchain['capture_directory'])
    require(capture_directory.is_absolute() and toolchain['benchmark_tmpdir'] == str(capture_directory / 'benchmark-tmp'), 'benchmark temporary directory is not capture-owned')
    require(integer(toolchain['capture_filesystem_device']) and integer(toolchain['benchmark_filesystem_device'])
            and toolchain['benchmark_filesystem_device'] == toolchain['capture_filesystem_device']
            and bool(toolchain['benchmark_filesystem']), 'benchmark/capture filesystem mismatch or missing observation')
    require(toolchain['process_environment'] == {**config['runtime_environment'], 'TMPDIR': toolchain['benchmark_tmpdir'], 'GOWORK': 'off', 'GOTOOLCHAIN': 'local',
            'GOMAP_R1_LIFECYCLE_DOCUMENTS': str(config['documents']),
            'GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH': str(config['calls_per_epoch'])}, 'effective benchmark environment mismatch')
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
            f"{len(results)} fresh processes; {packet['config']['epochs']} final epochs/process; {packet['config']['documents']} live rows; {packet['config']['calls_per_epoch']} calls/epoch.",
            f"Every final epoch revisits the same {packet['config']['working_set']['distinct_ids_per_epoch']} IDs. Full row/posting oracles cover the live population; timed churn covers this bounded working set. This is finite hot-set evidence, not full-population or unlimited-capacity qualification.", '',
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
    text += ['', 'Maintenance API medians (oracles and census excluded):', '',
             '| Epoch | maintenance ns | fold ns | live rows folded | mutation parts before/after | active refs before/after | rewrite decision | typed deleted bytes | vacuum ns | vacuum completed |',
             '| --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |']
    for epoch in range(packet['config']['epochs']):
        rows = [row['result']['maintenance'][epoch] for row in results]
        med = lambda f: f'{statistics.median(f(row) for row in rows):.6g}'
        cells = [str(epoch), med(lambda r: r['maintenance_ns']), med(lambda r: r['fold_ns']), med(lambda r: r['fold']['stats']['RowsCompacted']),
                 med(lambda r: r['fold']['stats']['MutationPartsBefore'])+'/'+med(lambda r: r['fold']['stats']['MutationPartsAfter']),
                 med(lambda r: r['before_fold_gc']['Plan']['Sources']['ActiveManifestRefs'])+'/'+med(lambda r: r['reclaim']['plan_gc']['Plan']['Sources']['ActiveManifestRefs']),
                 ','.join(sorted({r['reclaim']['decision'] for r in rows})),
                 med(lambda r: r['before_fold_gc']['BytesDeleted']+r['reclaim']['plan_gc']['BytesDeleted']+r['reclaim']['typed_gc']['BytesDeleted']),
                 med(lambda r: r['vacuum_ns']), str(all(r['vacuum']['WorkCompleted'] for r in rows))]
        text.append('| '+' | '.join(cells)+' |')
    released = [row['result']['after_view_release_gc'] for row in results]
    text += ['', 'Post-view-release rewrite decisions: '+','.join(sorted({r['decision'] for r in released}))+'. Raw packets retain eligibility, completed remaps, deleted bytes and protected/recovery retention.',
             'Direct-backend command_wal_durable fixture; vacuum runs on the same live backend. Cached-wrapper checkpoint/reconcile overhead is omitted. Logical fold resets lineage; it does not itself prove physical reclamation.']
    text += ['', 'Go calibration results remain in raw logs and are excluded from this table.',
             'Call timers include encoding, full-row decode/oracle and callback bookkeeping. Epoch metrics also include ID preparation and latency/count/visited-ID bookkeeping. Maintenance, coverage aggregation and phase oracles are excluded.',
             'Heap high is sampled at epoch boundaries; retained heap after GC includes live oracle maps and latency samples. RSS and unsampled peak are unavailable.',
             'Component census uses logical file lengths, including redo WAL separately; no-op maintenance is recorded without a reclamation claim.',
             'Full aggregate typed reachability sources/ref/segment/mapped attribution and value-log active/pending/protected/referenced classifications remain separate in the packet. Source classes can overlap; their byte counts are not unique retained bytes. Release GC is recorded; no-op or protected work is not reclamation.',
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
