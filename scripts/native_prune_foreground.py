#!/usr/bin/env python3
"""Fresh-process v2 causal pilot and opt-in retained-v1 duration/fence capture."""
import argparse
import copy
import hashlib
import json
import math
import os
import pathlib
import shutil
import subprocess
import sys
import time

C = 'gomap-native-foreground-v2'
R = 'gomap-native-foreground-retained-v1'
PHASES = ('ReadActive', 'ReadDrain', 'WriteActive', 'QuantumActive', 'QuantumDrain')
POLICY = {'quantile': 'nearest-rank ceil(p*count), p95 and p99', 'minimum_samples': 1000, 'repetitions': 3, 'maximum_quantile_spread_ratio': 1.25, 'coverage': 'ReadActive ReadDrain WriteActive each run; quantum phases descriptive', 'reference': 'unbounded is descriptive; unmatched starting custody'}
FENCE_SITES = ('store.mu', 'cache.prepare.writeMu', 'cache.rotate.writeMu', 'cache.accept.writeMu', 'backend.source.writeMu', 'backend.apply.writeMu', 'backend.accept.writeMu', 'cache.foreground.writeMu', 'backend.writer.writeMu')

MATRIX = [(n, m, 'bounded') for n in (64, 128) for m in ('burst', 'growth', 'churn')] + [(n, 'burst', 'unbounded') for n in (64, 128)]
BUILD_KEYS = ('CGO_ENABLED', 'GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOMAXPROCS', 'GOROOT', 'PATH', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')

def need(x, message):
    if not x:
        raise ValueError(message)

def uint(x):
    return type(x) is int and x >= 0

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def valid_sha(x):
    return type(x) is str and len(x) == 64 and all(c in '0123456789abcdef' for c in x)

def bindings(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file() and '.git' not in p.parts and (p.suffix in ('.go', '.mod', '.sum') or p.name == 'AGENTS.md' or p == root / 'scripts/native_prune_foreground.py')}

def digest(source):
    return hashlib.sha256(json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def validate(x, n, mode, algorithm, retained=False):
    need(type(x['n']) is int and x['n'] == n and (x['schema'], x['Mode'], x['Algorithm']) == (R if retained else C, mode, algorithm), 'identity')
    need(uint(x['PID']) and x['PID'] > 0, 'PID')
    strings = {'schema', 'Mode', 'Algorithm', 'Error', 'WriterStopReason', 'AcceptanceCauseType', 'AcceptanceCauseText'}
    bools = {'ReadersStopWithWriter', 'ForcedBudgetError', 'AcceptanceCausePresent', 'PartialPrivateOutput', 'CompletedWhileWriterActive', 'CompletedAfterStop', 'PhysicalOracle', 'WriterOracle', 'PointerOracle', 'OldReaderOracle', 'ReopenOracle'}
    for k in strings:
        need(type(x[k]) is str, 'string ' + k)
    for k in bools:
        need(type(x[k]) is bool, 'bool ' + k)
    signed = {'LastCOWPhase', 'LastCOWSet', 'LastCOWItem', 'LastCOWAuxiliaryPhase', 'LastCOWPrunePhase', 'LastCOWMaterializerPhase', 'LastCOWBeginPhase', 'LastAcceptancePhase', 'LastNativeRunIndex'}
    for k, v in x.items():
        if k in signed:
            need(type(v) is int, 'signed counter ' + k)
        elif k not in strings | bools | {'ReadLatency', 'WriteLatency', 'QuantumLatency', 'retained'}:
            need(uint(v), 'counter ' + k)
    need(0 < x['ReadsAfterWriterStop'] <= x['Reads'], 'post-writer completed read witness')
    need(x['ReadersStopWithWriter'] is False and x['ForcedBudgetError'] is False, 'continuing-reader measurement only')
    need(not x['Error'] and x['Refusals'] == 0, 'failed/refused work')
    need(x['Pruned'] == n - 1 and x['ACKWhileWriterActive'] + x['ACKAfterWriterStop'] == n - 1, 'ACK accounting')
    need(all(x[k] > 0 for k in ('Calls', 'Reads', 'Writes', 'WriterActiveCalls')), 'foreground progress')
    need(x['ForegroundIntervalsAtQuantumStart'] + x['ReadIntervalsOverlappingQuantum'] + x['WriteIntervalsOverlappingQuantum'] > 0, 'operation interval overlap')
    need(all(x[k] is True for k in ('PhysicalOracle', 'WriterOracle', 'PointerOracle', 'OldReaderOracle', 'ReopenOracle')), 'oracles')
    need(x['CompletedWhileWriterActive'] != x['CompletedAfterStop'], 'completion distinction')
    need(x['Writes'] <= ({'burst': 16, 'growth': 8192, 'churn': 8192} if retained else {'burst': 16, 'growth': 256, 'churn': 1024})[mode], 'write ceiling')
    need(x['WriterStopReason'] in ('finite-burst', 'write-cap', 'observation-duration'), 'writer stop reason')
    if mode == 'burst':
        need(x['Writes'] == 16 and x['WriterStopReason'] == 'finite-burst', 'burst shape')
    if algorithm == 'bounded':
        need(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1 << 20 and x['PartialPrivateOutput'] is True, 'bounded actual private output')
    for name, count in [('ReadLatency', x['Reads']), ('WriteLatency', x['Writes']), ('QuantumLatency', x['Calls'])]:
        h = x[name]
        need(all(uint(h[k]) for k in ('Count', 'TotalNS', 'MaxNS')), 'latency types')
        need(type(h['Buckets']) is list and len(h['Buckets']) == 8 and all(uint(v) for v in h['Buckets']), 'histogram buckets')
        need(h['Count'] == count and h['TotalNS'] >= h['MaxNS'] and sum(h['Buckets']) == count, 'latency counters')

    if retained:
        validate_retained(x)
    else:
        need('retained' not in x, 'v2 pilot has no retained extension')

def metrics(m):
    need(type(m) is dict and m['Scope'] == 'combined_process_events_since_begin_including_ack_reopen_cleanup_close', 'observer scope')
    for k, v in m.items():
        if k not in ('Scope', 'Fences'):
            need(uint(v), 'observer scalar ' + k)
    need(len(m['Fences']) == len(FENCE_SITES), 'fence sites')
    for f, site in zip(m['Fences'], FENCE_SITES):
        need(f['Site'] == site and all(uint(v) for k, v in f.items() if k != 'Site'), 'per-site metric')
        for count, maximum in [('ForegroundAttempts', 'ForegroundWaitMaxNS'), ('MaintenanceHolds', 'MaintenanceHoldMaxNS'), ('WriterAttempts', 'WriterWaitMaxNS')]:
            need(f[count] > 0 or f[maximum] == 0, 'fence count/max consistency')
    need(m['FenceWaitMaxNS'] == max(f['ForegroundWaitMaxNS'] for f in m['Fences']) and m['FenceHoldMaxNS'] == max(f['MaintenanceHoldMaxNS'] for f in m['Fences']), 'aggregate maxima must be maxima')
    need(m['StorageSyncCount'] == sum(m[k + 'Attempts'] for k in ('File', 'Namespace', 'Mmap', 'DurableWrite')), 'physical attempts sum')
    need(all(m[k + 'Failures'] <= m[k + 'Attempts'] for k in ('File', 'Namespace', 'Mmap', 'DurableWrite')), 'physical failures')

def samples(h, joined):
    need(type(h) is dict and type(h['DurationsNS']) is list and len(h['DurationsNS']) <= 65536 and all(uint(v) and v > 0 for v in h['DurationsNS']), 'raw durations')
    need(h['Overflow'] is False and uint(h['FirstStartNS']) and uint(h['LastEndNS']), 'recorder overflow/types')
    if h['DurationsNS']:
        need(h['FirstStartNS'] <= h['LastEndNS'] <= joined and sum(h['DurationsNS']) <= h['LastEndNS'] - h['FirstStartNS'], 'clock/count duration bounds')
    else:
        need(h['FirstStartNS'] == h['LastEndNS'] == 0, 'empty phase interval')
    v = sorted(h['DurationsNS'])
    return {'count': len(v), 'total_ns': sum(v), 'max_ns': max(v, default=0), 'p95_ns': v[math.ceil(.95*len(v))-1] if v else None, 'p99_ns': v[math.ceil(.99*len(v))-1] if v else None}

def validate_retained(x):
    r = x['retained']
    need(type(r) is dict and r['Capacity'] == 65536 and type(r['Clock']) is str and r['Clock'] == 'Go time.Now monotonic duration in nanoseconds', 'recorder contract')
    need(r['PhaseRule'] == 'read start before writerDone closes = active; read start after writerDone closes = drain; quantum active flag sampled at entry; writes active; setup excluded; cleanup after workers join includes oracles checkpoint reopen and final close', 'phase rule')
    for k in ('SetupCalls', 'SetupTotalNS', 'SetupMaxNS', 'WriterStopNS', 'WorkersJoinedNS', 'CleanupEndNS', 'CleanupDurationNS'):
        need(uint(r[k]), 'phase timestamp')
    need(0 < r['WriterStopNS'] <= r['WorkersJoinedNS'] < r['CleanupEndNS'] and r['CleanupDurationNS'] == r['CleanupEndNS'] - r['WorkersJoinedNS'], 'phase ordering/cleanup duration')
    need(r['OwnersClosed'] is True and type(r['DBDir']) is str and pathlib.Path(r['DBDir']).is_absolute(), 'owner release')
    h = {k: samples(r[k], r['WorkersJoinedNS']) for k in PHASES}
    for k in ('ReadDrain','QuantumDrain'):
        need(not r[k]['DurationsNS'] or r[k]['FirstStartNS'] >= r['WriterStopNS'], 'post-stop phase clock')
    need(r['WriteActive']['LastEndNS'] <= r['WriterStopNS'], 'writer stop boundary')
    need(r['SetupCalls'] > 0 and 0 < r['SetupMaxNS'] <= r['SetupTotalNS'], 'bounded private setup timing')
    need(h['ReadActive']['count'] + h['ReadDrain']['count'] == x['Reads'] and h['ReadDrain']['count'] == x['ReadsAfterWriterStop'], 'read phase accounting')
    need(h['WriteActive']['count'] == x['Writes'], 'write phase accounting')
    need(h['QuantumActive']['count'] + h['QuantumDrain']['count'] + r['SetupCalls'] == x['Calls'], 'quantum phase accounting')
    for name, phases, setup in [('ReadLatency', ('ReadActive','ReadDrain'),0), ('WriteLatency',('WriteActive',),0), ('QuantumLatency',('QuantumActive','QuantumDrain'),r['SetupTotalNS'])]:
        need(sum(h[k]['total_ns'] for k in phases) + setup == x[name]['TotalNS'], 'raw duration total ' + name)
        need(max([h[k]['max_ns'] for k in phases] + ([r['SetupMaxNS']] if name == 'QuantumLatency' else [])) == x[name]['MaxNS'], 'raw maximum ' + name)
        buckets = [0]*8
        for k in phases:
            for d in r[k]['DurationsNS']:
                i=sum(d > limit for limit in (10000,100000,1000000,5000000,10000000,50000000,100000000))
                buckets[i]+=1
        if name == 'QuantumLatency':
            need(type(r['SetupBuckets']) is list and len(r['SetupBuckets']) == 8 and all(uint(v) for v in r['SetupBuckets']) and sum(r['SetupBuckets']) == r['SetupCalls'], 'setup histogram')
            buckets=[a+b for a,b in zip(buckets,r['SetupBuckets'])]
        need(buckets == x[name]['Buckets'], 'raw bucket consistency ' + name)
    metrics(r['AfterWorkers']); metrics(r['AfterCleanup'])
    before, after = r['AfterWorkers'], r['AfterCleanup']
    need(all(after[k] >= v for k, v in before.items() if k not in ('Scope','Fences')), 'cumulative observer scalars')
    for a, b in zip(before['Fences'], after['Fences']):
        need(all(b[k] >= v for k,v in a.items() if k != 'Site'), 'cumulative per-site metrics')
    need(before['Fences'][0]['ForegroundAttempts'] > 0 and before['Fences'][0]['MaintenanceHolds'] > 0 and before['FenceWaitMaxNS'] > 0 and before['FenceHoldMaxNS'] > 0, 'genuine foreground/fence observation')
    return h

def retained_matrix(cases, n, repetitions, smoke):
    need(type(n) is int and 64 <= n <= 4096 and type(repetitions) is int and 1 <= repetitions <= 10 and type(smoke) is bool, 'retained dimensions')
    need(all(type(c['n']) is int and type(c['repetition']) is int and type(c['mode']) is str and type(c['algorithm']) is str for c in cases), 'retained case types')
    expected = [(n, 'churn', 'bounded', 0)] if smoke else [(size, mode, 'bounded', rep) for size in (n, 2*n) for mode in ('growth','churn') for rep in range(repetitions)]
    need(len(cases) == len(expected) and [(c['n'],c['mode'],c['algorithm'],c['repetition']) for c in cases] == expected, 'retained matrix')
    need(all(type(c['exit_code']) is int and c['exit_code'] == 0 for c in cases), 'retained child status')

def phase_coverage(cases):
    for c in cases:
        need(all(c['quantiles'][k]['count'] >= POLICY['minimum_samples'] for k in ('ReadActive','ReadDrain','WriteActive')), 'insufficient phase sample coverage')

def spread(values):
    need(min(values) > 0 and max(values)/min(values) <= POLICY['maximum_quantile_spread_ratio'], 'excess repetition quantile spread')

def qualification(r):
    need(r['contract'] == R and not r['race'] and not r['smoke'], 'retained nonrace full coverage only')
    need(r['runtime_status'] == 'integrated-reviewed' and valid_sha(r['expected_source_digest']) and r['expected_source_digest'] == r['source_digest'], 'reviewed integrated expected runtime dependency')
    need(r['policy'] == POLICY and r['repetitions'] >= POLICY['repetitions'], 'predeclared repetition/noise policy')
    retained_matrix(r['cases'], r['n'], r['repetitions'], False)
    phase_coverage(r['cases'])
    for size in (r['n'], 2*r['n']):
        for mode in ('growth','churn'):
            group = [c for c in r['cases'] if (c['n'],c['mode']) == (size,mode)]
            for phase in ('ReadActive','ReadDrain','WriteActive'):
                for q in ('p95_ns','p99_ns'):
                    v = [c['quantiles'][phase][q] for c in group]
                    spread(v)
    return True

def matrix(cases):
    need(type(cases) is list and len(cases) == 8, 'eight cases')
    for c in cases:
        need(type(c['n']) is int and type(c['mode']) is str and type(c['algorithm']) is str and type(c['exit_code']) is int and c['exit_code'] == 0, 'case identity/process')
    need(len({(c['n'], c['mode'], c['algorithm']) for c in cases}) == 8 and {(c['n'], c['mode'], c['algorithm']) for c in cases} == set(MATRIX), 'unique matrix')

def packet(out, root=None, receipt=None):
    r = receipt if receipt is not None else json.loads((out / 'receipt.json').read_text())
    source = json.loads((out / 'source-bindings.json').read_text())
    need(type(source) is dict and all(type(k) is str and valid_sha(v) for k, v in source.items()), 'source hashes')
    retained = r['contract'] == R
    need(r['contract'] in (C, R) and digest(source) == r['source_digest'] and r['source_stable'] is True and type(r['source_count']) is int and r['source_count'] == len(source), 'source receipt')
    need(r['errors'] == [] and type(r['race']) is bool, 'receipt errors/race')
    if root:
        need(source == bindings(root), 'source drift')
    if retained:
        need(r['policy'] == POLICY and type(r['smoke']) is bool and r['runtime_status'] in ('unmerged-candidate-overlay','integrated-reviewed'), 'retained policy/dependency')
        retained_matrix(r['cases'], r['n'], r['repetitions'], r['smoke'])
    else:
        matrix(r['cases'])
    for file, key in [('foreground.test', 'binary_sha256'), ('build-command.json', 'build_command_sha256'), ('build.log', 'build_log_sha256')]:
        need(valid_sha(r[key]) and sha(out / file) == r[key], 'build binding ' + file)
    build = json.loads((out / 'build-command.json').read_text())
    need(build['go'] == r['go'] and build['argv'][0] == r['go']['path'] and type(r['go']['version']) is str and r['go']['version'].startswith('go version ') and valid_sha(r['go']['sha256']), 'Go identity')
    need(build['cwd'] == r['source_root'] and build['argv'] == [r['go']['path'], 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground' + (',mvcc_native_prune' if retained else '')] + (['-race'] if r['race'] else []) + ['-o', str(out / 'foreground.test'), './TreeDB/mvcc'], 'build invocation')
    need(build['env'] == r['build_env'] and all(k in r['build_env'] for k in BUILD_KEYS), 'build environment binding')
    need(all(r['build_env'][k] == v for k, v in {'CGO_ENABLED': '1', 'GOFLAGS': '-p=2', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOROOT': None, 'GOGC': None, 'GOMEMLIMIT': None, 'GODEBUG': None}.items()), 'controlled build environment')
    for c in r['cases']:
        for key in ('raw', 'result', 'command'):
            need(type(c[key]) is str and pathlib.Path(c[key]).name == c[key] and valid_sha(c[key + '_sha256']) and sha(out / c[key]) == c[key + '_sha256'], 'case binding ' + key)
        need(c['binary_before_sha256'] == c['binary_after_sha256'] == r['binary_sha256'], 'case binary stability')
        cmd = json.loads((out / c['command']).read_text())
        need(cmd['env'] == dict(r['build_env'], MVCC_FOREGROUND_RESULT=str(out / c['result']), MVCC_FOREGROUND_N=str(c['n']), MVCC_FOREGROUND_MODE=c['mode'], MVCC_FOREGROUND_ALGORITHM=c['algorithm'], MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0', **({'MVCC_FOREGROUND_RETAINED': '1'} if retained else {})), 'case environment')
        need(cmd['argv'] == [str(out / 'foreground.test'), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v'] and cmd['cwd'] == build['cwd'], 'case invocation/cwd')
        x = json.loads((out / c['result']).read_text())
        validate(x, c['n'], c['mode'], c['algorithm'], retained)
        if retained:
            need(c['quantiles'] == validate_retained(x) and c['db_disposed'] is True and not pathlib.Path(x['retained']['DBDir']).exists(), 'quantile/raw binding and disposable DB release')
    return r

def self_test(out, root):
    preflight_self_test()
    r = packet(out, root)
    x = json.loads((out / r['cases'][0]['result']).read_text())
    c = r['cases'][0]
    probes = [lambda y: y.update(ReadsAfterWriterStop=0), lambda y: y.update(ReadersStopWithWriter=True), lambda y: y.update(ForcedBudgetError=True), lambda y: y.update(Calls=float(y['Calls'])), lambda y: y.update(Refusals=False), lambda y: y['ReadLatency'].update(Buckets=[-1, y['Reads'] + 1] + [0] * 6)]
    for mutate in probes:
        y = copy.deepcopy(x); mutate(y)
        try:
            validate(y, c['n'], c['mode'], c['algorithm'], r['contract'] == R)
        except (ValueError, KeyError, TypeError):
            continue
        raise ValueError('negative result accepted')
    probes = [lambda y: y.update(race=not y['race']), lambda y: y['cases'].append(copy.deepcopy(y['cases'][0])), lambda y: y.update(errors=['rejected']), lambda y: y.update(binary_sha256='0' * 64), lambda y: y.update(build_command_sha256='0' * 64), lambda y: y.update(source_digest='0' * 64), lambda y: y['cases'][0].update(exit_code=False)]
    for mutate in probes:
        y = copy.deepcopy(r); mutate(y)
        try:
            packet(out, root, y)
        except (ValueError, KeyError, TypeError):
            continue
        raise ValueError('negative receipt accepted')
    if r['contract'] == R:
        retained_self_test(out, root, r, x, c)
    print('thirteen base in-memory negative checks PASS; retained measurements unchanged')

def retained_self_test(out, root, r, x, c):
    probes = [
        lambda y: y['retained']['ReadActive'].update(Overflow=True),
        lambda y: y['retained']['ReadDrain'].update(DurationsNS=[]),
        lambda y: y['retained']['AfterWorkers'].update(FenceWaitMaxNS=0),
        lambda y: y['retained']['AfterWorkers'].update(StorageSyncCount=999999),
        lambda y: y['retained'].update(WorkersJoinedNS=0),
        lambda y: y['retained'].update(OwnersClosed=False),
        lambda y: y['retained']['WriteActive']['DurationsNS'].__setitem__(0, False),
        lambda y: y['retained'].pop('AfterCleanup'),
        lambda y: y['ReadLatency'].update(MaxNS=y['ReadLatency']['MaxNS']+1),
        lambda y: y['WriteLatency']['Buckets'].__setitem__(0,y['WriteLatency']['Buckets'][0]+1)]
    results=[]
    for i, mutate in enumerate(probes):
        y=copy.deepcopy(x);mutate(y)
        (out/f'retained-negative-result-{i}.json').write_text(json.dumps(y,indent=2)+'\n')
        try: validate(y,c['n'],c['mode'],c['algorithm'],True)
        except (ValueError,KeyError,TypeError) as e: results.append({'probe':i,'rejected':str(e)});continue
        raise ValueError('retained negative result accepted')
    for name, mutate in [('race',lambda y:y.update(race=True)),('insufficient-samples',lambda y:y['cases'][0]['quantiles']['ReadDrain'].update(count=0)),('unmerged-dependency',lambda y:y.update(runtime_status='unmerged-candidate-overlay'))]:
        y=copy.deepcopy(r);y.update(smoke=False,runtime_status='integrated-reviewed',expected_source_digest=y['source_digest'],repetitions=3)
        # Coverage alone must refuse even if other prerequisite declarations pass.
        mutate(y)
        try: qualification(y)
        except (ValueError,KeyError,TypeError) as e: results.append({'probe':name,'rejected':str(e)});continue
        raise ValueError('unqualified packet accepted')
    y=copy.deepcopy(r['cases']);y[0]['quantiles']['ReadDrain']['count']=0
    try: phase_coverage(y)
    except ValueError as e: results.append({'probe':'isolated-insufficient-drain-coverage','rejected':str(e)})
    else: raise ValueError('insufficient drain coverage accepted')
    v=c['quantiles']['WriteActive']['p99_ns']
    try: spread([v,2*v])
    except ValueError as e: results.append({'probe':'isolated-excess-spread','rejected':str(e)})
    else: raise ValueError('excess spread accepted')
    (out/'retained-validator-negative.json').write_text(json.dumps(results,indent=2)+'\n')
    print('fifteen retained negative checks PASS; rejection evidence saved')

def expected_bindings(path, source):
    expected = json.loads(path.read_text())
    need(type(expected) is dict and bool(expected) and all(type(k) is str and valid_sha(v) for k, v in expected.items()), 'expected source hash map')
    need(expected == source, 'expected reviewed source mismatch')
    return expected

def preflight_self_test():
    # Exercise main, not just the map helper. No filesystem writes or Go jobs.
    from unittest import mock
    current = {'fixture.go': '0' * 64}
    argv = ['driver', '--root', '/preflight/source', '--out', '/preflight/output',
            '--retained', '--qualify', '--runtime-status', 'integrated-reviewed',
            '--expected-source', '/preflight/expected.json']
    probes = [
        ('nonexistent', FileNotFoundError('expected manifest absent'), None),
        ('malformed', None, '{'),
        ('wrong-source', None, json.dumps({'fixture.go': '1' * 64})),
        ('not-map', None, '[]'),
        ('bad-hash', None, json.dumps({'fixture.go': True}))]
    results = []
    for name, error, text in probes:
        with mock.patch.object(sys, 'argv', argv), \
             mock.patch.dict(globals(), bindings=lambda root: current), \
             mock.patch.object(pathlib.Path, 'read_text', side_effect=error, return_value=text) as read, \
             mock.patch.object(pathlib.Path, 'mkdir') as mkdir, \
             mock.patch.object(pathlib.Path, 'write_text') as write, \
             mock.patch.object(subprocess, 'run') as run:
            try:
                main()
            except (ValueError, OSError) as e:
                need(read.call_count == 1 and not mkdir.called and not write.called and not run.called, 'invalid expected manifest entered collection')
                results.append({'case': name, 'refusal': str(e), 'output_calls': mkdir.call_count + write.call_count, 'subprocess_calls': run.call_count})
            else:
                raise ValueError('invalid expected manifest accepted')
    class Admitted(Exception):
        pass
    with mock.patch.object(sys, 'argv', argv), \
         mock.patch.dict(globals(), bindings=lambda root: current), \
         mock.patch.object(pathlib.Path, 'read_text', return_value=json.dumps(current)) as read, \
         mock.patch.object(pathlib.Path, 'mkdir', side_effect=Admitted('valid manifest reached output gate')) as mkdir, \
         mock.patch.object(pathlib.Path, 'write_text') as write, \
         mock.patch.object(subprocess, 'run') as run:
        try:
            main()
        except Admitted:
            need(read.call_count == 1 and mkdir.call_count == 1 and not write.called and not run.called, 'valid expected preflight admission')
        else:
            raise ValueError('valid expected manifest refused')
    print(json.dumps({'expected_manifest_preflight': results, 'matching_map_admitted_before_build': True}, indent=2))
    return results

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1]); p.add_argument('--out', type=pathlib.Path, required=True)
    p.add_argument('--retained', action='store_true'); p.add_argument('--smoke', action='store_true'); p.add_argument('--n', type=int, default=512); p.add_argument('--repetitions', type=int, default=3); p.add_argument('--qualify', action='store_true'); p.add_argument('--runtime-status', choices=('unmerged-candidate-overlay','integrated-reviewed'), default='unmerged-candidate-overlay'); p.add_argument('--expected-source', type=pathlib.Path)
    p.add_argument('--go', default='go'); p.add_argument('--race', action='store_true'); p.add_argument('--validate', action='store_true'); p.add_argument('--self-test', action='store_true')
    args = p.parse_args(); root = args.root.resolve(); out = args.out.resolve()
    source = bindings(root) if args.expected_source is not None else None
    expected = expected_bindings(args.expected_source, source) if args.expected_source is not None else None
    if args.validate or args.self_test:
        if args.self_test:
            self_test(out, root)
        else:
            r = packet(out, root)
            if expected is not None: need(digest(expected) == r['source_digest'], 'expected reviewed packet mismatch')
            if args.qualify: qualification(r)
            print('source-bound packet PASS' + ('; retained qualification policy PASS' if args.qualify else ''))
        return
    need(not args.smoke or args.retained, 'smoke requires retained mode')
    need(not args.qualify or args.retained, 'v2 pilot cannot qualify')
    need(not args.qualify or (not args.race and not args.smoke and args.repetitions >= POLICY['repetitions'] and args.runtime_status == 'integrated-reviewed' and args.expected_source is not None), 'qualification capture requires nonrace complete repetitions and expected reviewed integrated source before collection')
    need(64 <= args.n <= 4096 and 1 <= args.repetitions <= 10, 'bounded retained dimensions')
    if source is None: source = bindings(root)
    out.mkdir(exist_ok=False, parents=True)
    (out / 'source-bindings.json').write_text(json.dumps(source, indent=2) + '\n')
    env = {k: v for k, v in os.environ.items() if not k.startswith('MVCC_FOREGROUND_')}; env.pop('GOROOT', None); env.update(CGO_ENABLED='1', GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='-p=2', MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0')
    for key in ('GOGC', 'GOMEMLIMIT', 'GODEBUG'):
        env.pop(key, None)
    if args.retained: env['MVCC_FOREGROUND_RETAINED'] = '1'
    go_path = pathlib.Path(shutil.which(args.go, path=env.get('PATH')) or args.go).resolve(); need(go_path.is_file(), 'Go executable')
    env['PATH'] = str(go_path.parent) + os.pathsep + env.get('PATH', '')
    version = subprocess.run([str(go_path), 'version'], env=env, cwd=root, capture_output=True, text=True, check=True).stdout.strip()
    go = {'path': str(go_path), 'version': version, 'sha256': sha(go_path)}; build_env = {k: env.get(k) for k in BUILD_KEYS}
    binary = out / 'foreground.test'; cmd = [str(go_path), 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground' + (',mvcc_native_prune' if args.retained else '')] + (['-race'] if args.race else []) + ['-o', str(binary), './TreeDB/mvcc']
    (out / 'build-command.json').write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'go': go, 'env': build_env}, indent=2) + '\n')
    result = subprocess.run(cmd, cwd=root, env=env, capture_output=True, timeout=180); (out / 'build.log').write_bytes(result.stdout + result.stderr)
    need(result.returncode == 0, 'build'); need(source == bindings(root), 'source drift'); binary_sha = sha(binary); cases = []; errors = []
    runs = [(args.n,'churn','bounded',0)] if args.smoke else ([(n,m,'bounded',rep) for n in (args.n,2*args.n) for m in ('growth','churn') for rep in range(args.repetitions)] if args.retained else [(n,m,a,0) for n,m,a in MATRIX])
    for n, mode, algorithm, repetition in runs:
        try:
            stable = source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except OSError:
            stable = False
        if not stable:
            errors.append(f'source/executable drift before {algorithm}-{mode}-{n}')
            break
        name = f'{algorithm}-{mode}-{n}' + (f'-r{repetition}' if args.retained else ''); output = out / (name + '.json'); raw = out / (name + '.log'); command = out / (name + '-command.json')
        ee = dict(env); ee.update(MVCC_FOREGROUND_RESULT=str(output), MVCC_FOREGROUND_N=str(n), MVCC_FOREGROUND_MODE=mode, MVCC_FOREGROUND_ALGORITHM=algorithm)
        cmd = [str(binary), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v']
        (command).write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'env': dict(build_env, **{k: v for k, v in ee.items() if k.startswith('MVCC_FOREGROUND_')})}, indent=2) + '\n')
        start = time.monotonic(); process_error = ''
        try:
            result = subprocess.run(cmd, cwd=root, env=ee, capture_output=True, timeout=150)
        except (subprocess.TimeoutExpired, OSError) as e:
            process_error = str(e)
            result = subprocess.CompletedProcess(cmd, -1, getattr(e, 'stdout', None) or b'', (getattr(e, 'stderr', None) or b'') + ('\nDRIVER ERROR: ' + process_error + '\n').encode())
        raw.write_bytes(result.stdout + result.stderr)
        try:
            stable = source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except OSError:
            stable = False
        c = {'n': n, 'mode': mode, 'algorithm': algorithm, 'exit_code': result.returncode, 'process_error': process_error, 'elapsed_seconds': time.monotonic() - start, 'binary_before_sha256': binary_sha, 'binary_after_sha256': sha(binary) if binary.is_file() else None}
        for key, file in [('raw', raw), ('result', output), ('command', command)]:
            c[key] = file.name; c[key + '_sha256'] = sha(file) if file.exists() else None
        if args.retained: c['repetition'] = repetition
        cases.append(c)
        if not stable:
            errors.append('source/executable drift after ' + name)
            break
        try:
            need(result.returncode == 0, 'process ' + name); x=json.loads(output.read_text()); validate(x, n, mode, algorithm, args.retained)
            if args.retained:
                c.update(repetition=repetition, quantiles=validate_retained(x), db_disposed=not pathlib.Path(x['retained']['DBDir']).exists())
                need(c['db_disposed'], 'DB not disposed after child release')
        except (ValueError, KeyError, TypeError, OSError) as e:
            errors.append(str(e))
        print(name + (' PASS' if result.returncode == 0 else ' RED'), flush=True)
    final_source = bindings(root)
    if expected is not None and expected != final_source: errors.append('expected reviewed source drift after collection')
    receipt = {'contract': R if args.retained else C, 'source_root': str(root), 'source_digest': digest(source), 'source_count': len(source), 'source_stable': source == final_source, 'race': args.race, 'go': go, 'build_env': build_env, 'binary_sha256': binary_sha, 'build_command_sha256': sha(out / 'build-command.json'), 'build_log_sha256': sha(out / 'build.log'), 'cases': cases, 'errors': errors, 'labels': {'latency': ('actual public operation intervals including lock wait; raw monotonic durations and nearest-rank p95/p99; coverage/noise policy recorded' if args.retained else 'actual public operation intervals including lock wait; fixed buckets; causal pilot only'), 'sustained': 'ACKWhileWriterActive sampled at public return; stop-drain completion is separate', 'growth': 'future timestamps grow surviving output; fixed timestamp churn fixes cardinality', 'counters': 'observed cursor transitions, not all internal invocations', 'reference': 'zero-work prune fences foreground; no partial-private-output start cut, not equivalent scheduler timing'}}
    if args.retained:
        receipt.update(policy=POLICY,n=args.n,repetitions=args.repetitions,smoke=args.smoke,runtime_status=args.runtime_status,expected_source_digest=digest(expected) if expected is not None else None)
    (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n'); need(not errors, 'causal failures ' + repr(errors)); packet(out, root)
    if args.qualify: qualification(receipt)
    print('source-bound retained capture PASS; qualification requires coverage and integrated reviewed runtime' if args.retained else 'causal packet PASS; retained latency qualification outstanding')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as e:
        print('FAIL CLOSED: ' + str(e), file=sys.stderr); sys.exit(1)
