#!/usr/bin/env python3
"""Fresh-process foreground causal pilot; no retained latency qualification."""
import argparse
import copy
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

C = 'gomap-native-foreground-v2'
MATRIX = [(n, m, 'bounded') for n in (64, 128) for m in ('burst', 'growth', 'churn')] + [(n, 'burst', 'unbounded') for n in (64, 128)]
BUILD_KEYS = ('CGO_ENABLED', 'GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOMAXPROCS', 'GOROOT', 'PATH', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')

# Explicit producer schema; missing or unexpected fields require a matching validator change.
RESULT_TYPES = {
    'ACKAfterWriterStop': 'uint',
    'ACKWhileWriterActive': 'uint',
    'AcceptanceCausePresent': 'bool',
    'AcceptanceCauseText': 'string',
    'AcceptanceCauseType': 'string',
    'AcceptanceCostBytes': 'uint',
    'AcceptanceCostRecords': 'uint',
    'Algorithm': 'string',
    'Bytes': 'uint',
    'Calls': 'uint',
    'CancelDrains': 'uint',
    'CancelTransitions': 'uint',
    'CompletedAfterStop': 'bool',
    'CompletedWhileWriterActive': 'bool',
    'CurrentBackendCommit': 'uint',
    'CurrentBackendRoot': 'uint',
    'DrainCalls': 'uint',
    'Error': 'string',
    'FinalWorkBytes': 'uint',
    'FinalWorkRecords': 'uint',
    'FloorRecaptures': 'uint',
    'ForcedBudgetError': 'bool',
    'ForegroundIntervalsAtQuantumStart': 'uint',
    'LastAcceptancePhase': 'signed',
    'LastAction': 'byte',
    'LastBackendCommit': 'uint',
    'LastBackendRoot': 'uint',
    'LastCOWAuxiliaryPhase': 'signed',
    'LastCOWBeginPhase': 'signed',
    'LastCOWCommit': 'uint',
    'LastCOWGeneration': 'uint',
    'LastCOWItem': 'signed',
    'LastCOWMaterializerPhase': 'signed',
    'LastCOWPhase': 'signed',
    'LastCOWPrunePhase': 'signed',
    'LastCOWSet': 'signed',
    'LastChunk': 'uint',
    'LastEnqueuePhase': 'uint',
    'LastNativeFramePhase': 'byte',
    'LastNativeLeafPhase': 'byte',
    'LastNativePhase': 'byte',
    'LastNativeRunIndex': 'signed',
    'LastPublicationGeneration': 'uint',
    'MaxBytes': 'uint',
    'MaxRecords': 'uint',
    'MinimumBytes': 'uint',
    'MinimumRecords': 'uint',
    'Mode': 'string',
    'NewPreparations': 'uint',
    'OldReaderOracle': 'bool',
    'PID': 'uint',
    'PartialPrivateOutput': 'bool',
    'PhysicalOracle': 'bool',
    'PointerOracle': 'bool',
    'Pruned': 'uint',
    'QualificationResets': 'uint',
    'QuantumLatency': 'latency',
    'ReadIntervalsOverlappingQuantum': 'uint',
    'ReadLatency': 'latency',
    'ReadersStopWithWriter': 'bool',
    'Reads': 'uint',
    'ReadsAfterWriterStop': 'uint',
    'Records': 'uint',
    'Refusals': 'uint',
    'ReopenOracle': 'bool',
    'SourceCostBytes': 'uint',
    'SourceCostRecords': 'uint',
    'WriteIntervalsOverlappingQuantum': 'uint',
    'WriteLatency': 'latency',
    'WriterActiveCalls': 'uint',
    'WriterDurationNS': 'uint',
    'WriterOracle': 'bool',
    'WriterStopReason': 'string',
    'Writes': 'uint',
    'n': 'uint',
    'schema': 'string',
}
LATENCY_FIELDS = {'Count', 'TotalNS', 'MaxNS', 'Buckets'}

def need(x, message):
    if not x:
        raise ValueError(message)

def uint(x):
    return type(x) is int and x >= 0


def measurement_labels():
    labels = {'latency': 'actual public operation intervals including lock wait; fixed buckets; causal pilot only', 'sustained': 'ACKWhileWriterActive sampled at public return; stop-drain completion is separate', 'growth': 'future timestamps grow surviving output; fixed timestamp churn fixes cardinality', 'counters': 'observed cursor transitions, not all internal invocations', 'reference': 'zero-work prune fences foreground; no partial-private-output start cut, not equivalent scheduler timing'}
    return labels

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def valid_sha(x):
    return type(x) is str and len(x) == 64 and all(c in '0123456789abcdef' for c in x)

def bindings(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file() and '.git' not in p.parts and (p.suffix in ('.go', '.mod', '.sum') or p.name == 'AGENTS.md' or p == root / 'scripts/native_prune_foreground.py')}

def digest(source):
    return hashlib.sha256(json.dumps(source, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def validate_latency(h, count):
    need(type(h) is dict and set(h) == LATENCY_FIELDS, 'complete latency schema')
    need(all(uint(h[k]) and h[k] < 1 << 64 for k in ('Count', 'TotalNS', 'MaxNS')), 'latency types')
    need(type(h['Buckets']) is list and len(h['Buckets']) == 8 and all(uint(v) and v < 1 << 64 for v in h['Buckets']), 'histogram buckets')
    need(h['Count'] == count and sum(h['Buckets']) == count and h['MaxNS'] <= h['TotalNS'] <= count * h['MaxNS'], 'latency counters')
    if count == 0:
        need(h['TotalNS'] == h['MaxNS'] == 0, 'empty histogram')
        return
    limits = (10_000, 100_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000)
    maximum = h['MaxNS']
    bucket = sum(maximum > v for v in limits)
    need(max(i for i, v in enumerate(h['Buckets']) if v) == bucket, 'histogram maximum bucket')
    lower = (0,) + tuple(v + 1 for v in limits)
    upper = limits + (maximum,)
    minimum_total = sum(v * lower[i] for i, v in enumerate(h['Buckets'])) + maximum - lower[bucket]
    maximum_total = sum(v * min(upper[i], maximum) for i, v in enumerate(h['Buckets']))
    need(minimum_total <= h['TotalNS'] <= maximum_total, 'histogram total feasibility')

def validate(x, n, mode, algorithm):
    need(type(x) is dict and set(x) == set(RESULT_TYPES), 'complete foreground result schema')
    for k, kind in RESULT_TYPES.items():
        v = x[k]
        if kind == 'string':
            need(type(v) is str, 'string ' + k)
        elif kind == 'bool':
            need(type(v) is bool, 'bool ' + k)
        elif kind == 'signed':
            need(type(v) is int and -(1 << 63) <= v < 1 << 63, 'signed counter ' + k)
        elif kind in ('uint', 'byte'):
            need(uint(v) and v < 1 << (8 if kind == 'byte' else 64), 'counter ' + k)
        else:
            need(type(v) is dict and set(v) == LATENCY_FIELDS, 'complete latency schema ' + k)
    need(x['n'] == n and (x['schema'], x['Mode'], x['Algorithm']) == (C, mode, algorithm), 'identity')
    need(0 < x['PID'] < 1 << 63, 'PID')
    need(0 < x['ReadsAfterWriterStop'] <= x['Reads'], 'post-writer completed read witness')
    need(x['ReadersStopWithWriter'] is False and x['ForcedBudgetError'] is False, 'continuing-reader measurement only')
    need(not x['Error'] and x['Refusals'] == 0, 'failed/refused work')
    need(x['Pruned'] == n - 1 and x['ACKWhileWriterActive'] + x['ACKAfterWriterStop'] == n - 1, 'ACK accounting')
    need(all(x[k] > 0 for k in ('Calls', 'Reads', 'Writes', 'WriterActiveCalls')), 'foreground progress')
    need(x['ForegroundIntervalsAtQuantumStart'] + x['ReadIntervalsOverlappingQuantum'] + x['WriteIntervalsOverlappingQuantum'] > 0, 'operation interval overlap')
    need(all(x[k] is True for k in ('PhysicalOracle', 'WriterOracle', 'PointerOracle', 'OldReaderOracle', 'ReopenOracle')), 'oracles')
    need(x['CompletedWhileWriterActive'] != x['CompletedAfterStop'], 'completion distinction')
    need(x['Writes'] <= {'burst': 16, 'growth': 256, 'churn': 1024}[mode], 'write ceiling')
    need(x['WriterStopReason'] in ('finite-burst', 'write-cap', 'observation-duration'), 'writer stop reason')
    need(0 < x['WriterDurationNS'], 'writer duration')
    need(x['WriterActiveCalls'] + x['DrainCalls'] <= x['Calls'], 'writer phase call accounting')
    need(x['ReadIntervalsOverlappingQuantum'] <= x['Reads'] and x['WriteIntervalsOverlappingQuantum'] <= x['Writes'] and x['ForegroundIntervalsAtQuantumStart'] <= x['WriterActiveCalls'] + x['DrainCalls'], 'overlap count bounds')
    need(x['FinalWorkRecords'] <= x['MaxRecords'] <= x['Records'] <= x['Calls'] * x['MaxRecords'] and x['FinalWorkBytes'] <= x['MaxBytes'] <= x['Bytes'] <= x['Calls'] * x['MaxBytes'], 'work counter accounting')
    if algorithm == 'bounded':
        need(all(x[k] > 0 for k in ('Records', 'Bytes', 'MaxRecords', 'MaxBytes')), 'missing charged physical work')
    need(all(x[k] <= x['Calls'] for k in ('CancelTransitions', 'CancelDrains', 'FloorRecaptures', 'QualificationResets', 'NewPreparations')), 'transition count bounds')
    need(x['MinimumRecords'] == x['MinimumBytes'] == 0, 'successful refusal metadata')
    need(not x['CompletedAfterStop'] or x['DrainCalls'] > 0, 'completion drain witness')
    need(not x['CompletedWhileWriterActive'] or x['DrainCalls'] == 0, 'active completion phase')
    need(x['AcceptanceCausePresent'] and bool(x['AcceptanceCauseType']) or not x['AcceptanceCausePresent'] and x['AcceptanceCauseType'] == x['AcceptanceCauseText'] == '', 'acceptance cause shape')
    if algorithm == 'unbounded':
        need(x['WriterActiveCalls'] + x['DrainCalls'] == x['Calls'], 'unbounded call phases')
    if mode != 'burst':
        need(x['WriterStopReason'] != 'finite-burst', 'nonburst stop shape')
        if x['WriterStopReason'] == 'write-cap':
            need(x['Writes'] == {'growth': 256, 'churn': 1024}[mode], 'write-cap shape')
        else:
            need(x['WriterDurationNS'] >= 120_000_000, 'observation duration shape')
    if mode == 'burst':
        need(x['Writes'] == 16 and x['WriterStopReason'] == 'finite-burst', 'burst shape')
    if algorithm == 'bounded':
        need(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1 << 20 and x['PartialPrivateOutput'] is True, 'bounded actual private output')
    for name, count in [('ReadLatency', x['Reads']), ('WriteLatency', x['Writes']), ('QuantumLatency', x['Calls'])]:
        validate_latency(x[name], count)

def matrix(cases):
    need(type(cases) is list and len(cases) == 8, 'eight cases')
    for c in cases:
        need(type(c['n']) is int and type(c['mode']) is str and type(c['algorithm']) is str and type(c['exit_code']) is int and c['exit_code'] == 0, 'case identity/process')
    need(len({(c['n'], c['mode'], c['algorithm']) for c in cases}) == 8 and {(c['n'], c['mode'], c['algorithm']) for c in cases} == set(MATRIX), 'unique matrix')

def validate_version(out, receipt):
    for name, key in [('version-command.json', 'version_command_sha256'), ('version.log', 'version_log_sha256')]:
        value = receipt.get(key)
        need(type(value) is str and len(value) == 64 and all(c in '0123456789abcdef' for c in value) and (out / name).is_file() and sha(out / name) == value, 'version artifact binding ' + name)
    command = json.loads((out / 'version-command.json').read_text())
    go = receipt['go']
    need(type(go['path']) is str and pathlib.Path(go['path']).is_absolute() and type(go['sha256']) is str and len(go['sha256']) == 64 and all(c in '0123456789abcdef' for c in go['sha256']), 'version Go identity')
    need(command == {'cwd': receipt['source_root'], 'argv': [go['path'], 'version'], 'env': receipt['build_env'], 'go_path': go['path'], 'go_sha256': go['sha256']}, 'version invocation binding')
    need(receipt.get('version_go_before_sha256') == receipt.get('version_go_after_sha256') == go['sha256'], 'version executable stability')
    output = go.get('version_output')
    need(type(output) is str and (out / 'version.log').read_bytes() == output.encode() and type(go['version']) is str and go['version'].startswith('go version ') and output == go['version'] + '\n' and '\n' not in go['version'], 'version output binding')


def capture_version(go_path, root, env, out, errors):
    build_env = {k: env.get(k) for k in BUILD_KEYS}
    before = artifact_sha(go_path)
    command = {'cwd': str(root), 'argv': [str(go_path), 'version'], 'env': build_env, 'go_path': str(go_path), 'go_sha256': before}
    (out / 'version-command.json').write_text(json.dumps(command, indent=2) + '\n')
    command_before = artifact_sha(out / 'version-command.json')
    raw = None
    go = {'path': str(go_path), 'version': None, 'version_output': None, 'sha256': before}
    if before is None:
        errors.append('Go executable unreadable before version capture')
    else:
        version, process_error = run_logged(command['argv'], root, env, out / 'version.log', 30)
        if version.returncode != 0:
            errors.append('Go version failed: ' + (process_error or str(version.returncode)))
        else:
            raw = (version.stdout or b'') + (version.stderr or b'')
            output = raw.decode(errors='replace')
            go.update(version=output.strip(), version_output=output)
            if output.encode() != raw or not go['version'].startswith('go version ') or output != go['version'] + '\n' or '\n' in go['version']:
                errors.append('invalid Go version output')
    after = artifact_sha(go_path)
    stable = before is not None and before == after
    if not stable:
        errors.append('Go executable drift during version capture')
    metadata = {'version_command_sha256': artifact_sha(out / 'version-command.json'), 'version_log_sha256': artifact_sha(out / 'version.log'), 'version_go_before_sha256': before, 'version_go_after_sha256': after}
    if command_before is None or metadata['version_command_sha256'] != command_before:
        errors.append('version command drift during capture')
    if raw is not None and metadata['version_log_sha256'] != hashlib.sha256(raw).hexdigest():
        errors.append('version output drift during capture')
    return go, build_env, metadata, stable


def packet(out, root=None, receipt=None):
    r = receipt if receipt is not None else json.loads((out / 'receipt.json').read_text())
    source = json.loads((out / 'source-bindings.json').read_text())
    need(type(source) is dict and all(type(k) is str and valid_sha(v) for k, v in source.items()), 'source hashes')
    need(r['contract'] == C and digest(source) == r['source_digest'] and r['source_stable'] is True and type(r['source_count']) is int and r['source_count'] == len(source), 'source receipt')
    need(r.get('labels') == measurement_labels(), 'noncanonical foreground scope labels')
    need(r['errors'] == [] and type(r['race']) is bool, 'receipt errors/race')
    if root:
        need(source == bindings(root), 'source drift')
    matrix(r['cases'])
    need(type(r['capture_out']) is str and pathlib.Path(r['capture_out']).is_absolute(), 'capture directory')
    capture = pathlib.Path(r['capture_out'])
    for file, key in [('foreground.test', 'binary_sha256'), ('build-command.json', 'build_command_sha256'), ('build.log', 'build_log_sha256')]:
        need(valid_sha(r[key]) and sha(out / file) == r[key], 'build binding ' + file)
    build = json.loads((out / 'build-command.json').read_text())
    need(build['go'] == r['go'] and build['argv'][0] == r['go']['path'] and type(r['go']['version']) is str and r['go']['version'].startswith('go version ') and valid_sha(r['go']['sha256']), 'Go identity')
    need(build['cwd'] == r['source_root'] and build['argv'] == [r['go']['path'], 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground'] + (['-race'] if r['race'] else []) + ['-o', str(capture / 'foreground.test'), './TreeDB/mvcc'], 'build invocation')
    need(build['env'] == r['build_env'] and all(k in r['build_env'] for k in BUILD_KEYS), 'build environment binding')
    need(all(r['build_env'][k] == v for k, v in {'CGO_ENABLED': '1', 'GOFLAGS': '-p=2', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOROOT': None, 'GOGC': None, 'GOMEMLIMIT': None, 'GODEBUG': None}.items()), 'controlled build environment')
    validate_version(out, r)
    for c in r['cases']:
        for key in ('raw', 'result', 'command'):
            need(type(c[key]) is str and pathlib.Path(c[key]).name == c[key] and valid_sha(c[key + '_sha256']) and sha(out / c[key]) == c[key + '_sha256'], 'case binding ' + key)
        need(c['binary_before_sha256'] == c['binary_after_sha256'] == r['binary_sha256'], 'case binary stability')
        cmd = json.loads((out / c['command']).read_text())
        need(cmd['env'] == dict(r['build_env'], MVCC_FOREGROUND_RESULT=str(capture / c['result']), MVCC_FOREGROUND_N=str(c['n']), MVCC_FOREGROUND_MODE=c['mode'], MVCC_FOREGROUND_ALGORITHM=c['algorithm'], MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0'), 'case environment')
        need(cmd['argv'] == [str(capture / 'foreground.test'), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v'] and cmd['cwd'] == build['cwd'], 'case invocation/cwd')
        validate(json.loads((out / c['result']).read_text()), c['n'], c['mode'], c['algorithm'])
    return r

def contract_self_test(out, r):
    import tempfile
    c = r['cases'][0]
    x = json.loads((out / c['result']).read_text())
    positive = 0
    for original in r['cases']:
        validate(json.loads((out / original['result']).read_text()), original['n'], original['mode'], original['algorithm'])
        positive += 1
    edge = copy.deepcopy(x)
    values = (0, 10_000, 10_001, 100_000, 100_001, 1_000_000, 1_000_001, 100_000_001)
    edge.update(Reads=8, ReadsAfterWriterStop=1, ReadIntervalsOverlappingQuantum=0,
                AcceptanceCausePresent=False, AcceptanceCauseType='', AcceptanceCauseText='')
    edge['ReadLatency'] = {'Count': 8, 'TotalNS': sum(values), 'MaxNS': max(values), 'Buckets': [2, 2, 2, 1, 0, 0, 0, 1]}
    validate(edge, c['n'], c['mode'], c['algorithm']); positive += 1
    validate_latency({'Count': 0, 'TotalNS': 0, 'MaxNS': 0, 'Buckets': [0] * 8}, 0); positive += 1
    checks = []
    for k, kind in RESULT_TYPES.items():
        checks.append(('missing ' + k, lambda y, k=k: y.pop(k), 'complete foreground result schema'))
        value = False if kind in ('uint', 'byte', 'signed') else 0 if kind in ('bool', 'string') else []
        reason = 'counter ' + k if kind in ('uint', 'byte') else 'signed counter ' + k if kind == 'signed' else kind + ' ' + k if kind in ('bool', 'string') else 'complete latency schema ' + k
        checks.append(('type ' + k, lambda y, k=k, value=value: y.update({k: value}), reason))
    checks.append(('extra result', lambda y: y.update(UnexpectedCounter=0), 'complete foreground result schema'))
    for name in ('ReadLatency', 'WriteLatency', 'QuantumLatency'):
        for k in sorted(LATENCY_FIELDS):
            checks.append(('missing ' + name + '.' + k, lambda y, name=name, k=k: y[name].pop(k), 'complete latency schema ' + name))
            value = False if k != 'Buckets' else {}
            reason = 'latency types' if k != 'Buckets' else 'histogram buckets'
            checks.append(('type ' + name + '.' + k, lambda y, name=name, k=k, value=value: y[name].update({k: value}), reason))
        checks.append(('extra ' + name, lambda y, name=name: y[name].update(UnexpectedCounter=0), 'complete latency schema ' + name))
    checks.extend([
        ('read overlap', lambda y: y.update(ReadIntervalsOverlappingQuantum=y['Reads'] + 1), 'overlap count bounds'),
        ('write overlap', lambda y: y.update(WriteIntervalsOverlappingQuantum=y['Writes'] + 1), 'overlap count bounds'),
        ('entry overlap', lambda y: y.update(ForegroundIntervalsAtQuantumStart=y['Calls'] + 1), 'overlap count bounds'),
        ('phase calls', lambda y: y.update(DrainCalls=y['Calls']), 'writer phase call accounting'),
        ('zero duration', lambda y: y.update(WriterDurationNS=0), 'writer duration'),
        ('maximum records', lambda y: y.update(MaxRecords=y['Records'] + 1), 'work counter accounting'),
        ('maximum bytes', lambda y: y.update(MaxBytes=y['Bytes'] + 1), 'work counter accounting'),
        ('final records', lambda y: y.update(FinalWorkRecords=y['MaxRecords'] + 1), 'work counter accounting'),
        ('final bytes', lambda y: y.update(FinalWorkBytes=y['MaxBytes'] + 1), 'work counter accounting'),
        ('total records', lambda y: y.update(Records=y['Calls'] * y['MaxRecords'] + 1), 'work counter accounting'),
        ('total bytes', lambda y: y.update(Bytes=y['Calls'] * y['MaxBytes'] + 1), 'work counter accounting'),
        ('transition', lambda y: y.update(QualificationResets=y['Calls'] + 1), 'transition count bounds'),
        ('minimum records', lambda y: y.update(MinimumRecords=1), 'successful refusal metadata'),
        ('minimum bytes', lambda y: y.update(MinimumBytes=1), 'successful refusal metadata'),
        ('uint overflow', lambda y: y.update(Records=1 << 64), 'counter Records'),
        ('byte overflow', lambda y: y.update(LastAction=256), 'counter LastAction'),
        ('signed overflow', lambda y: y.update(LastCOWPhase=1 << 63), 'signed counter LastCOWPhase'),
        ('cause', lambda y: y.update(AcceptanceCausePresent=False, AcceptanceCauseType='invented'), 'acceptance cause shape'),
        ('cause type', lambda y: y.update(AcceptanceCausePresent=True, AcceptanceCauseType=''), 'acceptance cause shape'),
        ('histogram total', lambda y: y['ReadLatency'].update(TotalNS=y['Reads'] * y['ReadLatency']['MaxNS'] + 1), 'latency counters'),
    ])
    # Bucket-edge positives make these feasibility negatives independently discriminating.
    edge_checks = [
        ('maximum bucket', lambda y: y['ReadLatency'].update(MaxNS=100_000_000), 'histogram maximum bucket'),
        ('too-low total', lambda y: y['ReadLatency'].update(TotalNS=y['ReadLatency']['MaxNS']), 'histogram total feasibility'),
        ('too-high feasible total', lambda y: y['ReadLatency'].update(TotalNS=8 * y['ReadLatency']['MaxNS']), 'histogram total feasibility'),
    ]
    with tempfile.TemporaryDirectory(prefix='native-foreground-contract-') as folder:
        archive = pathlib.Path(folder)
        for file in out.iterdir():
            if file.is_file():
                shutil.copyfile(file, archive / file.name)
        count = 0
        for base, probes in ((x, checks), (edge, edge_checks)):
            for label, mutate, reason in probes:
                altered = copy.deepcopy(base); mutate(altered)
                try:
                    validate(altered, c['n'], c['mode'], c['algorithm'])
                except ValueError as error:
                    need(str(error) == reason, label + ': wrong case rejection ' + str(error))
                else:
                    raise ValueError(label + ': corrupted result accepted')
                target = archive / c['result']
                target.write_text(json.dumps(altered, indent=2) + '\n')
                receipt = copy.deepcopy(r)
                receipt['cases'][0]['result_sha256'] = sha(target)
                try:
                    packet(archive, receipt=receipt)
                except ValueError as error:
                    need(str(error) == reason, label + ': wrong coupled packet rejection ' + str(error))
                else:
                    raise ValueError(label + ': coupled corrupted packet accepted')
                count += 1
    print(f'{positive} positive schema/histogram cases and {count} checksum-refreshed contract refusals PASS; original packets unchanged')

def self_test(out, root):
    r = packet(out, root)
    x = json.loads((out / r['cases'][0]['result']).read_text())
    c = r['cases'][0]
    probes = [lambda y: y.update(ReadsAfterWriterStop=0), lambda y: y.update(ReadersStopWithWriter=True), lambda y: y.update(ForcedBudgetError=True), lambda y: y.update(Calls=float(y['Calls'])), lambda y: y.update(Refusals=False), lambda y: y['ReadLatency'].update(Buckets=[-1, y['Reads'] + 1] + [0] * 6)]
    for mutate in probes:
        y = copy.deepcopy(x); mutate(y)
        try:
            validate(y, c['n'], c['mode'], c['algorithm'])
        except (ValueError, KeyError, TypeError):
            continue
        raise ValueError('negative result accepted')
    probes = [lambda y: y.update(race=not y['race']), lambda y: y['cases'].append(copy.deepcopy(y['cases'][0])), lambda y: y.update(errors=['rejected']), lambda y: y.update(binary_sha256='0' * 64), lambda y: y.update(build_command_sha256='0' * 64), lambda y: y.update(source_digest='0' * 64), lambda y: y['cases'][0].update(exit_code=False), lambda y: y.pop('capture_out'), lambda y: y.update(capture_out='relative'), lambda y: y.update(capture_out=False), lambda y: y.update(capture_out=str(out / 'wrong-capture'))]
    for mutate in probes:
        y = copy.deepcopy(r); mutate(y)
        try:
            packet(out, root, y)
        except (ValueError, KeyError, TypeError):
            continue
        raise ValueError('negative receipt accepted')
    print('seventeen in-memory negative checks PASS; retained measurements unchanged')
    contract_self_test(out, r)


def artifact_sha(path):
    try:
        return sha(path)
    except OSError:
        return None

def run_logged(cmd, root, env, log, timeout):
    process_error = ''
    try:
        p = subprocess.run(cmd, cwd=root, env=env, capture_output=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError) as e:
        process_error = str(e)
        p = subprocess.CompletedProcess(cmd, -1, getattr(e, 'stdout', None) or b'', getattr(e, 'stderr', None) or b'')
    log.write_bytes((p.stdout or b'') + (p.stderr or b'') + (('\nDRIVER ERROR: ' + process_error + '\n').encode() if process_error else b''))
    return p, process_error

def final_bindings(root, errors):
    try:
        return bindings(root)
    except OSError as e:
        errors.append('source bindings unreadable: ' + str(e))
        return None

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1]); p.add_argument('--out', type=pathlib.Path, required=True)
    p.add_argument('--go', default='go'); p.add_argument('--race', action='store_true'); p.add_argument('--validate', action='store_true'); p.add_argument('--self-test', action='store_true')
    args = p.parse_args(); root = args.root.resolve(); out = args.out.resolve()
    if args.validate or args.self_test:
        if args.self_test:
            self_test(out, root)
        else:
            packet(out, root); print('source-bound packet PASS')
        return
    out.mkdir(exist_ok=False, parents=True)
    source = bindings(root); (out / 'source-bindings.json').write_text(json.dumps(source, indent=2) + '\n')
    env = {k: v for k, v in os.environ.items() if not k.startswith('MVCC_FOREGROUND_')}; env.pop('GOROOT', None); env.update(CGO_ENABLED='1', GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='-p=2', MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0')
    for key in ('GOGC', 'GOMEMLIMIT', 'GODEBUG'):
        env.pop(key, None)
    go_path = pathlib.Path(shutil.which(args.go, path=env.get('PATH')) or args.go).resolve()
    env['PATH'] = str(go_path.parent) + os.pathsep + env.get('PATH', '')
    cases = []; errors = []; binary_sha = None; source_stable = True
    go, build_env, version_metadata, source_stable = capture_version(go_path, root, env, out, errors)
    binary = out / 'foreground.test'; cmd = [str(go_path), 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground'] + (['-race'] if args.race else []) + ['-o', str(binary), './TreeDB/mvcc']
    (out / 'build-command.json').write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'go': go, 'env': build_env}, indent=2) + '\n')
    if not errors:
        result, process_error = run_logged(cmd, root, env, out / 'build.log', 180)
        if result.returncode != 0: errors.append('build failed: ' + (process_error or str(result.returncode)))
        else:
            binary_sha = artifact_sha(binary)
            after = final_bindings(root, errors)
            if source != after or binary_sha is None or artifact_sha(go_path) != go['sha256']:
                source_stable = False; errors.append('source/executable drift after build')
    for n, mode, algorithm in (MATRIX if not errors else []):
        try:
            stable = source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except OSError:
            stable = False
        if not stable:
            source_stable = False
            errors.append(f'source/executable drift before {algorithm}-{mode}-{n}')
            break
        name = f'{algorithm}-{mode}-{n}'; output = out / (name + '.json'); raw = out / (name + '.log'); command = out / (name + '-command.json')
        ee = dict(env); ee.update(MVCC_FOREGROUND_RESULT=str(output), MVCC_FOREGROUND_N=str(n), MVCC_FOREGROUND_MODE=mode, MVCC_FOREGROUND_ALGORITHM=algorithm)
        cmd = [str(binary), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v']
        (command).write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'env': dict(build_env, **{k: v for k, v in ee.items() if k.startswith('MVCC_FOREGROUND_')})}, indent=2) + '\n')
        start = time.monotonic()
        result, process_error = run_logged(cmd, root, ee, raw, 150)
        if result.returncode != 0: errors.append('process ' + name + ': ' + (process_error or str(result.returncode)))
        try:
            stable = source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except OSError:
            stable = False
        c = {'n': n, 'mode': mode, 'algorithm': algorithm, 'exit_code': result.returncode, 'process_error': process_error, 'elapsed_seconds': time.monotonic() - start, 'binary_before_sha256': binary_sha, 'binary_after_sha256': artifact_sha(binary)}
        for key, file in [('raw', raw), ('result', output), ('command', command)]:
            c[key] = file.name; c[key + '_sha256'] = artifact_sha(file)
        cases.append(c)
        if not stable:
            source_stable = False
            errors.append('source/executable drift after ' + name)
            break
        case_accepted = False
        try:
            need(result.returncode == 0, 'process ' + name); validate(json.loads(output.read_text()), n, mode, algorithm)
            case_accepted = True
        except (ValueError, KeyError, TypeError, OSError) as e:
            errors.append(str(e))
        print(name + (' PASS' if case_accepted else ' RED'), flush=True)
        if not case_accepted: break
    final_source = final_bindings(root, errors)
    if source != final_source: source_stable = False; errors.append('source drift after collection')
    receipt = {'capture_out': str(out), 'contract': C, 'source_root': str(root), 'source_digest': digest(source), 'source_count': len(source), 'source_stable': source_stable, 'race': args.race, 'go': go, 'build_env': build_env, 'binary_sha256': binary_sha, 'build_command_sha256': artifact_sha(out / 'build-command.json'), 'build_log_sha256': artifact_sha(out / 'build.log'), 'cases': cases, 'errors': errors, 'labels': measurement_labels(), **version_metadata}
    if not errors:
        try: validate_version(out, receipt)
        except (OSError,ValueError,KeyError,TypeError) as error: errors.append('version capture invalid: '+str(error))
    (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n'); need(not errors, 'causal failures ' + repr(errors)); packet(out, root)
    print('causal packet PASS; retained latency qualification outstanding')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as e:
        print('FAIL CLOSED: ' + str(e), file=sys.stderr); sys.exit(1)
