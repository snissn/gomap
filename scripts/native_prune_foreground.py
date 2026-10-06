#!/usr/bin/env python3
import tempfile
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
BUILD_KEYS = ('CGO_ENABLED', 'GOFLAGS', 'GOWORK', 'GOTOOLCHAIN', 'GOENV', 'HOME', 'PATH')
FIXED_ENV = {'CGO_ENABLED': '1', 'GOFLAGS': '-p=2', 'GOWORK': 'off', 'GOTOOLCHAIN': 'local', 'GOENV': 'off'}

def validate_environment(env, go_path):
    need(type(env) is dict and set(env) == set(BUILD_KEYS) | ({'GOMAXPROCS'} if 'GOMAXPROCS' in env else set()) and all(type(v) is str and v for v in env.values()), 'controlled build environment')
    need(all(env[k] == v for k, v in FIXED_ENV.items()), 'controlled build environment')
    need(pathlib.Path(env['HOME']).is_absolute() and env['PATH'] == str(pathlib.Path(go_path).parent) + os.pathsep + os.defpath, 'controlled build environment')
    if 'GOMAXPROCS' in env:
        value = env['GOMAXPROCS']
        need(value.isascii() and value.isdecimal() and 0 < int(value) <= 2147483647 and str(int(value)) == value, 'controlled build environment')

def controlled_environment(go_path):
    env = dict(FIXED_ENV, HOME=str(pathlib.Path.home()), PATH=str(go_path.parent) + os.pathsep + os.defpath)
    if 'GOMAXPROCS' in os.environ:
        env['GOMAXPROCS'] = os.environ['GOMAXPROCS']
    validate_environment(env, str(go_path))
    return env

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
SAMPLE_TYPES = {'DurationsNS': 'durations', 'FirstStartNS': 'uint', 'LastEndNS': 'uint', 'Overflow': 'bool'}
RETAINED_TYPES = {
    'SetupBuckets': 'buckets', 'Clock': 'string', 'PhaseRule': 'string', 'Capacity': 'uint',
    **{k: 'uint' for k in ('SetupCalls', 'SetupTotalNS', 'SetupMaxNS', 'WriterStopNS', 'WorkersJoinedNS', 'CleanupEndNS', 'CleanupDurationNS')},
    'DBDir': 'string', 'OwnersClosed': 'bool', **{k: 'samples' for k in PHASES},
    'AfterWorkers': 'metrics', 'AfterCleanup': 'metrics',
}
METRIC_TYPES = {
    'Scope': 'string', 'Fences': 'fences',
    **{k: 'uint' for k in ('FileAttempts', 'FileFailures', 'NamespaceAttempts', 'NamespaceFailures', 'MmapAttempts', 'MmapFailures', 'DurableWriteAttempts', 'DurableWriteFailures', 'StorageSyncCount', 'FenceWaitMaxNS', 'FenceHoldMaxNS')},
}
FENCE_TYPES = {'Site': 'string', **{k: 'uint' for k in ('ForegroundAttempts', 'ForegroundWaitMaxNS', 'MaintenanceHolds', 'MaintenanceHoldMaxNS', 'WriterAttempts', 'WriterWaitMaxNS')}}

def typed_schema(value, fields, label):
    need(type(value) is dict and set(value) == set(fields), 'complete ' + label + ' schema')
    for key, kind in fields.items():
        v = value[key]
        if kind == 'uint':
            ok = uint(v) and v < 1 << 64
        elif kind == 'bool':
            ok = type(v) is bool
        elif kind == 'string':
            ok = type(v) is str
        elif kind in ('durations', 'buckets', 'fences'):
            ok = type(v) is list
        else:
            ok = type(v) is dict
        need(ok, label + ' type ' + key)


def need(x, message):
    if not x:
        raise ValueError(message)

def uint(x):
    return type(x) is int and x >= 0


def measurement_labels(retained=False):
    labels = {'latency': 'actual public operation intervals including lock wait; fixed buckets; causal pilot only', 'sustained': 'ACKWhileWriterActive sampled at public return; stop-drain completion is separate', 'growth': 'future timestamps grow surviving output; fixed timestamp churn fixes cardinality', 'counters': 'observed cursor transitions, not all internal invocations', 'reference': 'zero-work prune fences foreground; no partial-private-output start cut, not equivalent scheduler timing'}
    if retained: labels['latency'] = 'actual public operation intervals including lock wait; raw monotonic durations and nearest-rank p95/p99; coverage/noise policy recorded'
    return labels

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def valid_sha(x):
    return type(x) is str and len(x) == 64 and all(c in '0123456789abcdef' for c in x)

def bindings(root):
    # Offline Go/module/policy/driver preflight only; compiled inputs are separate.
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*')) if p.is_file() and '.git' not in p.parts and (p.suffix in ('.go', '.mod', '.sum') or p.name == 'AGENTS.md' or p == root / 'scripts/native_prune_foreground.py')}

# The offline source map above admits reviewed Go/module/policy/driver bytes.
# Go resolves the separate compiled-input graph; do not parse go:embed ourselves.
BUILD_INPUT_CONTRACT = 'gomap-in-repo-build-inputs-v2'
BUILD_INPUT_FIELDS = ('GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles',
                      'HFiles', 'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles',
                      'SysoFiles', 'TestGoFiles', 'XTestGoFiles', 'EmbedFiles',
                      'TestEmbedFiles', 'XTestEmbedFiles')

def build_input_paths(raw, root):
    # Archived metadata is lexical: its original host paths need not exist.
    root = pathlib.Path(os.path.normpath(str(root)))
    decoder = json.JSONDecoder()
    paths = set()
    packages = {}
    remaining = raw
    while remaining.strip():
        package, end = decoder.raw_decode(remaining.lstrip())
        remaining = remaining.lstrip()[end:]
        need(type(package) is dict and not package.get('Error') and not package.get('DepsErrors') and not package.get('Incomplete'), 'incomplete Go build metadata')
        need(type(package.get('Dir')) is str and pathlib.Path(package['Dir']).is_absolute() and type(package.get('ImportPath')) is str and package['ImportPath'], 'malformed Go package identity')
        directory = pathlib.Path(os.path.normpath(package['Dir']))
        module = package.get('Module', {})
        need(type(module) is dict and type(module.get('Replace', {})) is dict, 'malformed Go module metadata')
        replacement = module.get('Replace', {})
        if replacement and not replacement.get('Version'):
            need(pathlib.Path(os.path.normpath(replacement['Dir'])).is_relative_to(root), 'local module replacement outside source root')
        if not directory.is_relative_to(root):
            continue  # External modules/stdlib are outside this in-repo contract.
        selected = {}
        for field in BUILD_INPUT_FIELDS:
            names = package.get(field, [])
            need(type(names) is list and all(type(name) is str and name for name in names), 'malformed Go build input field')
            selected[field] = []
            for name in names:
                path = directory / name
                # Synthetic testmain GoFiles live in Go's cache, not the repo.
                if pathlib.Path(name).is_absolute():
                    need(package.get('Name') == 'main' and package['ImportPath'].endswith('.test') and field == 'GoFiles', 'absolute Go build input')
                    continue
                need(path.is_relative_to(root), 'Go build input escapes source root')
                relative = str(path.relative_to(root))
                need('..' not in pathlib.Path(relative).parts, 'Go build input escapes source root')
                selected[field].append(relative)
                paths.add(relative)
            selected[field].sort()
        module = package.get('Module', {})
        module = module.get('Replace', module)
        if module.get('GoMod'):
            path = pathlib.Path(os.path.normpath(module['GoMod']))
            need(path.is_relative_to(root), 'in-repo package module escapes source root')
            paths.add(str(path.relative_to(root)))
        packages[package['ImportPath']] = selected
    need(packages and paths, 'empty in-repo Go build metadata')
    return {'schema': BUILD_INPUT_CONTRACT, 'packages': packages, 'files': sorted(paths)}

def hash_build_inputs(paths, root):
    root = root.resolve()
    result = {}
    for name in paths:
        path = root / name
        need(path.resolve().is_relative_to(root), 'Go build input escapes source root')
        result[name] = sha(path)
    return result

def build_input_argv(build):
    tags = build[build.index('-tags') + 1]
    return [build[0], 'list', '-deps', '-test', '-json', '-tags', tags] + (['-race'] if '-race' in build else []) + ['./TreeDB/mvcc']

def capture_build_inputs(build, root, env, out, name, go_sha, errors):
    command = {'cwd': str(root), 'argv': build_input_argv(build), 'env': dict(env), 'go_sha256': go_sha}
    (out / (name + '-command.json')).write_text(json.dumps(command, indent=2) + '\n')
    inventory = None
    if artifact_sha(pathlib.Path(build[0])) != go_sha:
        errors.append('Go executable drift before ' + name)
    else:
        process, process_error = run_logged(command['argv'], root, env, out / (name + '.log'), 180)
        (out / (name + '.stdout')).write_bytes(process.stdout or b'')
        if process.returncode != 0:
            errors.append('Go build inventory failed: ' + (process_error or str(process.returncode)))
        else:
            try:
                inventory = build_input_paths((process.stdout or b'').decode(), root)
                inventory['files'] = hash_build_inputs(inventory['files'], root)
                (out / (name + '.json')).write_text(json.dumps(inventory, indent=2) + '\n')
            except (ValueError, KeyError, TypeError, OSError, UnicodeError) as error:
                errors.append('Go build inventory invalid: ' + str(error))
    if artifact_sha(pathlib.Path(build[0])) != go_sha:
        errors.append('Go executable drift after ' + name)
    metadata = {key: artifact_sha(out / (name + suffix)) for key, suffix in
                (('command_sha256', '-command.json'), ('raw_sha256', '.log'), ('stdout_sha256', '.stdout'), ('inventory_sha256', '.json'))}
    return inventory, metadata

def build_inputs_stable(inventory, root):
    return inventory is not None and inventory['files'] == hash_build_inputs(inventory['files'], root)

def validate_build_inputs(out, receipt, build, root=None):
    need(receipt.get('build_input_contract') == BUILD_INPUT_CONTRACT, 'missing compiled build-input contract')
    queries = receipt.get('build_input_queries')
    need(type(queries) is dict and set(queries) == {'inputs-before', 'inputs-after', 'inputs-final'}, 'missing compiled build-input queries')
    inventories = []
    for name, metadata in queries.items():
        need(type(metadata) is dict and set(metadata) == {'command_sha256', 'raw_sha256', 'stdout_sha256', 'inventory_sha256'}, 'compiled build-input query bindings')
        for key, suffix in (('command_sha256', '-command.json'), ('raw_sha256', '.log'), ('stdout_sha256', '.stdout'), ('inventory_sha256', '.json')):
            need(type(metadata[key]) is str and len(metadata[key]) == 64 and all(c in '0123456789abcdef' for c in metadata[key]) and sha(out / (name + suffix)) == metadata[key], 'compiled build-input artifact ' + key)
        command = json.loads((out / (name + '-command.json')).read_text())
        need(command == {'cwd': receipt['source_root'], 'argv': build_input_argv(build['argv']), 'env': receipt['build_env'], 'go_sha256': receipt['go']['sha256']}, 'compiled build-input invocation')
        inventory = json.loads((out / (name + '.json')).read_text())
        paths = build_input_paths((out / (name + '.stdout')).read_text(), pathlib.Path(receipt['source_root']))
        need(type(inventory) is dict and set(inventory) == {'schema', 'packages', 'files'} and type(inventory['files']) is dict, 'compiled build-input inventory')
        need(dict(inventory, files=sorted(inventory['files'])) == paths and all(type(v) is str and len(v) == 64 and all(c in '0123456789abcdef' for c in v) for v in inventory['files'].values()), 'compiled build-input metadata/map binding')
        inventories.append(inventory)
    need(all(i == inventories[0] for i in inventories), 'compiled build-input graph drift')
    if root is not None:
        # Content/deletion drift must refuse before launching the metadata query.
        need(build_inputs_stable(inventories[0], root), 'compiled build-input content drift')
        go = pathlib.Path(receipt['go']['path'])
        need(sha(go) == receipt['go']['sha256'], 'Go executable drift before build-input validation')
        process = subprocess.run(build_input_argv(build['argv']), cwd=root, env=receipt['build_env'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
        need(process.returncode == 0, 'Go build-input validation query failed')
        current = build_input_paths(process.stdout.decode(), root)
        current['files'] = hash_build_inputs(current['files'], root)
        need(sha(go) == receipt['go']['sha256'], 'Go executable drift after build-input validation')
        need(current == inventories[0], 'compiled build-input graph drift')
    return inventories[0]

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

def validate(x, n, mode, algorithm, retained=False):
    need(type(x) is dict and set(x) == set(RESULT_TYPES) | ({'retained'} if retained else set()), 'complete foreground result schema')
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
    need(x['n'] == n and (x['schema'], x['Mode'], x['Algorithm']) == (R if retained else C, mode, algorithm), 'identity')
    need(0 < x['PID'] < 1 << 63, 'PID')
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
    need(0 < x['WriterDurationNS'], 'writer duration')
    need(x['ACKAfterWriterStop'] == 0 or x['DrainCalls'] > 0, 'after-stop ACK without drain call')
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
            need(x['Writes'] == (8192 if retained else {'growth': 256, 'churn': 1024}[mode]), 'write-cap shape')
        else:
            need(x['WriterDurationNS'] >= (1_000_000_000 if retained else 120_000_000), 'observation duration shape')
    if mode == 'burst':
        need(x['Writes'] == 16 and x['WriterStopReason'] == 'finite-burst', 'burst shape')
    need(x['PartialPrivateOutput'] == (algorithm == 'bounded'), 'algorithm private-output witness')
    if algorithm == 'bounded':
        need(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1 << 20 and x['PartialPrivateOutput'] is True, 'bounded actual private output')
    for name, count in [('ReadLatency', x['Reads']), ('WriteLatency', x['Writes']), ('QuantumLatency', x['Calls'])]:
        validate_latency(x[name], count)

    if retained:
        validate_retained(x)
    else:
        need('retained' not in x, 'v2 pilot has no retained extension')

def metrics(m):
    typed_schema(m, METRIC_TYPES, 'observer')
    need(m['Scope'] == 'combined_process_events_since_begin_including_ack_reopen_cleanup_close', 'observer scope')
    need(len(m['Fences']) == len(FENCE_SITES), 'fence sites')
    for f, site in zip(m['Fences'], FENCE_SITES):
        typed_schema(f, FENCE_TYPES, 'fence')
        need(f['Site'] == site, 'per-site metric')
        for count, maximum in [('ForegroundAttempts', 'ForegroundWaitMaxNS'), ('MaintenanceHolds', 'MaintenanceHoldMaxNS'), ('WriterAttempts', 'WriterWaitMaxNS')]:
            need(f[count] > 0 or f[maximum] == 0, 'fence count/max consistency')
    need(m['FenceWaitMaxNS'] == max(f['ForegroundWaitMaxNS'] for f in m['Fences']) and m['FenceHoldMaxNS'] == max(f['MaintenanceHoldMaxNS'] for f in m['Fences']), 'aggregate maxima must be maxima')
    need(m['StorageSyncCount'] == sum(m[k + 'Attempts'] for k in ('File', 'Namespace', 'Mmap', 'DurableWrite')), 'physical attempts sum')
    need(all(m[k + 'Failures'] <= m[k + 'Attempts'] for k in ('File', 'Namespace', 'Mmap', 'DurableWrite')), 'physical failures')

def samples(h, joined):
    typed_schema(h, SAMPLE_TYPES, 'samples')
    need(type(h['DurationsNS']) is list and len(h['DurationsNS']) <= 65536 and all(uint(v) and 0 < v < 1 << 64 for v in h['DurationsNS']), 'raw durations')
    need(h['Overflow'] is False and uint(h['FirstStartNS']) and uint(h['LastEndNS']), 'recorder overflow/types')
    if h['DurationsNS']:
        need(h['FirstStartNS'] <= h['LastEndNS'] <= joined and sum(h['DurationsNS']) <= h['LastEndNS'] - h['FirstStartNS'], 'clock/count duration bounds')
    else:
        need(h['FirstStartNS'] == h['LastEndNS'] == 0, 'empty phase interval')
    v = sorted(h['DurationsNS'])
    return {'count': len(v), 'total_ns': sum(v), 'max_ns': max(v, default=0), 'p95_ns': v[math.ceil(.95*len(v))-1] if v else None, 'p99_ns': v[math.ceil(.99*len(v))-1] if v else None}

def validate_retained(x):
    r = x['retained']
    typed_schema(r, RETAINED_TYPES, 'retained')
    validate_latency({'Count': r['SetupCalls'], 'TotalNS': r['SetupTotalNS'], 'MaxNS': r['SetupMaxNS'], 'Buckets': r['SetupBuckets']}, r['SetupCalls'])
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


def build_input_self_test():
    """Synthetic inventory fixtures; no real Go process or historical upgrade."""
    from unittest import mock
    with tempfile.TemporaryDirectory(prefix='native-build-input-contract-') as temporary:
        root = pathlib.Path(temporary) / 'source'; root.mkdir(); root = root.resolve()
        package = root / 'pkg'; package.mkdir()
        go = root / 'go'; go.write_bytes(b'fixture Go executable')
        (root / 'go.mod').write_text('module fixture\n')
        names = {field: [field + '.input'] for field in BUILD_INPUT_FIELDS}
        for field, files in names.items():
            (package / files[0]).write_text(field + '\n')
        listing = {'Dir': str(package), 'ImportPath': 'fixture/pkg', 'Name': 'pkg',
                   'Module': {'GoMod': str(root / 'go.mod')}, **names}
        raw = json.dumps(listing).encode()
        build = [str(go), 'test', '-c', '-tags', 'fixture', '-race', '-o', '/fixture/test', './TreeDB/mvcc']
        env = {'fixture': 'synthetic'}
        out = pathlib.Path(temporary) / 'packet'; out.mkdir()
        errors = []; queries = {}
        def logged(argv, cwd, environment, log, timeout):
            diagnostic = b'go: downloading fixture/dependency v1.0.0\n'
            log.write_bytes(raw + diagnostic)
            return subprocess.CompletedProcess(argv, 0, raw, diagnostic), ''
        with mock.patch.dict(globals(), run_logged=logged):
            for name in ('inputs-before', 'inputs-after', 'inputs-final'):
                inventory, queries[name] = capture_build_inputs(build, root, env, out, name, sha(go), errors)
        need(not errors and len(inventory['files']) == len(BUILD_INPUT_FIELDS) + 1, 'build-input positive fields')
        receipt = {'build_input_contract': BUILD_INPUT_CONTRACT, 'build_input_queries': queries,
                   'source_root': str(root), 'build_env': env, 'go': {'path': str(go), 'sha256': sha(go)}}
        binding = {'argv': build}
        with mock.patch.object(subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, raw)) as run:
            validate_build_inputs(out, receipt, binding, root)
            need(run.call_count == 1, 'build-input positive graph query')
        refusals = []
        def refused(label, expected, root_arg=root):
            with mock.patch.object(subprocess, 'run', side_effect=AssertionError('unexpected metadata process')) as run:
                try: validate_build_inputs(out, receipt, binding, root_arg)
                except (ValueError, OSError) as error:
                    need(str(error) == expected or isinstance(error, OSError) and expected == 'missing input', 'build-input wrong refusal ' + label + ': ' + str(error))
                    need(not run.called, 'build-input drift launched metadata process')
                    refusals.append(label)
                else: raise ValueError('build-input fault accepted: ' + label)
        for field in BUILD_INPUT_FIELDS:
            path = package / names[field][0]; original = path.read_bytes()
            path.write_bytes(b'changed')
            refused(field + ' content', 'compiled build-input content drift')
            path.unlink(); refused(field + ' deletion', 'missing input'); path.write_bytes(original)
        original_contract = receipt.pop('build_input_contract')
        refused('old receipt', 'missing compiled build-input contract', None)
        receipt['build_input_contract'] = 'gomap-in-repo-build-inputs-v1'
        refused('v1 archived contract', 'missing compiled build-input contract', None)
        receipt['build_input_contract'] = original_contract
        for name in queries:
            path = out / (name + '.stdout'); original = path.read_bytes()
            path.unlink(); refused(name + ' missing stdout', 'missing input', None)
            path.write_bytes(b'changed stdout')
            refused(name + ' stdout drift', 'compiled build-input artifact stdout_sha256', None)
            path.write_bytes(original)
        for field, value in (('cwd', '/other'), ('argv', [str(go), 'version']), ('env', {'inherited': 'bad'}), ('go_sha256', '0' * 64)):
            name = 'inputs-before'; path = out / (name + '-command.json'); original = path.read_bytes()
            command = json.loads(original); command[field] = value; path.write_text(json.dumps(command))
            queries[name]['command_sha256'] = sha(path)
            refused('refreshed command ' + field, 'compiled build-input invocation', None)
            path.write_bytes(original); queries[name]['command_sha256'] = sha(path)
        # Refresh both metadata and inventory checksums, so a rewritten
        # selected input must reach graph equality rather than hash refusal.
        name = 'inputs-before'; raw_path = out / (name + '.stdout'); map_path = out / (name + '.json')
        raw_original = raw_path.read_bytes(); map_original = map_path.read_bytes()
        altered = dict(listing); altered['EmbedFiles'] = []
        raw_path.write_text(json.dumps(altered))
        altered_map = build_input_paths(raw_path.read_text(), root)
        altered_map['files'] = hash_build_inputs(altered_map['files'], root)
        map_path.write_text(json.dumps(altered_map))
        queries[name]['stdout_sha256'] = sha(raw_path); queries[name]['inventory_sha256'] = sha(map_path)
        refused('refreshed embed removal', 'compiled build-input graph drift', None)
        raw_path.write_bytes(raw_original); map_path.write_bytes(map_original)
        queries[name]['stdout_sha256'] = sha(raw_path); queries[name]['inventory_sha256'] = sha(map_path)
        # A newly selected embed/native input changes graph membership even
        # when every previously archived input still has its original bytes.
        changed = dict(listing); changed['EmbedFiles'] = names['EmbedFiles'] + ['new.asset']
        (package / 'new.asset').write_bytes(b'new')
        with mock.patch.object(subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, json.dumps(changed).encode())) as run:
            try: validate_build_inputs(out, receipt, binding, root)
            except ValueError as error: need(str(error) == 'compiled build-input graph drift', 'new input wrong refusal')
            else: raise ValueError('new build input accepted')
            need(run.call_count == 1, 'new input membership query')
        for label, altered in (('local replace', dict(listing, Module={'Replace': {'Dir': '/outside', 'GoMod': '/outside/go.mod'}})),
                               ('package error', dict(listing, Error={'Err': 'failed'})),
                               ('incomplete package', dict(listing, Incomplete=True))):
            try: build_input_paths(json.dumps(altered), root)
            except ValueError: refusals.append(label)
            else: raise ValueError('unsafe build metadata accepted: ' + label)
        result = {'synthetic_build_input_fields': len(BUILD_INPUT_FIELDS), 'offline_refusals': refusals,
                  'added_input_query_refused': True, 'real_subprocesses': 0}
        print(json.dumps(result, indent=2))
        return result

def capture_version(go_path, root, env, out, errors):
    validate_environment(env, str(go_path))
    build_env = dict(env)
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
    retained = r['contract'] == R
    need(r['contract'] in (C, R) and digest(source) == r['source_digest'] and r['source_stable'] is True and type(r['source_count']) is int and r['source_count'] == len(source), 'source receipt')
    need(r.get('labels') == measurement_labels(retained), 'noncanonical foreground scope labels')
    need(r['errors'] == [] and type(r['race']) is bool, 'receipt errors/race')
    if root:
        need(source == bindings(root), 'source drift')
    if retained:
        need(r['policy'] == POLICY and type(r['smoke']) is bool and r['runtime_status'] in ('unmerged-candidate-overlay','integrated-reviewed'), 'retained policy/dependency')
        retained_matrix(r['cases'], r['n'], r['repetitions'], r['smoke'])
    else:
        matrix(r['cases'])
    need(type(r['capture_out']) is str and pathlib.Path(r['capture_out']).is_absolute(), 'capture directory')
    capture = pathlib.Path(r['capture_out'])
    for file, key in [('foreground.test', 'binary_sha256'), ('build-command.json', 'build_command_sha256'), ('build.log', 'build_log_sha256')]:
        need(valid_sha(r[key]) and sha(out / file) == r[key], 'build binding ' + file)
    build = json.loads((out / 'build-command.json').read_text())
    need(build['go'] == r['go'] and build['argv'][0] == r['go']['path'] and type(r['go']['version']) is str and r['go']['version'].startswith('go version ') and valid_sha(r['go']['sha256']), 'Go identity')
    need(build['cwd'] == r['source_root'] and build['argv'] == [r['go']['path'], 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground' + (',mvcc_native_prune' if retained else '')] + (['-race'] if r['race'] else []) + ['-o', str(capture / 'foreground.test'), './TreeDB/mvcc'], 'build invocation')
    need(build['env'] == r['build_env'], 'build environment binding')
    validate_environment(r['build_env'], r['go']['path'])
    validate_version(out, r)
    validate_build_inputs(out, r, build, root)
    for c in r['cases']:
        for key in ('raw', 'result', 'command'):
            need(type(c[key]) is str and pathlib.Path(c[key]).name == c[key] and valid_sha(c[key + '_sha256']) and sha(out / c[key]) == c[key + '_sha256'], 'case binding ' + key)
        need(c['binary_before_sha256'] == c['binary_after_sha256'] == r['binary_sha256'], 'case binary stability')
        cmd = json.loads((out / c['command']).read_text())
        need(cmd['env'] == dict(r['build_env'], MVCC_FOREGROUND_RESULT=str(capture / c['result']), MVCC_FOREGROUND_N=str(c['n']), MVCC_FOREGROUND_MODE=c['mode'], MVCC_FOREGROUND_ALGORITHM=c['algorithm'], MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0', **({'MVCC_FOREGROUND_RETAINED': '1'} if retained else {})), 'case environment')
        need(cmd['argv'] == [str(capture / 'foreground.test'), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v'] and cmd['cwd'] == build['cwd'], 'case invocation/cwd')
        x = json.loads((out / c['result']).read_text())
        validate(x, c['n'], c['mode'], c['algorithm'], retained)
        if retained:
            need(c['quantiles'] == validate_retained(x) and c['db_disposed'] is True and not pathlib.Path(x['retained']['DBDir']).exists(), 'quantile/raw binding and disposable DB release')
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
        ('after-stop ACK without drain', lambda y: y.update(ACKWhileWriterActive=0, ACKAfterWriterStop=y['Pruned'], DrainCalls=0), 'after-stop ACK without drain call'),
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
        for case in r['cases']:
            shutil.copyfile(out / case['result'], archive / case['result'])
        for index, case in enumerate(r['cases']):
            target = archive / case['result']
            original = target.read_bytes()
            altered = json.loads(original)
            altered['PartialPrivateOutput'] = not altered['PartialPrivateOutput']
            try:
                validate(altered, case['n'], case['mode'], case['algorithm'], r['contract'] == R)
            except ValueError as error:
                need(str(error) == 'algorithm private-output witness', 'wrong algorithm case refusal')
            else:
                raise ValueError('opposite algorithm output witness accepted')
            target.write_text(json.dumps(altered, indent=2) + '\n')
            receipt = copy.deepcopy(r)
            receipt['cases'][index]['result_sha256'] = sha(target)
            try:
                packet(archive, receipt=receipt)
            except ValueError as error:
                need(str(error) == 'algorithm private-output witness', 'wrong coupled algorithm refusal')
            else:
                raise ValueError('checksum-refreshed algorithm output witness accepted')
            finally:
                target.write_bytes(original)
            count += 1
    print(f'{positive} positive schema/histogram cases and {count} checksum-refreshed contract refusals PASS; original packets unchanged')

def self_test(out, root):
    build_input_self_test()
    preflight_self_test()
    r = packet(out, root)
    environment_self_test(out, r)
    ack_drain_self_test(out, r)
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
    probes = [lambda y: y.update(race=not y['race']), lambda y: y['cases'].append(copy.deepcopy(y['cases'][0])), lambda y: y.update(errors=['rejected']), lambda y: y.update(binary_sha256='0' * 64), lambda y: y.update(build_command_sha256='0' * 64), lambda y: y.update(source_digest='0' * 64), lambda y: y['cases'][0].update(exit_code=False), lambda y: y.pop('capture_out'), lambda y: y.update(capture_out='relative'), lambda y: y.update(capture_out=False), lambda y: y.update(capture_out=str(out / 'wrong-capture'))]
    for mutate in probes:
        y = copy.deepcopy(r); mutate(y)
        try:
            packet(out, root, y)
        except (ValueError, KeyError, TypeError):
            continue
        raise ValueError('negative receipt accepted')
    if r['contract'] == R:
        retained_contract_self_test(out, r, x, c)
        retained_self_test(out, root, r, x, c)
    else:
        contract_self_test(out, r)
    print('seventeen base in-memory negative checks PASS; retained measurements unchanged')

def retained_contract_self_test(out, receipt, original, case):
    # Mutations retain the original capture/runtime identity and refresh only the
    # result checksum in a disposable archive. They are parser probes, not runs.
    import tempfile
    schemas = [((), RESULT_TYPES), (('retained',), RETAINED_TYPES)]
    schemas += [((name,), {k: 'buckets' if k == 'Buckets' else 'uint' for k in LATENCY_FIELDS}) for name in ('ReadLatency', 'WriteLatency', 'QuantumLatency')]
    schemas += [(('retained', phase), SAMPLE_TYPES) for phase in PHASES]
    for name in ('AfterWorkers', 'AfterCleanup'):
        schemas.append((('retained', name), METRIC_TYPES))
        schemas += [(('retained', name, 'Fences', i), FENCE_TYPES) for i in range(len(FENCE_SITES))]
    def at(value, path):
        for key in path:
            value = value[key]
        return value
    probes = []
    for path, fields in schemas:
        for key, kind in fields.items():
            probes.append((path, key, 'delete', None))
            bad = False if kind in ('uint', 'byte', 'signed') else 0 if kind in ('bool', 'string') else {} if kind in ('buckets', 'durations', 'fences') else []
            probes.append((path, key, 'type', bad))
            if kind in ('uint', 'byte', 'signed'):
                probes.append((path, key, 'overflow', 1 << (8 if kind == 'byte' else 63 if kind == 'signed' else 64)))
        probes.append((path, 'UnexpectedField', 'extra', 0))
    probes += [(('retained', 'ReadActive', 'DurationsNS'), 0, 'overflow', 1 << 64),
               (('retained', 'SetupBuckets'), 0, 'type', False)]
    count = 0
    with tempfile.TemporaryDirectory(prefix='native-retained-contract-') as folder:
        archive = pathlib.Path(folder)
        for file in out.iterdir():
            if file.is_file():
                shutil.copyfile(file, archive / file.name)
        for path, key, action, bad in probes:
            altered = copy.deepcopy(original)
            value = at(altered, path)
            if action == 'delete':
                value.pop(key)
            else:
                value[key] = bad
            try:
                validate(altered, case['n'], case['mode'], case['algorithm'], True)
            except ValueError as error:
                reason = str(error)
            else:
                raise ValueError('retained schema mutation accepted ' + repr((path, key, action)))
            target = archive / case['result']
            target.write_text(json.dumps(altered, indent=2) + '\n')
            changed = copy.deepcopy(receipt)
            changed['cases'][0]['result_sha256'] = sha(target)
            try:
                packet(archive, receipt=changed)
            except ValueError as error:
                need(str(error) == reason, 'retained coupled rejection mismatch')
            else:
                raise ValueError('retained coupled schema mutation accepted')
            count += 1
    print(f'{count} checksum-refreshed retained/base schema and integer-bound refusals PASS; original runtime identity preserved')

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


def ack_drain_self_test(out, receipt):
    """Every mode/algorithm: refresh checksums without recapturing measurements."""
    import tempfile
    with tempfile.TemporaryDirectory(prefix='native-foreground-ack-drain-') as folder:
        archive = pathlib.Path(folder)
        for file in out.iterdir():
            if file.is_file():
                shutil.copyfile(file, archive / file.name)
        for index, case in enumerate(receipt['cases']):
            target = archive / case['result']
            original = (out / case['result']).read_bytes()
            altered = json.loads(original)
            altered.update(ACKWhileWriterActive=0, ACKAfterWriterStop=altered['Pruned'], DrainCalls=0)
            target.write_text(json.dumps(altered, indent=2) + '\n')
            changed = copy.deepcopy(receipt)
            changed['cases'][index]['result_sha256'] = sha(target)
            try:
                packet(archive, receipt=changed)
            except ValueError as error:
                need(str(error) == 'after-stop ACK without drain call', 'wrong coupled ACK/drain refusal')
            else:
                raise ValueError('checksum-refreshed after-stop ACK without drain accepted')
            finally:
                target.write_bytes(original)
    print(str(len(receipt['cases'])) + ' mode/algorithm checksum-refreshed ACK/drain refusals PASS; original packet unchanged')


def environment_self_test(out, receipt):
    # Refresh every command binding together so only the environment contract
    # rejects these probes. Original packets and measurement bytes stay intact.
    import tempfile
    faults = [(key, 'missing', None) for key in BUILD_KEYS]
    faults += [(key, 'set', 'unrecorded') for key in ('GOEXPERIMENT', 'GOAMD64', 'CC', 'CGO_CFLAGS', 'LD_PRELOAD', 'GOROOT', 'GOGC', 'GOMEMLIMIT', 'GODEBUG')]
    faults += [('GOENV', 'set', '/tmp/unbound-goenv'), ('HOME', 'set', 'relative'), ('PATH', 'set', receipt['build_env']['PATH'] + os.pathsep + '/tmp/unrecorded-tool-path')]
    faults += [('GOMAXPROCS', 'set', v) for v in (None, 0, '', '0', '01', '2147483648', '٢')]
    with tempfile.TemporaryDirectory(prefix='native-environment-contract-') as folder:
        archive = pathlib.Path(folder)
        for file in out.iterdir():
            if file.is_file():
                shutil.copyfile(file, archive / file.name)
        for key, action, value in faults:
            altered = json.loads(json.dumps(receipt))
            env = altered['build_env']
            if action == 'missing':
                env.pop(key)
            else:
                env[key] = value
            for name, binding in [('version-command.json', 'version_command_sha256'), ('build-command.json', 'build_command_sha256')]:
                command = json.loads((out / name).read_text())
                command['env'] = dict(env)
                target = archive / name
                target.write_text(json.dumps(command) + '\n')
                altered[binding] = sha(target)
            for case in altered['cases']:
                command = json.loads((out / case['command']).read_text())
                command['env'] = dict(env, **{k: v for k, v in command['env'].items() if k.startswith('MVCC_')})
                target = archive / case['command']
                target.write_text(json.dumps(command) + '\n')
                case['command_sha256'] = sha(target)
            (archive / 'receipt.json').write_text(json.dumps(altered) + '\n')
            try:
                packet(archive)
            except ValueError as error:
                if str(error) != 'controlled build environment':
                    raise
            else:
                raise ValueError('coupled uncontrolled environment accepted: ' + key)
    print(str(len(faults)) + ' coupled environment contract refusals PASS; original packets unchanged')

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
    go_path = pathlib.Path(shutil.which(args.go) or args.go).resolve()
    env = controlled_environment(go_path)
    cases = []; errors = []; binary_sha = None; source_stable = True
    go, build_env, version_metadata, source_stable = capture_version(go_path, root, env, out, errors)
    binary = out / 'foreground.test'; cmd = [str(go_path), 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground' + (',mvcc_native_prune' if args.retained else '')] + (['-race'] if args.race else []) + ['-o', str(binary), './TreeDB/mvcc']
    (out / 'build-command.json').write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'go': go, 'env': build_env}, indent=2) + '\n')
    inventory_build = list(cmd)
    build_inventory = None; build_queries = {}
    if not errors:
        build_inventory, build_queries['inputs-before'] = capture_build_inputs(cmd, root, env, out, 'inputs-before', go['sha256'], errors)
    if not errors:
        result, process_error = run_logged(cmd, root, env, out / 'build.log', 180)
        if result.returncode != 0: errors.append('build failed: ' + (process_error or str(result.returncode)))
        else:
            binary_sha = artifact_sha(binary)
            after = final_bindings(root, errors)
            if source != after or binary_sha is None or artifact_sha(go_path) != go['sha256']:
                source_stable = False; errors.append('source/executable drift after build')
    if not errors:
        after_inventory, build_queries['inputs-after'] = capture_build_inputs(inventory_build, root, env, out, 'inputs-after', go['sha256'], errors)
        if build_inventory != after_inventory:
            source_stable = False; errors.append('compiled build-input graph drift after build')
    runs = [(args.n,'churn','bounded',0)] if args.smoke else ([(n,m,'bounded',rep) for n in (args.n,2*args.n) for m in ('growth','churn') for rep in range(args.repetitions)] if args.retained else [(n,m,a,0) for n,m,a in MATRIX])
    for n, mode, algorithm, repetition in (runs if not errors else []):
        try:
            stable = build_inputs_stable(build_inventory, root) and source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except (OSError, ValueError):
            stable = False
        if not stable:
            source_stable = False
            errors.append(f'source/executable drift before {algorithm}-{mode}-{n}')
            break
        name = f'{algorithm}-{mode}-{n}' + (f'-r{repetition}' if args.retained else ''); output = out / (name + '.json'); raw = out / (name + '.log'); command = out / (name + '-command.json')
        ee = dict(env); ee.update(MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0', MVCC_FOREGROUND_RESULT=str(output), MVCC_FOREGROUND_N=str(n), MVCC_FOREGROUND_MODE=mode, MVCC_FOREGROUND_ALGORITHM=algorithm)
        if args.retained: ee['MVCC_FOREGROUND_RETAINED'] = '1'
        cmd = [str(binary), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v']
        (command).write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'env': dict(ee)}, indent=2) + '\n')
        start = time.monotonic()
        result, process_error = run_logged(cmd, root, ee, raw, 150)
        if result.returncode != 0: errors.append('process ' + name + ': ' + (process_error or str(result.returncode)))
        try:
            stable = build_inputs_stable(build_inventory, root) and source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except (OSError, ValueError):
            stable = False
        c = {'n': n, 'mode': mode, 'algorithm': algorithm, 'exit_code': result.returncode, 'process_error': process_error, 'elapsed_seconds': time.monotonic() - start, 'binary_before_sha256': binary_sha, 'binary_after_sha256': artifact_sha(binary)}
        for key, file in [('raw', raw), ('result', output), ('command', command)]:
            c[key] = file.name; c[key + '_sha256'] = artifact_sha(file)
        if args.retained: c['repetition'] = repetition
        cases.append(c)
        if not stable:
            source_stable = False
            errors.append('source/executable drift after ' + name)
            break
        case_accepted = False
        try:
            need(result.returncode == 0, 'process ' + name); x=json.loads(output.read_text()); validate(x, n, mode, algorithm, args.retained)
            if args.retained:
                c.update(repetition=repetition, quantiles=validate_retained(x), db_disposed=not pathlib.Path(x['retained']['DBDir']).exists())
                need(c['db_disposed'], 'DB not disposed after child release')
            case_accepted = True
        except (ValueError, KeyError, TypeError, OSError) as e:
            errors.append(str(e))
        print(name + (' PASS' if case_accepted else ' RED'), flush=True)
        if not case_accepted: break
    final_source = final_bindings(root, errors)
    if source != final_source: source_stable = False; errors.append('source drift after collection')
    if expected is not None and expected != final_source: errors.append('expected reviewed source drift after collection')
    if not errors:
        final_inventory, build_queries['inputs-final'] = capture_build_inputs(inventory_build, root, env, out, 'inputs-final', go['sha256'], errors)
        if build_inventory != final_inventory:
            source_stable = False; errors.append('compiled build-input graph drift after collection')
    receipt = {'build_input_contract': BUILD_INPUT_CONTRACT, 'build_input_queries': build_queries, 'capture_out': str(out), 'contract': R if args.retained else C, 'source_root': str(root), 'source_digest': digest(source), 'source_count': len(source), 'source_stable': source_stable, 'race': args.race, 'go': go, 'build_env': build_env, 'binary_sha256': binary_sha, 'build_command_sha256': artifact_sha(out / 'build-command.json'), 'build_log_sha256': artifact_sha(out / 'build.log'), 'cases': cases, 'errors': errors, 'labels': measurement_labels(args.retained), **version_metadata}
    if args.retained:
        receipt.update(policy=POLICY,n=args.n,repetitions=args.repetitions,smoke=args.smoke,runtime_status=args.runtime_status,expected_source_digest=digest(expected) if expected is not None else None)
    if not errors:
        try: validate_version(out, receipt)
        except (OSError,ValueError,KeyError,TypeError) as error: errors.append('version capture invalid: '+str(error))
    (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n'); need(not errors, 'causal failures ' + repr(errors)); packet(out, root)
    if args.qualify: qualification(receipt)
    print('source-bound retained capture PASS; qualification requires coverage and integrated reviewed runtime' if args.retained else 'causal packet PASS; retained latency qualification outstanding')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as e:
        print('FAIL CLOSED: ' + str(e), file=sys.stderr); sys.exit(1)
