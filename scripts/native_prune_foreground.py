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

def validate(x, n, mode, algorithm):
    need(type(x['n']) is int and x['n'] == n and (x['schema'], x['Mode'], x['Algorithm']) == (C, mode, algorithm), 'identity')
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
        elif k not in strings | bools | {'ReadLatency', 'WriteLatency', 'QuantumLatency'}:
            need(uint(v), 'counter ' + k)
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
    if mode == 'burst':
        need(x['Writes'] == 16 and x['WriterStopReason'] == 'finite-burst', 'burst shape')
    if algorithm == 'bounded':
        need(x['MaxRecords'] <= 32 and x['MaxBytes'] <= 1 << 20 and x['PartialPrivateOutput'] is True, 'bounded actual private output')
    for name, count in [('ReadLatency', x['Reads']), ('WriteLatency', x['Writes']), ('QuantumLatency', x['Calls'])]:
        h = x[name]
        need(all(uint(h[k]) for k in ('Count', 'TotalNS', 'MaxNS')), 'latency types')
        need(type(h['Buckets']) is list and len(h['Buckets']) == 8 and all(uint(v) for v in h['Buckets']), 'histogram buckets')
        need(h['Count'] == count and h['TotalNS'] >= h['MaxNS'] and sum(h['Buckets']) == count, 'latency counters')

def matrix(cases):
    need(type(cases) is list and len(cases) == 8, 'eight cases')
    for c in cases:
        need(type(c['n']) is int and type(c['mode']) is str and type(c['algorithm']) is str and type(c['exit_code']) is int and c['exit_code'] == 0, 'case identity/process')
    need(len({(c['n'], c['mode'], c['algorithm']) for c in cases}) == 8 and {(c['n'], c['mode'], c['algorithm']) for c in cases} == set(MATRIX), 'unique matrix')

def packet(out, root=None, receipt=None):
    r = receipt if receipt is not None else json.loads((out / 'receipt.json').read_text())
    source = json.loads((out / 'source-bindings.json').read_text())
    need(type(source) is dict and all(type(k) is str and valid_sha(v) for k, v in source.items()), 'source hashes')
    need(r['contract'] == C and digest(source) == r['source_digest'] and r['source_stable'] is True and type(r['source_count']) is int and r['source_count'] == len(source), 'source receipt')
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
    for c in r['cases']:
        for key in ('raw', 'result', 'command'):
            need(type(c[key]) is str and pathlib.Path(c[key]).name == c[key] and valid_sha(c[key + '_sha256']) and sha(out / c[key]) == c[key + '_sha256'], 'case binding ' + key)
        need(c['binary_before_sha256'] == c['binary_after_sha256'] == r['binary_sha256'], 'case binary stability')
        cmd = json.loads((out / c['command']).read_text())
        need(cmd['env'] == dict(r['build_env'], MVCC_FOREGROUND_RESULT=str(capture / c['result']), MVCC_FOREGROUND_N=str(c['n']), MVCC_FOREGROUND_MODE=c['mode'], MVCC_FOREGROUND_ALGORITHM=c['algorithm'], MVCC_FOREGROUND_READER_STOP_WITH_WRITER='0', MVCC_FOREGROUND_FORCE_BUDGET_ERROR='0'), 'case environment')
        need(cmd['argv'] == [str(capture / 'foreground.test'), '-test.run', '^TestNativePruneForegroundPilot$', '-test.count=1', '-test.timeout=120s', '-test.v'] and cmd['cwd'] == build['cwd'], 'case invocation/cwd')
        validate(json.loads((out / c['result']).read_text()), c['n'], c['mode'], c['algorithm'])
    return r

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
    go_path = pathlib.Path(shutil.which(args.go, path=env.get('PATH')) or args.go).resolve(); need(go_path.is_file(), 'Go executable')
    env['PATH'] = str(go_path.parent) + os.pathsep + env.get('PATH', '')
    version = subprocess.run([str(go_path), 'version'], env=env, cwd=root, capture_output=True, text=True, check=True).stdout.strip()
    go = {'path': str(go_path), 'version': version, 'sha256': sha(go_path)}; build_env = {k: env.get(k) for k in BUILD_KEYS}
    binary = out / 'foreground.test'; cmd = [str(go_path), 'test', '-c', '-tags', 'treedb_test,mvcc_native_foreground'] + (['-race'] if args.race else []) + ['-o', str(binary), './TreeDB/mvcc']
    (out / 'build-command.json').write_text(json.dumps({'cwd': str(root), 'argv': cmd, 'go': go, 'env': build_env}, indent=2) + '\n')
    result = subprocess.run(cmd, cwd=root, env=env, capture_output=True, timeout=180); (out / 'build.log').write_bytes(result.stdout + result.stderr)
    need(result.returncode == 0, 'build'); need(source == bindings(root), 'source drift'); binary_sha = sha(binary); cases = []; errors = []
    for n, mode, algorithm in MATRIX:
        try:
            stable = source == bindings(root) and sha(binary) == binary_sha and sha(go_path) == go['sha256']
        except OSError:
            stable = False
        if not stable:
            errors.append(f'source/executable drift before {algorithm}-{mode}-{n}')
            break
        name = f'{algorithm}-{mode}-{n}'; output = out / (name + '.json'); raw = out / (name + '.log'); command = out / (name + '-command.json')
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
        cases.append(c)
        if not stable:
            errors.append('source/executable drift after ' + name)
            break
        try:
            need(result.returncode == 0, 'process ' + name); validate(json.loads(output.read_text()), n, mode, algorithm)
        except (ValueError, KeyError, TypeError, OSError) as e:
            errors.append(str(e))
        print(name + (' PASS' if result.returncode == 0 else ' RED'), flush=True)
    receipt = {'capture_out': str(out), 'contract': C, 'source_root': str(root), 'source_digest': digest(source), 'source_count': len(source), 'source_stable': source == bindings(root), 'race': args.race, 'go': go, 'build_env': build_env, 'binary_sha256': binary_sha, 'build_command_sha256': sha(out / 'build-command.json'), 'build_log_sha256': sha(out / 'build.log'), 'cases': cases, 'errors': errors, 'labels': {'latency': 'actual public operation intervals including lock wait; fixed buckets; causal pilot only', 'sustained': 'ACKWhileWriterActive sampled at public return; stop-drain completion is separate', 'growth': 'future timestamps grow surviving output; fixed timestamp churn fixes cardinality', 'counters': 'observed cursor transitions, not all internal invocations', 'reference': 'zero-work prune fences foreground; no partial-private-output start cut, not equivalent scheduler timing'}}
    (out / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n'); need(not errors, 'causal failures ' + repr(errors)); packet(out, root)
    print('causal packet PASS; retained latency qualification outstanding')

if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as e:
        print('FAIL CLOSED: ' + str(e), file=sys.stderr); sys.exit(1)
