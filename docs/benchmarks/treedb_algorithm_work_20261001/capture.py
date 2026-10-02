#!/usr/bin/env python3
"""A-only binary preparation and complete raw capture; collection needs a grant."""
import argparse
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time

HERE = Path(__file__).resolve().parent
ENV_KEYS = ('GOROOT', 'GOVERSION', 'GOTOOLCHAIN', 'GOWORK', 'GOFLAGS', 'GOEXPERIMENT',
            'GOOS', 'GOARCH', 'CGO_ENABLED', 'CC', 'CXX', 'CGO_CFLAGS', 'CGO_CPPFLAGS',
            'CGO_CXXFLAGS', 'CGO_LDFLAGS', 'GOMOD', 'GOPATH', 'GOCACHE',
            'GOAMD64', 'GOARM64', 'GOARM', 'GO386', 'GOPPC64', 'GORISCV64', 'GOMIPS', 'GOMIPS64', 'GOWASM')
IDENTITY_KEYS = ('runtime_head', 'runtime_tree', 'harness_sha256', 'inputs', 'go_environment',
                 'modules', 'capture_sha256', 'overlay_generator_sha256')
RUNTIME_ENV = dict(GOWORK='off', GOTOOLCHAIN='local', GOFLAGS='', GOMAXPROCS='2', GOMEMLIMIT='2GiB',
                   GOGC='100', GODEBUG='', GOTRACEBACK='single', GORACE='')


def normalized_environment(inherited):
    # Only known build/path inputs cross the process boundary; no arbitrary
    # inherited settings or secrets. Runtime controls are fixed for every cohort.
    allowed = set(ENV_KEYS) | {'HOME', 'PATH', 'TMPDIR', 'GOTMPDIR', 'GOENV'}
    return dict({k: v for k, v in inherited.items() if k in allowed}, **RUNTIME_ENV)


def frozen_identity(freeze):
    if any(k not in freeze for k in IDENTITY_KEYS):
        raise ValueError('incomplete frozen identity schema')
    if any(not isinstance(freeze[k], dict) or not freeze[k] for k in ('inputs', 'go_environment', 'modules')):
        raise ValueError('empty compiled-input/module/environment inventory')
    return {k: freeze[k] for k in IDENTITY_KEYS}
WORK_COUNTERS = ('treedb.flush_apply.apply_ops_total',
                 'treedb.flush_apply.old_leaf_read_decode.node_loads_total',
                 'treedb.flush_apply.old_leaf_read_decode.bytes_total',
                 'treedb.flush_apply.merge_build.leaf_pages_written_total',
                 'treedb.flush_apply.merge_build.leaf_page_bytes_written_total',
                 'treedb.cache.flush_backlog_coalescing.admitted_runs_total')


def digest(path):
    hashed = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            hashed.update(block)
    return hashed.hexdigest()


def query(command, root, env):
    return subprocess.check_output(command, cwd=root, env=env, text=True)


def identity(root, go, env, overlay=None):
    if query(['git', 'status', '--porcelain', '--untracked-files=all'], root, env):
        raise ValueError('preparation/collection requires a clean checkout')
    # go list resolves the actual compiled package graph, including local replacements,
    # stdlib, cgo sources and embedded files; Git HEAD alone does not bind these.
    replacements = json.loads(Path(overlay).read_text())['Replace'] if overlay else {}
    overlay_flag = ['-overlay=' + str(overlay)] if overlay else []
    raw = query([go, 'list', *overlay_flag, '-deps', '-test', '-json', './TreeDB'], root, env)
    decoder, inputs, modules = json.JSONDecoder(), {}, {}
    while raw.strip():
        package, end = decoder.raw_decode(raw.lstrip())
        raw = raw.lstrip()[end:]
        if package.get('Module'):
            module = package['Module']
            modules[module['Path']] = module
            for resolved in (module, module.get('Replace', {})):
                if resolved.get('GoMod'):
                    inputs[resolved['GoMod']] = digest(resolved['GoMod'])
        for field in ('GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles',
                      'FFiles', 'SFiles', 'SysoFiles', 'EmbedFiles'):
            for name in package.get(field, []):
                path = Path(package['Dir']) / name
                path = Path(replacements.get(str(path), str(path)))
                inputs[str(path.resolve())] = digest(path)
    if overlay:
        for path in (Path(overlay), Path(overlay).with_name('manifest.json')):
            inputs[str(path.resolve())] = digest(path)
    settings = json.loads(query([go, 'env', '-json', *ENV_KEYS], root, env))
    tools = Path(settings['GOROOT']) / 'pkg/tool' / (settings['GOOS'] + '_' + settings['GOARCH'])
    # Standard assembly includes are inputs outside package GoFiles/SFiles.
    for path in sorted((Path(settings['GOROOT']) / 'pkg/include').rglob('*')):
        if path.is_file():
            inputs[str(path.resolve())] = digest(path)
    for path in (Path(go), tools / 'compile', tools / 'link', tools / 'asm', root / 'go.mod', root / 'go.sum'):
        inputs[str(path.resolve())] = digest(path)
    return dict(runtime_head=query(['git', 'rev-parse', 'HEAD'], root, env).strip(),
                runtime_tree=query(['git', 'rev-parse', 'HEAD:TreeDB'], root, env).strip(),
                harness_sha256=digest(root / 'TreeDB/algorithm_work_bench_test.go'),
                inputs=inputs, go_environment=settings,
                modules=modules,
                capture_sha256=digest(HERE / 'capture.py'),
                overlay_generator_sha256=digest(root / 'scripts/treedb_algorithm_work_overlay.py'))


def benchmark_names(family):
    if family == 'writes':
        return {f'BenchmarkAlgorithmSparseUpdates/pointer={p}/checkpoints={c}/wide={w}'
                for p, c, w in itertools.product(('false', 'true'), (4, 1), ('false', 'true'))}
    return {f'BenchmarkAlgorithmGetMany/pointer={p}/{s}/view={v}'
            for p, s, v in itertools.product(('false', 'true'), ('sorted', 'clustered', 'uniform'), ('false', 'true'))}


def validate_packet(packet, pilot, small):
    keys, updates = (8192, 8000) if pilot else (250000, 40000)
    expected = dict(schema='algorithm-work-v1', keys=keys, updates=updates,
                    commits=updates // 1000, ack_batch_ops=1000, permutation_multiplier=7919,
                    key_bytes=32, value_bytes=256, allowed_concurrent_generations=[0, 1],
                    flush_threshold=(1 if small else 64) << 20,
                    read_sample_stride=16, validated_all_values_and_misses=True, final_close_checked=True)
    if any(packet.get(k) != v for k, v in expected.items()):
        raise ValueError('wrong fixture/operation/closure counts')
    reads, samples = packet['read_count'], packet['read_samples']
    if not (reads >= 16 and samples == reads // 16 and 0 < samples <= 65536):
        raise ValueError('incomplete/overflowed read samples')
    if (packet['checkpoints'] not in (1, 4) or packet['pointer_threshold'] not in (1, 16384)
            or not isinstance(packet['coalescing_wide'], bool)):
        raise ValueError('wrong write control')
    if not (0 < packet['ack_ns'] and 0 < packet['checkpoint_ns'] and packet['ack_ns'] + packet['checkpoint_ns'] <= packet['interval_ns']
            and 0 <= packet['read_p99_ns'] <= packet['read_p999_ns'] <= packet['read_max_ns']):
        raise ValueError('invalid phase/tail interval')
    for key in ('before', 'after'):
        if not packet.get(key) or 'treedb.command_wal.applied_lsn' not in packet[key]:
            raise ValueError('missing work/LSN counters')
    after = packet['after']
    for key in WORK_COUNTERS:
        if key not in packet['before'] or key not in after or not (0 <= int(packet['before'][key]) <= int(after[key])):
            raise ValueError('missing/reset physical-work counter')
    if int(after['treedb.command_wal.applied_lsn']) < int(after['treedb.command_wal.live_accepted_max_lsn']):
        raise ValueError('incomplete checkpoint closure')


def validate_log(text, family, freeze, pilot, small):
    if not re.search(r'^PASS$', text, re.M) or re.search(r'^(FAIL|panic:|fatal error:)', text, re.M):
        raise ValueError('incomplete/failed test process')
    if family == 'internal':
        pattern = r'pointer=(false|true) shape=(sorted|clustered|uniform) view=(false|true) batches=128 keys_per_batch=64 internal_visits=(\d+) potential_union_visits=(\d+) repeated_visits=(\d+)$'
        rows = {}
        for line in text.splitlines():
            match = re.search(pattern, line)
            if match:
                p, s, v, actual, union, repeated = match.groups()
                key = (p, s, v)
                actual, union, repeated = map(int, (actual, union, repeated))
                if key in rows or not (0 < union <= actual and repeated == actual - union):
                    raise ValueError('invalid/duplicate internal counters')
                rows[key] = dict(actual=actual, potential_union=union, repeated=repeated)
        if set(rows) != set(itertools.product(('false', 'true'), ('sorted', 'clustered', 'uniform'), ('false', 'true'))):
            raise ValueError('incomplete internal counter cells')
        return {'internal_counters': {'/'.join(k): v for k, v in rows.items()}}
    rows, packets = {}, []
    for line in text.splitlines():
        match = re.match(r'^(Benchmark\S+)-\d+\s+(\d+)\s+(.*)$', line)
        if match:
            name, iterations, fields = match.groups()
            fields = fields.split()
            if name in rows or len(fields) % 2:
                raise ValueError('duplicate/malformed benchmark row')
            metrics = dict(zip(fields[1::2], map(float, fields[::2])))
            if len(metrics) != len(fields) // 2 or any(not math.isfinite(v) or v < 0 for v in metrics.values()):
                raise ValueError('invalid benchmark units/values')
            if not {'ns/op', 'B/op', 'allocs/op'} <= metrics.keys() or metrics['ns/op'] <= 0:
                raise ValueError('missing timing/allocation metrics')
            if (family == 'writes' and int(iterations) != 1) or (family == 'many' and (int(iterations) != 1000 or metrics.get('keys/op') != 64)):
                raise ValueError('wrong operation count')
            rows[name] = dict(iterations=int(iterations), metrics=metrics)
        # Go testing logs JSON on its own stdout line. Never repair a split line
        # or splice engine stderr diagnostics into a benchmark record.
        marker = line.find('{"ack_batch_ops":')
        if marker >= 0:
            packet = json.loads(line[marker:])
            validate_packet(packet, pilot, small)
            provenance = packet['provenance']
            if provenance.get('pilot') != pilot or (not pilot and any(provenance.get(k) != freeze[k] for k in (
                    'runtime_head', 'runtime_tree', 'harness_sha256', 'binary_sha256'))):
                raise ValueError('wrong runtime/harness/binary provenance')
            packets.append(packet)
    if set(rows) != benchmark_names(family):
        raise ValueError('missing/unexpected benchmark cells')
    if family == 'writes':
        controls = {(p['pointer_threshold'], p['checkpoints'], p['coalescing_wide']) for p in packets}
        if len(packets) != 8 or controls != set(itertools.product((1, 16384), (1, 4), (False, True))):
            raise ValueError('missing/duplicate write packets')
    return dict(rows=rows, write_packets=packets)


def execution_command(family, binary):
    if family == 'internal':
        return [str(binary), '-test.run=^TestAlgorithmWorkInternalVisits$', '-test.count=1', '-test.v', '-test.timeout=30m']
    benchmark = 'SparseUpdates' if family == 'writes' else 'GetMany'
    return [str(binary), '-test.run=^$', '-test.bench=^BenchmarkAlgorithm' + benchmark + '$', '-test.benchtime=' + ('1x' if family == 'writes' else '1000x'), '-test.count=1', '-test.benchmem', '-test.v', '-test.timeout=30m']


def validate_capture(output, freeze):
    record = json.loads((output / 'execution.json').read_text())
    if not record.get('complete') or record.get('returncode') != 0 or record.get('source_before') != record.get('source_after'):
        raise ValueError('failed/incomplete/source-changed capture')
    if record.get('freeze_sha256') != digest(Path(record['prepared']) / 'freeze.json'):
        raise ValueError('changed external freeze')
    if record.get('binary_sha256') != freeze['binary_sha256']:
        raise ValueError('wrong binary identity')
    if (record.get('command') != execution_command(record['family'], Path(record['prepared']) / 'algorithm-work.test')
            or record.get('environment') != freeze['environment'] or record.get('environment') != RUNTIME_ENV
            or not record.get('grant')):
        raise ValueError('wrong execution command/environment/grant')
    if record['source_before'] != frozen_identity(freeze):
        raise ValueError('capture differs from source/dependency freeze')
    for name in ('stdout', 'stderr'):
        if record.get(name + '_sha256') != digest(output / (name + '.log')):
            raise ValueError('changed raw stream')
    return validate_log((output / 'stdout.log').read_text(), record['family'], freeze, record['pilot'], record['small_flush'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('prepare', 'capture', 'validate'))
    parser.add_argument('--source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--prepared')
    parser.add_argument('--grant')
    parser.add_argument('--family', choices=('writes', 'many', 'internal'), default='writes')
    parser.add_argument('--overlay', help='counter-only overlay.json; separate prepared diagnostic binary')
    parser.add_argument('--pilot', action='store_true', help='explicitly unretained constructor qualification')
    parser.add_argument('--small-flush', action='store_true')
    args = parser.parse_args()
    if args.mode == 'validate':
        output = Path(args.output).resolve()
        record = json.loads((output / 'execution.json').read_text())
        freeze = json.loads((Path(record['prepared']) / 'freeze.json').read_text())
        validate_capture(output, freeze)
        print('Validated immutable raw capture')
        return
    if args.mode == 'capture' and (not args.prepared or not args.grant):
        parser.error('capture requires a prepared binary and coordinator exclusive grant')
    if bool(args.overlay) != (args.family == 'internal'):
        parser.error('internal family requires its overlay; timed families forbid overlays')
    root, output = Path(args.source).resolve(), Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = normalized_environment(os.environ)
    go = str(Path(env.get('GOROOT') or subprocess.check_output(['go', 'env', 'GOROOT'], text=True).strip()) / 'bin/go')
    overlay = Path(args.overlay).resolve() if args.overlay else None
    before = identity(root, go, env, overlay)
    freeze = dict(before, source=str(root), environment={k: env[k] for k in RUNTIME_ENV}, complete=False)
    if args.mode == 'prepare':
        binary = output / 'algorithm-work.test'
        command = [go, 'test', *(['-overlay=' + str(overlay)] if overlay else []), '-c', '-o', str(binary), './TreeDB']
    else:
        prepared = Path(args.prepared).resolve()
        freeze = json.loads((prepared / 'freeze.json').read_text())
        binary = prepared / 'algorithm-work.test'
        if (not freeze.get('complete') or frozen_identity(freeze) != before
                or freeze.get('environment') != {k: env[k] for k in RUNTIME_ENV}
                or digest(binary) != freeze['binary_sha256']):
            raise ValueError('source/dependency/toolchain/binary changed since preparation')
        env.update(TREEDB_ALGORITHM_RUNTIME_HEAD=freeze['runtime_head'], TREEDB_ALGORITHM_FREEZE_FILE=str(prepared / 'freeze.json'),
                   TREEDB_ALGORITHM_PILOT='1' if args.pilot else '0', TREEDB_ALGORITHM_SMALL_FLUSH='1' if args.small_flush else '0')
        command = execution_command(args.family, binary)
    record = dict(command=command, environment={k: env[k] for k in RUNTIME_ENV}, source_before=before, grant=args.grant,
                  pilot=args.pilot, small_flush=args.small_flush, family=args.family, complete=False)
    if args.mode == 'capture':
        record.update(prepared=str(prepared), freeze_sha256=digest(prepared / 'freeze.json'))
    packet = output / 'execution.json'
    packet.write_text(json.dumps(record, indent=2) + '\n')
    with (output / 'stdout.log').open('x') as stdout, (output / 'stderr.log').open('x') as stderr:
        began = time.time()
        result = subprocess.run(command, cwd=root, env=env, stdout=stdout, stderr=stderr)
    record.update(returncode=result.returncode, started_unix=began, ended_unix=time.time(),
                  stdout_sha256=digest(output / 'stdout.log'), stderr_sha256=digest(output / 'stderr.log'))
    packet.write_text(json.dumps(record, indent=2) + '\n')
    if result.returncode:
        raise SystemExit('failed process; incomplete raw packet retained')
    after = identity(root, go, env, overlay)
    if before != after:
        raise ValueError('source/dependency/toolchain changed during execution')
    if args.mode == 'prepare':
        freeze.update(binary_sha256=digest(binary), build_command=command,
                      binary_build_info=query([go, 'version', '-m', str(binary)], root, env), complete=True)
        (output / 'freeze.json').write_text(json.dumps(freeze, indent=2) + '\n')
    else:
        if digest(binary) != freeze['binary_sha256']:
            raise ValueError('binary changed during execution')
        parsed = validate_log((output / 'stdout.log').read_text(), args.family, freeze, args.pilot, args.small_flush)
        (output / 'parsed.json').write_text(json.dumps(parsed, indent=2) + '\n')
    record.update(source_after=after, binary_sha256=digest(binary), complete=True)
    packet.write_text(json.dumps(record, indent=2) + '\n')
    if args.mode == 'capture':
        validate_capture(output, freeze)
    for name in ('stdout.log', 'stderr.log', 'execution.json'):
        (output / name).chmod(0o444)
    print('Complete preparation; no timing' if args.mode == 'prepare' else 'Complete capture; raw streams retained separately')


if __name__ == '__main__':
    main()
