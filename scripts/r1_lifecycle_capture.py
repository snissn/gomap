#!/usr/bin/env python3
"""Source-bound fresh-process capture for the standalone #5060 diagnostic."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import time

from r1_collection_source import source_identity as collection_source
from r1_lifecycle_validate import hexadecimal, source_valid, validate, summarize, working_set, SCOPE, SCHEDULE


def run(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def digest_manifest(manifest):
    h = hashlib.sha256()
    for path, value in sorted(manifest.items()):
        h.update(path.encode() + b'\0' + value.encode() + b'\0')
    return h.hexdigest()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source(go):
    identity = collection_source()
    package = json.loads(run([go, 'list', '-json', './TreeDB/collections']))
    tests = sorted('TreeDB/collections/' + name for key in ('TestGoFiles', 'XTestGoFiles')
                   for name in package.get(key, []))
    if 'TreeDB/collections/r1_lifecycle_5060_bench_test.go' not in tests:
        raise ValueError('lifecycle benchmark excluded from compiled test sources')
    paths = tests + sorted(str(p) for p in Path('scripts').glob('r1_lifecycle*')
                           if p.suffix in ('.py', '.sh')) + ['scripts/r1_collection_source.py']
    manifest = {path: sha(path) for path in paths}
    # A's reviewed committed runtime blob inventory plus actual bytes guards
    # dirty rehearsal inputs and changes during capture. Docs/artifacts excluded.
    actual_runtime = {path: sha(path) for path in identity['runtime_blobs']}
    return {'commit': identity['commit'], 'clean': identity['clean'],
            'runtime_sha256': identity['runtime_sha256'],
            'runtime_blobs': identity['runtime_blobs'],
            'runtime_working_sha256': digest_manifest(actual_runtime),
            'runtime_files': actual_runtime, 'compiled_test_files': tests,
            'harness_files': manifest, 'harness_sha256': digest_manifest(manifest)}


def observation():
    return {'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'monotonic_ns': time.monotonic_ns(), 'loadavg': list(os.getloadavg()),
            'cpu_count': os.cpu_count(),
            'affinity': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None}


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    parser.add_argument('--qualification', required=True, choices=('rehearsal', 'retained'))
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--epochs', type=int, default=5)
    parser.add_argument('--documents', type=int, default=4096)
    parser.add_argument('--calls-per-epoch', type=int, default=1024)
    parser.add_argument('--source-commit')
    parser.add_argument('--runtime-sha256')
    parser.add_argument('--harness-sha256')
    parser.add_argument('--landed-tooling-commit')
    parser.add_argument('--review-url')
    args = parser.parse_args()
    frozen_environment = {'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'GOFLAGS': ''}
    for key, value in frozen_environment.items():
        os.environ.setdefault(key, value)
        if os.environ[key] != value:
            parser.error(f'{key} must equal {value!r} for this frozen diagnostic')
    for value, multiple in ((args.documents, 32), (args.calls_per_epoch, 8)):
        if value < multiple or value % multiple or value > 1 << 20:
            parser.error(f'dimension must be a multiple of {multiple} in [{multiple},1048576]')
    if not 1 <= args.epochs <= 1000 or not 1 <= args.repetitions <= 100:
        parser.error('epochs must be in [1,1000], repetitions in [1,100]')
    if args.qualification == 'retained':
        if args.epochs != 5 or args.repetitions < 5 or args.documents != 4096 or args.calls_per_epoch != 1024:
            parser.error('retained default fixture requires >=5 processes, 5 epochs, 4096 documents, 1024 calls/epoch')
        if not all((args.source_commit, args.runtime_sha256, args.harness_sha256,
                    args.landed_tooling_commit, args.review_url)):
            parser.error('retained capture requires exact frozen identities, landed tooling commit and review URL')
        if not hexadecimal(args.landed_tooling_commit, 40) or not args.review_url.startswith('https://github.com/'):
            parser.error('retained capture requires an exact landed tooling SHA and GitHub review URL')
    os.environ['GOWORK'] = 'off'
    os.environ['GOTOOLCHAIN'] = 'local'
    go = os.environ.get('R1_GO', 'go')
    before = source(go)
    source_valid(before)
    expected = {'commit': args.source_commit, 'runtime_sha256': args.runtime_sha256,
                'harness_sha256': args.harness_sha256}
    for key, value in expected.items():
        if value is not None and before[key] != value:
            raise ValueError(f'frozen {key} mismatch')
    if args.qualification == 'retained':
        if not before['clean']:
            raise ValueError('retained source must be committed and clean')
        subprocess.run(['git', 'merge-base', '--is-ancestor', args.landed_tooling_commit, before['commit']], check=True)
    out = Path(args.out).resolve()
    if out == Path.cwd() or Path.cwd() in out.parents:
        raise ValueError('capture output must be outside the source checkout')
    # Never mix packets, old raw data, or calibration runs from prior binaries.
    out.mkdir(parents=True, exist_ok=False)
    benchmark_tmpdir = out / 'benchmark-tmp'
    benchmark_tmpdir.mkdir()
    write(out / 'source-before.json', before)
    env = json.loads(run([go, 'env', '-json']))
    cc = shlex.split(env['CC'])
    toolchain = {'go': run([go, 'version']), 'go_env': env,
                 'cc': run([*cc, '--version']), 'uname': platform.uname()._asdict(),
                 'environment': {k: os.environ.get(k) for k in ('GOMAXPROCS', 'GOGC', 'GOMEMLIMIT', 'GOCACHE', 'GOMODCACHE', 'TMPDIR', 'GOFLAGS')},
                 'cpu': Path('/proc/cpuinfo').read_text() if Path('/proc/cpuinfo').exists() else platform.processor(),
                 'capture_directory': str(out), 'benchmark_tmpdir': str(benchmark_tmpdir),
                 'capture_filesystem_device': out.stat().st_dev,
                 'benchmark_filesystem_device': benchmark_tmpdir.stat().st_dev,
                 'benchmark_filesystem': run(['df', '-Pk', str(benchmark_tmpdir)]),
                 'process_environment': {**frozen_environment, 'TMPDIR': str(benchmark_tmpdir), 'GOWORK': 'off', 'GOTOOLCHAIN': 'local',
                                         'GOMAP_R1_LIFECYCLE_DOCUMENTS': str(args.documents),
                                         'GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH': str(args.calls_per_epoch)},
                 'filesystem': run(['df', '-Pk', str(out)])}
    binary = out / 'collections.test'
    command = [go, 'test', '-c', '-o', str(binary), './TreeDB/collections']
    with (out / 'build.log').open('w') as log:
        built = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    if built.returncode:
        raise ValueError(f'build failed; preserved {out}/build.log')
    toolchain['binary_sha256'] = sha(binary)
    toolchain['binary_buildinfo'] = run([go, 'version', '-m', str(binary)])
    write(out / 'toolchain.json', toolchain)
    config = {'qualification': args.qualification, 'repetitions': args.repetitions,
              'epochs': args.epochs, 'documents': args.documents, 'calls_per_epoch': args.calls_per_epoch,
              'recipe': 'r1MutationRow5059; ascending IDs; load batches 32; deterministic stride 37; eight-call paired mix',
              'landed_tooling_commit': args.landed_tooling_commit, 'review_url': args.review_url}
    config['working_set'] = working_set(config)
    config.update(execution_scope=SCOPE, schedule=SCHEDULE, runtime_environment=frozen_environment)
    invocation = [str(binary), '-test.run=^$', '-test.bench=^BenchmarkR1Lifecycle5060$',
                  f'-test.benchtime={args.epochs}x', '-test.count=1', '-test.benchmem', '-test.v']
    process_env = dict(os.environ, GOMAP_R1_LIFECYCLE_DOCUMENTS=str(args.documents),
                       GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH=str(args.calls_per_epoch), TMPDIR=str(benchmark_tmpdir))
    records = []
    for repetition in range(args.repetitions):
        name = f'run-{repetition + 1:03d}.log'
        start = observation()
        with (out / name).open('w') as log:
            process = subprocess.Popen(invocation, env=process_env, stdout=log, stderr=subprocess.STDOUT)
            exit_code = process.wait()
        record = {'repetition': repetition + 1, 'log': name, 'log_sha256': sha(out / name),
                  'pid': process.pid, 'exit_code': exit_code, 'before': start, 'after': observation()}
        records.append(record)
        write(out / 'runs.json', records)
        if exit_code:
            raise ValueError(f'benchmark failed; preserved {out}/{name}')
    after = source(go)
    write(out / 'source-after.json', after)
    if before != after or sha(binary) != toolchain['binary_sha256']:
        raise ValueError('product, harness, source cleanliness or binary changed during capture')
    packet = {'schema': 'gomap-r1-lifecycle-packet-v3', 'config': config,
              'source_before': before, 'source_after': after, 'toolchain': toolchain,
              'build_command': command, 'build_log_sha256': sha(out / 'build.log'),
              'invocation': invocation, 'runs': records}
    write(out / 'packet.json', packet)
    results = validate(out / 'packet.json')
    (out / 'summary.md').write_text(summarize(packet, results))
    print(f'{args.qualification}: validated {len(results)} fresh processes; {out}/packet.json')


if __name__ == '__main__':
    main()
