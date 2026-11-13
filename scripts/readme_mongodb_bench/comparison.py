#!/usr/bin/env python3
"""Explicit Gomap-only side freeze for the original YCSB recipe (no builds)."""
import argparse
import fcntl
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

import run

HELPERS = ('go-ycsb', 'census', 'ycsb-correctness-fixture')
RECIPES = ('run.py', 'comparison.py', 'ycsb_log.py', 'census.go')
PROFILE = 'command_wal_relaxed'
HOST_LOCK = Path('/tmp/gomap-ycsb-qualification.lock')


def require_default_environment(*environments):
    for environment in environments:
        for key in environment:
            run.require(not key.startswith(('GOMAP_READ_', 'TREEDB_', 'GODEBUG', 'GOMEMLIMIT', 'GOGC', 'GOEXPERIMENT')),
                        'observer/runtime override present: '+key)


def output(argv, env=None):
    return subprocess.check_output([str(x) for x in argv], env=env, text=True, timeout=30).strip()


def source(path):
    run.require(not output(['git', '-C', path, 'status', '--porcelain', '--untracked-files=all']),
                'source checkout is dirty: '+str(path))
    return dict(path=str(path), commit=output(['git', '-C', path, 'rev-parse', 'HEAD']),
                tree=output(['git', '-C', path, 'rev-parse', 'HEAD^{tree}']))


def build_identity(receipt, repo, binary):
    run.require(type(receipt['schema_version']) is int and receipt['schema_version'] == 1, 'invalid build receipt schema')
    run.require(receipt['source_commit'] == repo['commit'] and receipt['source_tree'] == repo['tree'],
                'build source identity mismatch')
    run.require(receipt['binary_sha256'] == run.sha(binary), 'build binary hash mismatch')
    env = os.environ.copy()
    env.update(receipt['environment'])
    require_default_environment(env)
    run.require(env['GOTOOLCHAIN'] in ('local', 'go1.26.10') and env['GOWORK'] == 'off' and not env.get('GOFLAGS'),
                'build environment must pin Go 1.26.10 without overlays/GOFLAGS')
    argv = receipt['argv']
    go = Path(argv[0])
    run.require(go.is_absolute() and go.resolve() == (Path(env['GOROOT'])/'bin/go').resolve(),
                'build driver does not match GOROOT')
    build_args = argv[1:]
    if build_args[:2] == ['build', '-buildvcs=true']:
        build_args = build_args[:1]+build_args[2:]
    run.require(len(build_args) == 4 and build_args[:2] == ['build', '-o']
                and Path(build_args[2]).is_absolute() and build_args[3] == './TreeDB/mongo_gateway/server.go',
                'unexpected gateway build argv')
    version = output([go, 'version'], env)
    go_env = run.strict_json(output([go, 'env', '-json', 'GOVERSION', 'GOROOT', 'GOTOOLDIR'], env))
    compiler = output([go, 'tool', 'compile', '-V'], env)
    info = output([go, 'version', '-m', binary], env)
    run.require(go_env['GOVERSION'] == 'go1.26.10' and 'go1.26.10' in version
                and compiler == 'compile version go1.26.10'
                and Path(go_env['GOROOT']).resolve() == Path(env['GOROOT']).resolve()
                and Path(go_env['GOTOOLDIR']).resolve().is_relative_to(Path(env['GOROOT']).resolve()),
                'driver/GOROOT/compiler mismatch')
    def info_identity(text):
        lines = text.splitlines()
        return [lines[0].rsplit(': ', 1)[-1]]+lines[1:]
    run.require((version, go_env, compiler, info_identity(info)) ==
                (receipt['go_version'], receipt['go_env'], receipt['compiler_version'],
                 info_identity(receipt['binary_build_info'])), 'build toolchain receipt changed')
    run.require(info.splitlines()[0].endswith(': go1.26.10') and '-overlay' not in info
                and '-race' not in info, 'gateway is not a pinned uninstrumented build')
    # File-list builds may omit VCS settings, even with -buildvcs=true. The
    # receipt's clean source/tree and binary hashes remain mandatory above.
    settings = dict(line.strip().removeprefix('build\t').split('=', 1)
                    for line in info.splitlines() if line.strip().startswith('build\t') and '=' in line)
    run.require(not any(settings.get(key) for key in
                ('GOEXPERIMENT', '-overlay', '-gcflags', '-ldflags', '-tags', '-pgo',
                 'CGO_CFLAGS', 'CGO_CPPFLAGS', 'CGO_CXXFLAGS', 'CGO_LDFLAGS'))
                and all(settings.get(key) in (None, 'false') for key in ('-race', '-asan', '-msan', '-cover')),
                'gateway has custom build settings')
    if 'vcs.revision' in settings or 'vcs.modified' in settings:
        run.require(settings.get('vcs.revision') == repo['commit'] and settings.get('vcs.modified') == 'false',
                    'gateway VCS build identity mismatch')


def host():
    return dict(hostname=platform.node(), platform=platform.platform(), logical_cpus=os.cpu_count(),
                gomaxprocs='unset; runtime value not independently observed')


def verify(manifest, root, repo):
    run.require(type(manifest['schema_version']) is int and manifest['schema_version'] == 1 and manifest['kind'] == 'gomap-ycsb-side-freeze',
                'invalid comparison manifest')
    run.require(manifest['case'] == dict(count=100000, operations=manifest['case']['operations'],
                profile=PROFILE, ack='relaxed', target='gomap', workers=16)
                and manifest['case']['operations'] in (10000, 1000000), 'invalid selected case')
    run.require(manifest['host'] == host(), 'comparison host changed')
    require_default_environment(os.environ, run.ENV)
    run.require(source(repo) == manifest['product'], 'product source changed')
    original = Path(manifest['original_freeze']['path'])
    run.require(run.sha(original) == manifest['original_freeze']['sha256'], 'original freeze changed')
    frozen = run.strict_json(original.read_text())
    for name in HELPERS:
        expected = frozen['binaries'][name]
        run.require(run.sha(original.parent/'bin'/name) == expected and run.sha(root/'bin'/name) == expected,
                    'frozen helper changed: '+name)
    run.require(run.sha(Path(__file__).with_name('census.go')) == frozen['recipe_sources']['census.go'],
                'census source differs from frozen helper source')
    for name, expected in frozen['recipe_sources'].items():
        run.require(run.sha(original.parent/'recipe'/name) == expected, 'original recipe changed: '+name)
    client = frozen['sources'][1]
    identity = source(Path(manifest['client']['path']))
    run.require(identity == manifest['client'] and identity['commit'] == client['commit']
                and identity['tree'] == client['root_tree'], 'external client source changed')
    run.require(source(Path(manifest['harness']['path'])) == manifest['harness'], 'harness source changed')
    for name in RECIPES:
        run.require(run.sha(Path(__file__).with_name(name)) == manifest['recipe_sources'][name],
                    'comparison recipe changed: '+name)
    receipt_path = Path(manifest['build_receipt']['path'])
    run.require(run.sha(receipt_path) == manifest['build_receipt']['sha256'], 'build receipt changed')
    build_identity(run.strict_json(receipt_path.read_text()), manifest['product'], root/'bin/treedb-mongo-gateway')


def freeze(args):
    root, repo = args.root.resolve(), args.repo.resolve()
    original, receipt = args.original_freeze.resolve(), args.build_receipt.resolve()
    run.require(run.sha(original) == args.expected_original_sha256, 'original freeze hash mismatch')
    frozen = run.strict_json(original.read_text())
    client = source(args.client_repo.resolve() if args.client_repo else Path(frozen['sources'][1]['path']))
    harness = source(Path(__file__).resolve().parents[2])
    for name in RECIPES:
        tracked = output(['git', '-C', harness['path'], 'rev-parse', 'HEAD:scripts/readme_mongodb_bench/'+name])
        actual = output(['git', '-C', harness['path'], 'hash-object', Path(__file__).with_name(name)])
        run.require(actual == tracked, 'uncommitted recipe: '+name)
    value = dict(schema_version=1, kind='gomap-ycsb-side-freeze', created_utc=run.now(),
                 product=source(repo), harness=harness, client=client, host=host(),
                 original_freeze=dict(path=str(original), sha256=run.sha(original)),
                 build_receipt=dict(path=str(receipt), sha256=run.sha(receipt)),
                 recipe_sources={n: run.sha(Path(__file__).with_name(n)) for n in RECIPES},
                 case=dict(count=100000, operations=args.operations, profile=PROFILE,
                           ack='relaxed', target='gomap', workers=16))
    verify(value, root, repo)
    run.save(root/'comparison-freeze.json', value)
    print(run.sha(root/'comparison-freeze.json'))


def bounded_case(call, seconds=600):
    def interrupt(signum, frame):
        raise TimeoutError('case work deadline exceeded' if signum == signal.SIGALRM else 'case interrupted')
    signals = (signal.SIGALRM, signal.SIGINT, signal.SIGTERM)
    previous = {sig: signal.signal(sig, interrupt) for sig in signals}
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        return call()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def process_snapshot():
    try:
        return dict(ps=output(['ps', '-axo', 'pid,ppid,comm,%cpu,%mem,rss']))
    except (OSError, subprocess.SubprocessError) as error:
        return dict(unavailable=repr(error))


def execute(args):
    run.require(args.attempt_label and run.re.fullmatch('[a-z0-9-]+', args.attempt_label),
                'single case requires a unique attempt label')
    run.require(args.operations in (10000, 1000000) and args.expected_comparison_sha256,
                'single case requires operations and external comparison manifest hash')
    run.ROOT, run.REPO = args.root.resolve(), args.repo.resolve()
    run.BIN = run.ROOT/'bin'
    # The host lock is shared by every comparison side root on this host.
    with HOST_LOCK.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run.OUT = run.ROOT/'runs'/('ycsb-single-'+args.attempt_label)
        run.OUT.mkdir(parents=True, exist_ok=False)
        started = time.monotonic()
        run.save(run.OUT/'selector-attempt.json', dict(started_utc=run.now(), host=host(), argv=sys.argv,
                 process_snapshot=process_snapshot(),
                 environment={k: run.ENV.get(k) for k in ('GOTOOLCHAIN', 'GOWORK', 'GOMAXPROCS', 'GOROOT')},
                 operations=args.operations,
                 telemetry='pre/post only; allocation, retained/peak RSS and maintenance metrics unavailable'))
        try:
            path = run.ROOT/'comparison-freeze.json'
            run.require(run.sha(path) == args.expected_comparison_sha256, 'comparison manifest changed')
            value = run.strict_json(path.read_text())
            run.require(value['case']['operations'] == args.operations, 'selected operation count changed')
            run.COMPARISON = (value, args.expected_comparison_sha256)
            frozen = run.strict_json(Path(value['original_freeze']['path']).read_text())
            run.FREEZE = dict(product_commit=value['product']['commit'], binaries=dict(frozen['binaries']))
            run.FREEZE['binaries']['treedb-mongo-gateway'] = run.sha(run.BIN/'treedb-mongo-gateway')
            run.FREEZE_HASH = args.expected_comparison_sha256
            bounded_case(lambda: run.run_case('ycsb', 'relaxed', 'gomap', 1, 100000, args.operations))
        except BaseException as error:
            run.save(run.OUT/'selector-failure.json', dict(error=repr(error), ended_utc=run.now(),
                     elapsed_seconds=time.monotonic()-started, host=host()))
            raise
        finally:
            run.save(run.OUT/'selector-end.json', dict(ended_utc=run.now(), host=host(),
                     process_snapshot=process_snapshot(),
                     elapsed_seconds=time.monotonic()-started))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--original-freeze', type=Path, required=True)
    parser.add_argument('--expected-original-sha256', required=True)
    parser.add_argument('--build-receipt', type=Path, required=True)
    parser.add_argument('--client-repo', type=Path)
    parser.add_argument('--operations', type=int, choices=(10000, 1000000), required=True)
    freeze(parser.parse_args())


if __name__ == '__main__':
    main()
