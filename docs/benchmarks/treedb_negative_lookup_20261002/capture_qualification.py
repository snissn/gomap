#!/usr/bin/env python3
"""Prepare frozen binaries; collect only under an exclusive Linux timing grant."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from parse_qualification import (BINARY_SPECS, binary_id, digest, execution_command,
                                 plan, script_identity, validate, validate_preparation)


def source_identity(root, manifest):
    lines = Path(manifest).read_text().splitlines()
    for line in lines:
        expected, relative = line.split('  ', 1)
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts or digest(root / path) != expected:
            raise ValueError(f'source mismatch: {relative}')
    # Detect extra visible Go/module sources, including transfer metadata.
    actual = subprocess.check_output(['rg', '--files', '--no-require-git', '-g', '*.go', '-g', 'go.mod', '-g', 'go.sum'], cwd=root, text=True).splitlines()
    if set(actual) != {line.split('  ', 1)[1] for line in lines}:
        raise ValueError('source inventory differs from frozen manifest')
    return digest(manifest)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'reference', 'output', 'candidate-head', 'reference-head',
                 'candidate-manifest', 'reference-manifest'):
        ap.add_argument('--' + name, required=True)
    ap.add_argument('--prepare', action='store_true', help='compile only; no benchmark execution')
    ap.add_argument('--prepared-directory')
    ap.add_argument('--grant')
    args = ap.parse_args()
    if not args.prepare and (not args.prepared_directory or not args.grant):
        ap.error('collection requires --prepared-directory and --grant')
    roots = {'candidate': Path(args.source).resolve(), 'reference': Path(args.reference).resolve()}
    inventories = {'candidate': Path(args.candidate_manifest).resolve(), 'reference': Path(args.reference_manifest).resolve()}
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOROOT='/home/mikers/.gvm/gos/go1.26.3',
               PATH='/home/mikers/.gvm/gos/go1.26.3/bin:/usr/bin:/bin',
               GOWORK='off', GOMAXPROCS='2' if args.prepare else '4',
               GOMEMLIMIT='2GiB' if args.prepare else '1GiB')
    env.pop('TREEDB_HOT_PATH_STATS', None)
    env['GOTMPDIR'] = str(output / 'go-tmp')
    Path(env['GOTMPDIR']).mkdir()
    before = {k: source_identity(roots[k], inventories[k]) for k in roots}
    host = {k: subprocess.check_output(command, text=True).strip() for k, command in {
        'kernel': ['uname', '-a'], 'cpu': ['lscpu'], 'memory': ['cat', '/proc/meminfo'],
        'load': ['cat', '/proc/loadavg'], 'disk': ['df', '-B1', str(output)]}.items()}
    host['go'] = subprocess.check_output([env['GOROOT'] + '/bin/go', 'version'], env=env, text=True).strip()
    if 'go1.26.3 linux/amd64' not in host['go']:
        raise ValueError('unexpected Go toolchain')
    scripts = script_identity()
    if args.prepare:
        prepared = dict(candidate_head=args.candidate_head, reference_head=args.reference_head,
                        roots={k: str(v) for k, v in roots.items()}, source_before=before,
                        scripts_before=scripts, host=host, compiles=[], binaries_before={}, complete=False,
                        environment={k: env[k] for k in ('GOROOT', 'GOWORK', 'GOMAXPROCS', 'GOMEMLIMIT', 'GOTMPDIR')})
        packet = output / 'prepare.json'
        for identifier, revision, package in BINARY_SPECS:
            binary = output / (identifier + '.test')
            command = [env['GOROOT'] + '/bin/go', 'test', '-c', '-o', str(binary), package]
            log = output / (identifier + '.compile.log')
            began = time.time()
            with log.open('w') as stream:
                result = subprocess.run(command, cwd=roots[revision], env=env, stdout=stream, stderr=subprocess.STDOUT)
            prepared['compiles'].append(dict(id=identifier, command=command, cwd=str(roots[revision]),
                                             source_manifest_sha256=before[revision], returncode=result.returncode,
                                             started_unix=began, ended_unix=time.time(), log_sha256=digest(log)))
            packet.write_text(json.dumps(prepared, indent=2, sort_keys=True) + '\n')
            if result.returncode:
                raise SystemExit(f'{identifier} compilation failed; incomplete packet retained')
            prepared['binaries_before'][identifier] = dict(path=str(binary), sha256=digest(binary))
            print(f'{identifier} compiled {digest(binary)}', flush=True)
        prepared['source_after'] = {k: source_identity(roots[k], inventories[k]) for k in roots}
        prepared['scripts_after'] = script_identity()
        prepared['binaries_after'] = {k: dict(path=v['path'], sha256=digest(v['path']))
                                       for k, v in prepared['binaries_before'].items()}
        prepared['complete'] = True
        packet.write_text(json.dumps(prepared, indent=2, sort_keys=True) + '\n')
        validate_preparation(output)
        print('Preparation complete; no benchmarks executed', flush=True)
        return
    preparation_directory = Path(args.prepared_directory).resolve()
    prepared = validate_preparation(preparation_directory)
    if (before != prepared['source_before'] or scripts != prepared['scripts_before']
            or args.candidate_head != prepared['candidate_head'] or args.reference_head != prepared['reference_head']
            or {k: str(v) for k, v in roots.items()} != prepared['roots']):
        raise ValueError('collection differs from frozen preparation')
    manifest = dict(classification='bounded PR qualification; authoritative campaign waits landed H',
                    candidate_head=args.candidate_head, reference_head=args.reference_head,
                    grant=args.grant, source_before=before, host=host, scripts_before=scripts,
                    binaries_before=prepared['binaries_after'], preparation_directory=str(preparation_directory),
                    preparation_sha256=digest(preparation_directory / 'prepare.json'),
                    environment={k: env[k] for k in ('GOROOT', 'GOWORK', 'GOMAXPROCS', 'GOMEMLIMIT', 'GOTMPDIR')},
                    expected_performance_samples=930, expected_diagnostic_rows=160,
                    expected_processes=36, nominal_timed_seconds=450, runs=[], complete=False)

    def save():
        (output / 'capture.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')

    save()
    started = time.monotonic()
    for job in plan():
        run_env = dict(env)
        if job['counters']:
            run_env['TREEDB_HOT_PATH_STATS'] = '1'
        binary = manifest['binaries_before'][binary_id(job)]
        command = execution_command(job, binary)
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {job['id']} starting", flush=True)
        log = output / (job['id'] + '.log')
        began = time.time()
        with log.open('w') as stream:
            result = subprocess.run(command, cwd=roots[job['revision']], env=run_env,
                                    stdout=stream, stderr=subprocess.STDOUT)
        record = dict(job=job, command=command, cwd=str(roots[job['revision']]),
                      binary_sha256=binary['sha256'],
                      started_unix=began, ended_unix=time.time(), returncode=result.returncode,
                      log_sha256=digest(log))
        manifest['runs'].append(record)
        save()
        if result.returncode:
            raise SystemExit(f"{job['id']} failed; retained incomplete packet")
        # Reject missing routes immediately, before spending more runner time.
        from parse_qualification import parse_log
        parse_log(log.read_text(), job)
        print(f"{job['id']} complete ({time.monotonic()-started:.1f}s elapsed)", flush=True)
    manifest['source_after'] = {k: source_identity(roots[k], inventories[k]) for k in roots}
    manifest['scripts_after'] = script_identity()
    manifest['binaries_after'] = {k: dict(path=v['path'], sha256=digest(v['path']))
                                  for k, v in manifest['binaries_before'].items()}
    manifest['elapsed_seconds'] = time.monotonic() - started
    manifest['complete'] = True
    save()
    summary = validate(output)
    (output / 'parsed.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    print(f"Complete: {summary['performance_samples']} performance samples, {summary['diagnostic_rows']} diagnostic rows", flush=True)


if __name__ == '__main__':
    main()
