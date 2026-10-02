#!/usr/bin/env python3
"""Run only under the coordinator's exclusive Linux timing grant."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

from parse_qualification import digest, plan, validate


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
                 'candidate-manifest', 'reference-manifest', 'grant'):
        ap.add_argument('--' + name, required=True)
    args = ap.parse_args()
    roots = {'candidate': Path(args.source).resolve(), 'reference': Path(args.reference).resolve()}
    inventories = {'candidate': Path(args.candidate_manifest).resolve(), 'reference': Path(args.reference_manifest).resolve()}
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOROOT='/home/mikers/.gvm/gos/go1.26.3',
               PATH='/home/mikers/.gvm/gos/go1.26.3/bin:/usr/bin:/bin',
               GOWORK='off', GOMAXPROCS='4', GOMEMLIMIT='1GiB')
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
    scripts = {p.name: digest(p) for p in (Path(__file__), Path(__file__).with_name('parse_qualification.py'))}
    manifest = dict(classification='bounded PR qualification; authoritative campaign waits landed H',
                    candidate_head=args.candidate_head, reference_head=args.reference_head,
                    grant=args.grant, source_before=before, host=host, scripts=scripts,
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
        command = [env['GOROOT'] + '/bin/go', 'test', job['package'], '-run', '^$',
                   '-bench', job['pattern'], '-benchmem', '-benchtime=' + job['duration'],
                   '-count=1', '-timeout=20m']
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {job['id']} starting", flush=True)
        log = output / (job['id'] + '.log')
        began = time.time()
        with log.open('w') as stream:
            result = subprocess.run(command, cwd=roots[job['revision']], env=run_env,
                                    stdout=stream, stderr=subprocess.STDOUT)
        record = dict(job=job, command=command, cwd=str(roots[job['revision']]),
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
    manifest['elapsed_seconds'] = time.monotonic() - started
    manifest['complete'] = True
    save()
    summary = validate(output)
    (output / 'parsed.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    print(f"Complete: {summary['performance_samples']} performance samples, {summary['diagnostic_rows']} diagnostic rows", flush=True)


if __name__ == '__main__':
    main()
