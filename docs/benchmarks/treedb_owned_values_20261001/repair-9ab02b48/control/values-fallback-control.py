#!/usr/bin/env python3
"""One missing ab2/9ab control, using the already frozen Linux binaries."""
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time

ROOT = Path('/mnt/fast4tb/treedb-quicksilver-4891-values-20261001/final-9ab02b48')
OUT = ROOT.parent / 'fallback-control-ab2-9ab-v1'
GRANT = 'root-values-fallback-ab2-9ab-20261002T0648-exclusive'


def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            result.update(chunk)
    return result.hexdigest()


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def identity():
    freeze_path = ROOT / 'logs/repair-source-freeze.json'
    assert digest(freeze_path) == '28c71153df2362a6e0990955fc3f4d752a77c29ba31df96b2b5a9eee75b310da'
    freeze = json.loads(freeze_path.read_text())
    assert freeze['commits']['previous'] == 'ab2e9124fcca72710b896cf78577fa6bd37a11b0'
    assert freeze['commits']['candidate'] == '9ab02b48ce2397410940a7e42cc22bbf95a96c5a'
    for name, sha in freeze['input_sha256'].items():
        assert digest(ROOT / name) == sha, name
    sources = {}
    for revision in ('base', 'previous', 'candidate'):
        expected = json.loads((ROOT / f'logs/source-{revision}-prepared.json').read_text())
        directory = ROOT / 'src' / revision
        actual = {str(p.relative_to(directory)): {'bytes': p.stat().st_size, 'sha256': digest(p)}
                  for p in directory.rglob('*') if p.is_file()}
        assert actual == expected, revision
        sources[revision] = {'files': len(actual), 'inventory_sha256': digest(ROOT / f'logs/source-{revision}-prepared.json')}
    fixture = json.loads((ROOT / 'logs/fixture-manifest.json').read_text())
    actual_names = {str(p.relative_to(ROOT / 'fixture')) for p in (ROOT / 'fixture').rglob('*') if p.is_file()}
    assert actual_names == set(fixture['files'])
    for name, item in fixture['files'].items():
        path = ROOT / 'fixture' / name
        assert path.stat().st_size == item['bytes'] and digest(path) == item['sha256'], name
    return {'original_freeze_sha256': digest(freeze_path), 'input_sha256': freeze['input_sha256'],
            'sources': sources, 'fixture_manifest_sha256': digest(ROOT / 'logs/fixture-manifest.json'),
            'control_script_sha256': digest(Path(__file__)),
            'go_binary_sha256': digest(Path('/home/mikers/.gvm/gos/go1.26.3/bin/go'))}


def rows(stdout):
    pattern = re.compile(r'^BenchmarkFileReadAppendCompressedFallback/(nil_dst|reused_dst)-4\s+([1-9][0-9]*)\s+([0-9.]+) ns/op\s+([0-9]+) B/op\s+([0-9]+) allocs/op$')
    result = {}
    for line in stdout.splitlines():
        if not line.startswith('Benchmark'):
            continue
        match = pattern.fullmatch(line)
        assert match, line
        name, iterations, ns, allocated, allocations = match.groups()
        assert name not in result and float(ns) > 0
        assert (int(allocated), int(allocations)) == ((512, 1) if name == 'nil_dst' else (0, 0))
        result[name] = {'iterations': int(iterations), 'ns_per_op': float(ns),
                        'bytes_per_op': int(allocated), 'allocations_per_op': int(allocations)}
    assert set(result) == {'nil_dst', 'reused_dst'}
    assert 'PASS' in stdout.splitlines()
    return result


if __name__ == '__main__':
    OUT.mkdir(exist_ok=False)
    records = []
    manifest = {'schema': 'mmap-fallback-control-v1', 'grant': GRANT, 'complete': False,
                'timing_only': True, 'original_packet_modified': False, 'processes': records}
    before = identity()
    save('identity-before.json', before)
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'LANG') if k in os.environ}
    env.update(GOROOT='/home/mikers/.gvm/gos/go1.26.3', GOWORK='off', GOENV='off', GOTOOLCHAIN='local',
               GOMAXPROCS='4', GOMEMLIMIT='1GiB', TREEDB_HOT_PATH_STATS='1', TMPDIR=str(ROOT / 'tmp'))
    try:
        for repetition in range(1, 6):
            revisions = ('previous', 'candidate') if repetition % 2 else ('candidate', 'previous')
            for revision in revisions:
                name = f'{revision}-{repetition}'
                binary = ROOT / f'bin/valuelog-{revision}.test'
                argv = [str(binary), '-test.run=^$', '-test.bench=^BenchmarkFileReadAppendCompressedFallback$',
                        '-test.benchtime=1s', '-test.count=1', '-test.benchmem', '-test.timeout=3m']
                wrapped = ['/usr/bin/time', '-v', '-o', str(OUT / f'{name}.rss'), *argv]
                record = {'revision': revision, 'repetition': repetition, 'argv': argv, 'wrapper_argv': wrapped,
                          'cwd': str(ROOT), 'environment': env, 'binary_sha256': digest(binary), 'started_unix_ns': time.time_ns()}
                with (OUT / f'{name}.stdout').open('wb') as stdout, (OUT / f'{name}.stderr').open('wb') as stderr:
                    process = subprocess.Popen(wrapped, cwd=ROOT, env=env, stdout=stdout, stderr=stderr)
                    record.update(pid=process.pid, exit_code=process.wait(), finished_unix_ns=time.time_ns())
                for stream in ('stdout', 'stderr', 'rss'):
                    record[stream + '_sha256'] = digest(OUT / f'{name}.{stream}')
                records.append(record)
                save('capture.json', manifest)
                assert record['exit_code'] == 0, name
                rss = (OUT / f'{name}.rss').read_text()
                assert 'Exit status: 0' in rss
                record['max_rss_kib'] = int(re.search(r'Maximum resident set size \(kbytes\): ([0-9]+)', rss)[1])
                assert record['max_rss_kib'] > 0
                record['rows'] = rows((OUT / f'{name}.stdout').read_text())
                assert not (OUT / f'{name}.stderr').read_text().strip(), name
                save('capture.json', manifest)
                print(name + ' PASS', flush=True)
        after = identity()
        save('identity-after.json', after)
        assert after == before
        summary = {}
        for route in ('nil_dst', 'reused_dst'):
            samples = {revision: [p['rows'][route]['ns_per_op'] for p in records if p['revision'] == revision]
                       for revision in ('previous', 'candidate')}
            medians = {revision: statistics.median(values) for revision, values in samples.items()}
            summary[route] = {'samples': samples, 'medians': medians,
                              'candidate_change_percent': (medians['candidate'] / medians['previous'] - 1) * 100}
        manifest.update(complete=True, cells=20, summary=summary)
        save('capture.json', manifest)
        print(json.dumps(summary, sort_keys=True), flush=True)
    except BaseException as error:
        manifest['failure'] = repr(error)
        save('capture.json', manifest)
        raise
