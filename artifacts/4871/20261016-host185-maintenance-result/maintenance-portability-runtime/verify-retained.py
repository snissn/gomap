import datetime
import hashlib
import json
import pathlib
import re
import tarfile

R = pathlib.Path(__file__).resolve().parent
P = R / 'packet/mvcc-maintenance-a17fe'
B = P / 'performance-185/maintenance-build-02'
M = P / 'performance-185/maintenance-bounded-02'

def read(p):
    return json.loads(p.read_text())

def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def archive(p, expected):
    found = {}
    with tarfile.open(p, 'r|gz') as t:
        for m in t:
            name = m.name.removeprefix('./')
            assert not name.startswith('/') and '..' not in pathlib.PurePosixPath(name).parts, name
            if m.isdir():
                continue
            assert m.isfile() and name not in found, (name, m.type)
            h = hashlib.sha256()
            with t.extractfile(m) as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b''):
                    h.update(chunk)
            found[name] = h.hexdigest()
    assert found == expected, (str(p), len(found), len(expected), [k for k in set(found) | set(expected) if found.get(k) != expected.get(k)][:10])
    return {'sha256': digest(p), 'files': len(found)}

def joined(c):
    assert c['exit'] == 0 and c.get('interrupted') is None, c
    j = c['group_join']
    assert j['leader_joined'] and j['process_group_empty'] and not j['unexpected_survivors'] and not j['kill_requested'], j
    assert j['cleanup_limit_s'] == 2 and j['cleanup_elapsed_s'] <= 2, j

grant = read(R / 'grant.json')
hashes = read(P / 'artifact-hashes.json')
assert len(hashes) == 105
assert {str(p.relative_to(P)) for p in P.rglob('*') if p.is_file()} == set(hashes) | {'artifact-hashes.json'}
for n, sha in hashes.items():
    assert digest(P / n) == sha, n

manifests = {}
archives = {}
for side in ('baseline', 'candidate', 'harness'):
    manifests[side] = read(R / (side + '-source-manifest.json'))
    assert digest(R / (side + '-source-manifest.json')) == grant['sources'][side]['manifest_sha256']
    assert len(manifests[side]) == grant['sources'][side]['files']
    archives[side] = archive(P / 'archives' / (side + '.tar.gz'), manifests[side])
assert read(P / 'harness-source-manifest.json') == manifests['harness']
for n in ('sources-before-dependencies.json', 'sources-after-dependencies.json', 'sources-final-available.json'):
    d = read(P / n)
    for side in ('baseline', 'candidate'):
        assert d[side]['files'] == manifests[side]
        assert d[side]['head'] == grant[side] and d[side]['tree'] == grant['sources'][side]['tree']
        assert d[side]['status'] == ''
for n in ('sources-before.json', 'sources-after.json'):
    d = read(B / n)
    for side in ('baseline', 'candidate'):
        assert d[side]['files'] == manifests[side]
        assert d[side]['head'] == grant[side] and d[side]['tree'] == grant['sources'][side]['tree']

sdk = read(P / 'sdk-manifest.json')
archives['sdk'] = archive(P / 'go1.26.3-linux-amd64.tar.gz', sdk)
compiler = read(P / 'compiler.json')
assert compiler['go_sha256'] == sdk['bin/go'] == grant['environment']['go_sha256']
for n, sha in compiler['retained'].items():
    assert digest(P / 'compiler' / n) == sha
    assert sdk['bin/go' if n == 'go' else 'pkg/tool/linux_amd64/' + n] == sha

binding = read(M / 'binding.json')
auth = read(P / 'authorization.json')
hb = read(P / 'harness-binding.json')
freeze = read(B / 'freeze.json')
bins = read(B / 'binaries.json')
assert hb['head'] == auth['harness_head'] == grant['harness_head']
assert str(auth['run_id']) == '38833428170' and str(auth['run_attempt']) == '1'
assert auth['maximum_benchmark_calls'] == 14
assert binding['authorization'] == auth and binding['authorization_sha256'] == digest(P / 'authorization.json')
assert binding['build_freeze_sha256'] == digest(B / 'freeze.json')
assert binding['binaries'] == freeze['binaries'] == bins
assert freeze['source_unchanged'] and not freeze['test_bodies_executed']
assert freeze['source_before_sha256'] == digest(B / 'sources-before.json')
assert freeze['source_after_sha256'] == digest(B / 'sources-after.json')
assert freeze['compiler_sha256'] == compiler['go_sha256']
for n, sha in hb['files'].items():
    assert manifests['harness'][n] == sha == digest(P / 'harness' / n)
assert freeze['builder_sha256'] == hb['files']['.github/scripts/mvcc_maintenance_prepare.py']
assert auth['runner_sha256'] == binding['runner_sha256'] == hb['files']['.github/scripts/mvcc_maintenance_collect.py']
assert binding['observer_sha256'] == manifests['candidate']['scripts/mvcc_primary_cost_gate.py']
for k, v in {'CGO_ENABLED':'1','GOAMD64':'v1','GODEBUG':'','GOENV':'off','GOFLAGS':'','GOGC':'100','GOMAXPROCS':'2','GOMEMLIMIT':'8GiB','GOPROXY':'off','GOSUMDB':'off','GOTOOLCHAIN':'local','GOWORK':'off'}.items():
    assert read(B / 'environment.json')[k] == binding['env'][k] == v, k
assert binding['affinity'] == [0, 1]
assert set(bins) == {'baseline-db','baseline-public','candidate-db','candidate-public'}
for name, entry in bins.items():
    p = B / (name + '.test')
    assert digest(p) == entry['sha256']
    with p.open('rb') as f:
        assert f.read(4) == b'\x7fELF'
    assert entry['source_head'] == grant[name.split('-')[0]]
    info = (B / (name + '.test.buildinfo.txt')).read_text()
    assert ': go1.26.3\n' in info and '\tbuild\tCGO_ENABLED=1\n' in info and '\tbuild\tGOAMD64=v1\n' in info

bootstrap = read(P / 'bootstrap-commands.json')
assert len(bootstrap) == 34
for c in bootstrap:
    joined(c)
    assert digest(P / c['log']) == c['log_sha256']
    assert c['elapsed_s'] <= c['timeout_s']
build = read(B / 'commands.json')
assert len(build) == 4
for c in build:
    joined(c)
    assert digest(B / c['log']) == c['log_sha256']
    assert c['timeout_s'] == 600 and c['elapsed_s'] <= 600
    assert c['argv'][1:6] == ['test','-p','1','-c','-o']
    assert c['argv'][0].endswith('/toolchains/go1.26.3/bin/go')
for c in read(P / 'driver-commands.json'):
    assert c['exit'] == 0 and digest(P / c['log']) == c['log_sha256']
    assert c['sha256'] == hb['files']['.github/scripts/mvcc_maintenance_' + c['phase'] + '.py']

calls = read(M / 'commands.json')
assert len(calls) == len(auth['schedule']) == 14
for i, (c, scheduled) in enumerate(zip(calls, auth['schedule'])):
    joined(c)
    for key, val in scheduled.items():
        assert c[key] == val, (i, key)
    assert digest(M / c['raw']) == c['raw_sha256']
    assert c['outer_timeout_s'] == 150 and c['inner_timeout_s'] == 120 and c['elapsed_s'] <= 150
    binary = bins[c['revision'] + '-' + c['package']]['path']
    pattern = '/'.join('^' + re.escape(part) + '$' for part in c['name'].split('/'))
    expected = ['taskset','-c','0,1',binary,'-test.run=^$','-test.bench=' + pattern,'-test.benchtime=' + str(c['count']) + 'x','-test.count=1','-test.benchmem','-test.timeout=120s']
    assert c['argv'] == expected, (i, c['argv'], expected)

result = {'verified_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'verdict': 'PASS retained byte/source/SDK/ELF/command binding only', 'run': 38833428170, 'attempt': 1, 'grant_sha256': digest(R / 'grant.json'), 'zip_sha256': digest(R / 'attempt.zip'), 'artifact_files': len(hashes), 'archives': archives, 'binary_hashes': {k:v['sha256'] for k,v in bins.items()}, 'bootstrap_commands': len(bootstrap), 'build_commands': len(build), 'benchmark_calls': len(calls), 'process_groups_joined': len(bootstrap)+len(build)+len(calls), 'qualification': 'No binaries executed during this verification. Independent raw/quiet review and root cost disposition remain separate; no original M0/native/graph acceptance.'}
(R / 'retained-verification.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
print(json.dumps(result, indent=2, sort_keys=True))
