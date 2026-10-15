from pathlib import Path
import datetime
import hashlib
import json
import math
import re
import statistics
import subprocess

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
O = D / 'performance-185/maintenance-hosted-02'
P = O / 'packet/bounded-maintenance-38808351333-1/mvcc-maintenance-72e01'
B = P / 'performance-185/maintenance-build-02'
M = P / 'performance-185/maintenance-bounded-02'
REPO = D / 'ordinary-repair'


def read(p):
    return json.loads(p.read_text())


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO)


def check(condition, label):
    if not condition:
        raise AssertionError(label)


heads = {'baseline': '5bca87ffe6e1bfba2d693264d2dc82936adc234f',
         'candidate': '72e01efd767ef5d766f070e531fb53ea3f22674d'}
frozen = read(B / 'sources-after.json')
check(frozen == read(B / 'sources-before.json'), 'build source stability')
for phase in ('sources-before-dependencies.json', 'sources-after-dependencies.json', 'sources-final-available.json'):
    source = read(P / phase)
    for arm in heads:
        check(all(source[arm][key] == frozen[arm][key] for key in ('files', 'head', 'tree')), phase + arm)
        check(not source[arm]['status'], phase + ' clean')

blob_hashes = {}
cat = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=REPO, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
try:
    def blob_sha(oid):
        if oid not in blob_hashes:
            cat.stdin.write(oid.encode() + b'\n')
            cat.stdin.flush()
            header = cat.stdout.readline().split()
            check(header[1] == b'blob', 'git object kind')
            data = cat.stdout.read(int(header[2]))
            check(cat.stdout.read(1) == b'\n', 'git framing')
            blob_hashes[oid] = hashlib.sha256(data).hexdigest()
        return blob_hashes[oid]

    for arm, head in heads.items():
        entries = {}
        for entry in git('ls-tree', '-r', '-z', head).split(b'\0'):
            if not entry:
                continue
            metadata, name = entry.split(b'\t', 1)
            mode, kind, oid = metadata.decode().split()
            check(kind == 'blob' and mode != '120000', 'unexpected source type')
            entries[name.decode()] = oid
        check(frozen[arm]['head'] == head, 'exact head')
        check(frozen[arm]['tree'] == git('rev-parse', head + '^{tree}').decode().strip(), 'exact tree')
        check(entries.keys() == frozen[arm]['files'].keys(), 'complete source manifest')
        for name, oid in entries.items():
            check(blob_sha(oid) == frozen[arm]['files'][name], 'source blob ' + name)
    harness = read(P / 'harness-binding.json')
    check(harness['head'] == '28f32623a78ea11eca78d0ae7c0b8fa1106b121a', 'harness head')
    for name, digest in harness['files'].items():
        check(sha(P / 'harness' / name) == digest, 'retained harness ' + name)
        check(hashlib.sha256(git('show', harness['head'] + ':' + name)).hexdigest() == digest, 'committed harness ' + name)
finally:
    cat.stdin.close()
    check(cat.wait() == 0, 'git batch exit')

freeze = read(B / 'freeze.json')
check(freeze['source_unchanged'] and not freeze['test_bodies_executed'], 'build disposition')
for side in ('before', 'after'):
    check(sha(B / ('sources-' + side + '.json')) == freeze['source_' + side + '_sha256'], 'freeze source digest')
check(sha(P / 'compiler/go') == freeze['compiler_sha256'] == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65', 'pinned compiler')
for name, digest in read(P / 'compiler.json')['retained'].items():
    check(sha(P / 'compiler' / name) == digest, 'compiler ' + name)
check(sha(P / 'go1.26.3-linux-amd64.tar.gz') == read(P / 'toolchain-archive.json')['sha256'], 'full SDK')
binaries = read(B / 'binaries.json')
check(binaries == freeze['binaries'] and len(binaries) == 4, 'four binaries')
for key, info in binaries.items():
    check(sha(B / Path(info['path']).name) == info['sha256'], 'binary ' + key)
    check(info['source_head'] == heads[key.split('-')[0]], 'binary source')
for command in read(B / 'commands.json'):
    check(command['exit'] == 0 and command['interrupted'] is None, 'build exit')
    check(sha(B / command['log']) == command['log_sha256'], 'build log')
    check(command['elapsed_s'] < 600, 'build cap')

auth = read(P / 'authorization.json')
binding = read(M / 'binding.json')
check(auth['runtime_authorized'] and auth['maximum_benchmark_calls'] == 14, 'grant')
check(binding['authorization'] == auth and binding['authorization_sha256'] == sha(P / 'authorization.json'), 'binding authorization')
check(binding['binaries'] == binaries and binding['build_freeze_sha256'] == sha(B / 'freeze.json'), 'binding build')
check(binding['runner_sha256'] == auth['runner_sha256'] == harness['files']['.github/scripts/mvcc_maintenance_collect.py'], 'collector binding')
check(binding['observer_sha256'] == frozen['candidate']['files']['scripts/mvcc_primary_cost_gate.py'], 'quiet observer')
check(binding['affinity'] == [0, 1] and binding['env']['GOMAXPROCS'] == '2', 'execution geometry')

rows = read(M / 'commands.json')
check(len(rows) == len(auth['schedule']) == 14, 'complete fixed schedule')
raw_manifest = {}
for row, scheduled in zip(rows, auth['schedule']):
    check(all(row[k] == v for k, v in scheduled.items()), 'schedule ordering')
    check(row['exit'] == 0 and row['interrupted'] is None and row['elapsed_s'] < 150, 'child completion')
    raw = M / row['raw']
    check(sha(raw) == row['raw_sha256'], 'raw binding')
    raw_manifest[row['raw']] = sha(raw)
    text = raw.read_text()
    check('PASS' in text.splitlines() and not re.search(r'(?m)^(FAIL|--- FAIL:|panic:|WARNING: DATA RACE)', text), 'raw success')
    lines = [x for x in text.splitlines() if x.startswith('Benchmark')]
    check(len(lines) == 1, 'one leaf')
    fields = lines[0].split()
    check(fields[:2] == [row['name'] + '-2', str(row['count'])], 'route/iterations')
    check(len(fields[2:]) % 2 == 0, 'metric shape')
    metrics = {}
    for value, unit in zip(fields[2::2], fields[3::2]):
        check(unit not in metrics, 'duplicate metric')
        metrics[unit] = float(value)
        check(math.isfinite(metrics[unit]) and metrics[unit] >= 0, 'metric value')
    check(all(k in metrics for k in ('ns/op', 'B/op', 'allocs/op')) and metrics['ns/op'] > 0, 'operation metrics')
    if row['kind'] == 'checkpoint':
        for unit in ('fixture_checkpoint_runs', 'fixture_command_wal_sync_count', 'fixture_command_wal_file_sync_calls', 'fixture_value_log_sync_calls', 'p50-ns', 'p99-ns'):
            check(metrics[unit] > 0, 'checkpoint receipt')
    elif row['kind'] == 'vacuum':
        for key, value in {'foreground-exposure-misses/op': 0, 'vacuum-errors/op': 0, 'concurrent-aborts/op': 0, 'foreground-overlap-samples/op': 4096, 'foreground-points/op': 2048, 'foreground-ranges/op': 2048}.items():
            check(metrics[key] == value, 'vacuum receipt ' + key)
        for key in ('preclone-pages/op', 'vacuum-total-ns/op', 'max-writer-pause-ns', 'foreground-p99-ns/op'):
            check(metrics[key] > 0, 'vacuum work')
    else:
        check(metrics['held-cohort-files'] == row['size'] and metrics['closed-governor-bytes'] == 0, 'cohort cleanup')
        for key in ('held-cohort-bytes', 'held-old-data-bytes', 'held-old-primary-bytes'):
            check(metrics[key] > 0, 'cohort physical')
        for key in ('held-governor-bytes', 'scope-after-reader-governor-bytes', 'scope-complete-governor-bytes', 'last-reader-cleanup-ns'):
            check(key in metrics, 'cohort lifetime')
        for key in ('drained-cohort-files', 'drained-cohort-bytes'):
            metrics.setdefault(key, 0)
    check(metrics == row['metrics'], 'independent raw parse')

result = read(M / 'result.json')
check(result['status'] == 'COMPLETE_BOUNDED_DIAGNOSTIC' and result['executed'] == 14 and result['failure'] is None, 'result disposition')
summary = {}
for kind in ('checkpoint', 'vacuum'):
    arms = {arm: [x['metrics'] for x in rows if x['kind'] == kind and x['revision'] == arm] for arm in heads}
    units = set(arms['baseline'][0]) & set(arms['candidate'][0])
    medians = {arm: {unit: statistics.median(x[unit] for x in values) for unit in units} for arm, values in arms.items()}
    pairs = [(c['ns/op'] / b['ns/op'] - 1) * 100 for b, c in zip(arms['baseline'], arms['candidate'])]
    check(result['maintenance'][kind] == {'medians': medians, 'paired_ns_percent': pairs}, 'summary recomputation')
    summary[kind] = {'median_paired_ns_percent': statistics.median(pairs), 'all_paired_ns_percent': pairs,
                     'base': {k: medians['baseline'][k] for k in ('ns/op', 'B/op', 'allocs/op')},
                     'candidate': {k: medians['candidate'][k] for k in ('ns/op', 'B/op', 'allocs/op')}}
    summary[kind]['delta'] = {k: medians['candidate'][k] - medians['baseline'][k] for k in ('B/op', 'allocs/op')}
    summary[kind]['ops_per_second'] = {arm: 1e9 / medians[arm]['ns/op'] for arm in heads}
cohort = {str(x['size']): x['metrics'] for x in rows if x['kind'] == 'cohort'}
check(result['cohort'] == cohort, 'cohort recomputation')

pid_rows = [line.split() for line in (P / 'setup-processes.log').read_text().splitlines() if 'python3 -B .github/scripts/mvcc_maintenance_bootstrap.py' in line]
check(len(pid_rows) == 1, 'unique collector process')
collector_pid = int(pid_rows[0][0])
quiet = {}
for raw_name, summary_name in [('preflight-processes.jsonl', 'preflight.json'), ('measurement-processes.jsonl', 'measurement-quiet.json')]:
    previous, totals = {}, {}
    first = True
    samples = [json.loads(line) for line in (M / raw_name).read_text().splitlines()]
    for sample in samples:
        snap = {int(k): v for k, v in sample['processes'].items()}
        allowed, parent = {collector_pid}, collector_pid
        while parent in snap and snap[parent]['parent'] > 0:
            parent = snap[parent]['parent']
            allowed.add(parent)
        descendants = {collector_pid}
        while True:
            added = {pid for pid, r in snap.items() if r['parent'] in descendants}
            if added <= descendants:
                break
            descendants |= added
        allowed |= descendants
        for pid, row in snap.items():
            key = (pid, row['start'])
            old = previous.get(key, row['ticks'] if first else 0)
            if pid not in allowed:
                total = totals.setdefault(key, dict(pid=pid, start=row['start'], command=row['command'], ticks=0))
                total['ticks'] += max(0, row['ticks'] - old)
            previous[key] = row['ticks']
        first = False
    reported = read(M / summary_name)
    # Linux hosted USER_HZ=100; equality against every retained CPU value verifies this conversion.
    actual = [dict(r, cpu_percent=r['ticks'] / reported['elapsed_s']) for r in totals.values()]
    expected = reported['external_processes']
    check(len(actual) == len(expected), 'quiet process count')
    for a, e in zip(actual, expected):
        check(all(a[k] == e[k] for k in ('pid', 'start', 'command', 'ticks')), 'quiet process identity/ticks')
        check(math.isclose(a['cpu_percent'], e['cpu_percent'], rel_tol=1e-14, abs_tol=1e-14), 'quiet CPU recompute')
    check(reported['cpu_threshold_percent'] == 2 and not reported['busy'], 'unchanged quiet gate')
    check(all(r['cpu_percent'] < 2 for r in actual), 'quiet raw gate')
    quiet[summary_name] = dict(samples=len(samples), elapsed_s=reported['elapsed_s'], max_external_cpu_percent=max(r['cpu_percent'] for r in actual), collector_pid=collector_pid)

receipt = dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), status='VERIFIED_ALL_14_BOUNDED_SAMPLES',
               workflow_run=38808351333, harness_head=harness['head'], source_heads=heads,
               source_files={arm: len(v['files']) for arm, v in frozen.items()}, binaries=binaries,
               raw_sha256=raw_manifest, maintenance=summary, cohort=cohort, quiet=quiet,
               accepted_cost=False, pending='Checkpoint +40990 median B/op and +32 allocations need causal disposition; cohort retained storage tradeoff needs explicit acceptance.',
               limitations=result['limits'], verifier_sha256=sha(Path(__file__)))
(O / 'root-verification.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
print(json.dumps({k: receipt[k] for k in ('status', 'source_files', 'maintenance', 'quiet', 'accepted_cost', 'pending')}, indent=2))
