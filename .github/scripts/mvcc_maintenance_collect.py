"""Fourteen fixed existing-benchmark invocations; no retries or source overlay."""
from pathlib import Path
import datetime
import hashlib
import importlib.util
import json
import math
import os
import re
import signal
import statistics
import subprocess
import sys
import time
from mvcc_maintenance_bootstrap import join_group

D = Path(os.environ['RUNNER_TEMP']) / 'mvcc-maintenance-a17fe'
B = D / 'performance-185/maintenance-build-02'
P = D / 'performance-185/maintenance-bounded-02'
ROOTS = {'baseline': D / 'coordinator', 'candidate': D / 'ordinary-repair'}
CHECKPOINT = 'BenchmarkCOWPublicDirtyCost/command_wal_durable/btree/Pointer4096/N=1024/DirtyCheckpoint'
VACUUM = 'BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn/bytes_64x'
COHORT = 'BenchmarkPrimaryBorrowedValueLogCohort'

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def load_observer(expected_sha256=None):
    # Observation policy belongs to the reviewed diagnostic harness, not either
    # immutable product checkout used to build the benchmark binaries.
    path = Path(__file__).resolve().parents[2] / 'scripts/mvcc_primary_cost_gate.py'
    source = path.read_bytes()
    source_sha256 = hashlib.sha256(source).hexdigest()
    if expected_sha256 is not None and source_sha256 != expected_sha256:
        raise ValueError('maintained observer authorization mismatch')
    spec = importlib.util.spec_from_file_location('maintenance_observer', path)
    module = importlib.util.module_from_spec(spec)
    exec(compile(source, str(path), 'exec'), module.__dict__)
    if module.QUIET_LIMIT_PERCENT != 10.0 or module.QUIET_AGGREGATE_LIMIT_PERCENT != 50.0:
        raise ValueError('maintained observer policy mismatch')
    return module, path, source_sha256

def schedule():
    result = []
    for kind, name, count, package in [('checkpoint', CHECKPOINT, 16, 'public'),
                                       ('vacuum', VACUUM, 1, 'db')]:
        for pair in range(3):
            for revision in (('baseline', 'candidate') if pair % 2 == 0 else ('candidate', 'baseline')):
                result.append(dict(kind=kind, name=name, count=count, package=package,
                                   pair=pair, revision=revision))
    for size in (8, 16):
        result.append(dict(kind='cohort', name=COHORT + '/files=' + str(size), count=128,
                           package='db', pair=None, revision='candidate', size=size))
    return result

def parse(raw, item):
    if 'PASS' not in raw.splitlines() or re.search(r'(?m)^(FAIL|--- FAIL:|panic:|WARNING: DATA RACE)', raw):
        raise ValueError('benchmark did not finish successfully')
    lines = [line for line in raw.splitlines() if line.startswith('Benchmark')]
    if len(lines) != 1:
        raise ValueError('exactly one selected benchmark row required')
    fields = lines[0].split()
    if not re.fullmatch(re.escape(item['name']) + r'-2', fields[0]):
        raise ValueError('wrong benchmark route or concurrency')
    if int(fields[1]) != item['count'] or len(fields[2:]) % 2:
        raise ValueError('wrong iterations or malformed metric pairs')
    metrics = {}
    for raw_value, unit in zip(fields[2::2], fields[3::2]):
        value = float(raw_value)
        if unit in metrics or not math.isfinite(value) or value < 0:
            raise ValueError('duplicate, nonfinite or negative metric')
        metrics[unit] = value
    if any(unit not in metrics for unit in ('ns/op', 'B/op', 'allocs/op')) or metrics['ns/op'] <= 0:
        raise ValueError('missing complete operation metrics')
    if item['kind'] == 'vacuum':
        wanted = {'foreground-exposure-misses/op': 0, 'vacuum-errors/op': 0,
                  'concurrent-aborts/op': 0, 'foreground-overlap-samples/op': 4096,
                  'foreground-points/op': 2048, 'foreground-ranges/op': 2048}
        if any(metrics.get(k) != v for k, v in wanted.items()):
            raise ValueError('vacuum correctness/fixed-work receipt failed')
        if any(metrics.get(k, 0) <= 0 for k in ('preclone-pages/op', 'vacuum-total-ns/op',
                                               'max-writer-pause-ns', 'foreground-p99-ns/op')):
            raise ValueError('vacuum actual work/timing receipt missing')
    elif item['kind'] == 'checkpoint':
        # Fixture totals include reset/setup operations and are not timed-op deltas.
        if any(metrics.get(k, 0) <= 0 for k in ('fixture_checkpoint_runs',
                'fixture_command_wal_sync_count', 'fixture_command_wal_file_sync_calls',
                'fixture_value_log_sync_calls', 'p50-ns', 'p99-ns')):
            raise ValueError('checkpoint/durable pointer fixture activity missing')
    elif item['kind'] == 'cohort':
        if metrics.get('held-cohort-files') != item['size'] or metrics.get('closed-governor-bytes') != 0:
            raise ValueError('cohort geometry or actual final cleanup failed')
        for unit in ('held-cohort-bytes', 'held-old-data-bytes', 'held-old-primary-bytes',
                     'held-governor-bytes', 'scope-after-reader-governor-bytes',
                     'scope-complete-governor-bytes', 'last-reader-cleanup-ns'):
            if unit not in metrics or metrics[unit] < 0:
                raise ValueError('cohort physical/lifetime metric missing')
        if any(metrics[k] <= 0 for k in ('held-cohort-bytes', 'held-old-data-bytes', 'held-old-primary-bytes')):
            raise ValueError('cohort original physical storage missing')
        # This source only creates these map keys while counting surviving paths.
        # Their absence means zero survivors; no other missing metric is defaulted.
        for unit in ('drained-cohort-files', 'drained-cohort-bytes'):
            metrics.setdefault(unit, 0.0)
    return metrics

def selftest():
    item = dict(kind='vacuum', name=VACUUM, count=1)
    metrics = {'ns/op': 100, 'B/op': 20, 'allocs/op': 2,
               'foreground-exposure-misses/op': 0, 'vacuum-errors/op': 0, 'concurrent-aborts/op': 0,
               'foreground-overlap-samples/op': 4096, 'foreground-points/op': 2048,
               'foreground-ranges/op': 2048, 'preclone-pages/op': 345,
               'vacuum-total-ns/op': 90, 'max-writer-pause-ns': 10, 'foreground-p99-ns/op': 5}
    def raw(m):
        return VACUUM + '-2 1 ' + ' '.join(str(v) + ' ' + k for k, v in m.items()) + '\nPASS\n'
    parse(raw(metrics), item)
    rejected = 0
    for changed in (dict(metrics, **{'foreground-exposure-misses/op': 1}),
                    dict(metrics, **{'foreground-overlap-samples/op': 1}),
                    dict(metrics, **{'ns/op': float('nan')})):
        try:
            parse(raw(changed), item)
        except ValueError:
            rejected += 1
    for broken in (raw(metrics).replace('\nPASS', '\nFAIL'), raw(metrics) + raw(metrics),
                   raw(metrics).replace('-2 1 ', '-2 2 ')):
        try:
            parse(broken, item)
        except ValueError:
            rejected += 1
    assert rejected == 6
    print('parser positive and six negative receipt controls passed')

def main():
    auth_path = D / 'authorization.json'
    auth = json.loads(auth_path.read_text())
    assert auth['runner_sha256'] == digest(Path(__file__))
    assert auth['schedule'] == schedule() and auth['runtime_authorized'] is True
    module, observer_path, observer_sha256 = load_observer(auth['observer_sha256'])
    frozen = json.loads((B / 'sources-after.json').read_text())
    binaries = json.loads((B / 'binaries.json').read_text())
    assert json.loads((B / 'freeze.json').read_text())['source_unchanged']
    def verify():
        assert digest(observer_path) == observer_sha256
        for label, root in ROOTS.items():
            assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip() == frozen[label]['head']
            assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()
            assert all(digest(root / f) == h for f, h in frozen[label]['files'].items())
        assert all(digest(Path(x['path'])) == x['sha256'] for x in binaries.values())
    verify()
    P.mkdir()
    (P / 'tmp').mkdir()
    def save(name, value):
        (P / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    env = dict(os.environ)
    env.update(json.loads((B / 'environment.json').read_text()))
    env.update(TMPDIR=str(P / 'tmp'), PYTHONDONTWRITEBYTECODE='1')
    sys.dont_write_bytecode = True
    assert {0, 1}.issubset(os.sched_getaffinity(0))
    save('binding.json', {'authorization': auth, 'authorization_sha256': digest(auth_path),
        'build_freeze_sha256': digest(B / 'freeze.json'), 'sources': {k: {x: v[x] for x in ('head', 'tree')} for k, v in frozen.items()},
        'binaries': binaries, 'runner_sha256': digest(Path(__file__)),
        'observer_sha256': digest(observer_path), 'env': {k: env[k] for k in json.loads((B / 'environment.json').read_text())},
        'affinity': [0, 1], 'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'scope': 'Bounded premerge diagnostics only; not sustained/universal or native qualification'})
    (P / 'cpuinfo.txt').write_text(Path('/proc/cpuinfo').read_text())
    (P / 'host.txt').write_text(subprocess.check_output(['uname', '-a'], text=True) +
        subprocess.check_output(['df', '-PT', str(P)], text=True))
    observation = module.ProcessObservation(P / 'preflight-processes.jsonl')
    for _ in range(40):
        time.sleep(0.25)
        observation.observe()
    preflight = observation.finish()
    save('preflight.json', preflight)
    if preflight['busy']:
        save('result.json', {'status': 'REFUSED_HOST_NOT_QUIET', 'executed': 0, 'preflight': preflight})
        return 2
    rows = []
    observation = module.ProcessObservation(P / 'measurement-processes.jsonl')
    failure = None
    try:
        for i, item in enumerate(schedule()):
            binary = binaries[item['revision'] + '-' + item['package']]['path']
            pattern = '/'.join('^' + re.escape(part) + '$' for part in item['name'].split('/'))
            argv = ['taskset', '-c', '0,1', binary, '-test.run=^$', '-test.bench=' + pattern,
                    '-test.benchtime=' + str(item['count']) + 'x', '-test.count=1',
                    '-test.benchmem', '-test.timeout=120s']
            record = dict(item, argv=argv, cwd=str(ROOTS[item['revision']]), raw=f'{i:02d}-{item["kind"]}-{item["revision"]}.txt')
            raw_path = P / record['raw']
            save('pending-command.json', record)
            start = time.monotonic()
            interrupted = None
            with raw_path.open('wb') as stream:
                process = subprocess.Popen(argv, cwd=record['cwd'], env=env,
                    stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    while process.poll() is None:
                        observation.observe()
                        if time.monotonic() - start > 150:
                            raise TimeoutError('fixed child exceeded outer cap')
                        time.sleep(0.25)
                except BaseException as error:
                    group_join = join_group(process, interrupted=True)
                    interrupted = repr(error)
                else:
                    group_join = join_group(process)
                    if group_join['unexpected_survivors'] or not group_join['process_group_empty']:
                        interrupted = 'process group did not complete cleanly'
            record.update(exit=process.returncode, elapsed_s=time.monotonic() - start,
                          raw_sha256=digest(raw_path), interrupted=interrupted, group_join=group_join,
                          outer_timeout_s=150, inner_timeout_s=120)
            rows.append(record)
            save('commands.json', rows)
            if interrupted:
                raise RuntimeError(interrupted)
            if process.returncode:
                raise ValueError('benchmark child failed: ' + record['raw'])
            record['metrics'] = parse(raw_path.read_text(), item)
            save('commands.json', rows)
            print(i, item['kind'], item['revision'], 'receipt accepted', flush=True)
    except BaseException as error:
        failure = repr(error)
    quiet = observation.finish()
    save('measurement-quiet.json', quiet)
    unchanged = True
    try:
        verify()
    except BaseException as error:
        unchanged = False
        failure = (failure or '') + '; final source/binary verification: ' + repr(error)
    result = {'status': 'FAILED' if failure else ('CONTAMINATED' if quiet['busy'] else 'COMPLETE_BOUNDED_DIAGNOSTIC'),
              'failure': failure, 'executed': len(rows), 'planned': len(schedule()),
              'all_sources_and_binaries_unchanged': unchanged, 'quiet': not quiet['busy'],
              'timings_qualify': False, 'final_cost_acceptance': 'root disposition pending',
              'limits': ['Three matched pairs per maintenance operation; one capture sample per cohort size',
                         '250ms external-process sampling may miss shorter-lived processes',
                         'Cohort cleanup is one off-timer event, not a latency distribution',
                         'Vacuum cost fixture does not emit full digest, physical bytes or shrink ratio']}
    if not failure and len(rows) == len(schedule()):
        result['maintenance'] = {}
        for kind in ('checkpoint', 'vacuum'):
            arms = {rev: [x['metrics'] for x in rows if x['kind'] == kind and x['revision'] == rev]
                    for rev in ('baseline', 'candidate')}
            units = sorted(set(arms['baseline'][0]) & set(arms['candidate'][0]))
            result['maintenance'][kind] = {
                'medians': {rev: {u: statistics.median(x[u] for x in values) for u in units} for rev, values in arms.items()},
                'paired_ns_percent': [(c['ns/op'] / b['ns/op'] - 1) * 100 for b, c in zip(arms['baseline'], arms['candidate'])]}
        result['cohort'] = {str(x['size']): x['metrics'] for x in rows if x['kind'] == 'cohort'}
    save('result.json', result)
    return 0 if result['status'] == 'COMPLETE_BOUNDED_DIAGNOSTIC' else 1

if __name__ == '__main__':
    if sys.argv[1:] == ['--selftest']:
        selftest()
    else:
        assert not sys.argv[1:]
        raise SystemExit(main())
