import argparse
import ctypes
import datetime
import hashlib
import json
import os
import re
import signal
import subprocess
import time
from pathlib import Path

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
p = argparse.ArgumentParser()
p.add_argument('--selection', type=Path, required=True)
p.add_argument('--selection-sha256', required=True)
a = p.parse_args()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text())

def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

assert a.selection.resolve().is_relative_to(D / 'native-decision')
assert sha(a.selection) == a.selection_sha256
grant = read(a.selection)
assert grant['scope'] == 'canonical-ledger-firstack-comparison01'
assert grant['actual_native_driver_authorized'] is True
assert grant['runner_sha256'] == sha(Path(__file__))
proposal_path = Path(grant['proposal'])
assert sha(proposal_path) == grant['proposal_sha256']
proposal = read(proposal_path)
commands = proposal['commands']
assert len(commands) == 6 and [c['side'] for c in commands] == list('ABBAAB')
assert [c['name'] for c in commands] == grant['commands']
assert len(set(grant['commands'])) == 6
assert all(c['executed'] is False for c in commands)
assert proposal['retries'] == 0 and proposal['stop_first_failure'] is True
sides = grant['sides']
assert set(sides) == {'A', 'B'}
stage = 'samples'
O = Path(grant['output']).resolve()
assert O.is_relative_to(D / 'native-main-integration') and not O.exists()
compiler = D / 'toolchains/go1.26.3/bin/go'
assert sha(compiler) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert sha(compiler.parent.parent / 'pkg/tool/linux_amd64/compile') == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
required_env = {
    'CGO_ENABLED': '1', 'GOCACHE': str(D / 'native-build/cache'),
    'GOMODCACHE': str(D / 'ordinary-work/modcache'),
    'GOROOT': str(compiler.parent.parent), 'GOTOOLCHAIN': 'local',
    'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOWORK': 'off', 'GOENV': 'off',
    'GOFLAGS': '', 'GODEBUG': '', 'GOGC': '100', 'GOMAXPROCS': '2',
    'GOMEMLIMIT': '8GiB',
}
benchmark = 'BenchmarkNativeSelectedOriginalFirstACKCostV6'
for label, side in sides.items():
    packet = Path(side['packet']).resolve()
    source_root = Path(side['source_root']).resolve()
    assert packet.is_relative_to(D / 'native-main-integration')
    assert source_root.is_relative_to(D)
    assert sha(packet / 'source-frozen.json') == side['manifest_sha256']
    assert sha(packet / 'source-frozen.tar.gz') == side['archive_sha256']
    assert sha(packet / 'source-modes.json') == side['modes_sha256']
    assert sha(Path(side['elf'])) == side['elf_sha256']
    for name in ('independent_review', 'source_verify', 'controls_receipt', 'build_receipt'):
        assert sha(Path(side[name])) == side[name + '_sha256']
    assert side['review_accepted'] and side['controls_accepted'] and side['build_accepted']
    assert side['benchmark_source_sha256'] == read(packet / 'source-frozen.json')['TreeDB/mvcc/native_firstack_cost_v6_test.go']
# The wall meter itself is identical. Any changed shared helper/pricing inputs
# must have an explicit source comparison in the selected campaign.
assert sides['A']['benchmark_source_sha256'] == sides['B']['benchmark_source_sha256']
assert sha(Path(grant['source_comparison'])) == grant['source_comparison_sha256']
for c in commands:
    side = sides[c['side']]
    assert re.fullmatch(r'firstack-' + c['side'] + '-0[1-6]', c['name'])
    assert c['cwd'] == side['source_root']
    assert c['argv'] == [side['elf'], '-test.run=^$', '-test.bench=^' + benchmark + '$',
        '-test.benchtime=1x', '-test.count=1', '-test.parallel=1', '-test.timeout=3m', '-test.v']
    assert c['outer_timeout_seconds'] == 240
    assert c['env'] == dict(required_env, TMPDIR=str(O / c['name'] / 'tmp'))
assert grant['quietness_policy'] == {
    'preflight_windows': 2, 'preflight_window_seconds': 1,
    'preflight_max_aggregate_busy_percent': 5,
    'max_other_process_one_core_cpu_percent': 15,
    'minimum_available_memory_kib': 8 * 1024 * 1024,
    'sample_scope': 'whole process; observed surviving unrelated processes',
    'unobserved_exited_process_limitation': True,
}

def choose(label):
    global side, N, B, expected, modes, elf
    side = sides[label]
    N, B = Path(side['source_root']), Path(side['packet'])
    expected = read(B / 'source-frozen.json')
    modes = read(B / 'source-modes.json')
    elf = Path(side['elf'])

def identity():
    result = {'source_root': str(N), 'manifest_sha256': sha(B / 'source-frozen.json')}
    if (N / '.git').exists():
        result['HEAD'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=N, text=True).strip()
        result['index_sha256'] = hashlib.sha256(subprocess.check_output(['git', 'ls-files', '--stage', '-z'], cwd=N)).hexdigest()
    return result

def source():
    files = {}
    for base, ds, fs in os.walk(N):
        ds[:] = [d for d in ds if d != '.git']
        for name in fs:
            path = Path(base) / name
            rel = path.relative_to(N).as_posix()
            if rel != '.git' and path.is_file():
                assert not path.is_symlink(), rel
                files[rel] = sha(path)
                assert oct(path.stat().st_mode & 0o7777) == modes[rel], rel
    return files

def snapshot():
    rows = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            s = (path / 'stat').read_text()
            right = s.rfind(')')
            f = s[right + 2:].split()
            rows[int(path.name)] = {'pid': int(path.name), 'comm': s[s.find('(') + 1:right],
                'state': f[0], 'ppid': int(f[1]), 'pgid': int(f[2]),
                'ticks': int(f[11]) + int(f[12]), 'start_ticks': int(f[19])}
        except (OSError, ValueError, IndexError):
            pass
    cpu = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
    return {'monotonic': time.monotonic(), 'utc': utc(), 'cpu': cpu, 'processes': rows,
            'mem_available_kib': mem['MemAvailable']}

def activity(before, after, excluded_pgid=None):
    seconds = after['monotonic'] - before['monotonic']
    total = sum(after['cpu']) - sum(before['cpu'])
    idle = sum(after['cpu'][3:5]) - sum(before['cpu'][3:5])
    active = []
    for pid, end in after['processes'].items():
        start = before['processes'].get(pid)
        if not start or start['start_ticks'] != end['start_ticks'] or pid == os.getpid() or end['pgid'] == excluded_pgid:
            continue
        cpu_pct = 100 * (end['ticks'] - start['ticks']) / os.sysconf('SC_CLK_TCK') / seconds
        if cpu_pct > 0:
            active.append({'pid': pid, 'comm': end['comm'], 'one_core_cpu_percent': cpu_pct})
    return {'seconds': seconds, 'aggregate_busy_percent': 100 * (total - idle) / total if total else 0,
            'active_other_processes': sorted(active, key=lambda r: -r['one_core_cpu_percent']),
            'mem_available_kib': after['mem_available_kib']}

def reap():
    rows = []
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if pid == 0:
            break
        rows.append({'pid': pid, 'status': status})
    return rows

for label in ('A', 'B'):
    choose(label)
    assert identity() == side['source_identity'] and source() == expected
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
subreaper = ctypes.c_int()
assert libc.prctl(37, ctypes.byref(subreaper), 0, 0, 0) == 0 and subreaper.value == 1
O.mkdir(parents=True)
dump(O / 'identity.json', {'grant_sha256': sha(a.selection), 'runner_sha256': sha(Path(__file__)),
    'proposal_sha256': sha(proposal_path), 'sides': sides,
    'go_binary_sha256': sha(compiler), 'compile_binary_sha256': sha(compiler.parent.parent / 'pkg/tool/linux_amd64/compile'),
    'go_version_file': (compiler.parent.parent / 'VERSION').read_text(),
    'affinity': sorted(os.sched_getaffinity(0)), 'cpu_count': os.cpu_count(), 'runner_pid': os.getpid(),
    'runner_subreaper': True, 'quietness_policy': grant['quietness_policy']})
results = []
for c in commands:
    choose(c['side'])
    out = Path(c['env']['TMPDIR']).parent
    assert not out.exists()
    out.mkdir(parents=True)
    tmp = Path(c['env']['TMPDIR'])
    tmp.mkdir()
    probe = tmp / 'root-write-probe'
    probe.write_bytes(b'ledger-firstack-comparison01')
    assert probe.read_bytes() == b'ledger-firstack-comparison01'
    probe.unlink()
    before = source()
    dump(out / 'source-before.json', before)
    assert before == expected and identity() == side['source_identity']
    if stage == 'samples':
        assert sha(elf) == side['elf_sha256']
        quiet = []
        start = snapshot()
        for _ in range(2):
            time.sleep(1)
            end = snapshot()
            quiet.append(activity(start, end))
            start = end
        dump(out / 'quiet-preflight.json', quiet)
        acceptable = all(r['aggregate_busy_percent'] <= 5 and r['mem_available_kib'] >= 8 * 1024 * 1024 and all(p['one_core_cpu_percent'] <= 15 for p in r['active_other_processes']) for r in quiet)
        if not acceptable:
            result = {'name': c['name'], 'side': c['side'], 'status': 'STOP', 'executed': False, 'reasons': ['host preflight busy; no sample executed']}
            dump(out / 'result.json', result)
            results.append(result)
            print(json.dumps(result), flush=True)
            break
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'TZ') if k in os.environ}
    env.update(c['env'])
    dump(out / 'command.json', dict(c, executed=True, grant_sha256=sha(a.selection)))
    dump(out / 'environment.json', env)
    start = time.monotonic()
    host_before = snapshot()
    interventions = []
    observed = {}
    with (out / 'run.log').open('wb') as log:
        proc = subprocess.Popen(c['argv'], cwd=c['cwd'], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        pgid = os.getpgid(proc.pid)
        dump(out / 'process.json', {'pid': proc.pid, 'pgid': pgid, 'utc': utc(), 'subreaper': True})
        while proc.poll() is None:
            snap = snapshot()
            observed.update({pid: r for pid, r in snap['processes'].items() if r['pgid'] == pgid})
            if time.monotonic() - start > c['outer_timeout_seconds']:
                os.killpg(pgid, signal.SIGKILL)
                interventions.append('SIGKILL outer timeout')
                break
            time.sleep(.1)
        rc = proc.wait()
    host_after = snapshot()
    reaped = reap()
    rows = snapshot()['processes']
    residual = [r for r in rows.values() if r['pgid'] == pgid or r['ppid'] == os.getpid()]
    if residual:
        os.killpg(pgid, signal.SIGKILL)
        interventions.append('SIGKILL residual process group')
        time.sleep(.1)
        reaped += reap()
    after = source()
    dump(out / 'source-after.json', after)
    raw = (out / 'run.log').read_text(errors='replace')
    reasons = []
    if rc or interventions:
        reasons.append('nonzero exit or process intervention')
    if before != expected or after != expected or identity() != side['source_identity']:
        reasons.append('frozen source identity drift')
    if any(v in raw for v in ('WARNING: DATA RACE', 'panic:', 'goleak:', '--- FAIL:', '--- SKIP:')):
        reasons.append('test/race/panic/skip failure')
    runs = re.findall(r'^=== RUN\s+(\S+)', raw, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', raw, re.M)
    lines = re.findall(r'^BenchmarkNativeSelectedOriginalFirstACKCostV6-\d+\s+1\s+(.+)$', raw, re.M)
    if runs or len(lines) != 1 or raw.count('first-ACK wall-only totals ') != 1 or 'exactACKReturns=1' not in raw or not re.search(r'^PASS$', raw, re.M):
        reasons.append('benchmark/oracle inventory mismatch')
    if sha(elf) != side['elf_sha256']:
        reasons.append('ELF drift')
    host_activity = activity(host_before, host_after, pgid)
    dump(out / 'host-window.json', {'before': host_before, 'after': host_after, 'activity': host_activity,
        'scope': 'whole process; not individual public windows; exited unrelated processes may be absent'})
    if stage == 'samples' and any(p['one_core_cpu_percent'] > 15 for p in host_activity['active_other_processes']):
        reasons.append('competing CPU during sample; retain contaminated sample')
    rows = snapshot()['processes']
    remaining = [r for r in rows.values() if r['pgid'] == pgid or r['ppid'] == os.getpid()]
    if remaining or residual:
        reasons.append('process leak')
    result = {'name': c['name'], 'side': c['side'], 'status': 'PASS' if not reasons else 'STOP', 'executed': True,
        'reasons': reasons, 'exit_code': rc, 'elapsed_seconds': time.monotonic() - start,
        'pid': proc.pid, 'pgid': pgid, 'leader_joined': True, 'subreaper': True,
        'observed_group': list(observed.values()), 'reaped': reaped, 'interventions': interventions,
        'remaining_group_or_adopted_children': remaining, 'source_before_after_frozen': before == after == expected,
        'RUN': runs, 'PASS': passes, 'raw_sha256': sha(out / 'run.log'),
        'elf_sha256': sha(elf) if stage in ('build', 'samples') and elf.exists() else None}
    dump(out / 'result.json', result)
    results.append(result)
    print(json.dumps({k: result[k] for k in ('name', 'status', 'reasons', 'exit_code', 'elapsed_seconds')}), flush=True)
    if reasons:
        break
final_source_verification = {}
for label in ('A', 'B'):
    choose(label)
    final_source_verification[label] = source() == expected and identity() == side['source_identity'] and sha(elf) == side['elf_sha256']
summary = {'stage': stage, 'final_source_verification': final_source_verification, 'results': results, 'all_pass': all(final_source_verification.values()) and len(results) == len(commands) and all(r['status'] == 'PASS' for r in results),
           'unexecuted_commands': [c['name'] for c in commands if not any(r['name'] == c['name'] and r.get('executed') for r in results)], 'no_product_acceptance': True, 'utc': utc()}
dump(O / 'summary.json', summary)
print(json.dumps({'stage': stage, 'all_pass': summary['all_pass'], 'summary': str(O / 'summary.json')}), flush=True)
