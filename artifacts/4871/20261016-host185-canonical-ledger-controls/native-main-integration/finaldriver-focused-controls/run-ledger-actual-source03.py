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
N = D / 'native-integrated'
B = D / 'native-main-integration/native-ledger-source03'
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
assert grant['scope'] == 'actual-canonical-ledger-source03' and grant['actual_native_driver_authorized']
assert grant['runner_sha256'] == sha(Path(__file__))
assert grant['proposal_sha256'] == sha(B / 'proposed-commands.json')
assert grant['manifest_sha256'] == sha(B / 'source-frozen.json')
assert grant['archive_sha256'] == sha(B / 'source-frozen.tar.gz')
assert grant['independent_review_sha256'] == sha(Path(grant['independent_review']))
assert read(Path(grant['source_verify']))['live_source_and_modes_match']
expected = read(B / 'source-frozen.json')
modes = read(B / 'source-modes.json')
proposal = read(B / 'proposed-commands.json')
commands = [c for c in proposal['commands'] if c['name'] in grant['commands']]
assert [c['name'] for c in commands] == grant['commands']
stage = grant['stage']
assert stage == 'controls'
assert sha(Path(grant['source_verify'])) == grant['source_verify_sha256']
assert sha(B / 'controls/summary.json') == grant['focused_summary_sha256']
assert read(B / 'controls/summary.json')['all_authorized_pass']
assert stage in ('controls', 'build', 'samples')
names = {'controls': ['normal-all-shared-arms', 'race-all-shared-arms'],
         'build': ['build-firstack-elf'],
         'samples': ['firstack-sample01', 'firstack-sample02', 'firstack-sample03']}
assert grant['commands'] == names[stage]
O = B / 'runtime-stages' / 'actual-controls'
assert not O.exists()
compiler = D / 'toolchains/go1.26.3/bin/go'
elf = B / 'build/native-firstack-cost.test'
assert sha(compiler) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
assert sha(compiler.parent.parent / 'pkg/tool/linux_amd64/compile') == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
if stage != 'controls':
    assert read(B / 'runtime-stages/controls/summary.json')['all_pass']
if stage == 'samples':
    assert read(B / 'runtime-stages/build/summary.json')['all_pass']
    assert sha(elf) == grant['elf_sha256']
for c in commands:
    assert c['cwd'] == str(N) and c['executed'] is False
    assert Path(c['env']['TMPDIR']).is_relative_to(B)
    assert c['env']['GOMAXPROCS'] == '2' and c['env']['GOTOOLCHAIN'] == 'local'
    assert c['env']['GOROOT'] == str(compiler.parent.parent)
    assert c['env']['GOPROXY'] == 'off' and c['env']['GOFLAGS'] == ''
    assert c['argv'][0] == str(elf if stage == 'samples' else compiler)
    assert c['selection_class'] == 'actual_route_separate_grant_required'
    assert c['argv'] == [str(compiler), 'test', '-p=1', '-parallel=1', '-count=1', '-v', '-timeout=3m'] + (['-race'] if c['name'].startswith('race-') else []) + ['-run', '^TestNativeSelectedOriginalSplitDiagnosticV6$', './TreeDB/mvcc']
    assert c['outer_timeout_seconds'] == 600
    assert c['required_top_level_tests'] == ['TestNativeSelectedOriginalSplitDiagnosticV6']
    assert c['required_child_tests'] == {'TestNativeSelectedOriginalSplitDiagnosticV6': ['ready-cancel', 'stale-before-fence', 'frozen-limits', 'once-ack', 'paused-finish-close', 'pruner-enabled-refusal']}
    assert c['expected_top_level_count'] == 1 and c['expected_child_count'] == 6

def identity():
    def git(args):
        return subprocess.check_output(['git'] + args, cwd=N)
    return {'HEAD': git(['rev-parse', 'HEAD']).decode().strip(),
            'index_sha256': hashlib.sha256(git(['ls-files', '--stage', '-z'])).hexdigest()}

def source():
    files = {}
    for base, ds, fs in os.walk(N):
        ds[:] = [d for d in ds if d != '.git']
        for name in fs:
            path = Path(base) / name
            rel = path.relative_to(N).as_posix()
            if rel != '.git' and path.is_file():
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

assert identity() == grant['git_identity'] and source() == expected
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
subreaper = ctypes.c_int()
assert libc.prctl(37, ctypes.byref(subreaper), 0, 0, 0) == 0 and subreaper.value == 1
O.mkdir(parents=True)
dump(O / 'identity.json', {'grant_sha256': sha(a.selection), 'runner_sha256': sha(Path(__file__)),
    'proposal_sha256': sha(B / 'proposed-commands.json'), 'git_identity': identity(),
    'manifest_sha256': sha(B / 'source-frozen.json'), 'source_files': len(expected),
    'go_binary_sha256': sha(compiler), 'compile_binary_sha256': sha(compiler.parent.parent / 'pkg/tool/linux_amd64/compile'),
    'go_version_file': (compiler.parent.parent / 'VERSION').read_text(),
    'affinity': sorted(os.sched_getaffinity(0)), 'cpu_count': os.cpu_count(), 'runner_pid': os.getpid(),
    'runner_subreaper': True, 'elf_sha256': sha(elf) if stage == 'samples' else None})
results = []
for c in commands:
    out = Path(c['env']['TMPDIR']).parent
    assert not out.exists()
    out.mkdir(parents=True)
    tmp = Path(c['env']['TMPDIR'])
    tmp.mkdir()
    probe = tmp / 'root-write-probe'
    probe.write_bytes(b'firstack-cost01')
    assert probe.read_bytes() == b'firstack-cost01'
    probe.unlink()
    before = source()
    dump(out / 'source-before.json', before)
    assert before == expected and identity() == grant['git_identity']
    if stage == 'samples':
        assert sha(elf) == grant['elf_sha256']
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
            result = {'name': c['name'], 'status': 'STOP', 'executed': False, 'reasons': ['host preflight busy; no sample executed']}
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
    if before != expected or after != expected or identity() != grant['git_identity']:
        reasons.append('source/index drift')
    if any(v in raw for v in ('WARNING: DATA RACE', 'panic:', 'goleak:', '--- FAIL:', '--- SKIP:')):
        reasons.append('test/race/panic/skip failure')
    runs = re.findall(r'^=== RUN\s+(\S+)', raw, re.M)
    passes = re.findall(r'^\s*--- PASS: (\S+)', raw, re.M)
    if stage == 'controls':
        want = c['required_top_level_tests'] + [parent + '/' + child for parent, children in c['required_child_tests'].items() for child in children]
        if sorted(runs) != sorted(want) or sorted(passes) != sorted(want):
            reasons.append('RUN/PASS inventory mismatch')
        if raw.count('native-split prebirth G=') != 6 or raw.count('native-split genuine paused Finish position=') != 2:
            reasons.append('missing real diagnostic evidence')
    elif stage == 'build':
        if runs or not elf.is_file() or elf.read_bytes()[:4] != b'\x7fELF':
            reasons.append('invalid build or executed test body')
    else:
        lines = re.findall(r'^BenchmarkNativeSelectedOriginalFirstACKCostV6-\d+\s+1\s+(.+)$', raw, re.M)
        if runs or len(lines) != 1 or raw.count('first-ACK wall-only totals ') != 1 or 'exactACKReturns=1' not in raw or not re.search(r'^PASS$', raw, re.M):
            reasons.append('benchmark/oracle inventory mismatch')
        if sha(elf) != grant['elf_sha256']:
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
    result = {'name': c['name'], 'status': 'PASS' if not reasons else 'STOP', 'executed': True,
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
summary = {'stage': stage, 'results': results, 'all_pass': len(results) == len(commands) and all(r['status'] == 'PASS' for r in results),
           'unexecuted_commands': [c['name'] for c in commands[len(results):]], 'no_product_acceptance': True, 'utc': utc()}
dump(O / 'summary.json', summary)
print(json.dumps({'stage': stage, 'all_pass': summary['all_pass'], 'summary': str(O / 'summary.json')}), flush=True)
