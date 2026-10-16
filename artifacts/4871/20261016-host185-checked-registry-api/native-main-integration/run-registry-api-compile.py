import argparse
import ctypes
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W = D / 'native-integrated'
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
assert grant['scope'] == 'registry-api-compile-only'
assert grant['all_source_writers_released'] and grant['test_bodies_authorized'] is False
assert grant['runner_sha256'] == sha(Path(__file__))
B = Path(grant['packet']).resolve()
assert B.is_relative_to(D / 'native-main-integration')
assert sha(B / 'source-frozen.json') == grant['manifest_sha256']
assert sha(B / 'source-modes.json') == grant['modes_sha256']
expected = read(B / 'source-frozen.json')
modes = read(B / 'source-modes.json')
O = B / 'compile-only'
assert not O.exists()
go = D / 'toolchains/go1.26.3/bin/go'
assert sha(go) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
compiler = go.parent.parent / 'pkg/tool/linux_amd64/compile'
assert sha(compiler) == '3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1'
# Pinned Go source test.go: -c chooses build/install action, never runTestActor;
# -o /dev/null permits multiple packages with equal test binary basenames.
argv = [str(go), 'test', '-p=1', '-c', '-o', '/dev/null', './...']
assert grant['argv'] == argv
assert grant['outer_timeout_seconds'] == 900
env = {k: os.environ[k] for k in ('PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'TZ') if k in os.environ}
env.update({'CGO_ENABLED': '1', 'GOCACHE': str(D / 'native-build/cache'),
    'GOMODCACHE': str(D / 'ordinary-work/modcache'), 'GOROOT': str(go.parent.parent),
    'GOTOOLCHAIN': 'local', 'GOPROXY': 'off', 'GOSUMDB': 'off', 'GOWORK': 'off',
    'GOENV': 'off', 'GOFLAGS': '', 'GODEBUG': '', 'GOGC': '100',
    'GOMAXPROCS': '2', 'GOMEMLIMIT': '8GiB', 'TMPDIR': str(O / 'tmp')})

def identity():
    def git(args):
        return subprocess.check_output(['git'] + args, cwd=W)
    return {'HEAD': git(['rev-parse', 'HEAD']).decode().strip(),
        'index_sha256': hashlib.sha256(git(['ls-files', '--stage', '-z'])).hexdigest()}

def source():
    actual = {}
    for base, ds, fs in os.walk(W):
        ds[:] = [d for d in ds if d != '.git']
        for name in fs:
            path = Path(base) / name
            rel = path.relative_to(W).as_posix()
            if rel != '.git' and path.is_file():
                actual[rel] = sha(path)
                assert oct(path.stat().st_mode & 0o7777) == modes[rel], rel
    return actual

def processes():
    rows = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            s = (path / 'stat').read_text()
            right = s.rfind(')')
            f = s[right + 2:].split()
            rows[int(path.name)] = {'pid': int(path.name), 'comm': s[s.find('(') + 1:right],
                'state': f[0], 'ppid': int(f[1]), 'pgid': int(f[2])}
        except (OSError, ValueError, IndexError):
            pass
    return rows

def reap():
    rows = []
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            break
        if not pid:
            break
        rows.append({'pid': pid, 'status': status})
    return rows

assert source() == expected and identity() == grant['git_identity']
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
subreaper = ctypes.c_int()
assert libc.prctl(37, ctypes.byref(subreaper), 0, 0, 0) == 0 and subreaper.value == 1
O.mkdir()
(O / 'tmp').mkdir()
probe = O / 'tmp/write-probe'
probe.write_bytes(b'checked-registry-api-compile-only')
assert probe.read_bytes() == b'checked-registry-api-compile-only'
probe.unlink()
dump(O / 'command.json', {'argv': argv, 'cwd': str(W), 'env': env, 'selection_sha256': sha(a.selection),
    'runner_sha256': sha(Path(__file__)), 'manifest_sha256': grant['manifest_sha256'],
    'go_sha256': sha(go), 'compiler_sha256': sha(compiler), 'git_identity': identity(),
    'test_bodies_authorized': False, 'outer_timeout_seconds': 900})
before = source()
dump(O / 'source-before.json', before)
started = utc()
start = time.monotonic()
observed = {}
signals = []
with (O / 'run.log').open('wb') as log:
    proc = subprocess.Popen(argv, cwd=W, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    pgid = os.getpgid(proc.pid)
    dump(O / 'process.json', {'pid': proc.pid, 'pgid': pgid, 'runner_pid': os.getpid(), 'subreaper': True, 'utc': started})
    while proc.poll() is None:
        observed.update({pid: row for pid, row in processes().items() if row['pgid'] == pgid})
        if time.monotonic() - start > 900:
            try:
                os.killpg(pgid, signal.SIGKILL)
                signals.append('SIGKILL outer timeout')
            except ProcessLookupError:
                pass
            break
        time.sleep(.2)
    rc = proc.wait()
reaped = []
deadline = time.monotonic() + 5
while True:
    reaped += reap()
    residual = [r for r in processes().values() if r['pgid'] == pgid or r['ppid'] == os.getpid()]
    if not residual or time.monotonic() >= deadline:
        break
    time.sleep(.1)
initial_residual = residual[:]
if residual:
    for group in {r['pgid'] for r in residual}:
        assert group != os.getpgrp()
        try:
            os.killpg(group, signal.SIGKILL)
            signals.append('SIGKILL residual group ' + str(group))
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 3
    while residual and time.monotonic() < deadline:
        time.sleep(.1)
        reaped += reap()
        residual = [r for r in processes().values() if r['pgid'] == pgid or r['ppid'] == os.getpid()]
after = source()
dump(O / 'source-after.json', after)
raw = (O / 'run.log').read_text(errors='replace')
reasons = []
if rc:
    reasons.append('compile command failed')
if signals or initial_residual or residual:
    reasons.append('process intervention or leak')
if before != after or after != expected or identity() != grant['git_identity']:
    reasons.append('source or Git identity drift')
if any(token in raw for token in ('=== RUN', '--- PASS:', '--- FAIL:', 'WARNING: DATA RACE', 'panic:')):
    reasons.append('unexpected runtime output')
result = {'status': 'PASS' if not reasons else 'STOP', 'reasons': reasons, 'exit_code': rc,
    'started_utc': started, 'ended_utc': utc(), 'elapsed_seconds': time.monotonic() - start,
    'source_before_after_frozen': before == after == expected, 'git_identity_unchanged': identity() == grant['git_identity'],
    'leader_joined': True, 'groups_and_adopted_children_empty': not residual,
    'initial_residual': initial_residual, 'remaining_group_or_children': residual,
    'observed_group': list(observed.values()), 'reaped': reaped, 'signals': signals,
    'raw_log_sha256': sha(O / 'run.log'), 'test_bodies_executed': False,
    'scope': 'root-module compilation and default vet only; nested modules and runtime not qualified'}
dump(O / 'result.json', result)
print(json.dumps({k: result[k] for k in ('status', 'reasons', 'exit_code', 'elapsed_seconds', 'source_before_after_frozen', 'groups_and_adopted_children_empty')}), flush=True)
