import ctypes
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tarfile
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
W, A, G = D / 'ordinary-repair', D / 'ordinary-diagnostic-packing', D / 'toolchains/go1.26.3'
assert ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0
active = None

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

def sources():
    names = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=W).split(b'\0')
    return {os.fsdecode(n): digest(W / os.fsdecode(n)) for n in names if n and (W / os.fsdecode(n)).is_file()}

def reap():
    joined = []
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return joined
        if not pid:
            return joined
        joined.append({'pid': pid, 'wait_status': status})

def group_present(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False

def interrupted(signum, frame):
    if active is not None and group_present(active.pid):
        os.killpg(active.pid, signal.SIGKILL)
    raise RuntimeError('validation interrupted by signal ' + str(signum))

signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
selected = {'GOROOT': str(G), 'GOTOOLCHAIN': 'local', 'GOWORK': 'off', 'GOENV': 'off',
    'GOFLAGS': '', 'GOAMD64': 'v1', 'CGO_ENABLED': '1', 'GOMAXPROCS': '2', 'GOGC': '100',
    'GOMEMLIMIT': '8GiB', 'GODEBUG': '', 'GOPROXY': 'off', 'GOSUMDB': 'off',
    'GOCACHE': str(D / 'ordinary-work/cache'), 'GOMODCACHE': str(D / 'ordinary-work/modcache'),
    'TMPDIR': str(A / 'tmp')}
env = dict(os.environ, **selected)
before = sources()
save(A / 'source-before.json', before)
(A / 'source.patch').write_bytes(subprocess.check_output(['git', 'diff', '--binary'], cwd=W))
with tarfile.open(A / 'source-before.tar.gz', 'w:gz') as archive:
    for name in sorted(before):
        archive.add(W / name, arcname=name, recursive=False)
with tarfile.open(A / 'source-before.tar.gz', 'r:gz') as archive:
    actual = {item.name: hashlib.sha256(archive.extractfile(item).read()).hexdigest() for item in archive if item.isfile()}
assert actual == before
save(A / 'source-archive.json', {'sha256': digest(A / 'source-before.tar.gz'), 'files': len(actual), 'all_bytes_match': True})
save(A / 'environment.json', selected)
save(A / 'toolchain.json', {'root': str(G), 'files': {p: digest(G / p) for p in ['bin/go', 'pkg/tool/linux_amd64/compile', 'pkg/tool/linux_amd64/link']}})
tests = ['TestGenericResourceDiagnosticsOwnAllNamespaceText', 'TestGenericResourceDiagnosticsSurviveConcurrentPhysicalRelease',
    'TestGenericAbsentDiagnosticBackingDoesNotAllocate', 'TestGenericEmptyCloneUsesAbsentClosure',
    'TestRawLogicalUnionRefusesSelectedAliasesBeforeEscape', 'TestStableResourcePublicationDebtCoverage',
    'TestStableResourceSetConcurrentReadsAndRelease']
regex = '^(' + '|'.join(tests) + ')$'
normal, race = A / 'normal.test', A / 'race.test'
calls = [
    ('build-normal', [str(G / 'bin/go'), 'test', '-p=1', '-c', '-gcflags=github.com/snissn/gomap/TreeDB/internal/rootpublication=-m=2', '-o', str(normal), './TreeDB/internal/rootpublication'], 240),
    ('normal', [str(normal), '-test.run=' + regex, '-test.count=1', '-test.timeout=120s', '-test.v'], 150),
    ('build-race', [str(G / 'bin/go'), 'test', '-p=1', '-race', '-c', '-o', str(race), './TreeDB/internal/rootpublication'], 240),
    ('race', [str(race), '-test.run=' + regex, '-test.count=1', '-test.timeout=120s', '-test.v'], 150),
    ('normal-buildinfo', [str(G / 'bin/go'), 'version', '-m', str(normal)], 30),
    ('race-buildinfo', [str(G / 'bin/go'), 'version', '-m', str(race)], 30),
]
results = []
for name, argv, limit in calls:
    path = A / name
    path.mkdir()
    assert sources() == before
    command = {'argv': argv, 'cwd': str(W), 'env': selected, 'outer_timeout_seconds': limit,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'executable_sha256': digest(Path(argv[0]))}
    save(path / 'command.json', command)
    start = time.monotonic()
    failure, expired, joined = None, False, []
    with (path / 'stdout.txt').open('wb') as out, (path / 'stderr.txt').open('wb') as err:
        p = subprocess.Popen(argv, cwd=W, env=env, stdout=out, stderr=err, start_new_session=True)
        active = p
        pgid = os.getpgid(p.pid)
        save(path / 'process.json', {'pid': p.pid, 'pgid': pgid, 'sid': os.getsid(p.pid), 'subreaper': True})
        try:
            p.wait(timeout=limit)
        except BaseException as exc:
            failure, expired = repr(exc), isinstance(exc, subprocess.TimeoutExpired)
        finally:
            if group_present(pgid):
                os.killpg(pgid, signal.SIGKILL)
            p.wait(timeout=10)
            for _ in range(200):
                joined.extend(reap())
                if not group_present(pgid):
                    break
                time.sleep(0.01)
            active = None
    output = (path / 'stdout.txt').read_text(errors='replace')
    passing = re.findall(r'^--- PASS: (\S+)', output, re.MULTILINE)
    result = {'name': name, 'exit': p.returncode, 'elapsed_seconds': time.monotonic() - start,
        'failure': failure, 'timeout': expired, 'source_equal': sources() == before,
        'leader_joined': p.poll() is not None, 'process_group_absent': not group_present(pgid),
        'adopted_children_reaped': joined, 'top_level_passing_tests': passing,
        'stdout_sha256': digest(path / 'stdout.txt'), 'stderr_sha256': digest(path / 'stderr.txt')}
    if name in ('normal', 'race'):
        result['expected_tests_match'] = sorted(passing) == sorted(tests)
        result['no_race_report'] = 'WARNING: DATA RACE' not in output + (path / 'stderr.txt').read_text(errors='replace')
    save(path / 'result.json', result)
    results.append(result)
    save(A / 'validation.json', {'complete': False, 'checks': results})
    print(json.dumps(result), flush=True)
    assert not failure and p.returncode == 0 and result['source_equal'] and result['process_group_absent']
    assert result.get('expected_tests_match', True) and result.get('no_race_report', True)
save(A / 'source-after.json', sources())
save(A / 'validation.json', {'complete': True, 'all_pass': True, 'all_source_equal': sources() == before,
    'source_files': len(before), 'source_patch_sha256': digest(A / 'source.patch'), 'checks': results,
    'binaries': {p.name: {'sha256': digest(p), 'bytes': p.stat().st_size} for p in [normal, race]}})
