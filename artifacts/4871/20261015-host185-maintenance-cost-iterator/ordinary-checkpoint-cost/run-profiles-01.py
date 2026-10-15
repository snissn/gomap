from pathlib import Path
import datetime
import hashlib
import json
import os
import signal
import shutil
import subprocess
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
BASE = D / 'ordinary-checkpoint-cost'
OUT = BASE / 'profiles-01'
PROPOSAL = BASE / 'proposal.json'
GO = D / 'toolchains/go1.26.3/bin/go'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


assert not OUT.exists()
assert os.access(D, os.W_OK) and os.path.ismount('/mnt/fast4tb')
assert sha(PROPOSAL) == 'f7c7a70991260e81edcf7f9e0217c40de304a804c31b5bfb785a01c6b91a3ee3'
assert sha(GO) == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
proposal = json.loads(PROPOSAL.read_text())
assert proposal['proposed_first_stage']['calls'] == 2
assert {0, 1} <= os.sched_getaffinity(0)
OUT.mkdir()
(OUT / 'tmp').mkdir()
save('authorization.json', {
    'coordinator': '/root', 'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'proposal_sha256': sha(PROPOSAL), 'runner_sha256': sha(Path(__file__)),
    'maximum_calls': 2, 'fixed_order': ['baseline-public', 'candidate-public'],
    'maximum_inner_seconds': 120, 'maximum_outer_seconds': 150,
    'retries': 0, 'source_mutation': False, 'rebuild': False,
    'purpose': 'Whole-process allocation-stack attribution only. Instrumented timing and B/op are not acceptance results. Includes setup, reset/seed, ALL checkpoints and teardown.',
    'adaptations': 'Task output paths; executable byte-identical copies; CPU0,1; full original runtime environment; no workload or timer changes.'
})
env = dict(os.environ, GOROOT=str(GO.parent.parent), GOTOOLCHAIN='local', GOWORK='off',
           GOENV='off', GOFLAGS='', GOAMD64='v1', CGO_ENABLED='1', GOMAXPROCS='2',
           GOGC='100', GODEBUG='', GOMEMLIMIT='8GiB', GOPROXY='off', GOSUMDB='off',
           TMPDIR=str(OUT / 'tmp'))
save('environment.json', {k: env[k] for k in ('GOROOT', 'GOTOOLCHAIN', 'GOWORK', 'GOENV', 'GOFLAGS', 'GOAMD64', 'CGO_ENABLED', 'GOMAXPROCS', 'GOGC', 'GODEBUG', 'GOMEMLIMIT', 'GOPROXY', 'GOSUMDB', 'TMPDIR')})
(OUT / 'host.txt').write_text(subprocess.check_output(['uname', '-a'], text=True) + subprocess.check_output(['df', '-PT', str(OUT)], text=True))
(OUT / 'cpuinfo.txt').write_text(Path('/proc/cpuinfo').read_text())
receipts = []
try:
    for item in proposal['proposed_first_stage']['exact_calls']:
        original = Path(item['binary'])
        assert sha(original) == item['binary_sha256']
        arm = OUT / item['arm']
        arm.mkdir()
        binary = arm / 'checkpoint.test'
        shutil.copy2(original, binary)
        binary.chmod(0o755)
        assert sha(binary) == item['binary_sha256']
        argv = ['taskset', '-c', '0,1', str(binary)] + [
            '-test.memprofile=' + str(arm / 'alloc.pprof') if x.startswith('-test.memprofile=') else x
            for x in item['argv'][1:]
        ]
        record = dict(arm=item['arm'], source_head=item['source_head'], binary_sha256=item['binary_sha256'],
                      argv=argv, cwd=item['cwd'], started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        save('pending-command.json', record)
        started = time.monotonic()
        failure = None
        with (arm / 'stdout.txt').open('wb') as stdout, (arm / 'stderr.txt').open('wb') as stderr:
            child = subprocess.Popen(argv, cwd=item['cwd'], env=env, stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                child.wait(timeout=150)
            except BaseException as error:
                failure = repr(error)
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
        record.update(exit=child.returncode, elapsed_s=time.monotonic() - started, interrupted=failure,
                      stdout_sha256=sha(arm / 'stdout.txt'), stderr_sha256=sha(arm / 'stderr.txt'))
        if (arm / 'alloc.pprof').exists():
            record['profile_sha256'] = sha(arm / 'alloc.pprof')
        receipts.append(record)
        save('commands.json', receipts)
        assert failure is None and child.returncode == 0, record
        assert 'PASS' in (arm / 'stdout.txt').read_text().splitlines()
        assert (arm / 'alloc.pprof').stat().st_size > 0
        assert sha(binary) == sha(original) == item['binary_sha256']
        print(item['arm'], 'profile captured', record['elapsed_s'], flush=True)
    save('result.json', {'status': 'TWO_PROFILES_CAPTURED', 'calls': len(receipts), 'timing_qualifies': False, 'source_changed': False})
except BaseException as error:
    save('result.json', {'status': 'FAILED', 'calls': len(receipts), 'failure': repr(error), 'timing_qualifies': False})
    raise
