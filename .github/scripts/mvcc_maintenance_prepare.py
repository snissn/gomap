"""Compile frozen existing benchmark inputs; this stage executes no test body."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import signal
import subprocess
import time

D = Path(os.environ['RUNNER_TEMP']) / 'mvcc-maintenance-2588c'
P = D / 'performance-185/maintenance-build-02'
P.mkdir()
(P / 'tmp').mkdir()
GO = D / 'toolchains/go1.26.3/bin/go'
assert hashlib.sha256(GO.read_bytes()).hexdigest() == 'd68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65'
roots = {'baseline': D / 'coordinator', 'candidate': D / 'ordinary-repair'}
heads = {'baseline': '5bca87ffe6e1bfba2d693264d2dc82936adc234f',
         'candidate': '2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49'}
env = dict(os.environ)
env.update(GOROOT=str(GO.parent.parent), GOTOOLCHAIN='local', GOENV='off',
           GOWORK='off', GOFLAGS='', GOAMD64='v1', CGO_ENABLED='1',
           GOMAXPROCS='2', GOGC='100', GODEBUG='', GOMEMLIMIT='8GiB',
           GOPROXY='off', GOSUMDB='off',
           GOCACHE=str(D / 'ordinary-work/cache'),
           GOMODCACHE=str(D / 'ordinary-work/modcache'), TMPDIR=str(P / 'tmp'))

def save(name, value):
    (P / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def sources():
    result = {}
    for label, root in roots.items():
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=root, text=True).strip()
        assert git('rev-parse', 'HEAD') == heads[label]
        assert not git('status', '--porcelain'), label
        files = git('ls-files', '--cached', '--others', '--exclude-standard').splitlines()
        result[label] = {'root': str(root), 'head': heads[label],
                         'tree': git('rev-parse', 'HEAD^{tree}'),
                         'files': {f: digest(root / f) for f in files if (root / f).is_file()}}
    return result

before = sources()
save('sources-before.json', before)
save('environment.json', {k: env[k] for k in (
    'GOROOT', 'GOTOOLCHAIN', 'GOENV', 'GOWORK', 'GOFLAGS', 'GOAMD64', 'CGO_ENABLED',
    'GOMAXPROCS', 'GOGC', 'GODEBUG', 'GOMEMLIMIT', 'GOPROXY', 'GOSUMDB',
    'GOCACHE', 'GOMODCACHE', 'TMPDIR')})
receipts = []
binaries = {}
for label, root in roots.items():
    for package, relative in [('public', './TreeDB'), ('db', './TreeDB/db')]:
        binary = P / (label + '-' + package + '.test')
        argv = [str(GO), 'test', '-p', '1', '-c', '-o', str(binary), relative]
        start = time.monotonic()
        log = P / (label + '-' + package + '.build.log')
        with log.open('wb') as stream:
            process = subprocess.Popen(argv, cwd=root, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            interrupted = None
            try:
                code = process.wait(timeout=600)
            except BaseException as error:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                code = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
                interrupted = repr(error)
        receipts.append({'argv': argv, 'cwd': str(root), 'exit': code,
                         'elapsed_s': time.monotonic() - start, 'log': log.name,
                         'log_sha256': digest(log), 'interrupted': interrupted})
        save('commands.json', receipts)
        assert code == 0, log
        info = subprocess.check_output([str(GO), 'version', '-m', str(binary)], env=env, text=True)
        (P / (binary.name + '.buildinfo.txt')).write_text(info)
        binaries[label + '-' + package] = {'path': str(binary), 'sha256': digest(binary),
                                          'source_head': heads[label], 'package': relative}
        save('binaries.json', binaries)
        print(label, package, 'compiled; no test body executed', flush=True)
after = sources()
save('sources-after.json', after)
assert before == after
save('freeze.json', {'heads': heads, 'source_before_sha256': digest(P / 'sources-before.json'),
    'source_after_sha256': digest(P / 'sources-after.json'), 'binaries': binaries,
    'compiler_sha256': digest(GO), 'builder_sha256': digest(Path(__file__)),
    'source_unchanged': True, 'test_bodies_executed': False,
    'at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()})
