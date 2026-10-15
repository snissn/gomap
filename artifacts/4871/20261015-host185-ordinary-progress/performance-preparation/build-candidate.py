import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

root = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
out = root / 'performance-185'
worktree = out / 'candidate'
go_root = root / 'toolchains/go1.26.3'
freeze = json.loads((out / 'candidate-freeze.json').read_text())
env = os.environ.copy()
env.update(GOROOT=str(go_root), GOTOOLCHAIN='local', GOWORK='off', GOPROXY='off',
           GOSUMDB='off', CGO_ENABLED='1', GOMAXPROCS='2', TMPDIR=str(out / 'tmp'),
           GOCACHE=str(root / 'ordinary-work/cache'),
           GOMODCACHE=str(root / 'ordinary-work/modcache'))

def git(*args):
    return subprocess.check_output(['git', *args], cwd=worktree, text=True).strip()

def identity():
    assert not git('status', '--porcelain')
    assert git('rev-parse', 'HEAD') == freeze['source_head']
    return {key: git('rev-parse', 'HEAD' + suffix) for key, suffix in
            [('head', ''), ('tree', '^{tree}'), ('TreeDB', ':TreeDB'),
             ('go_mod_blob', ':go.mod'), ('go_sum_blob', ':go.sum')]}

before = identity()
assert (worktree / 'TreeDB/mvcc/primary_generation_routing_v6_test.go').read_bytes() == (
    root / 'performance-reference/guard/perf/primary_generation_routing_v6_test.go').read_bytes()

def run(label, argv, timeout):
    receipt = {'argv': list(map(str, argv)), 'cwd': str(worktree), 'source': before,
               'env': {key: env[key] for key in ['GOROOT', 'GOTOOLCHAIN', 'GOWORK',
                'GOPROXY', 'GOSUMDB', 'CGO_ENABLED', 'GOMAXPROCS', 'TMPDIR',
                'GOCACHE', 'GOMODCACHE']}, 'timeout_s': timeout}
    start = time.monotonic()
    with (out / (label + '.log')).open('wb') as stream:
        process = subprocess.Popen(receipt['argv'], cwd=worktree, env=env,
                                   stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            result = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            result = 124
    receipt.update(exit=result, elapsed_s=time.monotonic() - start)
    (out / (label + '.json')).write_text(json.dumps(receipt, indent=2))
    print(label, result, flush=True)
    assert result == 0, receipt

for label, package, flags in [
    ('candidate-db', './TreeDB/db', []),
    ('candidate-primary', './TreeDB/mvcc', ['-tags=treedb_test', '-trimpath', '-buildvcs=false'])
]:
    run(label + '-build', [go_root / 'bin/go', 'test', '-c', '-p=1', *flags,
                          '-o', out / (label + '.test'), package], 180)
assert identity() == before
run('candidate-public-primary-lifecycle', ['taskset', '-c', '0',
    out / 'candidate-primary.test',
    '-test.run=^TestCommitAtPrimaryOnlyRetainsActualDataGeneration$',
    '-test.count=1', '-test.timeout=90s', '-test.v'], 120)
assert identity() == before
binaries = {}
for name in ['baseline-db', 'baseline-primary', 'candidate-db', 'candidate-primary']:
    path = out / (name + '.test')
    binaries[name] = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
(out / 'binaries.json').write_text(json.dumps(binaries, indent=2))
(out / 'candidate-source.json').write_text(json.dumps(before, indent=2))
print('Frozen candidate builds and actual public lifecycle PASS; timing not started.', flush=True)
