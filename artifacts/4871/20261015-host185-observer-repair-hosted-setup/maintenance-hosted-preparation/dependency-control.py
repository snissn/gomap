from pathlib import Path
import datetime
import hashlib
import json
import os
import signal
import subprocess
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
root = D / 'maintenance-dependency-control'
root.mkdir()
source = root / 'module-input'
source.mkdir()
paths = ['go.mod', 'go.sum', 'third_party/hashicorp_raft_v1.7.3/go.mod', 'third_party/hashicorp_raft_v1.7.3/go.sum']
for rel in paths:
    origin = D / 'coordinator' / rel
    if origin.exists():
        target = source / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(origin.read_bytes())
for name in ['tmp', 'cache', 'modcache']:
    (root / name).mkdir()
def hashes():
    return {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source.rglob('*')) if p.is_file()}
before = hashes()
go = D / 'toolchains/go1.26.3/bin/go'
env = dict(os.environ, GOROOT=str(go.parent.parent), GOTOOLCHAIN='local', GOENV='off', GOWORK='off', GOFLAGS='', GOAMD64='v1', CGO_ENABLED='1', GOMAXPROCS='2', GOGC='100', GODEBUG='', GOMEMLIMIT='8GiB', GOCACHE=str(root/'cache'), GOMODCACHE=str(root/'modcache'), TMPDIR=str(root/'tmp'), GOPROXY='https://proxy.golang.org,direct', GOSUMDB='sum.golang.org')
offline = dict(env, GOPROXY='off', GOSUMDB='off')
records = []
def run(name, args, active_env, expect=0):
    path = root / (name + '.log')
    started = time.monotonic()
    with path.open('wb') as out:
        child = subprocess.Popen([str(go), *args], cwd=source, env=active_env, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=600)
        except BaseException:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            raise
    row = dict(label=name, argv=args, exit=code, expected_exit=expect, elapsed_s=time.monotonic()-started, log_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), source_unchanged=hashes()==before)
    records.append(row)
    (root/'commands.json').write_text(json.dumps(records, indent=2)+'\n')
    print(json.dumps(row), flush=True)
    assert code == expect and row['source_unchanged']
run('default-download', ['mod','download'], env)
run('original-offline-list', ['list','-m','-json','all'], offline, expect=1)
run('prefill-readonly-graph', ['list','-mod=readonly','-m','-json','all'], env)
run('offline-verify', ['mod','verify'], offline)
run('offline-readonly-list', ['list','-mod=readonly','-m','-json','all'], offline)
(root/'result.json').write_text(json.dumps({'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'module_inputs':before, 'after':hashes(), 'source_unchanged':hashes()==before, 'same_module_inventory':(root/'prefill-readonly-graph.log').read_bytes()==(root/'offline-readonly-list.log').read_bytes(), 'scope':'fresh-cache module metadata only; no package build or runtime'},indent=2)+'\n')
