from pathlib import Path
import hashlib
import json
import os
import signal
import subprocess
import time

D = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
root = D / 'maintenance-package-dependency-control'
root.mkdir()
for name in ['tmp', 'cache', 'modcache']:
    (root / name).mkdir()
go = D / 'toolchains/go1.26.3/bin/go'
env = dict(os.environ, GOROOT=str(go.parent.parent), GOTOOLCHAIN='local', GOENV='off', GOWORK='off', GOFLAGS='', GOAMD64='v1', CGO_ENABLED='1', GOMAXPROCS='2', GOGC='100', GODEBUG='', GOMEMLIMIT='8GiB', GOCACHE=str(root/'cache'), GOMODCACHE=str(root/'modcache'), TMPDIR=str(root/'tmp'), GOPROXY='https://proxy.golang.org,direct', GOSUMDB='sum.golang.org')
offline = dict(env, GOPROXY='off', GOSUMDB='off')
records = []
def inventory(source):
    files = subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=source,text=True).splitlines()
    return {p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in files if (source/p).is_file()}
def run(name, args, source, active_env, expect=0):
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
    row = dict(label=name, argv=args, cwd=str(source), exit=code, expected_exit=expect, elapsed_s=time.monotonic()-started, log_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    records.append(row)
    (root/'commands.json').write_text(json.dumps(records, indent=2)+'\n')
    print(json.dumps(row), flush=True)
    assert code == expect
before = {}
after = {}
for label, directory in [('baseline','coordinator'),('candidate-packages','maintenance-hosted-runner')]:
    source = D/directory
    before[label] = inventory(source)
    (root/'sources-before.json').write_text(json.dumps(before,indent=2)+'\n')
    run(label+'-download', ['mod','download'], source, env)
    if label == 'baseline':
        run('original-offline-list', ['list','-m','-json','all'], source, offline, expect=1)
    args=['list','-mod=readonly','-deps','-test','-json','./TreeDB','./TreeDB/db']
    run(label+'-packages-prefill', args, source, env)
    run(label+'-verify', ['mod','verify'], source, offline)
    run(label+'-packages-offline', args, source, offline)
    after[label]=inventory(source)
    (root/'sources-after.json').write_text(json.dumps(after,indent=2)+'\n')
    assert before[label] == after[label]
(root/'result.json').write_text(json.dumps({'source_unchanged':before==after, 'scope':'actual-package dependency metadata only; no build or test/benchmark body', 'candidate_binding':'harness worktree package files inherited exactly from72e01; source differences only diagnostic workflow/scripts'},indent=2)+'\n')
