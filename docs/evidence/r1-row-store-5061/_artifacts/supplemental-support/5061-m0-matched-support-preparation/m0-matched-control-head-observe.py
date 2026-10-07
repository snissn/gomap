import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path('/home/mikers/gomap-r1-evidence-20261005/m0-matched-3182')
REPO = Path('/home/mikers/gomap-r1-retained-c-2572')
TOOLCHAIN = Path('/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64')
SOURCES = [('control', 'f2c93cfcdf5f7ef54f6d7bc4ff9e6946fb21a72c'), ('candidate', '3182e15dfe1aa11120db9d309ad0d590283398d6')]

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')

def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()

def host():
    return {'utc': now(), 'hostname': subprocess.check_output(['hostname'], text=True).strip(), 'load': os.getloadavg(), 'cpu_count': os.cpu_count(), 'uname': list(os.uname()), 'processes': subprocess.check_output(['ps', '-eo', 'pid,comm,pcpu,pmem,args', '--sort=-pcpu'], text=True).splitlines()[:16]}

assert not ROOT.exists(), 'new output directory required'
assert git(REPO, 'rev-parse', 'HEAD') == '90fa3dc6819dfcd6823bf68d3287252b87c1b8d0'
assert not git(REPO, 'status', '--porcelain'), 'owned source must be clean'
ROOT.mkdir()
env = dict(os.environ)
env.update({'PATH': str(TOOLCHAIN / 'bin') + ':' + env['PATH'], 'GOROOT': str(TOOLCHAIN), 'GOTOOLCHAIN': 'local', 'GOENV': 'off', 'GOWORK': 'off', 'GOMODCACHE': '/home/mikers/go/pkg/mod', 'GOCACHE': '/home/mikers/.cache/gomap-r1-m0-matched-go1264', 'GOMAXPROCS': '2', 'GOMEMLIMIT': '8GiB', 'GOGC': '100', 'GODEBUG': '', 'GOFLAGS': '', 'CPU_SET': '0,1'})
write(ROOT / 'intent.json', {'utc': now(), 'sources_order': SOURCES, 'scope': 'One unchanged full ten-sample M0 control capture followed by one unchanged candidate capture on same host. Diagnostic only; not replacement of failed GitHub CI. No further automatic repeats.', 'canonical_lock': '/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock', 'env_overrides': {k: env[k] for k in ['PATH','GOROOT','GOTOOLCHAIN','GOENV','GOWORK','GOMODCACHE','GOCACHE','GOMAXPROCS','GOMEMLIMIT','GOGC','GODEBUG','GOFLAGS','CPU_SET']}})
write(ROOT / 'host-before.json', host())
write(ROOT / 'toolchain.json', {'argv': ['go','env','-json'], 'exit': 0, 'environment': json.loads(subprocess.check_output(['go','env','-json'], env=env, text=True)), 'go_sha256': hashlib.sha256((TOOLCHAIN / 'bin/go').read_bytes()).hexdigest()})
results=[]
for label, source in SOURCES:
    worktree = Path('/home/mikers/gomap-r1-m0-matched-' + label + '-3182')
    assert not worktree.exists(), 'new isolated worktree required'
    subprocess.run(['git','-C',str(REPO),'worktree','add','--detach',str(worktree),source], check=True)
    assert git(worktree,'rev-parse','HEAD') == source
    assert not git(worktree,'status','--porcelain')
    assert (worktree / 'scripts/treedb_vacuum_m0_capture.sh').exists()
    out = ROOT / label
    out.mkdir()
    source_env = dict(env, RUN_DIR=str(out / 'capture'))
    argv = ['bash','scripts/treedb_vacuum_m0_capture.sh']
    start={'utc':now(),'argv':argv,'cwd':str(worktree),'source':source,'status_before':git(worktree,'status','--porcelain'),'host':host(),'script_sha256':hashlib.sha256((worktree/'scripts/treedb_vacuum_m0_capture.sh').read_bytes()).hexdigest(),'run_dir':source_env['RUN_DIR']}
    write(out/'start.json',start)
    print(label,source,'started',flush=True)
    tick=time.monotonic()
    with (out/'driver.log').open('wb') as log:
        process=subprocess.run(argv,cwd=worktree,env=source_env,stdout=log,stderr=subprocess.STDOUT)
    result={'utc':now(),'source':source,'exit':process.returncode,'elapsed_seconds':time.monotonic()-tick,'status_after':git(worktree,'status','--porcelain'),'host':host(),'raw_hashes':{str(p.relative_to(out)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size} for p in sorted(out.rglob('*')) if p.is_file()}}
    write(out/'exit.json',result)
    results.append({'label':label,'source':source,'exit':process.returncode,'elapsed_seconds':result['elapsed_seconds']})
    print(label,'complete',json.dumps(results[-1]),flush=True)
write(ROOT/'host-after.json',host())
write(ROOT/'completed.json',{'utc':now(),'results':results,'scope':'diagnostic controls; no GitHub gate waiver or causal infrastructure/performance claim'})
print(json.dumps(results),flush=True)
