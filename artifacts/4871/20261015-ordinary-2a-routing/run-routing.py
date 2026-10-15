#!/usr/bin/env python3
"""One-shot ordinary 2a generation-origin routing comparison; no source mutation."""
from pathlib import Path
import argparse, hashlib, json, os, re, statistics, subprocess, time
E=Path(__file__).resolve().parent
P=json.loads((E/'source-preflight.json').read_text())
H=Path(P['worktree'])
G=Path(P['toolchain'])
A=argparse.ArgumentParser()
a=A.add_mutually_exclusive_group()
a.add_argument('--focused',action='store_true')
a.add_argument('--build',action='store_true')
a.add_argument('--execute',action='store_true')
args=A.parse_args()
env=os.environ.copy()
env.update(GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1',GOPROXY='off',GOSUMDB='off')
assert not {k:v for k,v in env.items() if k.startswith(('TREEDB','GOMAP','GOGC','GOMEMLIMIT','GODEBUG'))},'unexpected engine/runtime override'
keys=['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS','GOPROXY','GOSUMDB']
def sha(data): return hashlib.sha256(data).hexdigest()
def dump(name,v):
 p=E/name
 assert not p.exists(),'refuse overwrite: '+str(p)
 p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def git(*args): return subprocess.check_output(['git',*args],cwd=H)
def census():
 assert git('rev-parse','HEAD').decode().strip()==P['head']
 assert git('status','--porcelain=v1','--untracked-files=all','--ignored').decode()==P['status']
 st=H.stat()
 assert dict(dev=st.st_dev,inode=st.st_ino)==P['root_identity']
 entries=[]
 for p in sorted(set(git('ls-files','--cached','--others','--exclude-standard','-z').split(b'\0'))):
  if not p: continue
  rel=p.decode()
  if rel.endswith('.go') or Path(rel).name in ['go.mod','go.sum','go.work','go.work.sum']:
   d=(H/rel).read_bytes();entries.append(dict(path=rel,bytes=len(d),sha256=sha(d)))
 assert sha(json.dumps(entries,sort_keys=True).encode())==P['source_input_census_sha256'],'source drift'
 for r in P['production_files']:
  assert sha((E/'preimages'/r).read_bytes())==P['production_preimages'][r]['sha256']
  assert sha((H/r).read_bytes())==P['production_postimages'][r]['sha256']
 assert sha((E/'baseline-overlay.json').read_bytes())==P['overlay_sha256']
 return entries
def host():
 return dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),loadavg=Path('/proc/loadavg').read_text(),proc_stat=Path('/proc/stat').read_text(),processes=subprocess.check_output(['ps','-eo','pid,ppid,stat,pcpu,etimes,args'],text=True),free_bytes=os.statvfs(H).f_bavail*os.statvfs(H).f_frsize)
def run(name,argv,deadline):
 timeout=min(120,max(0,deadline-time.monotonic()))
 assert timeout>0,'aggregate deadline exhausted'
 start=time.monotonic()
 try:
  q=subprocess.run(argv,cwd=H,env=env,capture_output=True,timeout=timeout)
  out,err,code,timed=q.stdout,q.stderr,q.returncode,False
 except subprocess.TimeoutExpired as ex:
  out,err,code,timed=ex.stdout or b'',ex.stderr or b'',None,True
 (E/(name+'.stdout')).write_bytes(out)
 (E/(name+'.stderr')).write_bytes(err)
 r=dict(argv=argv,cwd=str(H),env={k:env[k] for k in keys},elapsed_seconds=time.monotonic()-start,exit=code,timeout=timed)
 dump(name+'.receipt.json',r);print(json.dumps(r),flush=True)
 assert code==0 and not timed,'command failed; stop without rerun'
 return out
def binary_record(role):
 p=E/'public-comparison'/(role+'-mvcc.test')
 s=p.stat()
 return dict(role=role,path=str(p),bytes=s.st_size,dev=s.st_dev,inode=s.st_ino,mtime_ns=s.st_mtime_ns,sha256=sha(p.read_bytes()))
build=[]
for role in ['baseline','candidate']:
 cmd=[str(G/'bin/go'),'test','-c','-tags=treedb_test','-trimpath','-buildvcs=false']
 if role=='baseline':cmd+=['-overlay',str(E/'baseline-overlay.json')]
 cmd+=['-o',str(E/'public-comparison'/(role+'-mvcc.test')),'./TreeDB/mvcc']
 build.append(dict(role=role,argv=cmd))
focused=[
 ('focused-producer',[str(G/'bin/go'),'test','./TreeDB/freelist','-run','^TestCOWGenerationRequirementDistinguishesInheritedMetadataV1$','-count=1','-timeout=55s','-v']),
 ('focused-public',[str(G/'bin/go'),'test','-tags=treedb_test','./TreeDB/mvcc','-run','^TestCommitAtPrimaryOnlyRetainsActualDataGeneration$','-count=1','-timeout=55s','-v'])]
plan=[]
for pair,roles in enumerate([['baseline','candidate'],['candidate','baseline'],['baseline','candidate']],1):
 for role in roles:
  plan.append(dict(index=len(plan)+1,pair=pair,role=role,argv=['taskset','-c','0',str(E/'public-comparison'/(role+'-mvcc.test')),'-test.run=^$','-test.bench=^BenchmarkCommitAtPrimaryOnlyAfterDataGeneration$','-test.benchtime=128x','-test.benchmem','-test.count=1','-test.timeout=60s']))
before=census()
if not (args.focused or args.build or args.execute):
 print(json.dumps(dict(preflight='PASS',source_census_sha256=P['source_input_census_sha256'],focused_commands=focused,build_commands=build,timing_plan=plan,env={k:env[k] for k in keys},limits=dict(focused_aggregate_seconds=120,build_each_seconds=120,build_aggregate_seconds=180,timing_each_seconds=60,timing_aggregate_seconds=180)),indent=2));raise SystemExit(0)
stage='focused' if args.focused else 'build' if args.build else 'timing'
dump(stage+'-started.json',dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
dump(stage+'-inputs-before.json',before)
dump(stage+'-host-before.json',host())
if args.focused:
 deadline=time.monotonic()+120
 out=run('gofmt-check',[str(G/'bin/gofmt'),'-d',*P['owned_paths']],deadline)
 assert not out,'frozen source needs formatting; stop before Go tests'
 for name,cmd in focused:run(name,cmd,deadline)
 dump('focused-inputs-after.json',census())
 dump('focused-host-after.json',host())
 dump('focused-result.json',dict(status='PASS',source_unchanged=True))
elif args.build:
 deadline=time.monotonic()+180
 (E/'public-comparison').mkdir(exist_ok=True)
 for x in build:run('build-'+x['role'],x['argv'],deadline)
 dump('build-inputs-after.json',census())
 dump('binaries.json',[binary_record(r) for r in ['baseline','candidate']])
 run('toolchain-version',[str(G/'bin/go'),'version'],deadline)
 run('toolchain-env',[str(G/'bin/go'),'env','-json'],deadline)
 dump('build-host-after.json',host())
 dump('build-result.json',dict(status='PASS',source_unchanged=True))
else:
 binaries=json.loads((E/'binaries.json').read_text())
 assert [binary_record(r) for r in ['baseline','candidate']]==binaries,'binary drift'
 dump('timing-binaries-before.json',binaries)
 dump('timing-plan.json',plan)
 deadline=time.monotonic()+180
 samples=[]
 for x in plan:
  # Each public measurement has its own strict 60s bound.
  out=run('sample-%02d-%s'%(x['index'],x['role']),x['argv'],min(deadline,time.monotonic()+60))
  line=re.search(rb'^BenchmarkCommitAtPrimaryOnlyAfterDataGeneration(?:-\d+)?\s+(\d+)\s+(.*?)\s*$',out,re.M)
  assert line and int(line[1])==128,'missing or unexpected benchmark'
  tokens=line[2].decode().split()
  assert len(tokens)%2==0
  metrics={tokens[i+1]:float(tokens[i]) for i in range(0,len(tokens),2)}
  assert set(metrics)=={'ns/op','B/op','allocs/op','generation_advances/op'},metrics
  samples.append(dict(index=x['index'],pair=x['pair'],role=x['role'],iterations=128,metrics=metrics))
  dump('sample-%02d-metrics.json'%x['index'],samples[-1])
  print(json.dumps(samples[-1]),flush=True)
 dump('timing-inputs-after.json',census())
 after=[binary_record(r) for r in ['baseline','candidate']]
 assert after==binaries,'binary drift'
 dump('timing-binaries-after.json',after)
 dump('timing-host-after.json',host())
 med={role:{k:statistics.median([s['metrics'][k] for s in samples if s['role']==role]) for k in samples[0]['metrics']} for role in ['baseline','candidate']}
 delta={k:(100*(med['candidate'][k]/med['baseline'][k]-1) if med['baseline'][k] else None) for k in med['baseline']}
 gates=dict(candidate_generation_stable=all(s['metrics']['generation_advances/op']==0 for s in samples if s['role']=='candidate'),latency=delta['ns/op']<=5,bytes=med['candidate']['B/op']<=med['baseline']['B/op'],objects=med['candidate']['allocs/op']<=med['baseline']['allocs/op'])
 result=dict(status='PASS' if all(gates.values()) else 'FAIL',samples=samples,medians=med,percent_change=delta,gates=gates,scope=P['scope'])
 dump('results.json',result);print(json.dumps(result,indent=2),flush=True)
