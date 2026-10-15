import pathlib,subprocess,json,hashlib,os,time,argparse,re,statistics
E=pathlib.Path(__file__).resolve().parent
p=argparse.ArgumentParser(); z=p.add_mutually_exclusive_group(); z.add_argument('--build',action='store_true'); z.add_argument('--execute',action='store_true'); a=p.parse_args()
pre=json.loads((E/'source-preflight.json').read_text()); sources=pre['sources']; by={r['role']:r for r in sources}
G='/home/mikers/benchmarks/mvcc-4871-repair-20261013/toolchains/go1.26.3-from-185-20261014'
env=os.environ.copy(); env.update(GOROOT=G,GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
assert not {k:v for k,v in env.items() if k.startswith(('TREEDB','GOMAP','GOGC','GOMEMLIMIT','GODEBUG'))},'unexpected engine/runtime override'
def dump(name,x): (E/name).write_text(json.dumps(x,indent=2)+'\n')
def census(row):
 w=pathlib.Path(row['worktree']); assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip()==row['head']; assert subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all','--ignored'],cwd=w,text=True)==row['status']; st=w.stat(); assert dict(dev=st.st_dev,inode=st.st_ino)==row['root_identity']
 entries=[]
 for pb in sorted(set(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=w).split(b'\0'))):
  x=pb.decode()
  if x and (x.endswith('.go') or pathlib.PurePosixPath(x).name in ['go.mod','go.sum','go.work','go.work.sum']):
   d=(w/x).read_bytes(); entries.append(dict(path=x,size=len(d),sha256=hashlib.sha256(d).hexdigest()))
 assert hashlib.sha256(json.dumps(entries,sort_keys=True).encode()).hexdigest()==row['source_input_census_sha256']; return entries
checks={r['role']:census(r) for r in sources}
plan=[]
for pair,roles in enumerate([['baseline','candidate'],['candidate','baseline'],['baseline','candidate']],1):
 for role in roles: plan.append(dict(index=len(plan)+1,pair=pair,role=role,args=['taskset','-c','0',by[role]['binary'],'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s','-test.benchtime=1000x']))
builds=[dict(role=r['role'],cwd=r['worktree'],args=[G+'/bin/go','test','-c','-trimpath','-buildvcs=false','-o',r['binary'],'./TreeDB/db'],timeout_seconds=90) for r in sources if r['role']=='candidate']
if not a.build and not a.execute:
 print(json.dumps(dict(preflight='PASS',build_commands=builds,timing_plan=plan,env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']}),indent=2)); raise SystemExit(0)
if a.build:
 assert not (E/'build-started.json').exists(),'build stage once only'; dump('build-started.json',dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
 dump('build-inputs-before.json',checks); records=[]; binaries=[pre['reuse_baseline_binary']]
 for x in builds:
  t=time.monotonic()
  try: q=subprocess.run(x['args'],cwd=x['cwd'],env=env,capture_output=True,timeout=90)
  except subprocess.TimeoutExpired as ex:
   (E/(x['role']+'-build.stdout')).write_bytes(ex.stdout or b''); (E/(x['role']+'-build.stderr')).write_bytes(ex.stderr or b''); dump('build-failure.json',dict(command=x,timeout=True)); raise
  (E/(x['role']+'-build.stdout')).write_bytes(q.stdout); (E/(x['role']+'-build.stderr')).write_bytes(q.stderr)
  record=dict(**x,elapsed_seconds=time.monotonic()-t,returncode=q.returncode,env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']}); records.append(record); dump('build-commands.json',records); print(json.dumps(record),flush=True); assert q.returncode==0
  bp=pathlib.Path(by[x['role']]['binary']); st=bp.stat(); binaries.append(dict(role=x['role'],path=str(bp),sha256=hashlib.sha256(bp.read_bytes()).hexdigest(),bytes=st.st_size,dev=st.st_dev,inode=st.st_ino,mtime_ns=st.st_mtime_ns))
 after={r['role']:census(r) for r in sources}; assert checks==after; dump('build-inputs-after.json',after); dump('binaries.json',binaries)
 for name,args in [('toolchain',[G+'/bin/go','version']),('toolchain-env',[G+'/bin/go','env','-json'])]:
  q=subprocess.run(args,env=env,capture_output=True); (E/(name+'.stdout')).write_bytes(q.stdout); (E/(name+'.stderr')).write_bytes(q.stderr); assert q.returncode==0
 print('BUILD_PASS',flush=True); raise SystemExit(0)
binaries=json.loads((E/'binaries.json').read_text())
for r in binaries: assert hashlib.sha256(pathlib.Path(r['path']).read_bytes()).hexdigest()==r['sha256']
assert not (E/'timing-started.json').exists(),'timing once only'
def host(): return dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),loadavg=pathlib.Path('/proc/loadavg').read_text(),stat=pathlib.Path('/proc/stat').read_text(),processes=subprocess.check_output(['ps','-eo','pid,ppid,stat,pcpu,etimes,args'],text=True))
dump('host-before.json',host()); dump('timing-inputs-before.json',checks); dump('timing-plan.json',plan); dump('timing-started.json',dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())))
samples=[]
for x in plan:
 t=time.monotonic(); prefix=E/('sample-%02d-%s'%(x['index'],x['role']))
 try: q=subprocess.run(x['args'],cwd=by[x['role']]['worktree'],env=env,capture_output=True,timeout=60)
 except subprocess.TimeoutExpired as ex:
  prefix.with_suffix('.stdout').write_bytes(ex.stdout or b''); prefix.with_suffix('.stderr').write_bytes(ex.stderr or b''); dump('timing-failure.json',dict(sample=x,timeout=True)); raise
 prefix.with_suffix('.stdout').write_bytes(q.stdout); prefix.with_suffix('.stderr').write_bytes(q.stderr)
 row=dict(**x,elapsed_seconds=time.monotonic()-t,returncode=q.returncode)
 m=re.search(rb'^BenchmarkConditionalTxnBaselineBatchWrite(?:-\d+)?\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op\s*$',q.stdout,re.M)
 if m: row.update(iterations=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4]))
 samples.append(row); dump('samples.json',samples); print(json.dumps(row),flush=True)
 assert q.returncode==0 and m and row['iterations']==1000,'failed or unexpected benchmark; retain without rerun'
dump('host-after.json',host()); after={r['role']:census(r) for r in sources}; assert checks==after; dump('timing-inputs-after.json',after)
for r in binaries: assert hashlib.sha256(pathlib.Path(r['path']).read_bytes()).hexdigest()==r['sha256']
metrics=['ns_per_op','bytes_per_op','allocs_per_op']; med={role:{k:statistics.median([x[k] for x in samples if x['role']==role]) for k in metrics} for role in by}; delta={k:100*(med['candidate'][k]/med['baseline'][k]-1) for k in metrics}
gates=dict(latency=delta['ns_per_op']<=5,bytes=med['candidate']['bytes_per_op']<=med['baseline']['bytes_per_op'],objects=med['candidate']['allocs_per_op']<=med['baseline']['allocs_per_op'])
result=dict(complete=True,samples=samples,medians=med,percent_change=delta,gates=gates,diagnostic_viable=all(gates.values()),scope='Short ordinary public default-engine foreground Batch.Write guard only; seed/setup, Checkpoint every8, final DB Close outside timer; batch Close included. No native ACK or final qualification claim; no arena implementation change.')
dump('results.json',result); print(json.dumps(result,indent=2),flush=True)
