import pathlib,subprocess,json,hashlib,os,time,argparse,re,statistics
E=pathlib.Path(__file__).resolve().parent
p=argparse.ArgumentParser(); p.add_argument('--execute',action='store_true'); a=p.parse_args()
manifest=json.loads((E/'binaries.json').read_text()); by={r['role']:r for r in manifest}
assert set(by)=={'pre','ordinary','combined'}
G='/home/mikers/benchmarks/mvcc-4871-repair-20261013/toolchains/go1.26.3-from-185-20261014'
env=os.environ.copy(); env.update(GOROOT=G,GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
orders=[['pre','ordinary','combined'],['ordinary','combined','pre'],['combined','pre','ordinary']]
plan=[]
for triad,roles in enumerate(orders,1):
 for position,role in enumerate(roles,1):
  plan.append(dict(index=len(plan)+1,triad=triad,position=position,role=role,args=['taskset','-c','0',by[role]['binary'],'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s','-test.benchtime=1000x']))
def census(row):
 w=pathlib.Path(row['worktree']); assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip()==row['source']
 assert not subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all','--ignored'],cwd=w)
 entries=[]
 for pb in subprocess.check_output(['git','ls-files','-z'],cwd=w).split(b'\0'):
  x=pb.decode()
  if x and (x.endswith('.go') or pathlib.PurePosixPath(x).name in ['go.mod','go.sum','go.work','go.work.sum']):
   data=(w/x).read_bytes(); entries.append(dict(path=x,size=len(data),sha256=hashlib.sha256(data).hexdigest()))
 assert hashlib.sha256(json.dumps(entries,sort_keys=True).encode()).hexdigest()==row['input_census_sha256']
 assert hashlib.sha256(pathlib.Path(row['binary']).read_bytes()).hexdigest()==row['binary_sha256']
 return entries
checks={r['role']:census(r) for r in manifest}
expected_plan=dict(samples=plan,env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']},fixture='unchanged public default-engine Batch.Write; 10000 seed entries; 100ms root publication delay; stopped-timer Checkpoint every eight writes; foreground publisher/debt assertions',scope='short component/pre-sprint diagnostic only; not final eight-repeat, M0, Dgraph, or native qualification')
if not a.execute:
 print(json.dumps(dict(preflight='PASS',plan=expected_plan),indent=2)); raise SystemExit(0)
assert not (E/'timing-started.json').exists(),'execute only once'
(E/'timing-plan.json').write_text(json.dumps(expected_plan,indent=2)+'\n')
(E/'timing-inputs-before.json').write_text(json.dumps(checks,indent=2)+'\n')
(E/'timing-started.json').write_text(json.dumps(dict(utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),affinity=sorted(os.sched_getaffinity(0))),indent=2)+'\n')
results=[]
for sample in plan:
 prefix=E/('sample-%02d-%s'%(sample['index'],sample['role'])); t=time.monotonic()
 try:
  out=subprocess.run(sample['args'],env=env,cwd=by[sample['role']]['worktree'],capture_output=True,timeout=65)
 except subprocess.TimeoutExpired as ex:
  prefix.with_suffix('.stdout').write_bytes(ex.stdout or b''); prefix.with_suffix('.stderr').write_bytes(ex.stderr or b'')
  (E/'failure.json').write_text(json.dumps(dict(sample=sample,reason='outer timeout',elapsed=time.monotonic()-t),indent=2)+'\n'); raise
 prefix.with_suffix('.stdout').write_bytes(out.stdout); prefix.with_suffix('.stderr').write_bytes(out.stderr)
 row=dict(**sample,elapsed=time.monotonic()-t,returncode=out.returncode)
 match=re.search(rb'^BenchmarkConditionalTxnBaselineBatchWrite(?:-\d+)?\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op\s*$',out.stdout,re.M)
 if match:
  row.update(iterations=int(match[1]),ns_per_op=float(match[2]),bytes_per_op=int(match[3]),allocs_per_op=int(match[4]))
 results.append(row); (E/'samples.json').write_text(json.dumps(results,indent=2)+'\n'); print(json.dumps(row),flush=True)
 if out.returncode!=0 or not match or row['iterations']!=1000:
  (E/'failure.json').write_text(json.dumps(dict(sample=row,reason='benchmark failed or unexpected result'),indent=2)+'\n'); raise SystemExit(1)
after={r['role']:census(r) for r in manifest}; assert after==checks
(E/'timing-inputs-after.json').write_text(json.dumps(after,indent=2)+'\n')
medians={role:{metric:statistics.median([r[metric] for r in results if r['role']==role]) for metric in ['ns_per_op','bytes_per_op','allocs_per_op']} for role in by}
deltas={role:{metric:100*(medians['combined'][metric]/medians[role][metric]-1) for metric in medians[role]} for role in ['pre','ordinary']}
summary=dict(samples=results,medians=medians,combined_percent_change=deltas,qualification=expected_plan['scope'],complete=True)
(E/'results.json').write_text(json.dumps(summary,indent=2)+'\n'); print(json.dumps(summary,indent=2),flush=True)
