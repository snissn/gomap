from pathlib import Path
import hashlib,json,os,re,statistics,subprocess,time,datetime
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');src=base/'allocation-integration-20261015';out=base/'evidence/allocation-workstream-20261015/private/public-combined-early';out.mkdir(exist_ok=False)
goroot=base/'toolchains/go1.26.3-from-185-20261014';go=str(goroot/'bin/go');env=dict(os.environ,GOROOT=str(goroot),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
commands=[];start=time.monotonic()
def sha(data):return hashlib.sha256(data).hexdigest()
def run(name,argv,cwd=src,timeout=90):
 log=out/(name+'.log');at=time.monotonic()
 with log.open('wb') as f:
  try: code=subprocess.run(argv,cwd=cwd,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout).returncode
  except subprocess.TimeoutExpired:code=124
 row=dict(name=name,argv=argv,cwd=str(cwd),env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']},exit=code,wall_seconds=time.monotonic()-at,sha256=sha(log.read_bytes()))
 commands.append(row);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');print(json.dumps(row),flush=True)
 if code:raise SystemExit(log.read_text())
 return log.read_text()
def census():
 return {str(p.relative_to(src)):dict(size=p.stat().st_size,sha256=sha(p.read_bytes())) for p in sorted(src.rglob('*')) if p.is_file() and (p.suffix=='.go' or p.name in ['go.mod','go.sum']) and '.git' not in p.parts}
inputs=census();(out/'source-before.json').write_text(json.dumps(dict(base=subprocess.check_output(['git','-C',str(src),'rev-parse','HEAD'],text=True).strip(),status=subprocess.check_output(['git','-C',str(src),'status','--porcelain','-uall'],text=True),inputs=inputs),indent=2)+'\n')
baseline=base/'evidence/direct-bottleneck-profiles-20261015/ordinary-db.test';assert sha(baseline.read_bytes())=='eef997f95aab5f91b7096f3e53b69a51f5c21eb9aed569c73f60df45e1f9f53a'
candidate=out/'combined-db.test'
run('build-combined',[go,'test','-c','-trimpath','-buildvcs=false','-o',str(candidate),'./TreeDB/db'])
(out/'binaries.json').write_text(json.dumps({name:dict(path=str(p),size=p.stat().st_size,sha256=sha(p.read_bytes())) for name,p in [('ordinary',baseline),('combined',candidate)]},indent=2)+'\n')
print('TIMED_PAIRS_START',flush=True)
rows=[]
for pair in range(3):
 for name,binary in [('ordinary',baseline),('combined',candidate)]:
  raw=run(f'pair{pair+1}-{name}',['taskset','-c','0',str(binary),'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.benchtime=300ms','-test.count=1','-test.benchmem','-test.timeout=60s'],timeout=70)
  m=re.search(r'^BenchmarkConditionalTxnBaselineBatchWrite\s+(\d+)\s+([0-9.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',raw,re.M);assert m,raw
  rows.append(dict(pair=pair+1,kind=name,n=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4])))
  (out/'rows.json').write_text(json.dumps(rows,indent=2)+'\n')
med={name:{k:statistics.median(r[k] for r in rows if r['kind']==name) for k in ['ns_per_op','bytes_per_op','allocs_per_op']} for name in ['ordinary','combined']}
summary=dict(status='DIRECTIONAL_DIAGNOSTIC_NOT_QUALIFICATION',rows=rows,medians=med,percent_change={k:100*(med['combined'][k]/med['ordinary'][k]-1) for k in med['ordinary']},wall_seconds=time.monotonic()-start,limitations=['300ms adaptive iteration counts differ; no final acceptance; fixed-count final qualification remains required.','Same10000key fixture and stopped-timer checkpoint every8writes; no setup/maintenance moved.','Rate1/CPU observer profiles kept separate from timing comparisons.'])
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('EARLY_SUMMARY '+json.dumps(summary),flush=True)
if time.monotonic()-start < 180:
 run('combined-alloc',['taskset','-c','0',str(candidate),'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.benchtime=1000x','-test.count=1','-test.benchmem','-test.timeout=60s','-test.memprofile='+str(out/'combined-alloc.pprof'),'-test.memprofilerate=1'],timeout=70)
 for sample in ['alloc_space','alloc_objects']:
  run('pprof-write-'+sample,[go,'tool','pprof','-top','-nodecount=60','-sample_index='+sample,'-focus=db\.\(\*Batch\)\.Write$',str(candidate),str(out/'combined-alloc.pprof')])
 run('combined-cpu',['taskset','-c','0',str(candidate),'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.benchtime=1s','-test.count=1','-test.benchmem','-test.timeout=60s','-test.cpuprofile='+str(out/'combined-cpu.pprof')],timeout=70)
 run('pprof-cpu',[go,'tool','pprof','-top','-nodecount=40',str(candidate),str(out/'combined-cpu.pprof')])
assert census()==inputs,'source drift during measurement'
(out/'source-after.json').write_bytes((out/'source-before.json').read_bytes())
(out/'processes-after.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu'],text=True))
(out/'receipt.json').write_text(json.dumps(dict(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),wall_seconds=time.monotonic()-start,commands=len(commands),status='CAPTURED_DIAGNOSTIC_NOT_QUALIFICATION'),indent=2)+'\n')
print('RUNTIME_RELEASED',flush=True)
