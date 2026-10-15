from pathlib import Path
import hashlib,json,os,re,statistics,subprocess,time,datetime
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');src=base/'allocation-integration-20261015';ev=base/'evidence/allocation-workstream-20261015/private';overlay=ev/'topology-only-overlay';out=ev/'public-private-incremental';out.mkdir(exist_ok=False)
goroot=base/'toolchains/go1.26.3-from-185-20261014';go=str(goroot/'bin/go');env=dict(os.environ,GOROOT=str(goroot),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
commands=[];start=time.monotonic()
def sha(data):return hashlib.sha256(data).hexdigest()
def run(name,argv,timeout=90):
 log=out/(name+'.log');at=time.monotonic()
 with log.open('wb') as f:
  try: code=subprocess.run(argv,cwd=src,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout).returncode
  except subprocess.TimeoutExpired:code=124
 row=dict(name=name,argv=argv,cwd=str(src),env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']},exit=code,wall_seconds=time.monotonic()-at,log_sha256=sha(log.read_bytes()))
 commands.append(row);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');print(json.dumps(row),flush=True)
 if code:raise SystemExit(log.read_text())
 return log.read_text()
def census():
 return {str(p.relative_to(src)):dict(size=p.stat().st_size,sha256=sha(p.read_bytes())) for p in sorted(src.rglob('*')) if p.is_file() and (p.suffix=='.go' or p.name in ['go.mod','go.sum']) and '.git' not in p.parts}
manifest=json.loads((overlay/'manifest.json').read_text());inputs=census()
for row in manifest['manifest']:
 assert sha(Path(row['original']).read_bytes())==row['original_sha256'],'overlay source drift'
 assert sha(Path(row['replacement']).read_bytes())==row['replacement_sha256'],'overlay replacement drift'
previous=json.loads((ev/'public-combined-early/source-before.json').read_text())['inputs'];assert inputs==previous,'combined binary no longer binds effective integration inputs'
(out/'source-before.json').write_text(json.dumps(dict(head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=src,text=True).strip(),inputs=inputs,overlay_manifest=manifest),indent=2)+'\n')
combined=ev/'public-combined-early/combined-db.test';assert sha(combined.read_bytes())=='f242def2fa2eb12761fd2a40b6817e92a63cfcf1390040f4febd8eb219e8b0d3'
topology=out/'topology-only-db.test'
run('build-topology-only',[go,'test','-overlay',str(overlay/'overlay.json'),'-c','-trimpath','-buildvcs=false','-o',str(topology),'./TreeDB/db'])
(out/'binaries.json').write_text(json.dumps({name:dict(path=str(p),size=p.stat().st_size,sha256=sha(p.read_bytes())) for name,p in [('combined',combined),('topology-only',topology)]},indent=2)+'\n')
rows=[];bench=['-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s']
print('TIMED_PAIRS_START',flush=True)
for pair in range(3):
 order=[('topology-only',topology),('combined',combined)]
 if pair%2:order.reverse()
 for name,binary in order:
  raw=run(f'pair{pair+1}-{name}',['taskset','-c','0',str(binary),*bench,'-test.benchtime=10000x'],timeout=70)
  m=re.search(r'^BenchmarkConditionalTxnBaselineBatchWrite\s+(\d+)\s+([0-9.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',raw,re.M);assert m,raw
  rows.append(dict(pair=pair+1,kind=name,n=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4])))
  (out/'rows.json').write_text(json.dumps(rows,indent=2)+'\n')
med={name:{k:statistics.median(r[k] for r in rows if r['kind']==name) for k in ['ns_per_op','bytes_per_op','allocs_per_op']} for name in ['topology-only','combined']}
summary=dict(status='MATCHED_FIXED_COUNT_INCREMENTAL_DIAGNOSTIC',rows=rows,medians=med,percent_change={k:100*(med['combined'][k]/med['topology-only'][k]-1) for k in med['combined']},limitations=['Benchmark-only topology counterfactual; private activation tests expected RED and this is not a production correctness candidate.','Same10000key fixture and stopped-timer checkpoint every8writes; no setup/maintenance moved.','Observer profiles separate from timing; clone metric must include flat detachUnmaterialized,stateChunk.clone,cloneStateNode rows.'])
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('INCREMENTAL_SUMMARY '+json.dumps(summary),flush=True)
for name,binary in [('topology-only',topology),('combined',combined)]:
 profile=out/(name+'-alloc.pprof')
 run(name+'-alloc',['taskset','-c','0',str(binary),*bench,'-test.benchtime=1000x','-test.memprofile='+str(profile),'-test.memprofilerate=1'],timeout=70)
 for sample in ['alloc_space','alloc_objects']:
  run(name+'-write-'+sample,[go,'tool','pprof','-top','-nodecount=0','-unit=bytes' if sample=='alloc_space' else '-unit=count','-sample_index='+sample,r'-focus=db\.\(\*Batch\)\.Write$',str(binary),str(profile)])
assert census()==inputs,'source drift during measurement'
(out/'source-after.json').write_bytes((out/'source-before.json').read_bytes())
(out/'processes-after.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu'],text=True))
(out/'receipt.json').write_text(json.dumps(dict(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),wall_seconds=time.monotonic()-start,commands=len(commands),status='CAPTURED_INCREMENTAL_DIAGNOSTIC'),indent=2)+'\n')
print('RUNTIME_RELEASED',flush=True)
