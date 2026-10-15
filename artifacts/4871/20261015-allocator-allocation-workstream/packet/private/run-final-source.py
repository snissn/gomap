from pathlib import Path
import hashlib,json,os,re,statistics,subprocess,time,datetime,sys
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');src=base/'allocation-integration-20261015';ev=base/'evidence/allocation-workstream-20261015';out=ev/'private/public-final-source';out.mkdir(exist_ok=True)
goroot=base/'toolchains/go1.26.3-from-185-20261014';go=str(goroot/'bin/go');env=dict(os.environ,GOROOT=str(goroot),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
commands=json.loads((out/'commands.json').read_text()) if (out/'commands.json').exists() else [];start=time.monotonic()
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
if sys.argv[1]=='build':
 manifest=ev/'review-repair-composition-source.json';assert sha(manifest.read_bytes())=='5a06010306af8c9120a5124902b4e25b250c0a1db3618efd58cdb239a30bd7b3'
 for row in json.loads(manifest.read_text()):assert sha((src/row['path']).read_bytes())==row['sha256'],row['path']
 inputs=census();(out/'source-before.json').write_text(json.dumps(dict(head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=src,text=True).strip(),composition_manifest_sha256=sha(manifest.read_bytes()),inputs=inputs),indent=2)+'\n')
 run('freelist-full',[go,'test','./TreeDB/freelist','-count=1','-timeout=60s'],timeout=70)
 race=r'^Test(FreelistTxnPrivate.*|FreelistTxnCloneRevokesPrivateAliases|FreelistTxnMaterializeValidationRevokesPrivateAuthority|GenerationTopology5131SnapshotDirectLeafWorkAdmission|SnapshotPageUnusedV1PreservesPinnedRetirementBoundary)$'
 run('freelist-private-snapshot-race',[go,'test','-race','./TreeDB/freelist','-run',race,'-count=1','-timeout=60s'],timeout=100)
 run('build-final',[go,'test','-c','-trimpath','-buildvcs=false','-o',str(out/'final-db.test'),'./TreeDB/db'],timeout=120)
 assert census()==inputs,'source drift'
 (out/'build-ready.json').write_text(json.dumps(dict(phase='BUILD_READY_TIMING_NOT_STARTED',wall_seconds=time.monotonic()-start,binary_sha256=sha((out/'final-db.test').read_bytes())),indent=2)+'\n');print('BUILD_READY_TIMING_NOT_STARTED',flush=True)
else:
 inputs=json.loads((out/'source-before.json').read_text())['inputs'];assert census()==inputs,'source drift'
 baseline=base/'evidence/direct-bottleneck-profiles-20261015/ordinary-db.test';assert sha(baseline.read_bytes())=='eef997f95aab5f91b7096f3e53b69a51f5c21eb9aed569c73f60df45e1f9f53a';final=out/'final-db.test'
 (out/'binaries.json').write_text(json.dumps({name:dict(path=str(p),size=p.stat().st_size,sha256=sha(p.read_bytes())) for name,p in [('ordinary',baseline),('final',final)]},indent=2)+'\n')
 rows=[];bench=['-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s','-test.benchtime=1000x'];print('TIMED_PAIRS_START',flush=True)
 for pair in range(3):
  order=[('ordinary',baseline),('final',final)]
  if pair%2:order.reverse()
  for name,binary in order:
   raw=run(f'pair{pair+1}-{name}',['taskset','-c','0',str(binary),*bench],timeout=70);m=re.search(r'^BenchmarkConditionalTxnBaselineBatchWrite\s+(\d+)\s+([0-9.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',raw,re.M);assert m,raw
   rows.append(dict(pair=pair+1,kind=name,n=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4])));(out/'rows.json').write_text(json.dumps(rows,indent=2)+'\n')
 med={name:{k:statistics.median(r[k] for r in rows if r['kind']==name) for k in ['ns_per_op','bytes_per_op','allocs_per_op']} for name in ['ordinary','final']};summary=dict(status='FINAL_SOURCE_MATCHED_FIXED_COUNT_PUBLIC_DIAGNOSTIC',rows=rows,medians=med,percent_change={k:100*(med['final'][k]/med['ordinary'][k]-1) for k in med['final']},limitations=['1000x short paired runs; unchanged10000key seed and stopped-timer Checkpoint every8writes.','Private incremental measurements apply to prior source; changes since then are gofmt and snapshot classifier repair unused by this benchmark. They are not relabeled as final-source profiles.'])
 (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('FINAL_SOURCE_SUMMARY '+json.dumps(summary),flush=True)
 focus='db'+chr(92)+'.'+chr(92)+'('+chr(92)+'*Batch'+chr(92)+')'+chr(92)+'.Write$'
 for name,binary in [('ordinary',baseline),('final',final)]:
  profile=out/(name+'-alloc.pprof');run(name+'-alloc',['taskset','-c','0',str(binary),*bench,'-test.memprofile='+str(profile),'-test.memprofilerate=1'],timeout=70)
  for sample,unit in [('alloc_space','bytes'),('alloc_objects','count')]:run(name+'-write-exact-'+sample,[go,'tool','pprof','-top','-nodecount=0','-nodefraction=0','-edgefraction=0','-unit='+unit,'-sample_index='+sample,'-focus='+focus,str(binary),str(profile)])
 assert census()==inputs,'source drift'
 (out/'source-after.json').write_bytes((out/'source-before.json').read_bytes());(out/'receipt.json').write_text(json.dumps(dict(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),phase2_wall_seconds=time.monotonic()-start,status='FINAL_SOURCE_CAPTURE_COMPLETE'),indent=2)+'\n');print('RUNTIME_RELEASED',flush=True)
