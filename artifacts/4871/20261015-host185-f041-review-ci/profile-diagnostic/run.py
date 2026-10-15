import pathlib,subprocess,json,os,hashlib,time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
A=D/'live-startup/f041-ci/raw-path-artifact'
O=D/'performance-185/f041-hosted-profile-01'
E=os.environ.copy();E.update(GOMAXPROCS='1',TMPDIR=str(O/'tmp'),GOROOT=str(D/'toolchains/go1.26.3'),GOTOOLCHAIN='local',GOWORK='off')
(O/'tmp').mkdir()
meta=json.loads((A/'measured-binaries.json').read_text()); mm={x['path']:x for x in meta['files']}
plan=[]
for label,pkg,rx,iters in [('get','db','^BenchmarkGetVersioned$',8000000),('seek','treedb','^BenchmarkSnapshotIteratorSeekNext$/^keys=1024$/^snapshot_seek$',4000000)]:
 for side in ['baseline','candidate']:
  rel='measured-binaries/'+side+'-'+pkg+'.test'; b=A/rel
  h=hashlib.sha256(b.read_bytes()).hexdigest();assert h==mm[rel]['sha256']
  b.chmod(mm[rel]['mode'])
  argv=['taskset','-c','0',str(b),'-test.run=^$','-test.bench='+rx,'-test.benchtime='+str(iters)+'x','-test.count=1','-test.cpuprofile='+str(O/(label+'-'+side+'.pprof'))]
  plan.append({'label':label+'-'+side,'argv':argv,'source':mm[rel]['source_sha'],'binary_sha256':h})
(O/'plan.json').write_text(json.dumps({'claim':'NONQUALIFYING_CPU_ATTRIBUTION_ONLY_BUSY_HOST_NO_LATENCY_ACCEPTANCE','source':'exact hosted Go1.26.9 binaries','analysis_toolchain':'local Go1.26.3','environment':{k:E[k] for k in ['GOMAXPROCS','TMPDIR','GOROOT','GOTOOLCHAIN','GOWORK']},'plan':plan},indent=2)+'\n')
(O/'processes-before.txt').write_text(subprocess.run(['ps','-eo','pid,pcpu,pmem,etime,comm','--sort=-pcpu'],capture_output=True,text=True).stdout)
results=[]
for p in plan:
 start=time.time()
 with (O/(p['label']+'.txt')).open('w') as f:
  r=subprocess.run(p['argv'],cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=60)
 result={**p,'exit':r.returncode,'elapsed_s':time.time()-start};results.append(result)
 (O/'results.json').write_text(json.dumps(results,indent=2)+'\n')
 print(p['label'],r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
 for suffix,extra in [('top',[]),('cum',['-cum'])]:
  cmd=[str(D/'toolchains/go1.26.3/bin/go'),'tool','pprof','-top','-nodecount=35',*extra,p['argv'][3],str(O/(p['label']+'.pprof'))]
  with (O/(p['label']+'-'+suffix+'.txt')).open('w') as f:q=subprocess.run(cmd,cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=30)
  if q.returncode:raise SystemExit(q.returncode)
