import pathlib,subprocess,json,os,hashlib,time
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');O=D/'performance-185/f041-variants-01';G=D/'toolchains/go1.26.3'
assert all(x['exit']==0 for x in json.loads((O/'controls.json').read_text()))
E=os.environ.copy();E.update(GOMAXPROCS='1',TMPDIR='/mnt/fast4tb/mvcc4871tmp',GOROOT=str(G),GOTOOLCHAIN='local',GOWORK='off')
plan=[]
for label,pkg,rx,iters,variants in [('get','db','^BenchmarkGetVersioned$',8000000,['control','reset','pager']),('seek','treedb','^BenchmarkSnapshotIteratorSeekNext$/^keys=1024$/^snapshot_seek$',4000000,['control','pager'])]:
 for variant in variants:
  b=O/(variant+'-'+pkg+'.test');name=label+'-'+variant
  argv=['taskset','-c','0',str(b),'-test.run=^$','-test.bench='+rx,'-test.benchtime='+str(iters)+'x','-test.count=1','-test.cpuprofile='+str(O/(name+'.pprof'))]
  plan.append({'label':name,'argv':argv,'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest()})
(O/'profile-plan.json').write_text(json.dumps({'claim':'NONQUALIFYING_CPU_ATTRIBUTION_BUSY_HOST','source':'f0415773+recorded overlay','toolchain':'Go1.26.3','plan':plan},indent=2)+'\n')
(O/'processes-before.txt').write_text(subprocess.run(['ps','-eo','pid,pcpu,pmem,etime,comm','--sort=-pcpu'],capture_output=True,text=True).stdout)
results=[]
for p in plan:
 t=time.time()
 with (O/(p['label']+'.txt')).open('w') as f:r=subprocess.run(p['argv'],cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT,timeout=60)
 results.append({**p,'exit':r.returncode,'elapsed_s':time.time()-t});(O/'profile-results.json').write_text(json.dumps(results,indent=2)+'\n');print(p['label'],r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
 for suffix,extra in [('top',[]),('cum',['-cum'])]:
  with (O/(p['label']+'-'+suffix+'.txt')).open('w') as f:subprocess.run([str(G/'bin/go'),'tool','pprof','-top','-nodecount=45',*extra,p['argv'][3],str(O/(p['label']+'.pprof'))],cwd=O,env=E,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=30)
