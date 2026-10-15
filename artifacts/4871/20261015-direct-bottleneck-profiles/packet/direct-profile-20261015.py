from pathlib import Path
import hashlib,json,os,re,subprocess,time,datetime
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');out=base/'evidence/direct-bottleneck-profiles-20261015';assert not out.exists();out.mkdir()
goroot=base/'toolchains/go1.26.3-from-185-20261014';go=str(goroot/'bin/go');ordinary=base/'ordinary-5111'
def sha(b):return hashlib.sha256(b).hexdigest()
env=dict(os.environ,GOROOT=str(goroot),GOWORK='off',GOTOOLCHAIN='local',CGO_ENABLED='1',GOMAXPROCS='4')
commands=[]
def run(name,argv,where=ordinary,extra=None,timeout=120):
 local=dict(env,**(extra or {}));log=out/(name+'.raw');assert not log.exists();start=time.monotonic();timed=False
 with log.open('wb') as f:
  try:code=subprocess.run(argv,cwd=where,env=local,stdout=f,stderr=subprocess.STDOUT,timeout=timeout).returncode
  except subprocess.TimeoutExpired:code=124;timed=True
 r={'name':name,'argv':argv,'cwd':str(where),'env':{k:local[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS']+list(extra or {})},'exit':code,'timeout':timed,'wall_seconds':time.monotonic()-start,'raw_sha256':sha(log.read_bytes()),'raw_bytes':log.stat().st_size}
 commands.append(r);(out/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');print(json.dumps(r),flush=True)
 return code
processes=subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu'],text=True);(out/'processes-before.txt').write_text(processes)
for line in processes.splitlines()[1:]:
 command=' '.join(line.split()[4:])
 if re.search(r'(?:/bin/go (?:test|build)|/pkg/tool/[^ ]+/(?:compile|link)|\.test(?: |$)|unified-bench(?: |$))',command):raise SystemExit('Active foreign/owned producer: '+line)
native=base/'evidence/native-shared-append-workspace-20261015/after-mvcc.test';assert sha(native.read_bytes())=='667990cf2be1cfa1fa1a85317f681e81a144bfc1d0515a33ddff0fc6e6a603d7'
(out/'native-binary.json').write_text(json.dumps({'path':str(native),'size':native.stat().st_size,'sha256':sha(native.read_bytes()),'source':'d7c27d7829e51ad06822340fcc55f03c3831f027','binding':'Shared-workspace frozen4399sourceinput packet; diagnostic only; native mutable source not compiled'},indent=2)+'\n')
assert run('native-diagnostic-cpu',['taskset','-c','0-3',str(native),'-test.run=^TestNativePruneCOWRetainedOutputDiagnosticLatency$/^pointertrue$','-test.count=1200','-test.timeout=90s','-test.v','-test.cpuprofile='+str(out/'native-diagnostic-cpu.pprof')],where=out)==0
raw=(out/'native-diagnostic-cpu.raw').read_text();assert raw.count('NATIVE_COST_CLOSE_REOPEN_CHECKS_COMPLETE')==1200 and raw.count('ACK=1 nativePrepared_observations=90')==1200
head=subprocess.check_output(['git','-C',str(ordinary),'rev-parse','HEAD'],text=True).strip();assert head=='177fb0c8db1d19c79d3d975126b4b0b64019568e'
assert not subprocess.check_output(['git','-C',str(ordinary),'status','--porcelain','-uall'])
paths=subprocess.check_output(['git','-C',str(ordinary),'ls-files','-z']).decode().split('\0');census={p:{'size':(ordinary/p).stat().st_size,'sha256':sha((ordinary/p).read_bytes())} for p in paths if p and (p.endswith('.go') or Path(p).name in ['go.mod','go.sum'])}
(out/'ordinary-source-before.json').write_text(json.dumps({'head':head,'inputs':census},sort_keys=True,indent=2)+'\n')
for package in ['db','treedb']:
 pkg='./TreeDB/db' if package=='db' else './TreeDB';binary=out/('ordinary-'+package+'.test')
 assert run('build-ordinary-'+package,[go,'test','-c','-trimpath','-buildvcs=false','-o',str(binary),pkg])==0
bins={p.name:{'size':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in out.glob('ordinary-*.test')};(out/'ordinary-binaries.json').write_text(json.dumps(bins,indent=2)+'\n')
cells=[('get','db','^BenchmarkGetVersioned$'),('batch','db','^BenchmarkConditionalTxnBaselineBatchWrite$'),('seek','treedb','^BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek$')]
for name,package,regex in cells:
 binary=out/('ordinary-'+package+'.test')
 assert run('ordinary-'+name+'-cpu',['taskset','-c','0',str(binary),'-test.run=^$','-test.bench='+regex,'-test.benchtime=3s','-test.count=1','-test.benchmem','-test.timeout=45s','-test.cpuprofile='+str(out/('ordinary-'+name+'-cpu.pprof'))],extra={'GOMAXPROCS':'1'},timeout=60)==0
 assert re.search(r'^Benchmark\S+\s+\d+\s+', (out/('ordinary-'+name+'-cpu.raw')).read_text(),re.M)
# Allocation attribution is a separate observer invocation; never use its wall as a speedup.
assert run('ordinary-batch-alloc',['taskset','-c','0',str(out/'ordinary-db.test'),'-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.benchtime=3000x','-test.count=1','-test.benchmem','-test.timeout=45s','-test.memprofile='+str(out/'ordinary-batch-alloc.pprof'),'-test.memprofilerate=1'],extra={'GOMAXPROCS':'1'},timeout=60)==0
# Contention diagnostics are separately instrumented and include setup/teardown.
assert run('native-diagnostic-contention',['taskset','-c','0-3',str(native),'-test.run=^TestNativePruneCOWRetainedOutputDiagnosticLatency$/^pointertrue$','-test.count=150','-test.timeout=45s','-test.mutexprofile='+str(out/'native-diagnostic-mutex.pprof'),'-test.mutexprofilefraction=1','-test.blockprofile='+str(out/'native-diagnostic-block.pprof'),'-test.blockprofilerate=1'],where=out,timeout=60)==0
for p,row in census.items():assert (ordinary/p).stat().st_size==row['size'] and sha((ordinary/p).read_bytes())==row['sha256']
assert not subprocess.check_output(['git','-C',str(ordinary),'status','--porcelain','-uall'])
(out/'ordinary-source-after.json').write_bytes((out/'ordinary-source-before.json').read_bytes())
(out/'processes-after.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,args','--sort=-pcpu'],text=True))
# Analysis uses the exact binaries; source listings will be bound separately to frozen committed inputs.
analyses=[('native-diagnostic-cpu',str(native),'native-diagnostic-cpu.pprof',[]),('native-prune-cpu',str(native),'native-diagnostic-cpu.pprof',['-focus=PruneVersionsUnboundedDiagnosticForTest']),('native-mutex',str(native),'native-diagnostic-mutex.pprof',[]),('native-block',str(native),'native-diagnostic-block.pprof',[])]
for name,package,regex in cells:analyses.append(('ordinary-'+name+'-cpu',str(out/('ordinary-'+package+'.test')),'ordinary-'+name+'-cpu.pprof',[]))
analyses.extend([('ordinary-batch-alloc-space',str(out/'ordinary-db.test'),'ordinary-batch-alloc.pprof',['-sample_index=alloc_space']),('ordinary-batch-alloc-objects',str(out/'ordinary-db.test'),'ordinary-batch-alloc.pprof',['-sample_index=alloc_objects'])])
for name,binary,profile,options in analyses:
 assert run('pprof-'+name,[go,'tool','pprof','-top','-nodecount=40',*options,binary,str(out/profile)])==0
 assert run('pprof-'+name+'-cum',[go,'tool','pprof','-top','-cum','-nodecount=35',*options,binary,str(out/profile)])==0
receipt={'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'CAPTURED_DIAGNOSTIC_NOT_QUALIFICATION','ordinary_source':head,'native_source':'d7c27d7829e51ad06822340fcc55f03c3831f027','native_repetitions':{'cpu':1200,'contention':150},'ordinary_source_rows':len(census),'commands':len(commands),'limitations':['Native explicit unbounded diagnostic; actual bounded production remains ACK0.','Native CPU profile includes fixture setup; only samples containing public prune frame attribute foreground prune; GC/background cannot be assigned from that filter.','Three ordinary public micro CPU profiles include setup/teardown, amortized over3s measurement.','CPU/mutex/block/Rate1allocation observers affect timings; no comparative speedup claims.','Short focused native diagnostic is not a representative concurrent Dgraph lock workload.']}
(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt),flush=True)
