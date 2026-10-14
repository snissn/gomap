from pathlib import Path
import json,subprocess,os,time,hashlib
base=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014')
out=base/'evidence/snapshot-allocation-attribution';out.mkdir(exist_ok=False)
goRoot=base.parent/'toolchains/go1.26.3-from-185-20261014'
env=dict(os.environ,GOROOT=str(goRoot),GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='4',SPRINT_MEMTABLE='cow_btree')
regex='^BenchmarkSprintMVCC$/^command_wal_relaxed$/^cow_btree$/^inline$/^point$'
def sha(b):return hashlib.sha256(b).hexdigest()
for label in ['recent-main','coherent-candidate']:
 binary=base/'binaries'/(label+'-mvcc.test');profile=out/(label+'.allocs.pprof');log=out/(label+'.log')
 temp=out/(label+'-temp');temp.mkdir();env['TMPDIR']=str(temp)
 argv=['taskset','-c','0-3',str(binary),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime=64x','-test.memprofile='+str(profile),'-test.memprofilerate=1','-test.count=1','-test.v','-test.timeout=30s']
 start=time.monotonic()
 with log.open('wb') as f:r=subprocess.run(argv,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=35)
 assert r.returncode==0
 (out/(label+'.receipt.json')).write_text(json.dumps({'scope':'Attribution ONLY, instrumented timing not used','argv':argv,'exit':r.returncode,'elapsed':time.monotonic()-start,'binary_sha256':sha(binary.read_bytes()),'raw_sha256':sha(log.read_bytes()),'profile_sha256':sha(profile.read_bytes())},indent=1)+'\n')
for metric in ['alloc_space','alloc_objects']:
 argv=[str(goRoot/'bin/go'),'tool','pprof','-top','-nodecount=35','-'+metric,'-base',str(out/'recent-main.allocs.pprof'),str(base/'binaries/coherent-candidate-mvcc.test'),str(out/'coherent-candidate.allocs.pprof')]
 r=subprocess.run(argv,env=env,text=True,capture_output=True);assert r.returncode==0
 (out/('diff-'+metric+'.txt')).write_text(r.stdout+r.stderr)
 print(r.stdout[:6000],flush=True)
