import os,json,hashlib,subprocess,time,re,statistics
from pathlib import Path
b=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');s=b/'successor-4876-pointer-tail-20261014';e=b/'evidence/successor-4876-pointer-tail-20261014';go=b/'toolchains/go1.26.3-from-185-20261014/bin/go';env=dict(os.environ,GOROOT=str(go.parent.parent),GOWORK='off',GOTOOLCHAIN='local',GOMAXPROCS='4',CGO_ENABLED='1',SPRINT_MEMTABLE='cow_btree',SPRINT_REQUIRE_PRIMARY='1');receipts=[]
def run(label,argv,environment):
 start=time.time();p=subprocess.run(argv,cwd=s,env=environment,stdout=subprocess.PIPE,stderr=subprocess.STDOUT);raw=e/(label+'.raw');raw.write_bytes(p.stdout);r=dict(label=label,argv=argv,env={k:environment[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','GOMAXPROCS','CGO_ENABLED','SPRINT_MEMTABLE','SPRINT_REQUIRE_PRIMARY','SPRINT_POPULATED_SIZE'] if k in environment},exit=p.returncode,elapsed_s=time.time()-start,raw=str(raw),sha256=hashlib.sha256(p.stdout).hexdigest());receipts.append(r);(e/'measurement-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n');print(label,p.returncode,flush=True)
 if p.returncode:raise SystemExit(p.returncode)
 return p.stdout.decode(),r
quiet=subprocess.check_output(['ps','-eo','pid,ppid,pcpu,stat,args'],text=True);(e/'compile-preflight.raw').write_text(quiet)
foreign=[l for l in quiet.splitlines() if re.search(r'(?:^|\s)(?:[^ ]*/)?(?:go|compile|link|[^ /]+\.test)(?:\s|$)',l) and 'measurement-driver' not in l]
if foreign:raise SystemExit('Go producer prevents compile: '+str(foreign))
for version in ['before','after']:
 argv=[str(go),'test'];
 if version=='before':argv+=['-overlay='+str(e/'baseline-overlay.json')]
 argv+=['-c','-trimpath','-buildvcs=false','-o',str(e/(version+'-mvcc.test')),'./TreeDB/mvcc/sprintperf'];run(version+'-build-measurement',argv,env)
identities={}
for version in ['before','after']:identities[version]=hashlib.sha256((e/(version+'-mvcc.test')).read_bytes()).hexdigest()
(e/'binary-identities.json').write_text(json.dumps(identities,indent=2)+'\n')
rows=[]
for size in ['small','multilevel']:
 for shape in ['forced_pointer']:
  for pair,order in [(1,['before','after']),(2,['after','before'])]:
   quiet=subprocess.check_output(['ps','-eo','pid,ppid,pcpu,stat,args'],text=True);(e/('quiet-'+size+'-'+shape+'-'+str(pair)+'.raw')).write_text(quiet)
   foreign=[l for l in quiet.splitlines() if re.search(r'(?:^|\s)(?:[^ ]*/)?(?:go|compile|link|[^ /]+\.test)(?:\s|$)',l) and 'measurement-driver' not in l]
   if foreign:raise SystemExit('Go producer prevents quiet measurement: '+str(foreign))
   for version in order:
    label=f'{size}-{shape}-pair{pair}-{version}';en=dict(env,SPRINT_POPULATED_SIZE=size)
    argv=['taskset','-c','0-3',str(e/(version+'-mvcc.test')),'-test.run=^$','-test.bench=^BenchmarkSprintMVCC$/^command_wal_relaxed$/^cow_btree$/^'+shape+'$/^point$','-test.benchtime=64x','-test.count=1','-test.benchmem','-test.v','-test.timeout=120s'];out,r=run(label,argv,en)
    matches=[l for l in out.splitlines() if l.startswith('BenchmarkSprintMVCC/') and re.search(r'\s64\s',l)]
    if len(matches)!=1:raise SystemExit('missing unique 64x row '+label)
    tokens=matches[0].split();idx=tokens.index('64');metrics={tokens[i+1]:float(tokens[i]) for i in range(idx+1,len(tokens)-1,2)}
    for k in ['ack_routing_before_ok','ack_routing_after_ok','close_ok','point_calls/op','cow_capture_calls_total/op','cow_publications_total/op']:
     if metrics.get(k)!=1:raise SystemExit('counter '+k+' '+str(metrics.get(k)))
    rows.append(dict(size=size,shape=shape,pair=pair,version=version,metrics=metrics,raw_sha256=r['sha256'],elapsed_s=r['elapsed_s']));(e/'measurement-rows.json').write_text(json.dumps(rows,indent=2)+'\n')
summary=[]
for size in ['small','multilevel']:
 for shape in ['forced_pointer']:
  d=dict(size=size,shape=shape)
  for v in ['before','after']:
   rr=[r for r in rows if r['size']==size and r['shape']==shape and r['version']==v];d[v]={k:statistics.median([r['metrics'][k] for r in rr]) for k in ['ns/op','B/op','allocs/op','point_p50_ns','point_p99_ns','writer_p50_ns','writer_p99_ns','loop_elapsed_ns','loop_residual_ns','writer_sum_ns','point_sum_ns','writer_mean_ns/op','point_mean_ns/op','writer_first_ns','point_first_ns','writer_max_ns','point_max_ns','writer_max_index','point_max_index']};d[v]['samples']=len(rr)
  summary.append(d)
(e/'measurement-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
