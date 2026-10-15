from pathlib import Path
import hashlib,json,os,re,statistics,subprocess,time
root=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');src=root/'allocation-main-20261015';out=root/'evidence/allocation-main-5130-20261015';tool=root/'toolchains/go1.26.3-from-185-20261014';go=str(tool/'bin/go');env=dict(os.environ,GOROOT=str(tool),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1');commands=[]
def sha(data):return hashlib.sha256(data).hexdigest()
def census():
 names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=src).decode().split('\0')
 return {p:dict(bytes=(src/p).stat().st_size,sha256=sha((src/p).read_bytes())) for p in sorted(set(names)) if p and (p.endswith('.go') or p in ['go.mod','go.sum'])}
def run(name,argv,timeout=120):
 log=out/(name+'.raw');at=time.monotonic()
 with log.open('wb') as f:q=subprocess.run(argv,cwd=src,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
 row=dict(name=name,argv=argv,cwd=str(src),env={k:env[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']},exit=q.returncode,wall_seconds=time.monotonic()-at,log_sha256=sha(log.read_bytes()));commands.append(row);(out/'public-commands.json').write_text(json.dumps(commands,indent=2)+'\n');print(json.dumps(row),flush=True)
 if q.returncode:raise SystemExit(log.read_text())
 return log.read_text()
before=census();(out/'source-before.json').write_text(json.dumps(before,indent=2)+'\n')
for name,args in [('candidate',[]),('baseline',['-overlay='+str(out/'main-baseline-overlay.json')])]:run('build-'+name,[go,'test','-c','-trimpath','-buildvcs=false',*args,'-o',str(out/(name+'-db.test')),'./TreeDB/db'])
(out/'binaries.json').write_text(json.dumps({name:dict(path=str(out/(name+'-db.test')),bytes=(out/(name+'-db.test')).stat().st_size,sha256=sha((out/(name+'-db.test')).read_bytes())) for name in ['baseline','candidate']},indent=2)+'\n')
assert census()==before
bench=['-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s','-test.benchtime=1000x'];rows=[];print('TIMED_PAIRS_START',flush=True)
for pair in range(3):
 order=['baseline','candidate'] if pair%2==0 else ['candidate','baseline']
 for name in order:
  raw=run(f'pair{pair+1}-{name}',['taskset','-c','0',str(out/(name+'-db.test')),*bench],timeout=70);m=re.search(r'^BenchmarkConditionalTxnBaselineBatchWrite\s+(\d+)\s+([0-9.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',raw,re.M);assert m,raw;rows.append(dict(pair=pair+1,kind=name,n=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4])))
(out/'rows.json').write_text(json.dumps(rows,indent=2)+'\n');med={name:{k:statistics.median(v[k] for v in rows if v['kind']==name) for k in ['ns_per_op','bytes_per_op','allocs_per_op']} for name in ['baseline','candidate']};summary=dict(status='MAIN_PROJECTION_PAIRED_PUBLIC_DIAGNOSTIC',rows=rows,medians=med,percent_change={k:100*(med['candidate'][k]/med['baseline'][k]-1) for k in med['baseline']},limitations=['Three fixed1000x short fresh-process paired runs; GOMAXPROCS1 CPU0.','Same unchanged main10000key seed fixture, checkpoint outside timer every8writes.','No claim of whole-main acceptance or native-path performance.']);(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');after=census();assert before==after;(out/'source-after.json').write_text(json.dumps(after,indent=2)+'\n');print('SUMMARY '+json.dumps(summary),flush=True)
