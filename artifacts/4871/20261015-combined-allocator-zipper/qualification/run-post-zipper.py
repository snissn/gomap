#!/usr/bin/env python3
"""Source-only prepared runner. Execute only after root grants serialized runtime."""
from pathlib import Path
import argparse,datetime,hashlib,json,os,re,statistics,subprocess,time
ROOT=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
SRC=ROOT/'allocation-main-20261015'
TOOL=ROOT/'toolchains/go1.26.3-from-185-20261014'
GO=str(TOOL/'bin/go')
HERE=Path(__file__).resolve().parent
OWNED=json.loads((HERE/'allocator-go-paths.json').read_text())
DOCS=['TreeDB/docs/spec/recovery.md','TreeDB/docs/spec/storage-format.md']
FIXTURE='TreeDB/db/conditional_kv_contract_bench_test.go'
ENV=dict(os.environ,GOROOT=str(TOOL),GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1',GOMAXPROCS='1')
def sha(data):return hashlib.sha256(data).hexdigest()
def git(*args):return subprocess.check_output(['git',*args],cwd=SRC)
def census():
 names=git('ls-files','--cached','--others','--exclude-standard','-z').decode().split('\0')
 return {p:dict(bytes=(SRC/p).stat().st_size,sha256=sha((SRC/p).read_bytes())) for p in sorted(set(names)) if p and (p.endswith('.go') or Path(p).name in ['go.mod','go.sum','go.work','go.work.sum'])}
def main():
 ap=argparse.ArgumentParser(description=__doc__)
 ap.add_argument('--candidate-head',required=True,help='Root-approved exact merged allocator HEAD (40 hex)')
 ap.add_argument('--baseline',required=True,help='Exact final-main commit containing zipper (40 hex)')
 ap.add_argument('--out',required=True,help='NEW evidence directory outside worktree')
 ap.add_argument('--execute',action='store_true',help='Required only after root runtime grant; otherwise read-only preflight')
 a=ap.parse_args()
 for value in [a.candidate_head,a.baseline]:assert re.fullmatch('[0-9a-f]{40}',value),value
 assert git('rev-parse','HEAD').decode().strip()==a.candidate_head,'HEAD mismatch'
 assert not git('status','--porcelain'),'worktree must be clean'
 assert git('rev-parse',a.baseline+'^{commit}').decode().strip()==a.baseline
 subprocess.run(['git','merge-base','--is-ancestor',a.baseline,a.candidate_head],cwd=SRC,check=True)
 changed=git('diff','--name-only',a.baseline,a.candidate_head).decode().splitlines()
 assert changed and set(changed)<=set(OWNED+DOCS),('unowned changes relative to final main',changed)
 fixture=(SRC/FIXTURE).read_bytes();assert fixture==git('show',a.baseline+':'+FIXTURE),'benchmark fixture differs'
 assert b'const count = 10000' in fixture and b'const foregroundGroup = 8' in fixture
 assert b'b.StopTimer()' in fixture and b'd.Checkpoint()' in fixture
 out=Path(a.out).resolve();assert out!=SRC and SRC not in out.parents,'evidence must be outside source'
 assert not out.exists(),'use a NEW evidence directory; protected prior evidence is never overwritten'
 if not a.execute:
  print(json.dumps(dict(status='READ_ONLY_PREFLIGHT_READY_NO_GO',candidate=a.candidate_head,baseline=a.baseline,changed=changed,out=str(out)),indent=2));return
 out.mkdir(parents=True);commands=[]
 def write(name,value):(out/name).write_text(json.dumps(value,indent=2)+'\n')
 def run(name,argv,timeout=120):
  log=out/(name+'.raw');at=time.monotonic()
  with log.open('wb') as f:
   try:code=subprocess.run(argv,cwd=SRC,env=ENV,stdout=f,stderr=subprocess.STDOUT,timeout=timeout).returncode
   except subprocess.TimeoutExpired:code=124
  row=dict(name=name,argv=argv,cwd=str(SRC),env={k:ENV[k] for k in ['GOROOT','GOTOOLCHAIN','GOWORK','CGO_ENABLED','GOMAXPROCS']},exit=code,wall_seconds=time.monotonic()-at,log_sha256=sha(log.read_bytes()))
  commands.append(row);write('commands.json',commands);print(json.dumps(row),flush=True)
  if code:raise RuntimeError(name+' failed; exact raw retained at '+str(log))
  return log.read_text()
 started=time.monotonic();before=census();write('source-before.json',before)
 write('qualification-inputs.json',dict(candidate=a.candidate_head,baseline=a.baseline,changed=changed,owned_go=OWNED,unchanged_fixture=dict(path=FIXTURE,sha256=sha(fixture)),runner_sha256=sha(Path(__file__).read_bytes()),pathlist_sha256=sha((HERE/'allocator-go-paths.json').read_bytes())))
 (out/'source-vs-final-main.patch').write_bytes(git('diff','--binary',a.baseline,a.candidate_head))
 overlay_dir=out/'baseline-inputs';replacements={};overlay_rows=[];effective={p:dict(v) for p,v in before.items()}
 for p in OWNED:
  q=subprocess.run(['git','show',a.baseline+':'+p],cwd=SRC,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
  if q.returncode:
   assert p.endswith('_test.go'),'cannot invent missing production baseline input'
   data=('package '+('freelist' if '/freelist/' in p else 'db')+'\n').encode();kind='new-candidate-test-package-stub'
  else:data=q.stdout;kind='exact-final-main-blob'
  dest=overlay_dir/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
  replacements[str(SRC/p)]=str(dest);effective[p]=dict(bytes=len(data),sha256=sha(data))
  overlay_rows.append(dict(path=p,kind=kind,bytes=len(data),sha256=sha(data),overlay_file=str(dest)))
 overlay=out/'baseline-overlay.json';write('baseline-overlay.json',dict(Replace=replacements));write('baseline-input-manifest.json',overlay_rows);write('baseline-effective-all-go-module-inputs.json',effective)
 for p in before:
  if p not in OWNED:assert effective[p]==before[p]
 assert (SRC/'TreeDB/zipper/zipper.go').read_bytes()==git('show',a.baseline+':TreeDB/zipper/zipper.go'),'zipper source must be final main in BOTH binaries'
 run('freelist-full',[GO,'test','./TreeDB/freelist','-count=1','-timeout=60s'],timeout=75)
 run('db-allocator-format-reopen',[GO,'test','./TreeDB/db','-run','TestFreelistPatriciaV2|TestFormatConfig_SaveLoadApply_RoundTrip|TestSaveFormatConfig_CannotClearOrReplacePersistedDurabilityProfile','-count=1','-timeout=60s'],timeout=90)
 assert census()==before,'source changed during checks'
 for name,args in [('candidate',[]),('baseline',['-overlay='+str(overlay)])]:
  run('build-'+name,[GO,'test','-c','-trimpath','-buildvcs=false',*args,'-o',str(out/(name+'-db.test')),'./TreeDB/db'],timeout=120)
 write('binaries.json',{name:dict(path=str(out/(name+'-db.test')),bytes=(out/(name+'-db.test')).stat().st_size,sha256=sha((out/(name+'-db.test')).read_bytes())) for name in ['baseline','candidate']})
 assert census()==before,'source changed during build'
 assert git('rev-parse','HEAD').decode().strip()==a.candidate_head and not git('status','--porcelain')
 args=['-test.run=^$','-test.bench=^BenchmarkConditionalTxnBaselineBatchWrite$','-test.count=1','-test.benchmem','-test.timeout=60s','-test.benchtime=1000x'];rows=[]
 print('TIMED_PAIRS_START_NO_OTHER_HOST_IO',flush=True)
 for pair in range(3):
  for name in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
   raw=run(f'pair{pair+1}-{name}',['taskset','-c','0',str(out/(name+'-db.test')),*args],timeout=70)
   m=re.search(r'^BenchmarkConditionalTxnBaselineBatchWrite\s+(\d+)\s+([0-9.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op',raw,re.M);assert m,raw
   rows.append(dict(pair=pair+1,kind=name,n=int(m[1]),ns_per_op=float(m[2]),bytes_per_op=int(m[3]),allocs_per_op=int(m[4])))
 write('rows.json',rows)
 med={name:{k:statistics.median(v[k] for v in rows if v['kind']==name) for k in ['ns_per_op','bytes_per_op','allocs_per_op']} for name in ['baseline','candidate']}
 summary=dict(status='POST_ZIPPER_MAIN_ALLOCATOR_PUBLIC_QUALIFICATION',baseline_final_main=a.baseline,candidate_head=a.candidate_head,rows=rows,medians=med,percent_change={k:100*(med['candidate'][k]/med['baseline'][k]-1) for k in med['baseline']},limitations=['Three short fixed1000x paired fresh-process CPU0/GOMAXPROCS1 runs; latency directional.','Baseline retains merged zipper and restores only allocator-owned Go inputs to final main.','Same unmodified main10000key/Checkpoint8 public Batch.Write fixture.','No wholeDB/race repetition or allocation profile/flat-encoding-union recapture.','No additive effect is assumed from prior pure-main measurements.'])
 after=census();assert before==after,'source changed during timed measurement';assert not git('status','--porcelain') and git('rev-parse','HEAD').decode().strip()==a.candidate_head
 write('source-after.json',after);write('summary.json',summary);write('receipt.json',dict(status='COMPLETE_RUNTIME_RELEASED',utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),elapsed_seconds=time.monotonic()-started,source_before_after_equal=True))
 print('SUMMARY '+json.dumps(summary),flush=True);print('RUNTIME_RELEASED',flush=True)
if __name__=='__main__':main()
