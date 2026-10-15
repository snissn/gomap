import pathlib,subprocess,os,json,hashlib,time,signal,sys
B=pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');W=B/'ordinary-5111-final-main-20261015';E=pathlib.Path(__file__).parent;G=B/'toolchains/go1.26.3-from-185-20261014'
env=dict(os.environ,GOROOT=str(G),GOWORK='off',GOTOOLCHAIN='local',GOPROXY='off',GOSUMDB='off',CGO_ENABLED='1',GOMAXPROCS='4',PATH=str(G/'bin')+':'+os.environ['PATH'])
label,package,selector=sys.argv[1:4]
subprocess.run([str(G/'bin/gofmt'),'-w','TreeDB/db/vacuum_collection_publication_guard_test.go'],cwd=W,env=env,check=True)
def census():
 return {str(p.relative_to(W)):hashlib.sha256(p.read_bytes()).hexdigest() for p in W.rglob('*') if p.is_file() and (p.suffix=='.go' or p.name in ['go.mod','go.sum']) and '.git' not in p.parts}
before=census(); (E/(label+'-before.json')).write_text(json.dumps(before,sort_keys=True,indent=2))
cmd=[str(G/'bin/go'),'test','-p=1',package,'-run',selector,'-count=1','-timeout=90s']
(E/(label+'-command.json')).write_text(json.dumps(cmd,indent=2));t=time.monotonic()
with (E/(label+'.raw')).open('w') as out:
 proc=subprocess.Popen(cmd,cwd=W,env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
 try: rc=proc.wait(timeout=120); timedout=False
 except subprocess.TimeoutExpired: os.killpg(proc.pid,signal.SIGKILL);rc=proc.wait();timedout=True
after=census();(E/(label+'-after.json')).write_text(json.dumps(after,sort_keys=True,indent=2))
r=dict(exit=rc,timeout=timedout,seconds=time.monotonic()-t,source_unchanged=before==after,rows=len(before));(E/(label+'-receipt.json')).write_text(json.dumps(r,indent=2));print(json.dumps(r));print((E/(label+'.raw')).read_text()[-20000:])
sys.exit(0 if rc==0 and before==after else 1)
