from pathlib import Path
import json,subprocess,hashlib,os,time,signal,re,statistics,sys
b=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013"); o=b/"ordinary-5130-allocation-integration-20261015"; e=b/"evidence/ordinary-shared-custody-integration-20261015/guard"; gr=b/"toolchains/go1.26.3-from-185-20261014"
env=os.environ.copy(); env.update(GOROOT=str(gr),GOTOOLCHAIN="local",GOWORK="off",GOPROXY="off",GOSUMDB="off",CGO_ENABLED="1",GOMAXPROCS="1")
assert not {k:v for k,v in env.items() if k.startswith(("TREEDB","GOMAP","GOGC","GOMEMLIMIT","GODEBUG"))}
def dump(n,x): (e/n).write_text(json.dumps(x,indent=2)+"\n")
def census():
 rows=[]
 for p in sorted(o.rglob("*")):
  if p.is_file() and (p.suffix==".go" or p.name in ["go.mod","go.sum"]) and ".git" not in p.parts and "artifacts" not in p.relative_to(o).parts:
   d=p.read_bytes(); rows.append(dict(path=str(p.relative_to(o)),sha256=hashlib.sha256(d).hexdigest(),bytes=len(d)))
 return rows
def state():
 index=Path(subprocess.check_output(["git","rev-parse","--git-path","index"],cwd=o,text=True).strip())
 if not index.is_absolute(): index=o/index
 return dict(head=subprocess.check_output(["git","rev-parse","HEAD"],cwd=o,text=True).strip(),index_sha256=hashlib.sha256(index.read_bytes()).hexdigest(),staged=subprocess.check_output(["git","diff","--cached","--name-only"],cwd=o,text=True))
def run(name,args,timeout):
 dump(name+".command.json",dict(argv=args,cwd=str(o),env={k:env[k] for k in ["GOROOT","GOTOOLCHAIN","GOWORK","GOPROXY","GOSUMDB","CGO_ENABLED","GOMAXPROCS"]},outer_timeout=timeout))
 start=time.monotonic(); p=subprocess.Popen(args,cwd=o,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True); timed=False
 try: raw=p.communicate(timeout=timeout)[0]
 except subprocess.TimeoutExpired:
  timed=True; os.killpg(p.pid,signal.SIGKILL); raw=p.communicate()[0]
 (e/(name+".raw")).write_bytes(raw); rec=dict(exit=p.returncode,timeout=timed,elapsed_s=time.monotonic()-start,raw_sha256=hashlib.sha256(raw).hexdigest()); dump(name+".receipt.json",rec); print(name,json.dumps(rec),flush=True)
 if p.returncode or timed: print(raw.decode(errors="replace")[-5000:],flush=True); raise SystemExit(1)
 return raw

ov=json.loads((e/"baseline-overlay.json").read_text()); fixture=str(o/"TreeDB/mvcc/primary_generation_routing_v6_test.go"); del ov["Replace"][fixture]; dump("baseline-primary-overlay.json",ov)
dump("primary-fixture-binding.json",dict(path=fixture,sha256=hashlib.sha256(Path(fixture).read_bytes()).hexdigest(),baseline="same test fixture only over exact2a production",candidate="same test fixture over frozen40"))
for role in ["baseline","candidate"]:
 args=[str(gr/"bin/go"),"test","-c","-tags=treedb_test","-trimpath","-buildvcs=false","-o",str(e/(role+"-primary.test"))]
 if role=="baseline": args+=["-overlay",str(e/"baseline-primary-overlay.json")]
 args+=["./TreeDB/mvcc"]; run("build-primary-"+role,args,120)
dump("primary-binaries.json",[dict(role=r,path=str(e/(r+"-primary.test")),sha256=hashlib.sha256((e/(r+"-primary.test")).read_bytes()).hexdigest(),bytes=(e/(r+"-primary.test")).stat().st_size) for r in ["baseline","candidate"]])
assert json.loads((e/"source-before.json").read_text())==census() and json.loads((e/"state-before.json").read_text())==state()
run("primary-public-lifecycle",["taskset","-c","0",str(e/"candidate-primary.test"),"-test.run=^TestCommitAtPrimaryOnlyRetainsActualDataGeneration$","-test.count=1","-test.timeout=90s","-test.v"],100)
print("PRIMARY_BUILD_AND_LIFECYCLE_PASS",flush=True)
