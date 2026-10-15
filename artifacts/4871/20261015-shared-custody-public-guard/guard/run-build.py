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
assert not (e/"build-baseline.command.json").exists()
before=census(); st=state(); dump("source-before.json",before); dump("state-before.json",st)
for row in json.loads((e/"candidate-40.json").read_text()): assert hashlib.sha256((o/row["path"]).read_bytes()).hexdigest()==row["sha256"]
for role in ["baseline","candidate"]:
 args=[str(gr/"bin/go"),"test","-c","-trimpath","-buildvcs=false","-o",str(e/(role+".test"))]
 if role=="baseline": args+=["-overlay",str(e/"baseline-overlay.json")]
 args+=["./TreeDB/db"]; run("build-"+role,args,120)
bins=[dict(role=r,path=str(e/(r+".test")),sha256=hashlib.sha256((e/(r+".test")).read_bytes()).hexdigest(),bytes=(e/(r+".test")).stat().st_size) for r in ["baseline","candidate"]]; dump("binaries.json",bins)
assert before==census() and st==state()
dump("build-source-after.json",census())
print("BUILD_PASS",flush=True)
