from pathlib import Path
import hashlib,json,subprocess,os,time,datetime
B=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013")
W=B/"native-4878-traversal"
E=B/"evidence/native-production-cost-20261015/attribution-v2"
E.mkdir(exist_ok=False)
T=B/"toolchains/go1.26.3-from-185-20261014"
ENV={**os.environ,"GOROOT":str(T),"GOTOOLCHAIN":"local","GOWORK":"off","CGO_ENABLED":"1","GOMAXPROCS":"4"}
def dump(n,x): (E/n).write_text(json.dumps(x,indent=2)+"\n")
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def run(n,args,timeout=120):
 started=datetime.datetime.now(datetime.timezone.utc).isoformat(); t=time.monotonic()
 with (E/(n+".raw")).open("wb") as f:
  try:
   p=subprocess.run(args,cwd=W,env=ENV,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
   r={"exit_code":p.returncode,"timeout":False}
  except subprocess.TimeoutExpired:r={"exit_code":None,"timeout":True}
 r.update(argv=args,cwd=str(W),elapsed_seconds=time.monotonic()-t,started_utc=started,raw_sha256=sha(E/(n+".raw")))
 dump(n+".receipt.json",r); return r
def quiet(n):
 p=subprocess.run(["ps","-eo","pid,ppid,stat,pcpu,pmem,comm,args"],capture_output=True,text=True,check=True)
 (E/(n+".processes.txt")).write_text(p.stdout)
 competing=[]
 for l in p.stdout.splitlines()[1:]:
  a=l.split(None,6)
  if len(a)<6:continue
  c=a[5]
  if c in ("go","compile","link","gofmt") or c.endswith(".test"):competing.append(l)
 if competing:raise RuntimeError("competing Go producers: "+repr(competing))
def metadata():
 g=Path((W/".git").read_text().strip().split(": ",1)[1]);g=g if g.is_absolute() else W/g
 common=(g/"commondir").read_text().strip() if (g/"commondir").exists() else "."
 c=(g/common).resolve()
 return {"gitdir":str(g),"HEAD":(g/"HEAD").read_text(),"index_sha256":sha(g/"index"),"packed_refs_sha256":sha(c/"packed-refs") if (c/"packed-refs").exists() else None}
quiet("preflight")
dump("environment.json",{k:ENV.get(k) for k in ["GOROOT","GOTOOLCHAIN","GOWORK","CGO_ENABLED","GOMAXPROCS","GOGC","GOMEMLIMIT","GODEBUG","GOFLAGS","GOCACHE","GOMODCACHE","GOENV","GOEXPERIMENT"]})
run("toolchain",[str(T/"bin/go"),"version"],30)
run("affinity",["taskset","-pc",str(os.getpid())],30)
run("hardware",["lscpu"],30)
# Format only the leased new test, before all frozen source captures.
r=run("format",[str(T/"bin/gofmt"),"-w",str(W/"TreeDB/mvcc/native_production_cost_v6_test.go")],30)
if r["exit_code"]!=0:raise RuntimeError("format failed")
# Exact effective compile inputs including standard library and local replacements.
r=run("dependencies",[str(T/"bin/go"),"list","-deps","-test","-json","-tags=treedb_test","./TreeDB/mvcc"])
if r["exit_code"]!=0:raise RuntimeError("dependency resolution failed")
raw=(E/"dependencies.raw").read_text(); dec=json.JSONDecoder(); at=0; packages=[]
while at<len(raw):
 while at<len(raw) and raw[at].isspace():at+=1
 if at>=len(raw):break
 x,at=dec.raw_decode(raw,at);packages.append(x)
paths=set()
for x in packages:
 root=Path(x["Dir"])
 for k in ["GoFiles","CgoFiles","CFiles","CXXFiles","MFiles","HFiles","FFiles","SFiles","SwigFiles","SwigCXXFiles","SysoFiles","EmbedFiles","TestGoFiles","XTestGoFiles","TestEmbedFiles","XTestEmbedFiles"]:
  for n in x.get(k,[]):
   p=Path(n);paths.add(str(p if p.is_absolute() else root/p))
 for mod in [x.get("Module",{}),x.get("Module",{}).get("Replace",{})]:
  if mod.get("GoMod"):paths.add(mod["GoMod"])
for n in ["go.mod","go.sum"]:paths.add(str(W/n))
paths=sorted(p for p in paths if Path(p).is_file())
def census():return [{"path":p,"bytes":Path(p).stat().st_size,"sha256":sha(p)} for p in paths]
before=census();bs=metadata();dump("source-before.json",before);dump("state-before.json",bs)
(E/"sources/TreeDB/mvcc").mkdir(parents=True)
for n in ["native_production_cost_v6_test.go","native_unbounded_diagnostic_v6_test.go"]:
 (E/"sources/TreeDB/mvcc"/n).write_bytes((W/"TreeDB/mvcc"/n).read_bytes())
binary=E/"native-cost.test"
r=run("compile",[str(T/"bin/go"),"test","-c","-tags=treedb_test","-o",str(binary),"./TreeDB/mvcc"])
if r["exit_code"]!=0:raise RuntimeError("compile failed")
dump("binary.json",{"path":str(binary),"bytes":binary.stat().st_size,"sha256":sha(binary)})
run("binary-buildinfo",[str(T/"bin/go"),"version","-m",str(binary)],30)
cells=[("diagnostic-attribution","TestNativePruneCOWRetainedOutputDiagnosticAllocationAttribution")]
results=[]
for name,test in cells:
 quiet(name+"-preflight")
 ENV["NATIVE_COST_PROFILE_PATH"]=str(E/"diagnostic-allocation-records.json")
 argv=["taskset","-c","0-3",str(T/"bin/go"),"tool","test2json","-t","-p","github.com/snissn/gomap/TreeDB/mvcc",str(binary),"-test.v=test2json","-test.count=1","-test.timeout=90s","-test.run=^"+test+"$/^pointertrue$"]
 r=run(name,argv)
 results.append({"cell":name,**r})
 raw=(E/(name+".raw")).read_text()
 if r["timeout"] or "NATIVE_COST_V6 " not in raw or "NATIVE_COST_CLOSE_REOPEN_CHECKS_COMPLETE" not in raw:break
 if name=="diagnostic-attribution" and r["exit_code"]!=0:break
after=census();ass=metadata();dump("source-after.json",after);dump("state-after.json",ass)
quiet("postflight")
dump("receipt.json",{"results":results,"source_rows":len(before),"source_unchanged":before==after,"state_unchanged":bs==ass,"binary_sha256_after":sha(binary),"binary_retained":True,"no_production_acceptance":True})
assert before==after and bs==ass
print(json.dumps({"results":[{"cell":x["cell"],"exit_code":x["exit_code"]} for x in results],"source_rows":len(before),"source_unchanged":True,"state_unchanged":True,"out":str(E)}))
