import sys,json,hashlib,subprocess,os,datetime,time
from pathlib import Path
wt=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013/ordinary-5111")
ev=wt.parent/"evidence/ordinary-5111/vacuum-outer-leaf-completion-20261014"
sp=ev/"source-offline-combined-after-shared-v1.json"; source=json.loads(sp.read_text())
plan=json.loads((ev/"offline-combined-after-shared-correction-plan-v1.json").read_text())
def same(): return all((wt/r["path"]).is_file() and hashlib.sha256((wt/r["path"]).read_bytes()).hexdigest()==r["sha256"] for r in source["rows"])
for mode in ["normal","race"]:
 assert same()
 name="candidate-offline-combined-"+mode+"-v1"
 raw=ev/(name+".jsonl");cp=ev/(name+".command.json"); assert not raw.exists()
 argv=plan[mode+"_argv"]
 env=os.environ.copy();env.update({"PATH":"/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin:"+env["PATH"],"GOWORK":"off","GOMAXPROCS":"2","TMPDIR":str(wt.parent/"scratch/ordinary-5111")})
 if mode=="race":env["GOMEMLIMIT"]="4GiB"
 receipt={"argv":argv,"cwd":str(wt),"env":{k:env.get(k) for k in ["PATH","GOWORK","GOMAXPROCS","GOMEMLIMIT","TMPDIR"]},"at_start_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"source_sha256":hashlib.sha256(sp.read_bytes()).hexdigest(),"status":"RUNNING"}
 cp.write_text(json.dumps(receipt,indent=2)+"\n");start=time.monotonic()
 with raw.open("wb") as f:result=subprocess.run(argv,cwd=wt,env=env,stdout=f,stderr=subprocess.STDOUT)
 (ev/(name+".exit")).write_text(str(result.returncode)+"\n")
 receipt.update({"exit":result.returncode,"elapsed_seconds":time.monotonic()-start,"at_end_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"raw_sha256":hashlib.sha256(raw.read_bytes()).hexdigest(),"raw_bytes":raw.stat().st_size,"status":"COMPLETE","source_unchanged":same()})
 cp.write_text(json.dumps(receipt,indent=2)+"\n")
 failed=[];counts={};top=[]
 for line in raw.read_text(errors="replace").splitlines():
  try:d=json.loads(line)
  except ValueError: failed.append(line);continue
  a=d.get("Action");counts[a]=counts.get(a,0)+1
  if a=="run" and "/" not in d.get("Test",""):top.append(d.get("Test"))
  if a=="fail" or (a=="output" and any(x in d.get("Output","") for x in ["panic:","FAIL","DATA RACE"])):failed.append(d)
 print(json.dumps({"receipt":receipt,"counts":counts,"literal_top_tests":top,"failures":failed[-40:]},indent=2),flush=True)
 if result.returncode or not receipt["source_unchanged"]:sys.exit(result.returncode or 99)
