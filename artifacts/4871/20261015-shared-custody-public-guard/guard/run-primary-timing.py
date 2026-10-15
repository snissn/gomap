from pathlib import Path
import json,subprocess,hashlib,os,time,signal,re,statistics
b=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013"); o=b/"ordinary-5130-allocation-integration-20261015"; e=b/"evidence/ordinary-shared-custody-integration-20261015/guard"
env=os.environ.copy(); env.update(GOROOT=str(b/"toolchains/go1.26.3-from-185-20261014"),GOTOOLCHAIN="local",GOWORK="off",GOPROXY="off",GOSUMDB="off",CGO_ENABLED="1",GOMAXPROCS="1")
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
assert not (e/"primary-timing-started.json").exists()
assert json.loads((e/"source-before.json").read_text())==census()
assert json.loads((e/"state-before.json").read_text())==state()
bins=json.loads((e/"primary-binaries.json").read_text())
for row in bins: assert hashlib.sha256(Path(row["path"]).read_bytes()).hexdigest()==row["sha256"]
proc=subprocess.check_output(["ps","-eo","pid,ppid,stat,pcpu,etimes,args"],text=True); (e/"primary-quiet-preflight.txt").write_text(proc)
active=[]
for p in Path("/proc").iterdir():
 if not p.name.isdigit(): continue
 try:
  exe=os.readlink(p/"exe"); cmd=(p/"cmdline").read_bytes().replace(b"\0",b" ").decode(errors="replace")
 except (OSError,PermissionError): continue
 if Path(exe).name in ["go","compile","link"] or exe.endswith(".test"): active.append(dict(pid=p.name,exe=exe,cmd=cmd))
assert not active, active
plan=[]
for pair,roles in enumerate([["baseline","candidate"],["candidate","baseline"],["baseline","candidate"]],1):
 for role in roles: plan.append(dict(index=len(plan)+1,pair=pair,role=role,argv=["taskset","-c","0",str(e/(role+"-primary.test")),"-test.run=^$","-test.bench=^BenchmarkCommitAtPrimaryOnlyAfterDataGeneration$","-test.count=1","-test.benchmem","-test.timeout=60s","-test.benchtime=128x"]))
dump("primary-timing-plan.json",dict(plan=plan,env={k:env[k] for k in ["GOROOT","GOTOOLCHAIN","GOWORK","GOPROXY","GOSUMDB","CGO_ENABLED","GOMAXPROCS"]}))
dump("primary-timing-started.json",dict(utc=time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),load=Path("/proc/loadavg").read_text()))
samples=[]; deadline=time.monotonic()+60
for row in plan:
 start=time.monotonic(); p=subprocess.Popen(row["argv"],cwd=o,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True); timed=False
 try: raw=p.communicate(timeout=max(.01,deadline-time.monotonic()))[0]
 except subprocess.TimeoutExpired: timed=True; os.killpg(p.pid,signal.SIGKILL); raw=p.communicate()[0]
 (e/("primary-sample-%02d-%s.raw"%(row["index"],row["role"]))).write_bytes(raw)
 rec=dict(**row,exit=p.returncode,timeout=timed,elapsed_s=time.monotonic()-start,raw_sha256=hashlib.sha256(raw).hexdigest())
 m=re.search(rb"^BenchmarkCommitAtPrimaryOnlyAfterDataGeneration(?:-\d+)?\s+(\d+)\s+(.+)$",raw,re.M)
 if m:
  values={unit.decode():float(value) for value,unit in re.findall(rb"([\d.]+)\s+(ns/op|B/op|allocs/op|generation_advances/op)",m[2])}
  rec.update(iterations=int(m[1]),ns_per_op=values["ns/op"],bytes_per_op=values["B/op"],allocs_per_op=values["allocs/op"],generation_advances_per_op=values["generation_advances/op"])
 samples.append(rec); dump("primary-samples.json",samples); print(json.dumps(rec),flush=True)
 assert p.returncode==0 and not timed and m and int(m[1])==128,raw.decode(errors="replace")[-3000:]
after=census(); dump("primary-source-after.json",after); dump("primary-state-after.json",state())
assert after==json.loads((e/"source-before.json").read_text()) and state()==json.loads((e/"state-before.json").read_text())
for row in bins: assert hashlib.sha256(Path(row["path"]).read_bytes()).hexdigest()==row["sha256"]
metrics=["ns_per_op","bytes_per_op","allocs_per_op","generation_advances_per_op"]; med={r:{k:statistics.median(x[k] for x in samples if x["role"]==r) for k in metrics} for r in ["baseline","candidate"]}; delta={k:(100*(med["candidate"][k]/med["baseline"][k]-1) if med["baseline"][k] else None) for k in metrics}; gates=dict(latency=delta["ns_per_op"]<=5,bytes=med["candidate"]["bytes_per_op"]<=med["baseline"]["bytes_per_op"],objects=med["candidate"]["allocs_per_op"]<=med["baseline"]["allocs_per_op"])
result=dict(samples=samples,medians=med,percent_change=delta,gates=gates,pass_guard=all(gates.values()),scope="Short unchanged public PRIMARY-only durable CommitAt post-DATA diagnostic, combined routing and slot custody versus exact2a. Setup/Stats/Close outside timer. Not isolated custody attribution, final qualification or native ACK.")
dump("primary-results.json",result); print(json.dumps(result,indent=2),flush=True)
