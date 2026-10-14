from pathlib import Path
import subprocess,hashlib,json,os,shutil,time
base=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013/sprint-performance-20261014")
repo=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal")
goRoot=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013/toolchains/go1.26.3-from-185-20261014")
# Pure diagnostic source projections: production/non-test source from exact Git
# objects, same external two-file public harness. Neither active index is used.
commits={"before-sprint":"28aea01e19f1d62743dec5625ec3d22c1acb6703","recent-main":"3f87d74b98e8b1e809d151611be03785f85e2283"}
extensions={".go",".s",".S",".h",".c",".cpp",".inc",".syso"}
for label,sha in commits.items():
 out=base/"sources"/label;out.mkdir(exist_ok=False)
 entries=subprocess.check_output(["git","-C",str(repo),"ls-tree","-rz",sha]).split(b"\0")
 rows=[]
 for e in entries:
  if not e:continue
  meta,name=e.split(b"\t",1);mode,kind,oid=meta.decode().split();name=name.decode()
  if kind!="blob":continue
  p=Path(name)
  selected=(name in {"go.mod","go.sum"} or (p.parts[0] in {"TreeDB","HashDB","internal"} and p.suffix in extensions and not name.endswith("_test.go")) or name=="TreeDB/mvcc/mvcctest/testdata/public_trace_v1.json")
  if not selected:continue
  blob=subprocess.check_output(["git","-C",str(repo),"cat-file","blob",oid])
  target=out/p;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(blob)
  rows.append({"path":name,"blob":oid,"sha256":hashlib.sha256(blob).hexdigest(),"bytes":len(blob)})
 harness=out/"TreeDB"/"mvcc"/"sprintperf";harness.mkdir(exist_ok=False)
 h=[]
 for source in sorted((base/"harness").glob("*.go")):
  shutil.copyfile(source,harness/source.name);h.append({"path":str((harness/source.name).relative_to(out)),"sha256":hashlib.sha256(source.read_bytes()).hexdigest()})
 receipt={"label":label,"commit":sha,"production_files":rows,"harness":h,"production_bytes":sum(x["bytes"] for x in rows),"purpose":"test-only source projection; all compiled local prerequisites must be covered by this exact object inventory"}
 path=base/"evidence"/(label+"-projection.json");path.write_text(json.dumps(receipt,indent=1)+"\n")
 env=dict(os.environ,GOROOT=str(goRoot),GOWORK="off",GOTOOLCHAIN="local",CGO_ENABLED="1")
 commands=[[str(goRoot/"bin/go"),"list","-deps","-f","{{if not .Standard}}{{.Dir}}{{end}}","./TreeDB/mvcc/sprintperf"],[str(goRoot/"bin/go"),"test","-c","-trimpath","-buildvcs=false","-o",str(base/"binaries"/(label+"-mvcc.test")),"./TreeDB/mvcc/sprintperf"]]
 for step,argv in enumerate(commands):
  log=base/"evidence"/(label+"-preflight-"+str(step)+".log")
  started=time.time()
  with log.open("wb") as f:r=subprocess.run(argv,cwd=out,env=env,stdout=f,stderr=subprocess.STDOUT)
  (log.with_suffix(".receipt.json")).write_text(json.dumps({"argv":argv,"env":{"GOROOT":str(goRoot),"GOWORK":"off","GOTOOLCHAIN":"local","CGO_ENABLED":"1"},"exit":r.returncode,"wall_seconds":time.time()-started,"raw_sha256":hashlib.sha256(log.read_bytes()).hexdigest()},indent=1)+"\n")
  print(json.dumps({"label":label,"step":step,"exit":r.returncode,"files":len(rows),"bytes":receipt["production_bytes"]}),flush=True)
  if r.returncode:print(log.read_text()[-4000:],flush=True);raise SystemExit(r.returncode)
