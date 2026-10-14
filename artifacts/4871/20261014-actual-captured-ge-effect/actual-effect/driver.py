from pathlib import Path
import os,json,hashlib,subprocess,time,re,statistics,shutil,platform
B=Path("/home/mikers/benchmarks/mvcc-4871-repair-20261013");A=B/"ordinary-5111";F=B/"successor-4876-pointer-tail-20261014";P=B/"evidence/successor-4876-pointer-tail-20261014";E=B/"evidence/successor-4876-actual-effect-20261014";M=E/"harness";GO=B/"toolchains/go1.26.3-from-185-20261014/bin/go";I=B/"evidence/ordinary-5111/captured-ge-production-integration-20261014"
ENV=dict(os.environ,GOROOT=str(GO.parent.parent),GOWORK="off",GOTOOLCHAIN="local",GOMAXPROCS="4",CGO_ENABLED="1",SPRINT_MEMTABLE="cow_btree",SPRINT_REQUIRE_PRIMARY="1"); GE=[];receipts=[];rows=[]
def sha(f):return hashlib.sha256(f.read_bytes()).hexdigest()
def put(n,d):(E/n).write_text(json.dumps(d,indent=2)+"\n")
def git(*args):return subprocess.check_output(["git","-C",str(A),*args],env=dict(os.environ,GIT_OPTIONAL_LOCKS="0"))
def census():
 files={}
 for n in sorted(set(git("ls-files","-c","-o","--exclude-standard","-z").decode().split("\0"))-{ "" }):
  p=A/n
  if p.is_symlink():files[n]={"symlink":os.readlink(p)}
  elif p.is_file():files[n]={"bytes":p.stat().st_size,"sha256":sha(p)}
  else:files[n]={"missing":True}
 index=Path(git("rev-parse","--git-path","index").decode().strip()); index=index if index.is_absolute() else A/index
 return dict(head=git("rev-parse","HEAD").decode().strip(),status=git("status","--porcelain=v1","--untracked-files=all").decode(),index_sha256=sha(index),files=files)
def quiet(label):
 ps=subprocess.check_output(["ps","-eo","pid,comm,args"],text=True);(E/(label+".processes.raw")).write_text(ps);bad=[]
 for l in ps.splitlines()[1:]:
  t=l.split(None,2)
  if len(t)>1 and (t[1] in {"go","compile","link"} or t[1].endswith(".test")):bad.append(l)
 if bad:raise RuntimeError("competing Go producer: "+str(bad))
 return ps

def run(label,argv,env=ENV):
 quiet("pre-"+label);start=time.time();r=subprocess.run(argv,cwd=M,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT);elapsed=time.time()-start;f=E/(label+".raw");f.write_bytes(r.stdout)
 rec=dict(label=label,argv=argv,cwd=str(M),env={k:env[k] for k in ["GOROOT","GOWORK","GOTOOLCHAIN","GOMAXPROCS","CGO_ENABLED","SPRINT_MEMTABLE","SPRINT_REQUIRE_PRIMARY","SPRINT_POPULATED_SIZE"] if k in env},exit=r.returncode,elapsed_s=elapsed,raw=f.name,sha256=sha(f));receipts.append(rec);put("command-receipts.json",receipts);print(label,r.returncode,round(elapsed,3),flush=True)
 if r.returncode:raise RuntimeError("first command failure: "+label)
 return r.stdout.decode(),rec

quiet("initial");c=census();put("actual-source-before.json",c)
assert c["head"]=="9bd10594dc87e0ccc9f4d1c59ce6aabbf5e69e85"
owner=json.loads((I/"ordinary-5111.final-source.json").read_text());assert c["files"]==owner["files"],"actual source differs from qualified owner receipt"
assert c["index_sha256"]==owner["index_sha256"] and c["status"]==owner["status"]
assert sha(I/"receipt.json")=="fabdf4e0e33c8f4f1c94fb75edeca13818156ff5fd672e52de6be591672c0478"
shutil.copyfile(I/"receipt.json",E/"integration-receipt.json");shutil.copyfile(I/"manifest.json",E/"integration-manifest.json")
M.mkdir();expected={"benchmark_test.go":"8b0efd309dd0c660d2b31fb6f8f81e3047b425e403511097677d1d98c1b46872","admission_test.go":"2978839f31fd5e5f511c4375ffc1109745ad5344d80bdaccdbdaa003f9b356ad","captured_successor_test.go":"00e82e5b073629475481f07eb738f1873f3f7a069f5cea2e9d6041814aa633b6"}
for n,h in expected.items():
 f=F/"TreeDB/mvcc/sprintperf"/n;assert sha(f)==h;shutil.copyfile(f,M/n)
assert sha(A/"go.mod")==sha(F/"go.mod") and sha(A/"go.sum")==sha(F/"go.sum"),"module graph changed since frozen harness"
mod=(A/"go.mod").read_text().replace("module github.com/snissn/gomap\n","module github.com/snissn/gomap/TreeDB/sprintperf_actual_effect\n",1).replace("replace github.com/hashicorp/raft => ./third_party/hashicorp_raft_v1.7.3","replace github.com/hashicorp/raft => "+str(A/"third_party/hashicorp_raft_v1.7.3"))
mod+="\nrequire github.com/snissn/gomap v0.0.0\nreplace github.com/snissn/gomap => "+str(A)+"\n";(M/"go.mod").write_text(mod);shutil.copyfile(A/"go.sum",M/"go.sum")
overlay={}
for old,new in json.loads((P/"baseline-overlay.json").read_text())["Replace"].items():
 rel=str(Path(old).relative_to(F));overlay[str(A/rel)]=new;GE.append(dict(path=rel,before_path=new,before_sha256=sha(Path(new)),after_sha256=sha(A/rel)))
assert len(GE)==6 and all(x["before_sha256"]!=x["after_sha256"] for x in GE)
put("before-overlay.json",{"Replace":overlay});put("effective-production-delta.json",GE)
put("route-identities.json",dict(harness=expected,go_binary_sha256=sha(GO),actual_module_sha256=sha(A/"go.mod"),actual_sum_sha256=sha(A/"go.sum"),temporary_module_sha256=sha(M/"go.mod"),raft_replacement=str(A/"third_party/hashicorp_raft_v1.7.3"),source_files=len(c["files"]),effective_production_delta_count=6))
# Resolve exact compiled dependency packages and source files under the same variant overlay.
for version in ["before","after"]:
 ov=["-overlay="+str(E/"before-overlay.json")] if version=="before" else []
 out,_=run(version+"-dependencies",[str(GO),"list","-mod=readonly",*ov,"-test","-deps","-json","."])
 dec=json.JSONDecoder();rest=out;pkgs=[]
 while rest.strip():
  obj,n=dec.raw_decode(rest.lstrip());pkgs.append(obj);rest=rest.lstrip()[n:]
 modules={x["Module"]["Path"]:x["Module"] for x in pkgs if x.get("Module")}
 assert modules["github.com/snissn/gomap"]["Replace"]["Dir"]==str(A)
 assert modules["github.com/snissn/compress"]["Version"]=="v1.18.2-snissn.0.0.20261014112447-88a027b09341"
 put(version+"-compiled-modules.json",modules)
 run(version+"-build",[str(GO),"test","-mod=readonly",*ov,"-c","-trimpath","-buildvcs=false","-o",str(E/(version+".test")),"."])
binaries={v:dict(path=str(E/(v+".test")),bytes=(E/(v+".test")).stat().st_size,sha256=sha(E/(v+".test"))) for v in ["before","after"]};put("binary-identities.json",binaries)
assert census()==c,"actual source/index/status drift before timing"
for size in ["small","multilevel"]:
 for shape in ["inline","forced_pointer"]:
  for pair,order in [(1,["before","after"]),(2,["after","before"])]:
   for version in order:
    label=f"{size}-{shape}-pair{pair}-{version}"
    argv=["taskset","-c","0-3",str(E/(version+".test")),"-test.run=^$","-test.bench=^BenchmarkSprintMVCC$/^command_wal_relaxed$/^cow_btree$/^"+shape+"$/^point$","-test.benchtime=64x","-test.count=1","-test.benchmem","-test.v","-test.timeout=120s"]
    out,rec=run(label,argv,dict(ENV,SPRINT_POPULATED_SIZE=size))
    matches=[l for l in out.splitlines() if l.startswith("BenchmarkSprintMVCC/") and re.search(r"\s64\s",l)]
    assert len(matches)==1,"missing unique 64x result"
    t=matches[0].split();i=t.index("64");m={t[j+1]:float(t[j]) for j in range(i+1,len(t)-1,2)}
    for k in ["ack_routing_before_ok","ack_routing_after_ok","close_ok","point_calls/op","output/op","cow_capture_calls_total/op","cow_prepare_calls_total/op","cow_publications_total/op","wal_appends/op"]:assert m.get(k)==1,(label,k,m.get(k))
    for k in ["wal_syncs/op","vlog_syncs/op","public_checkpoints/op","snapshot_rotations/op","rotated_shards/op","enqueued_records/op"]:assert m.get(k)==0,(label,k,m.get(k))
    expect=4 if size=="small" else 24582
    for phase in ["before","after"]:
     assert m["layout_"+phase+("_pointer" if shape=="forced_pointer" else "_inline")]==expect
     assert m["layout_"+phase+("_inline" if shape=="forced_pointer" else "_pointer")]==0
    assert "root_type=0xb" in out and "mutable_bytes=0 queue_len=0 read_timestamp=15 expected_timestamp=10" in out
    if size=="multilevel":assert "proven_cross_leaf_key=" in out and re.search(r"populated_depth=[3-9]",out)
    assert sha(E/(version+".test"))==binaries[version]["sha256"]
    rows.append(dict(size=size,shape=shape,pair=pair,version=version,metrics=m,raw_sha256=rec["sha256"],elapsed_s=rec["elapsed_s"]));put("measurement-rows.json",rows)
assert len(rows)==16
final=census();put("actual-source-after.json",final);assert final==c,"actual source/index/status drift"
quiet("final");put("quiescence.json",dict(actual_source_unchanged=True,head=c["head"],index_sha256=c["index_sha256"],file_count=len(c["files"]),go_quiet=True,binaries_unchanged={v:sha(Path(d["path"]))==d["sha256"] for v,d in binaries.items()},disposable_release_list=[]))
summary=[]
for size in ["small","multilevel"]:
 for shape in ["inline","forced_pointer"]:
  d=dict(size=size,shape=shape)
  for v in ["before","after"]:
   rr=[r for r in rows if r["size"]==size and r["shape"]==shape and r["version"]==v]
   d[v]={k:statistics.median([r["metrics"][k] for r in rr]) for k in rr[0]["metrics"]};d[v]["samples"]=len(rr)
  d["change_pct"]={k:(d["after"][k]/d["before"][k]-1)*100 for k in ["ns/op","B/op","allocs/op","point_p50_ns","point_mean_ns/op","writer_p50_ns","writer_mean_ns/op"] if d["before"][k]}
  summary.append(d)
put("measurement-summary.json",summary)
print("COMPLETE",json.dumps([dict(size=x["size"],shape=x["shape"],before_ns=x["before"]["ns/op"],after_ns=x["after"]["ns/op"],change_pct=x["change_pct"]) for x in summary]),flush=True)
