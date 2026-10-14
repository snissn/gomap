import hashlib,json,os,subprocess,sys,time
from pathlib import Path
wt=Path(__file__).resolve().parents[3]
folder=Path(__file__).resolve().parent
name=sys.argv[1]
argv=["/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go","test","-p","2","-mod=readonly","-count=1","-timeout=180s","-json"]
if "race" in name: argv += ["-race"]
argv += ["-run",r"^Test(PhysicalClose|DirtyChunkSyncRetainsFlushAndRetriesStableFileBarrier|EnqueueBuiltAppendsBeforeFinalLeaseReleaseAndHardAdmissionWait|CoalescedSupersedeUnionsAllUncoveredObligations|PrimaryFinish)","./TreeDB/pager","./TreeDB/internal/rootpublication"]
env=dict(os.environ)
binding={"GOWORK":"off","GOMAXPROCS":"2","CGO_ENABLED":"1","GOPROXY":"off","GOSUMDB":"off","GOTOOLCHAIN":"local"}
env.update(binding)
def census():
 rows=[]
 for p in sorted((wt/"TreeDB").rglob("*.go")):
  b=p.read_bytes();rows.append({"path":str(p.relative_to(wt)),"sha256":hashlib.sha256(b).hexdigest(),"bytes":len(b)})
 for p in [wt/"go.mod",wt/"go.sum"]:
  b=p.read_bytes();rows.append({"path":p.name,"sha256":hashlib.sha256(b).hexdigest(),"bytes":len(b)})
 return rows
before=census()
(folder/(name+".source-before.json")).write_text(json.dumps(before,indent=2)+"\n")
source=Path(__file__).read_bytes()
receipt={"argv":argv,"env":binding,"cwd":str(wt),"script_sha256":hashlib.sha256(source).hexdigest(),"head":subprocess.check_output(["git","rev-parse","HEAD"],cwd=wt,text=True).strip(),"started":time.time()}
(folder/(name+".command.json")).write_text(json.dumps(receipt,indent=2)+"\n")
with (folder/(name+".jsonl")).open("wb") as raw:
 result=subprocess.run(argv,cwd=wt,env=env,stdout=raw,stderr=subprocess.STDOUT)
after=census()
(folder/(name+".source-after.json")).write_text(json.dumps(after,indent=2)+"\n")
receipt.update(exit=result.returncode,source_unchanged=before==after,finished=time.time(),raw_sha256=hashlib.sha256((folder/(name+".jsonl")).read_bytes()).hexdigest())
(folder/(name+".receipt.json")).write_text(json.dumps(receipt,indent=2)+"\n")
print(json.dumps(receipt))
sys.exit(result.returncode)
