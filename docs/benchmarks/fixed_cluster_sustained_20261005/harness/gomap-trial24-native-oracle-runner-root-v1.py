from source_paths import source_path
"""Inert Trial24 native-prefix transport constructor. No automatic execution/retry.
--prepare writes a local packet and proposed ROOT-ONLY commands.
--capture-file validates a root-retained capture; never promotes oracle authority.
"""
import argparse, ast, base64, gzip, hashlib, importlib.util, io, json, pathlib, re, shlex, sys, tarfile
if not __debug__: raise RuntimeError("ordinary Python required")
sys.dont_write_bytecode=True
HELPER=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-prefix-oracle-prepare-root-v1.py')
HELPER_SHA="1f95ebd3d3c5eccb61c06eeb0cbaf0f38e0e903a14a456033a28f18c164f93e6"
COLLECTOR=source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA="32bfb32812b05e7ddadd45b7042ec06f2b046afc6dba7d995a0d9e23b1216d70"
CONTEXT=source_path('/tmp/gomap-trial17-native-runner-source-context-root-v1')
RUNNER="/home/mikers/gomap-1242-bounded-runner-prepare.py"
TEMPLATE="/home/mikers/gomap-1242-v4-assigned-owner-semantic-red-root-v1"
RUNNER_PINS={"gomap-1242-bounded-runner-prepare.py":"e09a70a724a98292398cc3333c0d20359afd5c195520710feba4ec8f76d428f5","run.sh":"049de2818f6e4140c02936703c2b929e1f94d226e9ba818b3bb95c9e3d31b8c7","inner.sh":"0bd4c035dc654e4776c4c6a69ce6b29e4a339bc540576c181a9ab7690c144c98"}
REMOTE="/home/mikers/gomap-4997-4998-trial24-oracle-root-v1"
SOURCE="__ROOT_FROZEN_SOURCE_ROOT__"
INVENTORY="__ROOT_FROZEN_INVENTORY_PATH__"
INVENTORY_SHA="__ROOT_FROZEN_INVENTORY_SHA__"
INVENTORY_ROWS="__ROOT_FROZEN_INVENTORY_ROWS__"
RESOLVER_SHA='143a0a0bcaac25014793aa03a09878a08ce12b6cd924e1ae0112a62b4974daf0'
HEAD="__ROOT_FROZEN_HEAD__";TREE="__ROOT_FROZEN_TREE__"
INPUT="/tmp/gomap-4997-4998-rf4trial24mixedchangingc1-inputs-root-v1"
ARCHIVE=INPUT+".tar.gz"
TEST="TestTrial24FrozenCanonicalPrefixOraclePreparation"
PACKAGE="github.com/snissn/gomap/cmd/treedb-query-under-write"
SSH=["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10","mikers@192.168.0.111"]
def need(ok,why):
    if not ok: raise ValueError(why)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def read(path,cap=64<<20):
    p=pathlib.Path(path);need(p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,"bounded regular file "+str(p))
    with p.open("rb") as f:raw=f.read(cap+1)
    need(len(raw)<=cap,"bounded read");return raw
def strict(raw):
    def pairs(rows):
        out={}
        for k,v in rows:need(k not in out,"duplicate JSON");out[k]=v
        return out
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda s:(_ for _ in ()).throw(ValueError(s)))
def path_name(name):
    p=pathlib.PurePosixPath(name)
    need(isinstance(name,str) and name and not p.is_absolute() and p.as_posix()==name and all(s not in (".","..") for s in p.parts),"canonical relative path")
    return name
def source_bytes(path,digest):
    raw=read(path);need(sha(raw)==digest,"exact source pin "+str(path));return raw
def input_archive(raw,inventory_raw):
    need(len(raw)<64<<20,"finite compressed archive")
    inv=strict(inventory_raw);need(isinstance(inv,dict) and 1<=len(inv)<=512,"finite input inventory")
    for n,h in inv.items():path_name(n);need(re.fullmatch("[0-9a-f]{64}",h) is not None,"input digest")
    # Bound decompression before tarfile decodes internal PAX/GNU metadata.
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as g: expanded=g.read((260<<20)+1)
    need(len(expanded)<=260<<20,"finite decompressed tar including metadata")
    with tarfile.open(fileobj=io.BytesIO(expanded),mode="r:") as t:
        rows=[];total=0
        for m in t:
            need(len(rows)<513 and m.isfile() and 0<=m.size<=64<<20,"finite regular tar members")
            total+=m.size;need(total<=256<<20,"finite expanded archive");rows.append(m)
        names=[m.name for m in rows]
        need(len(rows)==len(inv)+1 and len(names)==len(set(names)) and set(names)==set(inv)|{"input-inventory.json"},"exact tar inventory")
        need(sum(m.size for m in rows)<=256<<20,"finite expanded archive")
        for m in rows:
            path_name(m.name);need(m.isfile() and 0<=m.size<=64<<20,"regular bounded tar member")
            b=t.extractfile(m).read();need(sha(b)==(sha(inventory_raw) if m.name=="input-inventory.json" else inv[m.name]),"tar input bytes")
    return inv
def parse_go_json(raw):
    need(len(raw)<=8<<20,"bounded Go JSON")
    events=[strict(line) for line in raw.splitlines() if line.strip()]
    need(events and all(isinstance(e,dict) and e.get("Package")==PACKAGE for e in events),"exact measured package")
    tests=[e for e in events if "Test" in e]
    need(all(e["Test"]==TEST for e in tests),"only exact native test")
    need([e["Action"] for e in tests if e["Action"] in ("pass","fail","skip")]==["pass"],"exact native PASS, never SKIP")
    need(sum(e.get("Action")=="run" and e.get("Test")==TEST for e in events)==1,"one native test execution")
    need(events[-1]["Action"]=="pass" and "Test" not in events[-1] and not any(e["Action"] in ("fail","skip") for e in events),"package PASS with no failed/skipped event")
    return len(events)

# Executed only by root's separately retained SSH commands.
REMOTE_COMMON=r'''import base64,gzip,hashlib,importlib.util,io,json,os,pathlib,re,subprocess,sys,tarfile
def need(ok,why):
 if not ok:raise ValueError(why)
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p,cap=64<<20):
 p=pathlib.Path(p);need(p.is_file() and not p.is_symlink() and p.stat().st_size<=cap,"regular bounded file "+str(p));b=p.read_bytes();need(len(b)<=cap,"bound");return b
def source_verify():
 source=pathlib.Path(SOURCE);raw=read(ROOT/"git-source-inventory.json");need(sha(raw)==INV_SHA,"inventory pin");inv=json.loads(raw)
 need(inv["head"]==HEAD and inv["tree"]==TREE and inv["overlays"]=={} and type(INV_ROWS) is int and INV_ROWS>0 and isinstance(inv["rows"],list) and len(inv["rows"])==INV_ROWS,"exact source identity")
 expected=set()
 for row in inv["rows"]:
  name=row["path"];p=pathlib.PurePosixPath(name);need(name==p.as_posix() and not p.is_absolute() and all(v not in (".","..") for v in p.parts) and name not in expected,"source path")
  expected.add(name);f=source/name;b=read(f)
  need(row["mode"] in ("100644","100755") and bool(f.stat().st_mode&0o111)==(row["mode"]=="100755"),"source mode")
  need(hashlib.sha1(b"blob "+str(len(b)).encode()+bytes([0])+b).hexdigest()==row["git_blob"],"Git blob "+name)
 actual=set()
 for parent,dirs,files in os.walk(source,followlinks=False):
  need(all(not (pathlib.Path(parent)/n).is_symlink() for n in dirs+files),"no source symlinks")
  for n in files:actual.add(str((pathlib.Path(parent)/n).relative_to(source)))
 need(actual==expected,"complete source paths/no extras")
 need((ROOT/"source").is_symlink() and os.readlink(ROOT/"source")==SOURCE,"protected source symlink")
 return {"head":HEAD,"tree":TREE,"inventory_sha256":INV_SHA,"files":len(expected),"all_blobs_modes_no_extras":True}
def inputs_verify():
 raw=read(ROOT/"inputs/input-inventory.json");need(sha(raw)==INPUT_SHA,"input inventory pin");inv=json.loads(raw);total=0
 for n,h in inv.items():
  b=read(ROOT/"inputs"/n);total+=len(b);need(total<=256<<20 and sha(b)==h,"input pin "+n)
 return {"input_inventory_sha256":INPUT_SHA,"files":len(inv),"all_bytes_verified":True}
def no_go_voters():
 bad=[]
 for p in pathlib.Path("/proc").iterdir():
  if not p.name.isdigit():continue
  try:
   name=(p/"exe").resolve().name
   if name in {"go","compile","link","asm","cgo","treedb-fixed-peer"} or name.endswith(".test"):bad.append(p.name)
  except (OSError,RuntimeError):pass
 need(not bad,"no other Go/test/voter process")
 r=subprocess.run(["docker","ps","-q"],capture_output=True,text=True);need(r.returncode==0,"docker read admission")
 ids=r.stdout.split()
 if ids:
  r=subprocess.run(["docker","inspect",*ids],capture_output=True,text=True);need(r.returncode==0,"running inspect admission")
  for x in json.loads(r.stdout):
   need(not x["Name"].lstrip("/").startswith("treedb-") and not x["Config"].get("Labels",{}).get("treedb.fixed-cluster.run") and "treedb-fixed-peer" not in x.get("Path",""),"no active voters")
def write_new(p,b):
 p=pathlib.Path(p)
 with p.open("xb") as f:f.write(b)
def source_at_absolute(p,b,h):
 p=pathlib.Path(p);need(sha(b)==h,"staged source digest")
 if p.exists() or p.is_symlink():need(read(p)==b,"byte-identical existing source")
 else:write_new(p,b)
'''
REMOTE_PREPARE=r'''
x=json.load(sys.stdin);need(x["RootAcceptedFinalSourceAndInputs"] is True,"explicit root source/input admission")
need(os.getuid()==os.getgid()==1000 and not ROOT.exists() and not ROOT.is_symlink(),"fresh exclusive root/UID")
no_go_voters()
for n,h in RUNNER_PINS.items():
 p=RUNNER if n.startswith("gomap-") else TEMPLATE+"/"+n
 need(sha(read(p))==h,"actual retained runner source "+n)
ROOT.mkdir(mode=0o700);(ROOT/"source").symlink_to(SOURCE,target_is_directory=True)
write_new(ROOT/"git-source-inventory.json",base64.b64decode(x["source_inventory"],validate=True))
before=source_verify()
archive=base64.b64decode(x["archive"],validate=True);need(len(archive)<64<<20 and sha(archive)==x["archive_sha256"],"archive pin")
with gzip.GzipFile(fileobj=io.BytesIO(archive)) as g:expanded=g.read((260<<20)+1)
need(len(expanded)<=260<<20,"finite decompressed tar including metadata")
with tarfile.open(fileobj=io.BytesIO(expanded),mode="r:") as t:
 m=[];total=0
 for v in t:
  need(len(m)<513 and v.isfile() and 0<=v.size<=64<<20,"finite regular tar members")
  total+=v.size;need(total<=256<<20,"finite expanded archive");m.append(v)
 names=[v.name for v in m]
 need(len(m)<=513 and sum(v.size for v in m)<=256<<20 and len(names)==len(set(names)),"tar caps/unique")
 for v in m:
  p=pathlib.PurePosixPath(v.name);need(v.isfile() and 0<=v.size<=64<<20 and not p.is_absolute() and p.as_posix()==v.name and all(q not in (".","..") for q in p.parts),"canonical regular tar")
 (ROOT/"inputs").mkdir(mode=0o700);t.extractall(ROOT/"inputs",filter="data")
inv=json.loads(read(ROOT/"inputs/input-inventory.json"));need(set(names)==set(inv)|{"input-inventory.json"},"exact extracted inventory")
input_before=inputs_verify()
source_at_absolute(ROOT/"source_paths.py",base64.b64decode(x["resolver"],validate=True),RESOLVER_SHA)
sys.path.insert(0,str(ROOT))
source_at_absolute(HELPER,base64.b64decode(x["helper"],validate=True),HELPER_SHA)
source_at_absolute(COLLECTOR,base64.b64decode(x["collector"],validate=True),COLLECTOR_SHA)
initial=base64.b64decode(x["initial_oracle"],validate=True);need(sha(initial)==x["initial_oracle_sha256"],"initial oracle pin");write_new(ROOT/"initial-oracle.json",initial)
pins={"source_root":str(ROOT/"source"),"source_head":HEAD,"source_tree":TREE,"source_inventory":str(ROOT/"git-source-inventory.json"),"source_inventory_sha256":INV_SHA,"input_root":str(ROOT/"inputs"),"input_inventory_sha256":INPUT_SHA,"initial_oracle":str(ROOT/"initial-oracle.json"),"initial_oracle_sha256":x["initial_oracle_sha256"],"RootAcceptedFinalSourceAndInputs":True}
write_new(ROOT/"oracle-pins.json",(json.dumps(pins)+"\n").encode())
spec=importlib.util.spec_from_file_location("frozen_trial24_oracle_helper",HELPER);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
proposal=helper.prepare(ROOT/"oracle-pins.json",ROOT/"oracle-preparation")
overlay=str(ROOT/"oracle-preparation/overlay.json")
cfg={"unit_prefix":"gomap-trial24-prefix-oracle-","outer_timeout_seconds":300,"provenance":{"head":HEAD,"tree":TREE,"inventory_sha256":INV_SHA,"helper_sha256":HELPER_SHA,"collector_sha256":COLLECTOR_SHA,"input_inventory_sha256":INPUT_SHA,"native_test_only":True},"go_commands":[{"name":"native-prefix-oracle","args":["test","-json","-count=1","-timeout=180s","-overlay="+overlay,"-run=^TestTrial24FrozenCanonicalPrefixOraclePreparation$","./cmd/treedb-query-under-write"],"timeout_seconds":240}]}
write_new(ROOT/"config.json",(json.dumps(cfg)+"\n").encode())
r=subprocess.run(["python3","-B",RUNNER,str(ROOT)],capture_output=True);write_new(ROOT/"runner-prepare.stdout",r.stdout);write_new(ROOT/"runner-prepare.stderr",r.stderr);write_new(ROOT/"runner-prepare.exit",str(r.returncode).encode());need(r.returncode==0,"runner preparation")
inner=ROOT/"inner.sh";b=read(inner);marker=b"set -euo pipefail\n";need(b.count(marker)==1,"one generated prefix")
export=("export GOMAP_TRIAL24_ORACLE_INPUT="+__import__("shlex").quote(str(ROOT/"oracle-preparation/oracle-input.json"))+"\n").encode()
b=b.replace(marker,marker+export,1);inner.write_bytes(b)
after=source_verify();input_after=inputs_verify()
packet={"state":"PREPARED_NOT_EXECUTED_REQUIRES_ROOT_GENERATED_BYTES_REVIEW","before":before,"after":after,"inputs_before":input_before,"inputs_after":input_after,"files":{},"proposal":proposal}
for n in ("source_paths.py",pathlib.Path(HELPER).name,pathlib.Path(COLLECTOR).name,"config.json","run.sh","inner.sh","oracle-preparation/prefix_oracle_prepare_test.go","oracle-preparation/overlay.json","oracle-preparation/oracle-input.json"):
 raw=read(ROOT/n);packet["files"][n]={"sha256":sha(raw),"base64":base64.b64encode(raw).decode()}
write_new(ROOT/"prepared-packet.json",(json.dumps(packet)+"\n").encode())
print(json.dumps(packet))
'''
REMOTE_RUN=r'''
need(ROOT.is_dir() and not (ROOT/"run.started").exists(),"one-shot not started")
review=json.load(sys.stdin);prepared=json.loads(read(ROOT/"prepared-packet.json"))
need(review["RootReviewedGeneratedRunnerAndAdmittedRun"] is True and review["prepared_packet_sha256"]==sha(read(ROOT/"prepared-packet.json")),"explicit reviewed generated packet pin")
for n,item in prepared["files"].items():need(sha(read(ROOT/n))==item["sha256"],"prepared bytes unchanged")
before=source_verify();inputs=inputs_verify();no_go_voters()
write_new(ROOT/"run-admission.json",(json.dumps({"review":review,"source_before":before,"inputs_before":inputs})+"\n").encode())
# Exclusive run-admission prevents a second attempt even when admission fails before run.started.
r=subprocess.run(["/bin/bash",str(ROOT/"run.sh")])
raise SystemExit(r.returncode)
'''
REMOTE_CAPTURE=r'''
need((ROOT/"run-admission.json").is_file(),"actual one-shot run admission")
out={"state":"CAPTURED_ACTUAL_PENDING_LOCAL_AND_ROOT_VALIDATION","files":{},"errors":[]}
try:out["source_after"]=source_verify();out["inputs_after"]=inputs_verify()
except Exception as e:out["errors"].append(type(e).__name__+": "+str(e))
for prefix in ("receipts","oracle-preparation"):
 directory=ROOT/prefix
 for p in sorted(directory.iterdir()):
  if not p.is_file() or p.is_symlink():continue
  raw=read(p,8<<20);out["files"][prefix+"/"+p.name]={"sha256":sha(raw),"base64":base64.b64encode(raw).decode()}
for n in ("run-admission.json","prepared-packet.json","config.json","run.sh","inner.sh","initial-oracle.json","oracle-pins.json","git-source-inventory.json","source_paths.py",pathlib.Path(HELPER).name,pathlib.Path(COLLECTOR).name,"runner-prepare.stdout","runner-prepare.stderr","runner-prepare.exit"):
 raw=read(ROOT/n);out["files"][n]={"sha256":sha(raw),"base64":base64.b64encode(raw).decode()}
print(json.dumps(out))
'''
def remote_program(body,input_sha):
    constants=dict(ROOT=REMOTE,SOURCE=SOURCE,HEAD=HEAD,TREE=TREE,INV_SHA=INVENTORY_SHA,INV_ROWS=INVENTORY_ROWS,INPUT_SHA=input_sha,HELPER=REMOTE+"/"+pathlib.Path(HELPER).name,HELPER_SHA=HELPER_SHA,COLLECTOR=REMOTE+"/"+pathlib.Path(COLLECTOR).name,COLLECTOR_SHA=COLLECTOR_SHA,RESOLVER_SHA=RESOLVER_SHA,RUNNER=RUNNER,TEMPLATE=TEMPLATE,RUNNER_PINS=RUNNER_PINS)
    return "\n".join(k+"="+repr(v) for k,v in constants.items())+"\nROOT=__import__('pathlib').Path(ROOT)\n"+REMOTE_COMMON+body
def inventory_identity(inv):
    need(type(INVENTORY_ROWS) is int and INVENTORY_ROWS>0 and inv["head"]==HEAD and inv["tree"]==TREE and isinstance(inv["rows"],list) and len(inv["rows"])==INVENTORY_ROWS and inv["overlays"]=={},"exact source inventory")
def transport_argv(program):return SSH+[shlex.join(["python3","-B","-c",program])]
def prepare(opts):
    # Local artifact construction only; no subprocess is imported or called.
    helper=source_bytes(HELPER,HELPER_SHA);collector=source_bytes(COLLECTOR,COLLECTOR_SHA);resolver=source_bytes(pathlib.Path(__file__).parent/"source_paths.py",RESOLVER_SHA)
    for n,h in RUNNER_PINS.items():
        source_bytes(pathlib.Path(CONTEXT)/n,h)
    inventory=source_bytes(INVENTORY,INVENTORY_SHA);inv=strict(inventory)
    inventory_identity(inv)
    need(opts.archive==ARCHIVE,"fixed fresh archive path")
    archive=read(opts.archive);need(sha(archive)==opts.archive_sha256,"root supplied archive digest")
    input_inv=read(pathlib.Path(INPUT)/"input-inventory.json")
    need(sha(input_inv)==opts.input_inventory_sha256,"root supplied input inventory digest")
    input_archive(archive,input_inv)
    initial=read(opts.initial_oracle,1<<20);need(sha(initial)==opts.initial_oracle_sha256,"root supplied initial oracle digest")
    identity=strict(initial);need(identity["Rows"]==10005 and re.fullmatch("[0-9a-f]{64}",identity["SHA256"]) is not None,"actual initial full identity")
    need(opts.root_accepted_final_source_and_inputs,"explicit root acceptance, never default True")
    out=pathlib.Path(opts.out);need(out.is_absolute() and not out.exists() and not out.is_symlink(),"exclusive local artifact root");out.mkdir(mode=0o700)
    payload=dict(resolver=base64.b64encode(resolver).decode(),RootAcceptedFinalSourceAndInputs=True,source_inventory=base64.b64encode(inventory).decode(),archive=base64.b64encode(archive).decode(),archive_sha256=opts.archive_sha256,helper=base64.b64encode(helper).decode(),collector=base64.b64encode(collector).decode(),initial_oracle=base64.b64encode(initial).decode(),initial_oracle_sha256=opts.initial_oracle_sha256)
    (out/"prepare-stdin.json").write_text(json.dumps(payload)+"\n")
    commands={}
    for name,body in (("prepare",REMOTE_PREPARE),("run",REMOTE_RUN),("capture",REMOTE_CAPTURE)):
        program=remote_program(body,opts.input_inventory_sha256);ast.parse(program)
        (out/(name+"-remote.py")).write_text(program)
        argv=transport_argv(program);(out/(name+"-argv.json")).write_text(json.dumps(argv)+"\n");commands[name]=argv
    # Root must retain each transport's exact argv/stdin hash/stdout/stderr/timing/exit.
    proposal=dict(state="INERT_TRANSPORT_PREPARED_NOT_EXECUTED",remote_root=REMOTE,source_root=SOURCE,head=HEAD,tree=TREE,source_inventory_sha256=INVENTORY_SHA,input_inventory_sha256=opts.input_inventory_sha256,archive_sha256=opts.archive_sha256,initial_oracle_sha256=opts.initial_oracle_sha256,helper_sha256=HELPER_SHA,collector_sha256=COLLECTOR_SHA,runner_pins=RUNNER_PINS,commands=commands,runtime_started=False,requirements=["Root independently reviews preparation sources before prepare transport","After prepare, root retains/reviews actual run.sh, inner.sh, overlay/Go bytes/hash; Go source byte-identical to reviewed helper GO_SOURCE","Run requires stdin RootReviewedGeneratedRunnerAndAdmittedRun=true and actual prepared_packet_sha256","Root wraps transport with retained argv/stdinSHA/stdout/stderr/exit/start/finish; bounded prepare120/run340/capture120 seconds","No automatic retry: same exclusive root/run-admission/run.started rejects replay","Native pending prefix packet requires root canonical full-population validation; no promotion here"])
    (out/"proposal.json").write_text(json.dumps(proposal,indent=2)+"\n");return proposal
def capture_file(path):
    packet=strict(read(path,128<<20));need(not packet["errors"],"source/input after verification failed")
    raw={}
    for n,item in packet["files"].items():
        path_name(n);b=base64.b64decode(item["base64"],validate=True);need(len(b)<=8<<20 and sha(b)==item["sha256"],"raw capture digest");raw[n]=b
    prefix="receipts/00-native-prefix-oracle"
    need(raw[prefix+".exit"].strip()==b"0" and raw["receipts/scope.exit"].strip()==b"0","actual test and scope exit0")
    count=parse_go_json(raw[prefix+".jsonl"])
    need(sha(raw["source_paths.py"])==RESOLVER_SHA and sha(raw[pathlib.Path(HELPER).name])==HELPER_SHA and sha(raw[pathlib.Path(COLLECTOR).name])==COLLECTOR_SHA,"actual staged source closure pins")
    for name in ("start","end"):
        need(raw["receipts/"+name+"-memory.max.txt"].strip()==b"8589934592" and raw["receipts/"+name+"-memory.swap.max.txt"].strip()==b"0","actual 8GiB/swap0")
        events=dict(line.split() for line in raw["receipts/"+name+"-memory.events.txt"].decode().splitlines())
        need(all(int(events.get(k,"-1"))==0 for k in ("oom","oom_kill")) and int(events.get("oom_group_kill","0"))==0,"actual no OOM")
    peak=int(raw["receipts/end-memory.peak.txt"]);need(0<=peak<=8589934592,"actual finite peak")
    pending=strict(raw["oracle-preparation/native-prefix-oracles-pending.json"])
    need(pending["state"]=="NATIVE_CANONICAL_PREFIX_ORACLES_GENERATED_PENDING_ROOT_VALIDATION" and pending["source_head"]==HEAD and pending["source_tree"]==TREE and pending["RunID"]=="rf4trial24mixedchangingc1mixedc1v1" and pending["Profile"]=="changing-top10" and len(pending["Prefixes"])==59 and len(pending["OriginalRequests"])==58,"pending native packet shape/source")
    admission=strict(raw["run-admission.json"]);prepared=strict(raw["prepared-packet.json"])
    for n,item in prepared["files"].items():need(sha(raw[n])==item["sha256"],"generated runner/payload unchanged")
    need(admission["review"]["RootReviewedGeneratedRunnerAndAdmittedRun"] is True and admission["review"]["prepared_packet_sha256"]==sha(raw["prepared-packet.json"]),"actual root reviewed packet binding")
    source_inv=strict(raw["git-source-inventory.json"])
    need(sha(raw["git-source-inventory.json"])==INVENTORY_SHA,"captured source inventory digest");inventory_identity(source_inv)
    pins=strict(raw["oracle-pins.json"]);initial=strict(raw["initial-oracle.json"]);cfg=strict(raw["oracle-preparation/oracle-input.json"])
    need(sha(raw["initial-oracle.json"])==pins["initial_oracle_sha256"] and initial["Rows"]==10005 and pending["InitialPopulationSHA256"]==initial["SHA256"]==cfg["InitialPopulationSHA256"],"exact initial full identity")
    need(cfg["SourceHead"]==HEAD and cfg["SourceTree"]==TREE and cfg["InputInventorySHA256"]==pending["InputInventorySHA256"],"native input/source arguments")
    command=strict(raw["config.json"])["go_commands"]
    expected=[{"name":"native-prefix-oracle","args":["test","-json","-count=1","-timeout=180s","-overlay="+REMOTE+"/oracle-preparation/overlay.json","-run=^"+TEST+"$","./cmd/treedb-query-under-write"],"timeout_seconds":240}]
    need(command==expected and raw["runner-prepare.exit"].strip()==b"0","actual exact one native command")
    need(admission["source_before"]==packet["source_after"]==prepared["before"]==prepared["after"],"source before/after exact equality")
    need(admission["inputs_before"]==packet["inputs_after"]==prepared["inputs_before"]==prepared["inputs_after"],"inputs before/after exact equality")
    need(pending["InputInventorySHA256"]==packet["inputs_after"]["input_inventory_sha256"],"native exact input binding")
    return dict(state="ACTUAL_EXECUTION_PASS_PENDING_ROOT_CANONICAL_PREFIX_VALIDATION",head=HEAD,tree=TREE,go_json_events=count,peak_bytes=peak,oracle_sha256=sha(raw["oracle-preparation/native-prefix-oracles-pending.json"]),campaign_accepted=False)
def self_check():
    helper=source_bytes(HELPER,HELPER_SHA);source_bytes(COLLECTOR,COLLECTOR_SHA)
    tree=ast.parse(helper);nodes=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="GO_SOURCE" for t in n.targets)]
    need(len(nodes)==1 and TEST in ast.literal_eval(nodes[0].value),"exact unchanged native Go source")
    for n,h in RUNNER_PINS.items():
        source_bytes(pathlib.Path(CONTEXT)/n,h)
    positive=b'{"Action":"run","Package":"'+PACKAGE.encode()+b'","Test":"'+TEST.encode()+b'"}\n'+b'{"Action":"pass","Package":"'+PACKAGE.encode()+b'","Test":"'+TEST.encode()+b'"}\n'+b'{"Action":"pass","Package":"'+PACKAGE.encode()+b'"}\n'
    need(parse_go_json(positive)==3,"synthetic parser positive")
    for action in ("skip","fail"):
        try:parse_go_json(positive.replace(b'"Action":"pass"',('"Action":"'+action+'"').encode(),1))
        except ValueError:pass
        else:raise ValueError("synthetic failed/skipped oracle accepted")
    try:path_name("../foreign")
    except ValueError:pass
    else:raise ValueError("foreign path accepted")
    try:input_archive(b"",b"{}")
    except ValueError:pass
    else:raise ValueError("empty input accepted")
    inv=json.dumps({"ok":sha(b"")}).encode()
    def archive(rows):
        buf=io.BytesIO()
        with tarfile.open(fileobj=buf,mode="w:gz") as ar:
            for name,value in rows:
                m=tarfile.TarInfo(name);m.size=len(value);ar.addfile(m,io.BytesIO(value))
        return buf.getvalue()
    need(input_archive(archive([("ok",b""),("input-inventory.json",inv)]),inv)==strict(inv),"bounded tar positive")
    try:input_archive(archive([(str(i),b"") for i in range(2048)]),inv)
    except ValueError as e:need(str(e)=="finite regular tar members","member bound before full inventory")
    else:raise ValueError("tar member bomb accepted")
    for body in (REMOTE_PREPARE,REMOTE_RUN,REMOTE_CAPTURE):ast.parse(remote_program(body,"1"*64))
    return dict(state="INERT_SYNTHETIC_CHECK_ONLY",positive=2,negative=5,runtime_started=False)
if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);mode=p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--prepare",action="store_true");mode.add_argument("--capture-file");mode.add_argument("--self-check",action="store_true")
    p.add_argument("--archive",default=ARCHIVE);p.add_argument("--archive-sha256");p.add_argument("--input-inventory-sha256");p.add_argument("--initial-oracle");p.add_argument("--initial-oracle-sha256");p.add_argument("--root-accepted-final-source-and-inputs",action="store_true");p.add_argument("--out")
    opts=p.parse_args()
    print(json.dumps(self_check() if opts.self_check else capture_file(opts.capture_file) if opts.capture_file else prepare(opts),indent=2))
