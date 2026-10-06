if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from source_paths import source_path, isolate_paths, W
"""Unexecuted Trial24 inactive-input staging and isolated permission observation.
Root must review this source and supply exact final manifest/archive pins.
No campaign activation, voter start, DB mount, gate, mutations, or replay.
"""
import argparse, ast, copy, hashlib, importlib.util, inspect, io, json, math
import pathlib, re, secrets, shlex, subprocess, sys, tarfile, time, types
COLLECTOR = source_path('/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py')
COLLECTOR_SHA = "39f7025a63db628a3e6c3420b3685258ad68e8695afdae02ab1d1d614680ada2"
RESOURCE = source_path('/tmp/gomap-4994-mixed-window-resource-accounting-root-v5.py')
RESOURCE_SHA = "fe917502c45aa2f4619bd6d5cb294243d343c5cd93f3c6974ccff71b01eb5f96"
RUN = W.campaign
HEAD = "__ROOT_FROZEN_HEAD__"
TREE = "__ROOT_FROZEN_TREE__"
HOST = "mikers@192.168.0.185"
MANIFEST = "/tmp/gomap-4997-4998-"+RUN+"-final-inactive-root-v1.json"
ARCHIVE = "/tmp/gomap-4997-4998-"+RUN+"-inputs-root-v1.tar.gz"

def permission_name(c,a):
    checkpoint=W.issue=='5021' and a.get('receipts',{}).get('predeclaration',c.PREDECLARATION)!=c.PREDECLARATION
    return c.NAME+('-'+c.CHECKPOINT_PHASE if checkpoint else '')+'-isolated-permission'

def need(ok, why):
    if not ok: raise ValueError(why)
def sha(raw): return hashlib.sha256(raw).hexdigest()
def encode(value): return (json.dumps(value, indent=2)+"\n").encode()
def read(path, cap=64<<20):
    p=pathlib.Path(path)
    need(p.is_file() and not p.is_symlink() and p.stat().st_size<=cap, "bounded regular file "+str(p))
    with p.open("rb") as f: raw=f.read(cap+1)
    need(len(raw)<=cap, "bounded bytes")
    return raw
def pinned(path, digest):
    raw=read(path);need(sha(raw)==digest,"source/input pin "+str(path));return raw
def functions(raw):
    text=raw.decode();tree=ast.parse(text)
    return {n.name:ast.get_source_segment(text,n) for n in tree.body if isinstance(n,ast.FunctionDef)}
def load():
    raw=pinned(COLLECTOR,COLLECTOR_SHA)
    spec=importlib.util.spec_from_file_location("trial24_permission_collector",COLLECTOR)
    c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
    need(c.RUN==RUN and c.HOST==HOST,"collector namespace")
    f=functions(raw)["validate_manifest"]
    old="all(a[k] is True for k in FLAGS)"
    need(f.count(old)==1,"exact inactive-only validation adaptation")
    ns=dict(c.__dict__)
    exec(compile(f.replace(old,"all(a[k] is False for k in FLAGS)"),"inactive-manifest-adaptation","exec"),ns)
    inactive_validate=ns["validate_manifest"]
    rf=functions(pinned(RESOURCE,RESOURCE_SHA))
    resource_tree=ast.parse(read(RESOURCE))
    keynode=[n for n in resource_tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=="PERMISSION_PATH_KEYS" for t in n.targets)]
    need(len(keynode)==1,"permission key authority")
    keys=ast.literal_eval(keynode[0].value)
    pn=dict(need=need,sha=sha,strict=c.strict_json,math=math,re=re,shlex=shlex,
            core=types.SimpleNamespace(digest=c.digest_valid),RUN=c.RUN,QUERY_RUN=c.QUERY_RUN,
            ROOT=c.ROOT,INPUT_ROOT=c.INPUT_ROOT,PERMISSION_PATH_KEYS=keys)
    exec(compile("\n\n".join(rf[k] for k in ("finite","interval","permission_proof")),"pinned-permission-only","exec"),pn)
    return c,inactive_validate,pn["permission_proof"],keys

def isolated_arguments(c,a,nonce):
    c.APPROVED=a;c.root=c.ROOT;c.image=a["query_image"];c.launch_nonce=nonce
    argv=c.driver_arguments()
    argv[argv.index("--name")+1]=permission_name(c,a)
    argv[argv.index("--network=host")]="--network=none"
    gate=c.GATE_DIR+":"+c.MOUNT_GATE+":rw"
    at=argv.index(gate);need(argv[at-1]=="-v","exact gate bind");del argv[at-1:at+1]
    for flag in ("-read-resource-gate-dir","-mixed-interval","-mixed-profile","-mixed-originals"):
        need(argv.count(flag)==1,"exact removed mixed/gate flag")
        at=argv.index(flag);del argv[at:at+2]
    argv[argv.index("-mode")+1]="read-window"
    # Isolated credential observation uses the read-only driver's 60s bound.
    argv[argv.index("-read-window")+1]="60s"
    validate_command(c,a,nonce,argv)
    return argv
def validate_command(c,a,nonce,argv):
    # Compare to the exact frozen producer, independently of receipt claims.
    c.APPROVED=a;c.root=c.ROOT;c.image=a["query_image"];c.launch_nonce=nonce
    expected=c.driver_arguments()
    expected[expected.index("--name")+1]=permission_name(c,a)
    expected[expected.index("--network=host")]="--network=none"
    at=expected.index(c.GATE_DIR+":"+c.MOUNT_GATE+":rw");del expected[at-1:at+1]
    for flag in ("-read-resource-gate-dir","-mixed-interval","-mixed-profile","-mixed-originals"):
        at=expected.index(flag);del expected[at:at+2]
    expected[expected.index("-mode")+1]="read-window"
    expected[expected.index("-read-window")+1]="60s"
    need(argv==expected and a["driver_uid_gid"]=="1000:1000","exact readonly driver argv/user")
    need(re.fullmatch("[0-9a-f]{32}",nonce) is not None,"fresh invocation nonce")

def archive_check(c,a,inv,archive):
    need(len(archive)<64<<20,"archive compressed cap")
    with tarfile.open(fileobj=io.BytesIO(archive),mode="r:gz") as t:
        members=t.getmembers();names=[m.name for m in members]
        need(len(members)==len(inv)+1<=513 and len(names)==len(set(names)),"exact unique archive entries")
        need(set(names)==set(inv)|{"input-inventory.json"},"exact inventory/archive paths")
        need(sum(m.size for m in members)<=256<<20,"archive expanded cap")
        for m in members:
            need(m.isfile() and 0<=m.size<=64<<20 and c.input_name(m.name),"regular canonical archive member")
            raw=t.extractfile(m).read()
            digest=a["input_inventory_sha256"] if m.name=="input-inventory.json" else inv[m.name]
            need(sha(raw)==digest,"archive member digest "+m.name)

STAGE = r"""if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import os,sys,io,tarfile,pathlib,hashlib,json
p=pathlib.Path(sys.argv[1]);raw=sys.stdin.buffer.read((64<<20)+1)
assert len(raw)<64<<20 and hashlib.sha256(raw).hexdigest()==sys.argv[2]
assert not p.exists() and not p.is_symlink() and os.getuid()==os.getgid()==1000
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as t:
 m=t.getmembers();names=[x.name for x in m]
 assert len(m)<=513 and sum(x.size for x in m)<=256<<20
 assert len(names)==len(set(names)) and all(x.isfile() and 0<=x.size<=64<<20 and not pathlib.PurePosixPath(x.name).is_absolute() and pathlib.PurePosixPath(x.name).as_posix()==x.name and all(v not in ('.','..') for v in pathlib.PurePosixPath(x.name).parts) for x in m)
 isolate_paths([p],[sys.argv[4],sys.argv[5]])
 p.mkdir(mode=0o700);t.extractall(p,filter='data')
b=(p/'input-inventory.json').read_bytes();assert hashlib.sha256(b).hexdigest()==sys.argv[3]
inv=json.loads(b);assert set(inv)|{'input-inventory.json'}==set(names)
assert all(hashlib.sha256((p/n).read_bytes()).hexdigest()==h for n,h in inv.items())
print(json.dumps(dict(state='PASS_EXCLUSIVE_POST_INPUT_STAGING',path=str(p),input_inventory_sha256=sys.argv[3],archive_sha256=sys.argv[2],files=len(names))))
"""

STAGE=STAGE.replace("import os,sys,io,tarfile,pathlib,hashlib,json\n","import os,sys,io,tarfile,pathlib,hashlib,json\n"+inspect.getsource(isolate_paths)+"\n",1)

VERIFY_STAGED = r"""if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import os,sys,pathlib,hashlib,json,stat
p=pathlib.Path(sys.argv[1]);assert p.is_absolute() and '..' not in p.parts
assert os.getuid()==os.getgid()==1000
assert not any(v.is_symlink() for v in [p]+list(p.parents))
assert p.is_dir() and p.stat().st_uid==p.stat().st_gid==1000
ip=p/'input-inventory.json';assert ip.is_file() and not ip.is_symlink() and ip.stat().st_size<=1<<20
b=ip.read_bytes();assert len(b)<=1<<20 and hashlib.sha256(b).hexdigest()==sys.argv[2]
def unique(pairs):
 d={}
 for k,v in pairs:assert k not in d;d[k]=v
 return d
inv=json.loads(b,object_pairs_hook=unique);assert inv==json.loads(sys.argv[3]) and 1<=len(inv)<=512
expected_dirs={str(parent) for n in inv for parent in pathlib.PurePosixPath(n).parents if str(parent)!='.'}
entries=list(p.rglob('*'));assert len(entries)<=4096
files=set();dirs=set();total=0
for v in entries:
 assert not v.is_symlink();s=v.stat();assert s.st_uid==s.st_gid==1000
 n=v.relative_to(p).as_posix()
 if stat.S_ISDIR(s.st_mode):dirs.add(n);continue
 assert stat.S_ISREG(s.st_mode) and s.st_size<=64<<20
 files.add(n);total+=s.st_size;assert total<=256<<20
 raw=v.read_bytes();assert hashlib.sha256(raw).hexdigest()==(sys.argv[2] if n=='input-inventory.json' else inv[n])
assert files==set(inv)|{'input-inventory.json'} and dirs==expected_dirs
print(json.dumps(dict(state='PASS_READ_ONLY_EXISTING_POST_INPUT_STAGING',path=str(p),input_inventory_sha256=sys.argv[2],files=len(files),mutated=False)))
"""

def validate_permission(c,permission_proof,keys,pr,raw,a,nonce):
    need(set(raw)=={pr[k] for k in keys} and len(raw)==8,"exact eight raw permission paths")
    need(pr["raw_evidence"]=={p:sha(b) for p,b in raw.items()},"all raw permission hashes")
    validate_command(c,a,nonce,c.strict_json(raw[pr["command_path"]]))
    validated=permission_proof(pr,raw,a)
    name=permission_name(c,a)
    x=c.strict_json(raw[pr["inspect_path"]])[0]
    need(x["Name"]=="/"+name and x["Config"]["Labels"]["treedb.fixed-cluster.run"]==RUN and x["Config"]["Labels"]["treedb.fixed-cluster.invocation"]==nonce,"exact name/run/nonce")
    h=x["HostConfig"]
    need(h["Memory"]==h["MemorySwap"]==2147483648 and h["NanoCpus"]==2000000000 and h["RestartPolicy"]["Name"]=="no","isolated cap/restart")
    reports=[c.strict_json(line) for line in raw[pr["stdout_path"]].splitlines() if line.strip()]
    error=reports[1]["Report"]["Error"]
    need(error=="all-four active readiness/applied-prefix observation bound exhausted","native readiness refusal after credentials")
    observations=reports[1]["Report"]["Admission"]["ReadinessBefore"]
    need(len(observations)==4 and {o["RequestedNode"] for o in observations}==set(c.HOSTS) and all(o["Round"]==0 for o in observations),"all four isolated readiness observations")
    for o in observations:
        err=o["Error"]
        need(("connect:" in err or "dial tcp" in err) and not any(s in err.lower() for s in ("permission denied","no such file","certificate","private key")),"credential readable; only network refusal")
    return validated,error

def main(opts):
    c,inactive_validate,permission_proof,keys=load()
    checkpoint_manifest=MANIFEST.replace('-root-v1.json','-'+ 'trial24-checkpoint-window-v1'+'-root-v1.json')
    need(opts.manifest in (MANIFEST,checkpoint_manifest) and opts.archive==ARCHIVE,"exact Trial24 planned inputs")
    a=c.strict_json(pinned(opts.manifest,opts.manifest_sha256))
    inactive_validate(a,COLLECTOR_SHA)
    need(a["source_head"]==HEAD and a["source_tree"]==TREE,"exact source binding")
    pinned_inputs,inv,nodes,vectors=c.prepare_local(a) # actual FOUR-value interface; all 13 receipts remain required
    budget=c.checkpoint_budget(a,pinned_inputs)
    need((opts.manifest==checkpoint_manifest)==(budget is not None),'distinct checkpoint manifest; no original overwrite')
    if budget is not None:need(not c.OUTPUT.exists() and not c.OUTPUT.is_symlink(),'original local window output remains unused')
    archive=pinned(opts.archive,opts.archive_sha256);archive_check(c,a,inv,archive)
    nonce=secrets.token_hex(16);argv=isolated_arguments(c,a,nonce)
    # No activation flag is changed or written; this is permission observation only.
    out=pathlib.Path(opts.out);need(out.is_absolute() and not out.exists() and not out.is_symlink(),"fresh exclusive evidence root")
    isolate_paths([out],[c.LOCAL_INPUT_ROOT,pathlib.Path(__file__).resolve().parent,opts.manifest,opts.archive,COLLECTOR,RESOURCE]+list(pinned_inputs))
    out.mkdir(mode=0o700)
    serial=0;closing=False
    def call(label,args,stdin=None,timeout=45):
        nonlocal serial
        if not closing:timeout=c.phase_timeout(budget,timeout,first_permission=serial==0)
        serial+=1;argv=["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10",HOST,shlex.join(args)]
        rec=dict(argv=argv,started_unix=time.time());prefix=out/("%04d-%s"%(serial,label))
        if stdin is not None:rec.update(stdin_sha256=sha(stdin),stdin_bytes=len(stdin))
        stdout=stderr=b""
        try:
            r=subprocess.run(argv,input=stdin,capture_output=True,timeout=timeout)
            stdout,stderr=r.stdout,r.stderr;rec["exit_code"]=r.returncode
        except subprocess.TimeoutExpired as e:
            stdout=e.stdout or b"";stderr=e.stderr or b""
            rec.update(exit_code=None,timed_out=True);raise
        except BaseException as e:
            rec.update(exit_code=None,exception_type=type(e).__name__);raise
        finally:
            rec.update(finished_unix=time.time(),stdout=stdout.decode("utf-8","surrogateescape"),stderr=stderr.decode("utf-8","surrogateescape"))
            prefix.with_suffix(".json").write_bytes(encode(rec))
            prefix.with_suffix(".stdout").write_bytes(stdout);prefix.with_suffix(".stderr").write_bytes(stderr)
        need(r.returncode==0,"failed retained command "+label)
        return stdout,prefix.with_suffix(".json"),rec
    if budget is None:
        call("exclusive-stage",["python3","-c",STAGE,c.INPUT_ROOT,sha(archive),a["input_inventory_sha256"],"__ROOT_FROZEN_SOURCE_ROOT__",c.ROOT],stdin=archive,timeout=60)
    else:
        call("verify-existing-stage",["python3","-c",VERIFY_STAGED,c.INPUT_ROOT,a["input_inventory_sha256"],json.dumps(inv)],timeout=60)
    (out/"command.json").write_bytes(encode(argv))
    name=argv[argv.index("--name")+1]
    check="if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')\nimport subprocess; r=subprocess.run(['docker','inspect',"+repr(name)+"],capture_output=True,text=True); assert r.returncode!=0 and 'no such' in r.stderr.lower(); print('PASS_EXCLUSIVE_PERMISSION_NAME')"
    call("exclusive-name",["python3","-c",check])
    launched,launch_path,_=call("launch",argv);cid=launched.decode().strip()
    need(c.digest_valid(cid),"actual launch CID")
    closing=True  # Already-owned wait/inspect/logs/stop remain bounded after phase exhaustion.
    try:
        _,exit_path,_=call("wait",["docker","wait",cid],timeout=45)
        inspected,inspect_receipt_path,_=call("inspect",["docker","inspect",cid])
        (out/"inspect.json").write_bytes(inspected)
        logged,logs_path,logs=call("logs",["docker","logs",cid])
        (out/"stdout.jsonl").write_bytes(logged);(out/"stderr.log").write_bytes(logs["stderr"].encode("utf-8","surrogateescape"))
    finally:
        if budget is not None:
            final,_,_=call("permission-final-inspect",["docker","inspect",cid])
            owned=c.strict_json(final);need(isinstance(owned,list) and len(owned)==1,"one owned permission container")
            owned=owned[0]
            need(owned['Id']==cid and owned['Image']==a['query_image'] and owned['Name']=='/'+name,"exact owned permission CID/image/name")
            need(owned['Config']['User']==a['driver_uid_gid'] and owned['Config']['Labels']['treedb.fixed-cluster.invocation']==nonce and owned['Config']['Labels']['treedb.fixed-cluster.run']==RUN,"owned permission user/nonce/run")
            if owned['State']['Running']:
                call("permission-final-stop",["docker","stop","-t","3",cid],timeout=10)
                final,_,_=call("permission-stopped-inspect",["docker","inspect",cid])
                stopped=c.strict_json(final)
                need(isinstance(stopped,list) and len(stopped)==1 and stopped[0]['Id']==cid and stopped[0]['State']['Running'] is False,"owned permission stopped")
    pr=dict(NetworkMode="none",DBMounts=[],CredentialsReadable=True,DriverSHA256=a["driver_sha256"],Image=a["query_image"],User=a["driver_uid_gid"],RunID=c.RUN,MutationInvocations=0,PlannedSHA256=sha(logged),ApprovedArgv=argv,
        inspect_path=str(out/"inspect.json"),command_path=str(out/"command.json"),stdout_path=str(out/"stdout.jsonl"),stderr_path=str(out/"stderr.log"),
        launch_path=str(launch_path),exit_path=str(exit_path),inspect_receipt_path=str(inspect_receipt_path),logs_path=str(logs_path))
    raw={pr[k]:read(pr[k],256<<20) for k in keys}
    need(len(raw)==8,"eight distinct raw evidence files")
    pr["raw_evidence"]={p:sha(b) for p,b in raw.items()}
    validated,error=validate_permission(c,permission_proof,keys,pr,raw,a,nonce)
    pr.update(actual_validation=validated,CredentialsReadEvidence=error,manifest_sha256=opts.manifest_sha256,archive_sha256=opts.archive_sha256,collector_sha256=COLLECTOR_SHA,permission_source_sha256=RESOURCE_SHA,scope="ISOLATED_PERMISSION_ONLY_NOT_CAMPAIGN_ACCEPTANCE")
    (out/"permission-proof.json").write_bytes(encode(pr))
    print(json.dumps(dict(state="PASS_ISOLATED_READONLY_PERMISSION",proof=str(out/"permission-proof.json"),sha256=sha(read(out/"permission-proof.json")),raw_count=8,mutations=0)))

def self_check():
    # Synthetic argv controls only, not runtime or campaign evidence.
    c,check,proof,keys=load()
    a={"driver_uid_gid":"1000:1000","query_image":"sha256:"+"1"*64}
    nonce="a"*32;argv=isolated_arguments(c,a,nonce)
    validate_command(c,a,nonce,argv)
    need(len(keys)==8 and len([v for v in argv if v=="-v"])==4,"eight raw files / four readonly binds")
    for extra in (["-mixed-profile","changing-top10"],["-v","/foreign:/data:rw"],["--mount=type=bind,src=/foreign,dst=/data"]):
        try: validate_command(c,a,nonce,argv+extra)
        except ValueError: pass
        else: raise ValueError("unsafe argv synthetic control accepted")
    ast.parse(STAGE)
    return dict(state="INERT_SOURCE_CHECK_ONLY",positive=1,negative=3,source_pins={COLLECTOR:COLLECTOR_SHA,RESOURCE:RESOURCE_SHA},runtime_executed=False)

if __name__=="__main__":
    if sys.argv[1:]==["--self-check"]: print(json.dumps(self_check(),sort_keys=True))
    else:
        p=argparse.ArgumentParser(description=__doc__)
        p.add_argument("--manifest",default=MANIFEST);p.add_argument("--manifest-sha256",required=True)
        p.add_argument("--archive",default=ARCHIVE);p.add_argument("--archive-sha256",required=True)
        p.add_argument("--out",required=True)
        main(p.parse_args())
