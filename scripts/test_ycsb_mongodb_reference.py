"""Synthetic offline MongoDB reference evidence; no container or product runs."""
import base64
import copy
import datetime as dt
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import ycsb_compare_validate as c
import ycsb_compare_live as live
import ycsb_mongodb_reference as reference
from test_ycsb_correctness_fixture import fixture

ROOT=pathlib.Path(__file__).parent


def ts(second):
    return (dt.datetime(2026,10,29,tzinfo=dt.timezone.utc)+dt.timedelta(seconds=second)).isoformat().replace("+00:00","Z")


def result(start,end):
    return dict(attempted=True,success=True,error="",start_utc=ts(start),end_utc=ts(end),elapsed_ns=(end-start)*1_000_000_000)


class ReferencePackets(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="synthetic-mongodb-reference-",dir=os.environ.get("TMPDIR","/home/mikers"))
        self.addCleanup(self.temp.cleanup)
        self.out=pathlib.Path(self.temp.name);self.directory=self.out/"mongodb";self.directory.mkdir()
        self.m=dict(schema_version=1,kind=reference.KIND,lifecycle_contract=1,campaign_id="f"*32,targets=["mongodb"],profile=reference.PROFILE,formats={"mongodb":"bson"},write_concern=dict(w=1,journal_requested=None),recordcount=64,operationcount=10000,threadcount=16,run_repeats=1,uid=1000,gid=1000,address="127.0.0.1:27018",docker_binary="/synthetic/docker",client_binary="/synthetic/ycsb",reference_source_sha256=c.digest(reference.__file__),deadlines=dict(phase_seconds=600,helper_seconds=180,cleanup_seconds=10,phase_outer_seconds=640,helper_outer_seconds=220,readiness_seconds=60,outer_seconds=7200))
        b=dict(schema_version=1,kind="mongodb_image_build",image_id="sha256:"+"a"*64,repo_digest="mongo@sha256:"+"b"*64,os="linux",architecture="amd64",version="8.2.7",source_commit=reference.SUPPORTED_SOURCE["8.2.7"],exe_sha256="c"*64,docker_cli_sha256="d"*64,runtime=dict(engine="synthetic",engine_commit="synthetic",containerd="synthetic",runc="synthetic",driver="overlay2",cgroup_version="2",default_runtime="runc"))
        self.m["server_builds"]={"mongodb":b}
        b["image_labels"]={"org.opencontainers.image.version":"24.04"}
        self.m["client"]=dict(schema_version=1,producer_commit=c.PRODUCER,source_commit=c.PRODUCER,source_patch_sha256=hashlib.sha256(b"").hexdigest(),build_id="synthetic-offline-client",exe_sha256="e"*64,toolchain="synthetic",compatibility_configuration="synthetic")
        for key,name in (("fixture","fixture-source.go"),("control","reference-control-source.go")):
            (self.out/name).write_bytes(b"SYNTHETIC offline source")
            self.m[key]=dict(schema_version=1,binary="/synthetic/"+key,exe_sha256="9"*64,source_sha256=c.digest(self.out/name))
        self.save(self.out/"manifest.json",self.m)
        self.save(self.out/"reference-build.json",b)
        self.save(self.out/"client-build.json",self.m["client"])
        (self.out/"client.patch").write_bytes(b"");(self.out/"client-buildinfo.txt").write_text("SYNTHETIC offline client\n")
        (self.out/"reference-source.py").write_bytes(pathlib.Path(reference.__file__).read_bytes())
        self.save(self.out/"reference-image.json",dict(Id=b["image_id"],RepoDigests=[b["repo_digest"]],Architecture="amd64",Os="linux",Config=dict(Env=["MONGO_VERSION=8.2.7"],Labels=b["image_labels"])))
        self.save(self.out/"reference-runtime.json",dict(version=dict(Version="synthetic",GitCommit="synthetic",Components=[dict(Name="containerd",Version="synthetic"),dict(Name="runc",Version="synthetic")]),info=dict(Driver="overlay2",CgroupVersion="2",DefaultRuntime="runc")))
        self.ordinal=0;self.owners={}
        runtime=c.read(self.out/"reference-runtime.json")
        inventory={name:self.docker(args,json.dumps(value)+"\n",0) for name,args,value in (("image",["image","inspect",b["image_id"]],[c.read(self.out/"reference-image.json")]),("version",["version","--format","{{json .Server}}"],runtime["version"]),("info",["info","--format","{{json .}}"],runtime["info"]))}
        self.save(self.out/"reference-inventory.json",dict(schema_version=1,actions=inventory))
        for n,role in enumerate(reference.ROLES):
            start=n*200
            data=str(self.directory/("db" if n<2 else "fixture-db"))
            owner=dict(schema_version=1,kind="mongodb_container_process_observation",container_id=str(n+1)*64,process=dict(pid=100+n,start_ticks=500+n,exe_sha256="c"*64,gomaxprocs_unset=True),namespace_pid=1,hostname="ycsb-"+self.m["campaign_id"][:12]+"-"+role,data_dir=data,data_device=10,data_inode=21 if n<2 else 22,mounted_data_device=10,mounted_data_inode=21 if n<2 else 22,pid_namespace="pid:[100]",mount_namespace="mnt:[101]",network_namespace="net:[102]",observed_utc=ts(start))
            self.owners[role]=owner
            self.save(self.directory/(role+".identity.json"),owner)
            initial=self.inspect(owner,role,start,False)
            terminal=self.inspect(owner,role,start,True)
            self.save(self.directory/(role+".inspect.json"),initial)
            self.save(self.directory/(role+".terminal.json"),terminal)
            opening={name:self.docker(args,output,start) for name,args,output in (("create",reference.create_argv(self.m,role,data),owner["container_id"]+"\n"),("start",["start",owner["container_id"]],owner["container_id"]+"\n"),("inspect",["inspect",owner["container_id"]],json.dumps([initial])+"\n"))}
            self.save(self.directory/(role+".start.json"),dict(schema_version=1,container_id=owner["container_id"],actions=opening))
            self.control(role+"-ready",owner,"observe",start,start+1)
            self.control(role+"-before-close",owner,"observe",start+90,start+91)
            actions={}
            cid=owner["container_id"]
            for key,args,out,s in (("sigterm",["kill","--signal=SIGTERM",cid],cid+"\n",start+92),("wait",["wait",cid],"0\n",start+93),("terminal",["inspect",cid],json.dumps([terminal])+"\n",start+94)):
                actions[key]=self.docker(args,out,s)
            logs="".join(json.dumps(dict(id=id,t={"$date":ts(start+92)}))+"\n" for id in (4795901,8423404))
            actions["logs"]=self.docker(["logs",cid],logs,start+96)
            (self.directory/(role+".log")).write_text(logs)
            self.save(self.directory/(role+".close.json"),dict(schema_version=1,kind="mongodb_observed_graceful_process_exit",container_id=cid,process=owner["process"],signal="SIGTERM",forced_kill=False,close_return_observed=False,exit_code=0,observed_exit=result(start+92,start+95),actions=actions,terminal_sha256=c.digest(self.directory/(role+".terminal.json")),logs_sha256=c.digest(self.directory/(role+".log"))))
            action=self.docker(["rm",cid],cid+"\n",start+97)
            self.save(self.directory/(role+".removed.json"),dict(schema_version=1,container_id=cid,removed=True,action=action))
        for stem,start,end in (("load",10,20),("run_1",30,40)):
            self.save(self.directory/(stem+".command.json"),dict(schema_version=1,argv=reference.phase_argv(self.m,stem)))
            content=(ROOT/"testdata/ycsb_compare"/(stem+".out")).read_text()
            if stem=="load":content=content.replace("Count: 100000","Count: 64")
            (self.directory/(stem+".out")).write_text(content)
            phase_id=self.m["campaign_id"]+":"+stem
            receipt=dict(schema_version=1,phase_id=phase_id,client_build_id=self.m["client"]["build_id"],db="mongodb",phase="load" if stem=="load" else "run",declared_profile=reference.PROFILE,total_excludes_close=True,command=dict(attempted=True,success=True,error="",start_utc="0001-01-01T00:00:00Z",end_utc="0001-01-01T00:00:00Z",elapsed_ns=0),db_close=result(end-2,end-1),workload_close=result(end-1,end),native_flush=dict(applicable=False,flush_enabled=False))
            self.save(self.directory/(stem+".close.json"),receipt)
            self.launch(stem,start,end,"e"*64)
            self.change(stem+".launch.json",lambda x:x.update(phase_id=phase_id,timeout_seconds=600,outer_timeout_seconds=640))
            self.control(stem+"-owner",self.owners["server"],"observe",start-1,start-1)
        ident=dict(schema_version=1,source_id=b["source_commit"],fixture_id=self.m["campaign_id"]+":mongodb:random",profile=reference.PROFILE,target="mongodb",format="bson")
        fields={"field"+str(i):dict(type="binary",subtype=0,base64=base64.b64encode(b"x"*100).decode()) for i in range(10)}
        rows=[dict(id=k,fields=fields) for k in sorted(live.expected_keys(64))]
        for stage,start,end,role in (("loaded",22,23,"server"),("preclose",42,43,"server"),("reopened",210,211,"reopened")):
            stem="snapshot_"+stage
            self.save(self.directory/(stem+".json"),dict(ident,stage="random_"+stage,rows=rows,operations=[]))
            self.fixture_helper(stem,"snapshot",start,end,role,[stem+".json"],ident["fixture_id"])
        expected,observed=fixture("bson")
        exact_id=dict(ident,fixture_id=self.m["campaign_id"]+":mongodb:deterministic")
        expected.update(exact_id)
        for snap in observed:snap.update(exact_id)
        self.save(self.directory/"fixture.expected.json",expected)
        for name,snap in zip(("fixture.updated.json.load.json","fixture.updated.json","fixture.reopened.json"),observed):self.save(self.directory/name,snap)
        self.fixture_helper("fixture_write","write",410,420,"fixture-write",["fixture.expected.json","fixture.updated.json","fixture.updated.json.load.json"],exact_id["fixture_id"])
        self.fixture_helper("fixture_verify","verify",610,620,"fixture-reopened",["fixture.expected.json","fixture.reopened.json"],exact_id["fixture_id"])
        for stem,start,role,snapshot in (("random-checkpoint",44,"server","snapshot_preclose.json"),("fixture-checkpoint",422,"fixture-write","fixture.updated.json")):
            self.control(stem,self.owners[role],"checkpoint",start,start+1)
            self.save(self.directory/(stem+".binding.json"),dict(schema_version=1,container_id=self.owners[role]["container_id"],snapshot_sha256=c.digest(self.directory/snapshot),control_sha256=c.digest(self.directory/(stem+".json"))))
        self.save(self.out/"reference-complete.json",dict(schema_version=1,kind="mongodb_reference_completed_attempt",campaign_id=self.m["campaign_id"],manifest_sha256=c.digest(self.out/"manifest.json"),completed_utc=ts(1000)))

    @staticmethod
    def save(path,value):path.write_text(json.dumps(value,allow_nan=False)+"\n")
    def change(self,name,fn):
        path=self.directory/name;value=c.read(path);fn(value);self.save(path,value)
    def inspect(self,owner,role,start,terminal):
        b=self.m["server_builds"]["mongodb"]
        return dict(Id=owner["container_id"],Image=b["image_id"],RestartCount=0,Config=dict(Image=b["image_id"],Labels=dict(b["image_labels"],**{"gomap.ycsb.campaign":self.m["campaign_id"],"gomap.ycsb.role":role,"gomap.ycsb.target":"mongodb"}),Hostname=owner["hostname"],User="1000:1000",Entrypoint=["/usr/bin/mongod"],Cmd=["--dbpath","/data/db","--storageEngine","wiredTiger","--bind_ip_all","--port","27017"],Env=["MONGO_VERSION=8.2.7"]),HostConfig=dict(Runtime="runc",RestartPolicy=dict(Name="no"),AutoRemove=False,Privileged=False,Init=None,NetworkMode="bridge",PortBindings={"27017/tcp":[dict(HostIp="127.0.0.1",HostPort="27018")]}),Mounts=[dict(Type="bind",Source=owner["data_dir"]+suffix,Destination=dest,RW=True) for suffix,dest in (("","/data/db"),(".configdb","/data/configdb"))],State=dict(Running=not terminal,Pid=0 if terminal else owner["process"]["pid"],ExitCode=0,OOMKilled=False,Dead=False,Error="",StartedAt=ts(start),FinishedAt=ts(start+92) if terminal else "0001-01-01T00:00:00Z"))
    def launch(self,stem,start,end,exe="9"*64):
        out=self.directory/(stem+".out")
        if not out.exists():out.write_text("SYNTHETIC offline child\n")
        self.save(self.directory/(stem+".launch.json"),dict(schema_version=1,campaign_id=self.m["campaign_id"],process=dict(pid=1234,start_ticks=5678,exe_sha256=exe,gomaxprocs_unset=True),exit_code=0,timed_out=False,timeout_seconds=180,cleanup_timeout_seconds=10,outer_timeout_seconds=220,start_utc=ts(start),end_utc=ts(end),receipt_sha256=c.digest(self.directory/(stem+".close.json")) if (self.directory/(stem+".close.json")).exists() else None,output_sha256=c.digest(out)))
    def docker(self,args,output,start):
        self.ordinal+=1;stem=f"docker-{self.ordinal:04d}"
        self.save(self.directory/(stem+".command.json"),dict(schema_version=1,argv=[self.m["docker_binary"],*args]))
        (self.directory/(stem+".out")).write_text(output)
        self.launch(stem,start,start,"d"*64)
        return stem
    def control(self,stem,owner,mode,start,end):
        self.save(self.directory/(stem+".command.json"),dict(schema_version=1,argv=reference.control_argv(self.directory,stem,self.m,mode)))
        role=next(k for k,v in self.owners.items() if v==owner)
        b=self.m["server_builds"]["mongodb"]
        replies=dict(hello=dict(isWritablePrimary=True,topologyVersion=dict(processId={"$oid":str(reference.ROLES.index(role)+1)*24})),buildInfo=dict(version=b["version"],gitVersion=b["source_commit"]),serverStatus=dict(version=b["version"],process="mongod",pid={"$numberLong":"1"},host=owner["hostname"]+":27017",storageEngine=dict(name="wiredTiger",persistent=True)),getCmdLineOpts=dict(parsed=dict(storage=dict(dbPath="/data/db",engine="wiredTiger"),net=dict(bindIpAll=True,port={"$numberInt":"27017"}))))
        commands={p+n:dict(request={n:{"$numberInt":"1"}},reply=dict(reply,ok={"$numberDouble":"1.0"}),connection_id=self.m["address"]+"[-1]",result=result(start,start)) for p in ("before.","after.") for n,reply in replies.items()}
        if mode=="checkpoint":commands["checkpoint"]=dict(request=dict(fsync={"$numberInt":"1"},lock=False),reply=dict(ok={"$numberDouble":"1.0"}),connection_id=self.m["address"]+"[-1]",result=result(start,start))
        packet=dict(schema_version=1,kind="mongodb_reference_public_commands",mode=mode,address=self.m["address"],fixture_id=self.m["campaign_id"]+":"+stem,commands=commands)
        self.save(self.directory/(stem+".json"),packet)
        self.save(self.directory/(stem+".close.json"),dict(schema_version=1,kind="mongodb_reference_control_client_close",mode=mode,fixture_id=packet["fixture_id"],pid=1234,started_utc=ts(start),ended_utc=ts(end),result_error="",observed_sha256=c.digest(self.directory/(stem+".json")),disconnect=result(end,end)))
        self.launch(stem,start,end)
    def fixture_helper(self,stem,mode,start,end,role,artifacts,fixture_id):
        self.save(self.directory/(stem+".command.json"),dict(schema_version=1,argv=reference.fixture_argv(self.directory,stem,self.m,mode,stem.removeprefix("snapshot_") if mode=="snapshot" else None)))
        self.save(self.directory/(stem+".close.json"),dict(schema_version=1,kind="random_snapshot_client_close" if mode=="snapshot" else "deterministic_fixture_client_close",source_id=self.m["server_builds"]["mongodb"]["source_commit"],fixture_id=fixture_id,mode=mode,pid=1234,started_utc=ts(start),ended_utc=ts(end),result_error="",close_error="",checkpoint_attempted=False,checkpoint_succeeded=False,checkpoint=None,owner_checkpoint=None,artifacts_sha256={name:c.digest(self.directory/name) for name in artifacts}))
        self.launch(stem,start,end)
        self.control(stem+"-owner",self.owners[role],"observe",start-1,start-1)
    def rehash(self):
        for path in self.directory.glob("*.close.json"):
            value=c.read(path);stem=path.name.removesuffix(".close.json")
            if "observed_sha256" in value:value["observed_sha256"]=c.digest(self.directory/(stem+".json"))
            if "artifacts_sha256" in value:value["artifacts_sha256"]={k:c.digest(self.directory/k) for k in value["artifacts_sha256"]}
            if "terminal_sha256" in value:value["terminal_sha256"]=c.digest(self.directory/(stem+".terminal.json"));value["logs_sha256"]=c.digest(self.directory/(stem+".log"))
            self.save(path,value)
            launch=self.directory/(stem+".launch.json")
            if launch.exists():self.change(launch.name,lambda v:v.update(receipt_sha256=c.digest(path)))
        for stem in ("random-checkpoint","fixture-checkpoint"):
            self.change(stem+".binding.json",lambda v:v.update(control_sha256=c.digest(self.directory/(stem+".json"))))
        for path in self.directory.glob("docker-*.launch.json"):
            self.change(path.name,lambda v:v.update(output_sha256=c.digest(self.directory/(path.name.removesuffix(".launch.json")+".out"))))
    def reject(self,name,fn):
        original={p:p.read_bytes() for p in self.out.rglob("*") if p.is_file()}
        try:
            self.change(name,fn);self.rehash()
            with self.assertRaises((ValueError,KeyError,TypeError,OSError)):reference.validate(self.out)
        finally:
            for p,data in original.items():p.write_bytes(data)

    def test_complete_synthetic_packet_and_actual_validation_cli(self):
        report=reference.validate(self.out)
        self.assertEqual(report["status"],"validated-reference-lifecycle-evidence")
        for key in ("close_return_observed","cow_applicable","runtime_elf_attested","wal_replay_proven","power_loss_certified","performance_matrix_complete"):self.assertIs(report[key],False)
        child=subprocess.run([sys.executable,str(ROOT/"ycsb_mongodb_reference.py"),"validate",str(self.out)],capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode,0,child.stderr)
        env=dict(os.environ,PARSE_ONLY="true",BUILD="false",BUILD_GOYCSB="false",OUT_DIR=str(self.out),TARGETS="mongodb",PYTHON=sys.executable)
        child=subprocess.run(["bash",str(ROOT/"ycsb_compare_mongodb_treedb.sh")],env=env,capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode,0,child.stderr)
        self.assertEqual(c.read(self.out/"validation.json")["status"],"validated-reference-lifecycle-evidence")

    def test_identity_mount_image_runtime_and_public_build_fail_closed(self):
        for name,fn in (("server.identity.json",lambda v:v["process"].update(start_ticks=500.0)),("server.identity.json",lambda v:v.update(data_inode=21.0)),("server.identity.json",lambda v:v.update(namespace_pid=True)),("server.inspect.json",lambda v:v.update(Image="sha256:"+"0"*64)),("server.inspect.json",lambda v:v["Config"]["Labels"].update({"gomap.ycsb.campaign":"unrelated"})),("server.inspect.json",lambda v:v["Mounts"][0].update(Source="/unrelated")),("server.inspect.json",lambda v:v["HostConfig"].update(Init=0)),("server-ready.json",lambda v:v["commands"]["before.buildInfo"]["reply"].update(version="8.0.0")),("server-ready.json",lambda v:v["commands"]["before.serverStatus"]["reply"].update(pid={"$numberDouble":"1.0"})),("server-ready.json",lambda v:v["commands"]["before.getCmdLineOpts"]["reply"]["parsed"]["storage"].update(journal={"enabled":False}))):
            with self.subTest(name=name,fn=fn):self.reject(name,fn)

    def test_checkpoint_failure_request_owner_route_and_order(self):
        for stem in ("random-checkpoint","fixture-checkpoint"):
            for fn in (lambda v:v["commands"]["checkpoint"]["result"].update(success=False,error="actual fsync failure"),lambda v:v["commands"]["checkpoint"].update(reply=dict(ok={"$numberDouble":"0.0"})),lambda v:v["commands"]["checkpoint"]["request"].update(lock=True),lambda v:v["commands"]["checkpoint"].update(connection_id="127.0.0.1:27130[1]"),lambda v:v["commands"]["after.hello"]["reply"]["topologyVersion"].update(processId={"$oid":"0"*24}),lambda v:v["commands"]["checkpoint"]["result"].update(elapsed_ns=0.0)):
                with self.subTest(stem=stem,fn=fn):self.reject(stem+".json",fn)
        self.reject("random-checkpoint.launch.json",lambda v:v.update(start_utc=ts(41)))
        self.reject("random-checkpoint.json",lambda v:v["commands"]["checkpoint"]["result"].update(end_utc="2026-10-29T00:00:44.000000001Z",elapsed_ns=1))
        self.reject("fixture_verify.launch.json",lambda v:v.update(start_utc="2026-10-29T00:10:00.999999999Z"))

    def test_actual_close_logs_wait_exit_and_owned_removal_required(self):
        for name,fn in (("server.close.json",lambda v:v.update(close_return_observed=True)),("server.close.json",lambda v:v.update(forced_kill=True)),("server.close.json",lambda v:v.update(exit_code=False)),("server.terminal.json",lambda v:v["State"].update(ExitCode=137)),("server.terminal.json",lambda v:v["State"].update(OOMKilled=True)),("server.removed.json",lambda v:v.update(removed=False)),("server.close.json",lambda v:v["actions"].update(wait=v["actions"]["sigterm"])),("reopened.identity.json",lambda v:v.update(data_inode=999,mounted_data_inode=999)),("fixture-write.identity.json",lambda v:v.update(data_dir=self.owners["server"]["data_dir"]))):
            with self.subTest(name=name,fn=fn):self.reject(name,fn)
        original=(self.directory/"server.log").read_text()
        (self.directory/"server.log").write_text(original.replace('4795901','0'))
        self.rehash()
        with self.assertRaises(ValueError):reference.validate(self.out)

    def test_parent_failure_and_strict_json_cannot_be_requalified(self):
        for directory in (self.out,self.directory):
            for content in ("{broken",'{"error":"cancelled after cleanup"}'):
                path=directory/"failure.json";path.write_text(content)
                try:
                    with self.assertRaisesRegex(ValueError,"failed reference"):reference.validate(self.out)
                finally:path.unlink()
        path=self.directory/"server.identity.json";original=path.read_text()
        for text in (original.replace('"schema_version": 1','"schema_version": true'),original.replace('"pid": 100','"pid": NaN'),original.replace('"pid": 100','"pid": 100,"pid": 100')):
            path.write_text(text)
            with self.assertRaises((ValueError,KeyError)):reference.validate(self.out)
        path.write_text(original)

    def test_wrong_reference_settings_and_ambiguous_mixed_targets(self):
        path=self.out/"manifest.json";original=path.read_bytes()
        for fn in (lambda v:v.update(profile="command_wal_durable"),lambda v:v.update(targets=["mongodb","treedb_mongo"]),lambda v:v["write_concern"].update(journal_requested=True),lambda v:v["write_concern"].update(w=True),lambda v:v.update(address="127.0.0.1:27130?other=1"),lambda v:v["server_builds"]["mongodb"].update(version="8.0.0")):
            value=copy.deepcopy(self.m);fn(value);self.save(path,value)
            with self.assertRaises((ValueError,KeyError)):reference.validate(self.out)
        path.write_bytes(original)

    def test_raw_inventory_and_launch_arguments_are_bound(self):
        for name,fn in (("load.command.json",lambda v:v["argv"].__setitem__(4,"mongodb.url=mongodb://127.0.0.1:27130/ycsb?w=1")),("snapshot_preclose.command.json",lambda v:v["argv"].__setitem__(6,"127.0.0.1:27130")),("random-checkpoint.command.json",lambda v:v["argv"].__setitem__(4,"127.0.0.1:27130")),("server.start.json",lambda v:v["actions"].update(inspect=v["actions"]["start"]))):
            with self.subTest(name=name,fn=fn):self.reject(name,fn)
        path=self.out/"reference-image.json";original=path.read_bytes()
        value=c.read(path);value["Config"]["Env"].append("UNOBSERVED=copy")
        self.save(path,value)
        with self.assertRaisesRegex(ValueError,"actual Docker output"):reference.validate(self.out)
        path.write_bytes(original)

    def test_matching_malformed_process_and_result_copies_stay_invalid(self):
        originals={p:p.read_bytes() for p in self.out.rglob("*") if p.is_file()}
        for role in reference.ROLES:
            for field,value in (("pid",float(self.owners[role]["process"]["pid"])),("start_ticks",float(self.owners[role]["process"]["start_ticks"])),("gomaxprocs_unset",1)):
                with self.subTest(role=role,field=field):
                    for suffix in (".identity.json",".close.json"):self.change(role+suffix,lambda v:v["process"].update({field:value}))
                    self.rehash()
                    with self.assertRaises(ValueError):reference.validate(self.out)
                    for path,data in originals.items():path.write_bytes(data)
        for stem in ("random-checkpoint","fixture-checkpoint"):
            for field,value in (("attempted",1),("success",1),("elapsed_ns",0.0)):
                with self.subTest(stem=stem,field=field):
                    self.change(stem+".json",lambda v:v["commands"]["checkpoint"]["result"].update({field:value}))
                    self.rehash()
                    with self.assertRaises(ValueError):reference.validate(self.out)
                    for path,data in originals.items():path.write_bytes(data)

    def test_random_and_deterministic_values_independently_required(self):
        self.reject("snapshot_reopened.json",lambda v:v["rows"][0]["fields"]["field0"].update(base64=base64.b64encode(b"y"*100).decode()))
        self.reject("snapshot_preclose.json",lambda v:v["rows"][0]["fields"]["field0"].update(subtype=True))
        self.reject("fixture.reopened.json",lambda v:v["rows"][0]["fields"]["field0"].update(base64=base64.b64encode(b"y"*100).decode()))
        self.reject("fixture.updated.json",lambda v:v["operations"][0].update(acknowledged=False,error="ambiguous public write"))

    def test_cleanup_refuses_mismatched_container_before_signal(self):
        driver=reference.Docker(self.out,self.m["docker_binary"])
        owner=self.owners["server"]
        driver.owned={"server":dict(cid=owner["container_id"],name="synthetic",data=pathlib.Path(owner["data_dir"]))}
        wrong=c.read(self.directory/"server.inspect.json");wrong["Config"]["Labels"]["gomap.ycsb.campaign"]="someone-else"
        with mock.patch.object(driver,"emergency_command",return_value=json.dumps([wrong])) as emergency,mock.patch.object(driver,"command") as command:
            with self.assertRaisesRegex(ValueError,"unresolved owned reference custody"):driver.cleanup()
            command.assert_not_called()
            self.assertEqual(emergency.call_args_list,[mock.call("inspect",owner["container_id"])])

    def test_emergency_identity_recovery_requires_exact_intent_and_full_contract(self):
        owner=self.owners["server"];cid=owner["container_id"]
        for known,change in ((True,lambda v:v.update(Id="0"*64)),(False,lambda v:v.update(Name="/unrelated")),(False,lambda v:v["Mounts"][0].update(Source="/unrelated"))):
            driver=reference.Docker(self.out,self.m["docker_binary"])
            driver.owned={"server":dict(cid=cid if known else None,name="synthetic",data=pathlib.Path(owner["data_dir"]))}
            value=c.read(self.directory/"server.inspect.json");value["Name"]="/synthetic";change(value)
            with self.subTest(known=known,change=change),mock.patch.object(driver,"emergency_command",return_value=json.dumps([value])) as command:
                with self.assertRaisesRegex(ValueError,"unresolved owned reference custody"):driver.cleanup()
                self.assertEqual(command.call_args_list,[mock.call("inspect",cid if known else "synthetic")])
                self.assertIn("server",driver.owned)

    def test_forced_cleanup_is_owned_and_permanently_nonqualifying(self):
        driver=reference.Docker(self.out,self.m["docker_binary"])
        owner=self.owners["server"];cid=owner["container_id"]
        driver.owned={"server":dict(cid=cid,name="synthetic",data=pathlib.Path(owner["data_dir"]))}
        inspected=c.read(self.directory/"server.inspect.json");terminal=c.read(self.directory/"server.terminal.json");terminal["State"]["ExitCode"]=137
        with mock.patch.object(driver,"emergency_command",side_effect=[json.dumps([inspected]),cid,"137",json.dumps([terminal]),cid]) as command,mock.patch.object(driver,"command") as ordinary:
            driver.cleanup()
        self.assertEqual(command.call_args_list,[mock.call("inspect",cid),mock.call("kill","--signal=SIGKILL",cid),mock.call("wait",cid),mock.call("inspect",cid),mock.call("rm",cid)])
        ordinary.assert_not_called()
        self.assertEqual(driver.owned,{})
        self.save(self.directory/"server.cleanup.json",driver.cleanup_records["server"])
        self.assertIs(c.read(self.directory/"server.cleanup.json")["qualification_invalid"],True)
        with self.assertRaisesRegex(ValueError,"forced reference cleanup"):reference.validate(self.out)
        # The run's parent marker is retained before forced cleanup, and the
        # validator refuses it even after every owned container has been removed.
        self.save(self.out/"failure.json",dict(schema_version=1,error="injected reference failure"))
        with self.assertRaisesRegex(ValueError,"failed reference"):reference.validate(self.out)

    def test_incomplete_and_missing_or_malformed_completion_never_qualify(self):
        path=self.out/"reference-complete.json";original=path.read_bytes();path.unlink()
        with self.assertRaises(OSError):reference.validate(self.out)
        for value in (dict(schema_version=True),dict(schema_version=1,kind="mongodb_reference_completed_attempt",campaign_id=self.m["campaign_id"],manifest_sha256=c.digest(self.out/"manifest.json"),completed_utc=ts(1))):
            self.save(path,value)
            with self.assertRaises((ValueError,KeyError)):reference.validate(self.out)
        path.write_bytes(original)
        (self.out/"reference-incomplete.json").write_text("partial")
        with self.assertRaisesRegex(ValueError,"incomplete"):reference.validate(self.out)

    def injected_run_publication_failure(self,persistent):
        # Real run() failure orchestration, synthetic Docker answers only.
        out=self.out/("persistent-enospc" if persistent else "marker-enospc");out.mkdir();(out/"mongodb").mkdir()
        for source in self.out.iterdir():
            if source.is_file() and source.name!="reference-complete.json":shutil.copyfile(source,out/source.name)
        driver=reference.Docker(out,self.m["docker_binary"])
        owner=self.owners["server"];cid=owner["container_id"]
        data=pathlib.Path(owner["data_dir"])
        def original_failure(*args):
            driver.owned["server"]=dict(cid=cid,name="synthetic",data=data)
            raise ValueError("primary simulated operation failure")
        initial=c.read(self.directory/"server.inspect.json");terminal=c.read(self.directory/"server.terminal.json");terminal["State"]["ExitCode"]=137
        answers=[json.dumps([initial]),cid,"137",json.dumps([terminal]),cid]
        write=c.write_new
        def publication(path,value):
            if path.name=="failure.json" or (persistent and path.name!="reference-incomplete.json"):raise OSError(28,"injected ENOSPC")
            return write(path,value)
        hashes={self.m["docker_binary"]:"d"*64,self.m["client_binary"]:"e"*64,self.m["fixture"]["binary"]:"9"*64,self.m["control"]["binary"]:"9"*64}
        with mock.patch.object(reference,"Docker",return_value=driver),mock.patch.object(driver,"command",side_effect=original_failure),mock.patch.object(c,"exe_digest",side_effect=lambda p:hashes[p]),mock.patch.object(c,"write_new",side_effect=publication),mock.patch.object(c,"launch") as launch,mock.patch.object(reference,"emergency_cli",side_effect=answers) as emergency:
            with self.assertRaisesRegex(RuntimeError,"primary simulated operation failure.*secondary failures:.*ENOSPC"):
                reference.run(out,self.m["docker_binary"])
        launch.assert_not_called()
        self.assertEqual([call.args[1] for call in emergency.call_args_list],[["inspect",cid],["kill","--signal=SIGKILL",cid],["wait",cid],["inspect",cid],["rm",cid]])
        self.assertEqual(driver.owned,{})
        self.assertTrue((out/"reference-incomplete.json").exists())
        self.assertFalse((out/"reference-complete.json").exists())
        self.assertEqual(len(driver.emergency_actions),5)
        self.assertTrue(all(x["success"] for x in driver.emergency_actions))
        if not persistent:
            self.assertIn("primary simulated",c.read(out/"cleanup.failure.json")["error"])
            self.assertIn("ENOSPC"," ".join(c.read(out/"cleanup.failure.json")["secondary_errors"]))
            self.assertIs(c.read(out/"mongodb/server.cleanup.json")["qualification_invalid"],True)
        # Even a later complete packet copied into the output cannot erase the
        # failed attempt marker once the simulated disk becomes writable.
        for source in self.directory.iterdir():
            if source.is_file() and not (out/"mongodb"/source.name).exists():shutil.copyfile(source,out/"mongodb"/source.name)
        shutil.copyfile(self.out/"reference-complete.json",out/"reference-complete.json")
        with self.assertRaisesRegex(ValueError,"failed reference|forced reference cleanup|incomplete"):reference.validate(out)

    def test_parent_marker_publication_failure_still_cleans_owned_container(self):
        self.injected_run_publication_failure(False)

    def test_persistent_artifact_failure_uses_only_exact_owned_emergency_actions(self):
        self.injected_run_publication_failure(True)

    def test_emergency_child_is_bounded_pinned_and_reaped_without_artifacts(self):
        m=copy.deepcopy(self.m);m["server_builds"]["mongodb"]["docker_cli_sha256"]=c.exe_digest(sys.executable);m["deadlines"]["cleanup_seconds"]=0.2
        popen=subprocess.Popen
        for mode in ("success","hang","pin-failure"):
            children=[]
            def spawn(*args,**kwargs):
                child=popen(*args,**kwargs);children.append(child);return child
            with self.subTest(mode=mode),mock.patch.object(reference.subprocess,"Popen",side_effect=spawn),mock.patch.object(c,"write_new",side_effect=OSError(28,"no artifacts")) as write:
                code="import os,time;print('unset='+str('GOMAXPROCS' not in os.environ),flush=True)"+(";time.sleep(30)" if mode!="success" else "")
                if mode=="pin-failure":
                    with mock.patch.object(os,"pidfd_open",side_effect=OSError("injected pin failure")):
                        with self.assertRaisesRegex(OSError,"injected pin failure"):reference.emergency_cli(sys.executable,["-c",code],m)
                elif mode=="hang":
                    with self.assertRaisesRegex(ValueError,"deadline"):reference.emergency_cli(sys.executable,["-c",code],m)
                else:
                    with mock.patch.dict(os.environ,{"GOMAXPROCS":"123"}):self.assertEqual(reference.emergency_cli(sys.executable,["-c",code],m),"unset=True\n")
                write.assert_not_called()
                self.assertEqual(len(children),1);self.assertIsNotNone(children[0].returncode)
                with self.assertRaises(ChildProcessError):os.waitpid(children[0].pid,os.WNOHANG)


if __name__=="__main__":unittest.main()
