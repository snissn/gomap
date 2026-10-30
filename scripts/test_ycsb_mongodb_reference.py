"""Offline MongoDB evidence: synthetic lifecycle and retained created inspect.

Tests never invoke a container or product service.
"""
import base64
import copy
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import select
import shutil
import signal
import socket
import subprocess
import struct
import sys
import tempfile
import time
import unittest
from unittest import mock

import ycsb_compare_validate as c
import ycsb_compare_live as live
import ycsb_mongodb_reference as reference
import ycsb_mongodb_reference_prepare as preparation
from test_ycsb_correctness_fixture import fixture

ROOT=pathlib.Path(__file__).parent


class MongoLogUTC(unittest.TestCase):
    def test_actual_retained_close_logs_accept_explicit_zero_offset(self):
        evidence=c.read(ROOT/'testdata/ycsb_mongodb_reference/mongodb827-close-log.extract.json')
        self.assertIn('failed nonqualifying',evidence['classification'])
        for role,entries in evidence['roles'].items():
            self.assertEqual([v['id'] for v in entries],[4795901,8423404])
            for entry in entries:
                actual=entry['t']['$date']
                self.assertTrue(actual.endswith('+00:00'))
                with self.subTest(role=role,id=entry['id']):
                    self.assertEqual(reference.log_timestamp(actual),c.timestamp(actual[:-6]+'Z'))

    def test_z_and_zero_offset_preserve_exact_nanoseconds(self):
        for fraction in ('','.1','.123','.123456789'):
            stamp='2026-10-30T05:16:33'+fraction
            self.assertEqual(reference.log_timestamp(stamp+'Z'),c.timestamp(stamp+'Z'))
            self.assertEqual(reference.log_timestamp(stamp+'+00:00'),c.timestamp(stamp+'Z'))
        self.assertEqual(reference.log_timestamp('2026-10-30T05:16:33.000000001+00:00')-reference.log_timestamp('2026-10-30T05:16:33Z'),1)
        with self.assertRaises(ValueError):c.timestamp('2026-10-30T05:16:33+00:00')

    def test_nonzero_unknown_missing_or_malformed_zone_rejected(self):
        values=(None,True,1,1.0,'2026-10-30T05:16:33','2026-10-30T05:16:33+01:00','2026-10-30T05:16:33-10:00','2026-10-30T05:16:33-00:00','2026-10-30T05:16:33+0000','2026-10-30T05:16:33+00:00Z','2026-10-30T05:16:33z','2026-10-30T05:16:33.1234567890+00:00','2026-02-30T05:16:33+00:00','2026-10-30T25:16:33+00:00','2026-10-30T05:16:33+00:00\n','0001-01-01T00:00:00+00:00')
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):reference.log_timestamp(value)


class ReferenceBSONIntegers(unittest.TestCase):
    def test_declared_integer_width_boundaries(self):
        for kind,bits in (("$numberInt",32),("$numberLong",64)):
            for value in (-(1<<(bits-1)),0,(1<<(bits-1))-1):
                with self.subTest(kind=kind,value=value):
                    self.assertEqual(reference.ext_integer({kind:str(value)},"boundary"),value)

    def test_declared_integer_width_overflow_and_malformed_values(self):
        for kind,bits in (("$numberInt",32),("$numberLong",64)):
            invalid=(str(-(1<<(bits-1))-1),str(1<<(bits-1)),True,1,1.0,"01","1.0","NaN","Infinity")
            for value in invalid:
                with self.subTest(kind=kind,value=value):
                    with self.assertRaises(ValueError):reference.ext_integer({kind:value},"boundary")


class RetainedMongo827PublicIdentity(unittest.TestCase):
    """Selected actual replies from a failed attempt, not a valid lifecycle."""
    def setUp(self):
        fixtures=ROOT/"testdata/ycsb_mongodb_reference"
        for name,digest in c.read(fixtures/"mongodb827-provenance.json")["fixture_sha256"].items():
            self.assertEqual(c.digest(fixtures/name),digest)
        self.evidence=c.read(fixtures/"mongodb827-public-identity.extract.json")
        self.origin=c.read(fixtures/"mongodb827-git-origin.json")
        self.build=copy.deepcopy(self.evidence["historical_server_build"])
        self.build["git_version"]=self.origin["reported_git_version"]
        self.m=dict(address=self.evidence["address"],server_builds=dict(mongodb=self.build))

    def test_actual_before_and_after_match_separate_origin_and_default_port_host(self):
        reference.build(self.build)
        self.assertIn("GitOrigin-RevId: "+self.build["git_version"],self.origin["commit_message"])
        self.assertEqual(self.build["source_commit"],self.origin["public_commit"])
        self.assertNotEqual(self.build["source_commit"],self.build["git_version"])
        receipt=self.evidence["call_bounds"]
        bounds=c.timestamp(receipt["started_utc"]),c.timestamp(receipt["disconnect"]["start_utc"])
        for prefix in ("before.","after."):
            observed=reference.public_observation(self.evidence["packet"]["commands"],prefix,self.m,self.evidence["owner"],bounds)
            self.assertEqual(observed["journal_parameters"],dict(journalCommitInterval={"$numberInt":"100"},syncdelay={"$numberDouble":"60.0"}))

    def test_public_commit_and_reported_origin_are_not_interchangeable(self):
        for field,value in (("git_version",self.origin["public_commit"]),("git_version","0"*40),("git_version",True),("source_commit",self.origin["reported_git_version"])):
            build=copy.deepcopy(self.build);build[field]=value
            with self.subTest(field=field,value=value):
                with self.assertRaises(ValueError):reference.build(build)
        build=copy.deepcopy(self.build);del build["git_version"]
        with self.assertRaises((KeyError,ValueError)):reference.build(build)

    def test_actual_reply_negative_identity_and_endpoint_controls(self):
        receipt=self.evidence["call_bounds"]
        bounds=c.timestamp(receipt["started_utc"]),c.timestamp(receipt["disconnect"]["start_utc"])
        mutations=(("buildInfo","gitVersion",self.origin["public_commit"]),("buildInfo","gitVersion","0"*40),
                   ("buildInfo","version","8.2.8"),("serverStatus","host",self.evidence["owner"]["hostname"]+":27017"),
                   ("serverStatus","host","unrelated"),("serverStatus","pid",{"$numberLong":"2"}))
        for prefix in ("before.","after."):
            for command,field,value in mutations:
                commands=copy.deepcopy(self.evidence["packet"]["commands"])
                commands[prefix+command]["reply"][field]=value
                with self.subTest(prefix=prefix,command=command,field=field,value=value):
                    with self.assertRaises(ValueError):reference.public_observation(commands,prefix,self.m,self.evidence["owner"],bounds)
            commands=copy.deepcopy(self.evidence["packet"]["commands"])
            commands[prefix+"getCmdLineOpts"]["reply"]["parsed"]["net"]["port"]={"$numberInt":"27018"}
            with self.assertRaisesRegex(ValueError,"internal endpoint mismatch"):reference.public_observation(commands,prefix,self.m,self.evidence["owner"],bounds)


class Docker29RetainedInspect(unittest.TestCase):
    """Real created-container input; every command/state transition is offline."""
    def setUp(self):
        fixture=ROOT/"testdata/ycsb_mongodb_reference"
        for name,digest in c.read(fixture/"provenance.json")["fixture_sha256"].items():
            self.assertEqual(c.digest(fixture/name),digest)
        self.m=c.read(fixture/"docker29-bootstrap.manifest.json")
        self.value=c.read(fixture/"docker29-created.inspect.json")[0]
        self.assertNotIn("Init",self.value["HostConfig"])
        self.cid=self.value["Id"]
        self.data=next(x["Source"] for x in self.value["Mounts"] if x["Destination"]=="/data/db")
        temp=tempfile.TemporaryDirectory(prefix="offline-docker29-inspect-")
        self.addCleanup(temp.cleanup)
        self.out=pathlib.Path(temp.name);(self.out/"mongodb").mkdir()
        c.write_new(self.out/"manifest.json",self.m)

    def driver(self,known=True):
        driver=reference.Docker(self.out,self.m["docker_binary"])
        driver.owned={"server":dict(cid=self.cid if known else None,name=self.value["Name"].removeprefix("/"),data=pathlib.Path(self.data))}
        return driver

    def test_bounded_inspect_and_create_arguments_preserve_host_cpu_policy(self):
        m=copy.deepcopy(self.m);m['bounded_container']=True
        value=copy.deepcopy(self.value)
        host=value['HostConfig']
        host.update(Memory=12884901888,MemorySwap=12884901888,Ulimits=[dict(Name='cpu',Soft=449,Hard=450)],LogConfig=dict(Type='local',Config={'max-size':'16m','max-file':'2'}))
        reference.container_inspect(value,m,'server',self.data)
        argv=reference.create_argv(m,'server',self.data)
        for item in ('cpu=449:450','12884901888','--memory','--memory-swap','--log-driver','local'):
            self.assertIn(item,argv)
        for item in ('--cpus','--cpuset-cpus','--cpu-quota','--cpu-period'):
            self.assertNotIn(item,argv)
        for key,bad in [('Memory',1),('MemorySwap',1),('Ulimits',[dict(Name='cpu',Soft=450,Hard=451)]),('LogConfig',dict(Type='json-file',Config={})),('NanoCpus',1),('CpuQuota',1),('CpuPeriod',100000),('CpusetCpus','0'),('CpusetMems','0')]:
            altered=copy.deepcopy(value);altered['HostConfig'][key]=bad
            with self.subTest(key=key),self.assertRaises(ValueError):reference.container_inspect(altered,m,'server',self.data)

    def test_real_inspect_accepts_optional_init_for_normal_and_stopped_cleanup(self):
        for variant in ("omitted",None,False):
            for known in (True,False):
                value=copy.deepcopy(self.value)
                if variant!="omitted":value["HostConfig"]["Init"]=variant
                with self.subTest(init=variant,known=known):
                    driver=self.driver(known)
                    with mock.patch.object(driver,"inspect",return_value=value):
                        self.assertEqual(driver.checked(self.cid,"server",pathlib.Path(self.data)),value)
                    with mock.patch.object(driver,"emergency_command",side_effect=[json.dumps([value]),json.dumps([value]),self.cid]) as emergency,mock.patch.object(driver,"command") as ordinary:
                        driver.cleanup()
                    self.assertEqual(emergency.call_args_list,[mock.call("inspect",self.cid if known else value["Name"].removeprefix("/")),mock.call("inspect",self.cid),mock.call("rm",self.cid)])
                    ordinary.assert_not_called()
                    self.assertEqual(driver.owned,{})
                    self.assertIs(driver.cleanup_records["server"]["qualification_invalid"],True)

    def test_true_and_malformed_init_refuse_normal_and_emergency_actions(self):
        for init in (True,0,0.0,1,1.0,"false","",[],{}):
            value=copy.deepcopy(self.value);value["HostConfig"]["Init"]=init
            with self.subTest(init=init):
                driver=self.driver()
                with mock.patch.object(driver,"inspect",return_value=value):
                    with self.assertRaisesRegex(ValueError,"runtime/custody mismatch"):driver.checked(self.cid,"server",pathlib.Path(self.data))
                with mock.patch.object(driver,"emergency_command",return_value=json.dumps([value])) as emergency:
                    with self.assertRaisesRegex(ValueError,"unresolved owned reference custody"):driver.cleanup()
                self.assertEqual(emergency.call_args_list,[mock.call("inspect",self.cid)])
                self.assertIn("server",driver.owned)

    def test_emergency_rechecks_init_before_removal(self):
        for init in (True,0,0.0,"false",[],{}):
            terminal=copy.deepcopy(self.value);terminal["HostConfig"]["Init"]=init
            with self.subTest(init=init):
                driver=self.driver()
                with mock.patch.object(driver,"emergency_command",side_effect=[json.dumps([self.value]),json.dumps([terminal])]) as emergency:
                    with self.assertRaisesRegex(ValueError,"unresolved owned reference custody"):driver.cleanup()
                self.assertEqual(emergency.call_args_list,[mock.call("inspect",self.cid),mock.call("inspect",self.cid)])
                self.assertIn("server",driver.owned)

    def test_omitted_init_does_not_weaken_other_exact_owner_checks(self):
        mutations=(lambda v:v.update(Id="0"*64),lambda v:v.update(Image="sha256:"+"0"*64),lambda v:v["Config"]["Labels"].update({"gomap.ycsb.campaign":"other"}),lambda v:v["Mounts"][0].update(Source="/unrelated"),lambda v:v["HostConfig"].update(Privileged=True),lambda v:v["HostConfig"].update(AutoRemove=True),lambda v:v["HostConfig"].update(Runtime="other"))
        for mutate in mutations:
            value=copy.deepcopy(self.value);mutate(value)
            with self.subTest(mutation=mutate):
                driver=self.driver()
                with mock.patch.object(driver,"inspect",return_value=value):
                    with self.assertRaises(ValueError):driver.checked(self.cid,"server",pathlib.Path(self.data))
                with mock.patch.object(driver,"emergency_command",return_value=json.dumps([value])) as emergency:
                    with self.assertRaisesRegex(ValueError,"unresolved owned reference custody"):driver.cleanup()
                self.assertEqual(emergency.call_args_list,[mock.call("inspect",self.cid)])

    def test_synthetic_running_derivative_keeps_exact_kill_wait_terminal_remove(self):
        # The actual fixture never started. Only these mocked states are
        # synthetic; no PID/container is contacted or signaled by this test.
        running=copy.deepcopy(self.value);running["State"].update(Running=True,Pid=12345)
        terminal=copy.deepcopy(self.value);terminal["State"]["ExitCode"]=137
        driver=self.driver()
        with mock.patch.object(driver,"emergency_command",side_effect=[json.dumps([running]),self.cid,"137",json.dumps([terminal]),self.cid]) as emergency:
            driver.cleanup()
        self.assertEqual(emergency.call_args_list,[mock.call("inspect",self.cid),mock.call("kill","--signal=SIGKILL",self.cid),mock.call("wait",self.cid),mock.call("inspect",self.cid),mock.call("rm",self.cid)])
        self.assertEqual(driver.owned,{})
        self.assertIs(driver.cleanup_records["server"]["qualification_invalid"],True)


def ts(second):
    return (dt.datetime(2026,10,29,tzinfo=dt.timezone.utc)+dt.timedelta(seconds=second)).isoformat().replace("+00:00","Z")


def result(start,end):
    return dict(attempted=True,success=True,error="",start_utc=ts(start),end_utc=ts(end),elapsed_ns=(end-start)*1_000_000_000)


class ReferencePackets(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="synthetic-mongodb-reference-")
        self.addCleanup(self.temp.cleanup)
        self.out=pathlib.Path(self.temp.name);self.directory=self.out/"mongodb";self.directory.mkdir()
        self.m=dict(schema_version=1,kind=reference.KIND,lifecycle_contract=1,campaign_id="f"*32,targets=["mongodb"],profile=reference.PROFILE,formats={"mongodb":"bson"},write_concern=dict(w=1,journal_requested=None),recordcount=64,operationcount=10000,threadcount=16,run_repeats=1,uid=os.getuid(),gid=os.getgid(),address="127.0.0.1:27018",docker_binary="/synthetic/docker",client_binary="/synthetic/ycsb",reference_source_sha256=c.digest(reference.__file__),deadlines=dict(phase_seconds=600,helper_seconds=180,cleanup_seconds=10,phase_outer_seconds=640,helper_outer_seconds=220,readiness_seconds=60,outer_seconds=7200))
        b=dict(schema_version=1,kind="mongodb_image_build",image_id="sha256:"+"a"*64,repo_digest="mongo@sha256:"+"b"*64,os="linux",architecture="amd64",version="8.2.7",source_commit=reference.SUPPORTED_SOURCE["8.2.7"],git_version=reference.SUPPORTED_GIT_VERSION["8.2.7"],exe_sha256="c"*64,docker_cli_sha256="d"*64,runtime=dict(engine="synthetic",engine_commit="synthetic",containerd="synthetic",runc="synthetic",driver="overlay2",cgroup_version="2",default_runtime="runc"))
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
        return dict(Id=owner["container_id"],Image=b["image_id"],RestartCount=0,Config=dict(Image=b["image_id"],Labels=dict(b["image_labels"],**{"gomap.ycsb.campaign":self.m["campaign_id"],"gomap.ycsb.role":role,"gomap.ycsb.target":"mongodb"}),Hostname=owner["hostname"],User=f'{self.m["uid"]}:{self.m["gid"]}',Entrypoint=["/usr/bin/mongod"],Cmd=["--dbpath","/data/db","--storageEngine","wiredTiger","--bind_ip_all","--port","27017"],Env=["MONGO_VERSION=8.2.7"]),HostConfig=dict(Runtime="runc",RestartPolicy=dict(Name="no"),AutoRemove=False,Privileged=False,Init=None,NetworkMode="bridge",PortBindings={"27017/tcp":[dict(HostIp="127.0.0.1",HostPort="27018")]}),Mounts=[dict(Type="bind",Source=owner["data_dir"]+suffix,Destination=dest,RW=True) for suffix,dest in (("","/data/db"),(".configdb","/data/configdb"))],State=dict(Running=not terminal,Pid=0 if terminal else owner["process"]["pid"],ExitCode=0,OOMKilled=False,Dead=False,Error="",StartedAt=ts(start),FinishedAt=ts(start+92) if terminal else "0001-01-01T00:00:00Z"))
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
        replies=dict(hello=dict(isWritablePrimary=True,topologyVersion=dict(processId={"$oid":str(reference.ROLES.index(role)+1)*24})),buildInfo=dict(version=b["version"],gitVersion=b["git_version"]),serverStatus=dict(version=b["version"],process="mongod",pid={"$numberLong":"1"},host=owner["hostname"],storageEngine=dict(name="wiredTiger",persistent=True)),getCmdLineOpts=dict(parsed=dict(storage=dict(dbPath="/data/db",engine="wiredTiger"),net=dict(bindIpAll=True,port={"$numberInt":"27017"}))),getParameter=dict(journalCommitInterval={"$numberInt":"100"},syncdelay={"$numberDouble":"60.0"}))
        commands={p+n:dict(request=reference.observation_request(n),reply=dict(reply,ok={"$numberDouble":"1.0"}),connection_id=self.m["address"]+"[-1]",result=result(start,start)) for p in ("before.","after.") for n,reply in replies.items()}
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

    def test_all_synthetic_lifecycle_inspect_copies_can_omit_init(self):
        # Exercise the normal complete lifecycle validator, including raw
        # Docker inspect action copies and checked graceful-close snapshots.
        paths=list(self.directory.glob("*.inspect.json"))+list(self.directory.glob("*.terminal.json"))
        paths += [p.with_name(p.name.removesuffix(".command.json")+".out") for p in self.directory.glob("docker-*.command.json") if c.read(p)["argv"][1]=="inspect"]
        for path in paths:
            value=c.read(path)
            values=value if type(value) is list else [value]
            changed=False
            for item in values:
                if type(item) is dict and "HostConfig" in item:
                    item["HostConfig"].pop("Init",None);changed=True
            if changed:self.save(path,value)
        self.rehash()
        self.assertEqual(reference.validate(self.out)["status"],"validated-reference-lifecycle-evidence")

    def test_checkpoint_failure_request_owner_route_and_order(self):
        for stem in ("random-checkpoint","fixture-checkpoint"):
            for fn in (lambda v:v["commands"]["checkpoint"]["result"].update(success=False,error="actual fsync failure"),lambda v:v["commands"]["checkpoint"].update(reply=dict(ok={"$numberDouble":"0.0"})),lambda v:v["commands"]["checkpoint"]["request"].update(lock=True),lambda v:v["commands"]["checkpoint"].update(connection_id="127.0.0.1:27130[1]"),lambda v:v["commands"]["after.hello"]["reply"]["topologyVersion"].update(processId={"$oid":"0"*24}),lambda v:v["commands"]["checkpoint"]["result"].update(elapsed_ns=0.0)):
                with self.subTest(stem=stem,fn=fn):self.reject(stem+".json",fn)
        self.reject("random-checkpoint.launch.json",lambda v:v.update(start_utc=ts(41)))
        self.reject("random-checkpoint.json",lambda v:v["commands"]["checkpoint"]["result"].update(end_utc="2026-10-29T00:00:44.000000001Z",elapsed_ns=1))
        self.reject("fixture_verify.launch.json",lambda v:v.update(start_utc="2026-10-29T00:10:00.999999999Z"))

    def test_matching_public_copies_cannot_swap_origin_or_append_default_port(self):
        original={p:p.read_bytes() for p in self.out.rglob("*") if p.is_file()}
        for mutation in ("mirror-as-origin","hostname-port-suffix"):
            with self.subTest(mutation=mutation):
                try:
                    for path in self.directory.glob("*.json"):
                        packet=c.read(path)
                        if type(packet) is dict and packet.get("kind")=="mongodb_reference_public_commands":
                            for name,command in packet["commands"].items():
                                if mutation=="mirror-as-origin" and name.endswith("buildInfo"):command["reply"]["gitVersion"]=self.m["server_builds"]["mongodb"]["source_commit"]
                                if mutation=="hostname-port-suffix" and name.endswith("serverStatus"):command["reply"]["host"]+=":27017"
                            self.save(path,packet)
                    self.rehash()
                    with self.assertRaises(ValueError):reference.validate(self.out)
                finally:
                    for path,data in original.items():path.write_bytes(data)

    def test_resolved_journal_parameters_are_typed_observed_and_stable(self):
        for field,value in (("journalCommitInterval",True),("journalCommitInterval",{"$numberInt":"0"}),("journalCommitInterval",{"$numberDouble":"100.0"}),("syncdelay",{"$numberDouble":"NaN"}),("syncdelay",{"$numberDouble":"Infinity"}),("syncdelay",{"$numberDouble":"1e999"}),("syncdelay",{"$numberDouble":"-0.1"})):
            self.reject("server-ready.json",lambda v:v["commands"]["before.getParameter"]["reply"].update({field:value}))
        self.reject("random-checkpoint.json",lambda v:v["commands"]["after.getParameter"]["reply"].update(syncdelay={"$numberDouble":"61.0"}))
        self.reject("random-checkpoint.json",lambda v:[v["commands"][p+"getParameter"]["reply"].update(syncdelay={"$numberDouble":"61.0"}) for p in ("before.","after.")])
        self.reject("server-ready.json",lambda v:v["commands"].pop("before.getParameter"))

    def test_matching_public_parameter_copies_enforce_declared_bson_width(self):
        # Every public command copy and binding is updated together. Rejection
        # must follow BSON width, rather than a stale digest or unequal copy.
        original={p:p.read_bytes() for p in self.out.rglob("*") if p.is_file()}
        cases=(("$numberInt","2147483647",True),("$numberLong","2147483648",True),
               ("$numberInt","2147483648",False),("$numberLong","9223372036854775808",False))
        for kind,value,valid in cases:
            with self.subTest(kind=kind,value=value):
                try:
                    changed=0
                    for path in self.directory.glob("*.json"):
                        packet=c.read(path)
                        if type(packet) is dict and packet.get("kind")=="mongodb_reference_public_commands":
                            for command in packet["commands"].values():
                                if "getParameter" in command["request"]:
                                    command["reply"]["syncdelay"]={kind:value};changed+=1
                            self.save(path,packet)
                    self.assertGreater(changed,0)
                    self.rehash()
                    if valid:self.assertEqual(reference.validate(self.out)["status"],"validated-reference-lifecycle-evidence")
                    else:
                        with self.assertRaisesRegex(ValueError,"out-of-range BSON integer"):reference.validate(self.out)
                finally:
                    for path,data in original.items():path.write_bytes(data)

    def test_actual_close_logs_wait_exit_and_owned_removal_required(self):
        for name,fn in (("server.close.json",lambda v:v.update(close_return_observed=True)),("server.close.json",lambda v:v.update(forced_kill=True)),("server.close.json",lambda v:v.update(exit_code=False)),("server.terminal.json",lambda v:v["State"].update(ExitCode=137)),("server.terminal.json",lambda v:v["State"].update(OOMKilled=True)),("server.removed.json",lambda v:v.update(removed=False)),("server.close.json",lambda v:v["actions"].update(wait=v["actions"]["sigterm"])),("reopened.identity.json",lambda v:v.update(data_inode=999,mounted_data_inode=999)),("fixture-write.identity.json",lambda v:v.update(data_dir=self.owners["server"]["data_dir"]))):
            with self.subTest(name=name,fn=fn):self.reject(name,fn)
        original=(self.directory/"server.log").read_text()
        (self.directory/"server.log").write_text(original.replace('4795901','0'))
        self.rehash()
        with self.assertRaises(ValueError):reference.validate(self.out)

    def test_matching_raw_close_log_timestamps_keep_utc_and_lifetime_bounds(self):
        original={p:p.read_bytes() for p in self.out.rglob('*') if p.is_file()}
        try:
            for role in reference.ROLES:
                close=c.read(self.directory/(role+'.close.json'))
                log=self.directory/(role+'.log')
                contents=log.read_text().replace('Z"','+00:00"')
                log.write_text(contents)
                (self.directory/(close['actions']['logs']+'.out')).write_text(contents)
            self.rehash()
            self.assertEqual(reference.validate(self.out)['status'],'validated-reference-lifecycle-evidence')
            accepted={p:p.read_bytes() for p in self.out.rglob('*') if p.is_file()}
            for role in reference.ROLES:
                for timestamp in ('2026-01-01T00:00:00+01:00','2026-01-01T00:00:00','2026-01-01T00:00:00-00:00'):
                    for path,data in accepted.items():path.write_bytes(data)
                    log=self.directory/(role+'.log');entries=[reference.loads(line) for line in log.read_text().splitlines()]
                    entries[0]['t']['$date']=timestamp
                    contents=''.join(json.dumps(v)+'\n' for v in entries)
                    log.write_text(contents)
                    close=c.read(self.directory/(role+'.close.json'))
                    (self.directory/(close['actions']['logs']+'.out')).write_text(contents)
                    self.rehash()
                    with self.subTest(role=role,timestamp=timestamp):
                        with self.assertRaises(ValueError):reference.validate(self.out)
                # Valid UTC format cannot hide a 1ns log before its signal or
                # reverse close/completion ordering within one microsecond.
                base=ts(reference.ROLES.index(role)*200+92)
                before=ts(reference.ROLES.index(role)*200+91).replace('Z','.999999999+00:00')
                for times in ((base.replace('Z','.000000001+00:00'),base.replace('Z','.000000000+00:00')),
                              (before,base.replace('Z','+00:00')),
                              (base.replace('Z','+00:00'),base.replace('Z','.000000001+00:00'))):
                    for path,data in accepted.items():path.write_bytes(data)
                    log=self.directory/(role+'.log');entries=[reference.loads(line) for line in log.read_text().splitlines()]
                    for entry,value in zip(entries,times):entry['t']['$date']=value
                    contents=''.join(json.dumps(v)+'\n' for v in entries);log.write_text(contents)
                    close=c.read(self.directory/(role+'.close.json'))
                    (self.directory/(close['actions']['logs']+'.out')).write_text(contents);self.rehash()
                    with self.subTest(role=role,times=times):
                        with self.assertRaises(ValueError):reference.validate(self.out)
        finally:
            for path,data in original.items():path.write_bytes(data)

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

    def preparation_inputs(self):
        # Synthetic support packet and tool metadata; no executable below is
        # invoked except the existing public validation Python CLI tests.
        binary=str(pathlib.Path(sys.executable).resolve());actual=c.exe_digest(binary)
        replacements={"/synthetic/docker":binary,"/synthetic/ycsb":binary,"/synthetic/fixture":binary,"/synthetic/control":binary,"d"*64:actual,"e"*64:actual,"9"*64:actual,"operationcount=10000":"operationcount=1000"}
        def convert(value):
            if type(value) is dict:return {key:convert(child) for key,child in value.items()}
            if type(value) is list:return [convert(child) for child in value]
            return replacements.get(value,value) if type(value) is str else value
        for path in self.out.rglob("*.json"):self.save(path,convert(c.read(path)))
        m=c.read(self.out/"manifest.json");m["operationcount"]=1000
        for key,name,source in (("fixture","fixture-source.go","ycsb_correctness_fixture.go"),("control","reference-control-source.go","ycsb_mongodb_reference_client.go")):
            (self.out/name).write_bytes((ROOT/source).read_bytes());m[key]["source_sha256"]=c.digest(self.out/name)
        self.save(self.out/"manifest.json",m)
        self.save(self.out/"client-build.json",m["client"]);self.save(self.out/"reference-build.json",m["server_builds"]["mongodb"])
        path=self.directory/"run_1.out"
        read_count=next(row["count"] for row in c.parse_log(path) if row["operation"]=="READ")//10
        counts={"READ":read_count,"UPDATE":1000-read_count,"TOTAL":1000}
        path.write_text("SYNTHETIC offline tiny support counts; no MongoDB workload executed\n"+re.sub(r"^(READ|UPDATE|TOTAL)(.*?Count: )([0-9]+)",lambda x:x[1]+x[2]+str(counts[x[1]]),path.read_text(),flags=re.MULTILINE))
        self.rehash()
        self.change("run_1.launch.json",lambda v:v.update(output_sha256=c.digest(path)))
        completion=c.read(self.out/"reference-complete.json");completion["manifest_sha256"]=c.digest(self.out/"manifest.json");self.save(self.out/"reference-complete.json",completion)
        output=tempfile.TemporaryDirectory(prefix="synthetic-reference-preparation-");self.addCleanup(output.cleanup)
        config=dict(schema_version=1,kind="mongodb_reference_preparation_request",support_dir=str(self.out),mongodb_addr=m["address"],client_source_dir=str(self.out),client_binary=binary,client_build_manifest=str(self.out/"client-build.json"),fixture_binary=binary,control_binary=binary,docker_binary=binary,go_binary=binary,recordcount=64,operationcount=10000,threadcount=16,run_repeats=1,deadlines=m["deadlines"])
        def metadata(argv):
            if argv[0]=="git":
                return (c.PRODUCER+"\n").encode() if "rev-parse" in argv else b""
            self.assertEqual(argv[:3],[binary,"version","-m"])
            return b"SYNTHETIC offline Go buildinfo; no executable invoked\n"
        return config,pathlib.Path(output.name)/"prepared",metadata

    def bounded_support(self,common_load=False):
        # Extend the synthetic lifecycle with the actual wire-validator contract.
        # No socket, Docker command, service or product executable is used.
        import ycsb_reference_wire as wire
        import ycsb_public_sentinel as sentinel
        m=c.read(self.out/'manifest.json');m['bounded_container']=True
        def frames(name,request,reply):
            directory=self.directory/name;directory.mkdir()
            for kind,rid,to,doc in [('request',1,0,request),('response',2,1,reply)]:
                body=b'\0'*5+wire.document(doc)
                (directory/('01-'+kind+'.bin')).write_bytes(struct.pack('<iiii',len(body)+16,rid,to,2013)+body)
        for n,role in enumerate(reference.ROLES):
            owner=self.owners[role];start=n*200
            for suffix in ('.inspect.json','.terminal.json'):
                self.change(role+suffix,lambda v:v['HostConfig'].update(Memory=12884901888,MemorySwap=12884901888,Ulimits=[dict(Name='cpu',Soft=449,Hard=450)],LogConfig=dict(Type='local',Config={'max-size':'16m','max-file':'2'})))
            opening=c.read(self.directory/(role+'.start.json'))['actions']
            self.save(self.directory/(opening['create']+'.command.json'),dict(schema_version=1,argv=[m['docker_binary'],*reference.create_argv(m,role,owner['data_dir'])]))
            for action,suffix in ((opening['inspect'],'.inspect.json'),(c.read(self.directory/(role+'.close.json'))['actions']['terminal'],'.terminal.json')):
                (self.directory/(action+'.out')).write_text(json.dumps([c.read(self.directory/(role+suffix))])+'\n')
            common=dict(schema_version=1,campaign_id=m['campaign_id'],role=role,process=owner['process'])
            group='/sys/fs/cgroup/docker/'+owner['container_id']
            policy=dict(process=owner['process'],leader_affinity=[0],cgroup=dict(membership='0::/docker/'+owner['container_id'],ancestors=[dict(path=group,cpu_max='max 100000')]))
            self.save(self.directory/(role+'.resource-observation.json'),dict(common,limits='Max cpu time 449 450 seconds',members=[owner['process']['pid']],cgroup=group,cgroup_device=27,cgroup_inode=n+1,memory_max='12884901888',memory_swap_max='0',cpu_policy=policy))
            self.save(self.directory/(role+'.default-concern.json'),dict(common,start_utc=ts(start+2),end_utc=ts(start+3),reply=dict(ok=1)))
            frames(role+'.default-concern-frames',{'getDefaultRWConcern':1,'$db':'admin'},dict(ok=1))
        if common_load:
            proof={}
            for name in ('synthetic-control-source','synthetic-control-result','synthetic-control-review'):
                path=self.out/name;path.write_text('SYNTHETIC offline control binding');proof[str(path)]=c.digest(path)
            m.update(profile='mongodb_w1_journaled',acknowledgement_class='mongodb_w1_journaled',fixture_profile=reference.PROFILE)
            m['write_concern']['journal_requested']=True
            m['common_load']=dict(comparison_class='same-request-jtrue',address=m['address'],write_concern=dict(w=1,journal_requested=None),reviewed_control_a=dict(scope='bson-binary-and-pointer-bearing-prefix',reviewed=True,files=proof))
        self.save(self.out/'manifest.json',m)
        if common_load:
            for stem in ('load','run_1'):
                argv=reference.phase_argv(m,stem)
                self.save(self.directory/(stem+'.command.json'),dict(schema_version=1,argv=argv))
                self.change(stem+'.close.json',lambda v:v.update(declared_profile=m['profile']))
                sentinel.phase_command(self.directory,stem,m,argv)
            frames('public-sentinel-frames',sentinel.command(m),dict(ok=1,n=1))
            directory=self.directory/'public-sentinel-frames'
            self.save(self.directory/'public-sentinel.json',dict(schema_version=1,campaign_id=m['campaign_id'],manifest_sha256=c.digest(self.out/'manifest.json'),process=self.owners['server']['process'],address=m['address'],start_utc=ts(24),end_utc=ts(25),request=sentinel.command(m),reply=dict(ok=1,n=1),request_sha256=c.digest(directory/'01-request.bin'),response_sha256=c.digest(directory/'01-response.bin')))
        self.rehash()
        completion=c.read(self.out/'reference-complete.json');completion['manifest_sha256']=c.digest(self.out/'manifest.json');self.save(self.out/'reference-complete.json',completion)
        reference.validate(self.out)

    def test_bounded_nested_frames_revalidate_after_preparation(self):
        config,out,metadata=self.preparation_inputs();self.bounded_support()
        with mock.patch.object(preparation,'command',side_effect=metadata),mock.patch.object(reference,'emergency_cli') as docker,mock.patch.object(c,'runtime_client_preflight'):
            preparation.prepare(config,out)
            docker.assert_not_called()
        preparation.verify_prepared(out,c.read(out/'manifest.json'))

    def test_common_load_nested_frames_revalidate_after_preparation(self):
        config,out,metadata=self.preparation_inputs();self.bounded_support(common_load=True)
        config.update(journal_requested=True,bounded_container=True,common_load=c.read(self.out/'manifest.json')['common_load'])
        with mock.patch.object(preparation,'command',side_effect=metadata),mock.patch.object(reference,'emergency_cli') as docker,mock.patch.object(c,'runtime_client_preflight'):
            preparation.prepare(config,out)
            docker.assert_not_called()
        preparation.verify_prepared(out,c.read(out/'manifest.json'))

    def test_preparation_copies_nested_frames_hashes_and_excludes_databases(self):
        config,out,metadata=self.preparation_inputs()
        expected={}
        for dirname in [role+'.default-concern-frames' for role in reference.ROLES]+['public-sentinel-frames']:
            frames=self.directory/dirname;frames.mkdir()
            for name,data in [('01-request.bin',b'SYNTHETIC raw request'),('01-response.bin',b'SYNTHETIC raw response')]:
                path=frames/name;path.write_bytes(data)
                expected['reference-support/mongodb/'+dirname+'/'+name]=data
        for dirname in ('db','fixture-db','db.configdb','fixture-db.configdb','unrecognized-frames'):
            directory=self.directory/dirname;directory.mkdir()
            (directory/'01-request.bin').write_bytes(b'DO NOT COPY database or unrecognized directory')
        with mock.patch.object(preparation,'command',side_effect=metadata),mock.patch.object(reference,'emergency_cli') as docker,mock.patch.object(c,'runtime_client_preflight'):
            preparation.prepare(config,out)
            docker.assert_not_called()
        m=c.read(out/'manifest.json');receipt=preparation.verify_prepared(out,m)
        for name,data in expected.items():
            self.assertEqual((out/name).read_bytes(),data)
            self.assertEqual(receipt['artifacts_sha256'][name],hashlib.sha256(data).hexdigest())
            (out/name).write_bytes(data+b'changed')
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'preparation artifact changed'):
                preparation.verify_prepared(out,m)
            (out/name).write_bytes(data)
        for dirname in ('db','fixture-db','db.configdb','fixture-db.configdb','unrecognized-frames'):
            self.assertFalse((out/'reference-support/mongodb'/dirname).exists())
        shutil.rmtree(self.out)
        preparation.verify_prepared(out,m,check_current_sources=True)

    def test_nested_frame_symlinks_are_rejected(self):
        frames=self.directory/'server.default-concern-frames';frames.mkdir()
        link=frames/'01-request.bin';link.symlink_to(self.out/'manifest.json')
        with self.assertRaisesRegex(ValueError,'linked support evidence'):
            preparation.evidence_files(self.out)
        link.unlink();frames.rmdir();frames.symlink_to(self.directory,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'linked support evidence'):
            preparation.evidence_files(self.out)

    def test_preparation_preserves_nondefault_task_custody(self):
        for uid,gid in ((0,0),(2001,3002)):
            with self.subTest(uid=uid,gid=gid),mock.patch.object(os,'getuid',return_value=uid),mock.patch.object(os,'getgid',return_value=gid):
                packet=ReferencePackets();self.addCleanup(packet.doCleanups);packet.setUp()
                config,out,metadata=packet.preparation_inputs()
                with mock.patch.object(preparation,'command',side_effect=metadata),mock.patch.object(reference,'emergency_cli') as docker,mock.patch.object(c,'runtime_client_preflight'):
                    preparation.prepare(config,out)
                    docker.assert_not_called()
                m=c.read(out/'manifest.json')
                self.assertEqual((m['uid'],m['gid']),(uid,gid))
                preparation.verify_prepared(out,m,check_current_sources=True)

    def test_preparation_unknown_runtime_elf_fails_before_metadata_or_docker(self):
        config,out,metadata=self.preparation_inputs()
        with mock.patch.object(preparation,'command',side_effect=metadata) as commands,mock.patch.object(reference,'emergency_cli') as docker:
            with self.assertRaisesRegex(ValueError,'client runtime'):preparation.prepare(config,out)
            commands.assert_not_called();docker.assert_not_called()
        self.assertFalse(out.exists())

    def test_journaled_preparation_projects_new_request_without_relabeling_old_support(self):
        config,out,metadata=self.preparation_inputs()
        config.update(journal_requested=True,bounded_container=True)
        with mock.patch.object(preparation,'command',side_effect=metadata),mock.patch.object(reference,'emergency_cli') as docker,mock.patch.object(c,'runtime_client_preflight'):
            preparation.prepare(config,out)
            docker.assert_not_called()
        m=c.read(out/'manifest.json')
        self.assertEqual(m['profile'],'mongodb_w1_journaled')
        self.assertEqual(m['acknowledgement_class'],m['profile'])
        self.assertEqual(m['support_write_concern'],dict(w=1,journal_requested=None))
        self.assertEqual(m['write_concern'],dict(w=1,journal_requested=True))
        self.assertEqual(m['fixture_profile'],reference.PROFILE)
        self.assertTrue(m['bounded_container'])
        preparation.verify_prepared(out,m,check_current_sources=True)
        self.assertIn('mongodb.url=mongodb://'+m['address']+'/ycsb?w=1&journal=true',reference.phase_argv(m,'run_1'))
        for field,value in [('support_write_concern',dict(w=1,journal_requested=True)),('fixture_profile','fake'),('acknowledgement_class','other')]:
            altered=copy.deepcopy(m);altered[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):preparation.verify_prepared(out,altered)

    def test_ack_class_projection_preserves_legacy_fixture_and_rejects_mismatch(self):
        reference.manifest(self.m)
        for journal in (None,True):
            m=copy.deepcopy(self.m);m.update(profile=reference.ack_profile(journal),acknowledgement_class=reference.ack_profile(journal),fixture_profile=reference.PROFILE)
            m['write_concern']['journal_requested']=journal
            reference.manifest(m)
            self.assertEqual(reference.fixture_manifest(m)['profile'],reference.PROFILE)
            for key,value in [('profile',reference.PROFILE),('acknowledgement_class','other'),('fixture_profile','fake')]:
                with self.subTest(journal=journal,key=key),self.assertRaises(ValueError):reference.manifest(dict(m,**{key:value}))
        self.m['write_concern']['journal_requested']=True
        with self.assertRaises(ValueError):reference.manifest(self.m)

    def test_offline_preparation_binds_full_support_without_invoking_services(self):
        config,out,metadata=self.preparation_inputs()
        with mock.patch.object(preparation,"command",side_effect=metadata) as commands,mock.patch.object(reference,"emergency_cli") as docker,mock.patch.object(c,"runtime_client_preflight") as qualification:
            report=preparation.prepare(config,out)
            docker.assert_not_called()
            qualification.assert_called_once_with(c.read(self.out/'client-build.json'))
        self.assertIs(report["activation_authorized"],False)
        self.assertTrue(all(call.args[0][0] in ("git",config["go_binary"]) for call in commands.call_args_list))
        self.assertTrue(all(call.args[0][1:3]==["version","-m"] for call in commands.call_args_list if call.args[0][0]!= "git"))
        m=c.read(out/"manifest.json")
        # Relocated evidence is independently usable after original artifacts
        # disappear, without rewriting their actual absolute output argv.
        shutil.rmtree(self.out)
        preparation.verify_prepared(out,m,check_current_sources=True)
        support=c.read(out/"reference-support.json")
        self.assertEqual(support["observations"]["server"]["buildInfo"]["version"],"8.2.7")
        self.assertIs(support["runtime_elf_attested"],False)
        self.assertEqual(m["address"],config["mongodb_addr"])
        self.assertFalse((out/"reference-support/mongodb/db").exists())
        self.assertEqual(list((out/"mongodb").iterdir()),[])

    def test_maintained_shell_offline_preparation_binds_actual_entry_source(self):
        config,out,_=self.preparation_inputs()
        request=out.parent/'request.json';self.save(request,config)
        metadata=out.parent/'metadata.jsonl';wrapper=out.parent/'offline-python'
        # Real shell/CLI dispatch and preparation; only Git/Go metadata is
        # synthetic. No Docker, public helper or product executable is run.
        wrapper.write_text('#!'+sys.executable+'\n'+'''import json,os,pathlib,sys
sys.path.insert(0,'''+repr(str(ROOT))+''')
import ycsb_mongodb_reference_prepare as preparation
assert 'GOMAXPROCS' not in os.environ
def metadata(argv):
    with pathlib.Path('''+repr(str(metadata))+''').open('a') as stream:stream.write(json.dumps(argv)+'\\n')
    if argv[0]=='git':return ('''+repr(c.PRODUCER+'\n')+''').encode() if 'rev-parse' in argv else b''
    assert argv[:3]==['''+repr(config['go_binary'])+''','version','-m']
    return b'SYNTHETIC offline Go metadata only\\n'
preparation.command=metadata
# Explicit synthetic Python-ELF qualification double; never product evidence.
def synthetic_runtime(build):
    assert build['exe_sha256']==preparation.c.exe_digest(sys.executable)
preparation.c.runtime_client_preflight=synthetic_runtime
sys.argv=sys.argv[1:]
raise SystemExit(preparation.main())
''');wrapper.chmod(0o700)
        with socket.socket() as unrelated:
            unrelated.bind(('127.0.0.1',0));unrelated.listen()
            environment=dict(os.environ,PYTHON=str(wrapper),GOMAXPROCS='123',
                OUT_DIR=str(out.parent/'must-not-create'),TARGETS='treedb_native\nmongodb',
                BUILD='true',BUILD_GOYCSB='true',PARSE_ONLY='true',
                TREEDB_MONGO_ADDR='127.0.0.1:'+str(unrelated.getsockname()[1]),MONGODB_ADDR='untrusted-environment')
            result=subprocess.run(['bash',str(ROOT/'ycsb_compare_mongodb_treedb.sh'),'--prepare-mongodb-reference',str(request),str(out)],env=environment,capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertFalse(select.select([unrelated],[],[],0)[0])
        self.assertFalse(pathlib.Path(environment['OUT_DIR']).exists())
        self.assertEqual(json.loads(result.stdout)['status'],'prepared-reference-inputs-not-activated')
        self.assertIs(json.loads(result.stdout)['activation_authorized'],False)
        calls=[json.loads(line) for line in metadata.read_text().splitlines()]
        self.assertTrue(all(argv[0]=='git' or argv[:3]==[config['go_binary'],'version','-m'] for argv in calls))
        manifest=c.read(out/'manifest.json');preparation.verify_prepared(out,manifest,check_current_sources=True)
        source=out/'preparation-sources/ycsb_compare_mongodb_treedb.sh'
        self.assertEqual(c.digest(source),c.digest(ROOT/source.name))
        source.write_bytes(source.read_bytes()+b'\n# changed entry\n')
        with mock.patch.object(reference,'Docker') as docker:
            with self.assertRaisesRegex(ValueError,'preparation artifact changed'):reference.run(out,manifest['docker_binary'])
            docker.assert_not_called()

    def test_maintained_shell_preparation_refuses_bad_arguments_and_failed_proof(self):
        config,out,_=self.preparation_inputs()
        request=out.parent/'request.json';self.save(request,config)
        marker=out.parent/'unexpected-python';wrapper=out.parent/'must-not-run'
        wrapper.write_text('#!/bin/sh\ntouch '+str(marker)+'\nexit 97\n');wrapper.chmod(0o700)
        for args in ([],[str(request)],[str(request),str(out),'extra'],[str(request),'relative-output']):
            result=subprocess.run(['bash',str(ROOT/'ycsb_compare_mongodb_treedb.sh'),'--prepare-mongodb-reference',*args],env=dict(os.environ,PYTHON=str(wrapper)),capture_output=True,text=True,timeout=5)
            self.assertEqual(result.returncode,2,result.stderr)
            self.assertIn('requires REQUEST.json and fresh absolute OUT',result.stderr)
            self.assertFalse(marker.exists());self.assertFalse(out.exists())
        self.save(self.out/'failure.json',dict(error='retained failed support'))
        result=subprocess.run(['bash',str(ROOT/'ycsb_compare_mongodb_treedb.sh'),'--prepare-mongodb-reference',str(request),str(out)],env=dict(os.environ,PYTHON=sys.executable),capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,3,result.stderr)
        self.assertIn('failed reference',result.stderr)
        self.assertFalse(out.exists())

    def test_preparation_missing_support_wrong_endpoint_and_changed_raw_build_fail_closed(self):
        config,out,metadata=self.preparation_inputs()
        for change in (lambda v:v.update(support_dir=str(out)),lambda v:v.update(mongodb_addr="127.0.0.1:27130")):
            bad=copy.deepcopy(config);change(bad)
            with mock.patch.object(preparation,"command",side_effect=metadata) as commands:
                with self.assertRaises((ValueError,OSError)):preparation.prepare(bad,out)
                commands.assert_not_called();self.assertFalse(out.exists())
        self.change("server-ready.json",lambda v:v["commands"]["before.buildInfo"]["reply"].update(gitVersion="0"*40));self.rehash()
        with mock.patch.object(preparation,"command",side_effect=metadata) as commands:
            with self.assertRaises(ValueError):preparation.prepare(config,out)
            commands.assert_not_called();self.assertFalse(out.exists())

    def test_prepared_artifact_and_manifest_changes_fail_before_launch(self):
        config,out,metadata=self.preparation_inputs()
        with mock.patch.object(preparation,"command",side_effect=metadata),mock.patch.object(c,"runtime_client_preflight"):preparation.prepare(config,out)
        m=c.read(out/"manifest.json")
        for name in ("reference-support/mongodb/server-ready.json","reference-support.json","client.patch","preparation-sources/ycsb_compare_validate.py"):
            path=out/name;original=path.read_bytes();path.write_bytes(original+b" ")
            with self.subTest(name=name),mock.patch.object(reference,"Docker") as docker:
                with self.assertRaisesRegex(ValueError,"preparation artifact changed"):reference.run(out,m["docker_binary"])
                docker.assert_not_called()
            path.write_bytes(original)
        changed=dict(m,address="127.0.0.1:27130")
        with self.assertRaisesRegex(ValueError,"manifest fields changed"):preparation.verify_prepared(out,changed)
        path=out/"reference-preparation.json";record=c.read(path);record["support_evidence_root"]="/unrelated-original-output"
        self.save(path,record)
        changed=dict(m,preparation_sha256=c.digest(path))
        with self.assertRaisesRegex(ValueError,"argv/address mismatch"):preparation.verify_prepared(out,changed)

    def test_preparation_sigterm_reaps_actual_slow_metadata_child_and_restores_handler(self):
        # Inject only the metadata stage into the real preparation main. Both
        # controller and slow metadata child are actual Python ELF processes;
        # no Docker, service, Go build or product executable is invoked.
        config=self.out/"signal-config.json";self.save(config,{})
        out=self.out/"interrupted-preparation";marker=self.out/"metadata-child.json";terminal=self.out/"controller-terminal.json"
        program=r'''
import importlib.util,json,os,pathlib,signal,sys
sys.path.insert(0,sys.argv[1])
import ycsb_mongodb_reference as reference
import ycsb_mongodb_reference_prepare as preparation
if os.environ.get("YCSB_PREPARATION_TEST_SOURCE"):
    spec=importlib.util.spec_from_file_location("old_preparation",os.environ["YCSB_PREPARATION_TEST_SOURCE"])
    preparation=importlib.util.module_from_spec(spec);spec.loader.exec_module(preparation)
children=[];original=reference.subprocess.Popen
def tracked(*args,**kwargs):
    child=original(*args,**kwargs);children.append(child);return child
reference.subprocess.Popen=tracked
def metadata_stage(config,out):
    code="import json,os,pathlib,time;pathlib.Path("+repr(sys.argv_marker)+").write_text(json.dumps(dict(pid=os.getpid(),sid=os.getsid(0),gomaxprocs_unset='GOMAXPROCS' not in os.environ)));time.sleep(30)"
    return preparation.command([sys.executable,"-c",code])
preparation.prepare=metadata_stage
config,out,marker,terminal=sys.argv[2:]
sys.argv_marker=marker
sys.argv=[preparation.__file__,config,out]
previous=signal.getsignal(signal.SIGTERM)
code=preparation.main()
reaped=False
try:os.waitpid(children[0].pid,os.WNOHANG)
except ChildProcessError:reaped=True
pathlib.Path(terminal).write_text(json.dumps(dict(status=code,signal_restored=signal.getsignal(signal.SIGTERM)==previous,pid=children[0].pid,exit_code=children[0].returncode,reaped=reaped)))
raise SystemExit(code)
'''
        env=dict(os.environ,GOMAXPROCS="123")
        controller=subprocess.Popen([sys.executable,"-c",program,str(ROOT),str(config),str(out),str(marker),str(terminal)],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        descriptor=None;observed=None;stdout=stderr=""
        try:
            deadline=time.monotonic()+5
            while not marker.exists() and controller.poll() is None and time.monotonic()<deadline:time.sleep(.01)
            self.assertTrue(marker.exists(),"metadata child did not start")
            child=c.read(marker);descriptor=os.pidfd_open(child["pid"])
            observed=c.observe(child["pid"])
            self.assertEqual(observed["exe_sha256"],c.exe_digest(sys.executable))
            controller.send_signal(signal.SIGTERM)
            stdout,stderr=controller.communicate(timeout=6)
            self.assertEqual(controller.returncode,3,stderr)
            result=c.read(terminal)
            self.assertEqual(result,dict(status=3,signal_restored=True,pid=child["pid"],exit_code=-signal.SIGTERM,reaped=True))
            self.assertEqual(child,dict(pid=result["pid"],sid=result["pid"],gomaxprocs_unset=True))
            self.assertTrue(select.select([descriptor],[],[],0)[0])
            self.assertFalse(out.exists())
            with self.assertRaises(OSError):reference.validate(out)
        finally:
            if controller.poll() is None:
                controller.send_signal(signal.SIGTERM)
                try:stdout,stderr=controller.communicate(timeout=6)
                except subprocess.TimeoutExpired:
                    controller.kill();stdout,stderr=controller.communicate(timeout=3)
            # Baseline RED must not strand its intentionally exposed child.
            # The descriptor was pinned before controller cancellation; no
            # numeric PID or unrelated process can authorize this cleanup.
            if descriptor is not None:
                if not select.select([descriptor],[],[],0)[0]:signal.pidfd_send_signal(descriptor,signal.SIGKILL)
                self.assertTrue(select.select([descriptor],[],[],3)[0]);os.close(descriptor)
            if os.environ.get("YCSB_PREPARATION_TEST_EVIDENCE_DIR"):
                retained=pathlib.Path(os.environ["YCSB_PREPARATION_TEST_EVIDENCE_DIR"]);retained.mkdir(parents=True,exist_ok=False)
                (retained/"controller.py").write_text(program)
                (retained/"stdout").write_text(stdout);(retained/"stderr").write_text(stderr)
                source=pathlib.Path(env.get("YCSB_PREPARATION_TEST_SOURCE",preparation.__file__))
                shutil.copyfile(source,retained/"preparation-source.py")
                for path in (marker,terminal):
                    if path.exists():shutil.copyfile(path,retained/path.name)
                self.save(retained/"status.json",dict(controller_exit=controller.returncode,metadata_process=observed,preparation_source_sha256=c.digest(source),synthetic_only=True,product_runs=0))


if __name__=="__main__":unittest.main()
