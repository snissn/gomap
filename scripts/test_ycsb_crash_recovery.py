"""Synthetic offline packets and owned stub processes; never product evidence."""
import copy
import json
import os
import pathlib
import signal
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import ycsb_crash_recovery as crash
import ycsb_compare_validate as c


def ts(second): return f"2026-10-29T00:00:{second:02d}Z"
def result(start, end):
    return dict(attempted=True, success=True, error="", start_utc=ts(start), end_utc=ts(end), elapsed_ns=(end-start)*1_000_000_000)


class CrashPackets(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="synthetic-crash-control-", dir=os.environ.get("TMPDIR", "/home/mikers"))
        self.addCleanup(self.temp.cleanup)
        self.out = pathlib.Path(self.temp.name)
        self.directory = self.out/"treedb_native"
        self.directory.mkdir()
        self.db = self.directory/"db"
        self.db.mkdir()
        self.build = dict(schema_version=1, version="synthetic-candidate", source_commit="a"*40, exe_sha256="b"*64, source_patch_sha256=__import__("hashlib").sha256(b"").hexdigest(), expected_owner="cow")
        self.m = dict(schema_version=1, kind=crash.SCOPE, replay_proof=crash.LIMITATION, campaign_id="synthetic-offline-only", target="treedb_native", profile="command_wal_durable", format="json", address="127.0.0.1:17130", build=self.build, server_builds={"treedb_native":self.build}, fixture=dict(schema_version=1, exe_sha256="c"*64, source_sha256=__import__("hashlib").sha256(b"synthetic fixture").hexdigest()), contract_sha256={}, deadlines=dict(timeout_seconds=180, cleanup_timeout_seconds=10, outer_timeout_seconds=600, readiness_seconds=60))
        self.save(self.directory/"server-build.json", self.build)
        (self.directory/"server-source.patch").write_bytes(b"")
        (self.directory/"server-buildinfo.txt").write_text("SYNTHETIC offline build\n")
        (self.directory/"fixture-source.go").write_bytes(b"synthetic fixture")
        for name in ("DURABILITY.md", "COMPATIBILITY.md"):
            (self.directory/name).write_text("SYNTHETIC source contract\n")
            self.m["contract_sha256"][name] = c.digest(self.directory/name)
        self.save(self.out/"manifest.json", self.m)
        self.expected = crash.canonical_expected(crash.identity(self.m))
        self.save(self.directory/"expected.json", self.expected)
        for prefix, n, start in (("server", 1, 0), ("reopened", 2, 10)):
            process = dict(pid=100+n, start_ticks=500+n, exe_sha256="b"*64, gomaxprocs_unset=True)
            obs = dict(schema_version=1, kind="live_owner_observation", process=process, owner_id=str(n)*32, data_dir=str(self.db), data_device=2, data_inode=3, profile=self.m["profile"], backend="cow", capture_error="", stats={"treedb.profile.resolved":self.m["profile"], "treedb.cache.memtable_mode":"cow_btree", "treedb.cache.cow.txn.logical_seq":"0"}, observed_gomaxprocs=4, observed_utc=ts(start))
            evidence = self.directory/(prefix+".evidence")
            evidence.mkdir()
            self.save(evidence/"owner.open.json", obs)
            self.save(self.directory/(prefix+".launch.json"), dict(process=process, data_dir=str(self.db), checkpoint_control_env_absent=True))
            self.save(self.directory/(prefix+".exit.json"), dict(process=process, exit_code=-9 if n==1 else 0))
            self.save(self.directory/(prefix+".endpoint.json"),dict(schema_version=1,process=process,address=self.m["address"],observed_utc=ts(start),proc_net_tcp="header\n0: 0100007F:42EA 00000000:0000 0A 0 0 0 0 0 777\n",fd_socket_inodes=[777],listener_inode=777))
            if n==1:
                self.save(self.directory/"server.crash.json",dict(schema_version=1,kind="owned_process_sigkill",process=process,pidfd_pinned=True,signal=9,exit_code=-9,reaped=True,start_utc=ts(6),end_utc=ts(7)))
            else:
                self.save(evidence/"owner.before-close.json",dict(obs,observed_utc=ts(15)))
                self.save(evidence/"owner.close.json",dict(schema_version=1,kind="original_server_owner_close",process=process,owner_id=obs["owner_id"],close=result(16,17)))
        self.journal = [dict(schema_version=1,ordinal=n,id=row["id"],acknowledged=True,requested_ack="AckVisible",write=result(2,2)) for n,row in enumerate(self.expected["load"])]
        self.write_journal()
        for mode, start, end in (("crash-write",1,4),("crash-verify",11,14)):
            self.save(self.directory/(mode+".json"),dict(crash.identity(self.m),stage=mode,rows=self.expected["load"],operations=[]))
            names={"expected.json",mode+".json"}
            if mode=="crash-write":names.add(mode+".json.acknowledged.jsonl")
            receipt=dict(schema_version=1,kind="public_process_crash_client_close",mode=mode,pid=300+start,source_id=self.build["source_commit"],fixture_id=self.m["campaign_id"],result_error="",close_error="",started_utc=ts(start),ended_utc=ts(end),checkpoint_attempted=False,checkpoint_succeeded=False,checkpoint=None,owner_checkpoint=None,artifacts_sha256={n:c.digest(self.directory/n) for n in names})
            self.save(self.directory/(mode+".close.json"),receipt)
            (self.directory/(mode+".out")).write_text("synthetic helper output\n")
            self.save(self.directory/(mode+".launch.json"),dict(schema_version=1,campaign_id=self.m["campaign_id"],process=dict(pid=receipt["pid"],start_ticks=800+start,exe_sha256="c"*64,gomaxprocs_unset=True),exit_code=0,timed_out=False,timeout_seconds=180,cleanup_timeout_seconds=10,outer_timeout_seconds=600,start_utc=ts(start),end_utc=ts(end),receipt_sha256=c.digest(self.directory/(mode+".close.json")),output_sha256=c.digest(self.directory/(mode+".out"))))
        self.rehash()

    @staticmethod
    def save(path,value): path.write_text(json.dumps(value,allow_nan=False)+"\n")
    def change(self,name,fn):
        path=self.directory/name
        value=c.read(path);fn(value);self.save(path,value)
    def write_journal(self):
        (self.directory/"crash-write.json.acknowledged.jsonl").write_text("".join(json.dumps(x)+"\n" for x in self.journal))
    def rehash(self):
        for mode in ("crash-write","crash-verify"):
            self.change(mode+".close.json",lambda v:v.update(artifacts_sha256={n:c.digest(self.directory/n) for n in v["artifacts_sha256"]}))
            self.change(mode+".launch.json",lambda v:v.update(receipt_sha256=c.digest(self.directory/(mode+".close.json"))))
        names={"expected.json","crash-write.json","crash-write.json.acknowledged.jsonl","crash-write.close.json","crash-write.launch.json"}
        self.save(self.directory/"precrash-barrier.json",dict(schema_version=1,kind="external_evidence_fsync_barrier",start_utc=ts(4),end_utc=ts(5),artifacts_sha256={n:c.digest(self.directory/n) for n in names},synced_directory_paths=[str(self.directory),str(self.directory.parent),str(self.directory.parent.parent)]))
    def reject(self,name,fn):
        originals={p:p.read_bytes() for p in self.directory.rglob("*") if p.is_file()}
        try:
            self.change(name,fn);self.rehash()
            with self.assertRaises((ValueError,TypeError,KeyError)):crash.validate(self.directory)
        finally:
            for p,b in originals.items():p.write_bytes(b)

    def test_complete_packet_and_cli(self):
        report=crash.validate(self.directory)
        self.assertEqual(report["recovered"],64)
        self.assertEqual(report["replay_proof"],crash.LIMITATION)
        process=subprocess.run([sys.executable,str(pathlib.Path(crash.__file__)),"validate",str(self.directory)],capture_output=True,text=True,timeout=10)
        self.assertEqual(process.returncode,0,process.stderr)
        self.assertFalse(json.loads(process.stdout)["power_loss_certified"])

    def test_parent_failure_and_malformed_terminal_markers_cannot_be_requalified(self):
        for location in (self.out,self.directory):
            for name in ("failure.json","server.failure.json","reopened.failure.json","crash-write.failure.json","crash-verify.failure.json"):
                path=location/name
                for kind in ("json","malformed","directory","broken-symlink"):
                    with self.subTest(path=path,kind=kind):
                        if kind=="json":self.save(path,dict(schema_version=1,kind="public_crash_control_failure",error="deferred cancellation"))
                        elif kind=="malformed":path.write_text("{partial failure receipt")
                        elif kind=="directory":path.mkdir()
                        else:path.symlink_to(location/"nonexistent-failure-target")
                        try:
                            with self.assertRaisesRegex(ValueError,"failed crash attempt"):crash.validate(self.directory)
                        finally:
                            if kind=="directory":path.rmdir()
                            else:path.unlink()
        self.assertEqual(crash.validate(self.directory)["recovered"],64)

    def test_all_supported_targets_formats_profiles_and_version_owner_bindings(self):
        for target,fmt in (("treedb_native","json"),("treedb_native","bson"),("treedb_mongo","bson")):
            for profile in crash.PROFILES:
                for version,backend in (("synthetic-baseline","legacy"),("synthetic-candidate","cow")):
                    with self.subTest(target=target,format=fmt,profile=profile,version=version):
                        self.m.update(target=target,format=fmt,profile=profile)
                        self.build.update(version=version,expected_owner=backend)
                        self.m["server_builds"]={target:self.build}
                        self.save(self.out/"manifest.json",self.m)
                        self.save(self.directory/"server-build.json",self.build)
                        self.expected=crash.canonical_expected(crash.identity(self.m))
                        self.save(self.directory/"expected.json",self.expected)
                        for prefix in ("server","reopened"):
                            for name in (("owner.open.json",) if prefix=="server" else ("owner.open.json","owner.before-close.json")):
                                self.change(prefix+".evidence/"+name,lambda v:v.update(profile=profile,backend=backend,capture_error="" if backend=="cow" else "treedb: command kind has no native semantic preparation",stats={"treedb.profile.resolved":profile,"treedb.cache.memtable_mode":"cow_btree","treedb.cache.cow.txn.logical_seq":"0"}))
                        for mode in ("crash-write","crash-verify"):
                            self.save(self.directory/(mode+".json"),dict(crash.identity(self.m),stage=mode,rows=self.expected["load"],operations=[]))
                        for operation in self.journal:operation["requested_ack"]="AckVisible" if target=="treedb_native" else "w:1,j:absent"
                        self.write_journal();self.rehash()
                        self.assertEqual(crash.validate(self.directory)["recovered"],64)

    def test_profile_guarantees_and_no_global_no_wal_prefix(self):
        before=crash.exact.rows(self.expected["load"],"json")
        for profile in crash.PROFILES:
            expected=dict(self.expected,profile=profile)
            self.assertEqual(crash.validate_state(expected,before,before)["lost"],0)
            for n in (0,1,63):
                after={r["id"]:r["fields"] for r in expected["load"][:n]}
                if profile=="command_wal_durable":
                    with self.assertRaises(ValueError):crash.validate_state(expected,before,after)
                else:self.assertEqual(crash.validate_state(expected,before,after)["lost"],64-n)
            after={expected["load"][63]["id"]:expected["load"][63]["fields"]}
            if profile=="no_wal_fast":crash.validate_state(expected,before,after)
            else:
                with self.assertRaises(ValueError):crash.validate_state(expected,before,after)

    def test_matching_malformed_scalar_and_process_copies(self):
        for name,path,values in (("server.crash.json",("signal",),(True,9.0)),("server.crash.json",("exit_code",),(True,-9.0)),("server.exit.json",("exit_code",),(True,-9.0)),("crash-write.close.json",("pid",),(True,301.0)),("server.launch.json",("process","start_ticks"),(True,501.0)),("crash-verify.launch.json",("process","pid"),(True,311.0)),("reopened.evidence/owner.open.json",("data_inode",),(True,3.0)),("crash-write.close.json",("schema_version",),(True,1.0))):
            for value in values:
                def mutate(v,path=path,value=value):
                    for key in path[:-1]:v=v[key]
                    v[path[-1]]=value
                with self.subTest(name=name,path=path,value=value):self.reject(name,mutate)

    def test_failed_ambiguous_journal_operations_never_demanded(self):
        for mutate in (lambda v:v.update(acknowledged=False),lambda v:v.update(acknowledged=1),lambda v:v.update(ordinal=0.0),lambda v:v.update(requested_ack="AckSynced"),lambda v:v["write"].update(success=False,error="transport timeout"),lambda v:v["write"].update(elapsed_ns=0.0),lambda v:v["write"].update(success=1)):
            original=copy.deepcopy(self.journal)
            try:
                mutate(self.journal[0]);self.write_journal();self.rehash()
                with self.assertRaises((ValueError,TypeError,KeyError)):crash.validate(self.directory)
            finally:self.journal=original;self.write_journal();self.rehash()
        self.journal.pop();self.write_journal();self.rehash()
        with self.assertRaises(ValueError):crash.validate(self.directory)

    def test_loss_torn_type_and_extra_row_rejected_full_packet(self):
        for mutate in (lambda v:v["rows"].pop(),lambda v:v["rows"][0]["fields"].pop("field0"),lambda v:v["rows"][0]["fields"]["field0"].update(type="binary"),lambda v:v["rows"].append(v["rows"][0])):
            self.reject("crash-verify.json",mutate)

    def test_no_checkpoint_close_and_complete_lifetime(self):
        for name,mutate in (("crash-write.close.json",lambda v:v.update(checkpoint_attempted=True)),("crash-write.close.json",lambda v:v.update(checkpoint=result(2,3))), ("server.launch.json",lambda v:v.update(checkpoint_control_env_absent=False)),("server.crash.json",lambda v:v.update(start_utc=ts(4))), ("crash-verify.launch.json",lambda v:v.update(start_utc=ts(9))), ("reopened.evidence/owner.before-close.json",lambda v:v.update(observed_utc=ts(12))), ("reopened.evidence/owner.open.json",lambda v:v.update(data_inode=4))):
            self.reject(name,mutate)
        for name in ("owner.close.json","owner.checkpoint.json","owner.checkpoint-control.json","owner.before-close.json"):
            path=self.directory/"server.evidence"/name
            path.write_text("{}")
            with self.assertRaises(ValueError):crash.validate(self.directory)
            path.unlink()
            path.symlink_to(path.parent/"missing-owner-receipt")
            with self.assertRaises(ValueError):crash.validate(self.directory)
            path.unlink()

    def test_listener_must_belong_to_same_owner_and_actual_loopback_endpoint(self):
        for name,mutate in (("server.endpoint.json",lambda v:v.update(fd_socket_inodes=[778])),("server.endpoint.json",lambda v:v.update(listener_inode=777.0)),("server.endpoint.json",lambda v:v["process"].update(start_ticks=501.0)),("server.endpoint.json",lambda v:v.update(address="127.0.0.1:1")),("reopened.endpoint.json",lambda v:v.update(observed_utc=ts(12)))):
            self.reject(name,mutate)

    def test_failed_retry_does_not_write_into_preexisting_output(self):
        process=subprocess.run([sys.executable,str(pathlib.Path(crash.__file__)),"run","--output",str(self.out),"--target","treedb_native","--profile","command_wal_durable","--format","json","--server-manifest",str(self.out/"unused"),"--fixture-manifest",str(self.out/"unused"),"--fixture-source",str(self.out/"unused"),"--server-binary",sys.executable,"--fixture-binary",sys.executable,"--address","127.0.0.1:17130"],capture_output=True,text=True,timeout=5)
        self.assertNotEqual(process.returncode,0)
        self.assertFalse((self.out/"failure.json").exists())

    def test_nonfinite_duplicate_keys_and_submicrosecond_bounds(self):
        self.reject("server.crash.json",lambda v:v.update(start_utc="2026-10-29T00:00:04.999999999Z"))
        path=self.directory/"server.crash.json";original=path.read_text()
        for raw in ('{"schema_version":1,"schema_version":1}', '{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}'):
            path.write_text(raw)
            with self.assertRaises(ValueError):crash.validate(self.directory)
        path.write_text(original)
        path=self.directory/"crash-write.json.acknowledged.jsonl";original=path.read_text()
        for raw in ('{"schema_version":1,"schema_version":1}', '{"value":NaN}', '{"value":Infinity}', '{"value":-Infinity}'):
            path.write_text(raw+"\n"+"\n".join(original.splitlines()[1:])+"\n");self.rehash()
            with self.assertRaises(ValueError):crash.validate(self.directory)
        path.write_text(original);self.rehash()
        self.journal[1]["write"]["start_utc"]="2026-10-29T00:00:01.999999999Z"
        self.write_journal();self.rehash()
        with self.assertRaises(ValueError):crash.validate(self.directory)

    def test_external_fsync_barrier_refuses_database_artifacts(self):
        (self.db/"bad").write_text("never fsync a DB as evidence")
        with self.assertRaises(ValueError):crash.artifact_barrier(self.directory,["db/bad"],self.db)
        barrier=crash.artifact_barrier(self.directory,["expected.json"],self.db)
        self.assertEqual(barrier["artifacts_sha256"]["expected.json"],c.digest(self.directory/"expected.json"))


class OwnedStubControls(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="synthetic-crash-child-",dir=os.environ.get("TMPDIR","/home/mikers"))
        self.addCleanup(self.temp.cleanup)
        self.directory=pathlib.Path(self.temp.name)
        self.m=dict(target="treedb_native",build=dict(exe_sha256=c.digest(sys.executable)),deadlines=dict(cleanup_timeout_seconds=.1,readiness_seconds=.1))

    def test_failed_readiness_and_pin_acquisition_reap_actual_owned_child(self):
        for pin_failure in (False,True):
            prefix="pin" if pin_failure else "ready"
            captured=[]
            original=subprocess.Popen
            def popen(*args,**kwargs):
                child=original(*args,**kwargs);captured.append(child);return child
            with mock.patch.object(subprocess,"Popen",popen), mock.patch.object(crash.live,"wait_owner",side_effect=TimeoutError("synthetic readiness failure")):
                with contextlib_pin_failure(pin_failure):
                    with self.assertRaises((OSError,TimeoutError)):
                        with crash.owned_server(self.directory,prefix,[sys.executable,"-c","import time; time.sleep(60)"],self.m):self.fail("clients reached")
            self.assertIsNotNone(captured[0].returncode)
            failure=c.read(self.directory/(prefix+".failure.json"))
            self.assertTrue(failure["cleanup"]["reaped"])
            self.assertTrue(failure["cleanup"]["success"])

    def test_actual_stub_sigkill_is_pinned_reaped_and_never_gracefully_closed(self):
        with mock.patch.object(crash.live,"wait_owner"):
            with crash.owned_server(self.directory,"server",[sys.executable,"-c","import time; time.sleep(60)"],self.m) as (child,descriptor,observed):
                crash.crash_owned(child,descriptor,observed,self.directory,.2)
                self.assertEqual(child.returncode,-signal.SIGKILL)
        self.assertEqual(c.read(self.directory/"server.exit.json")["exit_code"],-9)
        self.assertFalse((self.directory/"server.evidence/owner.close.json").exists())

    def test_changed_start_identity_refuses_crash_signal(self):
        with mock.patch.object(crash.live,"wait_owner"):
            with crash.owned_server(self.directory,"server",[sys.executable,"-c","import time; time.sleep(60)"],self.m) as (child,descriptor,observed):
                with mock.patch.object(signal,"pidfd_send_signal") as send:
                    with self.assertRaises(ValueError):crash.crash_owned(child,descriptor,dict(observed,start_ticks=observed["start_ticks"]+1),self.directory,.2)
                    send.assert_not_called()
        self.assertFalse((self.directory/"server.crash.json").exists())

    def test_real_sigterm_during_term_ignoring_stub_cleanup_is_deferred_until_reap(self):
        marker=self.directory/"cleanup-started"
        script=self.directory/"launcher.py"
        script.write_text('''import os,pathlib,signal,sys,time
import ycsb_crash_recovery as r
import ycsb_compare_validate as c
directory=pathlib.Path(sys.argv[1])
def cancel(signum,frame):raise InterruptedError("real SIGTERM during cleanup")
signal.signal(signal.SIGTERM,cancel)
r.live.wait_owner=lambda *args:None
original=c.terminate_pinned_child
def cleanup(fd,timeout):
 (directory/"cleanup-started").write_text("ready")
 return original(fd,timeout)
c.terminate_pinned_child=cleanup
manifest=dict(target="treedb_native",build=dict(exe_sha256=c.digest(sys.executable)),deadlines=dict(cleanup_timeout_seconds=.3,readiness_seconds=.1))
code="import os,pathlib,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);pathlib.Path("+repr(str(directory/"child-ready"))+").write_text(str(os.getpid()));time.sleep(60)"
try:
 with r.owned_server(directory,"server",[sys.executable,"-c",code],manifest):
  until=time.monotonic()+3
  while not (directory/"child-ready").exists():
   if time.monotonic()>until:raise TimeoutError("stub not ready")
   time.sleep(.01)
  raise ValueError("synthetic failure forcing cleanup")
except BaseException:pass
''')
        env=os.environ.copy();env["PYTHONPATH"]=str(pathlib.Path(crash.__file__).parent)
        launcher=subprocess.Popen([sys.executable,str(script),str(self.directory)],env=env)
        try:
            until=time.monotonic()+5
            while not marker.exists():
                self.assertIsNone(launcher.poll(),"stub launcher exited before cleanup")
                self.assertLess(time.monotonic(),until)
                time.sleep(.01)
            launcher.send_signal(signal.SIGTERM)
            self.assertEqual(launcher.wait(timeout=5),0)
        finally:
            if launcher.poll() is None:launcher.kill();launcher.wait(timeout=3)
        failure=c.read(self.directory/"server.failure.json")
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertEqual(failure["cleanup"]["exit_code"],-9)
        pid=int((self.directory/"child-ready").read_text())
        self.assertFalse(pathlib.Path(f"/proc/{pid}").exists(),"TERM-ignoring owned stub leaked")

    def test_actual_listener_identity_excludes_another_process(self):
        with socket.socket() as server:
            server.bind(("127.0.0.1",0));server.listen()
            address="127.0.0.1:"+str(server.getsockname()[1])
            observed=crash.observe_endpoint(address,c.observe(os.getpid()))
            self.assertGreater(observed["listener_inode"],0)
            env=os.environ.copy();env.pop("GOMAXPROCS",None)
            child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"],env=env)
            try:
                with self.assertRaises(ValueError):crash.observe_endpoint(address,c.observe(child.pid))
            finally:child.terminate();child.wait(timeout=3)


def contextlib_pin_failure(enabled):
    import contextlib
    return mock.patch.object(os,"pidfd_open",side_effect=OSError("injected pidfd acquisition failure")) if enabled else contextlib.nullcontext()


class SyntheticRunCLI(unittest.TestCase):
    def test_real_cli_stub_packet_reaches_final_validation_and_reaps_both_owners(self):
        go=os.environ.get("YCSB_TEST_GO") or shutil.which("go")
        if not go:self.skipTest("Go is needed to compile explicitly synthetic CLI stub")
        with tempfile.TemporaryDirectory(prefix="synthetic-crash-cli-",dir=os.environ.get("TMPDIR","/home/mikers")) as root:
            root=pathlib.Path(root);source=root/"source";source.mkdir()
            stub=pathlib.Path(crash.__file__).parent/"testdata/ycsb_crash/synthetic_stub.go"
            shutil.copyfile(stub,source/"stub.go")
            for name in ("docs/contracts/DURABILITY.md","TreeDB/mongo_gateway/COMPATIBILITY.md"):
                path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text("SYNTHETIC offline contract stub; not product evidence\n")
            def git(*args):return subprocess.check_output(["git","-C",str(source),*args],stderr=subprocess.DEVNULL,timeout=10)
            git("init");git("add",".");git("-c","user.name=Synthetic Control","-c","user.email=synthetic@example.invalid","commit","-m","Synthetic offline source")
            commit=git("rev-parse","HEAD").decode().strip()
            binary=root/"synthetic-stub"
            env=os.environ.copy();env.pop("GOROOT",None);env.update(GOWORK="off",GOTOOLCHAIN="local",GOPROXY="off",GO111MODULE="off")
            built=subprocess.run([go,"build","-p=1","-o",str(binary),str(source/"stub.go")],env=env,capture_output=True,text=True,timeout=60)
            self.assertEqual(built.returncode,0,built.stderr)
            build=dict(schema_version=1,version="SYNTHETIC-offline-only",source_commit=commit,source_dir=str(source),source_patch_sha256=__import__("hashlib").sha256(b"").hexdigest(),exe_sha256=c.digest(binary),expected_owner="cow")
            CrashPackets.save(root/"server.json",dict(schema_version=1,servers={"treedb_native":build}))
            CrashPackets.save(root/"fixture.json",dict(schema_version=1,exe_sha256=c.digest(binary),source_sha256=c.digest(source/"stub.go")))
            with socket.socket() as reserved:
                reserved.bind(("127.0.0.1",0));port=reserved.getsockname()[1]
            output=root/"attempt"
            env.update(GOMAXPROCS="13",YCSB_OWNER_CHECKPOINT="1")
            command=[sys.executable,str(pathlib.Path(crash.__file__)),"run","--output",str(output),"--target","treedb_native","--profile","command_wal_durable","--format","json","--server-manifest",str(root/"server.json"),"--fixture-manifest",str(root/"fixture.json"),"--fixture-source",str(source/"stub.go"),"--server-binary",str(binary),"--fixture-binary",str(binary),"--address",f"127.0.0.1:{port}","--go",go,"--helper-timeout","5","--cleanup-timeout","1","--readiness-timeout","5","--outer-timeout","30"]
            outcome=subprocess.run(command,env=env,capture_output=True,text=True,timeout=40)
            self.assertEqual(outcome.returncode,0,outcome.stderr)
            packet=output/"treedb_native"
            self.assertEqual(crash.validate(packet)["recovered"],64)
            for prefix,wanted in (("server",-9),("reopened",0)):
                exited=c.read(packet/(prefix+".exit.json"))
                self.assertEqual(exited["exit_code"],wanted)
                self.assertTrue(exited["process"]["gomaxprocs_unset"])
                try:actual=c.observe_core(exited["process"]["pid"])
                except (FileNotFoundError,ProcessLookupError):continue
                self.assertNotEqual(actual["start_ticks"],exited["process"]["start_ticks"],"owned stub survived final reap")

            # Actual cancellation during normal successful reopened close, and
            # after all cleanup but before final validation, must stay terminal.
            wrapper=root/"deferred-normal-close.py"
            wrapper.write_text('''import os,signal,sys
import ycsb_crash_recovery as r
import ycsb_compare_validate as c
stage=os.environ["SYNTHETIC_CANCEL_STAGE"]
signum=int(os.environ["SYNTHETIC_CANCEL_SIGNAL"])
if stage=="normal-close":
 original=c.terminate_pinned_child
 def cleanup(fd,timeout):
  original(fd,timeout)
  os.kill(os.getpid(),signum)
 c.terminate_pinned_child=cleanup
else:
 original=r.validate
 def validate(directory):
  os.kill(os.getpid(),signum)
  return original(directory)
 r.validate=validate
raise SystemExit(r.main())
''')
            env["PYTHONPATH"]=str(pathlib.Path(crash.__file__).parent)
            command[1]=str(wrapper)
            for stage in ("normal-close","prevalidation"):
                for signum,error_type in ((signal.SIGTERM,"InterruptedError"),(signal.SIGINT,"KeyboardInterrupt"),(signal.SIGALRM,"TimeoutError")):
                    with self.subTest(stage=stage,signal=signum):
                        failed_output=root/(stage+"-"+str(int(signum)))
                        command[command.index("--output")+1]=str(failed_output)
                        env.update(SYNTHETIC_CANCEL_STAGE=stage,SYNTHETIC_CANCEL_SIGNAL=str(int(signum)))
                        outcome=subprocess.run(command,env=env,capture_output=True,text=True,timeout=40)
                        self.assertEqual(outcome.returncode,3,outcome.stderr)
                        failed_packet=failed_output/"treedb_native"
                        failure=c.read(failed_output/"failure.json")
                        self.assertEqual(failure["error_type"],error_type)
                        self.assertTrue(failure["error"])
                        self.assertFalse((failed_packet/"reopened.failure.json").exists())
                        for prefix,wanted in (("server",-9),("reopened",0)):
                            exited=c.read(failed_packet/(prefix+".exit.json"))
                            self.assertEqual(exited["exit_code"],wanted)
                            try:actual=c.observe_core(exited["process"]["pid"])
                            except (FileNotFoundError,ProcessLookupError):continue
                            self.assertNotEqual(actual["start_ticks"],exited["process"]["start_ticks"],"cancelled attempt left owned stub live")
                        with self.assertRaisesRegex(ValueError,"failed crash attempt"):crash.validate(failed_packet)



if __name__=="__main__":unittest.main()
