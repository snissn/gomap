"""Offline CLI controls. Synthetic receipts here are not product evidence."""
import copy
import base64
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("compare", ROOT / "ycsb_compare_validate.py")
COMPARE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMPARE)
DATA = ROOT / "testdata/ycsb_compare"


def call_result(start=1, end=2):
    return dict(attempted=True, success=True, error="", start_utc=f"2026-10-29T00:00:{start:02d}Z", end_utc=f"2026-10-29T00:00:{end:02d}Z", elapsed_ns=(end-start)*1_000_000_000)


class CompareTests(unittest.TestCase):
    def setUp(self):
        # Honor the host storage policy without relying on space on fast4tb.
        base = os.environ.get("TMPDIR", "/home/mikers")
        self.temp = tempfile.TemporaryDirectory(prefix="ycsb-compare-offline-", dir=base)
        self.addCleanup(self.temp.cleanup)
        self.out = pathlib.Path(self.temp.name)
        self.target = "treedb_mongo"
        self.directory = self.out / self.target
        self.directory.mkdir()
        self.process = dict(pid=1234, start_ticks=500, exe_sha256="a"*64, gomaxprocs_unset=True)
        self.manifest = dict(schema_version=1,deadlines=dict(phase_seconds=600,helper_seconds=180,cleanup_seconds=10,phase_outer_seconds=640,helper_outer_seconds=220), campaign_id="offline-test-only", targets=[self.target], profile="command_wal_relaxed", formats={self.target:"bson"}, recordcount=64, operationcount=10000, threadcount=16, run_repeats=1, client=dict(schema_version=1, producer_commit=COMPARE.PRODUCER, source_commit=COMPARE.PRODUCER, source_patch_sha256=hashlib.sha256(b"").hexdigest(), build_id="offline-fixture-build", exe_sha256="a"*64, toolchain="fixture", compatibility_configuration="fixture"))
        self.manifest["server_builds"] = {self.target:dict(schema_version=1, version="candidate", source_commit="d"*40, exe_sha256="a"*64, expected_owner="cow")}
        self.save(self.out / "server-build.json", dict(schema_version=1, servers=self.manifest["server_builds"]))
        self.save(self.out / "manifest.json", self.manifest)
        self.save(self.out / "client-build.json", self.manifest["client"])
        (self.out / "client.patch").write_bytes(b"")
        (self.out / "client-buildinfo.txt").write_text("offline synthetic build fixture\n")
        self.receipts = {}
        ids = []
        for stem in ("load", "run_1"):
            content=(DATA / f"{stem}.out").read_text()
            if stem=="load": content=content.replace("Count: 100000","Count: 64")
            (self.directory / f"{stem}.out").write_text(content)
            phase_id = f"offline-test-only:{stem}"
            ids.append(phase_id)
            receipt = dict(schema_version=1, phase_id=phase_id, client_build_id="offline-fixture-build", db="mongodb", phase="load" if stem == "load" else "run", declared_profile="command_wal_relaxed", total_excludes_close=True, command=dict(attempted=True, success=True, error="", start_utc="0001-01-01T00:00:00Z", end_utc="0001-01-01T00:00:00Z", elapsed_ns=0), db_close=call_result(1,2), workload_close=call_result(2,3), native_flush=dict(applicable=False, flush_enabled=False))
            if stem!="load": receipt.update(db_close=call_result(4,4),workload_close=call_result(4,4))
            self.receipts[stem] = receipt
            self.save(self.directory / f"{stem}.close.json", receipt)
            self.save(self.directory / f"{stem}.launch.json", dict(schema_version=1, campaign_id="offline-test-only", phase_id=phase_id, timeout_seconds=600,cleanup_timeout_seconds=10,outer_timeout_seconds=640,timed_out=False,process=self.process, start_utc="2026-10-29T00:00:00Z" if stem=="load" else "2026-10-29T00:00:04Z", end_utc="2026-10-29T00:00:04Z", exit_code=0, output_sha256=COMPARE.digest(self.directory / f"{stem}.out"), receipt_sha256=COMPARE.digest(self.directory / f"{stem}.close.json")))
        data_dir = str(self.directory / "db")
        self.save(self.directory / "server.launch.json", dict(process=self.process, data_dir=data_dir))
        shared = dict(schema_version=1, version="candidate", source_commit="d"*40, campaign_id="offline-test-only", target=self.target, profile="command_wal_relaxed", format="bson", process=self.process, data_dir=data_dir, owner_id="1"*32, phase_ids=ids)
        self.save(self.directory / "runtime.json", dict(**shared, observed_gomaxprocs=16, backend="cow", observation="synthetic offline test"))
        self.save(self.directory / "lifecycle.json", dict(**shared, server_exit_code=0, checkpoint=call_result(4,5), server_close=call_result(5,6), reopen=call_result(6,7), random_state_preservation=call_result(7,8), preclose_state_sha256="b"*64, reopened_state_sha256="b"*64, preclose_state_utc="2026-10-29T00:00:04Z", reopened_state_utc="2026-10-29T00:00:07Z", random_state_scope="observed-state-preservation-only"))
        self.save(self.directory / "server.exit.json", dict(process=self.process, exit_code=0))
        self.live_fixture()

    def live_fixture(self):
        import ycsb_compare_live as live
        from test_ycsb_correctness_fixture import fixture
        build=self.manifest["server_builds"][self.target]
        build["source_patch_sha256"]=hashlib.sha256(b"").hexdigest()
        self.manifest.update(lifecycle_contract=2,fixture=dict(schema_version=1,exe_sha256="a"*64,source_sha256=hashlib.sha256(b"offline fixture source").hexdigest()))
        (self.out/"fixture-source.go").write_bytes(b"offline fixture source")
        (self.directory/"server-source.patch").write_bytes(b"")
        (self.directory/"server-buildinfo.txt").write_text("offline buildinfo")
        self.save(self.out/"server-build.json",dict(schema_version=1,servers=self.manifest["server_builds"]))
        self.save(self.out/"manifest.json",self.manifest)
        owners={}
        for i,(prefix,opened,closed) in enumerate((("server",0,5),("reopened",6,8),("fixture-write",10,13),("fixture-reopened",15,18))):
            proc=dict(self.process,pid=1234+i,start_ticks=500+i)
            path=str(self.directory/("fixture-db" if prefix.startswith("fixture") else "db"))
            observation=dict(schema_version=1,kind="live_owner_observation",owner_id=str(i+1)*32,process=proc,data_dir=path,data_device=10,data_inode=22 if prefix.startswith("fixture") else 21,profile=self.manifest["profile"],observed_gomaxprocs=16,backend=build["expected_owner"],capture_error="" if build["expected_owner"]=="cow" else "treedb: command kind has no native semantic preparation",stats={"treedb.profile.resolved":self.manifest["profile"],"treedb.cache.memtable_mode":"cow_btree","treedb.cache.cow.txn.logical_seq":"0"},observed_utc=f"2026-10-29T00:00:{opened:02d}Z")
            evidence=self.directory/(prefix+".evidence");evidence.mkdir(exist_ok=True)
            self.save(evidence/"owner.open.json",observation)
            before=dict(observation,observed_utc=f"2026-10-29T00:00:{closed:02d}Z")
            self.save(evidence/"owner.before-close.json",before)
            self.save(evidence/"owner.close.json",dict(schema_version=1,kind="original_server_owner_close",owner_id=observation["owner_id"],process=proc,close=call_result(closed,closed+1)))
            self.save(self.directory/(prefix+".launch.json"),dict(process=proc,data_dir=path))
            self.save(self.directory/(prefix+".exit.json"),dict(process=proc,exit_code=0))
            owners[prefix]=observation
        self.change("lifecycle.json",lambda v:v.update(reopened_process=owners["reopened"]["process"],reopened_owner_id=owners["reopened"]["owner_id"],preclose_state_utc="2026-10-29T00:00:05Z",reopened_state_utc="2026-10-29T00:00:08Z",reopen=call_result(6,6),random_state_preservation=call_result(8,9)))
        for name in ("runtime.json","lifecycle.json"):
            self.change(name,lambda v:v.update(data_dir=owners["server"]["data_dir"]))
        identity=dict(schema_version=1,source_id=build["source_commit"],fixture_id=self.manifest["campaign_id"]+":"+self.target+":random",profile=self.manifest["profile"],target="native" if self.target=="treedb_native" else "mongo",format="bson")
        fields={"field"+str(i):dict(type="binary",subtype=0,base64=base64.b64encode(b"x"*100).decode()) for i in range(10)}
        rows=[dict(id=key,fields=fields) for key in sorted(live.expected_keys(self.manifest["recordcount"]))]
        for stage,start,end in (("loaded",4,4),("preclose",4,5),("reopened",7,8)):
            value=dict(identity,stage="random_"+stage,rows=rows,operations=[])
            self.save(self.directory/("snapshot_"+stage+".json"),value)
            self.helper_fixture("snapshot_"+stage,"snapshot",identity["fixture_id"],start,end,[("snapshot_"+stage+".json")],stage=="preclose")
            if stage!="loaded":
                digest=live.canonical_snapshot(value,self.manifest,self.target,stage)[1]
                self.change("lifecycle.json",lambda v:v.update({stage+"_state_sha256":digest}))
        expected,snaps=fixture("bson")
        exact_id=dict(identity,fixture_id=self.manifest["campaign_id"]+":"+self.target+":deterministic")
        expected.update(exact_id)
        for snap in snaps:snap.update(exact_id)
        self.save(self.directory/"fixture.expected.json",expected)
        for name,snap in zip(("fixture.updated.json.load.json","fixture.updated.json","fixture.reopened.json"),snaps): self.save(self.directory/name,snap)
        self.helper_fixture("fixture_write","write",exact_id["fixture_id"],11,12,["fixture.expected.json","fixture.updated.json","fixture.updated.json.load.json"],True)
        self.helper_fixture("fixture_verify","verify",exact_id["fixture_id"],16,17,["fixture.expected.json","fixture.reopened.json"],False)

    def helper_fixture(self,stem,mode,fixture_id,start,end,artifacts,checkpoint):
        receipt=dict(schema_version=1,kind="random_snapshot_client_close" if mode=="snapshot" else "deterministic_fixture_client_close",source_id=self.manifest["server_builds"][self.target]["source_commit"],fixture_id=fixture_id,mode=mode,pid=4321,started_utc=f"2026-10-29T00:00:{start:02d}Z",ended_utc=f"2026-10-29T00:00:{end:02d}Z",result_error="",close_error="",checkpoint_attempted=checkpoint,checkpoint_succeeded=checkpoint,checkpoint=call_result(start,end) if checkpoint else None,artifacts_sha256={name:COMPARE.digest(self.directory/name) for name in artifacts})
        if checkpoint and self.target=="treedb_mongo":
            prefix="server" if mode=="snapshot" else "fixture-write"
            opened=COMPARE.read(self.directory/(prefix+".evidence")/"owner.open.json")
            snapshot=stem+".json" if mode=="snapshot" else "fixture.updated.json"
            request=dict(schema_version=1,owner_id=opened["owner_id"],request_id="f"*32,client=dict(self.process,pid=4321),source_id=receipt["source_id"],fixture_id=fixture_id,snapshot_sha256=COMPARE.digest(self.directory/snapshot))
            proof=dict(schema_version=1,kind="explicit_live_owner_checkpoint",request=request,owner=dict(opened,observed_utc=receipt["started_utc"]),collections_flush_all=call_result(start,start),backend_checkpoint=call_result(start,end),result=receipt["checkpoint"])
            self.save(self.directory/(prefix+".evidence")/"owner.checkpoint-control.json",dict(schema_version=1,kind="private_owner_checkpoint_control",owner_id=opened["owner_id"],address="127.0.0.1:12345"))
            self.save(self.directory/(prefix+".evidence")/"owner.checkpoint-request.json",request)
            self.save(self.directory/(prefix+".evidence")/"owner.checkpoint.json",proof)
            receipt["owner_checkpoint"]=proof
        self.save(self.directory/(stem+".close.json"),receipt)
        self.save(self.directory/(stem+".launch.json"),dict(schema_version=1,process=dict(self.process,pid=4321),campaign_id=self.manifest["campaign_id"],phase_id=stem,exit_code=0,timeout_seconds=180,cleanup_timeout_seconds=10,outer_timeout_seconds=220,timed_out=False,start_utc=receipt["started_utc"],end_utc=receipt["ended_utc"],receipt_sha256=COMPARE.digest(self.directory/(stem+".close.json"))))

    @staticmethod
    def save(path, value):
        path.write_text(json.dumps(value)+"\n")

    def change(self, filename, mutate):
        path = self.directory / filename
        value = json.loads(path.read_text())
        mutate(value)
        self.save(path, value)
        if filename.endswith(".close.json"):
            stem = filename[:-11]
            self.change(stem+".launch.json", lambda launch: launch.update(receipt_sha256=COMPARE.digest(path)))

    def check(self, valid, message=None, **env):
        environment = os.environ.copy()
        environment.update(PARSE_ONLY="true", OUT_DIR=str(self.out), **env)
        outcome = subprocess.run(["bash", str(ROOT / "ycsb_compare_mongodb_treedb.sh")], env=environment, text=True, capture_output=True)
        if valid:
            self.assertEqual(outcome.returncode, 0, outcome.stderr)
        else:
            self.assertNotEqual(outcome.returncode, 0, outcome.stdout)
            self.assertEqual(json.loads((self.out / "validation.json").read_text())["status"], "invalid")
            if message:
                self.assertIn(message, outcome.stderr)
        return outcome

    def test_real_retained_logs_have_exact_counts_and_ops(self):
        self.check(True)
        summary = (self.out / "summary.tsv").read_text()
        retained=COMPARE.parse_log(DATA/"load.out")
        COMPARE.check_counts(retained,100000,"load",DATA/"load.out")
        self.assertIn("TOTAL\t64\t16622.7", summary)
        self.assertIn("TOTAL\t10000\t19501.1", summary)
        self.assertIn("READ\t9515\t18556.9", summary)
        self.assertEqual(json.loads((self.out / "validation.json").read_text())["status"], "validated-consumer-evidence")

    def test_missing_receipt(self):
        (self.directory / "load.close.json").unlink()
        self.check(False, "load.close.json")

    def test_failed_original_close_even_with_valid_totals(self):
        self.change("load.close.json", lambda receipt: receipt["db_close"].update(success=False, error="actual Close error"))
        self.check(False, "failed/skipped DB Close")

    def test_stale_phase_build_profile_and_db(self):
        for key, value in (("phase_id", "stale"), ("client_build_id", "wrong-build"), ("declared_profile", "no_wal_fast"), ("db", "treedb-native"), ("phase", "run")):
            with self.subTest(key=key):
                self.save(self.directory / "load.close.json", self.receipts["load"])
                self.change("load.close.json", lambda receipt: receipt.update({key:value}))
                self.check(False)

    def test_truncated_and_duplicate_key_receipts(self):
        for content in ('{"schema_version":', '{"schema_version":1,"schema_version":1}'):
            with self.subTest(content=content):
                (self.directory / "load.close.json").write_text(content)
                self.change("load.launch.json", lambda launch: launch.update(receipt_sha256=COMPARE.digest(self.directory / "load.close.json")))
                self.check(False)

    def test_missing_total_missing_operation_and_malformed_rows(self):
        original = (self.directory / "load.out").read_text()
        for content in ("\n".join(line for line in original.splitlines() if not line.startswith("TOTAL")), "\n".join(line for line in original.splitlines() if not line.startswith("INSERT")), original.replace("Count: 64", "Count: broken")):
            with self.subTest(content=content[-100:]):
                (self.directory / "load.out").write_text(content)
                self.check(False)

    def test_actual_retained_operation_error_log_is_rejected(self):
        shutil.copyfile(DATA / "error-load.out", self.directory / "load.out")
        self.check(False, "TOTAL count mismatch")
        summary = (self.out / "summary.tsv").read_text()
        self.assertIn("INSERT_ERROR\t95229", summary)
        self.assertIn("95229\tfalse", summary)

    def test_errors_cannot_be_waived_when_totals_pass(self):
        path = self.directory / "load.out"
        original = path.read_text()
        insert = next(line for line in original.splitlines() if line.startswith("INSERT"))
        path.write_text(original+insert.replace("INSERT -", "INSERT_ERROR -").replace("Count: 64", "Count: 1")+"\n")
        self.check(False, "operation errors", ALLOW_YCSB_ERRORS="true")

    def test_missing_runtime_cow_and_owner_mismatch(self):
        original = json.loads((self.directory / "runtime.json").read_text())
        for mutate in (lambda value:value.pop("observed_gomaxprocs"), lambda value:value.update(backend="legacy"), lambda value:value.update(owner_id="wrong-owner"), lambda value:value["process"].update(start_ticks=501), lambda value:value.update(phase_ids=[])):
            with self.subTest(mutate=mutate):
                self.save(self.directory / "runtime.json", copy.deepcopy(original))
                self.change("runtime.json", mutate)
                self.check(False)
        (self.directory / "runtime.json").unlink()
        self.check(False, "runtime.json")

    def test_failed_checkpoint_close_reopen_or_state_preservation(self):
        original = json.loads((self.directory / "lifecycle.json").read_text())
        for name in ("checkpoint", "server_close", "reopen", "random_state_preservation"):
            with self.subTest(name=name):
                self.save(self.directory / "lifecycle.json", original)
                self.change("lifecycle.json", lambda value:value[name].update(success=False, error="injected offline failure"))
                self.check(False, name)

    def test_postreopen_digest_mismatch(self):
        self.change("lifecycle.json", lambda value:value.update(reopened_state_sha256="c"*64))
        self.check(False, "preservation mismatch")

    def test_original_child_error_and_cleanup_error_are_fatal(self):
        self.change("load.launch.json", lambda value:value.update(exit_code=9))
        self.check(False, "client exit failure")
        self.change("load.launch.json", lambda value:value.update(exit_code=0))
        self.change("server.exit.json", lambda value:value.update(exit_code=137))
        self.check(False, "owned service cleanup failed")

    def test_child_identity_environment_and_hash_mismatch(self):
        original = json.loads((self.directory / "load.launch.json").read_text())
        for mutate in (lambda value:value["process"].update(gomaxprocs_unset=False), lambda value:value["process"].pop("start_ticks"), lambda value:value["process"].update(exe_sha256="f"*64), lambda value:value.update(receipt_sha256="f"*64), lambda value:value.update(campaign_id="stale")):
            with self.subTest(mutate=mutate):
                self.save(self.directory / "load.launch.json", copy.deepcopy(original))
                self.change("load.launch.json", mutate)
                self.check(False)

    def native(self):
        self.target = "treedb_native"
        new_dir = self.out / self.target
        self.directory.rename(new_dir)
        self.directory = new_dir
        self.manifest["server_builds"][self.target] = self.manifest["server_builds"].pop("treedb_mongo")
        self.manifest["targets"] = [self.target]
        self.manifest["formats"] = {self.target:"bson"}
        self.save(self.out / "server-build.json", dict(schema_version=1, servers=self.manifest["server_builds"]))
        self.save(self.out / "manifest.json", self.manifest)
        for name in ("runtime.json", "lifecycle.json"):
            self.change(name, lambda value:value.update(target=self.target))
        for stem in ("load", "run_1"):
            start,end=(1,2) if stem=="load" else (4,4)
            self.change(stem+".close.json", lambda receipt: receipt.update(db="treedb-native", native_flush=dict(applicable=True, flush_enabled=True, ack=1, dial=call_result(start,start), flush=call_result(start,end), connection_close=call_result(end,end))))

        self.live_fixture()

    def test_native_actual_schema_success_and_skipped_failed_wrong_ack(self):
        self.native()
        self.check(True)
        original = json.loads((self.directory / "load.close.json").read_text())
        for mutate in (lambda value:value["native_flush"].update(flush_enabled=False), lambda value:value["native_flush"].update(ack=2), lambda value:value["native_flush"]["dial"].update(attempted=False), lambda value:value["native_flush"]["flush"].update(attempted=False), lambda value:value["native_flush"]["flush"].update(success=False, error="EOF")):
            with self.subTest(mutate=mutate):
                self.save(self.directory / "load.close.json", copy.deepcopy(original))
                self.change("load.close.json", mutate)
                self.check(False)

    def test_actual_upstream_original_close_failure_receipt_rejected(self):
        self.native()
        receipt = json.loads((DATA / "native-close-failure.json").read_text())
        receipt.update(phase_id="offline-test-only:load", client_build_id="offline-fixture-build", declared_profile="command_wal_relaxed")
        self.save(self.directory / "load.close.json", receipt)
        self.change("load.launch.json", lambda value:value.update(receipt_sha256=COMPARE.digest(self.directory / "load.close.json")))
        self.check(False, "failed/skipped DB Close")

    def test_truncated_manifest_invalidates_previous_success(self):
        self.check(True)
        (self.out / "manifest.json").write_text("{")
        self.check(False)

    def test_lifecycle_cannot_precede_client_close_or_reopen(self):
        original = json.loads((self.directory / "lifecycle.json").read_text())
        for mutate in (lambda value:value.update(checkpoint=call_result(1,2)), lambda value:value.update(reopen=call_result(4,5)), lambda value:value.update(preclose_state_utc="2026-10-29T00:00:01Z"), lambda value:value.update(reopened_state_utc="2026-10-29T00:00:05Z")):
            with self.subTest(mutate=mutate):
                self.save(self.directory / "lifecycle.json", copy.deepcopy(original))
                self.change("lifecycle.json", mutate)
                self.check(False)

    def test_parse_only_rejects_unbound_build_patch(self):
        (self.out / "client.patch").write_text("unbound overlay")
        self.check(False, "client patch mismatch")

    def test_launcher_observes_real_child_pid_elf_and_unset_environment(self):
        env = os.environ.copy()
        env["GOMAXPROCS"] = "1"
        command = [sys.executable, str(ROOT / "ycsb_compare_validate.py"), "launch", str(self.directory), "probe", "--", sys.executable, "-c", "import os,time; print('GOMAXPROCS' in os.environ); time.sleep(.05)"]
        outcome = subprocess.run(command, env=env, capture_output=True, text=True)
        self.assertEqual(outcome.returncode, 0, outcome.stderr)
        record = json.loads((self.directory / "probe.launch.json").read_text())
        self.assertTrue(record["process"]["gomaxprocs_unset"])
        self.assertEqual(record["process"]["exe_sha256"], COMPARE.digest(sys.executable))
        self.assertGreater(record["process"]["start_ticks"], 0)
        self.assertEqual((self.directory / "probe.out").read_text().strip(), "False")
        self.assertIsNone(record["receipt_sha256"])

    def test_single_target_supplied_binaries_format_and_owned_error_cleanup(self):
        self.exercise_shell_cleanup(ready=True)

    def test_failed_readiness_retains_identity_and_cleans_actual_owned_process(self):
        self.exercise_shell_cleanup(ready=False)

    def test_shell_hanging_phase_deadline_cleans_client_and_server(self):
        self.exercise_shell_cleanup(ready=True,hang=True)

    def test_shell_requires_owner_observation_after_tcp_and_cleans_missing_owner(self):
        self.exercise_shell_cleanup(ready=True,owner_ready="missing")

    def test_shell_rejects_stale_owner_before_client_and_cleans_service(self):
        self.exercise_shell_cleanup(ready=True,owner_ready="stale")

    def test_shell_waits_for_partial_owner_receipt_to_complete(self):
        self.exercise_shell_cleanup(ready=True,owner_ready="partial")

    def test_shell_times_out_malformed_owner_receipt_and_cleans_service(self):
        self.exercise_shell_cleanup(ready=True,owner_ready="malformed")

    def test_shell_rejects_complete_invalid_owner_schema_before_client(self):
        self.exercise_shell_cleanup(ready=True,owner_ready="invalid-schema")

    def exercise_shell_cleanup(self, ready, hang=False, owner_ready="matching"):
        # Offline dependency stubs exercise launcher dispatch without a product DB.
        tools = self.out / "offline-tools"
        tools.mkdir()
        def executable(name, source):
            path = tools / name
            path.write_text(source)
            path.chmod(0o755)
            return path
        executable("git", "#!/usr/bin/env python3\nimport sys\nif 'rev-parse' in sys.argv: print('"+COMPARE.PRODUCER+"')\n")
        executable("go", "#!/bin/sh\necho offline-buildinfo\n")
        marker = self.out / "server-marker.json"
        client_marker = self.out / "client-marker.json"
        source = tools / "server.c"
        source.write_text(r"""#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
void done(int s) { _exit(0); }
int main(int argc,char **argv) {
 char *addr=NULL;for(int i=1;i+1<argc;i++)if(!strcmp(argv[i],"-addr"))addr=argv[i+1];
 FILE *f=fopen(MARKER,"w");fprintf(f,"{\"env_unset\":%s}",getenv("GOMAXPROCS")?"false":"true");fclose(f);
 int fd=socket(AF_INET,SOCK_STREAM,0);struct sockaddr_in a={0};a.sin_family=AF_INET;a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);a.sin_port=htons(atoi(strrchr(addr,':')+1));
 if(bind(fd,(struct sockaddr*)&a,sizeof(a)))return 4;
 if(READY)listen(fd,5);else close(fd);
 signal(SIGTERM,done);while(1)pause();
}""")
        server = tools / "native-server"
        subprocess.run(["cc", str(source), "-o", str(server), '-DMARKER='+json.dumps(str(marker)), "-DREADY="+str(int(ready))],check=True,capture_output=True)
        client = executable("go-ycsb", "#!/usr/bin/env python3\nimport json,os,sys,time,signal\njson.dump(dict(argv=sys.argv[1:],env_unset='GOMAXPROCS' not in os.environ),open("+repr(str(client_marker))+",'w'))\n"+("signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)" if hang else "time.sleep(.1); sys.exit(37)")+"\n")
        import socket
        with socket.socket() as sock:
            sock.bind(("127.0.0.1",0))
            port = sock.getsockname()[1]
        build = dict(self.manifest["client"], exe_sha256=COMPARE.digest(client))
        build_path = self.out / "offline-build.json"
        self.save(build_path, build)
        server_build_path = self.out / "offline-server-build.json"
        self.save(server_build_path, dict(schema_version=1, servers={"treedb_native":dict(schema_version=1,version="candidate",source_commit=COMPARE.PRODUCER,exe_sha256=COMPARE.digest(server),expected_owner="cow",source_dir=str(tools),source_patch_sha256=COMPARE.digest(self.out/"client.patch"))}))
        execution = self.out / "execution"
        environment = os.environ.copy()
        environment.update(PATH=str(tools)+os.pathsep+environment["PATH"], GOMAXPROCS="1", PARSE_ONLY="false", BUILD="false", BUILD_GOYCSB="false", OUT_DIR=str(execution), TARGETS="treedb_native", MONGODB_MODE="skip", TREEDB_NATIVE_DOCUMENT_FORMAT="json", TREEDB_NATIVE_SERVER_BIN=str(server), TREEDB_MONGO_GATEWAY_BIN=str(tools/"must-not-launch"), NATIVE_ADDR=f"127.0.0.1:{port}", CLIENT_BUILD_MANIFEST=str(build_path), SERVER_BUILD_MANIFEST=str(server_build_path), GOYCSB_DIR=str(tools), GOYCSB_BIN=str(client), FIXTURE_BIN=str(client), READINESS_TIMEOUT="1", PHASE_TIMEOUT_SECONDS="1", CHILD_CLEANUP_TIMEOUT_SECONDS="1")
        # The offline service binds TCP first; a separate synthetic observer
        # supplies actual proc/data identities later, without running a product DB.
        import threading,time
        writer_errors=[]
        def publish_owner():
            if not ready or owner_ready=="missing":return
            try:
                deadline=time.monotonic()+5
                while True:
                    try:
                        observed_launch=COMPARE.read(execution/"treedb_native/server.launch.json")
                        COMPARE.process(observed_launch["process"])
                        break
                    except (OSError,ValueError,KeyError,TypeError):
                        if time.monotonic()>=deadline:raise TimeoutError("offline launch identity missing")
                        time.sleep(.01)
                time.sleep(.2)
                data=pathlib.Path(observed_launch["data_dir"]).stat()
                value=dict(schema_version=1,kind="live_owner_observation",owner_id="1"*32,process=observed_launch["process"],data_dir=observed_launch["data_dir"],data_device=data.st_dev,data_inode=data.st_ino,profile=self.manifest["profile"],backend="cow",capture_error="",observed_gomaxprocs=1,stats={"treedb.profile.resolved":self.manifest["profile"],"treedb.cache.memtable_mode":"cow_btree","treedb.cache.cow.txn.logical_seq":"0"},observed_utc=COMPARE.now())
                if owner_ready=="stale":value["process"]["start_ticks"]+=1
                if owner_ready=="invalid-schema":value["schema_version"]=True
                evidence=execution/"treedb_native/server.evidence/owner.open.json"
                # Match Go's actual O_EXCL-create-then-write publication, not
                # an atomic rename that would hide incomplete-receipt races.
                with evidence.open("x") as stream:
                    raw=json.dumps(value)
                    if owner_ready in ("partial","malformed"):
                        stream.write(raw[:18]);stream.flush()
                        if owner_ready=="malformed":return
                        time.sleep(.2)
                        stream.write(raw[18:])
                    else:stream.write(raw)
            except BaseException as error:writer_errors.append(error)
        writer=threading.Thread(target=publish_owner)
        writer.start()
        try:outcome = subprocess.run(["bash", str(ROOT / "ycsb_compare_mongodb_treedb.sh")],env=environment,text=True,capture_output=True,timeout=20)
        finally:writer.join(timeout=6)
        self.assertFalse(writer.is_alive())
        self.assertEqual(writer_errors,[])
        expected=(124 if hang else 37) if ready else 1
        if ready and owner_ready not in ("matching","partial"):expected=3
        self.assertEqual(outcome.returncode,expected,outcome.stderr)
        launch = json.loads((execution/"treedb_native/server.launch.json").read_text())
        self.assertFalse(pathlib.Path(f"/proc/{launch['process']['pid']}").exists())
        self.assertEqual(json.loads((execution/"treedb_native/server.exit.json").read_text())["exit_code"],0)
        if not ready:
            self.assertIn("timed out waiting",outcome.stderr)
            self.assertFalse(client_marker.exists())
            return
        if owner_ready not in ("matching","partial"):
            self.assertFalse(client_marker.exists())
            failure=COMPARE.read(execution/"treedb_native/server.readiness.failure.json")
            self.assertEqual(failure["kind"],"owner_readiness_failure")
            self.assertFalse((execution/"treedb_native/server.ready.json").exists())
            return
        readiness=COMPARE.read(execution/"treedb_native/server.ready.json")
        observation=COMPARE.read(execution/"treedb_native/server.evidence/owner.open.json")
        self.assertGreaterEqual(COMPARE.timestamp(readiness["ready_utc"]),COMPARE.timestamp(observation["observed_utc"]))
        client_launch=COMPARE.read(execution/"treedb_native/load.launch.json")
        self.assertGreaterEqual(COMPARE.timestamp(client_launch["start_utc"]),COMPARE.timestamp(readiness["ready_utc"]))
        self.assertTrue(json.loads(marker.read_text())["env_unset"])
        child = json.loads(client_marker.read_text())
        self.assertTrue(child["env_unset"])
        if hang:
            failure=COMPARE.read(execution/"treedb_native/load.failure.json")
            self.assertTrue(failure["timed_out"])
            self.assertTrue(failure["cleanup"]["reaped"])
            self.assertEqual(failure["cleanup"]["exit_code"],-9)
            self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())
        self.assertEqual(child["argv"][:2],["load","treedb-native"])
        self.assertIn("treedb.document_format=json",child["argv"])
        self.assertIn("treedb.ack=visible",child["argv"])
        self.assertIn("treedb.flush_on_close=true",child["argv"])
        self.assertFalse((execution/"treedb_mongo").exists())
        self.assertFalse((execution/"mongodb").exists())
        self.assertEqual(json.loads((execution/"treedb_native/server.exit.json").read_text())["exit_code"],0)
        self.assertEqual(json.loads((execution/"validation.json").read_text())["status"],"invalid")

    def test_version_bound_legacy_and_candidate_owners(self):
        # The identical target API can be served by different historical owners.
        build = self.manifest["server_builds"][self.target]
        build.update(version="baseline",source_commit="e"*40,expected_owner="legacy")
        self.save(self.out/"manifest.json",self.manifest)
        self.save(self.out/"server-build.json",dict(schema_version=1,servers=self.manifest["server_builds"]))
        for name in ("runtime.json","lifecycle.json"):
            self.change(name,lambda value:value.update(version="baseline",source_commit="e"*40))
        self.change("runtime.json",lambda value:value.update(backend="legacy"))
        self.live_fixture()
        self.check(True)
        self.change("runtime.json",lambda value:value.update(backend="cow"))
        self.check(False,"actual owner/backend")
        self.change("runtime.json",lambda value:value.update(backend="legacy",source_commit="f"*40))
        self.check(False,"server version/source mismatch")

    def test_wrong_version_server_elf_rejected(self):
        self.change("server.launch.json",lambda value:value["process"].update(exe_sha256="f"*64))
        self.check(False,"server ELF binding mismatch")

    def test_run_requires_positive_read_and_update_and_exact_actual_sum(self):
        path = self.directory/"run_1.out"
        original = path.read_text()
        without_update = "\n".join(line for line in original.splitlines() if not line.startswith("UPDATE"))+"\n"
        controls = [without_update.replace("Count: 9515","Count: 10000"), original.replace("Count: 9515","Count: 10000").replace("Count: 485","Count: 0"), original.replace("Count: 485","Count: 484"), original.replace("UPDATE - Takes(s): 0.5, Count: 485", "UPDATE - Takes(s): 0.5, Count: malformed")]
        for content in controls:
            with self.subTest(content=content[-140:]):
                path.write_text(content)
                self.change("run_1.launch.json",lambda value:value.update(output_sha256=COMPARE.digest(path)))
                self.check(False)
        # A different random split is accepted, with no invented fixed 95/5 totals.
        path.write_text(original.replace("Count: 9515","Count: 9487").replace("Count: 485","Count: 513"))
        self.change("run_1.launch.json",lambda value:value.update(output_sha256=COMPARE.digest(path)))
        self.check(True)

    def test_pidfd_cleanup_refuses_stale_start_and_wrong_elf_without_signal(self):
        env=os.environ.copy(); env.pop("GOMAXPROCS",None)
        child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"],env=env)
        identity=COMPARE.observe(child.pid)
        try:
            for mutate in (lambda value:value.update(start_ticks=value["start_ticks"]+1),lambda value:value.update(exe_sha256="f"*64)):
                wrong=dict(identity); mutate(wrong)
                self.save(self.directory/"cleanup-probe.json",dict(process=wrong))
                outcome=subprocess.run([sys.executable,str(ROOT/"ycsb_compare_validate.py"),"stop",str(self.directory/"cleanup-probe.json"),"--timeout",".05"],capture_output=True,text=True)
                self.assertNotEqual(outcome.returncode,0)
                self.assertIn("refusing signal",outcome.stderr)
                self.assertIsNone(child.poll(),"mismatched process must remain unsignaled")
        finally:
            COMPARE.terminate_owned(identity,timeout=.05)
            child.wait(timeout=2)

    def test_pidfd_cleanup_revalidates_before_kill(self):
        from unittest import mock
        identity=dict(self.process)
        changed=dict(identity,exe_sha256="f"*64)
        with mock.patch.object(COMPARE.os,"pidfd_open",return_value=55), mock.patch.object(COMPARE.os,"close"), mock.patch.object(COMPARE.select,"select",return_value=([],[],[])), mock.patch.object(COMPARE,"observe_core",side_effect=[{k:identity[k] for k in ("pid","start_ticks","exe_sha256")},{k:changed[k] for k in ("pid","start_ticks","exe_sha256")}]), mock.patch.object(COMPARE.signal,"pidfd_send_signal") as send:
            with self.assertRaisesRegex(ValueError,"refusing signal"):
                COMPARE.terminate_owned(identity,timeout=0)
            send.assert_called_once_with(55,COMPARE.signal.SIGTERM)

    def test_wrong_executable_refuses_clients_but_retains_safe_cleanup_identity(self):
        env=os.environ.copy();env.pop("GOMAXPROCS",None)
        child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(30)"],env=env)
        path=self.directory/"wrong-exe.launch.json"
        try:
            outcome=subprocess.run([sys.executable,str(ROOT/"ycsb_compare_validate.py"),"observe",str(child.pid),str(self.directory),str(path),"--await-exec","/bin/true"],text=True,capture_output=True)
            self.assertNotEqual(outcome.returncode,0)
            self.assertIn("never matched requested ELF",outcome.stderr)
            identity=json.loads(path.read_text())["process"]
            self.assertEqual(identity,COMPARE.observe_core(child.pid))
            COMPARE.terminate_owned(identity,.05)
            child.wait(timeout=2)
        finally:
            if child.poll() is None:
                COMPARE.terminate_owned(COMPARE.observe_core(child.pid),.05);child.wait(timeout=2)

    def test_observation_error_retains_cleanup_before_environment_read(self):
        from unittest import mock
        env=os.environ.copy();env.pop("GOMAXPROCS",None)
        child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(30)"],env=env)
        path=self.directory/"failed-observation.launch.json"
        try:
            with mock.patch.object(COMPARE,"observe",side_effect=PermissionError("injected environ read failure")):
                with self.assertRaises(PermissionError):
                    COMPARE.observe_owned(child.pid,str(self.directory),path)
            identity=json.loads(path.read_text())["process"]
            self.assertEqual(identity,COMPARE.observe_core(child.pid))
            COMPARE.terminate_owned(identity,.05);child.wait(timeout=2)
        finally:
            if child.poll() is None:
                COMPARE.terminate_owned(COMPARE.observe_core(child.pid),.05);child.wait(timeout=2)

    def test_required_live_schema_and_source_overlay_cannot_be_omitted(self):
        self.manifest.pop("lifecycle_contract")
        self.save(self.out/"manifest.json",self.manifest)
        self.check(False,"live lifecycle contract 2")
        self.manifest["lifecycle_contract"]=2
        self.save(self.out/"manifest.json",self.manifest)
        (self.directory/"server-source.patch").write_text("unbound instrumentation")
        self.check(False,"instrumentation source overlay mismatch")

    def test_raw_owner_capture_and_original_close_fail_closed(self):
        path=self.directory/"server.evidence/owner.open.json"
        original=COMPARE.read(path)
        for mutate in (lambda v:v.update(backend="legacy"),lambda v:v.update(capture_error="forged startup label"),lambda v:v["stats"].pop("treedb.cache.cow.txn.logical_seq"),lambda v:v["process"].update(start_ticks=501)):
            with self.subTest(mutate=mutate):
                value=copy.deepcopy(original);mutate(value);self.save(path,value)
                self.check(False)
        self.save(path,original)
        path=self.directory/"server.evidence/owner.close.json"
        value=COMPARE.read(path);value["close"].update(success=False,error="original owning Close failed");self.save(path,value)
        self.check(False,"original owner Close")

    def test_reopen_requires_independent_process_owner_and_same_data_inode(self):
        path=self.directory/"reopened.evidence/owner.open.json"
        original=COMPARE.read(path)
        for mutate in (lambda v:v.update(owner_id="1"*32),lambda v:v.update(data_inode=900),lambda v:v["process"].update(exe_sha256="f"*64)):
            with self.subTest(mutate=mutate):
                value=copy.deepcopy(original);mutate(value);self.save(path,value)
                self.check(False)
        path.unlink()
        self.check(False,"owner.open.json")

    def test_actual_canonical_random_rows_reject_malformed_controls_after_rebinding_hash(self):
        path=self.directory/"snapshot_reopened.json"
        original=COMPARE.read(path)
        mutations=(lambda v:v["rows"].pop(),lambda v:v["rows"][0]["fields"].pop("field0"),lambda v:v["rows"][0]["fields"]["field0"].update(subtype=False),lambda v:v["rows"][0]["fields"]["field0"].update(base64=base64.b64encode(b"x"*99).decode()),lambda v:v["rows"][0]["fields"]["field0"].update(base64=base64.b64encode(b"y"*100).decode()),lambda v:v["rows"][0].update(id="unexpected-key"))
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                value=copy.deepcopy(original);mutate(value);self.save(path,value)
                self.change("snapshot_reopened.close.json",lambda v:v["artifacts_sha256"].update({path.name:COMPARE.digest(path)}))
                self.check(False)

    def test_snapshot_actual_child_close_and_checkpoint_must_succeed(self):
        for name,mutate in (("snapshot_preclose.close.json",lambda v:v.update(checkpoint_attempted=False)),("snapshot_loaded.close.json",lambda v:v.update(close_error="original client close failed")),("snapshot_reopened.close.json",lambda v:v.update(pid=9999)),("fixture_verify.close.json",lambda v:v.update(result_error="verify failed"))):
            with self.subTest(name=name):
                original=COMPARE.read(self.directory/name)
                self.change(name,mutate)
                self.check(False)
                self.save(self.directory/name,original)
                self.change(name,lambda v:None)

    def test_native_deterministic_checkpoint_requires_actual_timed_result(self):
        self.native()
        self.check(True)
        for value in (None, dict(call_result(11,12),attempted=False),
                      dict(call_result(11,12),success=False,error="actual failure"),
                      dict(call_result(11,12),elapsed_ns=1_000_000_000.0),
                      dict(call_result(11,12),success=1),call_result(0,1)):
            with self.subTest(value=value):
                self.reject_mutations([(self.directory/"fixture_write.close.json",lambda v,x=value:v.update(checkpoint=x))])

    def test_boolean_schema_and_unbound_runtime_are_rejected(self):
        self.change("runtime.json",lambda v:v.update(observed_gomaxprocs=999))
        self.check(False,"aggregate runtime")
        self.change("runtime.json",lambda v:v.update(observed_gomaxprocs=16))
        path=self.directory/"snapshot_reopened.json"
        value=COMPARE.read(path);value["schema_version"]=True;self.save(path,value)
        self.change("snapshot_reopened.close.json",lambda v:v["artifacts_sha256"].update({path.name:COMPARE.digest(path)}))
        self.check(False,"random snapshot source mismatch")

    def test_mongo_checkpoint_requires_raw_owner_control(self):
        (self.directory/"server.evidence/owner.checkpoint.json").unlink()
        self.check(False)

    def test_mongo_checkpoint_rejects_missing_endpoint_and_wrong_schema(self):
        path=self.directory/"server.evidence/owner.checkpoint-control.json"
        endpoint=COMPARE.read(path)
        for mutate in (lambda v:v.update(schema_version=2),lambda v:v.update(owner_id="e"*32),lambda v:v.update(address="0.0.0.0:12345"),lambda v:v.update(address=[])):
            value=dict(endpoint);mutate(value);self.save(path,value)
            self.check(False)
        path.unlink()
        self.check(False)

    def test_mongo_checkpoint_rejects_failed_calls_and_wrong_order(self):
        original=COMPARE.read(self.directory/"server.evidence/owner.checkpoint.json")
        controls=[(lambda v:v["collections_flush_all"].update(success=False,error="injected flush failure"),"FlushAll"),
                  (lambda v:v["backend_checkpoint"].update(success=False,error="injected checkpoint failure"),"Checkpoint"),
                  (lambda v:v.update(collections_flush_all=call_result(4,5),backend_checkpoint=call_result(4,4)),"ordering")]
        for mutate,message in controls:
            proof=json.loads(json.dumps(original));mutate(proof)
            self.save(self.directory/"server.evidence/owner.checkpoint.json",proof)
            self.change("snapshot_preclose.close.json",lambda v:v.update(owner_checkpoint=proof))
            self.check(False,message)

    def test_mongo_checkpoint_rejects_forged_identity_and_snapshot(self):
        original=COMPARE.read(self.directory/"server.evidence/owner.checkpoint.json")
        controls=[lambda v:v["owner"]["process"].update(start_ticks=999),
                  lambda v:v["owner"].update(data_inode=999),
                  lambda v:v["owner"].update(observed_gomaxprocs=True),
                  lambda v:v["owner"]["stats"].update({"treedb.cache.cow.txn.logical_seq":1}),
                  lambda v:v["request"].update(owner_id="e"*32),
                  lambda v:v["request"]["client"].update(pid=999),
                  lambda v:v["request"].update(snapshot_sha256="e"*64),
                  lambda v:v["request"].update(source_id="e"*40),
                  lambda v:v["request"].update(fixture_id="stale-campaign")]
        for mutate in controls:
            proof=json.loads(json.dumps(original));mutate(proof)
            self.save(self.directory/"server.evidence/owner.checkpoint.json",proof)
            self.save(self.directory/"server.evidence/owner.checkpoint-request.json",proof["request"])
            self.change("snapshot_preclose.close.json",lambda v:v.update(owner_checkpoint=proof))
            self.check(False)

    def test_checkpoint_nested_identity_types_are_strict_even_when_values_compare_equal(self):
        original=COMPARE.read(self.directory/"server.evidence/owner.checkpoint.json")
        controls=[lambda v:v["owner"].update(schema_version=True),
                  lambda v:v["owner"].update(data_inode=21.0),
                  lambda v:v["owner"].update(data_device=10.0),
                  lambda v:v["owner"].update(observed_gomaxprocs=16.0),
                  lambda v:v["owner"]["process"].update(pid=1234.0),
                  lambda v:v["owner"]["process"].update(start_ticks=500.0),
                  lambda v:v["owner"]["process"].update(gomaxprocs_unset=1),
                  lambda v:v["request"]["client"].update(pid=4321.0),
                  lambda v:v["request"]["client"].update(start_ticks=500.0),
                  lambda v:v["request"]["client"].update(gomaxprocs_unset=1)]
        for mutate in controls:
            proof=json.loads(json.dumps(original));mutate(proof)
            self.save(self.directory/"server.evidence/owner.checkpoint.json",proof)
            self.save(self.directory/"server.evidence/owner.checkpoint-request.json",proof["request"])
            self.change("snapshot_preclose.close.json",lambda v:v.update(owner_checkpoint=proof))
            self.check(False)

    def test_checkpoint_copy_comparison_preserves_json_number_types(self):
        self.change("snapshot_preclose.close.json",lambda v:v["owner_checkpoint"]["owner"].update(data_inode=21.0))
        self.check(False,"data directory inode")

    def test_executable_hash_ignores_read_atime_but_rejects_stable_identity_changes(self):
        from types import SimpleNamespace
        from unittest import mock
        fields=dict(st_dev=1,st_ino=2,st_size=3,st_mtime_ns=4,st_ctime_ns=5,st_atime_ns=6)
        for change,accepted in ((dict(st_atime_ns=7),True),(dict(st_size=7),False),(dict(st_mtime_ns=7),False),(dict(st_ctime_ns=7),False),(dict(st_ino=7),False)):
            COMPARE.EXE_HASH_CACHE.clear()
            before=SimpleNamespace(**fields);after=SimpleNamespace(**dict(fields,**change))
            with mock.patch.object(COMPARE.os,"stat",side_effect=[before,after]),mock.patch.object(COMPARE,"digest",return_value="f"*64):
                if accepted:self.assertEqual(COMPARE.exe_digest("fixture"),"f"*64)
                else:
                    with self.assertRaisesRegex(ValueError,"executable changed"):
                        COMPARE.exe_digest("fixture")
                    self.assertEqual(COMPARE.EXE_HASH_CACHE,{})
        COMPARE.EXE_HASH_CACHE.clear()

    def test_short_lived_large_elf_child_is_prehashed_before_observation(self):
        binary=self.out/"large-python"
        shutil.copyfile(sys.executable,binary)
        with binary.open("ab") as stream:stream.write(b"\0"*(24*1024*1024))
        binary.chmod(0o755)
        command=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch",str(self.directory),"large-elf-probe","--",str(binary),"-c","import time;time.sleep(.03)"]
        outcome=subprocess.run(command,text=True,capture_output=True)
        self.assertEqual(outcome.returncode,0,outcome.stderr)
        observed=COMPARE.read(self.directory/"large-elf-probe.launch.json")
        self.assertEqual(observed["process"]["exe_sha256"],COMPARE.digest(binary))
        self.assertTrue(observed["process"]["gomaxprocs_unset"])

    def test_actual_child_is_stopped_after_initial_qualification_observation_failure(self):
        import argparse
        import signal
        from unittest import mock
        previous=signal.getsignal(signal.SIGTERM)
        try:
            args=argparse.Namespace(directory=str(self.directory),stem="observation-failure",command=[sys.executable,"-c","import time;time.sleep(30)"])
            with mock.patch.object(COMPARE,"observe",side_effect=PermissionError("injected qualification read failure")):
                with self.assertRaises(PermissionError):COMPARE.launch(args)
            cleanup=COMPARE.read(self.directory/"observation-failure.cleanup.json")
            self.assertFalse(pathlib.Path(f"/proc/{cleanup['process']['pid']}").exists())
            self.assertFalse((self.directory/"observation-failure.launch.json").exists())
        finally:signal.signal(signal.SIGTERM,previous)

    def test_hanging_child_deadlines_retain_failure_kill_and_reap(self):
        controls=(("child-timeout",.15,.05,.5,124),("outer-timeout",30,.05,.15,3),("outer-during-cleanup",.1,.2,.12,124))
        for stem,timeout,cleanup,outer,expected in controls:
            with self.subTest(stem=stem):
                command=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch","--timeout",str(timeout),"--cleanup-timeout",str(cleanup),"--outer-timeout",str(outer),str(self.directory),stem,"--",sys.executable,"-c","import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)"]
                outcome=subprocess.run(command,text=True,capture_output=True,timeout=4)
                self.assertEqual(outcome.returncode,expected,outcome.stderr)
                failure=COMPARE.read(self.directory/(stem+".failure.json"))
                self.assertTrue(failure["timed_out"])
                self.assertTrue(failure["pidfd_pinned"])
                self.assertTrue(failure["cleanup"]["success"])
                self.assertTrue(failure["cleanup"]["reaped"])
                self.assertEqual(failure["cleanup"]["exit_code"],-9)
                self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())
                self.assertFalse((self.directory/(stem+".close.json")).exists(),"must never manufacture producer close receipt")
        self.check(False,"retained launcher/helper failure")

    def test_hanging_helper_child_is_bounded_and_reaped(self):
        binary=self.out/"hanging-helper"
        binary.write_text("#!/usr/bin/env python3\nimport signal,time\nsignal.signal(signal.SIGTERM,signal.SIG_IGN)\ntime.sleep(30)\n")
        binary.chmod(0o755)
        self.manifest["fixture"]["binary"]=str(binary)
        self.save(self.out/"manifest.json",self.manifest)
        command=[sys.executable,str(ROOT/"ycsb_compare_live.py"),"snapshot",str(self.directory),self.target,"--stage","loaded","--addr","127.0.0.1:1","--timeout",".15","--cleanup-timeout",".05","--outer-timeout",".5"]
        # Use a fresh phase name: delete the synthetic phase artifacts only.
        for suffix in (".out",".err",".launch.json",".close.json"):(self.directory/("snapshot_loaded"+suffix)).unlink(missing_ok=True)
        outcome=subprocess.run(command,text=True,capture_output=True,timeout=4)
        self.assertNotEqual(outcome.returncode,0)
        self.assertTrue((self.directory/"snapshot_loaded.failure.json").exists(),outcome.stderr)
        failure=COMPARE.read(self.directory/"snapshot_loaded.failure.json")
        self.assertTrue(failure["timed_out"])
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())

    def test_first_core_observation_failure_uses_pinned_direct_child_for_cleanup(self):
        import argparse
        from unittest import mock
        args=argparse.Namespace(directory=str(self.directory),stem="core-observation-failure",command=[sys.executable,"-c","import time;time.sleep(30)"],cleanup_timeout=.05)
        with mock.patch.object(COMPARE,"observe_core",side_effect=PermissionError("injected first procfs failure")):
            with self.assertRaises(PermissionError):COMPARE.launch(args)
        failure=COMPARE.read(self.directory/"core-observation-failure.failure.json")
        self.assertIsNone(failure["process"])
        self.assertTrue(failure["pidfd_pinned"])
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())

    def test_measured_phase_overlap_rejected_with_individually_valid_close_bounds(self):
        # Both runs have valid receipt intervals, and their maximum end precedes
        # checkpoint. Only their actual overlap makes this packet invalid.
        half="2026-10-29T00:00:04.500000Z"
        self.change("run_1.launch.json",lambda v:v.update(end_utc=half))
        checkpoint=call_result(4,5)
        checkpoint.update(start_utc=half,elapsed_ns=500_000_000)
        self.change("snapshot_preclose.close.json",lambda v:v.update(started_utc=half,checkpoint=checkpoint))
        self.change("snapshot_preclose.launch.json",lambda v:v.update(start_utc=half))
        self.change("lifecycle.json",lambda v:v.update(checkpoint=checkpoint))
        proof=COMPARE.read(self.directory/"server.evidence/owner.checkpoint.json")
        point=call_result(4,4);point.update(start_utc=half,end_utc=half)
        proof.update(result=checkpoint,collections_flush_all=point,backend_checkpoint=checkpoint)
        proof["owner"]["observed_utc"]=half
        self.save(self.directory/"server.evidence/owner.checkpoint.json",proof)
        self.change("snapshot_preclose.close.json",lambda v:v.update(owner_checkpoint=proof))
        self.manifest["run_repeats"]=2
        self.save(self.out/"manifest.json",self.manifest)
        for suffix in (".out",".close.json",".launch.json"):
            shutil.copyfile(self.directory/("run_1"+suffix),self.directory/("run_2"+suffix))
        phase_id="offline-test-only:run_2"
        self.change("run_2.launch.json",lambda v:v.update(phase_id=phase_id))
        self.change("run_2.close.json",lambda v:v.update(phase_id=phase_id))
        for name in ("runtime.json","lifecycle.json"):
            self.change(name,lambda v:v["phase_ids"].append(phase_id))
        self.check(False,"measured phases overlap/out of order")
        point=call_result(4,4);point.update(start_utc=half,end_utc=half)
        self.change("run_2.close.json",lambda v:v.update(db_close=point,workload_close=point))
        self.change("run_2.launch.json",lambda v:v.update(start_utc=half))
        self.check(True)

    def test_each_measured_phase_start_must_follow_original_owner_open(self):
        self.change("load.launch.json",lambda v:v.update(start_utc="2026-10-28T23:59:59Z"))
        self.check(False,"measured phase outside complete observed owner lifetime")

    def test_helpers_must_finish_before_actual_before_close_observation(self):
        for prefix,utc in (("server","2026-10-29T00:00:04Z"),("reopened","2026-10-29T00:00:07Z"),("fixture-write","2026-10-29T00:00:11Z"),("fixture-reopened","2026-10-29T00:00:16Z")):
            with self.subTest(prefix=prefix):
                path=self.directory/(prefix+".evidence/owner.before-close.json")
                original=COMPARE.read(path)
                value=dict(original,observed_utc=utc);self.save(path,value)
                self.check(False,"outside complete observed owner lifetime")
                self.save(path,original)

    def test_aggregate_bounds_must_match_original_calls_and_compare_after_snapshot(self):
        original=COMPARE.read(self.directory/"lifecycle.json")
        for mutate,message in ((lambda v:v.update(reopen=call_result(6,7)),"reopen bounds"),(lambda v:v.update(preclose_state_utc="2026-10-29T00:00:04.500000Z"),"snapshot timestamps"),(lambda v:v.update(random_state_preservation=call_result(7,9)),"comparison precedes")):
            with self.subTest(message=message):
                self.save(self.directory/"lifecycle.json",copy.deepcopy(original))
                self.change("lifecycle.json",mutate)
                self.check(False,message)

    def test_success_exit_cannot_waive_timeout_or_failure_receipt(self):
        self.change("load.launch.json",lambda v:v.update(timed_out=True))
        self.check(False)
        self.change("load.launch.json",lambda v:v.update(timed_out=False))
        self.save(self.directory/"helper-assemble.failure.json",dict(schema_version=1,error="outer assembly deadline"))
        self.check(False,"retained launcher/helper failure")

    def test_nonfinite_or_nonpositive_deadlines_are_rejected_before_spawn(self):
        for timeout in ("nan","inf","0","-1"):
            with self.subTest(timeout=timeout):
                command=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch","--timeout",timeout,str(self.directory),"invalid-deadline","--",sys.executable,"-c","raise SystemExit(0)"]
                outcome=subprocess.run(command,text=True,capture_output=True)
                self.assertNotEqual(outcome.returncode,0)
                self.assertIn("finite and positive",outcome.stderr)
                self.assertFalse((self.directory/"invalid-deadline.cleanup.json").exists())

    def test_timeout_remains_failure_when_term_returns_success(self):
        command=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch","--timeout",".15","--cleanup-timeout",".1",str(self.directory),"graceful-timeout","--",sys.executable,"-c","import signal,sys,time;signal.signal(signal.SIGTERM,lambda *_:sys.exit(0));time.sleep(30)"]
        outcome=subprocess.run(command,text=True,capture_output=True,timeout=3)
        self.assertEqual(outcome.returncode,124,outcome.stderr)
        launch=COMPARE.read(self.directory/"graceful-timeout.launch.json")
        self.assertEqual(launch["exit_code"],0)
        self.assertTrue(launch["timed_out"])
        failure=COMPARE.read(self.directory/"graceful-timeout.failure.json")
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())

    def test_measured_phases_cannot_overlap_across_targets(self):
        from unittest import mock
        target="mongodb"
        other=self.out/target
        shutil.copytree(self.directory,other)
        self.manifest["targets"].append(target)
        self.manifest["formats"][target]="bson"
        self.save(self.out/"manifest.json",self.manifest)
        for stem in ("load","run_1"):
            receipt=COMPARE.read(other/(stem+".close.json"))
            receipt["phase_id"] += ":other-target"
            self.save(other/(stem+".close.json"),receipt)
            launch=COMPARE.read(other/(stem+".launch.json"))
            launch.update(phase_id=receipt["phase_id"],receipt_sha256=COMPARE.digest(other/(stem+".close.json")))
            self.save(other/(stem+".launch.json"),launch)
        # Isolate the campaign-wide sequencing gate; no server claims are made.
        with mock.patch.object(COMPARE,"server_evidence"), mock.patch("sys.stderr"):
            self.assertEqual(COMPARE.validate(self.out),3)
        errors=COMPARE.read(self.out/"validation.json")["errors"]
        self.assertTrue(any("measured phases overlap/out of order" in error for error in errors))

    def test_pidfd_acquisition_failure_reaps_exact_unreaped_direct_child(self):
        import argparse
        import time
        from unittest import mock
        ready=self.directory/"pin-failure.ready"
        command=[sys.executable,"-c","import pathlib,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);pathlib.Path("+repr(str(ready))+").write_text('ready');time.sleep(30)"]
        observed=[]
        def fail_pin(pid):
            observed.append(pid)
            deadline=time.monotonic()+2
            while not ready.exists() and time.monotonic()<deadline:time.sleep(.005)
            self.assertTrue(ready.exists())
            raise OSError("injected pidfd acquisition failure")
        args=argparse.Namespace(directory=str(self.directory),stem="pin-failure",command=command,cleanup_timeout=.05)
        with mock.patch.object(COMPARE.os,"pidfd_open",side_effect=fail_pin):
            with self.assertRaisesRegex(OSError,"injected pidfd acquisition failure"):COMPARE.launch(args)
        failure=COMPARE.read(self.directory/"pin-failure.failure.json")
        self.assertEqual(failure["pid"],observed[0])
        self.assertFalse(failure["pidfd_pinned"])
        self.assertEqual(failure["cleanup"]["method"],"popen-direct-child-unreaped")
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertEqual(failure["cleanup"]["exit_code"],-9)
        self.assertIn("injected pidfd acquisition failure",failure["error"])
        self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())
        self.assertFalse((self.directory/"pin-failure.close.json").exists())

    def test_real_outer_alarm_during_pin_acquisition_is_deferred_until_guarded(self):
        import argparse
        import signal
        import time
        from unittest import mock
        ready=self.directory/"slow-pin.ready"
        original_pin=COMPARE.os.pidfd_open
        def slow_pin(pid):
            deadline=time.monotonic()+2
            while not ready.exists() and time.monotonic()<deadline:time.sleep(.005)
            self.assertTrue(ready.exists())
            time.sleep(.08)
            return original_pin(pid)
        command=[sys.executable,"-c","import pathlib,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);pathlib.Path("+repr(str(ready))+").write_text('ready');time.sleep(30)"]
        args=argparse.Namespace(directory=str(self.directory),stem="slow-pin",command=command,cleanup_timeout=.05)
        with mock.patch.object(COMPARE.os,"pidfd_open",side_effect=slow_pin):
            with self.assertRaises(TimeoutError):
                with COMPARE.outer_deadline(.04):COMPARE.launch(args)
        failure=COMPARE.read(self.directory/"slow-pin.failure.json")
        self.assertTrue(failure["timed_out"])
        self.assertIn(signal.SIGALRM,failure["startup_cancellation_signals"])
        self.assertTrue(failure["pidfd_pinned"])
        self.assertTrue(failure["cleanup"]["reaped"])
        self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())

    def test_real_cancellation_during_cleanup_waits_for_kill_reap_and_failure_receipt(self):
        import signal
        import time
        for signum in (signal.SIGTERM,signal.SIGINT):
            with self.subTest(signum=signum):
                stem="cleanup-cancellation-"+str(signum)
                marker=self.directory/(stem+".term-seen")
                source="import pathlib,signal,time;signal.signal(signal.SIGTERM,lambda *_:pathlib.Path("+repr(str(marker))+").write_text('term'));time.sleep(30)"
                command=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch","--timeout",".2","--cleanup-timeout",".4","--outer-timeout","3",str(self.directory),stem,"--",sys.executable,"-c",source]
                launcher=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                pin=os.pidfd_open(launcher.pid)
                try:
                    deadline=time.monotonic()+2
                    while not marker.exists() and time.monotonic()<deadline:time.sleep(.005)
                    self.assertTrue(marker.exists(),"child must receive cleanup TERM before cancellation")
                    signal.pidfd_send_signal(pin,signum)
                    _,stderr=launcher.communicate(timeout=3)
                    self.assertNotEqual(launcher.returncode,0,stderr)
                    failure=COMPARE.read(self.directory/(stem+".failure.json"))
                    self.assertTrue(failure["timed_out"])
                    self.assertEqual(failure["error"],"child wait deadline exceeded")
                    self.assertIn(signum,failure["cleanup"]["pending_cancellation_signals"])
                    self.assertTrue(failure["cleanup"]["success"])
                    self.assertTrue(failure["cleanup"]["reaped"])
                    self.assertEqual(failure["cleanup"]["exit_code"],-9)
                    self.assertFalse(pathlib.Path(f"/proc/{failure['pid']}").exists())
                    self.assertFalse((self.directory/(stem+".close.json")).exists())
                finally:
                    if launcher.returncode is None:
                        COMPARE.terminate_pinned_child(pin,2);launcher.communicate(timeout=3)
                    os.close(pin)

    def test_spawn_failure_retains_truthful_no_child_failure_receipt(self):
        import argparse
        from unittest import mock
        args=argparse.Namespace(directory=str(self.directory),stem="spawn-failure",command=[sys.executable,"-c","import time;time.sleep(30)"])
        with mock.patch.object(COMPARE.subprocess,"Popen",side_effect=OSError("injected spawn failure")):
            with self.assertRaisesRegex(OSError,"injected spawn failure"):COMPARE.launch(args)
        failure=COMPARE.read(self.directory/"spawn-failure.failure.json")
        self.assertIsNone(failure["pid"])
        self.assertFalse(failure["pidfd_pinned"])
        self.assertFalse(failure["cleanup"]["attempted"])
        self.assertFalse(failure["cleanup"]["reaped"])
        self.assertEqual(failure["cleanup"]["method"],"no-child-created")
        self.assertIn("injected spawn failure",failure["error"])
        self.assertFalse((self.directory/"spawn-failure.close.json").exists())

    def test_submicrosecond_measured_phase_overlap_rejected_with_valid_call_bounds(self):
        # Both runs have valid receipt intervals, and their maximum end precedes
        # checkpoint. Only their actual overlap makes this packet invalid.
        half="2026-10-29T00:00:04.000000900Z"
        self.change("run_1.launch.json",lambda v:v.update(end_utc=half))
        checkpoint=call_result(4,5)
        checkpoint.update(start_utc=half,elapsed_ns=999_999_100)
        self.change("snapshot_preclose.close.json",lambda v:v.update(started_utc=half,checkpoint=checkpoint))
        self.change("snapshot_preclose.launch.json",lambda v:v.update(start_utc=half))
        self.change("lifecycle.json",lambda v:v.update(checkpoint=checkpoint))
        proof=COMPARE.read(self.directory/"server.evidence/owner.checkpoint.json")
        point=call_result(4,4);point.update(start_utc=half,end_utc=half)
        proof.update(result=checkpoint,collections_flush_all=point,backend_checkpoint=checkpoint)
        proof["owner"]["observed_utc"]=half
        self.save(self.directory/"server.evidence/owner.checkpoint.json",proof)
        self.change("snapshot_preclose.close.json",lambda v:v.update(owner_checkpoint=proof))
        self.manifest["run_repeats"]=2
        self.save(self.out/"manifest.json",self.manifest)
        for suffix in (".out",".close.json",".launch.json"):
            shutil.copyfile(self.directory/("run_1"+suffix),self.directory/("run_2"+suffix))
        phase_id="offline-test-only:run_2"
        self.change("run_2.launch.json",lambda v:v.update(phase_id=phase_id))
        self.change("run_2.close.json",lambda v:v.update(phase_id=phase_id))
        for name in ("runtime.json","lifecycle.json"):
            self.change(name,lambda v:v["phase_ids"].append(phase_id))
        self.check(False,"measured phases overlap/out of order")
        point=call_result(4,4);point.update(start_utc=half,end_utc=half)
        self.change("run_2.close.json",lambda v:v.update(db_close=point,workload_close=point))
        self.change("run_2.launch.json",lambda v:v.update(start_utc=half))
        self.check(True)

    def reject_mutations(self, changes, message=None):
        """Mutate retained copies and recompute real helper/phase receipt hashes."""
        originals={}
        try:
            for path,mutate in changes:
                path=pathlib.Path(path)
                originals.setdefault(path,path.read_bytes())
                value=json.loads(path.read_text());mutate(value);self.save(path,value)
            for path in list(originals):
                if path.parent==self.directory and path.name.endswith(".close.json"):
                    launch=path.with_name(path.name[:-11]+".launch.json")
                    originals.setdefault(launch,launch.read_bytes())
                    value=json.loads(launch.read_text());value["receipt_sha256"]=COMPARE.digest(path);self.save(launch,value)
            self.check(False,message)
        finally:
            for path,original in originals.items():path.write_bytes(original)

    def test_every_process_replica_rejects_equal_float_and_numeric_bool(self):
        replicas=[]
        for prefix in ("server","reopened","fixture-write","fixture-reopened"):
            replicas += [(self.directory/(prefix+suffix),("process",)) for suffix in (".launch.json",".exit.json")]
            replicas += [(self.directory/(prefix+".evidence")/name,("process",)) for name in ("owner.open.json","owner.before-close.json","owner.close.json")]
        replicas += [(self.directory/name,("process",)) for name in ("runtime.json","lifecycle.json","load.launch.json","run_1.launch.json","snapshot_loaded.launch.json","snapshot_preclose.launch.json","snapshot_reopened.launch.json","fixture_write.launch.json","fixture_verify.launch.json")]
        replicas += [(self.directory/"lifecycle.json",("reopened_process",))]
        for prefix,stem in (("server","snapshot_preclose"),("fixture-write","fixture_write")):
            replicas += [(self.directory/(prefix+".evidence/owner.checkpoint.json"),("owner","process")),(self.directory/(prefix+".evidence/owner.checkpoint.json"),("request","client")),(self.directory/(prefix+".evidence/owner.checkpoint-request.json"),("client",)),(self.directory/(stem+".close.json"),("owner_checkpoint","owner","process")),(self.directory/(stem+".close.json"),("owner_checkpoint","request","client"))]
        for path,keys in replicas:
            for key in ("pid","start_ticks","gomaxprocs_unset"):
                def mutate(value,keys=keys,key=key):
                    for part in keys:value=value[part]
                    value[key]=1 if key=="gomaxprocs_unset" else float(value[key])
                with self.subTest(path=path.name,keys=keys,key=key):self.reject_mutations([(path,mutate)])
        self.check(True)

    def test_exit_pid_schema_and_contract_scalars_are_exact_integers(self):
        fields=[(self.out/"manifest.json",("lifecycle_contract",)),(self.out/"manifest.json",("client","schema_version")),(self.out/"manifest.json",("fixture","schema_version")),(self.out/"manifest.json",("server_builds",self.target,"schema_version")),(self.out/"manifest.json",("schema_version",)),(self.out/"client-build.json",("schema_version",)),(self.out/"server-build.json",("schema_version",)),(self.out/"server-build.json",("servers",self.target,"schema_version")),(self.directory/"lifecycle.json",("server_exit_code",))]
        for name in ("runtime.json","lifecycle.json"):fields.append((self.directory/name,("schema_version",)))
        for stem in ("load","run_1","snapshot_loaded","snapshot_preclose","snapshot_reopened","fixture_write","fixture_verify"):
            fields += [(self.directory/(stem+".launch.json"),("schema_version",)),(self.directory/(stem+".launch.json"),("exit_code",)),(self.directory/(stem+".close.json"),("schema_version",))]
            if stem not in ("load","run_1"):fields.append((self.directory/(stem+".close.json"),("pid",)))
        for prefix in ("server","reopened","fixture-write","fixture-reopened"):
            fields.append((self.directory/(prefix+".exit.json"),("exit_code",)))
            fields += [(self.directory/(prefix+".evidence")/name,("schema_version",)) for name in ("owner.open.json","owner.before-close.json","owner.close.json")]
        for prefix,stem in (("server","snapshot_preclose"),("fixture-write","fixture_write")):
            fields += [(self.directory/(prefix+".evidence")/name,("schema_version",)) for name in ("owner.checkpoint-control.json","owner.checkpoint-request.json","owner.checkpoint.json")]
            fields += [(self.directory/(stem+".close.json"),("owner_checkpoint","schema_version")),(self.directory/(stem+".close.json"),("owner_checkpoint","owner","schema_version")),(self.directory/(stem+".close.json"),("owner_checkpoint","request","schema_version")),(self.directory/(prefix+".evidence/owner.checkpoint.json"),("owner","schema_version")),(self.directory/(prefix+".evidence/owner.checkpoint.json"),("request","schema_version"))]
        for path,keys in fields:
            for as_bool in (False,True):
                def mutate(value,keys=keys,as_bool=as_bool):
                    for part in keys[:-1]:value=value[part]
                    value[keys[-1]]=bool(value[keys[-1]]) if as_bool else float(value[keys[-1]])
                with self.subTest(path=path.name,keys=keys,as_bool=as_bool):self.reject_mutations([(path,mutate)])
        self.check(True)

    def test_shared_loader_and_serializers_reject_nonfinite_and_duplicate_json(self):
        import ycsb_correctness_validate as exact
        path=self.out/"strict-json-control.json"
        for text in ('{"x":NaN}','{"x":Infinity}','{"x":-Infinity}','{"x":1e999}','{"x":1,"x":1}'):
            path.write_text(text)
            for reader in (COMPARE.read,exact.read):
                with self.assertRaises(ValueError):reader(path)
        for bad in (float("nan"),float("inf"),float("-inf")):
            with self.assertRaises(ValueError):COMPARE.same({"x":bad},{"x":bad})
        self.assertFalse(COMPARE.same({"x":True},{"x":1}))
        self.assertFalse(COMPARE.same({"x":1},{"x":1.0}))
        self.assertTrue(COMPARE.same({"a":1,"b":True},{"b":True,"a":1}))

    def test_actual_phase_and_live_helper_cli_packet_reaches_final_validation(self):
        # Exercise the real argparse/launcher/receipt path, including its float
        # deadline serialization. Owner/storage evidence remains synthetic;
        # these Python stubs do not start a product service or qualify a cell.
        import datetime as dt
        delta=COMPARE.timestamp(COMPARE.now())-1_000_000_000-COMPARE.timestamp("2026-10-29T00:00:00Z")
        interpreter_hash=COMPARE.digest(sys.executable)
        def shift(value):
            if isinstance(value,dict):
                result={k:shift(v) for k,v in value.items()}
                if result.get("pid")==4321:result["exe_sha256"]=interpreter_hash
                return result
            if isinstance(value,list):return [shift(v) for v in value]
            if isinstance(value,str) and value.startswith("2026-10-29T"):
                seconds,nanos=divmod(COMPARE.timestamp(value)+delta,1_000_000_000)
                return dt.datetime.fromtimestamp(seconds,dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")+f".{nanos:09d}Z"
            return value
        for path in self.out.rglob("*.json"):self.save(path,shift(COMPARE.read(path)))
        manifest=COMPARE.read(self.out/"manifest.json")
        manifest["client"]["exe_sha256"]=interpreter_hash
        manifest["fixture"]["exe_sha256"]=interpreter_hash
        producer=self.out/"offline-success-client.py"
        producer.write_text("""#!/usr/bin/env python3
import os,pathlib,sys,time
sys.path.insert(0,ROOT)
import ycsb_compare_validate as c
time.sleep(.05)
point=c.now()
receipt=dict(schema_version=1,phase_id=os.environ['YCSB_PHASE_ID'],client_build_id=os.environ['YCSB_CLIENT_BUILD_ID'],db='mongodb',phase='load',declared_profile=os.environ['YCSB_PROFILE'],total_excludes_close=True,command=dict(attempted=True,success=True,error='',start_utc='0001-01-01T00:00:00Z',end_utc='0001-01-01T00:00:00Z',elapsed_ns=0),db_close=dict(attempted=True,success=True,error='',start_utc=point,end_utc=point,elapsed_ns=0),workload_close=dict(attempted=True,success=True,error='',start_utc=point,end_utc=point,elapsed_ns=0),native_flush=dict(applicable=False,flush_enabled=False))
c.write_new(os.environ['YCSB_CLOSE_RECEIPT_PATH'],receipt)
print(pathlib.Path(LOG).read_text(),end='')
""".replace("ROOT",repr(str(ROOT))).replace("LOG",repr(str(DATA/"load.out"))))
        # Use the existing 64-record stdout fixture, not its 100k source count.
        log=self.out/"offline-load.out"
        log.write_bytes((self.directory/"load.out").read_bytes())
        producer.write_text(producer.read_text().replace(repr(str(DATA/"load.out")),repr(str(log))))
        producer.write_text(producer.read_text().replace("#!/usr/bin/env python3","#!"+sys.executable))
        producer.chmod(0o755)
        helper=self.out/"offline-success-helper.py"
        template=COMPARE.read(self.directory/"snapshot_loaded.close.json")
        template_path=self.out/"offline-helper-template.json";self.save(template_path,template)
        helper.write_text("""#!/usr/bin/env python3
import os,pathlib,sys,time
sys.path.insert(0,ROOT)
import ycsb_compare_validate as c
args=dict(zip(sys.argv[1::2],sys.argv[2::2]))
receipt=c.read(TEMPLATE)
receipt.update(pid=os.getpid(),started_utc=c.now())
time.sleep(.05)
receipt['ended_utc']=c.now()
receipt['artifacts_sha256']={pathlib.Path(args['-observed']).name:c.digest(args['-observed'])}
c.write_new(args['-receipt'],receipt)
""".replace("ROOT",repr(str(ROOT))).replace("TEMPLATE",repr(str(template_path))))
        helper.write_text(helper.read_text().replace("#!/usr/bin/env python3","#!"+sys.executable))
        helper.chmod(0o755)
        manifest["fixture"]["binary"]=str(helper)
        self.save(self.out/"manifest.json",manifest);self.save(self.out/"client-build.json",manifest["client"])
        self.change("run_1.launch.json",lambda v:v["process"].update(exe_sha256=interpreter_hash))
        for stem in ("load","snapshot_loaded"):
            for suffix in (".out",".err",".launch.json",".close.json"):(self.directory/(stem+suffix)).unlink(missing_ok=True)
        launch=[sys.executable,str(ROOT/"ycsb_compare_validate.py"),"launch","--timeout","600","--cleanup-timeout","10","--outer-timeout","640",str(self.directory),"load","--",str(producer)]
        outcome=subprocess.run(launch,text=True,capture_output=True,timeout=5)
        self.assertEqual(outcome.returncode,0,outcome.stderr)
        phase=COMPARE.read(self.directory/"load.launch.json")
        self.assertIs(type(phase["timeout_seconds"]),float)
        for name in ("runtime.json","lifecycle.json"):
            self.change(name,lambda v:v["phase_ids"].__setitem__(0,phase["phase_id"]))
        command=[sys.executable,str(ROOT/"ycsb_compare_live.py"),"snapshot",str(self.directory),self.target,"--stage","loaded","--addr","127.0.0.1:1","--timeout","180","--cleanup-timeout","10","--outer-timeout","220"]
        outcome=subprocess.run(command,text=True,capture_output=True,timeout=5)
        self.assertEqual(outcome.returncode,0,outcome.stderr)
        snapshot_launch=COMPARE.read(self.directory/"snapshot_loaded.launch.json")
        self.assertIs(type(snapshot_launch["timeout_seconds"]),float)
        # Recompute hashes for the shifted synthetic supporting helper copies.
        self.change("run_1.close.json",lambda v:None)
        for stem in ("snapshot_preclose","snapshot_reopened","fixture_write","fixture_verify"):
            self.change(stem+".close.json",lambda v:v.update(artifacts_sha256={name:COMPARE.digest(self.directory/name) for name in v["artifacts_sha256"]}))
        self.check(True)

    def test_duration_seconds_are_finite_numeric_values_only(self):
        self.assertTrue(COMPARE.same_timeout(600.0,600))
        self.assertTrue(COMPARE.same_timeout(.125,.125))
        self.assertFalse(COMPARE.same_timeout(600.001,600))
        for bad in (True,False,"600",None,0,-1,float("nan"),float("inf"),float("-inf")):
            for left,right in ((bad,600),(600,bad)):
                with self.assertRaises(ValueError):COMPARE.same_timeout(left,right)
        for stem,manifest_key in (("load","phase_seconds"),("snapshot_loaded","helper_seconds")):
            for field,key in (("timeout_seconds",manifest_key),("cleanup_timeout_seconds","cleanup_seconds"),("outer_timeout_seconds","phase_outer_seconds" if stem=="load" else "helper_outer_seconds")):
                for bad in (True,"600",0,-1,float("nan"),float("inf"),float("-inf")):
                    self.reject_mutations([(self.directory/(stem+".launch.json"),lambda v,k=field,b=bad:v.update({k:b})),(self.out/"manifest.json",lambda v,k=key,b=bad:v["deadlines"].update({k:b}))])
                self.reject_mutations([(self.directory/(stem+".launch.json"),lambda v,k=field:v.update({k:v[k]+.001}))],"deadline binding mismatch")
        self.check(True)

    def test_all_owner_observation_integer_types_are_strict(self):
        replicas=[(self.directory/(prefix+".evidence")/name,()) for prefix in ("server","reopened","fixture-write","fixture-reopened") for name in ("owner.open.json","owner.before-close.json")]
        for prefix,stem in (("server","snapshot_preclose"),("fixture-write","fixture_write")):
            replicas += [(self.directory/(prefix+".evidence/owner.checkpoint.json"),("owner",)),(self.directory/(stem+".close.json"),("owner_checkpoint","owner"))]
        for path,keys in replicas:
            for field in ("data_device","data_inode","observed_gomaxprocs"):
                def mutate(value,keys=keys,field=field):
                    for key in keys:value=value[key]
                    value[field]=float(value[field])
                self.reject_mutations([(path,mutate)])
        self.check(True)

    def test_checkpoint_aggregate_results_and_matching_malformed_copies(self):
        for prefix,stem in (("server","snapshot_preclose"),("fixture-write","fixture_write")):
            receipt=self.directory/(stem+".close.json")
            for field in ("attempted","success","elapsed_ns"):
                def aggregate(value,field=field):value["checkpoint"][field]=float(value["checkpoint"][field]) if field=="elapsed_ns" else 1
                self.reject_mutations([(receipt,aggregate)])
                for result_name in ("result","collections_flush_all","backend_checkpoint"):
                    def raw(value,field=field,name=result_name):value[name][field]=float(value[name][field]) if field=="elapsed_ns" else 1
                    def embedded(value,mutate=raw):mutate(value["owner_checkpoint"])
                    self.reject_mutations([(self.directory/(prefix+".evidence/owner.checkpoint.json"),raw),(receipt,embedded)])
            # Every request copy agrees on malformed client integers: agreement
            # must not substitute for independent schema validation.
            for field in ("pid","start_ticks","gomaxprocs_unset"):
                def request(value,field=field):value["client"][field]=1 if field=="gomaxprocs_unset" else float(value["client"][field])
                self.reject_mutations([(self.directory/(prefix+".evidence/owner.checkpoint-request.json"),request),(self.directory/(prefix+".evidence/owner.checkpoint.json"),lambda v,f=request:f(v["request"])),(receipt,lambda v,f=request:f(v["owner_checkpoint"]["request"]))])
        self.check(True)

    def test_whole_build_and_result_copy_comparison_preserves_types(self):
        # Unspecified build metadata is still part of the exact accepted copy.
        for name in ("client-build.json","server-build.json"):
            if name=="client-build.json":
                files=[(self.out/"manifest.json",lambda v:v["client"].update(copy_marker=1)),(self.out/name,lambda v:v.update(copy_marker=1.0))]
            else:
                files=[(self.out/"manifest.json",lambda v:v["server_builds"][self.target].update(copy_marker=1)),(self.out/name,lambda v:v["servers"][self.target].update(copy_marker=1.0))]
            self.reject_mutations(files,"build manifest mismatch")
        # Matching malformed schemas must fail independently as well.
        self.reject_mutations([(self.out/"manifest.json",lambda v:v["client"].update(schema_version=True)),(self.out/"client-build.json",lambda v:v.update(schema_version=True))])
        self.reject_mutations([(self.out/"manifest.json",lambda v:v["server_builds"][self.target].update(schema_version=True)),(self.out/"server-build.json",lambda v:v["servers"][self.target].update(schema_version=True))])
        for name,key in (("checkpoint","checkpoint"),("server_close","server_close")):
            self.reject_mutations([(self.directory/"lifecycle.json",lambda v,k=key:v[k].update(success=1))])
        self.check(True)

    def test_nonfinite_json_and_numeric_owner_stats_are_rejected(self):
        files=[(self.directory/(prefix+".evidence")/name,()) for prefix in ("server","reopened","fixture-write","fixture-reopened") for name in ("owner.open.json","owner.before-close.json")]
        files += [(self.directory/"server.evidence/owner.checkpoint.json",("owner",)),(self.directory/"fixture-write.evidence/owner.checkpoint.json",("owner",)),(self.directory/"snapshot_preclose.close.json",("owner_checkpoint","owner")),(self.directory/"fixture_write.close.json",("owner_checkpoint","owner"))]
        for path,keys in files:
            for bad in (float("nan"),float("inf"),float("-inf"),1):
                def mutate(value,keys=keys,bad=bad):
                    for key in keys:value=value[key]
                    value["stats"]["unused_counter"]=bad
                self.reject_mutations([(path,mutate)])
        for bad in (float("nan"),float("inf"),float("-inf")):
            self.reject_mutations([(self.out/"client-build.json",lambda v,b=bad:v.update(extra=b))])
        self.check(True)

    def test_exact_nanoseconds_and_elapsed_tolerance(self):
        first="2026-10-29T00:00:04.000000100Z";last="2026-10-29T00:00:04.000000900Z"
        self.assertEqual(COMPARE.timestamp(last)-COMPARE.timestamp(first),800)
        self.assertIs(type(COMPARE.timestamp(first)),int)
        reversed_result=dict(attempted=True,success=True,error="",start_utc=last,end_utc=first,elapsed_ns=0)
        with self.assertRaisesRegex(ValueError,"duration"):COMPARE.result(reversed_result,"reversed")
        tolerated=dict(reversed_result,start_utc=first,end_utc=last,elapsed_ns=800+2_000_000)
        COMPARE.result(tolerated,"documented wall-vs-monotonic tolerance")
        with self.assertRaisesRegex(ValueError,"inconsistent"):
            COMPARE.result(dict(tolerated,elapsed_ns=800+2_000_001),"outside tolerance")
        for bad in ("2026-10-29T00:00:04.0000000001Z","2026-10-29T00:00:04+00:00"):
            with self.assertRaises(ValueError):COMPARE.timestamp(bad)

    def test_nanosecond_timestamp_copies_and_lifetimes_fail_closed(self):
        for key,value in (("preclose_state_utc","2026-10-29T00:00:05.000000001Z"),("reopened_state_utc","2026-10-29T00:00:08.000000001Z"),("reopened_state_utc","2026-10-29T00:00:08.000000000Z")):
            self.reject_mutations([(self.directory/"lifecycle.json",lambda v,k=key,t=value:v.update({k:t}))])
        self.reject_mutations([(self.directory/"lifecycle.json",lambda v:v["reopen"].update(end_utc="2026-10-29T00:00:06.000000001Z"))],"reopen bounds")
        self.reject_mutations([(self.directory/"server.evidence/owner.open.json",lambda v:v.update(observed_utc="2026-10-29T00:00:00.000000001Z"))],"outside complete observed owner lifetime")
        self.reject_mutations([(self.directory/"server.evidence/owner.before-close.json",lambda v:v.update(observed_utc="2026-10-29T00:00:04.999999999Z"))],"outside complete observed owner lifetime")
        self.check(True)

    def test_checkpoint_submicrosecond_flush_ordering_is_not_rounded(self):
        def mutate(value):
            value["collections_flush_all"].update(end_utc="2026-10-29T00:00:04.000000900Z",elapsed_ns=900)
            value["backend_checkpoint"].update(start_utc="2026-10-29T00:00:04.000000100Z",elapsed_ns=999_999_900)
        for prefix,stem in (("server","snapshot_preclose"),("fixture-write","fixture_write")):
            # Native fixture timestamps are 11..12; keep this packet's calls
            # otherwise valid by shifting the same 800ns reversal to that second.
            def at_owner(value,prefix=prefix):
                mutate(value)
                if prefix=="fixture-write":
                    value["collections_flush_all"].update(end_utc="2026-10-29T00:00:11.000000900Z")
                    value["backend_checkpoint"].update(start_utc="2026-10-29T00:00:11.000000100Z")
            self.reject_mutations([(self.directory/(prefix+".evidence/owner.checkpoint.json"),at_owner),(self.directory/(stem+".close.json"),lambda v,f=at_owner:f(v["owner_checkpoint"]))],"ordering")
        self.check(True)

    def test_launcher_rejects_reused_receipt_and_propagates_error(self):
        command = [sys.executable, str(ROOT / "ycsb_compare_validate.py"), "launch", str(self.directory), "load", "--", "/bin/true"]
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        command[command.index("load")] = "failed-probe"
        command[-1:] = [sys.executable, "-c", "import time; time.sleep(.05); raise SystemExit(37)"]
        self.assertEqual(subprocess.run(command, capture_output=True).returncode, 37)


if __name__ == "__main__":
    unittest.main()
