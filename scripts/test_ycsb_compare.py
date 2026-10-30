"""Offline CLI controls. Synthetic receipts here are not product evidence."""
import copy
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
        self.manifest = dict(schema_version=1, campaign_id="offline-test-only", targets=[self.target], profile="command_wal_relaxed", formats={self.target:"bson"}, recordcount=100000, operationcount=10000, threadcount=16, run_repeats=1, client=dict(schema_version=1, producer_commit=COMPARE.PRODUCER, source_commit=COMPARE.PRODUCER, source_patch_sha256=hashlib.sha256(b"").hexdigest(), build_id="offline-fixture-build", exe_sha256="a"*64, toolchain="fixture", compatibility_configuration="fixture"))
        self.manifest["server_builds"] = {self.target:dict(schema_version=1, version="candidate", source_commit="d"*40, exe_sha256="a"*64, expected_owner="cow")}
        self.save(self.out / "server-build.json", dict(schema_version=1, servers=self.manifest["server_builds"]))
        self.save(self.out / "manifest.json", self.manifest)
        self.save(self.out / "client-build.json", self.manifest["client"])
        (self.out / "client.patch").write_bytes(b"")
        (self.out / "client-buildinfo.txt").write_text("offline synthetic build fixture\n")
        self.receipts = {}
        ids = []
        for stem in ("load", "run_1"):
            shutil.copyfile(DATA / f"{stem}.out", self.directory / f"{stem}.out")
            phase_id = f"offline-test-only:{stem}"
            ids.append(phase_id)
            receipt = dict(schema_version=1, phase_id=phase_id, client_build_id="offline-fixture-build", db="mongodb", phase="load" if stem == "load" else "run", declared_profile="command_wal_relaxed", total_excludes_close=True, command=dict(attempted=True, success=True, error="", start_utc="0001-01-01T00:00:00Z", end_utc="0001-01-01T00:00:00Z", elapsed_ns=0), db_close=call_result(1,2), workload_close=call_result(2,3), native_flush=dict(applicable=False, flush_enabled=False))
            self.receipts[stem] = receipt
            self.save(self.directory / f"{stem}.close.json", receipt)
            self.save(self.directory / f"{stem}.launch.json", dict(schema_version=1, campaign_id="offline-test-only", phase_id=phase_id, process=self.process, start_utc="2026-10-29T00:00:00Z", end_utc="2026-10-29T00:00:04Z", exit_code=0, output_sha256=COMPARE.digest(self.directory / f"{stem}.out"), receipt_sha256=COMPARE.digest(self.directory / f"{stem}.close.json")))
        data_dir = str(self.directory / "db")
        self.save(self.directory / "server.launch.json", dict(process=self.process, data_dir=data_dir))
        shared = dict(schema_version=1, version="candidate", source_commit="d"*40, campaign_id="offline-test-only", target=self.target, profile="command_wal_relaxed", format="bson", process=self.process, data_dir=data_dir, owner_id="offline-owner", phase_ids=ids)
        self.save(self.directory / "runtime.json", dict(**shared, observed_gomaxprocs=16, backend="cow", observation="synthetic offline test"))
        self.save(self.directory / "lifecycle.json", dict(**shared, server_exit_code=0, checkpoint=call_result(4,5), server_close=call_result(5,6), reopen=call_result(6,7), random_state_preservation=call_result(7,8), preclose_state_sha256="b"*64, reopened_state_sha256="b"*64, preclose_state_utc="2026-10-29T00:00:04Z", reopened_state_utc="2026-10-29T00:00:07Z", random_state_scope="observed-state-preservation-only"))
        self.save(self.directory / "server.exit.json", dict(process=self.process, exit_code=0))

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
        self.assertIn("TOTAL\t100000\t16622.7", summary)
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
        for content in ("\n".join(line for line in original.splitlines() if not line.startswith("TOTAL")), "\n".join(line for line in original.splitlines() if not line.startswith("INSERT")), original.replace("Count: 100000", "Count: broken")):
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
        path.write_text(original+insert.replace("INSERT -", "INSERT_ERROR -").replace("Count: 100000", "Count: 1")+"\n")
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
            self.change(stem+".close.json", lambda receipt: receipt.update(db="treedb-native", native_flush=dict(applicable=True, flush_enabled=True, ack=1, dial=call_result(1,1), flush=call_result(1,2), connection_close=call_result(2,2))))

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
        server = executable("native-server", "#!/usr/bin/env python3\nimport json,os,signal,socket,sys,time\nargs=sys.argv[1:]; addr=args[args.index('-addr')+1]; host,port=addr.split(':')\njson.dump(dict(argv=args,env_unset='GOMAXPROCS' not in os.environ),open("+repr(str(marker))+",'w'))\ns=socket.socket(); s.bind((host,int(port))); s.listen()\nsignal.signal(signal.SIGTERM,lambda *_:sys.exit(0))\nwhile True: time.sleep(.05)\n")
        client = executable("go-ycsb", "#!/usr/bin/env python3\nimport json,os,sys,time\njson.dump(dict(argv=sys.argv[1:],env_unset='GOMAXPROCS' not in os.environ),open("+repr(str(client_marker))+",'w'))\ntime.sleep(.1)\nsys.exit(37)\n")
        import socket
        with socket.socket() as sock:
            sock.bind(("127.0.0.1",0))
            port = sock.getsockname()[1]
        build = dict(self.manifest["client"], exe_sha256=COMPARE.digest(client))
        build_path = self.out / "offline-build.json"
        self.save(build_path, build)
        server_build_path = self.out / "offline-server-build.json"
        self.save(server_build_path, dict(schema_version=1, servers={"treedb_native":dict(schema_version=1,version="candidate",source_commit="d"*40,exe_sha256=COMPARE.digest(server),expected_owner="cow")}))
        execution = self.out / "execution"
        environment = os.environ.copy()
        environment.update(PATH=str(tools)+os.pathsep+environment["PATH"], GOMAXPROCS="1", PARSE_ONLY="false", BUILD="false", BUILD_GOYCSB="false", OUT_DIR=str(execution), TARGETS="treedb_native", MONGODB_MODE="skip", TREEDB_NATIVE_DOCUMENT_FORMAT="json", TREEDB_NATIVE_SERVER_BIN=str(server), TREEDB_MONGO_GATEWAY_BIN=str(tools/"must-not-launch"), NATIVE_ADDR=f"127.0.0.1:{port}", CLIENT_BUILD_MANIFEST=str(build_path), SERVER_BUILD_MANIFEST=str(server_build_path), GOYCSB_DIR=str(tools), GOYCSB_BIN=str(client))
        outcome = subprocess.run(["bash", str(ROOT / "ycsb_compare_mongodb_treedb.sh")],env=environment,text=True,capture_output=True,timeout=20)
        self.assertEqual(outcome.returncode,37,outcome.stderr)
        self.assertTrue(json.loads(marker.read_text())["env_unset"])
        child = json.loads(client_marker.read_text())
        self.assertTrue(child["env_unset"])
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
        with mock.patch.object(COMPARE.os,"pidfd_open",return_value=55), mock.patch.object(COMPARE.os,"close"), mock.patch.object(COMPARE.select,"select",return_value=([],[],[])), mock.patch.object(COMPARE,"observe",side_effect=[identity,changed]), mock.patch.object(COMPARE.signal,"pidfd_send_signal") as send:
            with self.assertRaisesRegex(ValueError,"refusing signal"):
                COMPARE.terminate_owned(identity,timeout=0)
            send.assert_called_once_with(55,COMPARE.signal.SIGTERM)

    def test_launcher_rejects_reused_receipt_and_propagates_error(self):
        command = [sys.executable, str(ROOT / "ycsb_compare_validate.py"), "launch", str(self.directory), "load", "--", "/bin/true"]
        self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
        command[command.index("load")] = "failed-probe"
        command[-1:] = [sys.executable, "-c", "import time; time.sleep(.05); raise SystemExit(37)"]
        self.assertEqual(subprocess.run(command, capture_output=True).returncode, 37)


if __name__ == "__main__":
    unittest.main()
