"""Python-only census, offline refusal, and owned wait4/cancellation contracts.

Synthetic process records are never product timings or host exclusivity proof.
"""
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from collect import ChildMonitor, host_gate, run_child, wait_child
from protocol import HOST_ISOLATION, benchmark_comms, process_census, validate_host_isolation, validate_run_processes, linux_comm, sha, write

INIT = "1 0 100 0.0 1 Ss 1 init\n"

def census(pid, comm, state="S", ppid=700, pgid=None):
    return f"{pid} {ppid} 1 0.0 1 {state} {pid if pgid is None else pgid} {comm}\n"

class CensusTests(unittest.TestCase):
    def test_active_foreign_tooling_and_tests(self):
        for comm in ("go", "compile", "link", "asm", "cgo", "vet", "cover", "test2json", "preprofile", "db.test", "freelist.test",
                     "integration.tes", "abcdefghijklm.t", "abcdefghijkl.te", "\u00e9" * 5 + "x.tes"):
            for state in ("S", "R", "D", "T", "I"):
                with self.subTest(comm=comm, state=state), self.assertRaisesRegex(ValueError, "foreign active"):
                    process_census(INIT + census(901, comm, state))

    def test_low_load_host_gate_does_not_establish_process_admission(self):
        policy = {"system": "Linux", "node": "synthetic", "machine": "x86_64", "release": "synthetic",
                  "cpu_count": 8, "cpu_affinity": [0, 1, 2, 3], "max_load1": 5, "max_load5": 5,
                  "min_free_bytes": 1, "tmpdir": "/synthetic/tmp", "tmpdir_device": 1}
        snapshot = {"uname": {k: policy[k] for k in ("system", "node", "machine", "release")},
                    "cpu_count": 8, "cpu_affinity": [0, 1, 2, 3], "load": [1, 1, 1],
                    "free_bytes": 2, "storage_path": "/synthetic/tmp", "storage_device": 1}
        host_gate(snapshot, policy)  # Previous admission checked identity/load/storage only.
        with self.assertRaisesRegex(ValueError, "foreign active"):
            process_census(INIT + census(901, "compile"))

    def test_zombies_and_unrelated_processes(self):
        self.assertEqual(process_census(INIT + census(901, "go", "Z") + census(902, "db.test", "Z+") + census(903, "python3")), 4)

    def test_only_actual_owned_pid(self):
        owner = {"pid": 701, "ppid": 700, "pgid": 701, "comm": "db.test"}
        process_census(INIT + census(701, "db.test"), owner)
        with self.assertRaisesRegex(ValueError, "foreign active"):
            process_census(INIT + census(701, "db.test") + census(702, "db.test"), owner)
        for wrong in (census(701, "db.test", ppid=9), census(701, "db.test", pgid=9), census(701, "go")):
            with self.assertRaisesRegex(ValueError, "custody mismatch"):
                process_census(INIT + wrong, owner)

    def test_configured_custom_and_truncated_benchmark_names(self):
        config = {"variants": {"baseline": {"binary": "/owned/c3.baseline.test"},
                               "candidate": {"binary": "/owned/custom-reader-benchmark.test"}}}
        names = benchmark_comms(config)
        self.assertEqual(names, ["c3.baseline.tes", "custom-reader-b"])
        for comm in names:
            with self.subTest(comm=comm), self.assertRaisesRegex(ValueError, "foreign active"):
                process_census(INIT + census(901, comm), benchmark_names=names)
            process_census(INIT + census(901, comm, "Z"), benchmark_names=names)
        for basename in ("integration.test", "abcdefghijklm.test", "abcdefghijkl.test"):
            comm = linux_comm("/foreign/" + basename)
            self.assertEqual(len(comm.encode()), 15)
            with self.subTest(basename=basename), self.assertRaisesRegex(ValueError, "foreign active"):
                process_census(INIT + census(901, comm))
            process_census(INIT + census(901, comm, "Z"))
            process_census(INIT + census(901, comm), {"pid": 901, "ppid": 700, "pgid": 901, "comm": comm})
        # A full-width unrelated comm or shorter partial suffix is not evidence
        # of a truncated .test name. Unknown long prefixes remain observational limits.
        for comm in ("x" * 15, "ordinary.tes", "ordinary.te", "ordinary.t", "custom-reader-b"):
            process_census(INIT + census(901, comm))
        for path in ("/owned/\u2603.test", "/owned/bad\nname", "/"):
            with self.assertRaises(ValueError):
                linux_comm(path)

    def test_invalid_owned_identity(self):
        for owner in (None, {}, {"pid": True, "ppid": 2, "pgid": True}, {"pid": -1, "ppid": 2, "pgid": -1},
                      {"pid": 7, "ppid": 7, "pgid": 7}, {"pid": 7, "ppid": 2, "pgid": 8},
                      {"pid": 7.0, "ppid": 2, "pgid": 7}, {"pid": 7, "ppid": 2, "pgid": 7, "name": "go"}):
            with self.subTest(owner=owner), self.assertRaises(ValueError):
                process_census(INIT + census(7, "go", ppid=2), owner)

    def test_malformed_census(self):
        for text in ("", "1 0 1 0.0 1 init\n", INIT + INIT,
                     "1.0 0 1 0.0 1 S 1 init\n", "-1 0 1 0.0 1 S 1 init\n",
                     "1 0 1 NaN 1 S 1 init\n", "1 0 1 inf 1 S 1 init\n",
                     "1 0 1 0.0 1 Q 1 init\n", "1 0 1 0.0 1 S x init\n",
                     "1 0 1 0.0 1 S 1 init\x00\n"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                process_census(text)

    def test_missing_or_mutated_contract(self):
        validate_host_isolation(dict(HOST_ISOLATION))
        for value in (None, {}, dict(HOST_ISOLATION, sample_interval_seconds=1), dict(HOST_ISOLATION, exemption="*.test")):
            with self.assertRaises(ValueError):
                validate_host_isolation(value)

class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="cow-isolation-")
        self.packet, self.name = Path(self.temp.name), "synthetic"
        self.value = {"contract": dict(HOST_ISOLATION), "owner": {"pid": 701, "ppid": 700, "pgid": 701, "comm": "db.test"}, "benchmark_comms": [],
            "started_monotonic_ns": 1_000_000_000, "reaped_monotonic_ns": 1_400_000_000,
            "waited_pid": 701, "wait_status": 0, "failure": None, "stopped_on_contamination": False, "samples": []}
        self.receipt = {"child_pid": 701, "collector_pid": 700, "child_pgid": 701,
                        "child_comm": "db.test", "command": ["/synthetic/db.test"],
                        "waited_pid": 701, "wait_status": 0, "exit_code": 0}
        for index, start in enumerate((1_010_000_000, 1_120_000_000, 1_320_000_000)):
            path = self.packet / f"synthetic-monitor-{index:06d}-processes.txt"
            path.write_text(INIT + census(701, "db.test"))
            self.value["samples"].append({"index": index, "started_monotonic_ns": start,
                "completed_monotonic_ns": start + 10_000_000, "path": path.name, "sha256": sha(path)})
        for direction in ("before", "after"):
            path = self.packet / f"synthetic-{direction}-processes.txt"
            path.write_text(INIT + census(902, "go", "Z"))
            self.receipt[direction] = {"processes_sha256": sha(path)}
        self.seal()

    def tearDown(self):
        self.temp.cleanup()

    def seal(self):
        path = self.packet / "synthetic-monitor.json"
        write(path, self.value)
        self.receipt["monitor_sha256"] = sha(path)

    def check(self):
        validate_run_processes(self.packet, self.name, self.receipt)

    def test_positive_quiet_owned_child_and_zombies(self):
        self.check()

    def test_foreign_before_after_and_reused_pid(self):
        for direction in ("before", "after"):
            path = self.packet / f"synthetic-{direction}-processes.txt"
            original = path.read_text()
            for pid in (901, 701):
                path.write_text(INIT + census(pid, "db.test"))
                self.receipt[direction]["processes_sha256"] = sha(path)
                with self.assertRaisesRegex(ValueError, "foreign active"):
                    self.check()
            path.write_text(original)
            self.receipt[direction]["processes_sha256"] = sha(path)

    def test_between_endpoint_foreign_record_even_resealed(self):
        sample = self.value["samples"][1]
        path = self.packet / sample["path"]
        original = path.read_text()
        for comm in ("compile", "integration.tes", "abcdefghijklm.t", "abcdefghijkl.te"):
            path.write_text(original + census(909, comm))
            sample["sha256"] = sha(path)
            self.seal()
            with self.subTest(comm=comm), self.assertRaisesRegex(ValueError, "foreign active"):
                self.check()

    def test_raw_tamper_and_missing(self):
        path = self.packet / self.value["samples"][1]["path"]
        path.write_text(INIT)
        with self.assertRaisesRegex(ValueError, "census drift"):
            self.check()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "missing/nonregular"):
            self.check()

    def test_missing_or_tampered_monitor(self):
        path = self.packet / "synthetic-monitor.json"
        path.write_text("{}")
        with self.assertRaisesRegex(ValueError, "tampered monitor"):
            self.check()
        path.unlink()
        with self.assertRaisesRegex(ValueError, "missing/tampered"):
            self.check()

    def test_pid_and_wait_status_joins(self):
        original = copy.deepcopy(self.receipt)
        for field, value in (("child_pid", 702), ("collector_pid", 999), ("child_pgid", 702),
                             ("waited_pid", 702), ("wait_status", 256), ("exit_code", 7),
                             ("child_pid", 701.0), ("wait_status", False), ("waited_pid", None),
                             ("child_comm", "go"), ("command", ["/foreign/go"]), ("command", [])):
            self.receipt = copy.deepcopy(original)
            self.receipt[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.check()

    def test_configured_names_bound_and_audited_offline(self):
        names = ["custom-reader-b"]
        self.value["benchmark_comms"] = names
        self.seal()
        validate_run_processes(self.packet, self.name, self.receipt, names)
        with self.assertRaisesRegex(ValueError, "unbound configured"):
            self.check()
        sample = self.value["samples"][1]
        path = self.packet / sample["path"]
        path.write_text(path.read_text() + census(909, names[0]))
        sample["sha256"] = sha(path)
        self.seal()
        with self.assertRaisesRegex(ValueError, "foreign active"):
            validate_run_processes(self.packet, self.name, self.receipt, names)

    def test_missing_extra_and_reordered_samples(self):
        original = copy.deepcopy(self.value)
        for samples in ([], original["samples"][1:], list(reversed(original["samples"]))):
            self.value = copy.deepcopy(original)
            self.value["samples"] = samples
            self.seal()
            with self.assertRaises(ValueError):
                self.check()
        self.value = original
        self.seal()
        (self.packet / "synthetic-monitor-000999-processes.txt").write_text(INIT)
        with self.assertRaisesRegex(ValueError, "extra monitor"):
            self.check()

    def test_gap_postjoin_and_contamination_proof(self):
        original = copy.deepcopy(self.value)
        for mutate in (lambda v: v.update(reaped_monotonic_ns=2_000_000_000),
                       lambda v: v.update(reaped_monotonic_ns=1_315_000_000),
                       lambda v: v.update(failure="foreign compile"),
                       lambda v: v.update(stopped_on_contamination=True),
                       lambda v: v["samples"][0].update(path="../foreign")):
            self.value = copy.deepcopy(original)
            mutate(self.value)
            self.seal()
            with self.assertRaises(ValueError):
                self.check()

class OwnedWaitTests(unittest.TestCase):
    def test_spawn_and_monitor_transition_cancellation_custody(self):
        real_spawn, real_kill = subprocess.Popen, os.killpg
        for seam, cancellation in (("after-spawn", signal.SIGTERM), ("after-spawn", signal.SIGQUIT),
                                   ("monitor-init", signal.SIGINT), ("monitor-init-error", signal.SIGQUIT)):
            with self.subTest(seam=seam, cancellation=cancellation), tempfile.TemporaryDirectory(prefix="cow-transition-") as temporary:
                children = []
                def spawn(*args, **kwargs):
                    child = real_spawn(*args, **kwargs)
                    children.append(child)
                    if seam == "after-spawn":
                        signal.raise_signal(cancellation)
                    return child
                def monitor(*args, **kwargs):
                    if seam != "after-spawn":
                        signal.raise_signal(cancellation)
                    if seam == "monitor-init-error":
                        raise OSError("synthetic initialization failure")
                    return ChildMonitor(*args, **kwargs)
                def kill(pid, sig):
                    # Repeated real cancellation during cleanup must stay sticky.
                    signal.raise_signal(signal.SIGTERM)
                    signal.raise_signal(signal.SIGINT)
                    signal.raise_signal(signal.SIGQUIT)
                    return real_kill(pid, sig)
                previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)}
                with patch("collect.subprocess.Popen", side_effect=spawn), patch("collect.ChildMonitor", side_effect=monitor), patch("collect.os.killpg", side_effect=kill):
                    with self.assertRaises(OSError if seam == "monitor-init-error" else KeyboardInterrupt):
                        run_child([sys.executable, "-B", "-c", "import time; time.sleep(5)"], None,
                                  dict(os.environ), subprocess.DEVNULL, subprocess.DEVNULL, 10,
                                  Path(temporary), "transition")
                self.assertEqual(len(children), 1)
                self.assertEqual(children[0].returncode, -signal.SIGKILL)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(children[0].pid, os.WNOHANG)
                for s, handler in previous.items():
                    self.assertEqual(signal.getsignal(s), handler)
                proof = json.loads((Path(temporary) / ("transition-initialization-failure-join.json" if seam == "monitor-init-error" else "transition-monitor.json")).read_text())
                self.assertEqual(proof["waited_pid"], children[0].pid)
                self.assertEqual(os.waitstatus_to_exitcode(proof["wait_status"]), -signal.SIGKILL)

    def run_owned(self, foreign=False, cancel=False, cancel_signal=False):
        with tempfile.TemporaryDirectory(prefix="cow-owned-") as temporary:
            packet = Path(temporary)
            child = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(0.3)"], start_new_session=True)
            monitor = ChildMonitor(packet, "owned", child, {}, time.monotonic_ns())
            calls = 0
            def raw(env):
                nonlocal calls
                calls += 1
                if cancel:
                    raise KeyboardInterrupt("synthetic cancellation")
                if cancel_signal:
                    signal.raise_signal(signal.SIGTERM)
                text = INIT + census(child.pid, monitor.owner["comm"], ppid=os.getpid())
                if foreign and calls == 2:
                    text += census(987654, "compile")
                return text.encode()
            previous = signal.getsignal(signal.SIGTERM)
            with patch("collect.census_bytes", side_effect=raw):
                if cancel or cancel_signal:
                    with self.assertRaises(KeyboardInterrupt):
                        wait_child(child, 2, monitor=monitor)
                    self.assertEqual(child.returncode, -signal.SIGKILL)
                else:
                    status, usage, timed_out = wait_child(child, 2, monitor=monitor)
                    self.assertFalse(timed_out)
                    self.assertEqual(child.returncode, -signal.SIGKILL if foreign else 0)
                    self.assertEqual(os.waitstatus_to_exitcode(status), child.returncode)
                    self.assertGreater(usage.ru_maxrss, 0)
            self.assertEqual(signal.getsignal(signal.SIGTERM), previous)
            with self.assertRaises(ChildProcessError):
                os.waitpid(child.pid, os.WNOHANG)
            proof = json.loads((packet / "owned-monitor.json").read_text())
            self.assertEqual(proof["waited_pid"], child.pid)
            self.assertTrue(proof["reaped_monotonic_ns"])
            self.assertEqual(bool(proof["failure"]), foreign or cancel or cancel_signal)
            self.assertEqual(proof["stopped_on_contamination"], foreign)
            self.assertGreaterEqual(len(proof["samples"]), 2 if foreign else 0)

    def test_owned_wait4_positive(self):
        self.run_owned()

    def test_foreign_between_samples_stops_and_reaps_owned_child(self):
        self.run_owned(foreign=True)

    def test_cancel_stops_and_reaps_owned_child(self):
        self.run_owned(cancel=True)

    def test_sigterm_stops_and_reaps_owned_child(self):
        self.run_owned(cancel_signal=True)

if __name__ == "__main__":
    unittest.main()
