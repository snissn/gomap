#!/usr/bin/env python3
"""Failure controls for the source-bound public replacement cost contract."""

import copy
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest import mock

path = Path(__file__).with_name("mvcc_primary_cost_gate.py")
if not path.exists():
    path = Path(__file__).resolve().parents[2] / "scripts/mvcc_primary_cost_gate.py"
spec = importlib.util.spec_from_file_location("primary_gate", path)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class PrimaryCostGateTests(unittest.TestCase):
    authorities = {"baseline": "PRIMARY", "candidate": "DATA"}

    def sample(self, ns=100, size=200, allocs=3, generations=0):
        return f"{gate.BENCHMARK} 128 {ns} ns/op {generations} generation_advances/op {size} B/op {allocs} allocs/op\nPASS\n"

    def rows(self):
        return [{"pair": pair, "revision": revision,
                 "metrics": gate.parse_sample(self.sample(generations=2 if revision == "baseline" else 0)),
                 "authority": self.authorities[revision]}
                for pair in range(8) for revision in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline"))]

    def test_retains_generation_telemetry_and_requires_source_bound_activation(self):
        rows = self.rows()
        self.assertTrue(gate.evaluate(rows, self.authorities)["measurement_pass"])
        rows[1]["metrics"]["generation_advances/op"] = 1
        self.assertTrue(gate.evaluate(rows, self.authorities)["measurement_pass"])
        for revision in ("baseline", "candidate"):
            invalid = copy.deepcopy(rows)
            next(row for row in invalid if row["revision"] == revision)["authority"] = "wrong"
            self.assertFalse(gate.evaluate(invalid, self.authorities)["gates"][revision + "_route_activation"])
        primary = dict(self.authorities, candidate="PRIMARY")
        for row in rows:
            row["authority"] = "PRIMARY"
        self.assertFalse(gate.evaluate(rows, primary)["gates"]["candidate_route_activation"])
        rows[1]["metrics"]["generation_advances/op"] = 0
        self.assertTrue(gate.evaluate(rows, primary)["measurement_pass"])
        data = {"baseline": "DATA", "candidate": "DATA"}
        for row in rows:
            row["authority"] = "DATA"
            row["metrics"]["generation_advances/op"] = 1
        self.assertTrue(gate.evaluate(rows, data)["measurement_pass"])
        for invalid in ({}, {"baseline": "DATA"}, dict(data, candidate="unknown")):
            with self.assertRaises(ValueError):
                gate.evaluate(rows, invalid)

    def test_authority_requires_actual_logged_role(self):
        line = "    durable_replacement_cost_test.go:75: MVCC_COST_AUTHORITY authority=DATA root=42 commit=9 profile=no_wal_fast mode=cow_btree redo=off\n"
        self.assertEqual(gate.parse_authority(line + line, "DATA"), "DATA")
        for invalid in ("", line.replace("DATA", "PRIMARY"), line + line.replace("DATA", "PRIMARY"),
                        line.replace("redo=off", "redo=on")):
            with self.assertRaises(ValueError):
                gate.parse_authority(invalid, "DATA")

    def test_setup_is_bound_to_one_recognized_source_capability(self):
        template = (path.parents[1] / gate.HARNESS).read_text()
        self.assertEqual(gate.render_harness(template, "DATA"), template)
        primary = gate.render_harness(template, "PRIMARY")
        self.assertIn('const durableReplacementAuthority = "PRIMARY"', primary)
        self.assertIn("opts.IndexPrimaryDirectory = true", primary)
        self.assertNotIn("opts.IndexPrimaryDirectory = true", template)
        for invalid in (template.replace(gate.PRIMARY_SETUP, ""), template + gate.ROLE_DECLARATION):
            with self.assertRaises(ValueError):
                gate.render_harness(invalid, "DATA")
        with self.assertRaises(ValueError):
            gate.render_harness(template, "unknown")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                gate.source_authority(root)
            for authority in ("PRIMARY", "DATA"):
                relative, declaration = gate.CAPABILITIES[authority]
                source = root / relative
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text(declaration)
                record = gate.source_authority(root)
                self.assertEqual(record, {"authority": authority, "path": relative, "sha256": gate.digest(source)})
                source.write_text("unrecognized source")
                with self.assertRaises(ValueError):
                    gate.source_authority(root)
                source.unlink()
            for relative, declaration in gate.CAPABILITIES.values():
                (root / relative).write_text(declaration)
            with self.assertRaises(ValueError):
                gate.source_authority(root)

    def test_refuses_incomplete_duplicate_or_reordered_evidence(self):
        rows = self.rows()
        for invalid in (rows[:-1], rows + rows[:1], list(reversed(rows)), rows[:1] + rows):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    gate.evaluate(invalid, self.authorities)

    def test_timing_and_allocation_regressions_fail_independently(self):
        for metric, value, name in (("ns/op", 106, "timing"), ("B/op", 201, "bytes"), ("allocs/op", 4, "allocations")):
            rows = self.rows()
            for row in rows:
                if row["revision"] == "candidate":
                    row["metrics"][metric] = value
            result = gate.evaluate(rows, self.authorities)
            self.assertFalse(result["measurement_pass"])
            self.assertFalse(result["gates"][name])

    def test_malformed_nonfinite_missing_and_unsuccessful_samples_fail(self):
        good = self.sample()
        invalids = (good.replace("128 ", "127 "), good + good, good.replace("PASS", "FAIL"),
                    good.replace("200 B/op", "nan B/op"), good.replace("200 B/op", "-1 B/op"),
                    good.replace("200 B/op", "200 B/op 200 B/op"), good.replace("3 allocs/op", ""),
                    good.replace("100 ns/op", "0 ns/op"))
        for text in invalids:
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    gate.parse_sample(text)

    def test_paired_delta_uses_matching_samples(self):
        rows = self.rows()
        for row in rows:
            baseline = 100 + row["pair"] * 100
            row["metrics"]["ns/op"] = baseline * (1.02 if row["revision"] == "candidate" else 1)
        self.assertAlmostEqual(gate.evaluate(rows, self.authorities)["median_paired_delta_percent"], 2)

    def test_equivalent_bytes_qualification_retains_raw_failure(self):
        rows = self.rows()
        for row in rows:
            if row["revision"] == "candidate":
                row["metrics"]["B/op"] += 0.5
        raw = gate.evaluate(rows, self.authorities)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, loader, libc = [root / name for name in ("binary", "loader", "libc.so.6")]
            for path in (binary, loader, libc):
                path.write_bytes(path.name.encode())
            def command(label, argv):
                if argv[0] == "readelf":
                    return f"[Requesting program interpreter: {loader}]"
                return f"linux-vdso.so.1 (0x123)\nlibc.so.6 => {libc} (0x456)\n{loader} (0x789)\n"
            binding = gate.runtime_binding(binary, command, "before", {})
            binding["inputs"] = {"harness_sha256": "harness", "runner_sha256": "runner",
                                 "environment_sha256": "environment", "cwd": str(root), "argv": ["fixed benchmark"]}
            before = {"baseline": binding, "candidate": copy.deepcopy(binding)}
            result = gate.qualify_runtime(raw, before, copy.deepcopy(before), quiet=True)
            self.assertFalse(result["gates"]["bytes"])
            self.assertFalse(result["measurement_pass"])
            self.assertTrue(result["accepted"])
            self.assertEqual(result["bytes_disposition"], "EQUIVALENT_NON_ATTRIBUTABLE")
            changed = copy.deepcopy(before)
            changed["candidate"]["executable"]["sha256"] = "different"
            self.assertFalse(gate.qualify_runtime(raw, changed, copy.deepcopy(changed), quiet=True)["accepted"])
            for metric in ("allocations", "baseline_route_activation", "candidate_route_activation"):
                failing = copy.deepcopy(raw)
                failing["gates"][metric] = False
                self.assertFalse(gate.qualify_runtime(failing, before, before, quiet=True)["accepted"])
            self.assertFalse(gate.qualify_runtime(raw, before, before, quiet=False)["accepted"])
            for field in ("harness_sha256", "runner_sha256", "environment_sha256", "cwd", "argv"):
                mismatch = copy.deepcopy(before)
                mismatch["candidate"]["inputs"][field] = "different"
                self.assertFalse(gate.qualify_runtime(raw, mismatch, mismatch, quiet=True)["accepted"])
            for missing in ({}, {"baseline": binding}, {"baseline": {}, "candidate": {}}):
                with self.assertRaisesRegex(RuntimeError, "missing"):
                    gate.qualify_runtime(raw, missing, missing, quiet=True)
            reordered = copy.deepcopy(before)
            reordered["baseline"]["libraries"].reverse()
            reordered["candidate"]["libraries"].reverse()
            with self.assertRaisesRegex(RuntimeError, "drift"):
                gate.qualify_runtime(raw, before, reordered, quiet=True)
            libc.write_bytes(b"changed libc")
            after = {"baseline": gate.runtime_binding(binary, command, "after", {}),
                     "candidate": gate.runtime_binding(binary, command, "after", {})}
            with self.assertRaisesRegex(RuntimeError, "drift"):
                gate.qualify_runtime(raw, before, after, quiet=True)
            libc.unlink()
            with self.assertRaises((OSError, ValueError)):
                gate.runtime_binding(binary, command, "missing", {})

    def test_retained_ab2_timing_failure_requires_complete_runtime_equivalence(self):
        # Job118411039651: retain the exact eight pairs, not a tolerance around 5%.
        pairs = [(3090995, 5053100), (5029492, 4709566), (4698419, 5128241),
                 (4629356, 4917921), (5050966, 4805452), (4839474, 5046748),
                 (4666022, 5040831), (4941016, 4856478)]
        rows = self.rows()
        for row in rows:
            row["metrics"] = {"ns/op": pairs[row["pair"]][row["revision"] == "candidate"],
                              "B/op": 222247, "allocs/op": 378, "generation_advances/op": 0}
        raw = gate.evaluate(rows, self.authorities)
        retained = copy.deepcopy(raw)
        self.assertAlmostEqual(raw["median_paired_delta_percent"], 5.258179271710761)
        self.assertFalse(raw["gates"]["timing"])
        self.assertFalse(raw["measurement_pass"])
        self.assertTrue(all(value for name, value in raw["gates"].items() if name != "timing"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, loader, libc = [root / name for name in ("binary", "loader", "libc.so.6")]
            for path in (binary, loader, libc):
                path.write_bytes(path.name.encode())
            def command(label, argv):
                return (f"[Requesting program interpreter: {loader}]" if argv[0] == "readelf"
                        else f"libc.so.6 => {libc} (0x456)\n{loader} (0x789)\n")
            binding = gate.runtime_binding(binary, command, "before", {})
            binding["inputs"] = {"harness_sha256": "harness", "runner_sha256": "runner",
                                 "environment_sha256": "environment", "cwd": str(root), "argv": ["fixed benchmark"]}
            before = {"baseline": binding, "candidate": copy.deepcopy(binding)}
            result = gate.qualify_runtime(raw, before, copy.deepcopy(before), quiet=True)
            self.assertTrue(result["accepted"])
            self.assertEqual(result["timing_disposition"], "EQUIVALENT_NON_ATTRIBUTABLE")
            self.assertEqual(result["bytes_disposition"], "PASS")
            self.assertEqual(raw, retained)
            self.assertFalse(result["gates"]["timing"])
            self.assertFalse(result["measurement_pass"])
            paths = [("executable", "sha256"), ("interpreter", "sha256"), ("libraries", 0, "sha256")]
            paths += [("inputs", field) for field in binding["inputs"]]
            for path in paths:
                changed = copy.deepcopy(before)
                target = changed["candidate"]
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = "different"
                with self.subTest(changed=path):
                    refused = gate.qualify_runtime(raw, changed, copy.deepcopy(changed), quiet=True)
                    self.assertFalse(refused["accepted"])
                    self.assertFalse(refused["acceptance_gates"]["timing"])
                    self.assertEqual(refused["timing_disposition"], "FAIL")
                with self.assertRaisesRegex(RuntimeError, "drift"):
                    gate.qualify_runtime(raw, before, changed, quiet=True)
            for metric in ("allocations", "baseline_route_activation", "candidate_route_activation"):
                failing = copy.deepcopy(raw)
                failing["gates"][metric] = False
                self.assertFalse(gate.qualify_runtime(failing, before, before, quiet=True)["accepted"])
            self.assertFalse(gate.qualify_runtime(raw, before, before, quiet=False)["accepted"])
            for invalid in ({}, {"baseline": binding}, {"baseline": {}, "candidate": {}}):
                with self.assertRaisesRegex(RuntimeError, "missing"):
                    gate.qualify_runtime(raw, invalid, invalid, quiet=True)

    def test_runtime_resolution_refuses_unresolved_or_unsupported_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "binary"
            binary.write_bytes(b"executable")
            for text in ("", "no interpreter", "[Requesting program interpreter: /missing]"):
                with self.subTest(text=text), self.assertRaises((ValueError, OSError)):
                    gate.runtime_binding(binary, lambda *args: text, "invalid", {})

    def test_static_elf_binding_requires_complete_headers_without_dynamic_inputs(self):
        headers = "Elf file type is EXEC (Executable file)\nProgram Headers:\n  LOAD 0x0 0x0\n"
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "static.test"
            binary.write_bytes(b"static executable contents")
            calls = []
            def command(label, argv):
                calls.append(argv)
                return headers
            binding = gate.runtime_binding(binary, command, "static", {})
            self.assertEqual(len(calls), 1)  # No dynamic loader is invoked.
            self.assertEqual(binding, {"linkage": "static", "executable": {
                "sha256": gate.digest(binary), "bytes": binary.stat().st_size},
                "interpreter": None, "libraries": []})
            binding["inputs"] = {"harness_sha256": "harness", "runner_sha256": "runner",
                                 "environment_sha256": "environment", "cwd": str(binary.parent), "argv": []}
            before = {"baseline": binding, "candidate": copy.deepcopy(binding)}
            raw = gate.evaluate(self.rows(), self.authorities)
            raw["gates"]["bytes"], raw["measurement_pass"] = False, False
            result = gate.qualify_runtime(raw, before, copy.deepcopy(before), quiet=True)
            self.assertEqual(result["bytes_disposition"], "EQUIVALENT_NON_ATTRIBUTABLE")
            self.assertFalse(result["measurement_pass"])
            self.assertTrue(result["accepted"])
            for invalid in (headers + "  DYNAMIC 0x0 0x0\n", headers + "  INTERP 0x0 0x0\n",
                            headers.replace("  LOAD", "  NOTE"), headers.replace("Program Headers:", ""),
                            headers.replace("Elf file type is EXEC", "unknown file type")):
                with self.subTest(headers=invalid), self.assertRaisesRegex(ValueError, "static ELF"):
                    gate.runtime_binding(binary, lambda *args: invalid, "invalid", {})

    @unittest.skipUnless(shutil.which("readelf") and Path("/bin/true").exists(), "Linux ELF tools required")
    def test_resolves_actual_loader_libraries_and_file_contents(self):
        def command(label, argv):
            return subprocess.check_output([str(item) for item in argv], text=True)
        binding = gate.runtime_binding(Path("/bin/true"), command, "real", {})
        self.assertEqual(binding["executable"]["sha256"], gate.digest(Path("/bin/true")))
        self.assertIn(binding["interpreter"], binding["libraries"])
        for library in binding["libraries"]:
            self.assertEqual(library, gate.file_identity(Path(library["path"])))
        with self.assertRaisesRegex(ValueError, "LD_AUDIT"):
            gate.runtime_binding(Path("/bin/true"), command, "audit", {"LD_AUDIT": "unknown.so"})

    def test_loader_listing_rejects_missing_libraries_and_unbound_interpreter(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, loader = [Path(directory) / name for name in ("binary", "loader")]
            binary.write_bytes(b"executable")
            loader.write_bytes(b"loader")
            for listing in ("libc.so.6 => not found", "", f"/missing (0x123)", "linux-vdso.so.1 (0x123)"):
                def command(label, argv):
                    return f"[Requesting program interpreter: {loader}]" if argv[0] == "readelf" else listing
                with self.subTest(listing=listing), self.assertRaises((ValueError, OSError)):
                    gate.runtime_binding(binary, command, "invalid", {})

    def test_process_cpu_survives_exit_and_pid_reuse(self):
        watch = gate.ProcessObservation.__new__(gate.ProcessObservation)
        watch.previous, watch.totals, watch.first = {}, {}, True
        def record(ticks, start="first", parent=0):
            return {"ticks": ticks, "start": start, "parent": parent, "command": "external"}
        watch.accumulate({-1: record(100)})
        watch.accumulate({-1: record(105)})
        watch.accumulate({})
        watch.accumulate({-1: record(2, "reused")})
        self.assertEqual(watch.totals[(-1, "first")]["ticks"], 5)
        self.assertEqual(watch.totals[(-1, "reused")]["ticks"], 2)

    def test_only_own_descendants_and_ancestors_are_excluded(self):
        own = os.getpid()
        snapshot = {own: {"parent": own+1}, own+1: {"parent": 0},
                    own+2: {"parent": own}, own+3: {"parent": own+2}, own+4: {"parent": own+1}}
        self.assertEqual(gate.allowed_processes(snapshot), {own, own+1, own+2, own+3})


class QuietAcquisitionTests(unittest.TestCase):
    class Clock:
        def __init__(self):
            self.now = 0.0

        def __call__(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    def observer_factory(self, clock, busy_windows, *, finish_cost=0):
        created = []

        class Observer:
            def __init__(self, path):
                self.path = path
                self.started = clock()
                self.busy = busy_windows[len(created)]
                created.append(self)
                self.observe()

            def observe(self):
                with self.path.open("a") as stream:
                    stream.write(json.dumps({"elapsed_s": clock() - self.started}) + "\n")

            def finish(self):
                clock.sleep(finish_cost)
                self.observe()
                rows = [{"pid": 91, "start": "original", "command": "flush-8:0",
                         "ticks": 16, "cpu_percent": 3.2}] if self.busy else []
                return {"elapsed_s": clock() - self.started, "external_processes": rows,
                        "busy": rows, "cpu_threshold_percent": gate.QUIET_LIMIT_PERCENT,
                        "sampling_interval_s": 0.25}

        return Observer, created

    def acquire(self, directory, clock, factory):
        return gate.acquire_quiet_window(Path(directory), clock=clock, sleep=clock.sleep,
                                         observer_factory=factory)

    def test_already_quiet_acquires_first_full_window_without_extra_delay(self):
        clock = self.Clock()
        factory, created = self.observer_factory(clock, [False])
        with tempfile.TemporaryDirectory() as directory:
            result = self.acquire(directory, clock, factory)
            receipt = json.loads((Path(directory) / "preflight-acquisition.json").read_text())
            self.assertEqual(receipt["status"], "acquired")
            self.assertEqual(len(created), 1)
            self.assertEqual(clock(), 5)
            self.assertEqual(result, json.loads((Path(directory) / "preflight.json").read_text()))
            self.assertTrue(receipt["windows"][0]["qualified"])

    def test_busy_then_quiet_retains_failed_window_and_never_launches_early(self):
        clock = self.Clock()
        factory, created = self.observer_factory(clock, [True, False])
        launched = []
        with tempfile.TemporaryDirectory() as directory:
            self.acquire(directory, clock, factory)
            launched.append(clock())  # Caller may start timing only after acquisition returns.
            receipt = json.loads((Path(directory) / "preflight-acquisition.json").read_text())
            self.assertEqual(launched, [10])
            self.assertEqual(len(created), 2)
            self.assertEqual(receipt["status"], "acquired")
            self.assertEqual([w["qualified"] for w in receipt["windows"]], [False, True])
            self.assertEqual(receipt["windows"][0]["result"]["busy"][0]["pid"], 91)
            self.assertEqual(receipt["windows"][1]["result"]["busy"], [])
            for window in receipt["windows"]:
                self.assertTrue((Path(directory) / window["observation_file"]).read_text())
            self.assertEqual(json.loads((Path(directory) / "preflight.json").read_text()),
                             receipt["windows"][1]["result"])

    def test_exhaustion_retains_every_window_without_launch(self):
        clock = self.Clock()
        factory, created = self.observer_factory(clock, [True] * 12)
        launched = []
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "exhausted; no timed samples"):
                self.acquire(directory, clock, factory)
                launched.append(clock())
            self.assertEqual(launched, [])
            self.assertEqual(clock(), 60)
            self.assertEqual(len(created), 12)
            receipt = json.loads((Path(directory) / "preflight-acquisition.json").read_text())
            self.assertEqual(receipt["status"], "exhausted")
            self.assertEqual(len(receipt["windows"]), 12)
            self.assertTrue(all(w["result"]["busy"] and not w["qualified"]
                                for w in receipt["windows"]))

    def test_does_not_start_short_window_or_accept_completion_after_cap(self):
        for cap, finish_cost, expected_windows, expected_elapsed in [(12, 0, 2, 10), (5, 1, 1, 6)]:
            with self.subTest(cap=cap), tempfile.TemporaryDirectory() as directory:
                clock = self.Clock()
                factory, created = self.observer_factory(clock, [True] * 3 if cap == 12 else [False],
                                                         finish_cost=finish_cost)
                with mock.patch.object(gate, "QUIET_ACQUISITION_SECONDS", cap):
                    with self.assertRaisesRegex(RuntimeError, "exhausted"):
                        self.acquire(directory, clock, factory)
                receipt = json.loads((Path(directory) / "preflight-acquisition.json").read_text())
                self.assertEqual(len(created), expected_windows)
                self.assertEqual(clock(), expected_elapsed)
                self.assertFalse(any(w["qualified"] for w in receipt["windows"]))
                if finish_cost:
                    self.assertFalse(receipt["windows"][-1]["within_cap"])

    def test_original_process_predicate_rejects_exact_two_percent(self):
        for ticks, busy in [(9, False), (10, True), (16, True)]:
            with self.subTest(ticks=ticks):
                observer = gate.ProcessObservation.__new__(gate.ProcessObservation)
                observer.started = 0
                observer.totals = {(91, "original"): {"pid": 91, "start": "original",
                    "command": "flush-8:0", "ticks": ticks}}
                observer.observe = lambda: None
                with mock.patch.object(gate.time, "monotonic", return_value=5), \
                        mock.patch.object(gate.os, "sysconf", return_value=100):
                    result = observer.finish()
                self.assertEqual(bool(result["busy"]), busy)
                self.assertEqual(result["cpu_threshold_percent"], 2.0)

    def test_run_gate_exhaustion_never_starts_measured_observer_or_benchmark(self):
        self.assert_run_gate_never_launches([True] * 12)

    def test_run_gate_artifact_writes_crossing_cap_never_launch(self):
        for name in ("preflight.json", "preflight-acquisition.json"):
            with self.subTest(delayed_write=name):
                self.assert_run_gate_never_launches([False], delayed_write=name)

    def assert_run_gate_never_launches(self, busy_windows, *, delayed_write=None):
        clock = self.Clock()
        factory, created = self.observer_factory(clock, busy_windows)
        commands = []
        original_write = Path.write_text

        def write_text(path, content, *args, **kwargs):
            result = original_write(path, content, *args, **kwargs)
            if path.name == delayed_write and (path.name == "preflight.json" or
                                               json.loads(content)["status"] == "acquired"):
                clock.sleep(gate.QUIET_ACQUISITION_SECONDS)
            return result

        class SetupProcess:
            def __init__(self, argv, **kwargs):
                commands.append(argv)
                if argv[1:3] == ["worktree", "add"]:
                    source = Path(argv[-2]) / gate.CAPABILITIES["PRIMARY"][0]
                    source.parent.mkdir(parents=True, exist_ok=True)
                    source.write_text(gate.CAPABILITIES["PRIMARY"][1])
                if "-o" in argv:
                    Path(argv[argv.index("-o") + 1]).write_bytes(b"bound setup binary")
                if any(arg.startswith("-test.run=^" + gate.LIFECYCLE) for arg in argv):
                    authority = "PRIMARY" if Path(argv[3]).name.startswith("baseline-") else "DATA"
                    evidence = (f"MVCC_COST_AUTHORITY authority={authority} root=7 commit=5 "
                                "profile=no_wal_fast mode=append_only redo=off\n"
                                f"MVCC_COST_LIFECYCLE authority={authority}\n")
                    if authority == "DATA":
                        evidence += "MVCC_COST_DATA_SLOT fixture=setup\n" * 6
                    kwargs["stdout"].write((evidence + "--- PASS: " + gate.LIFECYCLE + " (0.00s)\nPASS\n").encode())

            def wait(self, timeout):
                return 0

        with tempfile.TemporaryDirectory() as directory:
            root, output = Path(directory) / "source", Path(directory) / "output"
            harness = root / gate.HARNESS
            harness.parent.mkdir(parents=True)
            harness.write_text((path.parents[1] / gate.HARNESS).read_text())
            capability = root / gate.CAPABILITIES["DATA"][0]
            capability.parent.mkdir(parents=True, exist_ok=True)
            capability.write_text(gate.CAPABILITIES["DATA"][1])
            with mock.patch.object(gate.subprocess, "Popen", SetupProcess), \
                    mock.patch.object(gate, "runtime_binding", return_value={}), \
                    mock.patch.object(gate, "ProcessObservation", factory), \
                    mock.patch.object(gate.time, "monotonic", clock), \
                    mock.patch.object(gate.time, "sleep", clock.sleep), \
                    mock.patch.object(Path, "write_text", write_text):
                with self.assertRaisesRegex(RuntimeError, "exhausted; no timed samples"):
                    gate.run_gate(root, output, "a" * 40, "b" * 40)
            self.assertEqual(len(created), len(busy_windows))  # No measured observer is constructed.
            self.assertFalse((output / "measured-processes.jsonl").exists())
            self.assertFalse((output / "samples.json").exists())
            self.assertTrue(any(any(arg.startswith("-test.run=^" + gate.LIFECYCLE) for arg in argv)
                                for argv in commands))
            self.assertFalse(any(any(arg.startswith("-test.bench=") for arg in argv)
                                 for argv in commands))
            failure = json.loads((output / "failure.json").read_text())
            self.assertEqual(failure["retained_samples"], 0)
            self.assertFalse(failure["accepted"])
            acquisition = json.loads((output / "preflight-acquisition.json").read_text())
            self.assertEqual(acquisition["status"], "exhausted")
            if delayed_write:
                self.assertGreater(acquisition["elapsed_s"], gate.QUIET_ACQUISITION_SECONDS)
                window = acquisition["windows"][-1]
                self.assertFalse(window["result"]["busy"])
                self.assertFalse(window["qualified"])
                self.assertFalse(window["artifact_writes_within_cap"])


if __name__ == "__main__":
    unittest.main()
