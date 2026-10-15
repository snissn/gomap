#!/usr/bin/env python3
"""One fixed M0 observation schedule; never a replacement acceptance gate."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import shlex
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True

CANDIDATE = "2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49"
GO_VERSION = "go1.26.9"
GO_SHA = "e90c4e12df236ea0c2e7536fd68da8a47fc0b585c19588bf6c1a70cfc296a0f9"
ARCHIVE_SHA = "42d158b4d8f7b61ac0a830567c940a86098fb7aac52e467a5ebec03ef5cc2f8d"
BENCH_SOURCE = "TreeDB/db/vacuum_collection_bench_test.go"
BENCH_SHA = "86365555acec7e0f11ff9f90913a6bd07c1998fd4c07341c1dd1feaab21ec6a6"
OBSERVER_SOURCE = "scripts/mvcc_primary_cost_gate.py"
OBSERVER_SHA = "58c58cbd8ec10a44681e9d3bd6f2dc8049869b16a45388530983438c2a9f44fb"
LEGACY = "^BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x$"
PUBLIC = "^BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x$"
FIXTURES = ["TestVacuumM0FixtureDeterministicDebtAndOfflineCeiling", "TestVacuumM0WriteArtifact"]
SCRIPTS = Path(__file__).resolve().parent


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def checked_overlay(source: bytes) -> bytes:
    if hashlib.sha256(source).hexdigest() != BENCH_SHA:
        raise ValueError("benchmark preimage mismatch")
    text = source.decode()
    start = text.index("func benchmarkVacuumIndexOnlineCollectionForegroundChurn(")
    stop = text.index("// disableRootPublicationForLegacyVacuumBenchmark", start)
    body = text[start:stop]
    before = "\t\t\tb.ResetTimer()"
    after = "\t\t\tb.StopTimer()"
    if body.count(before) != 1 or body.count(after) != 1:
        raise ValueError("timer seam is not unique")
    body = body.replace(before, "\t\t\tobservation := m0ObservationBegin()\n" + before)
    body = body.replace(after, after + "\n\t\t\tm0ObservationEnd(b, observation, foregroundLatencies)")
    return (text[:start] + body + text[stop:]).encode()


def schedule():
    result = [("fixture-debt", ["-run", "^" + FIXTURES[0] + "$", "-count=1"]),
              ("fixture-artifact", ["-run", "^" + FIXTURES[1] + "$", "-count=1", "-v"])]
    for sample in range(1, 11):
        for arm, selector in (("legacy", LEGACY), ("public", PUBLIC)):
            result.append((f"{arm}-{sample}", ["-run", "^$", "-bench", selector,
                           "-benchtime=1x", "-count=1", "-benchmem"]))
    return result


def validate_operations(record):
    operations = record.get("Operations", [])
    if len(operations) != 4096 or record.get("WallEnd", 0) <= record.get("WallStart", 0):
        raise ValueError("incomplete legacy operation export or wall span")
    for ordinal, operation in enumerate(operations):
        if (operation.get("ordinal") != ordinal or operation.get("type") != ("range" if ordinal % 2 else "point")
                or type(operation.get("duration_ns")) is not int or operation["duration_ns"] < 0):
            raise ValueError("legacy operation export lost order/type/duration")
    for side in ("Before", "After"):
        if not all(name in record.get(side, {}) for name in ("NumGC", "PauseTotalNs", "PauseNs", "PauseEnd")):
            raise ValueError("GC observation missing")


def load_observer(product):
    path = product / OBSERVER_SOURCE
    if digest(path) != OBSERVER_SHA:
        raise ValueError("candidate observer preimage mismatch")
    spec = importlib.util.spec_from_file_location("fixed_primary_observer", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_optional(path):
    try:
        return {"available": True, "raw": Path(path).read_text()}
    except OSError as error:
        return {"available": False, "reason": str(error)}


def host_observation(proc=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
    # Raw cumulative counters preserve units and permit later deltas. Missing
    # files/permissions are explicit, never interpreted as zero contention.
    paths = ["stat", "vmstat", "diskstats", "pressure/cpu", "pressure/io", "pressure/memory", "self/cgroup"]
    result = {name: read_optional(proc / name) for name in paths}
    membership = result["self/cgroup"].get("raw", "")
    v2 = [line[3:] for line in membership.splitlines() if line.startswith("0::")]
    if len(v2) == 1 and ".." not in Path(v2[0]).parts:
        group = cgroup_root / v2[0].lstrip("/")
        # Include visible ancestors: a parent quota can throttle an unlimited
        # leaf. Host ancestors hidden by a cgroup namespace remain unavailable.
        groups = []
        while True:
            groups.append({"path": str(group), "files": {name: read_optional(group / name)
                           for name in ("cpu.max", "cpu.stat", "cpu.pressure", "io.stat", "io.pressure", "memory.pressure")}})
            if group == cgroup_root:
                break
            group = group.parent
        result["cgroups_v2"] = {"available": True, "groups": groups}
    else:
        result["cgroups_v2"] = {"available": False, "reason": "no usable unified cgroup membership; v1 not inferred"}
    return result


class Observation:
    def __init__(self, original, path):
        self.original = original.ProcessObservation(path.with_suffix(".processes.jsonl"))
        self.path = path
        self.observe()

    @property
    def started(self):
        return self.original.started

    def observe(self):
        self.original.observe()
        processes = {}
        # Per-process status/io provide voluntary/involuntary context switches,
        # while stat includes minor/major faults and CPU. Processes that exit
        # entirely between observations remain an explicit blind spot.
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                processes[entry.name] = {name: read_optional(entry / name)
                                        for name in ("stat", "status", "io")}
        with self.path.open("a") as stream:
            stream.write(json.dumps({"wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(),
                                    "host": host_observation(), "processes": processes}) + "\n")

    def finish(self):
        self.observe()
        return self.original.finish()


def manifest(root):
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).split(b"\0")
    return {os.fsdecode(name): digest(root / os.fsdecode(name)) for name in names if name}


def terminate_and_join(process):
    """Kill the session group and reap the leader plus adopted descendants."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()
    end = time.monotonic() + 0.5
    while True:
        try:
            pid, _ = os.waitpid(-process.pid, os.WNOHANG)
        except ChildProcessError:
            return {"leader_joined": True, "adopted_group_children_joined": True}
        if pid == 0:
            if time.monotonic() >= end:
                raise RuntimeError("killed process-group descendants did not join")
            time.sleep(0.01)


class Runner:
    def __init__(self, product, output, goroot, deadline):
        self.product, self.output, self.deadline = product, output, deadline
        self.receipts = []
        self.observation = None
        # Linux-only hosted harness: orphaned grandchildren must be reaped here.
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
        self.environment = dict(os.environ, GOROOT=str(goroot), GOTOOLCHAIN="local", GOWORK="off",
            GOENV="off", GOFLAGS="", GOMAXPROCS="2", GOMEMLIMIT="8GiB", GOGC="100", GODEBUG="",
            GOOS="linux", GOARCH="amd64", GOAMD64="v1", CGO_ENABLED="1", GOEXPERIMENT="",
            GOPROXY="https://proxy.golang.org,direct", GOSUMDB="sum.golang.org",
            CPU_SET="0,1", TMPDIR=str(output / "tmp"), GOCACHE=str(output / "cache"),
            GOMODCACHE=str(output / "modcache"), PATH=str(goroot / "bin") + ":" + os.environ["PATH"])
        for name in ("tmp", "cache", "modcache", "raw", "executables"):
            (output / name).mkdir()
        self.record_environment()

    def record_environment(self):
        names = ["GOROOT", "GOTOOLCHAIN", "GOWORK", "GOENV", "GOFLAGS", "GOMAXPROCS", "GOMEMLIMIT",
                 "GOGC", "GODEBUG", "CPU_SET", "TMPDIR", "GOCACHE", "GOMODCACHE", "PATH", "GOPROXY", "GOSUMDB",
                 "GOOS", "GOARCH", "GOAMD64", "CGO_ENABLED", "GOEXPERIMENT"]
        save(self.output / "environment.json", {name: self.environment.get(name, "<inherited default>") for name in names})

    def command(self, label, argv, timeout=600, extra=None, allow_failure=False):
        argv = [str(value) for value in argv]
        started = time.monotonic()
        remaining = min(timeout, self.deadline - time.time() - 1)
        if remaining <= 0:
            raise TimeoutError("original overall schedule cap exhausted")
        env = dict(self.environment, **(extra or {}))
        receipt = {"label": label, "argv": argv, "cwd": str(self.product), "wall_start_ns": time.time_ns(),
                   "timeout_s": remaining, "environment_additions": extra or {}, "group_killed": False}
        self.receipts.append(receipt)
        save(self.output / "commands.json", self.receipts)
        path = self.output / "raw" / (label + ".txt")
        with path.open("wb") as stream:
            process = subprocess.Popen(argv, cwd=self.product, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            receipt.update(pid=process.pid, pgid=process.pid)
            try:
                while True:
                    tick = time.monotonic()
                    if self.observation:
                        self.observation.observe()
                    left = remaining - (time.monotonic() - started)
                    if left <= 0:
                        raise subprocess.TimeoutExpired(argv, remaining)
                    try:
                        status = process.wait(timeout=min(max(0, 0.25 - (time.monotonic() - tick)), left))
                        break
                    except subprocess.TimeoutExpired:
                        continue
            except BaseException:
                receipt.update(terminate_and_join(process))
                receipt["group_killed"] = True
                receipt["interrupted"] = True
                raise
            finally:
                receipt.update(exit=process.returncode, joined=process.returncode is not None,
                               elapsed_s=time.monotonic() - started, wall_end_ns=time.time_ns(),
                               output=path.relative_to(self.output).as_posix(), output_sha256=digest(path))
                save(self.output / "commands.json", self.receipts)
        if status and not allow_failure:
            raise RuntimeError(f"{label} exit {status}")
        return status


def run(product, output, goroot, job_started):
    output.mkdir(parents=True, exist_ok=True)
    runner = None
    before = None
    preflight = None
    try:
        # Reserve one minute within the unchanged 20-minute job for finalization
        # and artifact upload. No operation or overall bound is enlarged.
        runner = Runner(product, output, goroot, job_started + 19 * 60)
        if digest(goroot / "bin/go") != GO_SHA:
            raise ValueError("exact official Go1.26.9 executable required")
        if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=product, text=True).strip() != CANDIDATE:
            raise ValueError("fixed candidate required")
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=product):
            raise ValueError("pristine product worktree required")
        original = load_observer(product)
        pristine = manifest(product)
        before = pristine
        save(output / "source-pristine.json", pristine)
        runner.command("module-download", [goroot / "bin/go", "mod", "download"])
        runner.command("module-verify", [goroot / "bin/go", "mod", "verify"])
        runner.environment.update(GOPROXY="off", GOSUMDB="off")
        runner.record_environment()
        before = manifest(product)
        if before != pristine:
            raise ValueError("dependency preparation changed pristine candidate inputs")
        save(output / "source-before.json", before)
        runner.command("archive-pristine", ["git", "archive", "--format=tar.gz", "-o", output / "pristine-source.tar.gz", "HEAD"])
        harness = SCRIPTS.parent.parent
        harness_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=harness, text=True).strip()
        if os.environ.get("GITHUB_SHA") != harness_head:
            raise ValueError("execution requires the exact reviewed hosted harness commit")
        if subprocess.check_output(["git", "status", "--porcelain"], cwd=harness):
            raise ValueError("pristine reviewed harness checkout required")
        save(output / "harness-source.json", manifest(harness))
        retained = output / "harness"
        retained.mkdir()
        for path in SCRIPTS.glob("*m0_observation*"):
            if path.is_file():
                shutil.copy2(path, retained / path.name)
        save(output / "sdk-manifest.json", {str(path.relative_to(goroot)): digest(path)
             for path in sorted(goroot.rglob("*")) if path.is_file()})
        runner.command("go-version", [goroot / "bin/go", "version"])
        runner.command("go-environment", [goroot / "bin/go", "env", "-json"])
        for label, argv in (("uname", ["uname", "-a"]), ("lscpu", ["lscpu"]),
                            ("filesystem", ["df", "-PT", output]), ("c-compiler", ["gcc", "--version"])):
            runner.command(label, argv)
        overlay_source = output / "vacuum_collection_bench_test.go"
        overlay_source.write_bytes(checked_overlay((product / BENCH_SOURCE).read_bytes()))
        helper = SCRIPTS / "mvcc_m0_observation_gc.go.txt"
        overlay = output / "overlay.json"
        save(overlay, {"Replace": {str(product / BENCH_SOURCE): str(overlay_source),
             str(product / "TreeDB/db/mvcc_m0_observation_overlay_test.go"): str(helper)}})
        wrapper = SCRIPTS / "mvcc_m0_observation_exec.py"
        save(output / "bindings.json", {"diagnostic_only": True, "candidate": CANDIDATE,
            "harness_head": harness_head,
            "overlay_preimage_sha256": BENCH_SHA, "overlay_source_sha256": digest(overlay_source),
            "overlay_helper_sha256": digest(helper), "exec_wrapper_sha256": digest(wrapper),
            "observer_source_sha256": OBSERVER_SHA, "go_archive_sha256": ARCHIVE_SHA,
            "go_sha256": GO_SHA, "compiler_sha256": digest(goroot / "pkg/tool/linux_amd64/compile"),
            "linker_sha256": digest(goroot / "pkg/tool/linux_amd64/link"),
            "schedule": schedule(), "infrastructure": [
                "INFRASTRUCTURE_UNAVAILABLE: runner: dedicated persistent runner; ephemeral ubuntu-24.04 used",
                "INFRASTRUCTURE_UNAVAILABLE: cache: persistent build cache; fresh task cache used",
                "artifact storage: GitHub artifact retention, not a permanent external archive"]})
        for arm, flags in (("pristine", []), ("overlay", ["-overlay", overlay])):
            binary = output / (arm + ".test")
            runner.command("compile-" + arm, [goroot / "bin/go", "test", *flags, "-c", "-o", binary, "./TreeDB/db"])
            runner.command("buildinfo-" + arm, [goroot / "bin/go", "version", "-m", binary])
        preflight = Observation(original, output / "quiet-preflight.jsonl")
        while time.monotonic() - preflight.started < 10:
            if time.time() >= runner.deadline:
                raise TimeoutError("overall cap exhausted during single quiet preflight")
            tick = time.monotonic()
            preflight.observe()
            time.sleep(max(0, 0.25 - (time.monotonic() - tick)))
        quiet = preflight.finish()
        preflight = None
        save(output / "quiet-preflight.json", quiet)
        if quiet["busy"]:
            save(output / "refusal.json", {"reason": "single 10s/2% quiet preflight refused", "executed_calls": 0})
            return 2
        runner.observation = Observation(original, output / "observations.jsonl")
        filesystem = subprocess.check_output(["df", "-PT", str(output)], text=True).splitlines()[-1].split()
        common = ["taskset", "-c", "0,1", str(goroot / "bin/go"), "test", "-overlay", str(overlay),
                  "-exec", f"{sys.executable} {wrapper}", "./TreeDB/db"]
        (output / "commands.txt").write_text("\n".join(shlex.join(common + args) for _, args in schedule()) + "\n")
        for label, args in schedule():
            extra = {"MVCC_M0_EXEC_DIR": str(output / "executables" / label),
                     "MVCC_M0_OPERATIONS": str(output / (label + "-operations.json"))}
            if label == "fixture-artifact":
                extra.update(TREEDB_VACUUM_M0_ARTIFACT=str(output / "fixture.json"),
                    TREEDB_VACUUM_M0_COMMAND="observation-only original M0 schedule (commands.json)",
                    TREEDB_VACUUM_M0_GIT_SHA=CANDIDATE, TREEDB_VACUUM_M0_DIRTY_STATE="clean+explicit-test-overlay",
                    TREEDB_VACUUM_M0_FILESYSTEM=filesystem[1], TREEDB_VACUUM_M0_DEVICE=filesystem[0],
                    TREEDB_VACUUM_M0_TIMING_BOUNDARY="one fixed-work vacuum operation; setup excluded; diagnostic observer")
            runner.command(label, common + args, extra=extra)
            if label.startswith("legacy-"):
                validate_operations(json.loads(Path(extra["MVCC_M0_OPERATIONS"]).read_text()))
        (output / "legacy-benchstat-input.txt").write_text("".join(
            (output / "raw" / f"legacy-{sample}.txt").read_text() for sample in range(1, 11)))
        # Use the exact candidate summarizer and preserve its exit/outcome.
        runner.command("summarizer-unit", [sys.executable, product / "scripts/treedb_vacuum_m0_summarize_test.py"])
        return runner.command("summarizer", [sys.executable, product / "scripts/treedb_vacuum_m0_summarize.py",
            "--repo-root", product, "--run-dir", output], allow_failure=True)
    except BaseException as error:
        save(output / "failure.json", {"error": repr(error), "diagnostic_only": True})
        raise
    finally:
        if runner and runner.observation:
            save(output / "observations-summary.json", runner.observation.finish())
        if preflight is not None:
            save(output / "quiet-preflight-incomplete.json", preflight.finish())
        if before is not None:
            after = manifest(product)
            save(output / "source-after.json", after)
            save(output / "source-equality.json", {"equal": before == after})
            if before != after:
                save(output / "failure-source-drift.json", {"error": "source changed", "diagnostic_only": True})
        save(output / "artifact-hashes.json", {str(path.relative_to(output)): digest(path)
             for path in sorted(output.rglob("*")) if path.is_file() and
             not any(part in ("tmp", "cache", "modcache") for part in path.relative_to(output).parts)
             and path.name != "artifact-hashes.json"})
        if before is not None and before != after:
            raise RuntimeError("product source drift invalidates diagnostic")


if __name__ == "__main__":
    def interrupted(signum, _frame):
        raise InterruptedError(f"received signal {signum}; terminate and join active group")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--goroot", type=Path, required=True)
    parser.add_argument("--job-started", type=float, required=True)
    options = parser.parse_args()
    sys.exit(run(options.product.resolve(), options.output.resolve(), options.goroot.resolve(), options.job_started))
