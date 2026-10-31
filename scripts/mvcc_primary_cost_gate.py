#!/usr/bin/env python3
"""Measure the same public PRIMARY replacement on exact base/head binaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import stat
import subprocess
import sys
import tempfile
import time


BENCHMARK = "BenchmarkCommitAtPrimaryOnlyAfterDataGeneration"
HARNESS = "TreeDB/mvcc/primary_generation_routing_v6_test.go"
LIFECYCLE = "TestCommitAtPrimaryOnlyRetainsActualDataGeneration"
SAMPLES = 8
ITERATIONS = 128
TIME_LIMIT_PERCENT = 5.0
QUIET_LIMIT_PERCENT = 10.0
QUIET_AGGREGATE_LIMIT_PERCENT = 50.0
QUIET_WARNING_PERCENT = 2.0
QUIET_WINDOW_SECONDS = 5.0
QUIET_ACQUISITION_SECONDS = 60.0
ROW = re.compile(r"^" + BENCHMARK + r"(?:-\d+)?\s+(\d+)\s+(.*)$")
METRICS = {"ns/op", "B/op", "allocs/op", "generation_advances/op"}
CORRECTNESS_PR = 5346
CORRECTNESS_BASE = "4dc879a15b9acaf5dd45444627c7e62b3f829a9d"
CORRECTNESS_SOURCE = "c5c25181898550e66139fb327f89ce79b7a454b2"
CORRECTNESS_TIME_LIMIT_PERCENT = 6.58
CORRECTNESS_POLICY_PATHS = {
    "scripts/mvcc_primary_cost_gate.py",
    ".github/scripts/test_mvcc_primary_cost_gate.py",
    ".github/workflows/treedb-tests.yml",
    ".github/ci/ci_impact.json",
    "scripts/mvcc_raw_path_gate.md",
}


def correctness_timing_scope(command, baseline: str, candidate: str, pull_request: int | None) -> dict | None:
    """Bind the owner's one-PR decision to unchanged production and harness bytes."""
    if pull_request != CORRECTNESS_PR or baseline != CORRECTNESS_BASE:
        return None
    command("timing-policy-ancestry", ["git", "merge-base", "--is-ancestor", CORRECTNESS_SOURCE, candidate])
    paths = command("timing-policy-diff", ["git", "diff", "--name-only", CORRECTNESS_SOURCE, candidate]).splitlines()
    if set(paths) - CORRECTNESS_POLICY_PATHS:
        return None
    return {"pull_request": pull_request, "baseline": baseline, "candidate": candidate,
            "frozen_production_source": CORRECTNESS_SOURCE, "changed_paths": paths,
            "max_paired_regression_percent": CORRECTNESS_TIME_LIMIT_PERCENT,
            "decision": "https://github.com/snissn/gomap/pull/5346#issuecomment-6548619056",
            "historical_raw_failure": {"run": 41235196330, "job": 124637377629,
                "artifact": 13055681212,
                "artifact_sha256": "a4679d9b40299986ef8a1f4576cc6315fdb038b4d43ea1cbc4c9dc7d2cfad0f1"}}


def apply_correctness_timing_acceptance(result: dict, scope: dict | None) -> dict:
    """Accept only the authorized timing cost; never alter raw evidence."""
    result = dict(result, correctness_timing_policy=scope)
    delta = result["median_paired_delta_percent"]
    if (scope is not None and result["timing_disposition"] == "FAIL" and result["quiet"]
            and math.isfinite(delta) and delta <= scope["max_paired_regression_percent"]
            and all(value for name, value in result["gates"].items() if name != "timing")
            and all(value for name, value in result["acceptance_gates"].items() if name != "timing")):
        acceptance = dict(result["acceptance_gates"], timing=True)
        result.update(acceptance_gates=acceptance, accepted=all(acceptance.values()),
                      timing_disposition="ACCEPTED_CORRECTNESS_TRADEOFF")
    return result


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        result = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def file_identity(path: Path) -> dict:
    resolved = path.resolve(strict=True)
    if not stat.S_ISREG(resolved.stat().st_mode):
        raise ValueError(f"runtime input is not a regular file: {path}")
    return {"path": str(resolved), "sha256": digest(resolved), "bytes": resolved.stat().st_size}


def runtime_binding(binary: Path, command, label: str, environment: dict) -> dict:
    """Resolve the ELF's actual loader inputs under the measurement environment."""
    if environment.get("LD_AUDIT"):
        raise ValueError("LD_AUDIT runtime inputs are not supported for identity binding")
    headers = command(label + "-elf", ["readelf", "--program-headers", "--wide", binary])
    interpreters = re.findall(r"\[Requesting program interpreter: (/[^\]\n]+)\]", headers)
    executable = file_identity(binary)
    executable.pop("path")  # The two measured copies intentionally have different names.
    if not interpreters:
        if (not re.search(r"(?m)^Elf file type is (?:EXEC|DYN)\b", headers)
                or "Program Headers:" not in headers
                or not re.search(r"(?m)^\s*LOAD\s+", headers)
                or re.search(r"(?m)^\s*(?:INTERP|DYNAMIC)\s+", headers)):
            raise ValueError("static ELF requires valid load headers without INTERP or DYNAMIC")
        return {"linkage": "static", "executable": executable,
                "interpreter": None, "libraries": []}
    if len(interpreters) != 1:
        raise ValueError("one dynamic ELF interpreter is required for runtime binding")
    interpreter = file_identity(Path(interpreters[0]))
    listing = command(label + "-loader", [interpreter["path"], "--list", binary])
    libraries = {}
    for line in listing.splitlines():
        line = line.strip()
        if re.fullmatch(r"linux-vdso\.so\.\d+ \(0x[0-9a-fA-F]+\)", line):
            continue  # Kernel mapping, not a filesystem input.
        match = re.fullmatch(r"(?:\S+ => )?(/.+?) \(0x[0-9a-fA-F]+\)", line)
        if not match:
            raise ValueError(f"unresolved or unsupported ELF runtime input: {line}")
        identity = file_identity(Path(match[1]))
        libraries[identity["path"]] = identity
    if interpreter["path"] not in libraries:
        raise ValueError("ELF loader listing did not bind its interpreter")
    return {"linkage": "dynamic", "executable": executable, "interpreter": interpreter,
            "libraries": list(libraries.values())}


def qualify_runtime(result: dict, before: dict, after: dict, *, quiet: bool) -> dict:
    """Keep raw measurements; qualify timing/bytes only for the same bound runtime."""
    if set(before) != {"baseline", "candidate"} or before != after:
        raise RuntimeError("measured runtime/input drift or missing bindings")
    for binding in before.values():
        runtime_present = ((binding.get("linkage") == "static"
                            and "interpreter" in binding and binding["interpreter"] is None
                            and binding.get("libraries") == [])
                           or (binding.get("linkage") == "dynamic"
                               and binding.get("interpreter") and binding.get("libraries")))
        if (not binding.get("executable") or not runtime_present
                or set(binding.get("inputs", {})) != {"harness_sha256", "runner_sha256", "environment_sha256", "cwd", "argv"}):
            raise RuntimeError("missing measured runtime/input bindings")
    equivalent = before["baseline"] == before["candidate"]
    acceptance = dict(result["gates"])
    dispositions = {}
    for metric in ("timing", "bytes"):
        raw_pass = result["gates"][metric]
        acceptance[metric] = raw_pass or equivalent
        dispositions[metric + "_disposition"] = ("PASS" if raw_pass else
            "EQUIVALENT_NON_ATTRIBUTABLE" if equivalent else "FAIL")
    return dict(result, quiet=quiet, runtime_input_equivalent=equivalent, **dispositions,
                acceptance_gates=acceptance, accepted=all(acceptance.values()) and quiet)


def parse_sample(text: str) -> dict[str, float]:
    rows = [m for line in text.splitlines() if (m := ROW.match(line.strip()))]
    if len(rows) != 1 or int(rows[0][1]) != ITERATIONS:
        raise ValueError("exactly one 128-iteration PRIMARY sample is required")
    parts = rows[0][2].split()
    if len(parts) % 2:
        raise ValueError("malformed benchmark metrics")
    result = {}
    for raw, unit in zip(parts[::2], parts[1::2]):
        if unit in result or unit not in METRICS:
            raise ValueError("duplicate or unexpected benchmark metric")
        value = float(raw)
        if not math.isfinite(value) or value < 0:
            raise ValueError("nonfinite or negative metric")
        result[unit] = value
    if set(result) != METRICS or result["ns/op"] <= 0:
        raise ValueError("missing metrics or nonpositive timing")
    if "PASS" not in text.splitlines() or re.search(r"(?m)^(FAIL|--- FAIL:|panic:)", text):
        raise ValueError("benchmark did not complete successfully")
    return result


def evaluate(rows: list[dict]) -> dict:
    expected = [(pair, revision) for pair in range(SAMPLES)
                for revision in (("baseline", "candidate") if pair % 2 == 0
                                 else ("candidate", "baseline"))]
    if [(r["pair"], r["revision"]) for r in rows] != expected:
        raise ValueError("missing, duplicated or reordered planned samples")
    by_revision = {revision: [r["metrics"] for r in rows if r["revision"] == revision]
                   for revision in ("baseline", "candidate")}
    medians = {revision: {unit: statistics.median(r[unit] for r in values)
                          for unit in METRICS}
               for revision, values in by_revision.items()}
    paired = [(head["ns/op"] / base["ns/op"] - 1) * 100
              for base, head in zip(by_revision["baseline"], by_revision["candidate"])]
    delta = statistics.median(paired)
    base, head = medians["baseline"], medians["candidate"]
    gates = {"timing": delta <= TIME_LIMIT_PERCENT,
             "bytes": head["B/op"] <= base["B/op"],
             "allocations": head["allocs/op"] <= base["allocs/op"],
             "candidate_primary_activation": all(r["generation_advances/op"] == 0
                                                 for r in by_revision["candidate"])}
    return {"medians": medians, "paired_deltas_percent": paired,
            "median_paired_delta_percent": delta, "gates": gates,
            "measurement_pass": all(gates.values())}


def processes() -> dict:
    result = {}
    for directory in Path("/proc").iterdir():
        if not directory.name.isdigit():
            continue
        try:
            raw = (directory / "stat").read_text()
            fields = raw[raw.rindex(")") + 2:].split()
            result[int(directory.name)] = {"parent": int(fields[1]),
                "ticks": int(fields[11]) + int(fields[12]), "start": fields[19],
                "command": raw[raw.index("(") + 1:raw.rindex(")")]}
        except (OSError, ValueError, IndexError):
            continue
    return result


def allowed_processes(snapshot: dict) -> set[int]:
    allowed = {os.getpid()}
    parent = os.getpid()
    while parent in snapshot and snapshot[parent]["parent"] > 0:
        parent = snapshot[parent]["parent"]
        allowed.add(parent)
    # Only this runner's descendants are part of the workload. Sibling runner
    # jobs remain external even though they share one of our ancestors.
    descendants = {os.getpid()}
    while True:
        added = {pid for pid, record in snapshot.items() if record["parent"] in descendants}
        if added <= descendants:
            break
        descendants |= added
    return allowed | descendants


class ProcessObservation:
    """Retain sampled CPU increments, including processes that exit mid-run."""

    def __init__(self, path: Path):
        self.path = path
        self.started = time.monotonic()
        self.previous = {}
        self.totals = {}
        self.first = True
        self.observe()

    def observe(self):
        snapshot = processes()
        self.accumulate(snapshot)
        with self.path.open("a") as stream:
            stream.write(json.dumps({"elapsed_s": time.monotonic() - self.started,
                                     "processes": snapshot}, sort_keys=True) + "\n")

    def accumulate(self, snapshot):
        allowed = allowed_processes(snapshot)
        for pid, record in snapshot.items():
            key = (pid, record["start"])
            previous = self.previous.get(key, record["ticks"] if self.first else 0)
            if pid not in allowed:
                total = self.totals.setdefault(key, {"pid": pid, "start": record["start"],
                                                     "command": record["command"], "ticks": 0})
                total["ticks"] += max(0, record["ticks"] - previous)
            self.previous[key] = record["ticks"]
        self.first = False

    def finish(self):
        self.observe()
        elapsed = time.monotonic() - self.started
        rows = [dict(record, cpu_percent=100 * record["ticks"] / os.sysconf("SC_CLK_TCK") / elapsed)
                for record in self.totals.values()]
        return dict(elapsed_s=elapsed, **classify_external_cpu(rows))


def classify_external_cpu(rows: list[dict]) -> dict:
    """Conservative admission policy in percent of ONE logical CPU, not host capacity.

    No process-name exemptions or affinity discounts: sampled placement cannot
    establish where a process ran throughout an interval. Aggregate includes all
    observed external processes, including kernel workers, and is a partial estimate: processes between samples may be missed,
    and first-seen processes can include CPU from before the interval.
    """
    total = sum(record["cpu_percent"] for record in rows)
    aggregate_busy = total >= QUIET_AGGREGATE_LIMIT_PERCENT
    return {"external_processes": rows,
            "busy": [record for record in rows if record["cpu_percent"] >= QUIET_LIMIT_PERCENT
                     or (aggregate_busy and record["cpu_percent"] > 0)],
            "warnings": [record for record in rows if record["cpu_percent"] >= QUIET_WARNING_PERCENT],
            "aggregate_cpu_percent": total, "aggregate_busy": aggregate_busy,
            "cpu_threshold_percent": QUIET_LIMIT_PERCENT,
            "aggregate_cpu_threshold_percent": QUIET_AGGREGATE_LIMIT_PERCENT,
            "warning_cpu_threshold_percent": QUIET_WARNING_PERCENT,
            "cpu_percent_basis": "one logical CPU (100 * ticks / CLK_TCK / elapsed seconds)",
            "policy": "external-cpu-v2", "sampling_interval_s": 0.25,
            "limit": "observed aggregate is partial; processes entirely between observations may be missed; "
                     "first-seen processes may include pre-window CPU; kernel workers count as external; CPU admission does not prove absence of I/O or timing noise"}



def acquire_quiet_window(output: Path, *, clock=None, sleep=None, observer_factory=None) -> dict:
    """Acquire the first original quiet window before any timed sample, or fail closed."""
    clock = clock or time.monotonic
    sleep = sleep or time.sleep
    observer_factory = observer_factory or ProcessObservation
    started = clock()
    deadline = started + QUIET_ACQUISITION_SECONDS
    acquisition = {"status": "acquiring", "setup_cap_s": QUIET_ACQUISITION_SECONDS,
                   "window_s": QUIET_WINDOW_SECONDS, "cpu_threshold_percent": QUIET_LIMIT_PERCENT,
                   "aggregate_cpu_threshold_percent": QUIET_AGGREGATE_LIMIT_PERCENT, "policy": "external-cpu-v2",
                   "windows": []}

    def save_acquisition():
        acquisition["elapsed_s"] = clock() - started
        (output / "preflight-acquisition.json").write_text(
            json.dumps(acquisition, indent=2, sort_keys=True) + "\n")

    save_acquisition()
    # Do not shorten the original predicate to use an incomplete final window.
    while deadline - clock() >= QUIET_WINDOW_SECONDS:
        number = len(acquisition["windows"]) + 1
        name = "preflight-processes.jsonl" if number == 1 else f"preflight-processes-{number:02d}.jsonl"
        observer = observer_factory(output / name)
        while clock() - observer.started < QUIET_WINDOW_SECONDS:
            remaining = min(QUIET_WINDOW_SECONDS - (clock() - observer.started), deadline - clock())
            if remaining <= 0:
                break
            sleep(min(0.25, remaining))
            observer.observe()
        result = observer.finish()
        finished = clock()
        complete = result["elapsed_s"] >= QUIET_WINDOW_SECONDS
        within_cap = finished <= deadline
        qualified = complete and within_cap and not result["busy"]
        acquisition["windows"].append({"observation_file": name, "result": result,
            "finished_after_s": finished - started, "complete": complete,
            "within_cap": within_cap, "qualified": qualified})
        # Keep the existing final-window schema for existing artifact consumers.
        (output / "preflight.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        if qualified:
            acquisition["status"] = "acquired"
        save_acquisition()
        if qualified:
            authorized = clock()
            if authorized <= deadline:
                return result
            # Artifact writes are part of acquisition, even when observation was quiet.
            acquisition["windows"][-1].update(qualified=False,
                authorization_after_s=authorized - started, artifact_writes_within_cap=False)
            break
    acquisition["status"] = "exhausted"
    save_acquisition()
    raise RuntimeError("host quiet preflight acquisition exhausted; no timed samples started")


def run_gate(root: Path, output: Path, baseline: str, candidate: str, pull_request: int | None = None) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", baseline) or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ValueError("full immutable source SHAs are required")
    if baseline == candidate or output.exists() or output == root or root in output.parents:
        raise ValueError("distinct SHAs and fresh external output directory are required")
    output.mkdir(parents=True)
    (output / "tmp").mkdir()
    environment = dict(os.environ, GOWORK="off", GOMAXPROCS="1", TMPDIR=str(output / "tmp"))
    receipts = []
    observation = None

    def save(name, value):
        (output / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")

    def command(label, argv, cwd=root, timeout=180):
        started = time.monotonic()
        path = output / (label + ".txt")
        interrupted = None
        with path.open("wb") as stream:
            process = subprocess.Popen([str(x) for x in argv], cwd=cwd, env=environment,
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                while True:
                    if observation is not None:
                        observation.observe()
                    remaining = timeout - (time.monotonic() - started)
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, timeout)
                    try:
                        status = process.wait(timeout=min(0.25, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        continue
            except BaseException as error:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                status = 124 if isinstance(error, subprocess.TimeoutExpired) else 125
                interrupted = error
        receipts.append({"label": label, "argv": [str(x) for x in argv], "cwd": str(cwd),
                         "exit": status, "elapsed_s": time.monotonic() - started,
                         "output": path.name, "output_sha256": digest(path)})
        save("commands.json", receipts)
        if interrupted is not None:
            raise RuntimeError(f"{label} interrupted; child process group terminated and joined") from interrupted
        if status:
            raise RuntimeError(f"{label} failed with exit {status}")
        return path.read_text()

    command("candidate-guard-before", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
    timing_scope = correctness_timing_scope(command, baseline, candidate, pull_request)
    save("timing-acceptance-policy.json", {"default_limit_percent": TIME_LIMIT_PERCENT,
                                         "correctness_exception": timing_scope})
    for label, argv in (("go-version", ["go", "version"]), ("uname", ["uname", "-a"]),
                        ("lscpu", ["lscpu"]), ("processes", ["ps", "-eo", "pid,ppid,etime,pcpu,args"])):
        command(label, argv)
    work = Path(tempfile.mkdtemp(prefix="mvcc-primary-cost-", dir=os.environ.get("RUNNER_TEMP") or os.environ.get("TMPDIR")))
    base_root = work / "baseline"
    worktree_added = False
    rows = []
    try:
        command("baseline-worktree", ["git", "worktree", "add", "--detach", base_root, baseline])
        worktree_added = True
        harness = output / "primary_generation_routing_v6_test.go"
        harness.write_bytes((root / HARNESS).read_bytes())
        overlay = output / "baseline-overlay.json"
        save(overlay.name, {"Replace": {str(base_root / HARNESS): str(harness)}})
        binaries = {}
        for revision, source in (("baseline", base_root), ("candidate", root)):
            binary = output / (revision + "-primary.test")
            argv = ["go", "test", "-c", "-p=1", "-tags=treedb_test", "-trimpath", "-buildvcs=false"]
            if revision == "baseline":
                argv += ["-overlay", overlay]
            command("build-" + revision, argv + ["-o", binary, "./TreeDB/mvcc"], source, timeout=600)
            binaries[revision] = {"sha256": digest(binary), "bytes": binary.stat().st_size,
                                  "source_sha": baseline if revision == "baseline" else candidate,
                                  "path": binary.name}
        save("bindings.json", {"baseline": baseline, "candidate": candidate,
             "harness_sha256": digest(harness), "runner_sha256": digest(Path(__file__)),
             "samples": SAMPLES, "iterations": ITERATIONS, "cpu": 0, "gomaxprocs": 1,
             "max_paired_regression_percent": TIME_LIMIT_PERCENT,
             "correctness_timing_policy": timing_scope,
             "quiet_preflight": {"window_s": QUIET_WINDOW_SECONDS, "setup_cap_s": QUIET_ACQUISITION_SECONDS,
                                 "cpu_threshold_percent": QUIET_LIMIT_PERCENT,
                                 "aggregate_cpu_threshold_percent": QUIET_AGGREGATE_LIMIT_PERCENT,
                                 "policy": "external-cpu-v2"},
             "bytes_and_allocations": "no median growth", "binaries": binaries})
        benchmark_argv = ["-test.run=^$", "-test.bench=^" + BENCHMARK + "$",
                          "-test.benchtime=128x", "-test.count=1", "-test.benchmem", "-test.timeout=90s"]

        def bind_inputs(phase):
            bindings = {}
            for revision, record in binaries.items():
                binding = runtime_binding(output / record["path"], command, phase + "-" + revision, environment)
                binding["inputs"] = {
                    "harness_sha256": digest(harness), "runner_sha256": digest(Path(__file__)),
                    "environment_sha256": hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest(),
                    "cwd": str(root), "argv": ["taskset", "-c", "0", "<measured-binary>"] + benchmark_argv,
                }
                bindings[revision] = binding
            save("runtime-inputs-" + phase + ".json", bindings)
            return bindings

        inputs_before = bind_inputs("before")
        lifecycle = command("candidate-public-lifecycle", ["taskset", "-c", "0", output / "candidate-primary.test",
                "-test.run=^" + LIFECYCLE + "$", "-test.count=1", "-test.timeout=90s", "-test.v"])
        if f"--- PASS: {LIFECYCLE} " not in lifecycle or "--- SKIP:" in lifecycle:
            raise RuntimeError("actual candidate public lifecycle control did not pass")
        acquire_quiet_window(output)
        observation = ProcessObservation(output / "measured-processes.jsonl")
        for pair in range(SAMPLES):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for revision in order:
                text = command(f"pair-{pair+1}-{revision}", ["taskset", "-c", "0",
                     output / binaries[revision]["path"]] + benchmark_argv)
                rows.append({"pair": pair, "revision": revision, "metrics": parse_sample(text)})
                save("samples.json", rows)
        observed = observation.finish()
        observation = None
        busy = observed["busy"]
        save("measured-processes.json", observed)
        command("candidate-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, candidate])
        command("baseline-guard-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", base_root, baseline])
        for record in binaries.values():
            if digest(output / record["path"]) != record["sha256"]:
                raise RuntimeError("measured binary drift")
        if digest(root / HARNESS) != digest(harness):
            raise RuntimeError("benchmark input drift")
        inputs_after = bind_inputs("after")
        result = qualify_runtime(evaluate(rows), inputs_before, inputs_after, quiet=not busy)
        result = apply_correctness_timing_acceptance(result, timing_scope)
        result.update(baseline=baseline, candidate=candidate)
        save("summary.json", result)
        (output / "summary.md").write_text(
            "## Public PRIMARY replacement cost\n\n"
            f"- Verdict: {'PASS' if result['accepted'] else 'FAIL'}\n"
            f"- Exact base/head: `{baseline}` / `{candidate}`\n"
            "- Eight alternating AB/BA pairs; 128 durable public replacements per sample.\n"
            f"- Median paired time delta: {result['median_paired_delta_percent']:+.3f}% (limit +5%).\n"
            f"- Raw gates: `{json.dumps(result['gates'], sort_keys=True)}`; measurement pass: {result['measurement_pass']}; quiet: {not busy}.\n"
            f"- Timing acceptance: `{result['timing_disposition']}`; bytes acceptance: `{result['bytes_disposition']}`; bound runtime/input equivalent: {result['runtime_input_equivalent']}.\n"
            f"- Scoped correctness timing policy: `{json.dumps(timing_scope, sort_keys=True)}`. Raw gates remain unchanged.\n"
            "- Scope: this public replacement workload; not final graph qualification.\n")
        return 0 if result["accepted"] else 1
    except Exception as error:
        if observation is not None:
            save("measured-processes.json", observation.finish())
            observation = None
        save("failure.json", {"error": str(error), "retained_samples": len(rows), "accepted": False})
        raise
    finally:
        if worktree_added:
            command("baseline-worktree-remove", ["git", "worktree", "remove", base_root])
        shutil.rmtree(work)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--pull-request", type=int)
    args = parser.parse_args()
    return run_gate(args.root.resolve(), args.output.resolve(), args.baseline, args.candidate, args.pull_request)


if __name__ == "__main__":
    sys.exit(main())
