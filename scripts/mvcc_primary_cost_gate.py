#!/usr/bin/env python3
"""Measure the same public durable replacement on exact base/head binaries."""

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


BENCHMARK = "BenchmarkCommitAtDurableReplacementAfterDataGeneration"
HARNESS = "TreeDB/mvcc/durable_replacement_cost_test.go"
LIFECYCLE = "TestCommitAtDurableReplacementLifecycle"
ROLE_DECLARATION = 'const durableReplacementAuthority = "DATA"'
PRIMARY_SETUP = "// MVCC_COST_PRIMARY_SETUP"
CAPABILITIES = {
    "PRIMARY": ("TreeDB/db/durable_primary_runtime_v5.go", "func (db *DB) attachPrimaryArenaV5("),
    "DATA": ("TreeDB/db/retired_primary_format.go", "func ValidateDATAIndexOpen("),
}
SAMPLES = 8
ITERATIONS = 128
TIME_LIMIT_PERCENT = 5.0
QUIET_LIMIT_PERCENT = 2.0
QUIET_WINDOW_SECONDS = 5.0
QUIET_ACQUISITION_SECONDS = 60.0
ROW = re.compile(r"^" + BENCHMARK + r"(?:-\d+)?\s+(\d+)\s+(.*)$")
METRICS = {"ns/op", "B/op", "allocs/op", "generation_advances/op"}


def source_authority(root: Path) -> dict:
    """Select an explicit setup from actual implementation; never retry another route."""
    found = []
    for authority, (relative, declaration) in CAPABILITIES.items():
        path = root / relative
        if path.exists():
            if declaration not in path.read_text():
                raise ValueError(f"unrecognized {authority} source capability: {relative}")
            found.append({"authority": authority, "path": relative, "sha256": digest(path)})
    if len(found) != 1:
        raise ValueError("exactly one recognized PRIMARY or DATA source capability is required")
    return found[0]


def render_harness(template: str, authority: str) -> str:
    if template.count(ROLE_DECLARATION) != 1 or template.count(PRIMARY_SETUP) != 1:
        raise ValueError("maintained cost fixture requires one role and one setup marker")
    if authority not in CAPABILITIES:
        raise ValueError("unsupported source authority")
    if authority == "PRIMARY":
        return template.replace(ROLE_DECLARATION, 'const durableReplacementAuthority = "PRIMARY"').replace(
            PRIMARY_SETUP, "opts.IndexPrimaryDirectory = true // Explicit PRIMARY control.")
    return template


def parse_authority(text: str, expected: str) -> str:
    observed = re.findall(r"(?m)^\s*(?:\S+\.go:\d+:\s+)?MVCC_COST_AUTHORITY authority=(PRIMARY|DATA) root=\d+ commit=\d+ profile=\S+ mode=\S+ redo=off$", text)
    if expected not in CAPABILITIES or not observed or any(role != expected for role in observed):
        raise ValueError("missing or mismatched actual public authority evidence")
    return expected


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
        raise ValueError("exactly one 128-iteration durable replacement sample is required")
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


def evaluate(rows: list[dict], authorities: dict[str, str]) -> dict:
    if set(authorities) != {"baseline", "candidate"} or any(role not in CAPABILITIES for role in authorities.values()):
        raise ValueError("both source-bound arm authorities are required")
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
             "baseline_route_activation": all(r.get("authority") == authorities["baseline"]
                                               for r in rows if r["revision"] == "baseline"),
             "candidate_route_activation": all(r.get("authority") == authorities["candidate"]
                                                for r in rows if r["revision"] == "candidate")
                and (authorities["candidate"] != "PRIMARY" or all(
                    r["generation_advances/op"] == 0 for r in by_revision["candidate"]))}
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
        return {"elapsed_s": elapsed, "external_processes": rows,
                "busy": [record for record in rows if record["cpu_percent"] >= QUIET_LIMIT_PERCENT],
                "cpu_threshold_percent": QUIET_LIMIT_PERCENT, "sampling_interval_s": 0.25,
                "limit": "processes entirely between observations may be missed"}



def acquire_quiet_window(output: Path, *, clock=None, sleep=None, observer_factory=None) -> dict:
    """Acquire the first original quiet window before any timed sample, or fail closed."""
    clock = clock or time.monotonic
    sleep = sleep or time.sleep
    observer_factory = observer_factory or ProcessObservation
    started = clock()
    deadline = started + QUIET_ACQUISITION_SECONDS
    acquisition = {"status": "acquiring", "setup_cap_s": QUIET_ACQUISITION_SECONDS,
                   "window_s": QUIET_WINDOW_SECONDS, "cpu_threshold_percent": QUIET_LIMIT_PERCENT,
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


def failed_timing_trace_inputs(root: Path, output: Path) -> tuple[dict, dict]:
    """Refuse diagnostics unless the original, complete gate failed only timing."""
    load = lambda name: json.loads((output / name).read_text())
    bindings, summary, rows = load("bindings.json"), load("summary.json"), load("samples.json")
    if (bindings["samples"] != SAMPLES or bindings["iterations"] != ITERATIONS
            or bindings["cpu"] != 0 or bindings["gomaxprocs"] != 1
            or bindings["max_paired_regression_percent"] != TIME_LIMIT_PERCENT):
        raise ValueError("diagnostic requires original eight-pair/128x/CPU0/GOMAXPROCS1 contract")
    for arm in ("baseline", "candidate"):
        if not re.fullmatch(r"[0-9a-f]{40}", bindings[arm]) or summary[arm] != bindings[arm]:
            raise ValueError("diagnostic source identities do not match the failed gate")
    authorities = {arm: record["authority"] for arm, record in bindings["source_authorities"].items()}
    before, after = load("runtime-inputs-before.json"), load("runtime-inputs-after.json")
    expected = qualify_runtime(evaluate(rows, authorities), before, after, quiet=summary["quiet"])
    if any(summary.get(key) != value for key, value in expected.items()):
        raise ValueError("retained summary differs from the complete original evidence")
    if (summary["accepted"] or summary["timing_disposition"] != "FAIL" or not summary["quiet"]
            or summary["gates"] != dict.fromkeys(summary["gates"], True) | {"timing": False}
            or summary["source_authorities"] != bindings["source_authorities"]):
        raise ValueError("diagnostic requires a quiet timing-only FAIL with activation/bytes/objects PASS")
    if (digest(root / HARNESS) != bindings["harness_sha256"]
            or digest(Path(__file__)) != bindings["runner_sha256"]
            or source_authority(root) != bindings["source_authorities"]["candidate"]):
        raise ValueError("current fixture, runner or candidate authority drift")
    observation = load("measured-processes.json")
    if observation["busy"] or observation["cpu_threshold_percent"] != QUIET_LIMIT_PERCENT:
        raise ValueError("original measured process observation is not quiet")
    commands = load("commands.json")
    argv = ["taskset", "-c", "0", "<measured-binary>", "-test.run=^$",
            "-test.bench=^" + BENCHMARK + "$", "-test.benchtime=128x", "-test.count=1",
            "-test.benchmem", "-test.timeout=90s"]
    for arm in ("baseline", "candidate"):
        binary, fixture = bindings["binaries"][arm], bindings["arm_harnesses"][arm]
        if (binary["path"] != arm + "-primary.test" or binary["source_sha"] != bindings[arm]
                or fixture["path"] != arm + "-durable_replacement_cost_test.go"
                or digest(output / binary["path"]) != binary["sha256"]
                or (output / binary["path"]).stat().st_size != binary["bytes"]
                or digest(output / fixture["path"]) != fixture["sha256"]
                or before[arm]["executable"] != {"sha256": binary["sha256"], "bytes": binary["bytes"]}
                or before[arm]["inputs"]["harness_sha256"] != fixture["sha256"]
                or before[arm]["inputs"]["runner_sha256"] != bindings["runner_sha256"]
                or before[arm]["inputs"]["cwd"] != str(root)
                or before[arm]["inputs"]["argv"] != argv):
            raise ValueError("retained binary/fixture/runtime input mismatch")
        label = arm + "-public-lifecycle"
        matches = [record for record in commands if record["label"] == label]
        text = (output / (label + ".txt")).read_text()
        parse_authority(text, authorities[arm])
        if (len(matches) != 1 or matches[0]["exit"] != 0
                or matches[0]["output_sha256"] != digest(output / (label + ".txt"))
                or f"--- PASS: {LIFECYCLE} " not in text or "--- SKIP:" in text
                or f"MVCC_COST_LIFECYCLE authority={authorities[arm]}" not in text
                or (authorities[arm] == "DATA" and text.count("MVCC_COST_DATA_SLOT ") != 6)):
            raise ValueError("original full public lifecycle evidence is incomplete")
    for row in rows:
        label = f"pair-{row['pair']+1}-{row['revision']}"
        matches = [record for record in commands if record["label"] == label]
        text = (output / (label + ".txt")).read_text()
        if (len(matches) != 1 or matches[0]["exit"] != 0
                or matches[0]["output_sha256"] != digest(output / (label + ".txt"))
                or parse_sample(text) != row["metrics"]
                or parse_authority(text, authorities[row["revision"]]) != row["authority"]):
            raise ValueError("original sample receipts are incomplete or changed")
    return bindings, before


def trace_failed_timing(root: Path, output: Path) -> int:
    """One post-failure syscall diagnostic, never a gate retry or acceptance input."""
    directory = output / "sync-trace"
    directory.mkdir()  # Never overwrite or repeat an existing diagnostic attempt.
    deadline = time.monotonic() + 180
    environment = dict(os.environ, GOWORK="off", GOMAXPROCS="1", TMPDIR=str(output / "tmp"))
    result = {"status": "DIAGNOSTIC_NOT_APPLICABLE", "arms": {"baseline": "UNRUN", "candidate": "UNRUN"},
              "scope": "whole process including calibration/setup/Close; not exact timed-loop attribution",
              "acceptance": "original strict result unchanged; diagnostic timing is not acceptance",
              "commands": [], "environment_sha256": hashlib.sha256(
                  json.dumps(environment, sort_keys=True).encode()).hexdigest()}

    def save():
        (directory / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")

    def command(label, argv, timeout=90, check=True):
        started = time.monotonic()
        path = directory / (label + ".txt")
        with path.open("wb") as stream:
            if time.monotonic() >= deadline:
                raise RuntimeError("post-failure diagnostic 180s budget exhausted; no next process launched")
            process = subprocess.Popen([str(x) for x in argv], cwd=root, env=environment,
                stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                status = process.wait(timeout=max(0.001, min(timeout, deadline - time.monotonic())))
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
                raise
            finally:
                result["commands"].append({"label": label, "argv": [str(x) for x in argv],
                    "exit": process.returncode, "child_reaped": True, "elapsed_s": time.monotonic() - started,
                    "output_sha256": digest(path)})
                save()
        if check and status:
            raise RuntimeError(f"{label} failed with exit {status}")
        return path.read_text()

    try:
        bindings, original_runtime = failed_timing_trace_inputs(root, output)
        result.update(status="DIAGNOSTIC_STOP", baseline=bindings["baseline"], candidate=bindings["candidate"],
                      bindings_sha256=digest(output / "bindings.json"),
                      original_evidence_sha256={name: digest(output / name) for name in (
                          "summary.json", "samples.json", "commands.json", "measured-processes.json",
                          "runtime-inputs-before.json", "runtime-inputs-after.json")},
                      original_environment_sha256={arm: original_runtime[arm]["inputs"]["environment_sha256"]
                                                   for arm in ("baseline", "candidate")},
                      runtime_knobs={key: environment.get(key) for key in (
                          "GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GOWORK", "TMPDIR", "LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT")},
                      environment_scope="new CI step; full environment hash may differ from strict step; ELF/loader must match")
        if command("source-head", ["git", "rev-parse", "HEAD"]).strip() != bindings["candidate"]:
            raise ValueError("candidate checkout differs from measured source")
        command("source-clean", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, bindings["candidate"]])
        # Installation is AFTER all strict samples and cannot influence their result.
        strace = shutil.which("strace")
        if not strace:
            try:
                command("strace-setup", ["timeout", "--kill-after=5s", "120s", "bash", "-c",
                    "sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends strace || "
                    "{ sudo -n apt-get update && sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends strace; }"],
                    timeout=125, check=False)
            except (OSError, subprocess.SubprocessError) as error:
                result.update(status="DIAGNOSTIC_UNAVAILABLE", error=f"bounded strace setup failed: {error}")
                return 0
            strace = shutil.which("strace")
        if not strace:
            result.update(status="DIAGNOSTIC_UNAVAILABLE", error="strace unavailable after bounded setup")
            return 0
        result["strace"] = file_identity(Path(strace))
        command("strace-version", [strace, "--version"], timeout=5)
        for arm in ("baseline", "candidate"):
            binary = output / bindings["binaries"][arm]["path"]
            current = runtime_binding(binary, command, arm + "-runtime-before", environment)
            if current != {key: value for key, value in original_runtime[arm].items() if key != "inputs"}:
                raise ValueError("ELF/loader runtime drift since the strict samples")
            trace = directory / (arm + ".strace")
            result["arms"][arm] = "RUNNING"
            save()
            text = command(arm + "-traced-128", ["taskset", "-c", "0", strace, "-f", "-ttt", "-T", "-yy", "-s", "96",
                "-e", "trace=fsync,fdatasync,pwritev2,pwrite64,msync,openat,close,ftruncate", "-o", trace,
                binary] + original_runtime[arm]["inputs"]["argv"][4:])
            metrics = parse_sample(text)
            parse_authority(text, bindings["source_authorities"][arm]["authority"])
            if runtime_binding(binary, command, arm + "-runtime-after", environment) != current:
                raise ValueError("ELF/loader runtime drift during trace")
            counts = {}
            for line in trace.read_text().splitlines():
                match = re.search(r"(?:\b(fsync|fdatasync|pwritev2|pwrite64|msync|openat|close|ftruncate)\(|<\.\.\. (\w+) resumed>).* = (-?\d+).* <([\d.]+)>$", line)
                if match:
                    key = match[1] or match[2]
                    record = counts.setdefault(key, {"completed": 0, "seconds": 0.0, "errors": 0})
                    record["completed"] += 1
                    record["seconds"] += float(match[4])
                    record["errors"] += int(match[3]) < 0
            result["arms"][arm] = {"status": "COMPLETE", "metrics": metrics, "trace_sha256": digest(trace),
                "syscalls": counts, "unfinished_lines": trace.read_text().count("<unfinished ...>"),
                "fd_paths": "retained verbatim by strace -yy; split calls may lack a path on their resumed line"}
        command("source-clean-after", ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, bindings["candidate"]])
        if any(digest(output / name) != sha for name, sha in result["original_evidence_sha256"].items()):
            raise ValueError("original strict evidence changed during diagnostics")
        result["status"] = "DIAGNOSTIC_COMPLETE"
        return 0
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        result["error"] = str(error)
        for arm, state in result["arms"].items():
            if state == "RUNNING":
                trace = directory / (arm + ".strace")
                result["arms"][arm] = {"status": "FAILED", "trace_sha256": digest(trace) if trace.exists() else None}
        return 1 if result["status"] == "DIAGNOSTIC_STOP" else 0
    finally:
        save()


def run_gate(root: Path, output: Path, baseline: str, candidate: str) -> int:
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
        template = output / "durable_replacement_cost_test.go"
        template.write_bytes((root / HARNESS).read_bytes())
        source_bindings = {}
        harnesses = {}
        binaries = {}
        for revision, source in (("baseline", base_root), ("candidate", root)):
            source_bindings[revision] = source_authority(source)
            harness = output / (revision + "-durable_replacement_cost_test.go")
            harness.write_text(render_harness(template.read_text(), source_bindings[revision]["authority"]))
            harnesses[revision] = {"path": harness.name, "sha256": digest(harness)}
            overlay = output / (revision + "-overlay.json")
            save(overlay.name, {"Replace": {str(source / HARNESS): str(harness)}})
            binary = output / (revision + "-primary.test")
            argv = ["go", "test", "-c", "-p=1", "-tags=treedb_test", "-trimpath", "-buildvcs=false", "-overlay", overlay]
            command("build-" + revision, argv + ["-o", binary, "./TreeDB/mvcc"], source, timeout=600)
            binaries[revision] = {"sha256": digest(binary), "bytes": binary.stat().st_size,
                                  "source_sha": baseline if revision == "baseline" else candidate,
                                  "path": binary.name}
        save("bindings.json", {"baseline": baseline, "candidate": candidate,
             "harness_sha256": digest(template), "arm_harnesses": harnesses,
             "source_authorities": source_bindings, "runner_sha256": digest(Path(__file__)),
             "samples": SAMPLES, "iterations": ITERATIONS, "cpu": 0, "gomaxprocs": 1,
             "max_paired_regression_percent": TIME_LIMIT_PERCENT,
             "quiet_preflight": {"window_s": QUIET_WINDOW_SECONDS, "setup_cap_s": QUIET_ACQUISITION_SECONDS,
                                 "cpu_threshold_percent": QUIET_LIMIT_PERCENT},
             "bytes_and_allocations": "no median growth", "binaries": binaries})
        benchmark_argv = ["-test.run=^$", "-test.bench=^" + BENCHMARK + "$",
                          "-test.benchtime=128x", "-test.count=1", "-test.benchmem", "-test.timeout=90s"]

        def bind_inputs(phase):
            bindings = {}
            for revision, record in binaries.items():
                binding = runtime_binding(output / record["path"], command, phase + "-" + revision, environment)
                binding["inputs"] = {
                    "harness_sha256": digest(output / harnesses[revision]["path"]), "runner_sha256": digest(Path(__file__)),
                    "environment_sha256": hashlib.sha256(json.dumps(environment, sort_keys=True).encode()).hexdigest(),
                    "cwd": str(root), "argv": ["taskset", "-c", "0", "<measured-binary>"] + benchmark_argv,
                }
                bindings[revision] = binding
            save("runtime-inputs-" + phase + ".json", bindings)
            return bindings

        inputs_before = bind_inputs("before")
        authorities = {revision: record["authority"] for revision, record in source_bindings.items()}
        for revision, record in binaries.items():
            lifecycle = command(revision + "-public-lifecycle", ["taskset", "-c", "0", output / record["path"],
                    "-test.run=^" + LIFECYCLE + "$", "-test.count=1", "-test.timeout=90s", "-test.v"])
            parse_authority(lifecycle, authorities[revision])
            if (f"--- PASS: {LIFECYCLE} " not in lifecycle or "--- SKIP:" in lifecycle
                    or f"MVCC_COST_LIFECYCLE authority={authorities[revision]}" not in lifecycle
                    or (authorities[revision] == "DATA" and lifecycle.count("MVCC_COST_DATA_SLOT ") != 6)):
                raise RuntimeError(f"actual {revision} public lifecycle control did not pass")
        acquire_quiet_window(output)
        observation = ProcessObservation(output / "measured-processes.jsonl")
        for pair in range(SAMPLES):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for revision in order:
                text = command(f"pair-{pair+1}-{revision}", ["taskset", "-c", "0",
                     output / binaries[revision]["path"]] + benchmark_argv)
                rows.append({"pair": pair, "revision": revision, "metrics": parse_sample(text),
                             "authority": parse_authority(text, authorities[revision])})
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
        if digest(root / HARNESS) != digest(template):
            raise RuntimeError("benchmark input drift")
        for revision, source in (("baseline", base_root), ("candidate", root)):
            if (source_authority(source) != source_bindings[revision]
                    or digest(output / harnesses[revision]["path"]) != harnesses[revision]["sha256"]):
                raise RuntimeError("source authority or arm fixture drift")
        inputs_after = bind_inputs("after")
        result = qualify_runtime(evaluate(rows, authorities), inputs_before, inputs_after, quiet=not busy)
        result.update(baseline=baseline, candidate=candidate, source_authorities=source_bindings,
                      generation_advances="raw telemetry; DATA advancement is not a PRIMARY activation failure")
        save("summary.json", result)
        (output / "summary.md").write_text(
            "## Public durable replacement cost\n\n"
            f"- Verdict: {'PASS' if result['accepted'] else 'FAIL'}\n"
            f"- Exact base/head: `{baseline}` / `{candidate}`\n"
            f"- Source-bound public authority: `{authorities['baseline']}` / `{authorities['candidate']}`; both lifecycle controls passed.\n"
            "- Eight alternating AB/BA pairs; 128 durable public replacements per sample.\n"
            f"- Median paired time delta: {result['median_paired_delta_percent']:+.3f}% (limit +5%).\n"
            f"- Raw gates: `{json.dumps(result['gates'], sort_keys=True)}`; measurement pass: {result['measurement_pass']}; quiet: {not busy}.\n"
            f"- Timing acceptance: `{result['timing_disposition']}`; bytes acceptance: `{result['bytes_disposition']}`; bound runtime/input equivalent: {result['runtime_input_equivalent']}.\n"
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
    args = parser.parse_args()
    return run_gate(args.root.resolve(), args.output.resolve(), args.baseline, args.candidate)


if __name__ == "__main__":
    sys.exit(main())
