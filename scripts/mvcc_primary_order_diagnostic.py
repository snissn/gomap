#!/usr/bin/env python3
"""Run full-length public PRIMARY controls on one hosted runner. No acceptance verdict."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import mvcc_primary_cost_gate as gate


# Reverse complete block order on the second runner. Each protocol/control gets
# one full AB start and one full BA start per runner. No sample is discarded.
BLOCKS = (
    ("unconditioned", "identical", "baseline"),
    ("preconditioned", "slow", "candidate"),
    ("preconditioned", "identical", "candidate"),
    ("unconditioned", "slow", "baseline"),
    ("unconditioned", "slow", "candidate"),
    ("preconditioned", "identical", "baseline"),
    ("preconditioned", "slow", "baseline"),
    ("unconditioned", "identical", "candidate"),
)
SLOW_CPU_NS = 1_000_000
CLOCK = re.compile(r"(?m)^PRIMARY_CLOCK n=(\d+) cpu_ns=(\d+) injected_ns=(\d+)$")
TRACE_ROW = re.compile(r"^\s*(\d+)\s+(\d+\.\d+)\s+(.*)$")
TRACE_MARKER = re.compile(r'write\(2(?:<[^>]+>)?, "PRIMARY_(START|STOP) n=(\d+)')
TRACE_SYNC = re.compile(r"^(fsync|fdatasync|msync)\((.*)")
TRACE_RESUME = re.compile(r"^<\.\.\. (fsync|fdatasync|msync) resumed>")
TRACE_FD = re.compile(r"^\d+<([^>]+)>")
TRACE_RETURN = re.compile(r" = (-?\d+)(?:[ ].*)? <(\d+\.\d+)>$")


def replace_once(text: str, original: str, replacement: str) -> str:
    if text.count(original) != 1:
        raise ValueError("maintained harness anchor changed: " + original)
    return text.replace(original, replacement)


def instrument(text: str) -> str:
    text = replace_once(text, '\t"fmt"\n', '\t"fmt"\n\t"os"\n')
    text = replace_once(text, '\t"testing"\n', '\t"testing"\n\n\t"golang.org/x/sys/unix"\n')
    text = replace_once(text, "\tb.ReportAllocs()\n\tb.ResetTimer()\n", """
	dose := int64(0)
	if os.Getenv("GOMAP_PRIMARY_DIAGNOSTIC_SLOW") == "1" {
		dose = %d
	}
	var injected int64
	b.ReportAllocs()
	fmt.Fprintf(os.Stderr, "PRIMARY_START n=%%d\\n", b.N)
	cpuStart := primaryDiagnosticCPU(b)
	b.ResetTimer()
""" % SLOW_CPU_NS)
    text = replace_once(text, "\t}\n\tb.StopTimer()\n\tafter := generation()\n", """
		if dose != 0 {
			start := primaryDiagnosticCPU(b)
			elapsed := int64(0)
			for elapsed < dose {
				elapsed = primaryDiagnosticCPU(b) - start
			}
			injected += elapsed
		}
	}
	b.StopTimer()
	cpuEnd := primaryDiagnosticCPU(b)
	fmt.Fprintf(os.Stderr, "PRIMARY_STOP n=%d\\nPRIMARY_CLOCK n=%d cpu_ns=%d injected_ns=%d\\n", b.N, b.N, cpuEnd-cpuStart, injected)
	after := generation()
""")
    return text + """
// Account actual process CPU with the same CPU0 and GOMAXPROCS=1 as the gate.
func primaryDiagnosticCPU(b *testing.B) int64 {
	var now unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &now); err != nil {
		b.Fatal(err)
	}
	return now.Nano()
}
"""


def sample(text: str, *, slow: bool) -> dict:
    rows = [(int(n), int(cpu), int(injected)) for n, cpu, injected in CLOCK.findall(text)]
    if [n for n, _, _ in rows] != [1, gate.ITERATIONS]:
        raise ValueError("missing or repeated PRIMARY CPU measurements")
    for n, cpu, injected in rows:
        if not 0 <= injected <= cpu or (slow and injected < SLOW_CPU_NS * n) or (not slow and injected != 0):
            raise ValueError("wrong actual CPU control dose")
    metrics = gate.parse_sample(text)
    if metrics["generation_advances/op"] != 0:
        raise ValueError("fixture left PRIMARY replacement route")
    n, cpu, injected = rows[-1]
    return {"metrics": metrics, "cpu_ns_per_op": cpu / n, "injected_cpu_ns_per_op": injected / n}


def sync_trace(path: Path) -> dict:
    active = False
    complete = False
    pending = {}
    seen = []
    for line in path.read_text().splitlines():
        match = TRACE_ROW.match(line)
        if not match:
            continue
        pid, _, body = match.groups()
        if marker := TRACE_MARKER.match(body):
            if int(marker[2]) != gate.ITERATIONS:
                continue
            if marker[1] == "START" and not active and not complete:
                active = True
            elif marker[1] == "STOP" and active and not pending:
                active = False
                complete = True
            else:
                raise ValueError("malformed PRIMARY trace boundary")
            continue
        if not active:
            continue
        if call := TRACE_SYNC.match(body):
            fd = TRACE_FD.match(call[2])
            operation, name = call[1], Path(fd[1]).name if fd else "<unresolved>"
            if body.endswith(" <unfinished ...>"):
                pending[pid] = (operation, name)
                continue
        elif resumed := TRACE_RESUME.match(body):
            operation, name = pending.pop(pid)
            if resumed[1] != operation:
                raise ValueError("mismatched resumed sync syscall")
        else:
            continue
        status = TRACE_RETURN.search(body)
        if not status or status[1] != "0":
            raise ValueError("sync failed or lacked a completed trace result")
        seen.append((operation, name, float(status[2])))
    if not seen or not complete:
        raise ValueError("missing PRIMARY trace boundary or sync calls")
    counts = Counter(f"{syscall} {name}" for syscall, name, _ in seen)
    primary_syncs = sum(counts[key] for key in ("fsync index.db.primary", "fdatasync index.db.primary"))
    if primary_syncs != gate.ITERATIONS:
        raise ValueError("expected one real index.db.primary sync per durable replacement")
    return {"calls": dict(counts),
            "sum_syscall_ns_per_op": sum(duration for _, _, duration in seen) * 1e9 / gate.ITERATIONS}


def run(root: Path, output: Path, slot: int, head: str) -> None:
    if root in output.parents or output == root:
        raise ValueError("diagnostic output must be outside the source checkout")
    output.mkdir(parents=True)
    temporary = output / "tmp"
    temporary.mkdir()
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="1", TMPDIR=str(temporary),
               GOMAP_PRIMARY_DIAGNOSTIC_SLOW="0")

    def save(path, data):
        path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")

    def command(label, argv, *, slow=False, observer=None, timeout=90):
        argv = [str(arg) for arg in argv]
        log = output / (label + ".txt")
        errors = output / (label + ".stderr")
        started = time.monotonic()
        with log.open("xb") as stream, errors.open("xb") as error_stream:
            proc = subprocess.Popen(argv, cwd=root, env=dict(env, GOMAP_PRIMARY_DIAGNOSTIC_SLOW=str(int(slow))),
                                    stdout=stream, stderr=error_stream, start_new_session=True)
            try:
                while True:
                    remaining = timeout - (time.monotonic() - started)
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, timeout)
                    try:
                        status = proc.wait(timeout=min(0.25, remaining))
                        break
                    except subprocess.TimeoutExpired:
                        if observer is not None:
                            observer.observe()
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                with (output / "commands.jsonl").open("a") as ledger:
                    ledger.write(json.dumps({"label": label, "argv": argv, "slow": slow,
                                             "exit": proc.returncode, "elapsed_s": time.monotonic() - started,
                                             "stdout_sha256": gate.digest(log), "stderr_sha256": gate.digest(errors)}) + "\n")
        if status:
            raise RuntimeError(f"{label} failed with exit {status}")
        return log.read_text()

    def measure(label, argv, *, slow=False, observer=None):
        stdout = command(label, argv, slow=slow, observer=observer)
        stderr = (output / (label + ".stderr")).read_text()
        return sample(stdout + "\n" + stderr, slow=slow)

    guard = ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, head]
    command("checkout-before", guard)
    tree = command("checkout-tree", ["git", "rev-parse", "HEAD^{tree}"]).strip()
    command("go", ["go", "version"])
    command("go-env", ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS"])
    command("kernel", ["uname", "-a"])
    command("cpu", ["lscpu"])
    command("storage", ["findmnt", "-T", temporary])
    command("kernel-tracer", ["strace", "-V"])

    blocks = BLOCKS if slot == 1 else BLOCKS[::-1]
    fixture = root / gate.HARNESS
    overlay_fixture = output / fixture.name
    overlay_fixture.write_text(instrument(fixture.read_text()))
    overlay = output / "overlay.json"
    save(overlay, {"Replace": {str(fixture): str(overlay_fixture)}})
    binary = output / "primary-control.test"
    command("build", ["go", "test", "-c", "-p=1", "-tags=treedb_test", "-trimpath",
                      "-buildvcs=false", "-overlay", overlay, "-o", binary, "./TreeDB/mvcc"], timeout=600)
    bench = ["taskset", "-c", "0", binary, "-test.run=^$", f"-test.bench=^{gate.BENCHMARK}$",
             f"-test.benchtime={gate.ITERATIONS}x", "-test.count=1", "-test.benchmem", "-test.timeout=90s"]
    source_paths = (fixture, overlay_fixture, Path(__file__), Path(gate.__file__))
    source_digests = {str(path): gate.digest(path) for path in source_paths}
    runtime = gate.runtime_binding(binary, command, "before", env)
    runner = {key: env.get(key) for key in ("ImageOS", "ImageVersion", "RUNNER_ARCH", "RUNNER_ENVIRONMENT",
                                            "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")}
    save(output / "plan.json", {"head": head, "tree": tree, "runner": runner,
                               "slot": slot, "blocks": blocks, "argv": [str(arg) for arg in bench],
                               "iterations": gate.ITERATIONS, "pairs": gate.SAMPLES, "cpu": 0, "gomaxprocs": 1,
                               "synthetic_process_cpu_ns_per_op": SLOW_CPU_NS,
                               "time_limit_percent": gate.TIME_LIMIT_PERCENT,
                               "input_sha256": source_digests, "runtime": runtime, "enforcing": False})
    lifecycle = command("lifecycle", ["taskset", "-c", "0", binary, f"-test.run=^{gate.LIFECYCLE}$",
                                      "-test.count=1", "-test.timeout=90s", "-test.v"])
    if f"--- PASS: {gate.LIFECYCLE} " not in lifecycle or "--- SKIP:" in lifecycle:
        raise ValueError("public lifecycle control did not complete")

    results = []
    for index, (protocol, control, first) in enumerate(blocks, 1):
        cell = output / f"block-{index}"
        cell.mkdir()
        gate.acquire_quiet_window(cell)
        observation = gate.ProcessObservation(cell / "measured-processes.jsonl")
        order = gate.sample_order(first)
        rows = []
        try:
            if protocol == "preconditioned":
                for _, revision in order[:2]:
                    measure(f"block-{index}/warm-{revision}", bench, observer=observation)
            for pair, revision in order:
                slow = control == "slow" and revision == "candidate"
                metrics = measure(f"block-{index}/pair-{pair + 1}-{revision}", bench, slow=slow, observer=observation)
                rows.append({"pair": pair, "revision": revision, **metrics})
                save(cell / "samples.json", rows)
        finally:
            host = observation.finish()
            save(cell / "measured-processes.json", host)
        result = gate.evaluate(rows, first_revision=first)
        results.append({"protocol": protocol, "control": control, "first": first, "quiet": not host["busy"], **result})
        save(output / "blocks.json", results)

    # Separate traced processes. Their timing never enters a comparison.
    traces = {}
    for mode in ("identical", "slow"):
        trace_path = output / (mode + ".strace")
        slow = mode == "slow"
        metrics = measure("trace-" + mode,
                          ["strace", "-f", "-yy", "-ttt", "-T", "-s", "64", "-o", trace_path,
                           "-e", "trace=fsync,fdatasync,msync,write", *bench], slow=slow)
        traces[mode] = {**metrics, **sync_trace(trace_path)}
    command("checkout-after", guard)
    if gate.runtime_binding(binary, command, "after", env) != runtime or any(
            gate.digest(path) != source_digests[str(path)] for path in source_paths):
        raise ValueError("measured runtime or source changed")
    save(output / "summary.json", {"blocks": results, "separate_traces": traces, "enforcing": False})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--slot", type=int, choices=(1, 2), required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.head):
        parser.error("require exact full checkout SHA")
    run(args.root.resolve(), args.output.resolve(), args.slot, args.head)
