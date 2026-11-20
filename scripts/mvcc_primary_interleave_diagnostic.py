#!/usr/bin/env python3
"""Measure paired 16-operation PRIMARY interleaving without changing its gate."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time

import mvcc_primary_cost_gate as gate
import mvcc_primary_order_diagnostic as prior

CHUNKS = 8
CHUNK = 16
EXTRA_STRIDE = 4
SCHEDULES = ("original", "resident-serial", "interleaved")
BLOCKS = tuple((schedule, control, first) for schedule in SCHEDULES
               for control, first in (("null", "baseline"), ("io", "candidate"),
                                      ("null", "candidate"), ("io", "baseline")))
ROLE = ("baseline", "candidate")
STATUS_FD = "GOMAP_PRIMARY_INTERLEAVE_STATUS_FD"
DOSE = "GOMAP_PRIMARY_INTERLEAVE_IO"
WINDOW_START = re.compile(r'PRIMARY_WINDOW_START part=(\d+) dir=([^\n]+)')
WINDOW_STOP = re.compile(r'PRIMARY_WINDOW_STOP part=(\d+) total_ns=(\d+)')
READBACK = re.compile(r'(?m)^PRIMARY_READBACK normal=(\d+) extra=(\d+) value=(\w+)$')
FILE_WRITE = re.compile(r'^(?:write|writev|pwrite64|pwritev|pwritev2|ftruncate|fallocate|sync_file_range)\(\d+<([^>]+)>')
PRIMARY_SYNC = re.compile(r'^(fsync|fdatasync)\(\d+<([^>]+)>')
SYNC_RESUME = re.compile(r'^<\.\.\. (fsync|fdatasync) resumed>')
TRACE_FILTER = ("execve,write,writev,pwrite64,pwritev,pwritev2,fsync,fdatasync,msync,"
                "ftruncate,fallocate,sync_file_range,rename,renameat,renameat2,unlink,unlinkat,mkdir,mkdirat,rmdir")
METADATA_WRITE = re.compile(r'^(?:rename|renameat|renameat2|unlink|unlinkat|mkdir|mkdirat|rmdir)\(')


def instrument(src: str, helpers: str) -> str:
    anchor = f"func {gate.BENCHMARK}("
    before, bench = src.split(anchor)
    before = prior.replace_once(before, '\t"fmt"\n', '\t"fmt"\n\t"os"\n')
    bench = prior.replace_once(bench, "if err := d.Close(); err != nil {\n\t\t\tb.Fatal(err)\n\t\t}",
                               "if d != nil {\n\t\t\tif err := d.Close(); err != nil {\n\t\t\t\tb.Fatal(err)\n\t\t\t}\n\t\t}")
    start = "\tb.ReportAllocs()\n\tb.ResetTimer()"
    stop = "\tb.StopTimer()\n\tafter := generation()"
    original = bench[bench.index(start):bench.index(stop) + len(stop)]
    if original.count("st.CommitAt(100, mutation, CommitDurable)") != 1:
        raise ValueError("maintained public benchmark changed")
    bench = prior.replace_once(bench, original, """	b.ReportAllocs()
	injected := primaryInterleave(b, st, opts, mutation)
	after := generation()""")
    report = '\tb.ReportMetric(float64(after-before)/float64(b.N), "generation_advances/op")'
    bench = prior.replace_once(bench, report, report + """
	if b.N == 128 {
		err := d.Close()
		d = nil
		if err != nil {
			b.Fatal(err)
		}
		primaryInterleaveReadback(b, opts, injected)
	}""")
    return before + anchor + bench + "\n" + helpers


def rotate(slot: int):
    # Cross the run order on the fixed offered slots while preserving every factor.
    start = (slot - 1) * 3
    order = BLOCKS[start:] + BLOCKS[:start]
    return order if slot % 2 else order[::-1]


def roles(first: str):
    return (first, next(role for role in ROLE if role != first))


def grants(schedule: str, first: str) -> list[tuple[int, str]]:
    initial = roles(first)
    if schedule == "resident-serial":
        return [(part, role) for role in initial for part in range(CHUNKS)]
    if schedule == "interleaved":
        return gate.sample_order(first)
    raise ValueError("grant plan is resident only")


def check_sample(stdout: str, stderr: str, schedule: str, positive: bool, progress: list) -> dict:
    metrics = gate.parse_sample(stdout)
    expected = 1 if schedule == "original" else CHUNKS
    starts = [(int(part), json.loads(path)) for part, path in WINDOW_START.findall(stderr)]
    stops = [(int(part), int(ns)) for part, ns in WINDOW_STOP.findall(stderr)]
    if [part for part, _ in starts] != list(range(expected)) or [part for part, _ in stops] != list(range(expected)):
        raise ValueError("invalid measured window inventory")
    if max(path != starts[0][1] for _, path in starts) or any(
            new[1] <= old[1] for old, new in zip([(-1, 0)] + stops, stops)):
        raise ValueError("measured windows drifted or failed to advance time")
    if progress and progress != stops:
        raise ValueError("dispatcher and Go timing receipts disagree")
    # The formatted Go metric can round. The dispatcher never adds wait time.
    if abs(metrics["ns/op"] - stops[-1][1] / gate.ITERATIONS) > 1.0:
        raise ValueError("Go wall metric differs from the sum of measured windows")
    extra = gate.ITERATIONS // EXTRA_STRIDE if positive else 0
    if (READBACK.findall(stderr) != [(str(gate.ITERATIONS), str(extra), "again" if positive else "after")]
            or metrics["generation_advances/op"] != 0):
        raise ValueError("durable readback, injected work, or PRIMARY route failed")
    return {"metrics": metrics, "store": starts[0][1], "windows": stops, "extra": extra}


def save(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


class Child:
    def __init__(self, stack, command, env, output, *, resident, positive):
        self.output = output
        self.progress = []
        self.buffer = b""
        self.record = {"argv": command, "resident": resident, "extra_io": positive}
        self.reader = None
        writer = None
        pass_fds = ()
        child_env = dict(env, **{DOSE: str(int(positive))})
        child_env.pop(STATUS_FD, None)
        try:
            if resident:
                self.reader, writer = os.pipe()
                stack.callback(os.close, self.reader)
                pass_fds = (writer,)
                child_env[STATUS_FD] = str(writer)
                self.record["status_descriptor"] = writer
            with output.with_suffix(".stdout").open("xb") as stdout, output.with_suffix(".stderr").open("xb") as stderr:
                self.proc = subprocess.Popen(command, env=child_env, stdin=subprocess.PIPE if resident else None,
                                             stdout=stdout, stderr=stderr, pass_fds=pass_fds, start_new_session=True)
            stack.callback(self.close)
        finally:
            if writer is not None:
                os.close(writer)

    def receive(self, deadline, observer):
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"child {self.output.name} did not report progress")
            ready, _, _ = select.select((self.reader,), (), (), min(0.25, remaining))
            if ready:
                data = os.read(self.reader, 1024)
                if not data:
                    raise RuntimeError(f"child {self.output.name} closed its grant channel")
                self.buffer += data
                if len(self.buffer) > 2048:
                    raise ValueError("oversize child message")
            elif observer is not None:
                observer.observe()
        line, self.buffer = self.buffer.split(b"\n", 1)
        return line.decode()

    def send(self, byte):
        self.proc.stdin.write(bytes((byte,)))
        self.proc.stdin.flush()

    def join(self, deadline, observer):
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"child {self.output.name} did not exit")
            try:
                result = self.proc.wait(timeout=min(0.25, remaining))
                if result:
                    raise RuntimeError(f"child {self.output.name} failed with status {result}")
                return
            except subprocess.TimeoutExpired:
                if observer is not None:
                    observer.observe()

    def close(self):
        if self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait()
        if self.proc.stdin is not None:
            self.proc.stdin.close()
        save(self.output.with_suffix(".command.json"),
             {**self.record, "exit": self.proc.returncode,
              "stdout_sha256": gate.digest(self.output.with_suffix(".stdout")),
              "stderr_sha256": gate.digest(self.output.with_suffix(".stderr"))})


def run_pair(cell: Path, schedule: str, first: str, control: str, argv: list, env: dict, *,
             observer=None, trace=False) -> dict:
    cell.mkdir()
    resident = schedule != "original"
    deadline = time.monotonic() + 90
    children = {}
    events = []
    with ExitStack() as stack:
        for role in roles(first):
            command = argv
            if trace:
                command = ["strace", "-f", "-yy", "-ttt", "-T", "-s", "384", "-o", str(cell / (role + ".strace")),
                           "-e", "trace=" + TRACE_FILTER, *argv]
            child = Child(stack, command, env, cell / role, resident=resident,
                          positive=control == "io" and role == "candidate")
            children[role] = child
            if resident:
                if child.receive(deadline, observer) != "READY":
                    raise ValueError("child did not announce checked seed before measuring")
                events.append({"role": role, "ready_ns": time.monotonic_ns()})
            else:
                child.join(deadline, observer)
        if resident:
            for part, role in grants(schedule, first):
                child = children[role]
                granted = time.monotonic_ns()
                child.send(part)
                message = child.receive(deadline, observer)
                prefix, ordinal, duration = message.split()
                if prefix != "DONE" or int(ordinal) != part:
                    raise ValueError(f"unexpected grant receipt: {message}")
                child.progress.append((part, int(duration)))
                events.append({"role": role, "grant": part, "grant_ns": granted,
                               "done_ns": time.monotonic_ns(), "benchmark_total_ns": int(duration)})
                save(cell / "grants.json", events)
            for child in children.values():
                child.send(255)
            for child in children.values():
                child.join(deadline, observer)
    rows = {role: check_sample(child.output.with_suffix(".stdout").read_text(),
                               child.output.with_suffix(".stderr").read_text(), schedule,
                               control == "io" and role == "candidate", child.progress)
            for role, child in children.items()}
    if len({row["store"] for row in rows.values()}) != 2:
        raise ValueError("the two measured stores were not independently created")
    if trace:
        rows["physical"] = check_traces(cell, rows, schedule, first)
    save(cell / "result.json", rows)
    return rows


def trace_events(path: Path, store: str) -> dict:
    spans = []
    calls = []
    writes = []
    in_flight = {}
    for line in path.read_text().splitlines():
        row = prior.TRACE_ROW.match(line)
        if not row:
            continue
        thread, timestamp, body = row.groups()
        timestamp = int(timestamp.replace(".", ""))
        if body.startswith("write(2<"):
            start = WINDOW_START.search(body)
            stop = WINDOW_STOP.search(body)
            if start:
                if int(start[1]) != len(spans) or spans and len(spans[-1]) != 2:
                    raise ValueError("malformed traced measurement start")
                spans.append([timestamp])
            elif stop:
                if int(stop[1]) != len(spans) - 1 or len(spans[-1]) != 1:
                    raise ValueError("malformed traced measurement stop")
                spans[-1].append(timestamp)
        if write := FILE_WRITE.match(body):
            if write[1].startswith(store + "/"):
                writes.append((timestamp, write[1]))
        if METADATA_WRITE.match(body) and store + "/" in body or body.startswith("msync("):
            writes.append((timestamp, body))
        if sync := PRIMARY_SYNC.match(body):
            operation, file = sync.groups()
            if not file.startswith(store + "/"):
                continue
            if body.endswith(" <unfinished ...>"):
                in_flight[thread] = (timestamp, operation, file)
                continue
        elif resumed := SYNC_RESUME.match(body):
            if thread not in in_flight:
                continue
            timestamp, operation, file = in_flight.pop(thread)
            if resumed[1] != operation:
                raise ValueError("mismatched traced sync")
        else:
            continue
        status = prior.TRACE_RETURN.search(body)
        if not status or status[1] != "0":
            raise ValueError("store sync failed")
        calls.append((timestamp, file, round(float(status[2]) * 1_000_000)))
    if not spans or any(len(span) != 2 for span in spans) or in_flight:
        raise ValueError("incomplete timed trace")
    return {"spans": spans, "sync": calls, "write": writes}


def check_traces(cell: Path, rows: dict, schedule: str, first: str) -> dict:
    traces = {role: trace_events(cell / (role + ".strace"), rows[role]["store"]) for role in ROLE}
    observed = sorted((start, stop, part, role) for role, trace in traces.items()
                      for part, (start, stop) in enumerate(trace["spans"]))
    planned = [(0, role) for role in roles(first)] if schedule == "original" else grants(schedule, first)
    if [(part, role) for _, _, part, role in observed] != planned or any(
            earlier[1] > later[0] for earlier, later in zip(observed, observed[1:])):
        raise ValueError("physical trace does not match exclusive grant plan")
    result = {}
    for role, trace in traces.items():
        count = 0
        timed_sync_us = 0
        windows = trace["spans"]
        by_window = [0] * len(windows)
        for stamp, file, usec in trace["sync"]:
            window = next((i for i, (start, stop) in enumerate(windows) if start <= stamp <= stop), None)
            if window is not None:
                if Path(file).name != "index.db.primary":
                    raise ValueError("non-PRIMARY sync during timed public operation")
                if stamp + usec > windows[window][1]:
                    raise ValueError("PRIMARY sync completed after its measured window")
                by_window[window] += 1
                count += 1
                timed_sync_us += usec
            elif schedule != "original" and observed[0][0] <= stamp <= observed[-1][1]:
                raise ValueError("parked process submitted a sync")
        if schedule != "original" and any(
                observed[0][0] <= stamp <= observed[-1][1] and not any(start <= stamp <= stop for start, stop in windows)
                for stamp, _ in trace["write"]):
            raise ValueError("parked process submitted a store write")
        expected = gate.ITERATIONS + rows[role]["extra"]
        if count != expected or by_window != [expected // len(windows)] * len(windows):
            raise ValueError(f"{role} had {count} timed PRIMARY syncs by window {by_window}")
        result[role] = {"primary_syncs": count, "primary_sync_total_us": timed_sync_us,
                        "primary_syncs_by_window": by_window, "spans": windows}
    return result


def run(root: Path, output: Path, slot: int, head: str, smoke: bool, cpu: int):
    if output == root or root in output.parents:
        raise ValueError("output must be outside the checkout")
    output.mkdir(parents=True)
    temporary = output / "tmp"
    temporary.mkdir()
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="1", TMPDIR=str(temporary))
    ledger = []

    def command(label, args, *, timeout=90):
        argv = [str(arg) for arg in args]
        stdout = output / (label + ".stdout")
        stderr = output / (label + ".stderr")
        with stdout.open("xb") as out, stderr.open("xb") as err:
            completed = subprocess.run(argv, cwd=root, env=env, timeout=timeout, stdout=out, stderr=err)
        ledger.append({"label": label, "argv": argv, "exit": completed.returncode,
                       "stdout_sha256": gate.digest(stdout), "stderr_sha256": gate.digest(stderr)})
        save(output / "commands.json", ledger)
        completed.check_returncode()
        return stdout.read_text()

    guard = ["bash", root / "scripts/mvcc_candidate_checkout_guard.sh", root, head]
    command("checkout-before", guard)
    tree = command("tree", ["git", "rev-parse", "HEAD^{tree}"]).strip()
    command("go", ["go", "version"])
    command("kernel", ["uname", "-a"])
    command("cpu", ["lscpu"])
    command("mount", ["findmnt", "-T", temporary])
    command("block-devices", ["lsblk", "-o", "NAME,MAJ:MIN,TYPE,FSTYPE,MOUNTPOINTS"])
    command("strace", ["strace", "-V"])
    fixture = root / gate.HARNESS
    helpers = Path(__file__).with_name("mvcc_primary_interleave_test.go.in")
    generated = output / fixture.name
    generated.write_text(instrument(fixture.read_text(), helpers.read_text()))
    overlay = output / "overlay.json"
    save(overlay, {"Replace": {str(fixture): str(generated)}})
    executable = output / "primary-interleave.test"
    command("build", ["go", "test", "-c", "-p=1", "-tags=treedb_test", "-trimpath",
                      "-buildvcs=false", "-overlay", overlay, "-o", executable, "./TreeDB/mvcc"], timeout=600)
    runtime = gate.runtime_binding(executable, command, "before", env)
    sources = (fixture, generated, helpers, Path(__file__), Path(prior.__file__), Path(gate.__file__))
    digests = {str(path): gate.digest(path) for path in sources}
    bench = ["taskset", "-c", str(cpu), str(executable), "-test.run=^$", f"-test.bench=^{gate.BENCHMARK}$",
             f"-test.benchtime={gate.ITERATIONS}x", "-test.count=1", "-test.benchmem", "-test.timeout=90s"]
    lifecycle = command("lifecycle", ["taskset", "-c", str(cpu), executable, f"-test.run=^{gate.LIFECYCLE}$",
                                      "-test.count=1", "-test.timeout=90s", "-test.v"])
    if f"--- PASS: {gate.LIFECYCLE} " not in lifecycle or "--- SKIP:" in lifecycle:
        raise ValueError("maintained lifecycle failed")
    plan = {"head": head, "tree": tree, "slot": slot, "runtime": runtime, "inputs": digests, "bench": bench,
            "chunks": CHUNKS, "chunk_length": CHUNK, "extra_stride": EXTRA_STRIDE,
            "quiet_required": not smoke, "hosted": {key: env.get(key) for key in (
                "ImageOS", "ImageVersion", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "RUNNER_ARCH")},
            "blocks": rotate(slot), "time_limit_percent": gate.TIME_LIMIT_PERCENT, "enforcing": False}
    save(output / "plan.json", plan)

    results = []
    if not smoke:
        for number, (schedule, control, first) in enumerate(rotate(slot), 1):
            block = output / f"block-{number}"
            block.mkdir()
            gate.acquire_quiet_window(block)
            observer = gate.ProcessObservation(block / "measured-processes.jsonl")
            rows = []
            order = gate.sample_order(first)
            try:
                for pair, leading in order[::2]:
                    pair_rows = run_pair(block / f"pair-{pair + 1}", schedule, leading, control,
                                         bench, env, observer=observer)
                    rows.extend({"pair": pair, "revision": role, "metrics": pair_rows[role]["metrics"]}
                                for role in roles(leading))
                    save(block / "samples.json", rows)
            finally:
                observed = observer.finish()
                save(block / "measured-processes.json", observed)
            result = gate.evaluate(rows, first_revision=first)
            metrics = result["medians"]
            byte_budget = gate.byte_growth_budget(metrics["baseline"]["B/op"], metrics["candidate"]["B/op"])
            result.update(schedule=schedule, control=control, first=first, quiet=not observed["busy"],
                          v2_byte_budget=byte_budget,
                          null_allocators_pass=result["gates"]["allocations"] and
                          byte_budget["growth_B_per_op"] <= byte_budget["allowance_B_per_op"])
            results.append(result)
            save(output / "blocks.json", results)

    physical_cases = [(schedule, control, first) for schedule in SCHEDULES
                      for control, first in (("io", "baseline"), ("io", "candidate"))]
    if smoke:
        physical_cases = [("original", "io", "baseline"), ("resident-serial", "null", "candidate"),
                          ("interleaved", "io", "candidate")]
    physical = {}
    for schedule, control, first in physical_cases:
        name = f"trace-{schedule}-{control}-{first}"
        physical[name] = run_pair(output / name, schedule, first, control, bench, env, trace=True)["physical"]
    command("checkout-after", guard)
    if gate.runtime_binding(executable, command, "after", env) != runtime or any(
            gate.digest(path) != digests[str(path)] for path in sources):
        raise ValueError("source or runtime changed")
    save(output / "summary.json", {"blocks": results, "physical": physical, "enforcing": False})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--slot", type=int, choices=range(1, 5), default=1)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--cpu", type=int, default=0)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.head) or not args.smoke and args.cpu != 0:
        parser.error("require exact source head and CPU0 in hosted runs")
    run(args.root.resolve(), args.output.resolve(), args.slot, args.head, args.smoke, args.cpu)
