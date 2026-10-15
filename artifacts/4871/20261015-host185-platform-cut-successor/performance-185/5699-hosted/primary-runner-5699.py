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
QUIET_LIMIT_PERCENT = 2.0
ROW = re.compile(r"^" + BENCHMARK + r"(?:-\d+)?\s+(\d+)\s+(.*)$")
METRICS = {"ns/op", "B/op", "allocs/op", "generation_advances/op"}


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        result = hashlib.file_digest(stream, "sha256")
    return result.hexdigest()


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
        return {"elapsed_s": elapsed, "external_processes": rows,
                "busy": [record for record in rows if record["cpu_percent"] >= QUIET_LIMIT_PERCENT],
                "cpu_threshold_percent": QUIET_LIMIT_PERCENT, "sampling_interval_s": 0.25,
                "limit": "processes entirely between observations may be missed"}


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
             "bytes_and_allocations": "no median growth", "binaries": binaries})
        lifecycle = command("candidate-public-lifecycle", ["taskset", "-c", "0", output / "candidate-primary.test",
                "-test.run=^" + LIFECYCLE + "$", "-test.count=1", "-test.timeout=90s", "-test.v"])
        if f"--- PASS: {LIFECYCLE} " not in lifecycle or "--- SKIP:" in lifecycle:
            raise RuntimeError("actual candidate public lifecycle control did not pass")
        preflight = ProcessObservation(output / "preflight-processes.jsonl")
        while time.monotonic() - preflight.started < 5:
            time.sleep(0.25)
            preflight.observe()
        preflight_result = preflight.finish()
        save("preflight.json", preflight_result)
        if preflight_result["busy"]:
            raise RuntimeError("host quiet preflight refused; no timed samples started")
        observation = ProcessObservation(output / "measured-processes.jsonl")
        for pair in range(SAMPLES):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for revision in order:
                text = command(f"pair-{pair+1}-{revision}", ["taskset", "-c", "0",
                     output / binaries[revision]["path"], "-test.run=^$", "-test.bench=^" + BENCHMARK + "$",
                     "-test.benchtime=128x", "-test.count=1", "-test.benchmem", "-test.timeout=90s"])
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
        result = evaluate(rows)
        result.update(baseline=baseline, candidate=candidate, quiet=not busy,
                      accepted=result["measurement_pass"] and not busy)
        save("summary.json", result)
        (output / "summary.md").write_text(
            "## Public PRIMARY replacement cost\n\n"
            f"- Verdict: {'PASS' if result['accepted'] else 'FAIL'}\n"
            f"- Exact base/head: `{baseline}` / `{candidate}`\n"
            "- Eight alternating AB/BA pairs; 128 durable public replacements per sample.\n"
            f"- Median paired time delta: {result['median_paired_delta_percent']:+.3f}% (limit +5%).\n"
            f"- Gates: `{json.dumps(result['gates'], sort_keys=True)}`; quiet: {not busy}.\n"
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
